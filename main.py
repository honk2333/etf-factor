from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

from factors import FACTOR_REGISTRY, factor_identity

PROJECT_ROOT = Path(__file__).resolve().parent
SOURCE_DB_PATH = PROJECT_ROOT / "etf-data" / "data" / "etf_daily.duckdb"
FACTOR_DB_PATH = PROJECT_ROOT / "data" / "etf_factor.duckdb"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS etf_factor (
    trade_date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    factor_key VARCHAR NOT NULL,
    factor_name VARCHAR NOT NULL,
    params_json VARCHAR NOT NULL,
    value DOUBLE,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, symbol, factor_key)
);

CREATE INDEX IF NOT EXISTS idx_etf_factor_factor_date
ON etf_factor (factor_key, trade_date);

CREATE INDEX IF NOT EXISTS idx_etf_factor_symbol_date
ON etf_factor (symbol, trade_date);
"""


def connect_db(path: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def ensure_factor_table(path: Path) -> duckdb.DuckDBPyConnection:
    conn = connect_db(path)
    conn.execute(SCHEMA_SQL)
    return conn


def load_market_data(source_db: Path):
    conn = connect_db(source_db, read_only=True)
    df = conn.execute(
        """
        SELECT symbol, trade_date, open, high, low, close, volume, amount
        FROM etf_daily
        ORDER BY symbol, trade_date
        """
    ).df()
    conn.close()

    if df.empty:
        return {}

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    data_dict: dict[str, pd.DataFrame] = {}
    for symbol, sub_df in df.groupby("symbol", sort=True):
        frame = sub_df.sort_values("trade_date").set_index("trade_date").copy()
        prev_close = frame["close"].shift(1)
        frame["pct_chg"] = frame["close"].pct_change()
        frame["amplitude"] = (frame["high"] - frame["low"]) / prev_close.replace(
            0, pd.NA
        )
        frame["amplitude"] = frame["amplitude"].fillna(
            (frame["high"] - frame["low"]) / frame["close"].replace(0, pd.NA)
        )
        data_dict[str(symbol)] = frame

    return data_dict


def get_latest_trade_date(source_db: Path):
    conn = connect_db(source_db, read_only=True)
    row = conn.execute("SELECT MAX(trade_date) FROM etf_daily").fetchone()
    conn.close()
    if row is None or row[0] is None:
        raise RuntimeError("etf_daily 中没有可用数据")
    return row[0]


def get_factor_latest_trade_date(conn, factor_key: str):
    row = conn.execute(
        "SELECT MAX(trade_date) FROM etf_factor WHERE factor_key = ?",
        [factor_key],
    ).fetchone()
    return row[0] if row and row[0] is not None else None


def compute_factor_matrix(
    data_dict: dict[str, pd.DataFrame], factor_func, **params
) -> pd.DataFrame:
    raw_factor = {symbol: factor_func(df, **params) for symbol, df in data_dict.items()}
    factor_df = pd.concat(raw_factor, axis=1)
    factor_df.columns = factor_df.columns.astype(str)
    return factor_df.sort_index()


def replace_factor_values(
    conn, factor_key: str, factor_name: str, params_json: str, factor_df: pd.DataFrame
) -> int:
    long_df = (
        factor_df.rename_axis(index="trade_date", columns="symbol")
        .reset_index()
        .melt(id_vars="trade_date", var_name="symbol", value_name="value")
    )
    long_df["trade_date"] = pd.to_datetime(long_df["trade_date"]).dt.date
    long_df["symbol"] = long_df["symbol"].astype(str)
    long_df["factor_key"] = factor_key
    long_df["factor_name"] = factor_name
    long_df["params_json"] = params_json

    conn.register("etf_factor_stage", long_df)
    conn.execute("DELETE FROM etf_factor WHERE factor_key = ?", [factor_key])
    conn.execute(
        """
        INSERT INTO etf_factor (
            trade_date, symbol, factor_key, factor_name, params_json, value
        )
        SELECT trade_date, symbol, factor_key, factor_name, params_json, value
        FROM etf_factor_stage
        """
    )
    conn.unregister("etf_factor_stage")
    return int(len(long_df))


def update_factors(source_db: Path, factor_db: Path, force: bool = False):
    data_dict = load_market_data(source_db)
    if not data_dict:
        raise RuntimeError("未从 etf-data 读取到可用 ETF 日线数据")

    latest_trade_date = get_latest_trade_date(source_db)
    conn = ensure_factor_table(factor_db)

    total = sum(len(params_list) for _, params_list in FACTOR_REGISTRY)
    refreshed = 0
    skipped = 0
    rows_written = 0
    count = 0

    for factor_func, params_list in FACTOR_REGISTRY:
        for params in params_list:
            factor_key, factor_name, params_json = factor_identity(factor_func, params)
            factor_latest_trade_date = get_factor_latest_trade_date(conn, factor_key)
            if not force and factor_latest_trade_date == latest_trade_date:
                skipped += 1
                count += 1
                if count % 10 == 0 or count == total:
                    print(f"进度: {count}/{total}")
                continue

            factor_df = compute_factor_matrix(data_dict, factor_func, **params)
            rows_written += replace_factor_values(
                conn, factor_key, factor_name, params_json, factor_df
            )
            refreshed += 1
            count += 1
            if count % 10 == 0 or count == total:
                print(f"进度: {count}/{total}")

    conn.close()
    return {
        "symbol_count": len(data_dict),
        "factor_count": total,
        "refreshed_factor_count": refreshed,
        "skipped_factor_count": skipped,
        "rows_written": rows_written,
        "latest_trade_date": latest_trade_date,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETF factor data management")
    parser.add_argument(
        "--source-db", default=str(SOURCE_DB_PATH), help="上游日线 DuckDB 路径"
    )
    parser.add_argument(
        "--factor-db", default=str(FACTOR_DB_PATH), help="因子 DuckDB 路径"
    )
    parser.add_argument(
        "--force", action="store_true", help="忽略最新日期检查，强制重算并覆盖"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = update_factors(
        source_db=Path(args.source_db),
        factor_db=Path(args.factor_db),
        force=args.force,
    )
    print(
        f"完成: symbols={summary['symbol_count']}, factors={summary['factor_count']}, "
        f"refreshed={summary['refreshed_factor_count']}, skipped={summary['skipped_factor_count']}, "
        f"rows_written={summary['rows_written']}, latest_trade_date={summary['latest_trade_date']}"
    )


if __name__ == "__main__":
    main()
