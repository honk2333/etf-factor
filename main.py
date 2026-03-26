from __future__ import annotations

import argparse
from dataclasses import dataclass
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


@dataclass
class MarketDataBundle:
    data_dict: dict[str, pd.DataFrame]
    universe_mask: pd.DataFrame


@dataclass
class ComputeSummary:
    symbol_count: int
    factor_count: int
    refreshed_factor_count: int
    skipped_factor_count: int
    rows_written: int
    latest_trade_date: object


def connect_db(path: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def init_factor_db(path: Path) -> None:
    conn = connect_db(path)
    conn.execute("DROP TABLE IF EXISTS factor_definitions")
    conn.execute("DROP TABLE IF EXISTS factor_values")
    conn.execute("DROP TABLE IF EXISTS factor_ic_daily")
    conn.execute("DROP TABLE IF EXISTS factor_metrics")
    conn.execute(SCHEMA_SQL)
    conn.close()


def ensure_factor_table(path: Path) -> duckdb.DuckDBPyConnection:
    conn = connect_db(path)
    conn.execute(SCHEMA_SQL)
    return conn


def load_market_data(source_db: Path) -> MarketDataBundle:
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
        return MarketDataBundle(data_dict={}, universe_mask=pd.DataFrame())

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    data_dict: dict[str, pd.DataFrame] = {}
    for symbol, sub_df in df.groupby("symbol", sort=True):
        frame = sub_df.sort_values("trade_date").set_index("trade_date").copy()
        prev_close = frame["close"].shift(1)
        frame["pct_chg"] = frame["close"].pct_change()
        frame["amplitude"] = (frame["high"] - frame["low"]) / prev_close.replace(0, pd.NA)
        frame["amplitude"] = frame["amplitude"].fillna((frame["high"] - frame["low"]) / frame["close"].replace(0, pd.NA))
        data_dict[str(symbol)] = frame

    universe_mask = pd.concat(
        {symbol: frame["close"].notna() for symbol, frame in data_dict.items()}, axis=1
    )
    universe_mask.columns = universe_mask.columns.astype(str)
    universe_mask = universe_mask.sort_index()
    return MarketDataBundle(data_dict=data_dict, universe_mask=universe_mask)


def apply_universe_mask(frame: pd.DataFrame, universe_mask: pd.DataFrame) -> pd.DataFrame:
    masked = frame.copy()
    common_cols = [c for c in masked.columns if c in universe_mask.columns]
    if common_cols:
        masked.loc[:, common_cols] = masked.loc[:, common_cols].where(universe_mask.loc[masked.index, common_cols])
    return masked


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


def compute_factor_matrix(data_dict: dict[str, pd.DataFrame], factor_func, universe_mask: pd.DataFrame, **params) -> pd.DataFrame:
    raw_factor = {symbol: factor_func(df, **params) for symbol, df in data_dict.items()}
    factor_df = pd.concat(raw_factor, axis=1)
    factor_df.columns = factor_df.columns.astype(str)
    factor_df = factor_df.sort_index()
    return apply_universe_mask(factor_df, universe_mask)


def replace_factor_values(conn, factor_key: str, factor_name: str, params_json: str, factor_df: pd.DataFrame) -> int:
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


def update_factors(source_db: Path, factor_db: Path, factor_names: set[str] | None = None, force: bool = False) -> ComputeSummary:
    bundle = load_market_data(source_db)
    if not bundle.data_dict:
        raise RuntimeError("未从 etf-data 读取到可用 ETF 日线数据")

    latest_trade_date = get_latest_trade_date(source_db)
    conn = ensure_factor_table(factor_db)

    selected_registry = [
        (factor_func, params_list)
        for factor_func, params_list in FACTOR_REGISTRY
        if factor_names is None or factor_func.__name__ in factor_names
    ]
    if not selected_registry:
        raise ValueError("未匹配到任何因子，请检查 --factor-name 参数")

    total = sum(len(params_list) for _, params_list in selected_registry)
    refreshed = 0
    skipped = 0
    rows_written = 0
    count = 0

    for factor_func, params_list in selected_registry:
        for params in params_list:
            factor_key, factor_name, params_json = factor_identity(factor_func, params)
            factor_latest_trade_date = get_factor_latest_trade_date(conn, factor_key)
            if not force and factor_latest_trade_date == latest_trade_date:
                skipped += 1
                count += 1
                if count % 10 == 0 or count == total:
                    print(f"进度: {count}/{total}")
                continue

            factor_df = compute_factor_matrix(bundle.data_dict, factor_func, bundle.universe_mask, **params)
            rows_written += replace_factor_values(conn, factor_key, factor_name, params_json, factor_df)
            refreshed += 1
            count += 1
            if count % 10 == 0 or count == total:
                print(f"进度: {count}/{total}")

    conn.close()
    return ComputeSummary(
        symbol_count=len(bundle.data_dict),
        factor_count=total,
        refreshed_factor_count=refreshed,
        skipped_factor_count=skipped,
        rows_written=rows_written,
        latest_trade_date=latest_trade_date,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETF factor data management")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-db", help="初始化因子库")
    init_parser.add_argument("--factor-db", default=str(FACTOR_DB_PATH), help="因子 DuckDB 路径")

    update_parser = subparsers.add_parser("update", help="检查并更新因子")
    update_parser.add_argument("--source-db", default=str(SOURCE_DB_PATH), help="上游日线 DuckDB 路径")
    update_parser.add_argument("--factor-db", default=str(FACTOR_DB_PATH), help="因子 DuckDB 路径")
    update_parser.add_argument(
        "--factor-name",
        action="append",
        default=None,
        help="仅更新指定因子，可重复传入",
    )
    update_parser.add_argument("--force", action="store_true", help="忽略最新日期检查，强制重算并覆盖")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "init-db":
        init_factor_db(Path(args.factor_db))
        print(f"因子库已初始化: {args.factor_db}")
        return

    summary = update_factors(
        source_db=Path(args.source_db),
        factor_db=Path(args.factor_db),
        factor_names=set(args.factor_name) if args.factor_name else None,
        force=args.force,
    )
    print(
        f"完成: symbols={summary.symbol_count}, factors={summary.factor_count}, "
        f"refreshed={summary.refreshed_factor_count}, skipped={summary.skipped_factor_count}, "
        f"rows_written={summary.rows_written}, latest_trade_date={summary.latest_trade_date}"
    )


if __name__ == "__main__":
    main()
