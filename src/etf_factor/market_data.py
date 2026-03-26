from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd


@dataclass
class MarketDataBundle:
    data_dict: dict[str, pd.DataFrame]
    universe_mask: pd.DataFrame


REQUIRED_COLUMNS = [
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
]


def load_market_data_dict(
    source_db: Path,
) -> MarketDataBundle:
    conn = duckdb.connect(str(source_db), read_only=True)
    query = """
        SELECT symbol, trade_date, open, high, low, close, volume, amount
        FROM etf_daily
        ORDER BY symbol, trade_date
    """
    df = conn.execute(query).df()
    conn.close()

    if df.empty:
        return MarketDataBundle(data_dict={}, universe_mask=pd.DataFrame())

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"etf_daily 缺少必要字段: {missing}")

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


def build_forward_return_matrix(data_dict: dict[str, pd.DataFrame], pred_days: int) -> pd.DataFrame:
    returns = {}
    for symbol, df in data_dict.items():
        returns[symbol] = df["close"].shift(-pred_days) / df["close"] - 1.0
    ret_mat = pd.concat(returns, axis=1)
    ret_mat.columns = ret_mat.columns.astype(str)
    return ret_mat.sort_index()


def apply_universe_mask(frame: pd.DataFrame, universe_mask: pd.DataFrame | None) -> pd.DataFrame:
    if universe_mask is None or universe_mask.empty:
        return frame

    masked = frame.copy()
    common_cols = [c for c in masked.columns if c in universe_mask.columns]
    if common_cols:
        masked.loc[:, common_cols] = masked.loc[:, common_cols].where(universe_mask.loc[masked.index, common_cols])
    return masked


def get_source_latest_trade_date(source_db: Path):
    conn = duckdb.connect(str(source_db), read_only=True)
    row = conn.execute("SELECT MAX(trade_date) FROM etf_daily").fetchone()
    conn.close()
    if row is None or row[0] is None:
        raise RuntimeError("etf_daily 中没有可用数据")
    return row[0]
