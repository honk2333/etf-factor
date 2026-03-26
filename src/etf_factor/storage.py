from __future__ import annotations

from datetime import date

import pandas as pd


def get_factor_latest_trade_date(conn, factor_key: str) -> date | None:
    row = conn.execute(
        "SELECT MAX(trade_date) FROM etf_factor WHERE factor_key = ?",
        [factor_key],
    ).fetchone()
    return row[0] if row and row[0] is not None else None


def replace_factor_values(
    conn,
    factor_key: str,
    factor_name: str,
    params_json: str,
    factor_df: pd.DataFrame,
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
        SELECT
            trade_date, symbol, factor_key, factor_name, params_json, value
        FROM etf_factor_stage
        """
    )
    conn.unregister("etf_factor_stage")
    return int(len(long_df))
