from __future__ import annotations

import pandas as pd


def upsert_factor_definition(conn, factor_key: str, factor_name: str, params_json: str) -> None:
    conn.execute(
        """
        INSERT INTO factor_definitions (factor_key, factor_name, params_json)
        VALUES (?, ?, ?)
        ON CONFLICT (factor_key) DO UPDATE SET
            factor_name = EXCLUDED.factor_name,
            params_json = EXCLUDED.params_json
        """,
        [factor_key, factor_name, params_json],
    )


def replace_factor_values(conn, factor_key: str, factor_name: str, factor_df: pd.DataFrame) -> None:
    long_df = (
        factor_df.rename_axis(index="trade_date", columns="symbol")
        .reset_index()
        .melt(id_vars="trade_date", var_name="symbol", value_name="value")
    )
    long_df["trade_date"] = pd.to_datetime(long_df["trade_date"]).dt.date
    long_df["symbol"] = long_df["symbol"].astype(str)
    long_df["factor_key"] = factor_key
    long_df["factor_name"] = factor_name

    conn.register("factor_values_stage", long_df)
    conn.execute("DELETE FROM factor_values WHERE factor_key = ?", [factor_key])
    conn.execute(
        """
        INSERT INTO factor_values (trade_date, symbol, factor_key, factor_name, value)
        SELECT trade_date, symbol, factor_key, factor_name, value
        FROM factor_values_stage
        """
    )
    conn.unregister("factor_values_stage")


def replace_factor_ic(conn, factor_key: str, factor_name: str, pred_days: int, ic_series: pd.Series) -> None:
    ic_df = ic_series.rename("rank_ic").reset_index().rename(columns={"index": "trade_date"})
    ic_df["trade_date"] = pd.to_datetime(ic_df["trade_date"]).dt.date
    ic_df["factor_key"] = factor_key
    ic_df["factor_name"] = factor_name
    ic_df["pred_days"] = pred_days

    conn.register("factor_ic_stage", ic_df)
    conn.execute(
        "DELETE FROM factor_ic_daily WHERE factor_key = ? AND pred_days = ?",
        [factor_key, pred_days],
    )
    conn.execute(
        """
        INSERT INTO factor_ic_daily (trade_date, factor_key, factor_name, pred_days, rank_ic)
        SELECT trade_date, factor_key, factor_name, pred_days, rank_ic
        FROM factor_ic_stage
        """
    )
    conn.unregister("factor_ic_stage")


def replace_factor_metrics(conn, factor_key: str, factor_name: str, pred_days: int, ic_series: pd.Series) -> None:
    valid = ic_series.dropna()
    ic_mean = float(valid.mean()) if not valid.empty else None
    ic_std = float(valid.std(ddof=0)) if not valid.empty else None
    ic_ir = (ic_mean / ic_std) if ic_mean is not None and ic_std not in (None, 0.0) else None
    observation_count = int(valid.shape[0])

    conn.execute(
        """
        INSERT INTO factor_metrics (
            factor_key, factor_name, pred_days, ic_mean, ic_std, ic_ir, observation_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (factor_key, pred_days) DO UPDATE SET
            factor_name = EXCLUDED.factor_name,
            ic_mean = EXCLUDED.ic_mean,
            ic_std = EXCLUDED.ic_std,
            ic_ir = EXCLUDED.ic_ir,
            observation_count = EXCLUDED.observation_count
        """,
        [factor_key, factor_name, pred_days, ic_mean, ic_std, ic_ir, observation_count],
    )
