from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .db import connect_duckdb, init_factor_db
from .factors import FACTOR_REGISTRY, factor_identity
from .market_data import (
    apply_universe_mask,
    get_source_latest_trade_date,
    load_market_data_dict,
)
from .storage import get_factor_latest_trade_date, replace_factor_values


@dataclass
class ComputeSummary:
    symbol_count: int
    factor_count: int
    refreshed_factor_count: int
    skipped_factor_count: int
    rows_written: int
    source_latest_trade_date: object
    source_db: Path
    factor_db: Path


def compute_factor_matrix(
    data_dict: dict[str, pd.DataFrame],
    factor_func,
    universe_mask: pd.DataFrame,
    **params,
) -> pd.DataFrame:
    raw_factor = {symbol: factor_func(df, **params) for symbol, df in data_dict.items()}
    factor_df = pd.concat(raw_factor, axis=1)
    factor_df.columns = factor_df.columns.astype(str)
    factor_df = factor_df.sort_index()
    return apply_universe_mask(factor_df, universe_mask)


def run_compute_pipeline(
    source_db: Path,
    factor_db: Path,
    factor_names: set[str] | None = None,
    force: bool = False,
) -> ComputeSummary:
    bundle = load_market_data_dict(source_db=source_db)
    if not bundle.data_dict:
        raise RuntimeError("未从 etf-data 读取到可用 ETF 日线数据")

    source_latest_trade_date = get_source_latest_trade_date(source_db)

    conn = connect_duckdb(factor_db)
    init_factor_db(conn)

    selected_registry = [
        (factor_func, params_list)
        for factor_func, params_list in FACTOR_REGISTRY
        if factor_names is None or factor_func.__name__ in factor_names
    ]
    if not selected_registry:
        raise ValueError("未匹配到任何因子，请检查 --factor-name 参数")

    total = sum(len(params_list) for _, params_list in selected_registry)
    count = 0
    refreshed_factor_count = 0
    skipped_factor_count = 0
    rows_written = 0

    for factor_func, params_list in selected_registry:
        for params in params_list:
            factor_key, factor_name, params_json = factor_identity(factor_func, params)
            factor_latest_trade_date = get_factor_latest_trade_date(conn, factor_key)

            if not force and factor_latest_trade_date == source_latest_trade_date:
                skipped_factor_count += 1
                count += 1
                if count % 10 == 0 or count == total:
                    print(f"进度: {count}/{total}")
                continue

            factor_df = compute_factor_matrix(
                data_dict=bundle.data_dict,
                factor_func=factor_func,
                universe_mask=bundle.universe_mask,
                **params,
            )
            rows_written += replace_factor_values(
                conn=conn,
                factor_key=factor_key,
                factor_name=factor_name,
                params_json=params_json,
                factor_df=factor_df,
            )
            refreshed_factor_count += 1
            count += 1
            if count % 10 == 0 or count == total:
                print(f"进度: {count}/{total}")

    conn.close()
    return ComputeSummary(
        symbol_count=len(bundle.data_dict),
        factor_count=total,
        refreshed_factor_count=refreshed_factor_count,
        skipped_factor_count=skipped_factor_count,
        rows_written=rows_written,
        source_latest_trade_date=source_latest_trade_date,
        source_db=source_db,
        factor_db=factor_db,
    )
