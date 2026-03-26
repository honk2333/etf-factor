from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .db import connect_duckdb, init_factor_db
from .factors import FACTOR_REGISTRY, factor_identity
from .market_data import apply_universe_mask, build_forward_return_matrix, load_market_data_dict
from .storage import (
    replace_factor_ic,
    replace_factor_metrics,
    replace_factor_values,
    upsert_factor_definition,
)


@dataclass
class ComputeSummary:
    symbol_count: int
    factor_count: int
    source_db: Path
    factor_db: Path


def compute_rank_ic(factor_df: pd.DataFrame, ret_df: pd.DataFrame) -> pd.Series:
    common_index = factor_df.index.intersection(ret_df.index)
    factor_df = factor_df.loc[common_index]
    ret_df = ret_df.loc[common_index]

    values = []
    for dt in common_index:
        cross_factor = factor_df.loc[dt]
        cross_ret = ret_df.loc[dt]
        valid = pd.concat([cross_factor, cross_ret], axis=1, keys=["factor", "ret"]).dropna()
        if len(valid) < 3:
            values.append(float("nan"))
            continue
        values.append(valid["factor"].corr(valid["ret"], method="spearman"))
    return pd.Series(values, index=common_index, name="rank_ic")


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
    start: str,
    pred_days: int,
    factor_names: set[str] | None = None,
    limit_symbols: int | None = None,
) -> ComputeSummary:
    bundle = load_market_data_dict(source_db=source_db, start=start, limit_symbols=limit_symbols)
    if not bundle.data_dict:
        raise RuntimeError("未从 etf-data 读取到可用 ETF 日线数据")

    ret_mat = build_forward_return_matrix(bundle.data_dict, pred_days)
    ret_mat = apply_universe_mask(ret_mat, bundle.universe_mask)

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
    for factor_func, params_list in selected_registry:
        for params in params_list:
            factor_key, factor_name, params_json = factor_identity(factor_func, params)
            factor_df = compute_factor_matrix(
                data_dict=bundle.data_dict,
                factor_func=factor_func,
                universe_mask=bundle.universe_mask,
                **params,
            )
            ic_series = compute_rank_ic(factor_df, ret_mat)

            upsert_factor_definition(conn, factor_key, factor_name, params_json)
            replace_factor_values(conn, factor_key, factor_name, factor_df)
            replace_factor_ic(conn, factor_key, factor_name, pred_days, ic_series)
            replace_factor_metrics(conn, factor_key, factor_name, pred_days, ic_series)

            count += 1
            if count % 10 == 0 or count == total:
                print(f"进度: {count}/{total}")

    conn.close()
    return ComputeSummary(
        symbol_count=len(bundle.data_dict),
        factor_count=total,
        source_db=source_db,
        factor_db=factor_db,
    )
