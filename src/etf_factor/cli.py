from __future__ import annotations

import argparse
from pathlib import Path

from .compute import run_compute_pipeline
from .config import FACTOR_DB_PATH, SOURCE_DB_PATH
from .db import connect_duckdb, drop_legacy_tables, init_factor_db


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ETF factor data management")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-db", help="初始化因子 DuckDB")
    init_parser.add_argument("--factor-db", default=str(FACTOR_DB_PATH), help="因子 DuckDB 路径")

    compute_parser = subparsers.add_parser("compute-factors", help="计算并写入全部因子")
    compute_parser.add_argument("--source-db", default=str(SOURCE_DB_PATH), help="上游日线 DuckDB 路径")
    compute_parser.add_argument("--factor-db", default=str(FACTOR_DB_PATH), help="因子 DuckDB 路径")
    compute_parser.add_argument(
        "--factor-name",
        action="append",
        default=None,
        help="仅计算指定因子，可重复传入，如 --factor-name momentum --factor-name rsi",
    )
    compute_parser.add_argument("--force", action="store_true", help="忽略最新日期检查，强制重算并覆盖")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "init-db":
        conn = connect_duckdb(Path(args.factor_db))
        drop_legacy_tables(conn)
        init_factor_db(conn)
        conn.close()
        print(f"因子库已初始化: {args.factor_db}")
        return

    if args.command == "compute-factors":
        summary = run_compute_pipeline(
            source_db=Path(args.source_db),
            factor_db=Path(args.factor_db),
            factor_names=set(args.factor_name) if args.factor_name else None,
            force=args.force,
        )
        print(
            f"完成: symbols={summary.symbol_count}, factors={summary.factor_count}, "
            f"refreshed={summary.refreshed_factor_count}, skipped={summary.skipped_factor_count}, "
            f"rows_written={summary.rows_written}, latest_trade_date={summary.source_latest_trade_date}, "
            f"source_db={summary.source_db}, factor_db={summary.factor_db}"
        )
        return

    raise ValueError(f"未知命令: {args.command}")


if __name__ == "__main__":
    main()
