from __future__ import annotations

from pathlib import Path

import duckdb

from .config import SCHEMA_SQL_PATH


def connect_duckdb(path: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def init_factor_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(SCHEMA_SQL_PATH.read_text(encoding="utf-8"))
