from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DB_PATH = PROJECT_ROOT / "etf-data" / "data" / "etf_daily.duckdb"
FACTOR_DB_PATH = PROJECT_ROOT / "data" / "etf_factor.duckdb"
SCHEMA_SQL_PATH = PROJECT_ROOT / "sql" / "schema.sql"
