"""
DuckDB connection manager for the data pipeline warehouse.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from data_pipeline.config import DB_PATH

_SCHEMA_FILE = Path(__file__).resolve().parent / "schema.sql"


def get_conn(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection to the warehouse file."""
    path = db_path or DB_PATH
    return duckdb.connect(path)


def init_db(db_path: str | None = None) -> None:
    """Run the DDL script to create all tables and sequences."""
    conn = get_conn(db_path)
    sql = _SCHEMA_FILE.read_text()
    conn.execute(sql)
    conn.close()


def reset_db(db_path: str | None = None) -> None:
    """Drop the database file and recreate from scratch."""
    path = db_path or DB_PATH
    if os.path.exists(path):
        os.remove(path)
    # Also remove WAL file if present
    wal = path + ".wal"
    if os.path.exists(wal):
        os.remove(wal)
    init_db(path)


def verify_tables(db_path: str | None = None) -> list[str]:
    """Return list of table names in the warehouse."""
    conn = get_conn(db_path)
    result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
    conn.close()
    return [r[0] for r in result]
