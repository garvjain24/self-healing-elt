"""DuckDB connection manager for the agent service database."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from agent_service.config import AGENT_DB_PATH

_SCHEMA_FILE = Path(__file__).resolve().parent / "schema.sql"


def get_conn(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path or AGENT_DB_PATH)


def init_db(db_path: str | None = None) -> None:
    conn = get_conn(db_path)
    sql = _SCHEMA_FILE.read_text()
    conn.execute(sql)
    conn.close()


def reset_db(db_path: str | None = None) -> None:
    path = db_path or AGENT_DB_PATH
    for p in (path, path + ".wal"):
        if os.path.exists(p):
            os.remove(p)
    init_db(path)


def verify_tables(db_path: str | None = None) -> list[str]:
    conn = get_conn(db_path)
    result = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    conn.close()
    return [r[0] for r in result]
