"""Shared schema migration: ensure table columns match expected schema by adding missing columns."""

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("fastscheduler")

# (sqlite_type, other_dialect_type) for ALTER TABLE ADD COLUMN
_TYPE_SQLITE_OTHER: Dict[str, Tuple[str, str]] = {
    "string": ("TEXT", "VARCHAR(255)"),
    "text": ("TEXT", "TEXT"),
    "float": ("REAL", "FLOAT"),
    "integer": ("INTEGER", "INTEGER"),
    "boolean": ("INTEGER", "BOOLEAN"),
}

# Expected columns per table for SQLAlchemy backend (order matches model; id excluded).
# Format: (column_name, type_key)
SCHEMA_SQLALCHEMY: Dict[str, List[Tuple[str, str]]] = {
    "scheduler_jobs": [
        ("job_id", "string"),
        ("job_name", "string"),
        ("func_name", "string"),
        ("func_module", "string"),
        ("next_run", "float"),
        ("interval", "float"),
        ("repeat", "boolean"),
        ("status", "string"),
        ("created_at", "float"),
        ("last_run", "float"),
        ("run_count", "integer"),
        ("max_retries", "integer"),
        ("retry_count", "integer"),
        ("catch_up", "boolean"),
        ("schedule_type", "string"),
        ("schedule_time", "string"),
        ("schedule_days", "text"),
        ("timeout", "float"),
        ("paused", "boolean"),
        ("timezone", "string"),
        ("cron_expression", "text"),
        ("args", "text"),
        ("kwargs", "text"),
        ("group", "string"),
    ],
    "scheduler_history": [
        ("job_id", "string"),
        ("job_name", "string"),
        ("func_name", "string"),
        ("status", "string"),
        ("timestamp", "float"),
        ("error", "text"),
        ("run_count", "integer"),
        ("retry_count", "integer"),
        ("execution_time", "float"),
    ],
    "scheduler_dead_letters": [
        ("job_id", "string"),
        ("job_name", "string"),
        ("func_name", "string"),
        ("status", "string"),
        ("timestamp", "float"),
        ("error", "text"),
        ("run_count", "integer"),
        ("retry_count", "integer"),
        ("execution_time", "float"),
    ],
}

# Expected columns per table for SQLModel backend (matches JobModel, JobHistoryModel, DeadLetterModel).
SCHEMA_SQLMODEL: Dict[str, List[Tuple[str, str]]] = {
    "scheduler_jobs": [
        ("job_id", "string"),
        ("job_name", "string"),
        ("func_name", "string"),
        ("func_module", "string"),
        ("next_run", "float"),
        ("interval", "float"),
        ("repeat", "boolean"),
        ("status", "string"),
        ("created_at", "float"),
        ("last_run", "float"),
        ("run_count", "integer"),
        ("max_retries", "integer"),
        ("retry_count", "integer"),
        ("catch_up", "boolean"),
        ("schedule_type", "string"),
        ("schedule_time", "string"),
        ("schedule_days", "text"),
        ("timeout", "float"),
        ("paused", "boolean"),
        ("timezone", "string"),
        ("cron_expression", "text"),
    ],
    "scheduler_history": [
        ("job_id", "string"),
        ("job_name", "string"),
        ("func_name", "string"),
        ("status", "string"),
        ("timestamp", "float"),
        ("error", "text"),
        ("run_count", "integer"),
        ("retry_count", "integer"),
        ("execution_time", "float"),
    ],
    "scheduler_dead_letters": [
        ("job_id", "string"),
        ("job_name", "string"),
        ("func_name", "string"),
        ("status", "string"),
        ("timestamp", "float"),
        ("error", "text"),
        ("run_count", "integer"),
        ("retry_count", "integer"),
        ("execution_time", "float"),
    ],
}


def ensure_schema(
    engine: Any,
    expected_schema: Dict[str, List[Tuple[str, str]]],
    dialect_name: str,
    quiet: bool = False,
    log: Optional[logging.Logger] = None,
) -> None:
    """
    Ensure tables have all expected columns; add any missing columns (no drops).

    Args:
        engine: SQLAlchemy/SQLModel engine.
        expected_schema: Dict mapping table_name -> list of (column_name, type_key).
        dialect_name: e.g. "sqlite", "postgresql", "mysql".
        quiet: If True, do not log info about added columns.
        log: Logger to use (default: module logger).
    """
    from sqlalchemy import text
    from sqlalchemy import inspect as sa_inspect

    _log = log or logger
    is_sqlite = dialect_name == "sqlite"
    insp = sa_inspect(engine)

    for table_name, columns_spec in expected_schema.items():
        if table_name not in insp.get_table_names():
            continue
        current_cols = [c["name"] for c in insp.get_columns(table_name)]

        for col_name, type_key in columns_spec:
            if col_name in current_cols:
                continue
            sqlite_type, other_type = _TYPE_SQLITE_OTHER.get(
                type_key, ("TEXT", "VARCHAR(255)")
            )
            type_sql = sqlite_type if is_sqlite else other_type
            try:
                with engine.connect() as conn:
                    # Table/column names are our own; no user input
                    stmt = text(
                        f"ALTER TABLE {table_name} ADD COLUMN {col_name} {type_sql}"
                    )
                    conn.execute(stmt)
                    conn.commit()
                if not quiet:
                    _log.info(
                        "Schema migration: added column %s to %s",
                        col_name,
                        table_name,
                    )
            except Exception as e:
                _log.warning(
                    "Schema migration: failed to add column %s to %s: %s",
                    col_name,
                    table_name,
                    e,
                )
