"""SQLModel-based database storage backend."""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    from sqlmodel import Session, SQLModel, create_engine, select
except ImportError as e:
    raise ImportError(
        "SQLModel storage requires sqlmodel. "
        "Install with: pip install fastscheduler[database]"
    ) from e

from .base import StorageBackend
from .models import DeadLetterModel, JobHistoryModel, JobModel, SchedulerMetadataModel

logger = logging.getLogger("fastscheduler")


class SQLModelStorageBackend(StorageBackend):
    """
    SQLModel-based database storage backend.

    Supports SQLite, PostgreSQL, MySQL, and other SQLAlchemy-compatible databases.
    Provides transactional integrity and better concurrency than JSON storage.

    Args:
        database_url: SQLAlchemy database URL (e.g., "sqlite:///scheduler.db",
            "postgresql://user:pass@host/db", "mysql://user:pass@host/db")
        echo: If True, log all SQL statements (default: False)
        quiet: If True, suppress info log messages

    Example:
        # SQLite (file-based)
        backend = SQLModelStorageBackend("sqlite:///scheduler.db")

        # SQLite (in-memory, for testing)
        backend = SQLModelStorageBackend("sqlite:///:memory:")

        # PostgreSQL
        backend = SQLModelStorageBackend("postgresql://user:pass@localhost/mydb")

        # MySQL
        backend = SQLModelStorageBackend("mysql://user:pass@localhost/mydb")
    """

    def __init__(
        self,
        database_url: str = "sqlite:///fastscheduler.db",
        echo: bool = False,
        quiet: bool = False,
    ):
        self.database_url = database_url
        self.quiet = quiet

        # Create engine with appropriate settings
        connect_args = {}
        if database_url.startswith("sqlite"):
            # SQLite needs check_same_thread=False for multi-threaded use
            connect_args["check_same_thread"] = False

        self.engine = create_engine(
            database_url,
            echo=echo,
            connect_args=connect_args,
        )

        # Create tables if they don't exist
        SQLModel.metadata.create_all(self.engine)

        if not quiet:
            logger.info(f"SQLModel storage initialized: {self._safe_url()}")

    def _safe_url(self) -> str:
        """Get database URL with password masked."""
        url = self.database_url
        if "@" in url and "://" in url:
            # Mask password in URL for logging
            pre, post = url.split("@", 1)
            if ":" in pre:
                proto_user = pre.rsplit(":", 1)[0]
                return f"{proto_user}:***@{post}"
        return url

    def save_state(
        self,
        jobs: List[Dict[str, Any]],
        history: List[Dict[str, Any]],
        statistics: Dict[str, Any],
        job_counter: int,
        scheduler_running: bool,
    ) -> None:
        """Save scheduler state to database."""
        try:
            with Session(self.engine) as session:
                # Update jobs - delete removed, update existing, add new
                existing_job_ids = set()
                for job_dict in jobs:
                    existing_job_ids.add(job_dict["job_id"])

                    # Check if job exists
                    statement = select(JobModel).where(
                        JobModel.job_id == job_dict["job_id"]
                    )
                    existing = session.exec(statement).first()

                    if existing:
                        # Update existing job
                        for key, value in job_dict.items():
                            if key == "schedule_days" and value is not None:
                                value = json.dumps(value)
                            if key == "status" and hasattr(value, "value"):
                                value = value.value
                            if hasattr(existing, key):
                                setattr(existing, key, value)
                        session.add(existing)
                    else:
                        # Add new job
                        new_job = JobModel.from_dict(job_dict)
                        session.add(new_job)

                # Delete jobs that no longer exist
                statement = select(JobModel)
                all_jobs = session.exec(statement).all()
                for db_job in all_jobs:
                    if db_job.job_id not in existing_job_ids:
                        session.delete(db_job)

                # Sync history - add new entries that aren't in the database
                # Get existing history timestamps to avoid duplicates
                statement = select(JobHistoryModel.timestamp)
                existing_timestamps = set(row for row in session.exec(statement).all())

                for hist_dict in history:
                    if hist_dict.get("timestamp") not in existing_timestamps:
                        # Remove timestamp_readable if present (not a model field)
                        clean_dict = {
                            k: v
                            for k, v in hist_dict.items()
                            if k != "timestamp_readable"
                        }
                        hist_model = JobHistoryModel.from_dict(clean_dict)
                        session.add(hist_model)

                # Save metadata
                self._save_metadata(
                    session,
                    "job_counter",
                    str(job_counter),
                )
                self._save_metadata(
                    session,
                    "statistics",
                    json.dumps(statistics),
                )
                self._save_metadata(
                    session,
                    "last_save",
                    json.dumps(
                        {
                            "timestamp": time.time(),
                            "readable": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "scheduler_running": scheduler_running,
                        }
                    ),
                )

                session.commit()

        except Exception as e:
            logger.error(f"Failed to save state to database: {e}")

    def _save_metadata(self, session: Session, key: str, value: str) -> None:
        """Save a metadata key-value pair."""
        statement = select(SchedulerMetadataModel).where(
            SchedulerMetadataModel.key == key
        )
        existing = session.exec(statement).first()

        if existing:
            existing.value = value
            session.add(existing)
        else:
            session.add(SchedulerMetadataModel(key=key, value=value))

    def _get_metadata(self, session: Session, key: str, default: str = "") -> str:
        """Get a metadata value by key."""
        statement = select(SchedulerMetadataModel).where(
            SchedulerMetadataModel.key == key
        )
        result = session.exec(statement).first()
        return result.value if result else default

    def load_state(self) -> Optional[Dict[str, Any]]:
        """Load scheduler state from database."""
        try:
            with Session(self.engine) as session:
                # Load jobs
                statement = select(JobModel)
                jobs = session.exec(statement).all()
                job_dicts = [job.to_dict() for job in jobs]

                # Load history (limited)
                statement = (
                    select(JobHistoryModel)
                    .order_by(JobHistoryModel.timestamp.desc())
                    .limit(1000)
                )
                history = session.exec(statement).all()
                history_dicts = [h.to_dict() for h in reversed(history)]

                # Load metadata
                job_counter = int(self._get_metadata(session, "job_counter", "0"))
                stats_json = self._get_metadata(session, "statistics", "{}")
                statistics = json.loads(stats_json)

                if not job_dicts and not history_dicts:
                    if not self.quiet:
                        logger.info("No previous state found, starting fresh")
                    return None

                return {
                    "jobs": job_dicts,
                    "history": history_dicts,
                    "statistics": statistics,
                    "_job_counter": job_counter,
                }

        except Exception as e:
            logger.error(f"Failed to load state from database: {e}")
            return None

    def save_history_entry(self, entry: Dict[str, Any]) -> None:
        """Save a single history entry (for incremental updates)."""
        try:
            with Session(self.engine) as session:
                history_model = JobHistoryModel.from_dict(entry)
                session.add(history_model)
                session.commit()
        except Exception as e:
            logger.error(f"Failed to save history entry: {e}")

    def cleanup_history(self, max_history: int, retention_days: int) -> None:
        """Clean up old history entries."""
        try:
            with Session(self.engine) as session:
                # Remove entries older than retention period
                if retention_days > 0:
                    cutoff_time = time.time() - (retention_days * 86400)
                    statement = select(JobHistoryModel).where(
                        JobHistoryModel.timestamp < cutoff_time
                    )
                    old_entries = session.exec(statement).all()
                    for entry in old_entries:
                        session.delete(entry)

                # Count remaining entries
                statement = select(JobHistoryModel)
                all_entries = session.exec(statement).all()

                # Remove oldest if over max
                if len(all_entries) > max_history:
                    # Sort by timestamp and delete oldest
                    sorted_entries = sorted(all_entries, key=lambda x: x.timestamp)
                    to_delete = sorted_entries[: len(all_entries) - max_history]
                    for entry in to_delete:
                        session.delete(entry)

                session.commit()

        except Exception as e:
            logger.error(f"Failed to cleanup history: {e}")

    def save_dead_letters(
        self, dead_letters: List[Dict[str, Any]], max_dead_letters: int
    ) -> None:
        """Save dead letter queue to database."""
        try:
            with Session(self.engine) as session:
                # Clear existing and add new ones
                statement = select(DeadLetterModel)
                existing = session.exec(statement).all()
                for entry in existing:
                    session.delete(entry)

                # Add new entries (respect limit)
                for dl_dict in dead_letters[-max_dead_letters:]:
                    dl_model = DeadLetterModel.from_dict(dl_dict)
                    session.add(dl_model)

                session.commit()

        except Exception as e:
            logger.error(f"Failed to save dead letters: {e}")

    def add_dead_letter(self, entry: Dict[str, Any], max_dead_letters: int) -> None:
        """Add a single dead letter entry."""
        try:
            with Session(self.engine) as session:
                # Add new entry
                dl_model = DeadLetterModel.from_dict(entry)
                session.add(dl_model)

                # Enforce limit
                statement = select(DeadLetterModel).order_by(DeadLetterModel.timestamp)
                all_entries = session.exec(statement).all()

                if len(all_entries) > max_dead_letters:
                    # Delete oldest entries
                    to_delete = all_entries[: len(all_entries) - max_dead_letters + 1]
                    for entry in to_delete:
                        session.delete(entry)

                session.commit()

        except Exception as e:
            logger.error(f"Failed to add dead letter: {e}")

    def load_dead_letters(self) -> List[Dict[str, Any]]:
        """Load dead letter queue from database."""
        try:
            with Session(self.engine) as session:
                statement = select(DeadLetterModel).order_by(DeadLetterModel.timestamp)
                dead_letters = session.exec(statement).all()
                return [dl.to_dict() for dl in dead_letters]

        except Exception as e:
            logger.error(f"Failed to load dead letters: {e}")
            return []

    def clear_dead_letters(self) -> int:
        """Clear all dead letter entries."""
        try:
            with Session(self.engine) as session:
                statement = select(DeadLetterModel)
                entries = session.exec(statement).all()
                count = len(entries)

                for entry in entries:
                    session.delete(entry)

                session.commit()
                return count

        except Exception as e:
            logger.error(f"Failed to clear dead letters: {e}")
            return 0

    def close(self) -> None:
        """Dispose of the database engine."""
        try:
            self.engine.dispose()
        except Exception as e:
            logger.error(f"Error disposing database engine: {e}")
