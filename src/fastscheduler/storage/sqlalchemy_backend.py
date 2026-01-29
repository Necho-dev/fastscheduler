"""SQLAlchemy-based database storage backend."""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    from sqlalchemy import (
        Column,
        Integer,
        String,
        Float,
        Boolean,
        Text,
        Index,
        create_engine,
        select,
    )
    from sqlalchemy.orm import declarative_base, Session, sessionmaker
except ImportError as e:
    raise ImportError(
        "SQLAlchemy storage requires sqlalchemy. "
        "Install with: pip install fastscheduler[database]"
    ) from e

from .base import StorageBackend
from .schema_migration import ensure_schema, SCHEMA_SQLALCHEMY

logger = logging.getLogger("fastscheduler")

Base = declarative_base()


class SchedulerJob(Base):
    """Database model for scheduled jobs."""

    __tablename__ = "scheduler_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, unique=True, nullable=False, index=True)
    job_name = Column(String, nullable=True, default="")  # Optional display name for persistence/recovery
    func_name = Column(String, nullable=False, index=True)
    func_module = Column(String, nullable=False)
    next_run = Column(Float, nullable=False, index=True)
    interval = Column(Float, nullable=True)
    repeat = Column(Boolean, default=True)
    status = Column(String, default="scheduled", index=True)
    created_at = Column(Float, nullable=False)
    last_run = Column(Float, nullable=True)
    run_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    retry_count = Column(Integer, default=0)
    catch_up = Column(Boolean, default=True)
    schedule_type = Column(String, default="interval")
    schedule_time = Column(String, nullable=True)
    schedule_days = Column(Text, nullable=True)  # Stored as JSON string
    timeout = Column(Float, nullable=True)
    paused = Column(Boolean, default=False, index=True)
    timezone = Column(String, nullable=True)
    cron_expression = Column(Text, nullable=True)
    args = Column(Text, nullable=True)  # JSON string
    kwargs = Column(Text, nullable=True)  # JSON string
    group = Column(String, default="default", index=True, nullable=False)  # Job group for business isolation

    __table_args__ = (
        Index("idx_jobs_next_run", "next_run"),
        Index("idx_jobs_status_paused", "status", "paused"),
        Index("idx_jobs_group", "group"),
        Index("idx_jobs_group_next_run", "group", "next_run"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Job reconstruction."""
        result = {
            "job_id": self.job_id,
            "job_name": self.job_name or "",
            "func_name": self.func_name,
            "func_module": self.func_module,
            "next_run": self.next_run,
            "interval": self.interval,
            "repeat": self.repeat,
            "status": self.status,
            "created_at": self.created_at,
            "last_run": self.last_run,
            "run_count": self.run_count,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "catch_up": self.catch_up,
            "schedule_type": self.schedule_type,
            "schedule_time": self.schedule_time,
            "timeout": self.timeout,
            "paused": self.paused,
            "timezone": self.timezone,
            "cron_expression": self.cron_expression,
            "group": self.group,
        }

        # Parse JSON fields
        if self.schedule_days:
            try:
                result["schedule_days"] = json.loads(self.schedule_days)
            except (json.JSONDecodeError, TypeError):
                result["schedule_days"] = None
        else:
            result["schedule_days"] = None

        if self.args:
            try:
                result["args"] = json.loads(self.args)
            except (json.JSONDecodeError, TypeError):
                result["args"] = ()
        else:
            result["args"] = ()

        if self.kwargs:
            try:
                result["kwargs"] = json.loads(self.kwargs)
            except (json.JSONDecodeError, TypeError):
                result["kwargs"] = {}
        else:
            result["kwargs"] = {}

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchedulerJob":
        """Create from job dictionary."""
        schedule_days = data.get("schedule_days")
        if schedule_days is not None and not isinstance(schedule_days, str):
            schedule_days = json.dumps(schedule_days)

        args = data.get("args", ())
        # Always serialize args as JSON string, even if empty
        if not isinstance(args, str):
            args = json.dumps(list(args) if args else [])

        kwargs = data.get("kwargs", {})
        # Always serialize kwargs as JSON string, even if empty
        if not isinstance(kwargs, str):
            kwargs = json.dumps(kwargs if kwargs else {})

        return cls(
            job_id=data["job_id"],
            job_name=data.get("job_name") or "",
            func_name=data["func_name"],
            func_module=data["func_module"],
            next_run=data["next_run"],
            interval=data.get("interval"),
            repeat=data.get("repeat", True),
            status=data.get("status", "scheduled"),
            created_at=data.get("created_at", time.time()),
            last_run=data.get("last_run"),
            run_count=data.get("run_count", 0),
            max_retries=data.get("max_retries", 3),
            retry_count=data.get("retry_count", 0),
            catch_up=data.get("catch_up", True),
            schedule_type=data.get("schedule_type", "interval"),
            schedule_time=data.get("schedule_time"),
            schedule_days=schedule_days,
            timeout=data.get("timeout"),
            paused=data.get("paused", False),
            timezone=data.get("timezone"),
            cron_expression=data.get("cron_expression"),
            group=data.get("group", "default"),
            args=args,
            kwargs=kwargs,
        )


class SchedulerHistory(Base):
    """Database model for job execution history."""

    __tablename__ = "scheduler_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, nullable=False, index=True)
    job_name = Column(String, nullable=True)  # Optional display name
    func_name = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False, index=True)
    timestamp = Column(Float, nullable=False, index=True)
    error = Column(Text, nullable=True)
    run_count = Column(Integer, default=0)
    retry_count = Column(Integer, default=0)
    execution_time = Column(Float, nullable=True)

    __table_args__ = (
        Index("idx_history_timestamp", "timestamp"),
        Index("idx_history_job_status", "job_id", "status"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "job_id": self.job_id,
            "job_name": getattr(self, "job_name", None),
            "func_name": self.func_name,
            "status": self.status,
            "timestamp": self.timestamp,
            "error": self.error,
            "run_count": self.run_count,
            "retry_count": self.retry_count,
            "execution_time": self.execution_time,
            "timestamp_readable": datetime.fromtimestamp(self.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchedulerHistory":
        """Create from history dictionary."""
        return cls(
            job_id=data["job_id"],
            job_name=data.get("job_name"),
            func_name=data["func_name"],
            status=data["status"],
            timestamp=data["timestamp"],
            error=data.get("error"),
            run_count=data.get("run_count", 0),
            retry_count=data.get("retry_count", 0),
            execution_time=data.get("execution_time"),
        )


class SchedulerDeadLetter(Base):
    """Database model for dead letter queue (failed jobs)."""

    __tablename__ = "scheduler_dead_letters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, nullable=False, index=True)
    job_name = Column(String, nullable=True)  # Optional display name
    func_name = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False)
    timestamp = Column(Float, nullable=False, index=True)
    error = Column(Text, nullable=True)
    run_count = Column(Integer, default=0)
    retry_count = Column(Integer, default=0)
    execution_time = Column(Float, nullable=True)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "job_id": self.job_id,
            "job_name": getattr(self, "job_name", None),
            "func_name": self.func_name,
            "status": self.status,
            "timestamp": self.timestamp,
            "error": self.error,
            "run_count": self.run_count,
            "retry_count": self.retry_count,
            "execution_time": self.execution_time,
            "timestamp_readable": datetime.fromtimestamp(self.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchedulerDeadLetter":
        """Create from dead letter dictionary."""
        return cls(
            job_id=data["job_id"],
            job_name=data.get("job_name"),
            func_name=data["func_name"],
            status=data["status"],
            timestamp=data["timestamp"],
            error=data.get("error"),
            run_count=data.get("run_count", 0),
            retry_count=data.get("retry_count", 0),
            execution_time=data.get("execution_time"),
        )


class SchedulerMetadata(Base):
    """Database model for scheduler metadata."""

    __tablename__ = "scheduler_metadata"

    key = Column(String, primary_key=True)
    value = Column(Text, nullable=False)
    updated_at = Column(Float, nullable=False, default=time.time)


class SQLAlchemyStorageBackend(StorageBackend):
    """
    SQLAlchemy-based database storage backend.

    Supports SQLite, PostgreSQL, MySQL, and other SQLAlchemy-compatible databases.
    Provides transactional integrity and better concurrency than JSON storage.

    Args:
        database_url: SQLAlchemy database URL (e.g., "sqlite:///scheduler.db",
            "postgresql://user:pass@host/db", "mysql://user:pass@host/db")
        echo: If True, log all SQL statements (default: False)
        quiet: If True, suppress info log messages

    Example:
        # SQLite (file-based)
        backend = SQLAlchemyStorageBackend("sqlite:///scheduler.db")

        # SQLite (in-memory, for testing)
        backend = SQLAlchemyStorageBackend("sqlite:///:memory:")

        # PostgreSQL
        backend = SQLAlchemyStorageBackend("postgresql://user:pass@localhost/mydb")

        # MySQL
        backend = SQLAlchemyStorageBackend("mysql://user:pass@localhost/mydb")
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
            pool_pre_ping=True,  # Verify connections before using
        )

        # Create session factory
        self.SessionLocal = sessionmaker(bind=self.engine)

        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
        # Ensure table schema: add any missing columns (backward-compat migration)
        ensure_schema(
            self.engine,
            SCHEMA_SQLALCHEMY,
            self.engine.dialect.name,
            quiet=self.quiet,
            log=logger,
        )

        if not quiet:
            logger.info(f"SQLAlchemy storage initialized: {self._safe_url()}")

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

    def _get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionLocal()

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
            with self._get_session() as session:
                # Update jobs - delete removed, update existing, add new
                existing_job_ids = set()
                for job_dict in jobs:
                    existing_job_ids.add(job_dict["job_id"])

                    # Check if job exists
                    existing = (
                        session.query(SchedulerJob)
                        .filter(SchedulerJob.job_id == job_dict["job_id"])
                        .first()
                    )

                    if existing:
                        # Update existing job
                        for key, value in job_dict.items():
                            if hasattr(existing, key):
                                if key == "args":
                                    if value is not None and not isinstance(value, str):
                                        value = json.dumps(list(value) if value else [])
                                    elif value is None:
                                        value = "[]"
                                elif key == "kwargs":
                                    if value is not None and not isinstance(value, str):
                                        value = json.dumps(value if value else {})
                                    elif value is None:
                                        value = "{}"
                                elif key == "schedule_days":
                                    if value is not None and not isinstance(value, str):
                                        value = json.dumps(value)
                                setattr(existing, key, value)
                        session.add(existing)
                    else:
                        # Add new job
                        new_job = SchedulerJob.from_dict(job_dict)
                        session.add(new_job)

                # Delete jobs that no longer exist
                session.query(SchedulerJob).filter(
                    ~SchedulerJob.job_id.in_(existing_job_ids)
                ).delete(synchronize_session=False)

                # Sync history - add new entries that aren't in the database
                # Get existing history timestamps to avoid duplicates
                existing_timestamps = {
                    row[0]
                    for row in session.query(SchedulerHistory.timestamp).all()
                }

                for hist_dict in history:
                    if hist_dict.get("timestamp") not in existing_timestamps:
                        # Remove timestamp_readable if present (not a model field)
                        clean_dict = {
                            k: v
                            for k, v in hist_dict.items()
                            if k != "timestamp_readable"
                        }
                        hist_model = SchedulerHistory.from_dict(clean_dict)
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
        existing = (
            session.query(SchedulerMetadata)
            .filter(SchedulerMetadata.key == key)
            .first()
        )

        if existing:
            existing.value = value
            existing.updated_at = time.time()
            session.add(existing)
        else:
            session.add(
                SchedulerMetadata(key=key, value=value, updated_at=time.time())
            )

    def _get_metadata(self, session: Session, key: str, default: str = "") -> str:
        """Get a metadata value by key."""
        result = (
            session.query(SchedulerMetadata)
            .filter(SchedulerMetadata.key == key)
            .first()
        )
        return result.value if result else default

    def load_state(self) -> Optional[Dict[str, Any]]:
        """Load scheduler state from database."""
        try:
            with self._get_session() as session:
                # Load jobs
                jobs = session.query(SchedulerJob).all()
                job_dicts = [job.to_dict() for job in jobs]

                # Load history (limited)
                history = (
                    session.query(SchedulerHistory)
                    .order_by(SchedulerHistory.timestamp.desc())
                    .limit(1000)
                    .all()
                )
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

    def save_dead_letters(
        self, dead_letters: List[Dict[str, Any]], max_dead_letters: int
    ) -> None:
        """Save dead letter queue to database."""
        try:
            with self._get_session() as session:
                # Clear existing and add new ones
                session.query(SchedulerDeadLetter).delete()

                # Add new entries (respect limit)
                for dl_dict in dead_letters[-max_dead_letters:]:
                    # Remove timestamp_readable if present
                    clean_dict = {
                        k: v
                        for k, v in dl_dict.items()
                        if k != "timestamp_readable"
                    }
                    dl_model = SchedulerDeadLetter.from_dict(clean_dict)
                    session.add(dl_model)

                session.commit()

        except Exception as e:
            logger.error(f"Failed to save dead letters: {e}")

    def load_dead_letters(self) -> List[Dict[str, Any]]:
        """Load dead letter queue from database."""
        try:
            with self._get_session() as session:
                dead_letters = (
                    session.query(SchedulerDeadLetter)
                    .order_by(SchedulerDeadLetter.timestamp)
                    .all()
                )
                return [dl.to_dict() for dl in dead_letters]

        except Exception as e:
            logger.error(f"Failed to load dead letters: {e}")
            return []

    def clear_dead_letters(self) -> int:
        """Clear all dead letter entries."""
        try:
            with self._get_session() as session:
                count = session.query(SchedulerDeadLetter).count()
                session.query(SchedulerDeadLetter).delete()
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
