"""FastScheduler - Simple, powerful, persistent task scheduler."""

import asyncio
import itertools
import logging
import threading
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional, Union
from zoneinfo import ZoneInfo

try:
    import anyio
    ANYIO_AVAILABLE = True
except ImportError:
    ANYIO_AVAILABLE = False
    anyio = None  # type: ignore

from .models import Job, JobHistory, JobStatus
from .schedulers import (
    CronScheduler,
    DailyScheduler,
    HourlyScheduler,
    IntervalScheduler,
    OnceScheduler,
    WeeklyScheduler,
)

if TYPE_CHECKING:
    from .storage import StorageBackend
    from .queue import QueueBackend

try:
    from croniter import croniter

    CRONITER_AVAILABLE = True
except ImportError:
    CRONITER_AVAILABLE = False
    croniter = None  # type: ignore

logger = logging.getLogger("fastscheduler")
logger.addHandler(logging.NullHandler())


class FastScheduler:
    """
    FastScheduler - Simple, powerful, persistent task scheduler with async support.

        Args:
        state_file: Path to the JSON file for persisting scheduler state (used with JSON backend)
        storage: Storage backend - "json" (default), "sqlalchemy", "sqlmodel" (deprecated), or a StorageBackend instance
        database_url: Database URL for SQLAlchemy backend (e.g., "sqlite:///scheduler.db",
            "postgresql://user:pass@host/db", "mysql://user:pass@host/db")
        queue: Queue backend - "heapq" (default, in-memory), "redis" (distributed), or a QueueBackend instance
        redis_url: Redis connection URL for Redis queue backend (e.g., "redis://localhost:6379/0")
        queue_key: Redis key for the queue (default: "fastscheduler:queue")
        auto_start: If True, start the scheduler immediately
        quiet: If True, suppress most log messages
        max_history: Maximum number of history entries to keep (default: 10000)
        max_workers: Maximum number of worker threads for job execution (default: 10)
        history_retention_days: Maximum age of history entries in days (default: 7)
        max_dead_letters: Maximum number of failed job entries to keep in dead letter queue (default: 500)

    Examples:
        # JSON storage (default, backward compatible)
        scheduler = FastScheduler(state_file="scheduler.json")

        # SQLite database
        scheduler = FastScheduler(
            storage="sqlalchemy",
            database_url="sqlite:///scheduler.db"
        )

        # PostgreSQL database
        scheduler = FastScheduler(
            storage="sqlalchemy",
            database_url="postgresql://user:pass@localhost/mydb"
        )

        # MySQL database
        scheduler = FastScheduler(
            storage="sqlalchemy",
            database_url="mysql://user:pass@localhost/mydb"
        )

        # Redis distributed queue
        scheduler = FastScheduler(
            queue="redis",
            redis_url="redis://localhost:6379/0"
        )

        # Custom storage backend
        scheduler = FastScheduler(storage=MyCustomStorageBackend())
        
        # Custom queue backend
        scheduler = FastScheduler(queue=MyCustomQueueBackend())
    """

    def __init__(
        self,
        state_file: str = "fastscheduler_state.json",
        storage: Optional[Union[str, "StorageBackend"]] = None,
        database_url: Optional[str] = None,
        queue: Optional[Union[str, "QueueBackend"]] = None,
        redis_url: Optional[str] = None,
        queue_key: Optional[str] = None,
        auto_start: bool = False,
        quiet: bool = False,
        max_history: int = 10000,
        max_workers: int = 10,
        history_retention_days: int = 7,
        max_dead_letters: int = 500,
    ):
        self._storage = self._init_storage(storage, state_file, database_url, quiet)
        self.state_file = Path(state_file)

        # Initialize queue backend
        self._queue = self._init_queue(queue, redis_url, queue_key, quiet)
        
        # Keep self.jobs for backward compatibility (property that wraps queue)
        # But internally use self._queue
        self.job_registry: Dict[str, Callable] = {}
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.RLock()
        self._job_counter: Iterator[int] = itertools.count()
        self._job_counter_value = 0
        self.history: List[JobHistory] = []
        self.max_history = max_history
        self.max_workers = max_workers
        self.history_retention_days = history_retention_days
        self.max_dead_letters = max_dead_letters
        self.quiet = quiet
        self._running_jobs: set = set()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="FastScheduler-Worker"
        )
        self._save_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="FastScheduler-Saver"
        )

        self.dead_letters: List[JobHistory] = []
        self._dead_letters_file = Path(
            str(self.state_file).replace(".json", "_dead_letters.json")
        )

        # Graceful shutdown coordination for SSE connections
        self._shutdown_event_set = False
        self._shutdown_event_lock = threading.Lock()

        self.stats = {
            "total_runs": 0,
            "total_failures": 0,
            "total_retries": 0,
            "start_time": None,
        }

        self._load_state()
        self._load_dead_letters()

        if auto_start:
            self.start()

    def shutdown_connection(self) -> None:
        """Signal shutdown to external components (e.g. SSE connections)."""
        with self._shutdown_event_lock:
            self._shutdown_event_set = True

    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        with self._shutdown_event_lock:
            return self._shutdown_event_set

    def _init_queue(
        self,
        queue: Optional[Union[str, "QueueBackend"]],
        redis_url: Optional[str],
        queue_key: Optional[str],
        quiet: bool,
    ) -> "QueueBackend":
        """Initialize the queue backend."""
        from .queue import QueueBackend, get_heapq_backend, get_redis_backend

        if isinstance(queue, QueueBackend):
            return queue

        if queue is None or queue == "heapq":
            HeapqQueueBackend = get_heapq_backend()
            return HeapqQueueBackend(quiet=quiet)

        if queue == "redis":
            RedisQueueBackend = get_redis_backend()
            url = redis_url or "redis://localhost:6379/0"
            key = queue_key or "fastscheduler:queue"
            return RedisQueueBackend(
                redis_url=url,
                queue_key=key,
                quiet=quiet
            )

        raise ValueError(
            f"Unknown queue backend: {queue}. "
            "Use 'heapq', 'redis', or provide a QueueBackend instance."
        )

    @property
    def jobs(self) -> List[Job]:
        """Get all jobs from the queue (for backward compatibility)."""
        return self._queue.get_all()

    def _init_storage(
        self,
        storage: Optional[Union[str, "StorageBackend"]],
        state_file: str,
        database_url: Optional[str],
        quiet: bool,
    ) -> "StorageBackend":
        """Initialize the storage backend."""
        from .storage import JSONStorageBackend, StorageBackend

        if isinstance(storage, StorageBackend):
            return storage

        if storage is None or storage == "json":
            return JSONStorageBackend(state_file=state_file, quiet=quiet)

        if storage == "sqlalchemy":
            from .storage import get_sqlalchemy_backend

            SQLAlchemyStorageBackend = get_sqlalchemy_backend()
            url = database_url or "sqlite:///fastscheduler.db"
            return SQLAlchemyStorageBackend(database_url=url, quiet=quiet)

        # Backward compatibility: support sqlmodel
        if storage == "sqlmodel":
            from .storage import get_sqlmodel_backend

            SQLModelStorageBackend = get_sqlmodel_backend()
            url = database_url or "sqlite:///fastscheduler.db"
            return SQLModelStorageBackend(database_url=url, quiet=quiet)

        raise ValueError(
            f"Unknown storage backend: {storage}. "
            "Use 'json', 'sqlalchemy', 'sqlmodel' (deprecated), or provide a StorageBackend instance."
        )

    # ==================== User-Friendly Scheduling API ====================

    def every(self, interval: Union[int, float]) -> IntervalScheduler:
        """Schedule a task to run every X seconds/minutes/hours/days."""
        return IntervalScheduler(self, interval)

    @property
    def daily(self) -> DailyScheduler:
        """Schedule a task to run daily at a specific time."""
        return DailyScheduler(self)

    @property
    def weekly(self) -> WeeklyScheduler:
        """Schedule a task to run weekly on specific days."""
        return WeeklyScheduler(self)

    @property
    def hourly(self) -> HourlyScheduler:
        """Schedule a task to run hourly at a specific minute."""
        return HourlyScheduler(self)

    def cron(self, expression: str) -> CronScheduler:
        """
        Schedule a task using a cron expression.

        Requires croniter: pip install fastscheduler[cron]

        Args:
            expression: Cron expression (e.g., "0 9 * * MON-FRI" for 9 AM on weekdays)

        Usage:
            @scheduler.cron("0 9 * * MON-FRI")
            def weekday_task():
                ...

            @scheduler.cron("*/5 * * * *")  # Every 5 minutes
            def frequent_task():
                ...
        """
        if not CRONITER_AVAILABLE:
            raise ImportError(
                "Cron scheduling requires croniter. "
                "Install with: pip install fastscheduler[cron]"
            )
        return CronScheduler(self, expression)

    def once(self, delay: Union[int, float]) -> OnceScheduler:
        """Schedule a one-time task."""
        scheduler = OnceScheduler(self, delay)
        scheduler._decorator_mode = True
        return scheduler

    def at(self, target_time: Union[datetime, str]) -> OnceScheduler:
        """Schedule a task at a specific datetime."""
        if isinstance(target_time, str):
            target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M:%S")

        delay = (target_time - datetime.now()).total_seconds()
        if delay < 0:
            raise ValueError("Target time is in the past")

        scheduler = OnceScheduler(self, delay)
        scheduler._decorator_mode = True
        return scheduler

    # ==================== Internal Methods ====================

    def _register_function(self, func: Callable):
        """Register a function for persistence."""
        self.job_registry[f"{func.__module__}.{func.__name__}"] = func

    def register_function(self, func: Callable, name: Optional[str] = None):
        """
        Register a function for task scheduling (public API).
        
        Args:
            func: The function to register
            name: Optional custom name. If not provided, uses func.__module__.func.__name__
        
        Example:
            def my_task():
                print("Task executed")
            
            scheduler.register_function(my_task)
        """
        func_name = name or f"{func.__module__}.{func.__name__}"
        self.job_registry[func_name] = func
        if not self.quiet:
            logger.info(f"Registered function: {func_name}")

    def _next_job_id(self) -> str:
        """Generate next job ID (thread-safe)."""
        self._job_counter_value = next(self._job_counter)
        return f"job_{self._job_counter_value}"

    def _add_job(self, job: Job):
        """Add job to the priority queue."""
        with self.lock:
            # Check if job already exists
            existing_jobs = self._queue.get_all()
            if any(j.job_id == job.job_id for j in existing_jobs):
                logger.warning(f"Job {job.job_id} already exists, skipping")
                return

            self._queue.push(job)
            self._log_history(job.job_id, job.func_name, JobStatus.SCHEDULED)

            schedule_desc = job.get_schedule_description()
            if not self.quiet:
                logger.info(f"Scheduled: {job.func_name} - {schedule_desc}")

        self._save_state_async()

    def _log_history(
        self,
        job_id: str,
        func_name: str,
        status: JobStatus,
        error: Optional[str] = None,
        run_count: int = 0,
        retry_count: int = 0,
        execution_time: Optional[float] = None,
    ):
        """Log job events to history."""
        history_entry = JobHistory(
            job_id=job_id,
            func_name=func_name,
            status=status.value,
            timestamp=time.time(),
            error=error,
            run_count=run_count,
            retry_count=retry_count,
            execution_time=execution_time,
        )

        with self.lock:
            self.history.append(history_entry)
            self._cleanup_history()

            if status == JobStatus.FAILED and error:
                self.dead_letters.append(history_entry)
                if len(self.dead_letters) > self.max_dead_letters:
                    self.dead_letters = self.dead_letters[-self.max_dead_letters :]
                self._save_dead_letters_async()

    def _cleanup_history(self):
        """Clean up old history entries based on count and age limits."""
        if self.history_retention_days > 0:
            cutoff_time = time.time() - (self.history_retention_days * 86400)
            self.history = [h for h in self.history if h.timestamp >= cutoff_time]

        if len(self.history) > self.max_history:
            self.history = self.history[-self.max_history :]

    def start(self):
        """Start the scheduler."""
        if self.running:
            logger.warning("Scheduler already running")
            return

        self.running = True
        self.stats["start_time"] = time.time()

        self._handle_missed_jobs()

        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="FastScheduler-Main",
        )
        self.thread.start()

        if not self.quiet:
            logger.info("FastScheduler started")
        self._save_state_async()

    def stop(self, wait: bool = True, timeout: int = 30):
        """Stop the scheduler gracefully."""
        if not self.running:
            return

        logger.info("Stopping scheduler...")
        self.shutdown_connection()
        self.running = False

        if wait and self.thread and self.thread.is_alive():
            self.thread.join(timeout=timeout)

        if wait:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._save_executor.shutdown(wait=True)
        else:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._save_executor.shutdown(wait=False)

        self._save_state()
        self._storage.close()
        self._queue.close()

        if not self.quiet:
            logger.info("FastScheduler stopped")

    def _handle_missed_jobs(self):
        """Handle jobs that should have run while scheduler was stopped."""
        now = time.time()

        with self.lock:
            all_jobs = self._queue.get_all()
            for job in all_jobs:
                if not job.catch_up:
                    continue

                if job.next_run < now and job.repeat:
                    if job.schedule_type in ["daily", "weekly", "hourly"]:
                        self._calculate_next_run(job)
                    elif job.interval:
                        missed_count = int((now - job.next_run) / job.interval)
                        if missed_count > 0:
                            if not self.quiet:
                                logger.warning(
                                    f"Job {job.func_name} missed {missed_count} runs, running now"
                                )
                            job.next_run = now

                elif job.next_run < now and not job.repeat:
                    if not self.quiet:
                        logger.warning(
                            f"One-time job {job.func_name} was missed, running now"
                        )
                    job.next_run = now

    def _calculate_next_run(self, job: Job):
        """Calculate next run time for time-based schedules."""
        if job.timezone:
            tz = ZoneInfo(job.timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()
            tz = None

        if job.schedule_type == "cron" and job.cron_expression and CRONITER_AVAILABLE:
            base_time = now if tz else datetime.now()
            cron = croniter(job.cron_expression, base_time)
            next_run = cron.get_next(datetime)
            job.next_run = next_run.timestamp()

        elif job.schedule_type == "daily" and job.schedule_time:
            hour, minute = map(int, job.schedule_time.split(":"))
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

            if next_run <= now:
                next_run += timedelta(days=1)

            job.next_run = next_run.timestamp()

        elif job.schedule_type == "weekly" and job.schedule_time and job.schedule_days:
            hour, minute = map(int, job.schedule_time.split(":"))

            for i in range(8):
                check_date = now + timedelta(days=i)
                if check_date.weekday() in job.schedule_days:
                    next_run = check_date.replace(
                        hour=hour, minute=minute, second=0, microsecond=0
                    )
                    if next_run > now:
                        job.next_run = next_run.timestamp()
                        return

        elif job.schedule_type == "hourly" and job.schedule_time:
            minute = int(job.schedule_time.strip(":"))
            next_run = now.replace(minute=minute, second=0, microsecond=0)

            if next_run <= now:
                next_run += timedelta(hours=1)

            job.next_run = next_run.timestamp()

    def _run(self):
        """Main scheduler loop - runs in background thread."""
        if not self.quiet:
            logger.info("Scheduler main loop started")

        while self.running:
            try:
                now = time.time()
                jobs_to_run = []

                with self.lock:
                    # Peek at the next job
                    next_job = self._queue.peek()
                    while next_job and next_job.next_run <= now:
                        job = self._queue.pop()
                        if job is None:
                            break

                        # Restore function reference if needed (for Redis backend)
                        if job.func is None and job.func_name and job.func_module:
                            func_key = f"{job.func_module}.{job.func_name}"
                            if func_key in self.job_registry:
                                job.func = self.job_registry[func_key]
                            else:
                                logger.warning(
                                    f"Job {job.job_id} function {func_key} not registered, skipping"
                                )
                                # Check next job
                                next_job = self._queue.peek()
                                continue

                        if job.paused:
                            job.next_run = time.time() + 1.0
                            self._queue.push(job)
                            # Check next job
                            next_job = self._queue.peek()
                            continue

                        jobs_to_run.append(job)

                        if job.repeat:
                            if job.schedule_type in [
                                "daily",
                                "weekly",
                                "hourly",
                                "cron",
                            ]:
                                self._calculate_next_run(job)
                            elif job.interval:
                                job.next_run = time.time() + job.interval

                            job.status = JobStatus.SCHEDULED
                            job.retry_count = 0
                            self._queue.push(job)
                        
                        # Check next job
                        next_job = self._queue.peek()

                for job in jobs_to_run:
                    self._executor.submit(self._execute_job, job)

                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in main loop: {e}\n{traceback.format_exc()}")
                time.sleep(1)

        if not self.quiet:
            logger.info("Scheduler main loop stopped")

    def _execute_job(self, job: Job):
        """Execute a job with retries."""
        if job.func is None:
            logger.error(f"Job {job.func_name} has no function, skipping")
            return

        with self.lock:
            self._running_jobs.add(job.job_id)

        job.status = JobStatus.RUNNING
        job.last_run = time.time()
        job.run_count += 1

        self._log_history(
            job.job_id,
            job.func_name,
            JobStatus.RUNNING,
            run_count=job.run_count,
            retry_count=job.retry_count,
        )

        start_time = time.time()

        try:
            if asyncio.iscoroutinefunction(job.func):
                if job.timeout:
                    asyncio.run(
                        asyncio.wait_for(
                            job.func(*job.args, **job.kwargs), timeout=job.timeout
                        )
                    )
                else:
                    asyncio.run(job.func(*job.args, **job.kwargs))
            else:
                if job.timeout:
                    from concurrent.futures import TimeoutError as FuturesTimeoutError

                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(job.func, *job.args, **job.kwargs)
                        try:
                            future.result(timeout=job.timeout)
                        except FuturesTimeoutError:
                            raise TimeoutError(f"Job timed out after {job.timeout}s")
                else:
                    job.func(*job.args, **job.kwargs)

            execution_time = time.time() - start_time

            if job.repeat:
                job.status = JobStatus.SCHEDULED
            else:
                job.status = JobStatus.COMPLETED

            with self.lock:
                self.stats["total_runs"] += 1

            self._log_history(
                job.job_id,
                job.func_name,
                JobStatus.COMPLETED,
                run_count=job.run_count,
                retry_count=job.retry_count,
                execution_time=execution_time,
            )

            if not self.quiet:
                logger.info(f"{job.func_name} completed ({execution_time:.2f}s)")

        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"{type(e).__name__}: {str(e)}"

            with self.lock:
                self.stats["total_failures"] += 1

            if job.retry_count < job.max_retries:
                job.retry_count += 1
                job.status = JobStatus.SCHEDULED
                retry_delay = 2**job.retry_count
                job.next_run = time.time() + retry_delay

                with self.lock:
                    self._queue.push(job)
                    self.stats["total_retries"] += 1

                if not self.quiet:
                    logger.warning(
                        f"{job.func_name} failed, retrying in {retry_delay}s "
                        f"({job.retry_count}/{job.max_retries})"
                    )

                self._log_history(
                    job.job_id,
                    job.func_name,
                    JobStatus.FAILED,
                    error=f"Retry {job.retry_count}/{job.max_retries}: {error_msg}",
                    run_count=job.run_count,
                    retry_count=job.retry_count,
                    execution_time=execution_time,
                )
            else:
                job.status = JobStatus.FAILED
                if not self.quiet:
                    logger.error(
                        f"{job.func_name} failed after {job.max_retries} retries: {error_msg}"
                    )

                self._log_history(
                    job.job_id,
                    job.func_name,
                    JobStatus.FAILED,
                    error=f"Max retries: {error_msg}",
                    run_count=job.run_count,
                    retry_count=job.retry_count,
                    execution_time=execution_time,
                )

        finally:
            with self.lock:
                self._running_jobs.discard(job.job_id)
            self._save_state_async()

    def _save_state_async(self):
        """Save state asynchronously to avoid blocking."""
        try:
            self._save_executor.submit(self._save_state)
        except Exception as e:
            logger.error(f"Failed to queue state save: {e}")

    def _save_state(self):
        """Save state using storage backend."""
        try:
            with self.lock:
                jobs = [job.to_dict() for job in self._queue.get_all()]
                history = [h.to_dict() for h in self.history[-1000:]]

            self._storage.save_state(
                jobs=jobs,
                history=history,
                statistics=self.stats,
                job_counter=self._job_counter_value,
                scheduler_running=self.running,
            )

        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def _load_state(self):
        """Load state using storage backend."""
        try:
            state = self._storage.load_state()

            if state is None:
                return

            self._job_counter_value = state.get("_job_counter", 0)
            self._job_counter = itertools.count(self._job_counter_value)

            self.history = [
                JobHistory(**{k: v for k, v in h.items() if k != "timestamp_readable"})
                for h in state.get("history", [])
            ]

            with self.lock:
                self._cleanup_history()

            self.stats.update(state.get("statistics", {}))

            job_data = state.get("jobs", [])
            restored_count = 0

            # Clear queue before loading
            self._queue.clear()

            for jd in job_data:
                func_key = f"{jd['func_module']}.{jd['func_name']}"

                if func_key in self.job_registry:
                    # Use from_dict for consistency (handles args/kwargs properly)
                    job = Job.from_dict(jd)
                    # Restore function reference
                    job.func = self.job_registry[func_key]
                    self._queue.push(job)
                    restored_count += 1

            if restored_count > 0:
                if not self.quiet:
                    logger.info(f"Loaded state: {restored_count} jobs restored")

        except Exception as e:
            logger.error(f"Failed to load state: {e}")

    def _load_dead_letters(self):
        """Load dead letter queue using storage backend."""
        try:
            dead_letter_dicts = self._storage.load_dead_letters()

            self.dead_letters = [
                JobHistory(**{k: v for k, v in h.items() if k != "timestamp_readable"})
                for h in dead_letter_dicts
            ]

            if len(self.dead_letters) > self.max_dead_letters:
                self.dead_letters = self.dead_letters[-self.max_dead_letters :]

            if not self.quiet and self.dead_letters:
                logger.info(f"Loaded {len(self.dead_letters)} dead letter entries")

        except Exception as e:
            logger.error(f"Failed to load dead letters: {e}")

    def _save_dead_letters_async(self):
        """Save dead letters asynchronously."""
        try:
            self._save_executor.submit(self._save_dead_letters)
        except Exception as e:
            logger.error(f"Failed to queue dead letters save: {e}")

    def _save_dead_letters(self):
        """Save dead letter queue using storage backend."""
        try:
            with self.lock:
                dead_letters = [dl.to_dict() for dl in self.dead_letters]

            self._storage.save_dead_letters(dead_letters, self.max_dead_letters)

        except Exception as e:
            logger.error(f"Failed to save dead letters: {e}")

    def get_dead_letters(self, limit: int = 100) -> List[Dict]:
        """Get dead letter queue entries (failed jobs)."""
        with self.lock:
            return [dl.to_dict() for dl in self.dead_letters[-limit:][::-1]]

    def clear_dead_letters(self) -> int:
        """Clear all dead letter entries."""
        with self.lock:
            count = len(self.dead_letters)
            self.dead_letters = []
        self._save_dead_letters_async()
        return count

    # ==================== Task Management API ====================

    def create_job(
        self,
        func_name: str,
        func_module: str,
        schedule_type: str,
        schedule_config: Dict[str, Any],
        max_retries: int = 3,
        timeout: Optional[float] = None,
        timezone: Optional[str] = None,
        enabled: bool = True,
        args: tuple = (),
        kwargs: dict = None,
        group: str = "default",
    ) -> Optional[str]:
        """
        Create a new scheduled job via API.
        
        Args:
            func_name: Function name (must be registered)
            func_module: Function module (must be registered)
            schedule_type: Type of schedule (interval/daily/weekly/hourly/cron/once)
            schedule_config: Schedule configuration dictionary
            max_retries: Maximum retry attempts
            timeout: Maximum execution time in seconds
            timezone: Optional timezone string
            enabled: Whether the job is enabled
            args: Function arguments
            kwargs: Function keyword arguments
            group: Job group name for business isolation (default: "default")
        
        Returns:
            Job ID if successful, None otherwise
        """
        if kwargs is None:
            kwargs = {}

        func_key = f"{func_module}.{func_name}"
        if func_key not in self.job_registry:
            logger.error(f"Function not registered: {func_key}")
            return None

        func = self.job_registry[func_key]
        now = time.time()

        try:
            # Calculate next_run based on schedule_type
            if schedule_type == "interval":
                interval = schedule_config.get("interval", 60)
                unit = schedule_config.get("unit", "seconds")
                if unit == "minutes":
                    interval *= 60
                elif unit == "hours":
                    interval *= 3600
                elif unit == "days":
                    interval *= 86400
                next_run = now + interval

                job = Job(
                    job_id=self._next_job_id(),
                    func=func,
                    func_name=func_name,
                    func_module=func_module,
                    next_run=next_run,
                    interval=interval,
                    repeat=True,
                    max_retries=max_retries,
                    timeout=timeout,
                    paused=not enabled,
                    args=args,
                    kwargs=kwargs,
                    group=group,
                )

            elif schedule_type == "daily":
                time_str = schedule_config.get("time", "00:00")
                if timezone:
                    tz = ZoneInfo(timezone)
                    now_dt = datetime.now(tz)
                else:
                    now_dt = datetime.now()
                    tz = None

                hour, minute = map(int, time_str.split(":"))
                next_run_dt = now_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if next_run_dt <= now_dt:
                    next_run_dt += timedelta(days=1)

                job = Job(
                    job_id=self._next_job_id(),
                    func=func,
                    func_name=func_name,
                    func_module=func_module,
                    next_run=next_run_dt.timestamp(),
                    repeat=True,
                    max_retries=max_retries,
                    schedule_type="daily",
                    schedule_time=time_str,
                    timeout=timeout,
                    timezone=timezone,
                    paused=not enabled,
                    args=args,
                    kwargs=kwargs,
                    group=group,
                )

            elif schedule_type == "weekly":
                time_str = schedule_config.get("time", "00:00")
                days = schedule_config.get("days", [0])  # Default to Monday
                
                if timezone:
                    tz = ZoneInfo(timezone)
                    now_dt = datetime.now(tz)
                else:
                    now_dt = datetime.now()
                    tz = None

                hour, minute = map(int, time_str.split(":"))
                
                # Find next matching day
                next_run_dt = None
                for i in range(8):  # Check next 7 days
                    check_date = now_dt + timedelta(days=i)
                    if check_date.weekday() in days:
                        next_run_dt = check_date.replace(
                            hour=hour, minute=minute, second=0, microsecond=0
                        )
                        if next_run_dt > now_dt:
                            break

                if next_run_dt is None:
                    # If no match found, schedule for next week's first matching day
                    next_run_dt = (now_dt + timedelta(days=7)).replace(
                        hour=hour, minute=minute, second=0, microsecond=0
                    )
                    # Find first matching day
                    while next_run_dt.weekday() not in days:
                        next_run_dt += timedelta(days=1)

                job = Job(
                    job_id=self._next_job_id(),
                    func=func,
                    func_name=func_name,
                    func_module=func_module,
                    next_run=next_run_dt.timestamp(),
                    repeat=True,
                    max_retries=max_retries,
                    schedule_type="weekly",
                    schedule_time=time_str,
                    schedule_days=days,
                    timeout=timeout,
                    timezone=timezone,
                    paused=not enabled,
                    args=args,
                    kwargs=kwargs,
                    group=group,
                )

            elif schedule_type == "hourly":
                minute_str = schedule_config.get("time", "0")
                # Handle "HH:MM" format or just minute
                if ":" in minute_str:
                    _, minute = map(int, minute_str.split(":"))
                else:
                    minute = int(minute_str)

                if timezone:
                    tz = ZoneInfo(timezone)
                    now_dt = datetime.now(tz)
                else:
                    now_dt = datetime.now()
                    tz = None

                next_run_dt = now_dt.replace(minute=minute, second=0, microsecond=0)
                if next_run_dt <= now_dt:
                    next_run_dt += timedelta(hours=1)

                job = Job(
                    job_id=self._next_job_id(),
                    func=func,
                    func_name=func_name,
                    func_module=func_module,
                    next_run=next_run_dt.timestamp(),
                    repeat=True,
                    max_retries=max_retries,
                    schedule_type="hourly",
                    schedule_time=minute_str,
                    timeout=timeout,
                    timezone=timezone,
                    paused=not enabled,
                    args=args,
                    kwargs=kwargs,
                    group=group,
                )

            elif schedule_type == "cron":
                if not CRONITER_AVAILABLE:
                    logger.error("Cron scheduling requires croniter")
                    return None

                cron_expr = schedule_config.get("expression")
                if not cron_expr:
                    logger.error("Cron expression required")
                    return None

                if timezone:
                    tz = ZoneInfo(timezone)
                    now_dt = datetime.now(tz)
                else:
                    now_dt = datetime.now()
                    tz = None

                cron = croniter(cron_expr, now_dt)
                next_run_dt = cron.get_next(datetime)

                job = Job(
                    job_id=self._next_job_id(),
                    func=func,
                    func_name=func_name,
                    func_module=func_module,
                    next_run=next_run_dt.timestamp(),
                    repeat=True,
                    max_retries=max_retries,
                    schedule_type="cron",
                    cron_expression=cron_expr,
                    timeout=timeout,
                    timezone=timezone,
                    paused=not enabled,
                    args=args,
                    kwargs=kwargs,
                    group=group,
                )

            elif schedule_type == "once":
                # One-time job: delay in seconds
                delay = schedule_config.get("delay", 0)
                unit = schedule_config.get("unit", "seconds")
                
                if unit == "minutes":
                    delay *= 60
                elif unit == "hours":
                    delay *= 3600
                elif unit == "days":
                    delay *= 86400
                
                next_run = now + delay

                job = Job(
                    job_id=self._next_job_id(),
                    func=func,
                    func_name=func_name,
                    func_module=func_module,
                    next_run=next_run,
                    repeat=False,  # One-time job
                    max_retries=max_retries,
                    timeout=timeout,
                    paused=not enabled,
                    args=args,
                    kwargs=kwargs,
                    group=group,
                )

            else:
                logger.error(f"Unsupported schedule_type: {schedule_type}")
                return None

            self._add_job(job)
            if not self.quiet:
                logger.info(f"Created job via API: {func_name} ({job.job_id})")
            return job.job_id

        except Exception as e:
            logger.error(f"Failed to create job: {e}", exc_info=True)
            return None

    def update_job(
        self,
        job_id: str,
        schedule_config: Optional[Dict[str, Any]] = None,
        max_retries: Optional[int] = None,
        timeout: Optional[float] = None,
        timezone: Optional[str] = None,
        enabled: Optional[bool] = None,
    ) -> bool:
        """
        Update an existing job.
        
        Args:
            job_id: Job ID to update
            schedule_config: Updated schedule configuration
            max_retries: Updated max retries
            timeout: Updated timeout
            timezone: Updated timezone
            enabled: Whether job is enabled
        
        Returns:
            True if successful, False otherwise
        """
        with self.lock:
            # Find the job
            all_jobs = self._queue.get_all()
            job_to_update = None
            for job in all_jobs:
                if job.job_id == job_id:
                    job_to_update = job
                    break
            
            if job_to_update is None:
                return False
            
            updated = False

            if max_retries is not None:
                job_to_update.max_retries = max_retries
                updated = True

            if timeout is not None:
                job_to_update.timeout = timeout
                updated = True

            if timezone is not None:
                job_to_update.timezone = timezone
                updated = True

            if enabled is not None:
                job_to_update.paused = not enabled
                updated = True

            if schedule_config is not None:
                # Recalculate next_run based on schedule_config
                if job_to_update.schedule_type == "interval":
                    interval = schedule_config.get("interval", job_to_update.interval or 60)
                    unit = schedule_config.get("unit", "seconds")
                    if unit == "minutes":
                        interval *= 60
                    elif unit == "hours":
                        interval *= 3600
                    elif unit == "days":
                        interval *= 86400
                    job_to_update.interval = interval
                    job_to_update.next_run = time.time() + interval
                    updated = True
                elif job_to_update.schedule_type == "daily":
                    time_str = schedule_config.get("time", job_to_update.schedule_time or "00:00")
                    job_to_update.schedule_time = time_str
                    self._calculate_next_run(job_to_update)
                    updated = True
                elif job_to_update.schedule_type == "cron":
                    cron_expr = schedule_config.get("expression", job_to_update.cron_expression)
                    if cron_expr:
                        job_to_update.cron_expression = cron_expr
                        self._calculate_next_run(job_to_update)
                        updated = True

            if updated:
                # Update job in queue
                self._queue.update(job_to_update)
                # If using heapq backend, heapify after update
                if hasattr(self._queue, 'heapify'):
                    self._queue.heapify()
                self._save_state_async()
                if not self.quiet:
                    logger.info(f"Updated job: {job_to_update.func_name} ({job_id})")
                return True

            return False

    def enable_job(self, job_id: str) -> bool:
        """Enable a job (equivalent to resume, but clearer semantics for API)."""
        return self.resume_job(job_id)

    def disable_job(self, job_id: str) -> bool:
        """Disable a job (equivalent to pause, but clearer semantics for API)."""
        return self.pause_job(job_id)

    # ==================== Monitoring & Management ====================

    def get_jobs(self, group: Optional[str] = None) -> List[Dict]:
        """
        Get all scheduled jobs, optionally filtered by group.
        
        Args:
            group: Optional group name to filter jobs. If None, returns all jobs.
        
        Returns:
            List of job dictionaries
        """
        with self.lock:
            all_jobs = self._queue.get_all()
            
            # Filter by group if specified
            if group is not None:
                all_jobs = [job for job in all_jobs if job.group == group]
            
            return [
                {
                    "job_id": job.job_id,
                    "func_name": job.func_name,
                    "group": job.group,
                    "status": (
                        JobStatus.RUNNING.value
                        if job.job_id in self._running_jobs
                        else ("paused" if job.paused else job.status.value)
                    ),
                    "schedule": job.get_schedule_description(),
                    "next_run": datetime.fromtimestamp(job.next_run).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "next_run_in": max(0, job.next_run - time.time()),
                    "run_count": job.run_count,
                    "retry_count": job.retry_count,
                    "paused": job.paused,
                    "last_run": (
                        datetime.fromtimestamp(job.last_run).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if job.last_run
                        else None
                    ),
                }
                for job in sorted(all_jobs, key=lambda j: j.next_run)
            ]

    def get_history(
        self, func_name: Optional[str] = None, limit: int = 50
    ) -> List[Dict]:
        """Get job history."""
        with self.lock:
            history = (
                self.history
                if not func_name
                else [h for h in self.history if h.func_name == func_name]
            )
            return [h.to_dict() for h in history[-limit:]]

    def get_statistics(self) -> Dict:
        """Get statistics."""
        with self.lock:
            stats = self.stats.copy()

            if stats["start_time"]:
                stats["uptime_seconds"] = time.time() - stats["start_time"]
                stats["uptime_readable"] = str(
                    timedelta(seconds=int(stats["uptime_seconds"]))
                )

            job_stats = defaultdict(
                lambda: {"completed": 0, "failed": 0, "total_runs": 0}
            )

            for event in self.history:
                if event.status in ["completed", "failed"]:
                    job_stats[event.func_name]["total_runs"] += 1
                    job_stats[event.func_name][event.status] += 1

            stats["per_job"] = dict(job_stats)
            stats["active_jobs"] = self._queue.size()

            return stats

    def cancel_job(self, job_id: str) -> bool:
        """Cancel and remove a scheduled job by ID."""
        with self.lock:
            # Find job to get its name for logging
            all_jobs = self._queue.get_all()
            job_to_cancel = None
            for job in all_jobs:
                if job.job_id == job_id:
                    job_to_cancel = job
                    break
            
            if job_to_cancel:
                removed = self._queue.remove(job_id)
                if removed:
                    self._log_history(job_id, job_to_cancel.func_name, JobStatus.COMPLETED)
                    if not self.quiet:
                        logger.info(f"Cancelled job: {job_to_cancel.func_name} ({job_id})")
                    self._save_state_async()
                    return True
        return False

    def cancel_job_by_name(self, func_name: str) -> int:
        """Cancel all jobs with the given function name."""
        with self.lock:
            cancelled = 0
            all_jobs = self._queue.get_all()
            jobs_to_remove = []
            
            for job in all_jobs:
                if job.func_name == func_name:
                    jobs_to_remove.append(job.job_id)
                    self._log_history(job.job_id, job.func_name, JobStatus.COMPLETED)
                    cancelled += 1

            if cancelled > 0:
                for job_id in jobs_to_remove:
                    self._queue.remove(job_id)
                if not self.quiet:
                    logger.info(f"Cancelled {cancelled} job(s) with name: {func_name}")
                self._save_state_async()

            return cancelled

    def pause_job(self, job_id: str) -> bool:
        """Pause a job (it will remain in the queue but won't execute)."""
        with self.lock:
            all_jobs = self._queue.get_all()
            for job in all_jobs:
                if job.job_id == job_id:
                    job.paused = True
                    self._queue.update(job)
                    if hasattr(self._queue, 'heapify'):
                        self._queue.heapify()
                    if not self.quiet:
                        logger.info(f"Paused job: {job.func_name} ({job_id})")
                    self._save_state()
                    return True
        return False

    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job."""
        with self.lock:
            all_jobs = self._queue.get_all()
            for job in all_jobs:
                if job.job_id == job_id:
                    job.paused = False
                    self._queue.update(job)
                    if hasattr(self._queue, 'heapify'):
                        self._queue.heapify()
                    if not self.quiet:
                        logger.info(f"Resumed job: {job.func_name} ({job_id})")
                    self._save_state()
                    return True
        return False

    def run_job_now(self, job_id: str) -> bool:
        """Trigger immediate execution of a job (useful for debugging)."""
        with self.lock:
            all_jobs = self._queue.get_all()
            for job in all_jobs:
                if job.job_id == job_id:
                    if job.job_id in self._running_jobs:
                        logger.warning(f"Job {job_id} is already running")
                        return False
                    
                    # Restore function reference if needed (for Redis backend)
                    if job.func is None and job.func_name and job.func_module:
                        func_key = f"{job.func_module}.{job.func_name}"
                        if func_key in self.job_registry:
                            job.func = self.job_registry[func_key]
                        else:
                            logger.error(
                                f"Job {job_id} function {func_key} not registered, cannot run"
                            )
                            return False
                    
                    if not self.quiet:
                        logger.info(f"Manually triggered: {job.func_name} ({job_id})")
                    self._executor.submit(self._execute_job, job)
                    return True
        return False

    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get a specific job by ID."""
        with self.lock:
            all_jobs = self._queue.get_all()
            for job in all_jobs:
                if job.job_id == job_id:
                    return {
                        "job_id": job.job_id,
                        "func_name": job.func_name,
                        "group": job.group,
                        "status": (
                            JobStatus.RUNNING.value
                            if job.job_id in self._running_jobs
                            else ("paused" if job.paused else job.status.value)
                        ),
                        "schedule": job.get_schedule_description(),
                        "next_run": datetime.fromtimestamp(job.next_run).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "next_run_in": max(0, job.next_run - time.time()),
                        "run_count": job.run_count,
                        "retry_count": job.retry_count,
                        "paused": job.paused,
                        "timeout": job.timeout,
                        "last_run": (
                            datetime.fromtimestamp(job.last_run).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            if job.last_run
                            else None
                        ),
                    }
        return None

    def get_groups(self) -> List[str]:
        """
        Get all job group names.
        
        Returns:
            List of unique group names
        """
        with self.lock:
            all_jobs = self._queue.get_all()
            groups = sorted(set(job.group for job in all_jobs if job.group))
            return groups

    def get_jobs_by_group(self, group: str) -> List[Dict]:
        """
        Get all jobs in a specific group.
        
        Args:
            group: Group name
        
        Returns:
            List of job dictionaries
        """
        return self.get_jobs(group=group)

    def cancel_group(self, group: str) -> int:
        """
        Cancel all jobs in a specific group.
        
        Args:
            group: Group name
        
        Returns:
            Number of jobs cancelled
        """
        with self.lock:
            all_jobs = self._queue.get_all()
            jobs_to_cancel = [job for job in all_jobs if job.group == group]
            
            cancelled = 0
            for job in jobs_to_cancel:
                if self._queue.remove(job.job_id):
                    self._log_history(job.job_id, job.func_name, JobStatus.COMPLETED)
                    cancelled += 1
            
            if cancelled > 0:
                if not self.quiet:
                    logger.info(f"Cancelled {cancelled} job(s) in group: {group}")
                self._save_state_async()
            
            return cancelled

    def pause_group(self, group: str) -> int:
        """
        Pause all jobs in a specific group.
        
        Args:
            group: Group name
        
        Returns:
            Number of jobs paused
        """
        with self.lock:
            all_jobs = self._queue.get_all()
            jobs_to_pause = [job for job in all_jobs if job.group == group and not job.paused]
            
            paused = 0
            for job in jobs_to_pause:
                job.paused = True
                self._queue.update(job)
                if hasattr(self._queue, 'heapify'):
                    self._queue.heapify()
                paused += 1
            
            if paused > 0:
                if not self.quiet:
                    logger.info(f"Paused {paused} job(s) in group: {group}")
                self._save_state()
            
            return paused

    def resume_group(self, group: str) -> int:
        """
        Resume all paused jobs in a specific group.
        
        Args:
            group: Group name
        
        Returns:
            Number of jobs resumed
        """
        with self.lock:
            all_jobs = self._queue.get_all()
            jobs_to_resume = [job for job in all_jobs if job.group == group and job.paused]
            
            resumed = 0
            for job in jobs_to_resume:
                job.paused = False
                self._queue.update(job)
                if hasattr(self._queue, 'heapify'):
                    self._queue.heapify()
                resumed += 1
            
            if resumed > 0:
                if not self.quiet:
                    logger.info(f"Resumed {resumed} job(s) in group: {group}")
                self._save_state()
            
            return resumed

    def print_status(self):
        """Print simple status."""
        status = "RUNNING" if self.running else "STOPPED"
        stats = self.get_statistics()
        jobs = self.get_jobs()

        print(f"\nFastScheduler [{status}]")
        if stats.get("uptime_readable"):
            print(f"Uptime: {stats['uptime_readable']}")
        print(
            f"Jobs: {len(jobs)} | Runs: {stats['total_runs']} | Failures: {stats['total_failures']}"
        )

        if jobs:
            print("\nActive jobs:")
            for job in jobs[:5]:
                next_in = job["next_run_in"]
                if next_in > 86400:
                    next_in_str = f"{int(next_in/86400)}d"
                elif next_in > 3600:
                    next_in_str = f"{int(next_in/3600)}h"
                elif next_in > 60:
                    next_in_str = f"{int(next_in/60)}m"
                elif next_in > 0:
                    next_in_str = f"{int(next_in)}s"
                else:
                    next_in_str = "now"

                status_char = {
                    "scheduled": " ",
                    "running": ">",
                    "completed": "+",
                    "failed": "x",
                }.get(job["status"], " ")

                print(
                    f"  [{status_char}] {job['func_name']:<20} {job['schedule']:<20} next: {next_in_str}"
                )

            if len(jobs) > 5:
                print(f"      ... and {len(jobs) - 5} more")
        print()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop(wait=True)
