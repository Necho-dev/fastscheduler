import asyncio
import heapq
import itertools
import json
import logging
import re
import threading
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Union
from zoneinfo import ZoneInfo

# Optional croniter import for cron expression support
try:
    from croniter import croniter

    CRONITER_AVAILABLE = True
except ImportError:
    CRONITER_AVAILABLE = False
    croniter = None  # type: ignore

# Set up logger without configuring global logging (let the application configure handlers)
logger = logging.getLogger("fastscheduler")
logger.addHandler(logging.NullHandler())


class JobStatus(Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    MISSED = "missed"


@dataclass(order=True)
class Job:
    next_run: float = field(compare=True)
    func: Optional[Callable] = field(default=None, compare=False, repr=False)
    interval: Optional[float] = field(default=None, compare=False)
    job_id: str = field(default="", compare=False)
    func_name: str = field(default="", compare=False)
    func_module: str = field(default="", compare=False)
    args: tuple = field(default_factory=tuple, compare=False, repr=False)
    kwargs: dict = field(default_factory=dict, compare=False, repr=False)
    repeat: bool = field(default=False, compare=False)
    status: JobStatus = field(default=JobStatus.SCHEDULED, compare=False)
    created_at: float = field(default_factory=time.time, compare=False)
    last_run: Optional[float] = field(default=None, compare=False)
    run_count: int = field(default=0, compare=False)
    max_retries: int = field(default=3, compare=False)
    retry_count: int = field(default=0, compare=False)
    catch_up: bool = field(default=True, compare=False)
    schedule_type: str = field(default="interval", compare=False)
    schedule_time: Optional[str] = field(default=None, compare=False)
    schedule_days: Optional[List[int]] = field(default=None, compare=False)
    timeout: Optional[float] = field(default=None, compare=False)  # Timeout in seconds
    paused: bool = field(default=False, compare=False)
    timezone: Optional[str] = field(
        default=None, compare=False
    )  # Timezone for time-based schedules
    cron_expression: Optional[str] = field(
        default=None, compare=False
    )  # Cron expression for cron schedules

    def to_dict(self) -> Dict:
        """Serialize job for persistence"""
        return {
            "job_id": self.job_id,
            "func_name": self.func_name,
            "func_module": self.func_module,
            "next_run": self.next_run,
            "interval": self.interval,
            "repeat": self.repeat,
            "status": self.status.value,
            "created_at": self.created_at,
            "last_run": self.last_run,
            "run_count": self.run_count,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "catch_up": self.catch_up,
            "schedule_type": self.schedule_type,
            "schedule_time": self.schedule_time,
            "schedule_days": self.schedule_days,
            "timeout": self.timeout,
            "paused": self.paused,
            "timezone": self.timezone,
            "cron_expression": self.cron_expression,
        }

    def get_schedule_description(self) -> str:
        """Get human-readable schedule description"""
        tz_suffix = f" ({self.timezone})" if self.timezone else ""

        if self.schedule_type == "cron" and self.cron_expression:
            return f"Cron: {self.cron_expression}{tz_suffix}"
        elif self.schedule_type == "daily" and self.schedule_time:
            return f"Daily at {self.schedule_time}{tz_suffix}"
        elif (
            self.schedule_type == "weekly" and self.schedule_time and self.schedule_days
        ):
            days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            day_names = [days[d] for d in self.schedule_days]
            return f"Every {', '.join(day_names)} at {self.schedule_time}{tz_suffix}"
        elif self.schedule_type == "hourly" and self.schedule_time:
            return f"Hourly at {self.schedule_time}{tz_suffix}"
        elif self.schedule_type == "interval" and self.interval:
            if self.interval < 60:
                return f"Every {int(self.interval)} seconds"
            elif self.interval < 3600:
                return f"Every {int(self.interval/60)} minutes"
            elif self.interval < 86400:
                return f"Every {int(self.interval/3600)} hours"
            else:
                return f"Every {int(self.interval/86400)} days"
        return "One-time job"


@dataclass
class JobHistory:
    job_id: str
    func_name: str
    status: str
    timestamp: float
    error: Optional[str] = None
    run_count: int = 0
    retry_count: int = 0
    execution_time: Optional[float] = None

    def to_dict(self):
        return {
            **asdict(self),
            "timestamp_readable": datetime.fromtimestamp(self.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }


class FastScheduler:
    """
    FastScheduler - Simple, powerful, persistent task scheduler with async support

    Args:
        state_file: Path to the JSON file for persisting scheduler state
        auto_start: If True, start the scheduler immediately
        quiet: If True, suppress most log messages
        max_history: Maximum number of history entries to keep (default: 10000)
        max_workers: Maximum number of worker threads for job execution (default: 10)
        history_retention_days: Maximum age of history entries in days (default: 7)
    """

    def __init__(
        self,
        state_file: str = "fastscheduler_state.json",
        auto_start: bool = False,
        quiet: bool = False,
        max_history: int = 10000,
        max_workers: int = 10,
        history_retention_days: int = 7,
    ):
        self.state_file = Path(state_file)
        self.jobs: List[Job] = []
        self.job_registry: Dict[str, Callable] = {}
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.RLock()  # Use RLock for re-entrant locking
        self._job_counter: Iterator[int] = itertools.count()
        self._job_counter_value = 0  # Track last value for persistence
        self.history: List[JobHistory] = []
        self.max_history = max_history
        self.max_workers = max_workers
        self.history_retention_days = history_retention_days
        self.quiet = quiet  # Quiet mode for less verbose output
        self._running_jobs: set = set()  # Track currently executing jobs
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="FastScheduler-Worker"
        )
        self._save_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="FastScheduler-Saver"
        )

        # Statistics
        self.stats = {
            "total_runs": 0,
            "total_failures": 0,
            "total_retries": 0,
            "start_time": None,
        }

        # Load previous state
        self._load_state()

        if auto_start:
            self.start()

    # ==================== User-Friendly Scheduling API ====================

    def every(self, interval: Union[int, float]) -> "IntervalScheduler":
        """Schedule a task to run every X seconds/minutes/hours/days"""
        return IntervalScheduler(self, interval)

    @property
    def daily(self) -> "DailyScheduler":
        """Schedule a task to run daily at a specific time"""
        return DailyScheduler(self)

    @property
    def weekly(self) -> "WeeklyScheduler":
        """Schedule a task to run weekly on specific days"""
        return WeeklyScheduler(self)

    @property
    def hourly(self) -> "HourlyScheduler":
        """Schedule a task to run hourly at a specific minute"""
        return HourlyScheduler(self)

    def cron(self, expression: str) -> "CronScheduler":
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

    def once(self, delay: Union[int, float]) -> "OnceScheduler":
        """Schedule a one-time task"""
        scheduler = OnceScheduler(self, delay)
        scheduler._decorator_mode = True
        return scheduler

    def at(self, target_time: Union[datetime, str]) -> "OnceScheduler":
        """Schedule a task at a specific datetime"""
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
        """Register a function for persistence"""
        self.job_registry[f"{func.__module__}.{func.__name__}"] = func

    def _next_job_id(self) -> str:
        """Generate next job ID (thread-safe)"""
        self._job_counter_value = next(self._job_counter)
        return f"job_{self._job_counter_value}"

    def _add_job(self, job: Job):
        """Add job to the priority queue"""
        with self.lock:
            if any(j.job_id == job.job_id for j in self.jobs):
                logger.warning(f"Job {job.job_id} already exists, skipping")
                return

            heapq.heappush(self.jobs, job)
            self._log_history(job.job_id, job.func_name, JobStatus.SCHEDULED)

            schedule_desc = job.get_schedule_description()
            if not self.quiet:
                logger.info(f"Scheduled: {job.func_name} - {schedule_desc}")

        # Save state asynchronously
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
        """Log job events to history"""
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
            # Apply both count and time-based limits
            self._cleanup_history()

    def _cleanup_history(self):
        """Clean up old history entries based on count and age limits.

        Must be called with self.lock held.
        """
        # Remove entries older than retention period
        if self.history_retention_days > 0:
            cutoff_time = time.time() - (self.history_retention_days * 86400)
            self.history = [h for h in self.history if h.timestamp >= cutoff_time]

        # Also enforce max count limit
        if len(self.history) > self.max_history:
            self.history = self.history[-self.max_history :]

    def start(self):
        """Start the scheduler"""
        if self.running:
            logger.warning("Scheduler already running")
            return

        self.running = True
        self.stats["start_time"] = time.time()

        self._handle_missed_jobs()

        # Start in daemon thread so it doesn't block
        self.thread = threading.Thread(
            target=self._run,
            daemon=True,  # Daemon thread won't block program exit
            name="FastScheduler-Main",
        )
        self.thread.start()

        if not self.quiet:
            logger.info("FastScheduler started")
        self._save_state_async()

    def stop(self, wait: bool = True, timeout: int = 30):
        """Stop the scheduler gracefully"""
        if not self.running:
            return

        logger.info("Stopping scheduler...")
        self.running = False

        if wait and self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)

        # Shutdown executors
        if wait:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._save_executor.shutdown(wait=True)
        else:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._save_executor.shutdown(wait=False)

        self._save_state()
        if not self.quiet:
            logger.info("FastScheduler stopped")

    def _handle_missed_jobs(self):
        """Handle jobs that should have run while scheduler was stopped"""
        now = time.time()

        with self.lock:
            for job in self.jobs:
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
        """Calculate next run time for time-based schedules"""
        # Get timezone-aware datetime if timezone is specified
        if job.timezone:
            tz = ZoneInfo(job.timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()
            tz = None

        if job.schedule_type == "cron" and job.cron_expression and CRONITER_AVAILABLE:
            # Use croniter for cron expressions
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
        """Main scheduler loop - runs in background thread"""
        if not self.quiet:
            logger.info("Scheduler main loop started")

        while self.running:
            try:
                now = time.time()
                jobs_to_run = []

                # Collect jobs to run (minimize lock time)
                with self.lock:
                    while self.jobs and self.jobs[0].next_run <= now:
                        job = heapq.heappop(self.jobs)

                        # Skip paused jobs but keep them in queue
                        if job.paused:
                            # Reschedule to check again later
                            job.next_run = time.time() + 1.0  # Check again in 1 second
                            heapq.heappush(self.jobs, job)
                            continue

                        jobs_to_run.append(job)

                        # Reschedule if recurring
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
                            heapq.heappush(self.jobs, job)

                # Execute jobs outside of lock
                for job in jobs_to_run:
                    self._executor.submit(self._execute_job, job)

                # Sleep to avoid busy-waiting
                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in main loop: {e}\n{traceback.format_exc()}")
                time.sleep(1)  # Sleep on error to prevent tight loop

        if not self.quiet:
            logger.info("Scheduler main loop stopped")

    def _execute_job(self, job: Job):
        """Execute a job with retries"""
        if job.func is None:
            logger.error(f"Job {job.func_name} has no function, skipping")
            return

        # Mark job as running
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
            # Check if function is async
            if asyncio.iscoroutinefunction(job.func):
                # Run async function with optional timeout
                if job.timeout:
                    asyncio.run(
                        asyncio.wait_for(
                            job.func(*job.args, **job.kwargs), timeout=job.timeout
                        )
                    )
                else:
                    asyncio.run(job.func(*job.args, **job.kwargs))
            else:
                # Run sync function with optional timeout
                if job.timeout:
                    from concurrent.futures import ThreadPoolExecutor
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

            # For recurring jobs, status should be SCHEDULED (already rescheduled)
            # For one-time jobs, status should be COMPLETED
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
                    heapq.heappush(self.jobs, job)
                    self.stats["total_retries"] += 1

                if not self.quiet:
                    logger.warning(
                        f"{job.func_name} failed, retrying in {retry_delay}s ({job.retry_count}/{job.max_retries})"
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
            # Mark job as no longer running
            with self.lock:
                self._running_jobs.discard(job.job_id)
            self._save_state_async()

    def _save_state_async(self):
        """Save state asynchronously to avoid blocking"""
        try:
            self._save_executor.submit(self._save_state)
        except Exception as e:
            logger.error(f"Failed to queue state save: {e}")

    def _save_state(self):
        """Save state to disk"""
        try:
            state = {
                "version": "1.0",
                "metadata": {
                    "last_save": time.time(),
                    "last_save_readable": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "scheduler_running": self.running,
                },
                "jobs": [job.to_dict() for job in self.jobs],
                "history": [h.to_dict() for h in self.history[-1000:]],
                "statistics": self.stats,
                "_job_counter": self._job_counter_value,
            }

            temp_file = self.state_file.with_suffix(".tmp")
            with open(temp_file, "w") as f:
                json.dump(state, f, indent=2)
            temp_file.replace(self.state_file)

        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def _load_state(self):
        """Load state from disk"""
        if not self.state_file.exists():
            if not self.quiet:
                logger.info("No previous state found, starting fresh")
            return

        try:
            with open(self.state_file, "r") as f:
                state = json.load(f)

            self._job_counter_value = state.get("_job_counter", 0)
            self._job_counter = itertools.count(self._job_counter_value)

            self.history = [
                JobHistory(**{k: v for k, v in h.items() if k != "timestamp_readable"})
                for h in state.get("history", [])
            ]

            # Clean up old history entries on load
            with self.lock:
                self._cleanup_history()

            self.stats.update(state.get("statistics", {}))

            job_data = state.get("jobs", [])
            restored_count = 0

            for jd in job_data:
                func_key = f"{jd['func_module']}.{jd['func_name']}"

                if func_key in self.job_registry:
                    job = Job(
                        job_id=jd["job_id"],
                        func=self.job_registry[func_key],
                        func_name=jd["func_name"],
                        func_module=jd["func_module"],
                        next_run=jd["next_run"],
                        interval=jd["interval"],
                        repeat=jd["repeat"],
                        status=JobStatus(jd["status"]),
                        created_at=jd["created_at"],
                        last_run=jd.get("last_run"),
                        run_count=jd.get("run_count", 0),
                        max_retries=jd.get("max_retries", 3),
                        retry_count=jd.get("retry_count", 0),
                        catch_up=jd.get("catch_up", True),
                        schedule_type=jd.get("schedule_type", "interval"),
                        schedule_time=jd.get("schedule_time"),
                        schedule_days=jd.get("schedule_days"),
                        timeout=jd.get("timeout"),
                        paused=jd.get("paused", False),
                        timezone=jd.get("timezone"),
                        cron_expression=jd.get("cron_expression"),
                    )
                    heapq.heappush(self.jobs, job)
                    restored_count += 1

            if restored_count > 0:
                if not self.quiet:
                    logger.info(f"Loaded state: {restored_count} jobs restored")

        except Exception as e:
            logger.error(f"Failed to load state: {e}")

    # ==================== Monitoring & Management ====================

    def get_jobs(self) -> List[Dict]:
        """Get all scheduled jobs"""
        with self.lock:
            return [
                {
                    "job_id": job.job_id,
                    "func_name": job.func_name,
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
                for job in sorted(self.jobs, key=lambda j: j.next_run)
            ]

    def get_history(
        self, func_name: Optional[str] = None, limit: int = 50
    ) -> List[Dict]:
        """Get job history"""
        with self.lock:
            history = (
                self.history
                if not func_name
                else [h for h in self.history if h.func_name == func_name]
            )
            return [h.to_dict() for h in history[-limit:]]

    def get_statistics(self) -> Dict:
        """Get statistics"""
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
            stats["active_jobs"] = len(self.jobs)

            return stats

    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel and remove a scheduled job by ID.

        Args:
            job_id: The job ID to cancel (e.g., "job_0")

        Returns:
            True if job was found and cancelled, False otherwise
        """
        with self.lock:
            for i, job in enumerate(self.jobs):
                if job.job_id == job_id:
                    self.jobs.pop(i)
                    heapq.heapify(self.jobs)
                    self._log_history(job_id, job.func_name, JobStatus.COMPLETED)
                    if not self.quiet:
                        logger.info(f"Cancelled job: {job.func_name} ({job_id})")
                    self._save_state_async()
                    return True
        return False

    def cancel_job_by_name(self, func_name: str) -> int:
        """
        Cancel all jobs with the given function name.

        Args:
            func_name: The function name to cancel

        Returns:
            Number of jobs cancelled
        """
        with self.lock:
            cancelled = 0
            jobs_to_keep = []
            for job in self.jobs:
                if job.func_name == func_name:
                    self._log_history(job.job_id, job.func_name, JobStatus.COMPLETED)
                    cancelled += 1
                else:
                    jobs_to_keep.append(job)

            if cancelled > 0:
                self.jobs = jobs_to_keep
                heapq.heapify(self.jobs)
                if not self.quiet:
                    logger.info(f"Cancelled {cancelled} job(s) with name: {func_name}")
                self._save_state_async()

            return cancelled

    def pause_job(self, job_id: str) -> bool:
        """
        Pause a job (it will remain in the queue but won't execute).

        Args:
            job_id: The job ID to pause

        Returns:
            True if job was found and paused, False otherwise
        """
        with self.lock:
            for job in self.jobs:
                if job.job_id == job_id:
                    job.paused = True
                    if not self.quiet:
                        logger.info(f"Paused job: {job.func_name} ({job_id})")
                    self._save_state_async()
                    return True
        return False

    def resume_job(self, job_id: str) -> bool:
        """
        Resume a paused job.

        Args:
            job_id: The job ID to resume

        Returns:
            True if job was found and resumed, False otherwise
        """
        with self.lock:
            for job in self.jobs:
                if job.job_id == job_id:
                    job.paused = False
                    if not self.quiet:
                        logger.info(f"Resumed job: {job.func_name} ({job_id})")
                    self._save_state_async()
                    return True
        return False

    def get_job(self, job_id: str) -> Optional[Dict]:
        """
        Get a specific job by ID.

        Args:
            job_id: The job ID to retrieve

        Returns:
            Job info dict or None if not found
        """
        with self.lock:
            for job in self.jobs:
                if job.job_id == job_id:
                    return {
                        "job_id": job.job_id,
                        "func_name": job.func_name,
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

    def print_status(self):
        """Print simple status"""
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


# ==================== Schedulers ====================


class IntervalScheduler:
    def __init__(self, scheduler: FastScheduler, interval: float):
        self.scheduler = scheduler
        self.interval = interval
        self._max_retries = 3
        self._catch_up = True
        self._timeout: Optional[float] = None

    @property
    def seconds(self):
        return self

    @property
    def minutes(self):
        self.interval *= 60
        return self

    @property
    def hours(self):
        self.interval *= 3600
        return self

    @property
    def days(self):
        self.interval *= 86400
        return self

    def retries(self, max_retries: int):
        self._max_retries = max_retries
        return self

    def no_catch_up(self):
        self._catch_up = False
        return self

    def timeout(self, seconds: float):
        """Set maximum execution time for this job (kills job if exceeded)"""
        self._timeout = seconds
        return self

    def do(self, func: Callable, *args, **kwargs):
        self.scheduler._register_function(func)

        job = Job(
            next_run=time.time() + self.interval,
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            interval=self.interval,
            job_id=self.scheduler._next_job_id(),
            args=args,
            kwargs=kwargs,
            repeat=True,
            max_retries=self._max_retries,
            catch_up=self._catch_up,
            schedule_type="interval",
            timeout=self._timeout,
        )
        self.scheduler._add_job(job)
        return func

    def __call__(self, func: Callable):
        return self.do(func)


class DailyScheduler:
    def __init__(self, scheduler: FastScheduler):
        self.scheduler = scheduler
        self._max_retries = 3
        self._timeout: Optional[float] = None
        self._timezone: Optional[str] = None

    def at(self, time_str: str, tz: Optional[str] = None):
        """
        Schedule daily at a specific time.

        Args:
            time_str: Time in HH:MM format (24-hour)
            tz: Optional timezone (e.g., "America/New_York", "Europe/London")
        """
        timezone = tz or self._timezone
        return DailyAtScheduler(
            self.scheduler, time_str, self._max_retries, self._timeout, timezone
        )

    def retries(self, max_retries: int):
        self._max_retries = max_retries
        return self

    def timeout(self, seconds: float):
        """Set maximum execution time for this job (kills job if exceeded)"""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule"""
        self._timezone = timezone
        return self


class DailyAtScheduler:
    def __init__(
        self,
        scheduler: FastScheduler,
        time_str: str,
        max_retries: int,
        timeout: Optional[float] = None,
        timezone: Optional[str] = None,
    ):
        self.scheduler = scheduler
        self.time_str = time_str
        self._max_retries = max_retries
        self._timeout = timeout
        self._timezone = timezone

        if not re.match(r"^\d{2}:\d{2}$", time_str):
            raise ValueError("Time must be in HH:MM format (24-hour)")

    def timeout(self, seconds: float):
        """Set maximum execution time for this job (kills job if exceeded)"""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule"""
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        self.scheduler._register_function(func)

        # Handle timezone-aware scheduling
        if self._timezone:
            tz = ZoneInfo(self._timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()

        hour, minute = map(int, self.time_str.split(":"))
        next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="daily",
            schedule_time=self.time_str,
            timeout=self._timeout,
            timezone=self._timezone,
        )
        self.scheduler._add_job(job)
        return func


class WeeklyScheduler:
    def __init__(self, scheduler: FastScheduler):
        self.scheduler = scheduler
        self._days: List[int] = []
        self._max_retries = 3
        self._timeout: Optional[float] = None
        self._timezone: Optional[str] = None

    @property
    def monday(self):
        self._days = [0]
        return self

    @property
    def tuesday(self):
        self._days = [1]
        return self

    @property
    def wednesday(self):
        self._days = [2]
        return self

    @property
    def thursday(self):
        self._days = [3]
        return self

    @property
    def friday(self):
        self._days = [4]
        return self

    @property
    def saturday(self):
        self._days = [5]
        return self

    @property
    def sunday(self):
        self._days = [6]
        return self

    @property
    def weekdays(self):
        self._days = [0, 1, 2, 3, 4]
        return self

    @property
    def weekends(self):
        self._days = [5, 6]
        return self

    def on(self, days: List[int]):
        self._days = days
        return self

    def at(self, time_str: str, tz: Optional[str] = None):
        """
        Schedule weekly at a specific time.

        Args:
            time_str: Time in HH:MM format (24-hour)
            tz: Optional timezone (e.g., "America/New_York", "Europe/London")
        """
        if not self._days:
            raise ValueError("Must specify days before time")
        timezone = tz or self._timezone
        return WeeklyAtScheduler(
            self.scheduler,
            self._days,
            time_str,
            self._max_retries,
            self._timeout,
            timezone,
        )

    def retries(self, max_retries: int):
        self._max_retries = max_retries
        return self

    def timeout(self, seconds: float):
        """Set maximum execution time for this job (kills job if exceeded)"""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule"""
        self._timezone = timezone
        return self


class WeeklyAtScheduler:
    def __init__(
        self,
        scheduler: FastScheduler,
        days: List[int],
        time_str: str,
        max_retries: int,
        timeout: Optional[float] = None,
        timezone: Optional[str] = None,
    ):
        self.scheduler = scheduler
        self.days = days
        self.time_str = time_str
        self._max_retries = max_retries
        self._timeout = timeout
        self._timezone = timezone

        if not re.match(r"^\d{2}:\d{2}$", time_str):
            raise ValueError("Time must be in HH:MM format")

    def timeout(self, seconds: float):
        """Set maximum execution time for this job (kills job if exceeded)"""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule"""
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        self.scheduler._register_function(func)

        # Handle timezone-aware scheduling
        if self._timezone:
            tz = ZoneInfo(self._timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()

        hour, minute = map(int, self.time_str.split(":"))

        next_run = None
        for i in range(8):
            check_date = now + timedelta(days=i)
            if check_date.weekday() in self.days:
                candidate = check_date.replace(
                    hour=hour, minute=minute, second=0, microsecond=0
                )
                if candidate > now:
                    next_run = candidate
                    break

        if not next_run:
            next_run = now + timedelta(days=7)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="weekly",
            schedule_time=self.time_str,
            schedule_days=self.days,
            timeout=self._timeout,
            timezone=self._timezone,
        )
        self.scheduler._add_job(job)
        return func


class HourlyScheduler:
    def __init__(self, scheduler: FastScheduler):
        self.scheduler = scheduler
        self._max_retries = 3
        self._timeout: Optional[float] = None
        self._timezone: Optional[str] = None

    def at(self, minute_str: str, tz: Optional[str] = None):
        """
        Schedule hourly at a specific minute.

        Args:
            minute_str: Minute in :MM format
            tz: Optional timezone (e.g., "America/New_York", "Europe/London")
        """
        timezone = tz or self._timezone
        return HourlyAtScheduler(
            self.scheduler, minute_str, self._max_retries, self._timeout, timezone
        )

    def retries(self, max_retries: int):
        self._max_retries = max_retries
        return self

    def timeout(self, seconds: float):
        """Set maximum execution time for this job (kills job if exceeded)"""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule"""
        self._timezone = timezone
        return self


class HourlyAtScheduler:
    def __init__(
        self,
        scheduler: FastScheduler,
        minute_str: str,
        max_retries: int,
        timeout: Optional[float] = None,
        timezone: Optional[str] = None,
    ):
        self.scheduler = scheduler
        self.minute_str = minute_str
        self._max_retries = max_retries
        self._timeout = timeout
        self._timezone = timezone

        if not re.match(r"^:\d{2}$", minute_str):
            raise ValueError("Minute must be in :MM format")

    def timeout(self, seconds: float):
        """Set maximum execution time for this job (kills job if exceeded)"""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule"""
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        self.scheduler._register_function(func)

        # Handle timezone-aware scheduling
        if self._timezone:
            tz = ZoneInfo(self._timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()

        minute = int(self.minute_str.strip(":"))
        next_run = now.replace(minute=minute, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(hours=1)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            interval=3600,
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="hourly",
            schedule_time=self.minute_str,
            timeout=self._timeout,
            timezone=self._timezone,
        )
        self.scheduler._add_job(job)
        return func


class OnceScheduler:
    def __init__(self, scheduler: FastScheduler, delay: float):
        self.scheduler = scheduler
        self.delay = delay
        self._decorator_mode = False
        self._max_retries = 3
        self._timeout: Optional[float] = None

    @property
    def seconds(self):
        return self

    @property
    def minutes(self):
        self.delay *= 60
        return self

    @property
    def hours(self):
        self.delay *= 3600
        return self

    def retries(self, max_retries: int):
        self._max_retries = max_retries
        return self

    def timeout(self, seconds: float):
        """Set maximum execution time for this job (kills job if exceeded)"""
        self._timeout = seconds
        return self

    def do(self, func: Callable, *args, **kwargs):
        self.scheduler._register_function(func)

        job = Job(
            next_run=time.time() + self.delay,
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            args=args,
            kwargs=kwargs,
            repeat=False,
            max_retries=self._max_retries,
            timeout=self._timeout,
        )
        self.scheduler._add_job(job)

        if self._decorator_mode:
            return func
        return job

    def __call__(self, func: Callable):
        return self.do(func)


class CronScheduler:
    """
    Scheduler for cron expressions.

    Requires croniter: pip install fastscheduler[cron]

    Usage:
        @scheduler.cron("0 9 * * MON-FRI")  # 9 AM on weekdays
        def market_open():
            ...

        @scheduler.cron("*/5 * * * *")  # Every 5 minutes
        def frequent_task():
            ...

        @scheduler.cron("0 9 * * MON-FRI", tz="America/New_York")
        def nyc_market_open():
            ...
    """

    def __init__(self, scheduler: FastScheduler, expression: str):
        self.scheduler = scheduler
        self.expression = expression
        self._max_retries = 3
        self._timeout: Optional[float] = None
        self._timezone: Optional[str] = None

        # Validate cron expression
        if not CRONITER_AVAILABLE:
            raise ImportError(
                "Cron scheduling requires croniter. "
                "Install with: pip install fastscheduler[cron]"
            )

        # Validate the expression by attempting to create a croniter
        try:
            croniter(expression)
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid cron expression '{expression}': {e}")

    def retries(self, max_retries: int):
        """Set maximum retry attempts on failure"""
        self._max_retries = max_retries
        return self

    def timeout(self, seconds: float):
        """Set maximum execution time for this job (kills job if exceeded)"""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for cron schedule evaluation"""
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        self.scheduler._register_function(func)

        # Calculate next run time using croniter
        if self._timezone:
            tz = ZoneInfo(self._timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()

        cron = croniter(self.expression, now)
        next_run = cron.get_next(datetime)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="cron",
            cron_expression=self.expression,
            timeout=self._timeout,
            timezone=self._timezone,
        )
        self.scheduler._add_job(job)
        return func

    def do(self, func: Callable, *args, **kwargs):
        """Alternative to decorator syntax for scheduling with arguments"""
        self.scheduler._register_function(func)

        # Calculate next run time using croniter
        if self._timezone:
            tz = ZoneInfo(self._timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()

        cron = croniter(self.expression, now)
        next_run = cron.get_next(datetime)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            args=args,
            kwargs=kwargs,
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="cron",
            cron_expression=self.expression,
            timeout=self._timeout,
            timezone=self._timezone,
        )
        self.scheduler._add_job(job)
        return func
