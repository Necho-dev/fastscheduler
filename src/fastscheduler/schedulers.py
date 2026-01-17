"""Scheduler classes for different scheduling patterns."""

import re
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable, List, Optional
from zoneinfo import ZoneInfo

from .models import Job

try:
    from croniter import croniter
    CRONITER_AVAILABLE = True
except ImportError:
    CRONITER_AVAILABLE = False
    croniter = None  # type: ignore

if TYPE_CHECKING:
    from .main import FastScheduler


class IntervalScheduler:
    def __init__(self, scheduler: "FastScheduler", interval: float):
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
        """Set maximum execution time for this job (kills job if exceeded)."""
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
    def __init__(self, scheduler: "FastScheduler"):
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
        """Set maximum execution time for this job (kills job if exceeded)."""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule."""
        self._timezone = timezone
        return self


class DailyAtScheduler:
    def __init__(
        self,
        scheduler: "FastScheduler",
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
        """Set maximum execution time for this job (kills job if exceeded)."""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule."""
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        self.scheduler._register_function(func)

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
    def __init__(self, scheduler: "FastScheduler"):
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
        """Set maximum execution time for this job (kills job if exceeded)."""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule."""
        self._timezone = timezone
        return self


class WeeklyAtScheduler:
    def __init__(
        self,
        scheduler: "FastScheduler",
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
        """Set maximum execution time for this job (kills job if exceeded)."""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule."""
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        self.scheduler._register_function(func)

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
    def __init__(self, scheduler: "FastScheduler"):
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
        """Set maximum execution time for this job (kills job if exceeded)."""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule."""
        self._timezone = timezone
        return self


class HourlyAtScheduler:
    def __init__(
        self,
        scheduler: "FastScheduler",
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
        """Set maximum execution time for this job (kills job if exceeded)."""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for this schedule."""
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        self.scheduler._register_function(func)

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
    def __init__(self, scheduler: "FastScheduler", delay: float):
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
        """Set maximum execution time for this job (kills job if exceeded)."""
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

    def __init__(self, scheduler: "FastScheduler", expression: str):
        self.scheduler = scheduler
        self.expression = expression
        self._max_retries = 3
        self._timeout: Optional[float] = None
        self._timezone: Optional[str] = None

        if not CRONITER_AVAILABLE:
            raise ImportError(
                "Cron scheduling requires croniter. "
                "Install with: pip install fastscheduler[cron]"
            )

        try:
            croniter(expression)
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid cron expression '{expression}': {e}")

    def retries(self, max_retries: int):
        """Set maximum retry attempts on failure."""
        self._max_retries = max_retries
        return self

    def timeout(self, seconds: float):
        """Set maximum execution time for this job (kills job if exceeded)."""
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """Set timezone for cron schedule evaluation."""
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        self.scheduler._register_function(func)

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
        """Alternative to decorator syntax for scheduling with arguments."""
        self.scheduler._register_function(func)

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
