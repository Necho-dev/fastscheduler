# FastScheduler

Simple, lightweight task scheduler for Python with async support, timezone handling, cron expressions, and a beautiful real-time dashboard.

If this saves you time, ⭐️ the repo and open an issue for ideas — I'm actively improving it.

[![GitHub Stars](https://img.shields.io/github/stars/MichielMe/fastscheduler?style=social)](https://github.com/MichielMe/fastscheduler)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

## Features

- 🎯 **Simple decorator-based API** - Schedule tasks in one line
- ⚡ **Async/await support** - Native async function support
- 🕐 **Timezone support** - Schedule jobs in any timezone
- 📅 **Cron expressions** - Complex schedules with cron syntax
- 💾 **Persistent state** - Survives restarts, handles missed jobs
- 🎨 **FastAPI dashboard** - Beautiful real-time monitoring UI
- 🔄 **Automatic retries** - Configurable retry with exponential backoff
- ⏱️ **Job timeouts** - Kill long-running jobs automatically
- ⏸️ **Pause/Resume** - Control jobs without removing them

## Installation

```bash
# Basic installation
pip install fastscheduler

# With FastAPI dashboard
pip install fastscheduler[fastapi]

# With cron expression support
pip install fastscheduler[cron]

# All features
pip install fastscheduler[all]
```

## Quick Start

```python
from fastscheduler import FastScheduler

scheduler = FastScheduler(quiet=True)

@scheduler.every(10).seconds
def task():
    print("Task executed")

@scheduler.daily.at("14:30")
async def daily_task():
    print("Daily task at 2:30 PM")

scheduler.start()
```

## Scheduling Options

### Interval-based

```python
@scheduler.every(10).seconds
@scheduler.every(5).minutes
@scheduler.every(2).hours
@scheduler.every(1).days
```

### Time-based

```python
@scheduler.daily.at("09:00")              # Daily at 9 AM
@scheduler.hourly.at(":30")               # Every hour at :30
@scheduler.weekly.monday.at("10:00")      # Every Monday at 10 AM
@scheduler.weekly.weekdays.at("09:00")    # Weekdays at 9 AM
@scheduler.weekly.weekends.at("12:00")    # Weekends at noon
```

### Cron Expressions

Requires: `pip install fastscheduler[cron]`

```python
@scheduler.cron("0 9 * * MON-FRI")        # 9 AM on weekdays
def market_open():
    ...

@scheduler.cron("*/15 * * * *")           # Every 15 minutes
def frequent_check():
    ...

@scheduler.cron("0 0 1 * *")              # First day of each month
def monthly_report():
    ...
```

### One-time Jobs

```python
@scheduler.once(60)                        # Run once after 60 seconds
def delayed_task():
    ...

@scheduler.at("2024-12-25 00:00:00")      # Run at specific datetime
def christmas_task():
    ...
```

## Timezone Support

Schedule jobs in any timezone:

```python
# Using the tz parameter
@scheduler.daily.at("09:00", tz="America/New_York")
def nyc_morning():
    print("Good morning, New York!")

# Using the .tz() method (chainable)
@scheduler.weekly.monday.tz("Europe/London").at("09:00")
def london_standup():
    print("Monday standup")

# With cron expressions
@scheduler.cron("0 9 * * MON-FRI").tz("Asia/Tokyo")
def tokyo_market():
    print("Tokyo market open")
```

Common timezones: `UTC`, `America/New_York`, `America/Los_Angeles`, `Europe/London`, `Europe/Paris`, `Asia/Tokyo`, `Asia/Shanghai`, `Australia/Sydney`

## Job Control

### Timeouts

Kill jobs that run too long:

```python
@scheduler.every(1).minutes.timeout(30)   # Kill if runs > 30 seconds
def quick_task():
    ...

@scheduler.daily.at("02:00").timeout(3600)  # 1 hour max
def nightly_backup():
    ...
```

### Retries

Configure automatic retries on failure:

```python
@scheduler.every(5).minutes.retries(5)    # Retry up to 5 times
def flaky_api_call():
    ...
```

Retries use exponential backoff (2s, 4s, 8s, 16s, ...).

### Skip Catch-up

Don't run missed jobs after restart:

```python
@scheduler.every(1).hours.no_catch_up()
def hourly_stats():
    ...
```

### Pause, Resume, and Cancel

```python
# Pause a job (stays in queue but won't execute)
scheduler.pause_job("job_0")

# Resume a paused job
scheduler.resume_job("job_0")

# Cancel and remove a job
scheduler.cancel_job("job_0")

# Cancel all jobs with a specific function name
scheduler.cancel_job_by_name("my_task")
```

## FastAPI Integration

Add a beautiful real-time dashboard to your FastAPI app:

```python
from fastapi import FastAPI
from fastscheduler import FastScheduler
from fastscheduler.fastapi_integration import create_scheduler_routes

app = FastAPI()
scheduler = FastScheduler(quiet=True)

# Add dashboard at /scheduler/
app.include_router(create_scheduler_routes(scheduler))

@scheduler.every(30).seconds
def background_task():
    print("Background work")

scheduler.start()
```

### Dashboard Features

Access at `http://localhost:8000/scheduler/`

- **Real-time updates** via Server-Sent Events (SSE)
- **Job cards** with status indicators and countdown timers
- **Quick actions** - Pause/Resume/Cancel directly from the UI
- **Execution history** with error logs
- **Statistics** - Success rate, uptime, jobs per hour
- **Filter & search** - Find jobs by status or name
- **Toast notifications** - Alerts for job completions and failures

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/scheduler/` | GET | Dashboard UI |
| `/scheduler/api/status` | GET | Scheduler status |
| `/scheduler/api/jobs` | GET | List all jobs |
| `/scheduler/api/jobs/{job_id}` | GET | Get specific job |
| `/scheduler/api/jobs/{job_id}/pause` | POST | Pause a job |
| `/scheduler/api/jobs/{job_id}/resume` | POST | Resume a job |
| `/scheduler/api/jobs/{job_id}/cancel` | POST | Cancel a job |
| `/scheduler/api/history` | GET | Execution history |
| `/scheduler/events` | GET | SSE event stream |

## Configuration

```python
scheduler = FastScheduler(
    state_file="scheduler.json",    # Persistence file (default: fastscheduler_state.json)
    quiet=True,                     # Suppress log messages (default: False)
    auto_start=False,               # Start immediately (default: False)
    max_history=5000,               # Max history entries to keep (default: 10000)
    max_workers=20,                 # Concurrent job threads (default: 10)
    history_retention_days=8,       # Delete history older than X days (default: 7)
)
```

### History Retention

History is automatically cleaned up based on two limits (both are enforced):
- **Count limit**: `max_history` - maximum number of entries
- **Time limit**: `history_retention_days` - maximum age in days

Set `history_retention_days=0` to disable time-based cleanup (only count limit applies).

## Monitoring

### Programmatic Access

```python
# Get all jobs
jobs = scheduler.get_jobs()

# Get specific job
job = scheduler.get_job("job_0")

# Get execution history
history = scheduler.get_history(limit=100)
history = scheduler.get_history(func_name="my_task", limit=50)

# Get statistics
stats = scheduler.get_statistics()
# Returns: total_runs, total_failures, uptime, per_job stats

# Print simple status to console
scheduler.print_status()
```

### Context Manager

```python
with FastScheduler(quiet=True) as scheduler:
    @scheduler.every(5).seconds
    def task():
        print("Running")
    
    # Scheduler starts automatically
    time.sleep(30)
# Scheduler stops automatically on exit
```

## State Persistence

FastScheduler automatically saves state to disk:

- Job definitions and schedules
- Execution history
- Statistics
- Job counter (ensures unique IDs across restarts)

On restart, it:
1. Restores all jobs
2. Calculates missed executions
3. Runs catch-up jobs (unless `no_catch_up()` is set)

## Examples

### Complete Example

```python
import asyncio
import time
from fastscheduler import FastScheduler

scheduler = FastScheduler(quiet=True)

# Simple interval job
@scheduler.every(10).seconds
def heartbeat():
    print(f"[{time.strftime('%H:%M:%S')}] ❤️ Heartbeat")

# Async job with timezone
@scheduler.daily.at("09:00", tz="America/New_York").timeout(60)
async def morning_report():
    print("Generating report...")
    await asyncio.sleep(5)
    print("Report sent!")

# Cron job with retries
@scheduler.cron("*/5 * * * *").retries(3)
def check_api():
    print("Checking API health")

# Weekly job
@scheduler.weekly.monday.at("10:00")
def weekly_standup():
    print("Time for standup!")

# Start scheduler
scheduler.start()

try:
    while True:
        time.sleep(60)
        scheduler.print_status()
except KeyboardInterrupt:
    scheduler.stop()
```

### FastAPI with Lifespan

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastscheduler import FastScheduler
from fastscheduler.fastapi_integration import create_scheduler_routes

scheduler = FastScheduler(quiet=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    yield
    scheduler.stop(wait=True)

app = FastAPI(lifespan=lifespan)
app.include_router(create_scheduler_routes(scheduler))

@scheduler.every(30).seconds
def background_job():
    print("Working...")
```

## API Reference

### FastScheduler

| Method | Description |
|--------|-------------|
| `start()` | Start the scheduler |
| `stop(wait=True, timeout=30)` | Stop gracefully |
| `get_jobs()` | List all scheduled jobs |
| `get_job(job_id)` | Get specific job by ID |
| `get_history(func_name=None, limit=50)` | Get execution history |
| `get_statistics()` | Get runtime statistics |
| `pause_job(job_id)` | Pause a job |
| `resume_job(job_id)` | Resume a paused job |
| `cancel_job(job_id)` | Cancel and remove a job |
| `cancel_job_by_name(func_name)` | Cancel all jobs by function name |
| `print_status()` | Print status to console |

### Scheduler Methods

| Method | Description |
|--------|-------------|
| `every(n).seconds/minutes/hours/days` | Interval scheduling |
| `daily.at("HH:MM")` | Daily at specific time |
| `hourly.at(":MM")` | Hourly at specific minute |
| `weekly.monday/tuesday/.../sunday.at("HH:MM")` | Weekly scheduling |
| `weekly.weekdays/weekends.at("HH:MM")` | Weekday/weekend scheduling |
| `cron("expression")` | Cron expression scheduling |
| `once(seconds)` | One-time delayed execution |
| `at("YYYY-MM-DD HH:MM:SS")` | One-time at specific datetime |

### Chainable Modifiers

| Modifier | Description |
|----------|-------------|
| `.timeout(seconds)` | Maximum execution time |
| `.retries(n)` | Maximum retry attempts |
| `.no_catch_up()` | Skip missed executions |
| `.tz("timezone")` | Set timezone for schedule |

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR on [GitHub](https://github.com/MichielMe/fastscheduler).
