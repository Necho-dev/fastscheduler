# FastScheduler

Simple, lightweight task scheduler for Python with async support, timezone handling, cron expressions, and a beautiful real-time dashboard.

If this saves you time, ⭐️ the repo and open an issue for ideas — I'm actively improving it.

[![GitHub Stars](https://img.shields.io/github/stars/MichielMe/fastscheduler?style=social)](https://github.com/MichielMe/fastscheduler)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

![FastScheduler Demo](docs/images/fastscheduler.gif)

## Features

- 🎯 **Simple decorator-based API** - Schedule tasks in one line
- ⚡ **Async/await support** - Native async function support
- 🕐 **Timezone support** - Schedule jobs in any timezone
- 📅 **Cron expressions** - Complex schedules with cron syntax
- 💾 **Persistent state** - Survives restarts, handles missed jobs
- 🗄️ **Database support** - SQLite, PostgreSQL, MySQL via SQLAlchemy / SQLModel
- 🎨 **FastAPI dashboard** - Beautiful real-time monitoring UI
- 🔄 **Automatic retries** - Configurable retry with exponential backoff
- ⏱️ **Job timeouts** - Kill long-running jobs automatically
- ⏸️ **Pause/Resume** - Control jobs without removing them
- 📋 **Dead Letter Queue** - Track and debug failed jobs

## Installation

```bash
# Basic installation
pip install fastscheduler

# With FastAPI dashboard
pip install fastscheduler[fastapi]

# With SQLAlchemy database support
pip install fastscheduler[sqlalchemy]

# With SQLModel database support
pip install fastscheduler[sqlmodel]

# With database support (SQLite, PostgreSQL, MySQL)
pip install fastscheduler[database]

# With Redis queue backend
pip install fastscheduler[redis]

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

Cron expression support is included by default. No additional installation required.

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

## Job Groups & Queue Backends

- 🧩 **Job Groups** - Assign tasks to groups (default: `"default"`), filter by group, pause/resume, and batch cancel
- 🔀 **Queue Backends** - Choose between `heapq` (in-memory) or `redis` (distributed & persistent)
- 📡 **RESTful API** - Full CRUD operations for job management via HTTP API
- 🖥️ **Dashboard** - Multi-language (English/中文), light/dark theme, group filtering, real-time monitoring (SSE)

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

Add a beautiful real-time dashboard and RESTful API to your FastAPI app.

### Basic Setup (Recommended)

Use FastAPI's lifespan context manager for proper startup/shutdown handling:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastscheduler import FastScheduler
from fastscheduler.fastapi_integration import create_scheduler_routes

scheduler = FastScheduler(quiet=True)

# Register your task functions
def my_task():
    print("Task executed")

scheduler.register_function(my_task)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start the scheduler
    scheduler.start()
    yield
    # Shutdown: Stop the scheduler
    scheduler.stop(wait=False)

app = FastAPI(lifespan=lifespan)

# Add dashboard and API routes
app.include_router(create_scheduler_routes(scheduler, prefix="/scheduler"))

# Schedule tasks
@scheduler.every(30).seconds
def background_task():
    print("Background work")
```

**Dashboard:** Visit `http://localhost:8000/scheduler/` to see the real-time monitoring UI.

### Graceful Shutdown with SSE (Advanced)

For applications using the SSE-based dashboard, ensure graceful shutdown when pressing Ctrl+C:

```python
from contextlib import asynccontextmanager
import anyio
from fastapi import FastAPI
from fastscheduler import FastScheduler
from fastscheduler.fastapi_integration import create_scheduler_routes, install_shutdown_handlers

scheduler = FastScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    install_shutdown_handlers(scheduler)  # Enable graceful SSE shutdown
    scheduler.start()
    yield
    # Shutdown
    await anyio.sleep(0.5)  # Brief grace period for SSE connections
    scheduler.stop(wait=False)

app = FastAPI(lifespan=lifespan)

# Important: Disable auto-install since we're doing it manually in lifespan
app.include_router(
    create_scheduler_routes(scheduler, install_signal_handlers=False)
)
```

**Why?** This ensures that when you press Ctrl+C:
1. SSE connections receive a shutdown signal
2. Frontend closes connections gracefully
3. Uvicorn completes shutdown in ~1 second (not 30 seconds timeout)

See [FastAPI Quick Start Guide](docs/FASTAPI_QUICKSTART.md) for details.

## RESTful API Documentation

FastScheduler provides a comprehensive RESTful API for managing scheduled jobs programmatically. All endpoints are prefixed with `/scheduler` by default.

### Prerequisites

Before using the API, ensure that:

1. **Functions are registered**: All functions you want to schedule must be registered with the scheduler:
   ```python
   scheduler.register_function(my_task)
   scheduler.register_function(another_task)
   ```

2. **Scheduler is running**: The scheduler must be started before jobs can execute:
   ```python
   scheduler.start()
   ```

### Creating Jobs (POST /api/jobs)

Create a new scheduled job via API.

**Endpoint:** `POST /scheduler/api/jobs`

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `func_name` | string | Yes | Function name (must be registered) |
| `func_module` | string | Yes | Function module path (e.g., `"dev.test_app"`, `"__main__"`) |
| `job_name` | string | No | Human-friendly job name, useful to distinguish multiple tasks using the same function (e.g., different scripts) |
| `schedule_type` | string | Yes | Schedule type: `"interval"`, `"daily"`, `"weekly"`, `"hourly"`, `"cron"`, or `"once"` |
| `schedule_config` | object | Yes | Schedule configuration (see examples below) |
| `max_retries` | integer | No | Maximum retry attempts (default: 3) |
| `timeout` | float | No | Maximum execution time in seconds |
| `timezone` | string | No | Timezone string (e.g., `"Asia/Shanghai"`, `"America/New_York"`) |
| `enabled` | boolean | No | Whether the job is enabled (default: `true`) |
| `args` | array | No | Function arguments (default: `[]`) |
| `kwargs` | object | No | Function keyword arguments (default: `{}`) |
| `group` | string | No | Job group name (default: `"default"`) |

**Response:**

```json
{
  "success": true,
  "message": "Job created successfully",
  "job_id": "job_0"
}
```

#### Schedule Type Examples

##### 1. Interval Scheduling

Run a task at regular intervals:

```python
import requests

# Every 30 seconds
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "data_sync_task",
    "func_module": "dev.test_app",
    "schedule_type": "interval",
    "schedule_config": {
        "interval": 30,
        "unit": "seconds"  # "seconds", "minutes", "hours", or "days"
    },
    "args": ["Real-time Sync"],
    "group": "data_sync"
})

# Every 5 minutes
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "health_check_task",
    "func_module": "dev.test_app",
    "schedule_type": "interval",
    "schedule_config": {
        "interval": 5,
        "unit": "minutes"
    }
})

# Every 2 hours
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "metrics_collection_task",
    "func_module": "dev.test_app",
    "schedule_type": "interval",
    "schedule_config": {
        "interval": 2,
        "unit": "hours"
    }
})
```

##### 2. Daily Scheduling

Run a task daily at a specific time:

```python
# Daily at 02:00
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "backup_task",
    "func_module": "dev.test_app",
    "schedule_type": "daily",
    "schedule_config": {
        "time": "02:00"  # Format: "HH:MM"
    },
    "args": ["Nightly Backup"],
    "group": "data_sync"
})

# Daily at 09:30 with timezone
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "report_generation_task",
    "func_module": "dev.test_app",
    "schedule_type": "daily",
    "schedule_config": {
        "time": "09:30"
    },
    "timezone": "Asia/Shanghai",
    "args": ["Morning Report"]
})
```

##### 3. Weekly Scheduling

Run a task on specific days of the week:

```python
# Every Monday and Friday at 10:00
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "report_generation_task",
    "func_module": "dev.test_app",
    "schedule_type": "weekly",
    "schedule_config": {
        "time": "10:00",
        "days": [0, 4]  # 0=Monday, 1=Tuesday, ..., 6=Sunday
    },
    "args": ["Weekly Summary"],
    "group": "notification"
})

# Every Sunday at 00:00
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "database_maintenance_task",
    "func_module": "dev.test_app",
    "schedule_type": "weekly",
    "schedule_config": {
        "time": "00:00",
        "days": [6]  # Sunday
    },
    "group": "security"
})
```

##### 4. Hourly Scheduling

Run a task every hour at a specific minute:

```python
# Every hour at minute 0
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "email_digest_task",
    "func_module": "dev.test_app",
    "schedule_type": "hourly",
    "schedule_config": {
        "time": "0"  # Minute (0-59) or "HH:MM" format
    },
    "args": ["Hourly Digest"],
    "group": "notification"
})

# Every hour at minute 30
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "metrics_collection_task",
    "func_module": "dev.test_app",
    "schedule_type": "hourly",
    "schedule_config": {
        "time": "30"
    }
})
```

##### 5. Cron Scheduling

Run a task using cron expressions:

```python
# Every 5 minutes
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "metrics_collection_task",
    "func_module": "dev.test_app",
    "schedule_type": "cron",
    "schedule_config": {
        "expression": "*/5 * * * *"
    },
    "group": "data_sync"
})

# Every 15 minutes
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "api_monitoring_task",
    "func_module": "dev.test_app",
    "schedule_type": "cron",
    "schedule_config": {
        "expression": "*/15 * * * *"
    },
    "args": ["/api/health"],
    "group": "maintenance"
})

# Every weekday at 9 AM
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "report_generation_task",
    "func_module": "dev.test_app",
    "schedule_type": "cron",
    "schedule_config": {
        "expression": "0 9 * * MON-FRI"
    },
    "timezone": "America/New_York"
})
```

##### 6. Once Scheduling (One-time Tasks)

Run a task once after a delay:

```python
# Execute after 10 seconds
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "one_time_task",
    "func_module": "dev.test_app",
    "schedule_type": "once",
    "schedule_config": {
        "delay": 10,
        "unit": "seconds"  # "seconds", "minutes", "hours", or "days"
    },
    "args": ["Initial Setup"],
    "group": "one_time"
})

# Execute after 1 minute
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "delayed_cleanup_task",
    "func_module": "dev.test_app",
    "schedule_type": "once",
    "schedule_config": {
        "delay": 1,
        "unit": "minutes"
    }
})
```

#### Complete Example with All Options

```python
response = requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "data_sync_task",
    "func_module": "dev.test_app",
    "schedule_type": "interval",
    "schedule_config": {
        "interval": 30,
        "unit": "seconds"
    },
    "max_retries": 5,
    "timeout": 60.0,
    "timezone": "Asia/Shanghai",
    "enabled": True,
    "args": ["Real-time Sync"],
    "kwargs": {"priority": "high"},
    "group": "data_sync"
})
```

### Updating Jobs (PUT /api/jobs/{job_id})

Update an existing job's configuration.

**Endpoint:** `PUT /scheduler/api/jobs/{job_id}`

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `job_id` | string | Job ID to update |

**Request Body:** (All fields are optional - only include fields you want to update)

| Field | Type | Description |
|-------|------|-------------|
| `schedule_config` | object | Updated schedule configuration |
| `max_retries` | integer | Updated max retries |
| `timeout` | float | Updated timeout in seconds |
| `timezone` | string | Updated timezone |
| `enabled` | boolean | Whether job is enabled |

**Response:**

```json
{
  "success": true,
  "message": "Job job_0 updated successfully"
}
```

**Examples:**

```python
# Update schedule interval
response = requests.put("http://localhost:8000/scheduler/api/jobs/job_0", json={
    "schedule_config": {
        "interval": 60,
        "unit": "seconds"
    }
})

# Disable a job
response = requests.put("http://localhost:8000/scheduler/api/jobs/job_0", json={
    "enabled": False
})

# Update timeout and max retries
response = requests.put("http://localhost:8000/scheduler/api/jobs/job_0", json={
    "timeout": 120.0,
    "max_retries": 5
})

# Update daily schedule time
response = requests.put("http://localhost:8000/scheduler/api/jobs/job_1", json={
    "schedule_config": {
        "time": "03:00"
    }
})

# Update cron expression
response = requests.put("http://localhost:8000/scheduler/api/jobs/job_2", json={
    "schedule_config": {
        "expression": "*/10 * * * *"
    }
})
```

### Deleting Jobs (DELETE /api/jobs/{job_id})

Delete a job permanently.

**Endpoint:** `DELETE /scheduler/api/jobs/{job_id}`

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `job_id` | string | Job ID to delete |

**Response:**

```json
{
  "success": true,
  "message": "Job job_0 deleted successfully"
}
```

**Example:**

```python
response = requests.delete("http://localhost:8000/scheduler/api/jobs/job_0")
```

### Getting Jobs (GET /api/jobs)

Retrieve all jobs or filter by group.

**Endpoint:** `GET /scheduler/api/jobs`

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `group` | string | No | Filter jobs by group name |

**Response:**

```json
{
  "jobs": [
    {
      "job_id": "job_0",
      "func_name": "data_sync_task",
      "group": "data_sync",
      "status": "scheduled",
      "schedule": "Every 30 seconds",
      "next_run": "2024-01-21 14:35:00",
      "next_run_in": 25.5,
      "run_count": 10,
      "retry_count": 0,
      "paused": false,
      "last_run": "2024-01-21 14:34:30"
    }
  ]
}
```

**Examples:**

```python
# Get all jobs
response = requests.get("http://localhost:8000/scheduler/api/jobs")
jobs = response.json()["jobs"]

# Get jobs filtered by group
response = requests.get("http://localhost:8000/scheduler/api/jobs?group=data_sync")
data_sync_jobs = response.json()["jobs"]
```

### Getting a Specific Job (GET /api/jobs/{job_id})

Get detailed information about a specific job.

**Endpoint:** `GET /scheduler/api/jobs/{job_id}`

**Response:**

```json
{
  "job": {
    "job_id": "job_0",
    "func_name": "data_sync_task",
    "group": "data_sync",
    "status": "scheduled",
    "schedule": "Every 30 seconds",
    "next_run": "2024-01-21 14:35:00",
    "next_run_in": 25.5,
    "run_count": 10,
    "retry_count": 0,
    "paused": false,
    "timeout": 60.0,
    "last_run": "2024-01-21 14:34:30"
  }
}
```

### Job Control Endpoints

#### Pause a Job

**Endpoint:** `POST /scheduler/api/jobs/{job_id}/pause`

Pause a job (it will remain in the queue but won't execute).

```python
response = requests.post("http://localhost:8000/scheduler/api/jobs/job_0/pause")
```

#### Resume a Job

**Endpoint:** `POST /scheduler/api/jobs/{job_id}/resume`

Resume a paused job.

```python
response = requests.post("http://localhost:8000/scheduler/api/jobs/job_0/resume")
```

#### Enable a Job

**Endpoint:** `POST /scheduler/api/jobs/{job_id}/enable`

Enable a job (same as resume, but clearer semantics).

```python
response = requests.post("http://localhost:8000/scheduler/api/jobs/job_0/enable")
```

#### Disable a Job

**Endpoint:** `POST /scheduler/api/jobs/{job_id}/disable`

Disable a job (same as pause, but clearer semantics).

```python
response = requests.post("http://localhost:8000/scheduler/api/jobs/job_0/disable")
```

#### Run Job Now

**Endpoint:** `POST /scheduler/api/jobs/{job_id}/run`

Trigger immediate execution of a job (useful for testing).

```python
response = requests.post("http://localhost:8000/scheduler/api/jobs/job_0/run")
```

**Note:** The job will execute immediately if it's not already running.

#### Cancel a Job

**Endpoint:** `POST /scheduler/api/jobs/{job_id}/cancel`

Cancel and remove a job (same as delete, but clearer semantics).

```python
response = requests.post("http://localhost:8000/scheduler/api/jobs/job_0/cancel")
```

### Dashboard Features

Access at `http://localhost:8000/scheduler/`

![FastScheduler Dashboard](docs/images/dashboard.png)

- **Real-time updates** via Server-Sent Events (SSE)
- **Job table** with status indicators, last 5 run results, and countdown timers
- **Quick actions** - Run/Pause/Resume/Cancel directly from the UI
- **Execution history** tab with filtering and search
- **Dead letter queue** tab - view failed jobs with error details
- **Statistics** - Success rate, uptime, active jobs count
- **Toast notifications** - Alerts for job completions and failures

### API Endpoints

#### Job Management

| Endpoint                              | Method | Description                          |
| ------------------------------------- | ------ | ------------------------------------ |
| `/scheduler/api/jobs`                 | GET    | List all jobs (supports `?group=xxx` filter) |
| `/scheduler/api/jobs`                 | POST   | Create a new job                     |
| `/scheduler/api/jobs/{job_id}`        | GET    | Get specific job by ID               |
| `/scheduler/api/jobs/{job_id}`        | PUT    | Update job (schedule, timeout, etc.) |
| `/scheduler/api/jobs/{job_id}`        | DELETE | Delete a job                         |
| `/scheduler/api/jobs/{job_id}/pause`  | POST   | Pause a job                          |
| `/scheduler/api/jobs/{job_id}/resume` | POST   | Resume a paused job                  |
| `/scheduler/api/jobs/{job_id}/enable` | POST   | Enable a job                         |
| `/scheduler/api/jobs/{job_id}/disable`| POST   | Disable a job                        |
| `/scheduler/api/jobs/{job_id}/run`    | POST   | Trigger immediate execution          |
| `/scheduler/api/jobs/{job_id}/cancel` | POST   | Cancel and remove a job              |

#### Group Management

| Endpoint                              | Method | Description                          |
| ------------------------------------- | ------ | ------------------------------------ |
| `/scheduler/api/groups`               | GET    | List all job groups                  |
| `/scheduler/api/groups/{group}/jobs`  | GET    | List all jobs in a group             |
| `/scheduler/api/groups/{group}/pause` | POST   | Pause all jobs in a group            |
| `/scheduler/api/groups/{group}/resume`| POST   | Resume all paused jobs in a group    |
| `/scheduler/api/groups/{group}`       | DELETE | Cancel all jobs in a group           |

#### Monitoring & Status

| Endpoint                              | Method | Description                          |
| ------------------------------------- | ------ | ------------------------------------ |
| `/scheduler/`                         | GET    | Dashboard UI                         |
| `/scheduler/api/status`               | GET    | Scheduler status and statistics      |
| `/scheduler/api/history`              | GET    | Execution history                    |
| `/scheduler/api/dead-letters`         | GET    | Dead letter queue (failed jobs)      |
| `/scheduler/api/dead-letters`         | DELETE | Clear dead letter queue              |
| `/scheduler/events`                   | GET    | SSE event stream for real-time updates |

### Error Handling

All API endpoints return standard HTTP status codes:

- **200 OK**: Request successful
- **201 Created**: Job created successfully
- **400 Bad Request**: Invalid request parameters
- **404 Not Found**: Job or resource not found
- **500 Internal Server Error**: Server error

**Error Response Format:**

```json
{
  "error": "Job not found",
  "job_id": "job_999"
}
```

**Common Errors:**

1. **Function not registered** (400):
   ```json
   {
     "detail": "Failed to create job. Make sure function dev.test_app.my_task is registered."
   }
   ```
   **Solution:** Register the function before creating the job:
   ```python
   scheduler.register_function(my_task)
   ```

2. **Job not found** (404):
   ```json
   {
     "error": "Job not found",
     "job_id": "job_999"
   }
   ```
   **Solution:** Check the job ID is correct using `GET /api/jobs`.

3. **Invalid schedule configuration** (400):
   ```json
   {
     "detail": "Unsupported schedule_type: invalid_type"
   }
   ```
   **Solution:** Use valid schedule types: `interval`, `daily`, `weekly`, `hourly`, `cron`, or `once`.

### Complete API Usage Example

Here's a complete example demonstrating all major API operations:

```python
import requests

BASE_URL = "http://localhost:8000/scheduler/api"

# 1. Register functions (must be done before creating jobs via API)
# This is done in your application code:
# scheduler.register_function(data_sync_task)
# scheduler.register_function(backup_task)

# 2. Create an interval job
response = requests.post(f"{BASE_URL}/jobs", json={
    "func_name": "data_sync_task",
    "func_module": "dev.test_app",
    "schedule_type": "interval",
    "schedule_config": {"interval": 30, "unit": "seconds"},
    "args": ["Real-time Sync"],
    "group": "data_sync",
    "max_retries": 3,
    "timeout": 60.0,
    "enabled": True
})
job_id = response.json()["job_id"]
print(f"Created job: {job_id}")

# 3. Get all jobs
response = requests.get(f"{BASE_URL}/jobs")
all_jobs = response.json()["jobs"]
print(f"Total jobs: {len(all_jobs)}")

# 4. Get jobs in a specific group
response = requests.get(f"{BASE_URL}/jobs?group=data_sync")
group_jobs = response.json()["jobs"]
print(f"Jobs in data_sync group: {len(group_jobs)}")

# 5. Get specific job details
response = requests.get(f"{BASE_URL}/jobs/{job_id}")
job = response.json()["job"]
print(f"Job status: {job['status']}")

# 6. Update job schedule
response = requests.put(f"{BASE_URL}/jobs/{job_id}", json={
    "schedule_config": {"interval": 60, "unit": "seconds"},
    "timeout": 120.0
})
print("Job updated")

# 7. Pause a job
response = requests.post(f"{BASE_URL}/jobs/{job_id}/pause")
print("Job paused")

# 8. Resume a job
response = requests.post(f"{BASE_URL}/jobs/{job_id}/resume")
print("Job resumed")

# 9. Trigger immediate execution
response = requests.post(f"{BASE_URL}/jobs/{job_id}/run")
print("Job triggered")

# 10. Get execution history
response = requests.get(f"{BASE_URL}/history?limit=10")
history = response.json()["history"]
print(f"Recent executions: {len(history)}")

# 11. Get scheduler status
response = requests.get(f"{BASE_URL}/status")
status = response.json()
print(f"Scheduler running: {status['running']}")
print(f"Total runs: {status['statistics']['total_runs']}")

# 12. Delete a job
response = requests.delete(f"{BASE_URL}/jobs/{job_id}")
print("Job deleted")

# 13. Group management - Get all groups
response = requests.get(f"{BASE_URL}/groups")
groups = response.json()["groups"]
print(f"Available groups: {groups}")

# 14. Group management - Pause all jobs in a group
response = requests.post(f"{BASE_URL}/groups/data_sync/pause")
result = response.json()
print(f"Paused {result['paused']} jobs in data_sync group")

# 15. Group management - Cancel all jobs in a group
response = requests.delete(f"{BASE_URL}/groups/data_sync")
result = response.json()
print(f"Cancelled {result['cancelled']} jobs in data_sync group")
```

## Configuration

```python
scheduler = FastScheduler(
    state_file="scheduler.json",    # Persistence file for JSON backend (default: fastscheduler_state.json)
    storage="json",                 # Storage backend: "json" (default), "sqlalchemy", or "sqlmodel"
    database_url=None,              # Database URL for SQLAlchemy/SQLModel backend
    queue="heapq",                  # Queue backend: "heapq" (default, in-memory) or "redis" (distributed)
    redis_url=None,                 # Redis URL for Redis queue backend (default: "redis://localhost:6379/0")
    queue_key=None,                 # Redis key for queue (default: "fastscheduler:queue")
    quiet=True,                     # Suppress log messages (default: False)
    auto_start=False,               # Start immediately (default: False)
    max_history=5000,               # Max history entries to keep (default: 10000)
    max_workers=20,                 # Concurrent job threads (default: 10)
    history_retention_days=8,       # Delete history older than X days (default: 7)
    max_dead_letters=500,           # Max failed jobs in dead letter queue (default: 500)
)
```

### History Retention

History is automatically cleaned up based on two limits (both are enforced):

- **Count limit**: `max_history` - maximum number of entries
- **Time limit**: `history_retention_days` - maximum age in days

Set `history_retention_days=0` to disable time-based cleanup (only count limit applies).

### Dead Letter Queue

Failed job executions are automatically stored in a separate dead letter queue for debugging:

```python
# Get failed jobs
dead_letters = scheduler.get_dead_letters(limit=100)

# Clear the queue
scheduler.clear_dead_letters()
```

The dead letter queue:

- Stores the last `max_dead_letters` failed jobs (default: 500)
- Persists to a separate JSON file (`*_dead_letters.json`) or database table
- Includes error messages, timestamps, run counts, and execution times
- Viewable in the dashboard "Failed" tab

## Storage Backends

FastScheduler supports multiple storage backends for persistence:

### JSON File Storage (Default)

Simple file-based storage, perfect for single-instance deployments:

```python
scheduler = FastScheduler(
    storage="json",  # or omit (default)
    state_file="scheduler.json"
)
```

### SQLAlchemy Storage

For production workloads requiring transactional integrity, concurrency, and multi-instance support.

**Requires:** `pip install fastscheduler[database]`

#### SQLite (Single-Server)

```python
scheduler = FastScheduler(
    storage="sqlalchemy",
    database_url="sqlite:///scheduler.db"
)
```

#### PostgreSQL (Production)

```python
scheduler = FastScheduler(
    storage="sqlalchemy",
    database_url="postgresql://user:password@localhost:5432/mydb"
)
```

#### MySQL

```python
scheduler = FastScheduler(
    storage="sqlalchemy",
    database_url="mysql://user:password@localhost:3306/mydb"
)
```

### SQLModel Storage

Alternative database storage backend using SQLModel (built on SQLAlchemy). Provides the same features as SQLAlchemy backend with a different API style.

**Requires:** `pip install fastscheduler[database]`

#### SQLite (Single-Server)

```python
scheduler = FastScheduler(
    storage="sqlmodel",
    database_url="sqlite:///scheduler.db"
)
```

#### PostgreSQL (Production)

```python
scheduler = FastScheduler(
    storage="sqlmodel",
    database_url="postgresql://user:password@localhost:5432/mydb"
)
```

#### MySQL

```python
scheduler = FastScheduler(
    storage="sqlmodel",
    database_url="mysql://user:password@localhost:3306/mydb"
)
```

**Note:** Both `sqlalchemy` and `sqlmodel` backends provide the same functionality. Choose based on your preference:
- Use `sqlalchemy` if you prefer SQLAlchemy's native API
- Use `sqlmodel` if you prefer SQLModel's Pydantic-integrated API

### Custom Storage Backend

Implement your own storage by subclassing `StorageBackend`:

```python
from fastscheduler.storage import StorageBackend

class MyCustomBackend(StorageBackend):
    def save_state(self, jobs, history, statistics, job_counter, scheduler_running):
        # Your implementation
        ...
    
    def load_state(self):
        # Your implementation
        ...
    
    # Implement other required methods...

scheduler = FastScheduler(storage=MyCustomBackend())
```

### Custom Queue Backend

Implement your own queue backend by subclassing `QueueBackend`:

```python
from fastscheduler.queue import QueueBackend
from fastscheduler.models import Job

class MyCustomQueueBackend(QueueBackend):
    def push(self, job: Job) -> None:
        # Your implementation
        ...
    
    def pop(self) -> Optional[Job]:
        # Your implementation
        ...
    
    # Implement other required methods...

scheduler = FastScheduler(queue=MyCustomQueueBackend())
```

### Database Tables

When using SQLAlchemy or SQLModel storage, the following tables are created automatically:

| Table | Purpose |
|-------|---------|
| `scheduler_jobs` | Active job definitions |
| `scheduler_history` | Execution history |
| `scheduler_dead_letters` | Failed job records |
| `scheduler_metadata` | Job counter, statistics |

## Queue Backends

FastScheduler supports pluggable queue backends for job scheduling:

### Heapq Queue (Default)

In-memory priority queue using Python's `heapq` module. Fast and simple, perfect for single-instance deployments:

```python
scheduler = FastScheduler(
    queue="heapq",  # or omit (default)
    # ... other config
)
```

**Features:**
- ✅ Fast in-memory operations
- ✅ No external dependencies
- ✅ Perfect for single-instance deployments

**Limitations:**
- ❌ Jobs lost on restart (unless persisted via storage backend)
- ❌ Not suitable for distributed deployments

### Redis Queue (Distributed)

Distributed queue using Redis sorted sets. Enables multiple scheduler instances to share the same queue:

```python
scheduler = FastScheduler(
    queue="redis",
    redis_url="redis://localhost:6379/0",
    queue_key="fastscheduler:queue",  # Optional, default: "fastscheduler:queue"
    # ... other config
)
```

**Requires:** `pip install fastscheduler[redis]` or `pip install redis`

**Features:**
- ✅ Distributed: Multiple scheduler instances can share the same queue
- ✅ Persistent: Jobs survive scheduler restarts
- ✅ Atomic operations: Redis ensures thread-safe operations
- ✅ Perfect for multi-instance deployments

**Note:** Both queue backends use the same `QueueBackend` abstraction, making it easy to switch between them or implement custom backends.

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

FastScheduler automatically saves state to disk (JSON) or database:

- Job definitions and schedules
- Execution history
- Statistics
- Job counter (ensures unique IDs across restarts)

On restart, it:

1. Restores all jobs
2. Calculates missed executions
3. Runs catch-up jobs (unless `no_catch_up()` is set)

Use JSON storage (default) for simple setups, or database storage for production workloads with multiple instances or high reliability requirements. See [Database Storage](#database-storage) for details.

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

| Method                                  | Description                         |
| --------------------------------------- | ----------------------------------- |
| `start()`                               | Start the scheduler                 |
| `stop(wait=True, timeout=30)`           | Stop gracefully                     |
| `get_jobs()`                            | List all scheduled jobs             |
| `get_job(job_id)`                       | Get specific job by ID              |
| `get_history(func_name=None, limit=50)` | Get execution history               |
| `get_statistics()`                      | Get runtime statistics              |
| `get_dead_letters(limit=100)`           | Get dead letter queue (failed jobs) |
| `clear_dead_letters()`                  | Clear all dead letter entries       |
| `pause_job(job_id)`                     | Pause a job                         |
| `resume_job(job_id)`                    | Resume a paused job                 |
| `run_job_now(job_id)`                   | Trigger immediate execution         |
| `cancel_job(job_id)`                    | Cancel and remove a job             |
| `cancel_job_by_name(func_name)`         | Cancel all jobs by function name    |
| `print_status()`                        | Print status to console             |

### Scheduler Methods

| Method                                         | Description                   |
| ---------------------------------------------- | ----------------------------- |
| `every(n).seconds/minutes/hours/days`          | Interval scheduling           |
| `daily.at("HH:MM")`                            | Daily at specific time        |
| `hourly.at(":MM")`                             | Hourly at specific minute     |
| `weekly.monday/tuesday/.../sunday.at("HH:MM")` | Weekly scheduling             |
| `weekly.weekdays/weekends.at("HH:MM")`         | Weekday/weekend scheduling    |
| `cron("expression")`                           | Cron expression scheduling    |
| `once(seconds)`                                | One-time delayed execution    |
| `at("YYYY-MM-DD HH:MM:SS")`                    | One-time at specific datetime |

### Chainable Modifiers

| Modifier            | Description               |
| ------------------- | ------------------------- |
| `.timeout(seconds)` | Maximum execution time    |
| `.retries(n)`       | Maximum retry attempts    |
| `.no_catch_up()`    | Skip missed executions    |
| `.tz("timezone")`   | Set timezone for schedule |

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR on [GitHub](https://github.com/MichielMe/fastscheduler).
