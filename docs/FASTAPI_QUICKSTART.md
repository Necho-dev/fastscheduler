# FastAPI Integration Quick Start

## Basic Setup (5 Minutes)

### 1. Install Dependencies

```bash
pip install fastscheduler[fastapi]
```

### 2. Create Your App

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastscheduler import FastScheduler
from fastscheduler.fastapi_integration import create_scheduler_routes

scheduler = FastScheduler()

# Define your tasks
def my_task():
    print("Task executed!")

scheduler.register_function(my_task)

# Setup FastAPI with proper lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    yield
    scheduler.stop(wait=False)

app = FastAPI(lifespan=lifespan)
app.include_router(create_scheduler_routes(scheduler))

# Schedule tasks
@scheduler.every(30).seconds
def background_task():
    print("Background work")
```

### 3. Run Your App

```bash
uvicorn myapp:app --reload
```

### 4. Access Dashboard

Open http://localhost:8000/scheduler/ to see:
- Real-time job status updates
- Execution history
- Job management controls

## Production Setup with Graceful Shutdown

For production applications, use this pattern to ensure SSE connections close gracefully:

```python
from contextlib import asynccontextmanager
import anyio
from fastapi import FastAPI
from fastscheduler import FastScheduler
from fastscheduler.fastapi_integration import (
    create_scheduler_routes,
    install_shutdown_handlers
)

scheduler = FastScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    install_shutdown_handlers(scheduler)  # Enable graceful shutdown
    scheduler.start()
    yield
    await anyio.sleep(0.5)  # Brief grace period for SSE
    scheduler.stop(wait=False)

app = FastAPI(lifespan=lifespan)
app.include_router(
    create_scheduler_routes(scheduler, install_signal_handlers=False)
)
```

**Benefits:**
- Ctrl+C shuts down in ~1 second (not 30 seconds)
- SSE connections close gracefully
- No timeout warnings or errors

---

## API Usage Examples

### Create Jobs via API

```python
import requests

# Interval job: Every 60 seconds, with a human-friendly job_name
requests.post("http://localhost:8000/scheduler/api/jobs", json={
    "func_name": "my_task",
    "func_module": "__main__",
    "job_name": "hourly_cleanup",  # Shown in dashboard to distinguish tasks using the same function
    "schedule_type": "interval",
    "schedule_config": {
        "interval": 60,
        "unit": "seconds"
    }
})
```

### Manage Jobs

```python
# Pause a job
requests.post("http://localhost:8000/scheduler/api/jobs/job_1/pause")

# Resume a job
requests.post("http://localhost:8000/scheduler/api/jobs/job_1/resume")

# Run job immediately
requests.post("http://localhost:8000/scheduler/api/jobs/job_1/run")

# Delete a job
requests.delete("http://localhost:8000/scheduler/api/jobs/job_1")
```

### Group Operations

```python
# Pause all jobs in a group
requests.post("http://localhost:8000/scheduler/api/groups/background/pause")

# Resume a group
requests.post("http://localhost:8000/scheduler/api/groups/background/resume")

# Cancel entire group
requests.delete("http://localhost:8000/scheduler/api/groups/background")
```

---

## Common Patterns

### Database Storage

```python
scheduler = FastScheduler(
    storage="sqlalchemy",
    database_url="postgresql://user:pass@localhost/mydb"
)
```

### Redis Queue

```python
scheduler = FastScheduler(
    queue="redis",
    redis_url="redis://localhost:6379/0"
)
```

### Timezone Support

```python
@scheduler.daily.at("09:00").timezone("America/New_York")
def morning_task():
    print("9 AM Eastern Time")
```

### Async Tasks

```python
@scheduler.every(10).seconds
async def async_task():
    await some_async_operation()
```

---

## Dashboard Features

The web dashboard provides:

1. **Real-time Monitoring** - Live job status via SSE, active jobs counter, success/failure rates
2. **Job Management** - Pause/Resume, run immediately, cancel jobs, group operations
3. **History & Debugging** - Execution history, failed job details, dead letter queue
4. **Theme & Language** - Dark/Light theme, English/Chinese (简体中文)

---

## Troubleshooting

### Slow Shutdown (30 seconds timeout)

**Solution:** Use the graceful shutdown pattern with `install_shutdown_handlers()` in lifespan.

### Jobs Not Running

**Check:**
1. `scheduler.start()` is called in lifespan
2. Functions are registered: `scheduler.register_function(my_task)`
3. Scheduler is not stopped

### Dashboard Not Loading

**Check:**
1. Dependencies installed: `pip install fastscheduler[fastapi]`
2. Routes registered: `app.include_router(create_scheduler_routes(scheduler))`
3. Correct URL: http://localhost:8000/scheduler/

### API Jobs Fail with "Function not registered"

**Solution:**
```python
scheduler.register_function(my_task)  # Register before creating jobs
```

---

## Advanced: SSE Graceful Shutdown Implementation

### Problem

When using the dashboard with FastAPI, pressing Ctrl+C causes:
- Uvicorn to wait up to 30 seconds for SSE connections to close
- Application appearing frozen during shutdown
- Need to press Ctrl+C twice or wait for timeout

### Root Cause

Uvicorn's shutdown sequence:
1. Receives SIGINT/SIGTERM
2. Waits for all HTTP connections to close (including SSE)
3. After connections close → Executes lifespan shutdown
4. Then completes shutdown

SSE connections are long-lived streams that don't close automatically, so step 2 blocks for 30 seconds.

### Solution

Signal SSE connections to close **before** Uvicorn starts waiting:

```
1. Ctrl+C → Our signal handler receives SIGINT
2. Handler calls scheduler.shutdown_connection()
3. SSE detects shutdown → Sends 'event: shutdown' to frontend
4. Frontend receives event → Calls eventSource.close()
5. Connection closes immediately
6. Handler chains to Uvicorn's handler
7. Uvicorn's "wait for connections" completes quickly
8. Application exits cleanly
```

### Implementation Components

#### 1. Scheduler Shutdown Flag

```python
class FastScheduler:
    def __init__(self, ...):
        self._shutdown_event_set = False
        self._shutdown_event_lock = threading.Lock()
    
    def shutdown_connection(self) -> None:
        """Non-blocking signal for shutdown."""
        with self._shutdown_event_lock:
            self._shutdown_event_set = True
    
    def is_shutdown_requested(self) -> bool:
        with self._shutdown_event_lock:
            return self._shutdown_event_set
```

#### 2. Signal Handler Installation

```python
def install_shutdown_handlers(scheduler: "FastScheduler"):
    """Install signal handlers for graceful SSE shutdown.
    
    Must be called in FastAPI lifespan startup, AFTER Uvicorn starts.
    """
    original_sigint = signal.getsignal(signal.SIGINT)
    
    def handle_shutdown_signal(signum, frame):
        scheduler.shutdown_connection()  # Signal SSE first
        if callable(original_sigint):
            original_sigint(signum, frame)  # Chain to Uvicorn
    
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
```

**Critical Timing:** Install in lifespan startup, not at module import:
- Uvicorn installs its handlers during startup
- If we install before Uvicorn, our handler gets overwritten
- Installing in lifespan allows us to save and chain to Uvicorn's handler

#### 3. SSE Event Generator

```python
async def event_generator(request: Request):
    while True:
        if await request.is_disconnected():
            break
        
        if scheduler.is_shutdown_requested():
            yield "event: shutdown\ndata: bye\n\n"  # Send shutdown signal
            break
        
        # Normal data streaming
        yield f"data: {json.dumps(data)}\n\n"
        
        # Responsive sleep: check shutdown every 0.1s
        for _ in range(10):
            if scheduler.is_shutdown_requested():
                break
            await anyio.sleep(0.1)
```

#### 4. Frontend Handler

```javascript
const eventSource = new EventSource(`${API}/events`);

eventSource.addEventListener("shutdown", (event) => {
    console.log("Server is shutting down, closing connection...");
    eventSource.close();  // Actively close connection
    updateConnection(false);
});

eventSource.onerror = () => {
    eventSource.close();
    updateConnection(false);
    setTimeout(connectSSE, 2000);  // Only reconnect on errors
};
```

### Testing

Run the test application:

```bash
cd tests
./run_shutdown_test.sh
```

Expected results:
1. Server starts, open http://localhost:8001/scheduler/
2. Press Ctrl+C once
3. See logs: "Signal 2 received, sent shutdown signal to SSE connections"
4. Shutdown completes in < 2 seconds
5. No timeout warnings or errors

### Why Not Use `anyio.open_signal_receiver`?

Initial attempts used `anyio.open_signal_receiver` inside SSE generator, but:
- Intercepts signals that Uvicorn needs
- Breaks Uvicorn's shutdown flow
- Causes lifespan to never execute shutdown
- Results in `CancelledError` exceptions

### Performance

- **Shutdown time:** ~1-2 seconds (vs 30 seconds)
- **Response time:** SSE detects shutdown within 0.1s
- **Overhead:** Negligible (boolean flag + lock, check every 0.1s)

### Troubleshooting Shutdown Issues

**Still seeing 30s timeout:**
1. Verify handlers installed in lifespan, not at import
2. Check `install_signal_handlers=False` in `create_scheduler_routes()`
3. Ensure browser console shows "Server is shutting down"

**Frontend not receiving shutdown event:**
1. Check SSE generator has `is_shutdown_requested()` check
2. Verify `shutdown_connection()` is called (check logs)
3. Ensure frontend has `addEventListener("shutdown", ...)` handler

**Multiple Ctrl+C required:**
1. Handlers installing too early (before Uvicorn)
2. Move `install_shutdown_handlers()` into lifespan startup
3. Set `install_signal_handlers=False` in routes

---

## Next Steps

- [Full Documentation](../README.md)
- [API Reference](../README.md#restful-api-documentation)
- Example: `tests/test_fastapi_app.py` - Full-featured demo
- Example: `tests/test_shutdown.py` - Graceful shutdown test
