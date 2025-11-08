# FastScheduler Tests

Comprehensive test suite for the FastScheduler package.

## Test Coverage

### Core Scheduler Tests (`test_scheduler.py`)

- **Basic Scheduling**: Tests for all scheduling methods (every, daily, weekly, hourly, once, at)
- **Job Execution**: Sync/async job execution, arguments, retries
- **State Persistence**: Save/load state, history persistence
- **Statistics**: Job tracking, monitoring, history retrieval
- **Lifecycle Management**: Start/stop, context manager, edge cases

### FastAPI Integration Tests (`test_fastapi_integration.py`)

- **Routes**: Dashboard endpoint, SSE events, custom prefixes
- **Display**: Jobs, statistics, status, history
- **Integration**: Full app lifecycle, multiple jobs, async support
- **Error Handling**: Failed jobs, no jobs, scheduler not running

## Running Tests

```bash
# Run all tests
uv run pytest tests/

# Run with verbose output
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_scheduler.py -v

# Run specific test
uv run pytest tests/test_scheduler.py::TestBasicScheduling::test_every_seconds_decorator -v

# Run with coverage
uv run pytest tests/ --cov=fastscheduler --cov-report=html
```

## Test Organization

- `conftest.py` - Shared fixtures and pytest configuration
- `test_scheduler.py` - Core scheduler functionality tests
- `test_fastapi_integration.py` - FastAPI integration tests

## Key Fixtures

- `scheduler` - Creates a test scheduler instance with temp state file
- `temp_state_file` - Provides temporary state file path
- `app` - FastAPI app with scheduler routes
- `client` - FastAPI test client

## Notes

- Tests use temporary files for state persistence
- Some tests include sleep() calls to allow jobs to execute
- All schedulers are cleaned up after tests (stopped properly)


