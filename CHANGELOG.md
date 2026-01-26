# Changelog

## [0.3.0] - 2026-01-18 (Unreleased)

### Added
- **SQLAlchemy storage backend**: Replaced SQLModel with SQLAlchemy for better compatibility
  - Supports SQLite, MySQL, PostgreSQL databases
  - Improved database connection management
  - Better error handling and transaction support
- **Task Management API**: New RESTful API endpoints for dynamic task management
  - `POST /api/jobs` - Create a new scheduled job
  - `PUT /api/jobs/{job_id}` - Update an existing job
  - `DELETE /api/jobs/{job_id}` - Delete a job
  - `POST /api/jobs/{job_id}/enable` - Enable a job
  - `POST /api/jobs/{job_id}/disable` - Disable a job
- **Public function registration**: `register_function()` method for programmatic function registration
- **Enhanced JSON storage**: Added file locking mechanism for better concurrency handling
- **Dashboard enhancements**: 
  - Language switching: English/Chinese (中文)
  - Theme switching: Light/Dark mode with system preference detection
  - All UI text is now translatable
  - User preferences (language and theme) are persisted in localStorage
- **Queue backend abstraction**: Pluggable queue backends for task scheduling
  - **Heapq backend** (default): Fast in-memory priority queue using Python's heapq
  - **Redis backend**: Distributed queue using Redis sorted sets (ZSET)
    - Enables multiple scheduler instances to share the same queue
    - Jobs persist across scheduler restarts
    - Atomic operations ensure thread safety
    - Install with: `pip install fastscheduler[redis]`

### Changed
- **Storage backend**: Default database backend changed from `sqlmodel` to `sqlalchemy`
  - `storage="sqlalchemy"` is now the recommended database backend
  - `storage="sqlmodel"` is deprecated but still supported for backward compatibility
- **Dependencies**: Core dependency changed from `sqlmodel` to `sqlalchemy`
  - `sqlmodel` moved to optional dependency for backward compatibility
- **Queue implementation**: Internal queue management refactored to use pluggable queue backends
  - Default behavior unchanged (heapq in-memory queue)
  - `self.jobs` is now a property that wraps the queue backend for backward compatibility

### Backward Compatibility
- Existing code using `storage="sqlmodel"` continues to work
- All existing decorator-based scheduling continues to work unchanged
- JSON storage backend remains unchanged and is still the default

### Migration Guide
To migrate from SQLModel to SQLAlchemy:
```python
# Old (still works)
scheduler = FastScheduler(storage="sqlmodel", database_url="sqlite:///scheduler.db")

# New (recommended)
scheduler = FastScheduler(storage="sqlalchemy", database_url="sqlite:///scheduler.db")
```

To use Redis distributed queue:
```python
# In-memory queue (default)
scheduler = FastScheduler()

# Redis distributed queue
scheduler = FastScheduler(
    queue="redis",
    redis_url="redis://localhost:6379/0"
)

# Multiple scheduler instances can share the same Redis queue
scheduler1 = FastScheduler(queue="redis", redis_url="redis://localhost:6379/0")
scheduler2 = FastScheduler(queue="redis", redis_url="redis://localhost:6379/0")
# Both schedulers will process jobs from the same queue
```

## [0.2.0] - 2026-01-17

### Added
- Database storage support via SQLModel (SQLite, PostgreSQL, MySQL)
- Pluggable storage backend architecture
- New `storage` and `database_url` parameters on FastScheduler
- `uv add fastscheduler --extras database` optional dependency
- Manual job trigger via dashboard "Run" button and `run_job_now()` method

### Changed
- Refactored persistence to use storage backends internally
- Split main module into `models.py`, `schedulers.py`, and `main.py` for better maintainability
- Storage backend cleanup on scheduler stop

### Backward Compatibility
Existing JSON storage works unchanged. To use database storage:
```python
scheduler = FastScheduler(storage="sqlmodel", database_url="sqlite:///scheduler.db")
```

## [0.1.2]

- Initial release
- JSON persistence
- Async support
- Cron expressions
- FastAPI dashboard
- Dead letter queue
