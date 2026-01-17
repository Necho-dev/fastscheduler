# Changelog

## [0.2.0] - 2026-01-17

### Added
- Database storage support via SQLModel (SQLite, PostgreSQL, MySQL)
- Pluggable storage backend architecture
- New `storage` and `database_url` parameters on FastScheduler
- `uv add fastscheduler --extras database` optional dependency

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
