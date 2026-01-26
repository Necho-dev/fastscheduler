"""Pytest configuration and shared fixtures"""

import pytest
from pathlib import Path

from fastscheduler import FastScheduler


def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "redis: marks tests that require Redis")
    config.addinivalue_line("markers", "cron: marks tests that require croniter")


@pytest.fixture
def temp_state_file(tmp_path):
    """Provide temporary state file path"""
    return tmp_path / "test_scheduler.json"


@pytest.fixture
def scheduler_with_sqlalchemy(tmp_path):
    """Create a scheduler with SQLAlchemy storage backend"""
    db_path = tmp_path / "test_scheduler.db"
    sched = FastScheduler(
        storage="sqlalchemy",
        database_url=f"sqlite:///{db_path.absolute()}",
        quiet=True,
        auto_start=False
    )
    yield sched
    if sched.running:
        sched.stop()


@pytest.fixture
def scheduler_with_heapq(tmp_path):
    """Create a scheduler with heapq queue backend"""
    state_file = tmp_path / "test_scheduler.json"
    sched = FastScheduler(
        state_file=str(state_file),
        queue="heapq",
        quiet=True,
        auto_start=False
    )
    yield sched
    if sched.running:
        sched.stop()


@pytest.fixture
def scheduler_with_redis(tmp_path):
    """Create a scheduler with Redis queue backend (if available)"""
    pytest.importorskip("redis")
    
    try:
        import redis
        r = redis.from_url("redis://localhost:6379/0")
        r.ping()
    except Exception:
        pytest.skip("Redis is not available")
    
    state_file = tmp_path / "test_scheduler.json"
    sched = FastScheduler(
        state_file=str(state_file),
        queue="redis",
        redis_url="redis://localhost:6379/0",
        queue_key="fastscheduler:test:queue",
        quiet=True,
        auto_start=False
    )
    yield sched
    if sched.running:
        sched.stop()
    # Clean up Redis queue
    try:
        r = redis.from_url("redis://localhost:6379/0")
        r.delete("fastscheduler:test:queue")
    except Exception:
        pass


