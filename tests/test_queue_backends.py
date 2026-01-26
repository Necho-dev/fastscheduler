"""Tests for queue backends (heapq and Redis)."""

import time

import pytest

from fastscheduler import FastScheduler


@pytest.fixture
def scheduler_heapq(tmp_path):
    """Create a scheduler with heapq backend"""
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
def scheduler_redis(tmp_path):
    """Create a scheduler with Redis backend (if Redis is available)"""
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


class TestHeapqQueueBackend:
    """Test heapq queue backend"""

    def test_heapq_initialization(self, scheduler_heapq):
        """Test heapq backend initialization"""
        assert scheduler_heapq._queue is not None
        assert scheduler_heapq._queue.__class__.__name__ == "HeapqQueueBackend"

    def test_heapq_job_creation(self, scheduler_heapq):
        """Test creating jobs with heapq backend"""
        @scheduler_heapq.every(10).seconds
        def test_job():
            pass
        
        assert len(scheduler_heapq.jobs) == 1
        assert scheduler_heapq.jobs[0].func_name == "test_job"

    def test_heapq_job_execution(self, scheduler_heapq):
        """Test job execution with heapq backend"""
        executed = []
        
        @scheduler_heapq.every(0.1).seconds
        def test_job():
            executed.append(time.time())
        
        scheduler_heapq.start()
        time.sleep(0.3)
        scheduler_heapq.stop()
        
        assert len(executed) >= 2

    def test_heapq_job_management(self, scheduler_heapq):
        """Test job management operations with heapq"""
        @scheduler_heapq.every(10).seconds
        def test_job():
            pass
        
        job_id = scheduler_heapq.jobs[0].job_id
        
        # Test pause
        scheduler_heapq.pause_job(job_id)
        assert scheduler_heapq.jobs[0].paused is True
        
        # Test resume
        scheduler_heapq.resume_job(job_id)
        assert scheduler_heapq.jobs[0].paused is False
        
        # Test cancel
        scheduler_heapq.cancel_job(job_id)
        assert len(scheduler_heapq.jobs) == 0


class TestRedisQueueBackend:
    """Test Redis queue backend"""

    def test_redis_initialization(self, scheduler_redis):
        """Test Redis backend initialization"""
        assert scheduler_redis._queue is not None
        assert scheduler_redis._queue.__class__.__name__ == "RedisQueueBackend"

    def test_redis_job_creation(self, scheduler_redis):
        """Test creating jobs with Redis backend"""
        def test_task():
            pass
        
        scheduler_redis.register_function(test_task)
        
        job_id = scheduler_redis.create_job(
            func_name="test_task",
            func_module="tests.test_queue_backends",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"}
        )
        
        assert job_id is not None
        assert len(scheduler_redis.jobs) == 1

    def test_redis_job_execution(self, scheduler_redis):
        """Test job execution with Redis backend"""
        executed = []
        
        def test_task():
            executed.append(time.time())
        
        scheduler_redis.register_function(test_task)
        
        scheduler_redis.create_job(
            func_name="test_task",
            func_module="tests.test_queue_backends",
            schedule_type="interval",
            schedule_config={"interval": 0.1, "unit": "seconds"}
        )
        
        scheduler_redis.start()
        time.sleep(0.3)
        scheduler_redis.stop()
        
        assert len(executed) >= 2

    def test_redis_job_persistence(self, scheduler_redis):
        """Test that jobs persist in Redis across scheduler instances"""
        def test_task():
            pass
        
        scheduler_redis.register_function(test_task)
        
        job_id = scheduler_redis.create_job(
            func_name="test_task",
            func_module="tests.test_queue_backends",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"}
        )
        
        # Create a new scheduler instance pointing to the same Redis queue
        import redis
        from pathlib import Path
        
        state_file = Path(scheduler_redis.state_file)
        scheduler2 = FastScheduler(
            state_file=str(state_file),
            queue="redis",
            redis_url="redis://localhost:6379/0",
            queue_key="fastscheduler:test:queue",
            quiet=True,
            auto_start=False
        )
        scheduler2.register_function(test_task)
        
        # Load state - jobs should be in Redis
        scheduler2._load_state()
        
        # Jobs should be available
        assert len(scheduler2.jobs) >= 1
        
        scheduler2.stop()
        
        # Clean up
        r = redis.from_url("redis://localhost:6379/0")
        r.delete("fastscheduler:test:queue")

    def test_redis_job_management(self, scheduler_redis):
        """Test job management operations with Redis"""
        def test_task():
            pass
        
        scheduler_redis.register_function(test_task)
        
        job_id = scheduler_redis.create_job(
            func_name="test_task",
            func_module="tests.test_queue_backends",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"}
        )
        
        # Test pause
        scheduler_redis.pause_job(job_id)
        jobs = scheduler_redis.get_jobs()
        assert jobs[0]["paused"] is True
        
        # Test resume
        scheduler_redis.resume_job(job_id)
        jobs = scheduler_redis.get_jobs()
        assert jobs[0]["paused"] is False
        
        # Test cancel
        scheduler_redis.cancel_job(job_id)
        assert len(scheduler_redis.jobs) == 0


class TestQueueBackendComparison:
    """Test comparison between queue backends"""

    def test_both_backends_support_same_operations(self, scheduler_heapq, scheduler_redis):
        """Test that both backends support the same operations"""
        def test_task():
            pass
        
        # Test heapq
        scheduler_heapq.register_function(test_task)
        job_id1 = scheduler_heapq.create_job(
            func_name="test_task",
            func_module="tests.test_queue_backends",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"}
        )
        assert job_id1 is not None
        
        # Test Redis
        scheduler_redis.register_function(test_task)
        job_id2 = scheduler_redis.create_job(
            func_name="test_task",
            func_module="tests.test_queue_backends",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"}
        )
        assert job_id2 is not None
        
        # Both should have jobs
        assert len(scheduler_heapq.jobs) == 1
        assert len(scheduler_redis.jobs) == 1
