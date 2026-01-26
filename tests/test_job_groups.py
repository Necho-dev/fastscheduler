"""Tests for job groups functionality."""

import time

import pytest

from fastscheduler import FastScheduler


@pytest.fixture
def scheduler(tmp_path):
    """Create a test scheduler"""
    state_file = tmp_path / "test_scheduler.json"
    sched = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)
    yield sched
    if sched.running:
        sched.stop()


class TestJobGroups:
    """Test job groups functionality"""

    def test_create_job_with_group(self, scheduler):
        """Test creating a job with a specific group"""
        def test_task():
            pass
        
        scheduler.register_function(test_task)
        
        job_id = scheduler.create_job(
            func_name="test_task",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"},
            group="test_group"
        )
        
        assert job_id is not None
        jobs = scheduler.get_jobs(group="test_group")
        assert len(jobs) == 1
        assert jobs[0]["group"] == "test_group"

    def test_default_group(self, scheduler):
        """Test jobs default to 'default' group"""
        @scheduler.every(10).seconds
        def test_job():
            pass
        
        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0]["group"] == "default"

    def test_get_groups(self, scheduler):
        """Test getting all groups"""
        def task1():
            pass
        
        def task2():
            pass
        
        scheduler.register_function(task1)
        scheduler.register_function(task2)
        
        scheduler.create_job(
            func_name="task1",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"},
            group="group1"
        )
        
        scheduler.create_job(
            func_name="task2",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 20, "unit": "seconds"},
            group="group2"
        )
        
        groups = scheduler.get_groups()
        assert "group1" in groups
        assert "group2" in groups
        assert "default" in groups

    def test_get_jobs_by_group(self, scheduler):
        """Test getting jobs filtered by group"""
        def task1():
            pass
        
        def task2():
            pass
        
        scheduler.register_function(task1)
        scheduler.register_function(task2)
        
        scheduler.create_job(
            func_name="task1",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"},
            group="group1"
        )
        
        scheduler.create_job(
            func_name="task2",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 20, "unit": "seconds"},
            group="group2"
        )
        
        group1_jobs = scheduler.get_jobs_by_group("group1")
        assert len(group1_jobs) == 1
        assert group1_jobs[0]["func_name"] == "task1"
        
        group2_jobs = scheduler.get_jobs_by_group("group2")
        assert len(group2_jobs) == 1
        assert group2_jobs[0]["func_name"] == "task2"

    def test_pause_group(self, scheduler):
        """Test pausing all jobs in a group"""
        def task1():
            pass
        
        def task2():
            pass
        
        scheduler.register_function(task1)
        scheduler.register_function(task2)
        
        job_id1 = scheduler.create_job(
            func_name="task1",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"},
            group="test_group"
        )
        
        job_id2 = scheduler.create_job(
            func_name="task2",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 20, "unit": "seconds"},
            group="test_group"
        )
        
        paused_count = scheduler.pause_group("test_group")
        assert paused_count == 2
        
        jobs = scheduler.get_jobs_by_group("test_group")
        assert all(job["paused"] for job in jobs)

    def test_resume_group(self, scheduler):
        """Test resuming all jobs in a group"""
        def task1():
            pass
        
        scheduler.register_function(task1)
        
        scheduler.create_job(
            func_name="task1",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"},
            group="test_group"
        )
        
        scheduler.pause_group("test_group")
        resumed_count = scheduler.resume_group("test_group")
        assert resumed_count == 1
        
        jobs = scheduler.get_jobs_by_group("test_group")
        assert not any(job["paused"] for job in jobs)

    def test_cancel_group(self, scheduler):
        """Test cancelling all jobs in a group"""
        def task1():
            pass
        
        def task2():
            pass
        
        scheduler.register_function(task1)
        scheduler.register_function(task2)
        
        scheduler.create_job(
            func_name="task1",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"},
            group="test_group"
        )
        
        scheduler.create_job(
            func_name="task2",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 20, "unit": "seconds"},
            group="test_group"
        )
        
        cancelled_count = scheduler.cancel_group("test_group")
        assert cancelled_count == 2
        
        jobs = scheduler.get_jobs_by_group("test_group")
        assert len(jobs) == 0

    def test_group_isolation(self, scheduler):
        """Test that groups are isolated from each other"""
        def task1():
            pass
        
        def task2():
            pass
        
        scheduler.register_function(task1)
        scheduler.register_function(task2)
        
        scheduler.create_job(
            func_name="task1",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"},
            group="group1"
        )
        
        scheduler.create_job(
            func_name="task2",
            func_module="tests.test_job_groups",
            schedule_type="interval",
            schedule_config={"interval": 20, "unit": "seconds"},
            group="group2"
        )
        
        # Pause group1 should not affect group2
        scheduler.pause_group("group1")
        
        group1_jobs = scheduler.get_jobs_by_group("group1")
        group2_jobs = scheduler.get_jobs_by_group("group2")
        
        assert all(job["paused"] for job in group1_jobs)
        assert not any(job["paused"] for job in group2_jobs)

    def test_decorator_with_group(self, scheduler):
        """Test decorator-based scheduling with groups"""
        @scheduler.every(10).seconds
        def task1():
            pass
        
        # Note: Decorators don't support group parameter directly
        # Groups are only available via API create_job
        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0]["group"] == "default"
