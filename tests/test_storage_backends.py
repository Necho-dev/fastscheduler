"""Tests for storage backends."""

import time

import pytest

from fastscheduler import FastScheduler
from fastscheduler.storage import JSONStorageBackend, StorageBackend


class TestJSONStorageBackend:

    def test_initialization(self, tmp_path):
        state_file = tmp_path / "test_state.json"
        backend = JSONStorageBackend(state_file=str(state_file), quiet=True)

        assert backend.state_file == state_file
        assert backend._dead_letters_file == tmp_path / "test_state_dead_letters.json"

    def test_save_load_state(self, tmp_path):
        state_file = tmp_path / "test_state.json"
        backend = JSONStorageBackend(state_file=str(state_file), quiet=True)

        jobs = [
            {
                "job_id": "job_0",
                "func_name": "test_func",
                "func_module": "test_module",
                "next_run": time.time() + 100,
                "interval": 10.0,
                "repeat": True,
                "status": "scheduled",
                "created_at": time.time(),
            }
        ]
        history = [
            {
                "job_id": "job_0",
                "func_name": "test_func",
                "status": "completed",
                "timestamp": time.time(),
            }
        ]
        statistics = {"total_runs": 5, "total_failures": 1}

        backend.save_state(
            jobs=jobs,
            history=history,
            statistics=statistics,
            job_counter=1,
            scheduler_running=True,
        )

        assert state_file.exists()

        loaded = backend.load_state()
        assert loaded is not None
        assert len(loaded["jobs"]) == 1
        assert loaded["jobs"][0]["job_id"] == "job_0"
        assert loaded["_job_counter"] == 1
        assert loaded["statistics"]["total_runs"] == 5

    def test_dead_letters(self, tmp_path):
        state_file = tmp_path / "test_state.json"
        backend = JSONStorageBackend(state_file=str(state_file), quiet=True)

        dead_letters = [
            {
                "job_id": "job_0",
                "func_name": "failing_job",
                "status": "failed",
                "timestamp": time.time(),
                "error": "Test error",
            }
        ]

        backend.save_dead_letters(dead_letters, max_dead_letters=100)

        loaded = backend.load_dead_letters()
        assert len(loaded) == 1
        assert loaded[0]["error"] == "Test error"

        count = backend.clear_dead_letters()
        assert count == 1
        assert len(backend.load_dead_letters()) == 0

    def test_no_existing_state(self, tmp_path):
        state_file = tmp_path / "nonexistent.json"
        backend = JSONStorageBackend(state_file=str(state_file), quiet=True)

        loaded = backend.load_state()
        assert loaded is None


class TestSQLModelStorageBackend:

    @pytest.fixture
    def sqlmodel_backend(self, tmp_path):
        from fastscheduler.storage import get_sqlmodel_backend

        SQLModelStorageBackend = get_sqlmodel_backend()
        db_path = tmp_path / "test.db"
        backend = SQLModelStorageBackend(
            database_url=f"sqlite:///{db_path}",
            quiet=True,
        )
        yield backend
        backend.close()

    def test_initialization(self, sqlmodel_backend):
        assert sqlmodel_backend.engine is not None

    def test_save_load_state(self, sqlmodel_backend):
        jobs = [
            {
                "job_id": "job_0",
                "func_name": "test_func",
                "func_module": "test_module",
                "next_run": time.time() + 100,
                "interval": 10.0,
                "repeat": True,
                "status": "scheduled",
                "created_at": time.time(),
            }
        ]
        history = [
            {
                "job_id": "job_0",
                "func_name": "test_func",
                "status": "completed",
                "timestamp": time.time(),
            }
        ]
        statistics = {"total_runs": 5, "total_failures": 1}

        sqlmodel_backend.save_state(
            jobs=jobs,
            history=history,
            statistics=statistics,
            job_counter=1,
            scheduler_running=True,
        )

        loaded = sqlmodel_backend.load_state()

        assert loaded is not None
        assert len(loaded["jobs"]) == 1
        assert loaded["jobs"][0]["job_id"] == "job_0"
        assert loaded["_job_counter"] == 1
        assert loaded["statistics"]["total_runs"] == 5

    def test_dead_letters(self, sqlmodel_backend):
        dead_letters = [
            {
                "job_id": "job_0",
                "func_name": "failing_job",
                "status": "failed",
                "timestamp": time.time(),
                "error": "Test error",
            }
        ]

        sqlmodel_backend.save_dead_letters(dead_letters, max_dead_letters=100)

        loaded = sqlmodel_backend.load_dead_letters()
        assert len(loaded) == 1
        assert loaded[0]["error"] == "Test error"

        count = sqlmodel_backend.clear_dead_letters()
        assert count == 1
        assert len(sqlmodel_backend.load_dead_letters()) == 0

    def test_no_existing_state(self, tmp_path):
        from fastscheduler.storage import get_sqlmodel_backend

        SQLModelStorageBackend = get_sqlmodel_backend()

        db_path = tmp_path / "empty.db"
        backend = SQLModelStorageBackend(
            database_url=f"sqlite:///{db_path}",
            quiet=True,
        )

        loaded = backend.load_state()
        assert loaded is None
        backend.close()

    def test_job_update(self, sqlmodel_backend):
        jobs = [
            {
                "job_id": "job_0",
                "func_name": "test_func",
                "func_module": "test_module",
                "next_run": time.time() + 100,
                "interval": 10.0,
                "repeat": True,
                "status": "scheduled",
                "created_at": time.time(),
                "run_count": 0,
            }
        ]

        sqlmodel_backend.save_state(
            jobs=jobs,
            history=[],
            statistics={},
            job_counter=1,
            scheduler_running=True,
        )

        jobs[0]["run_count"] = 5
        jobs[0]["next_run"] = time.time() + 200

        sqlmodel_backend.save_state(
            jobs=jobs,
            history=[],
            statistics={},
            job_counter=1,
            scheduler_running=True,
        )

        loaded = sqlmodel_backend.load_state()
        assert loaded["jobs"][0]["run_count"] == 5

    def test_job_deletion(self, sqlmodel_backend):
        jobs = [
            {
                "job_id": "job_0",
                "func_name": "test_func_0",
                "func_module": "test_module",
                "next_run": time.time() + 100,
                "interval": 10.0,
                "repeat": True,
                "status": "scheduled",
                "created_at": time.time(),
            },
            {
                "job_id": "job_1",
                "func_name": "test_func_1",
                "func_module": "test_module",
                "next_run": time.time() + 100,
                "interval": 10.0,
                "repeat": True,
                "status": "scheduled",
                "created_at": time.time(),
            },
        ]

        sqlmodel_backend.save_state(
            jobs=jobs,
            history=[],
            statistics={},
            job_counter=2,
            scheduler_running=True,
        )

        sqlmodel_backend.save_state(
            jobs=[jobs[0]],
            history=[],
            statistics={},
            job_counter=2,
            scheduler_running=True,
        )

        loaded = sqlmodel_backend.load_state()
        assert len(loaded["jobs"]) == 1
        assert loaded["jobs"][0]["job_id"] == "job_0"

    def test_schedule_days_serialization(self, sqlmodel_backend):
        jobs = [
            {
                "job_id": "job_0",
                "func_name": "weekly_func",
                "func_module": "test_module",
                "next_run": time.time() + 100,
                "interval": None,
                "repeat": True,
                "status": "scheduled",
                "created_at": time.time(),
                "schedule_type": "weekly",
                "schedule_time": "09:00",
                "schedule_days": [0, 2, 4],
            }
        ]

        sqlmodel_backend.save_state(
            jobs=jobs,
            history=[],
            statistics={},
            job_counter=1,
            scheduler_running=True,
        )

        loaded = sqlmodel_backend.load_state()
        assert loaded["jobs"][0]["schedule_days"] == [0, 2, 4]


class TestSchedulerWithBackends:

    def test_default_json_backend(self, tmp_path):
        state_file = tmp_path / "scheduler.json"
        scheduler = FastScheduler(
            state_file=str(state_file),
            quiet=True,
            auto_start=False,
        )

        assert isinstance(scheduler._storage, JSONStorageBackend)
        scheduler.stop()

    def test_explicit_json_backend(self, tmp_path):
        state_file = tmp_path / "scheduler.json"
        scheduler = FastScheduler(
            state_file=str(state_file),
            storage="json",
            quiet=True,
            auto_start=False,
        )

        assert isinstance(scheduler._storage, JSONStorageBackend)
        scheduler.stop()

    def test_sqlmodel_backend(self, tmp_path):
        db_path = tmp_path / "scheduler.db"
        scheduler = FastScheduler(
            storage="sqlmodel",
            database_url=f"sqlite:///{db_path}",
            quiet=True,
            auto_start=False,
        )

        from fastscheduler.storage import get_sqlmodel_backend

        SQLModelStorageBackend = get_sqlmodel_backend()
        assert isinstance(scheduler._storage, SQLModelStorageBackend)
        scheduler.stop()

    def test_custom_backend(self, tmp_path):
        state_file = tmp_path / "custom.json"
        custom_backend = JSONStorageBackend(state_file=str(state_file), quiet=True)

        scheduler = FastScheduler(
            storage=custom_backend,
            quiet=True,
            auto_start=False,
        )

        assert scheduler._storage is custom_backend
        scheduler.stop()

    def test_invalid_backend(self):
        with pytest.raises(ValueError, match="Unknown storage backend"):
            FastScheduler(
                storage="invalid_backend",
                quiet=True,
                auto_start=False,
            )

    def test_persistence_with_sqlmodel(self, tmp_path):
        db_path = tmp_path / "scheduler.db"

        scheduler1 = FastScheduler(
            storage="sqlmodel",
            database_url=f"sqlite:///{db_path}",
            quiet=True,
            auto_start=False,
        )

        @scheduler1.every(10).seconds
        def test_job():
            pass

        scheduler1.start()
        time.sleep(0.2)
        scheduler1.stop()

        assert db_path.exists()

        scheduler2 = FastScheduler(
            storage="sqlmodel",
            database_url=f"sqlite:///{db_path}",
            quiet=True,
            auto_start=False,
        )

        assert len(scheduler2.history) > 0
        scheduler2.stop()

    def test_dead_letters_with_sqlmodel(self, tmp_path):
        from fastscheduler.main import JobStatus

        db_path = tmp_path / "scheduler.db"

        scheduler = FastScheduler(
            storage="sqlmodel",
            database_url=f"sqlite:///{db_path}",
            quiet=True,
            auto_start=False,
        )

        scheduler._log_history(
            job_id="job_1",
            func_name="failing_job",
            status=JobStatus.FAILED,
            error="Max retries: Test error",
        )

        assert len(scheduler.dead_letters) == 1

        dead_letters = scheduler.get_dead_letters()
        assert len(dead_letters) == 1
        assert dead_letters[0]["error"] == "Max retries: Test error"

        count = scheduler.clear_dead_letters()
        assert count == 1
        assert len(scheduler.get_dead_letters()) == 0

        scheduler.stop()


class TestStorageBackendInterface:

    def test_json_backend_is_storage_backend(self):
        assert issubclass(JSONStorageBackend, StorageBackend)

    def test_sqlmodel_backend_is_storage_backend(self):
        from fastscheduler.storage import get_sqlmodel_backend

        SQLModelStorageBackend = get_sqlmodel_backend()
        assert issubclass(SQLModelStorageBackend, StorageBackend)

    def test_close_is_safe(self, tmp_path):
        state_file = tmp_path / "test.json"
        backend = JSONStorageBackend(state_file=str(state_file), quiet=True)

        backend.close()
        backend.close()
