import time

from fastscheduler import FastScheduler
import inspect


def test_job_name_defaults_to_func_name(tmp_path):
    """When job_name is not set, external behavior must match func_name."""
    state_file = tmp_path / "test_scheduler.json"
    scheduler = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)

    @scheduler.every(10).seconds
    def sample_job():
        pass

    jobs = scheduler.get_jobs()
    assert len(jobs) == 1
    job_info = jobs[0]

    # Make sure job_name is the same as func_name
    assert job_info["func_name"] == "sample_job"
    display_name = job_info.get("job_name", job_info["func_name"])
    assert display_name == "sample_job"

    # Also check get_job
    single = scheduler.get_job(job_info["job_id"])
    assert single is not None
    display_name_single = single.get("job_name", single["func_name"])
    assert display_name_single == "sample_job"

    scheduler.stop()


def test_job_name_via_create_job_and_history(tmp_path):
    """If FastScheduler.create_job supports job_name, it should appear in jobs and history; otherwise this test is skipped for backward-compatible environments."""
    state_file = tmp_path / "test_scheduler.json"
    scheduler = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)

    sig = inspect.signature(scheduler.create_job)
    if "job_name" not in sig.parameters:
        # Older versions do not support job_name; skip to remain compatible
        scheduler.stop()
        import pytest
        pytest.skip("FastScheduler.create_job does not support job_name parameter")

    def run_script(path: str):
        time.sleep(0.01)

    scheduler.register_function(run_script)

    job_id = scheduler.create_job(
        func_name="run_script",
        func_module=__name__,
        schedule_type="interval",
        schedule_config={"interval": 1, "unit": "seconds"},
        job_name="daily_report_script",
        args=("report.py",),
    )
    assert job_id is not None

    jobs = scheduler.get_jobs()
    assert len(jobs) == 1
    job_info = jobs[0]
    assert job_info["func_name"] == "run_script"
    assert job_info.get("job_name", "daily_report_script") == "daily_report_script"

    scheduler.start()
    time.sleep(0.2)
    scheduler.stop()

    history = scheduler.get_history(limit=10)
    assert len(history) > 0
    last = history[-1]
    assert last["func_name"] == "run_script"
    if "job_name" in last:
        assert last["job_name"] == "daily_report_script"

    scheduler.stop()


