import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from fastscheduler import FastScheduler
from fastscheduler.fastapi_integration import create_scheduler_routes


@pytest.fixture
def scheduler(tmp_path):
    """Create a test scheduler"""
    state_file = tmp_path / "test_scheduler.json"
    sched = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)

    # Add a test job
    @sched.every(10).seconds
    def test_job():
        pass

    yield sched
    if sched.running:
        sched.stop()


@pytest.fixture
def app(scheduler):
    """Create FastAPI app with scheduler routes"""
    app = FastAPI()
    app.include_router(create_scheduler_routes(scheduler))
    return app


@pytest.fixture
def client(app):
    """Create test client"""
    return TestClient(app)


# Helper function to get job data via API
def get_jobs_api(client):
    """Get jobs via the API endpoint"""
    response = client.get("/scheduler/api/jobs")
    return response.json()["jobs"]


class TestSchedulerRoutes:
    """Test FastAPI scheduler routes"""

    def test_dashboard_endpoint(self, client):
        """Test dashboard endpoint returns HTML"""
        response = client.get("/scheduler/")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert b"FastScheduler" in response.content

    def test_dashboard_shows_jobs(self, client, scheduler):
        """Test dashboard has SSE connection for jobs (client-side rendering)"""
        response = client.get("/scheduler/")
        assert response.status_code == 200

        content = response.content.decode()
        # Dashboard uses SSE for real-time job data
        assert "EventSource" in content
        assert "/events" in content
        
        # Verify jobs are available via API
        jobs = get_jobs_api(client)
        assert len(jobs) == 1
        assert jobs[0]["func_name"] == "test_job"
        assert "Every 10 seconds" in jobs[0]["schedule"]

    def test_dashboard_shows_status(self, client, scheduler):
        """Test dashboard shows scheduler status via API"""
        # Test stopped status via API
        response = client.get("/scheduler/api/status")
        assert response.status_code == 200
        data = response.json()
        assert data["running"] is False

        # Test running status via API
        scheduler.start()
        response = client.get("/scheduler/api/status")
        data = response.json()
        assert data["running"] is True
        scheduler.stop()

    def test_events_endpoint_exists(self, app):
        """Test SSE events endpoint exists"""
        # Verify the route is registered (don't actually consume the infinite stream)
        routes = [route.path for route in app.routes]
        assert "/scheduler/events" in routes

    def test_custom_prefix(self, scheduler):
        """Test custom route prefix"""
        app = FastAPI()
        app.include_router(create_scheduler_routes(scheduler, prefix="/custom"))
        client = TestClient(app)

        response = client.get("/custom/")
        assert response.status_code == 200

    def test_dashboard_with_history(self, client, scheduler):
        """Test dashboard shows execution history"""
        scheduler.start()
        time.sleep(0.2)
        scheduler.stop()

        response = client.get("/scheduler/")
        content = response.content.decode()

        # Should show some history or stats
        assert "Statistics" in content or "Jobs" in content

    def test_dashboard_styling(self, client):
        """Test dashboard has proper styling"""
        response = client.get("/scheduler/")
        content = response.content.decode()

        # Check for CSS and styling elements
        assert "<style>" in content
        assert "background" in content
        assert "color" in content


class TestIntegrationScenarios:
    """Test complete integration scenarios"""

    def test_full_app_lifecycle(self, tmp_path):
        """Test complete app lifecycle with scheduler"""
        app = FastAPI()
        state_file = tmp_path / "test_scheduler.json"
        scheduler = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)

        executed = []

        @scheduler.every(0.1).seconds
        def background_task():
            executed.append(time.time())

        app.include_router(create_scheduler_routes(scheduler))
        client = TestClient(app)

        # Start scheduler
        scheduler.start()

        # Access dashboard while running
        response = client.get("/scheduler/")
        assert response.status_code == 200

        # Let jobs execute
        time.sleep(0.3)

        # Check jobs ran
        assert len(executed) >= 2

        # Stop scheduler
        scheduler.stop()

        # Dashboard should still be accessible
        response = client.get("/scheduler/")
        assert response.status_code == 200

    def test_multiple_jobs_display(self, tmp_path):
        """Test multiple jobs are available via API"""
        app = FastAPI()
        state_file = tmp_path / "test_scheduler.json"
        scheduler = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)

        @scheduler.every(5).seconds
        def job1():
            pass

        @scheduler.every(10).minutes
        def job2():
            pass

        @scheduler.daily.at("14:30")
        async def job3():
            pass

        app.include_router(create_scheduler_routes(scheduler))
        client = TestClient(app)

        # Verify dashboard loads
        response = client.get("/scheduler/")
        assert response.status_code == 200

        # Verify all jobs via API
        jobs = get_jobs_api(client)
        job_names = [j["func_name"] for j in jobs]
        
        assert "job1" in job_names
        assert "job2" in job_names
        assert "job3" in job_names

        scheduler.stop()

    def test_async_jobs_with_fastapi(self, tmp_path):
        """Test async jobs work with FastAPI integration"""
        app = FastAPI()
        state_file = tmp_path / "test_scheduler.json"
        scheduler = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)

        executed = []

        @scheduler.every(0.1).seconds
        async def async_job():
            executed.append(1)

        app.include_router(create_scheduler_routes(scheduler))

        scheduler.start()
        time.sleep(0.3)
        scheduler.stop()

        assert len(executed) >= 2


class TestErrorHandling:
    """Test error handling in FastAPI integration"""

    def test_scheduler_not_running(self, client, scheduler):
        """Test endpoints work when scheduler is not running"""
        response = client.get("/scheduler/")
        assert response.status_code == 200

    def test_no_jobs_scheduled(self, tmp_path):
        """Test dashboard with no jobs"""
        app = FastAPI()
        state_file = tmp_path / "test_scheduler.json"
        scheduler = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)
        app.include_router(create_scheduler_routes(scheduler))
        client = TestClient(app)

        response = client.get("/scheduler/")
        assert response.status_code == 200

        scheduler.stop()

    def test_dashboard_with_failed_jobs(self, client, scheduler):
        """Test dashboard displays failed jobs"""

        @scheduler.every(0.1).seconds.retries(1)
        def failing_job():
            raise ValueError("Test error")

        scheduler.start()
        time.sleep(0.3)
        scheduler.stop()

        response = client.get("/scheduler/")
        assert response.status_code == 200
        # Dashboard should handle failed jobs gracefully


class TestDashboardContent:
    """Test specific dashboard content elements"""

    def test_job_metadata_display(self, client, scheduler):
        """Test job metadata is available via API"""
        # Verify dashboard loads with SSE structure
        response = client.get("/scheduler/")
        content = response.content.decode()
        assert "EventSource" in content
        
        # Verify job metadata via API
        jobs = get_jobs_api(client)
        assert len(jobs) == 1
        job = jobs[0]
        
        # Should show job details
        assert job["func_name"] == "test_job"
        # Should show timing info
        assert "second" in job["schedule"].lower() or "minute" in job["schedule"].lower()

    def test_statistics_display(self, client, scheduler):
        """Test statistics are displayed"""
        scheduler.start()
        time.sleep(0.2)
        scheduler.stop()

        response = client.get("/scheduler/")
        content = response.content.decode()

        # Should show some statistics
        assert "0" in content or "1" in content  # Some numeric stats

    def test_responsive_design(self, client):
        """Test dashboard has responsive design elements"""
        response = client.get("/scheduler/")
        content = response.content.decode()

        # Check for viewport meta tag
        assert "viewport" in content or "width" in content


class TestJobActionEndpoints:
    """Test job action API endpoints (pause/resume/cancel)"""

    def test_pause_job_endpoint(self, client, scheduler):
        """Test pause job API endpoint"""
        job_id = scheduler.jobs[0].job_id
        
        response = client.post(f"/scheduler/api/jobs/{job_id}/pause")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        
        # Verify job is paused
        assert scheduler.jobs[0].paused is True

    def test_resume_job_endpoint(self, client, scheduler):
        """Test resume job API endpoint"""
        job_id = scheduler.jobs[0].job_id
        scheduler.pause_job(job_id)
        
        response = client.post(f"/scheduler/api/jobs/{job_id}/resume")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        
        # Verify job is resumed
        assert scheduler.jobs[0].paused is False

    def test_cancel_job_endpoint(self, client, scheduler):
        """Test cancel job API endpoint"""
        job_id = scheduler.jobs[0].job_id
        
        response = client.post(f"/scheduler/api/jobs/{job_id}/cancel")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        
        # Verify job is cancelled
        assert len(scheduler.jobs) == 0

    def test_pause_nonexistent_job(self, client):
        """Test pausing a nonexistent job"""
        response = client.post("/scheduler/api/jobs/nonexistent/pause")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False

    def test_get_single_job_endpoint(self, client, scheduler):
        """Test get single job API endpoint"""
        job_id = scheduler.jobs[0].job_id
        
        response = client.get(f"/scheduler/api/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["job"]["job_id"] == job_id
        assert data["job"]["func_name"] == "test_job"

    def test_get_nonexistent_job(self, client):
        """Test getting a nonexistent job"""
        response = client.get("/scheduler/api/jobs/nonexistent")
        assert response.status_code == 200
        data = response.json()
        assert "error" in data


class TestDeadLetterAPI:
    """Test dead letter queue API endpoints"""

    @pytest.fixture
    def client_with_dead_letters(self, tmp_path):
        """Create a client with scheduler that has dead letters"""
        from fastscheduler.main import JobStatus

        app = FastAPI()
        state_file = tmp_path / "test_scheduler.json"
        scheduler = FastScheduler(state_file=str(state_file), quiet=True, auto_start=False)
        app.include_router(create_scheduler_routes(scheduler))

        # Add some dead letters
        scheduler._log_history(
            job_id="job_1",
            func_name="failing_job_1",
            status=JobStatus.FAILED,
            error="Max retries: Error 1",
        )
        scheduler._log_history(
            job_id="job_2",
            func_name="failing_job_2",
            status=JobStatus.FAILED,
            error="Max retries: Error 2",
        )

        client = TestClient(app)
        yield client, scheduler
        scheduler.stop()

    def test_get_dead_letters(self, client_with_dead_letters):
        """Test getting dead letters via API"""
        client, scheduler = client_with_dead_letters

        response = client.get("/scheduler/api/dead-letters")
        assert response.status_code == 200
        data = response.json()
        assert "dead_letters" in data
        assert "total" in data
        assert data["total"] == 2
        assert len(data["dead_letters"]) == 2

    def test_get_dead_letters_with_limit(self, client_with_dead_letters):
        """Test getting dead letters with limit parameter"""
        client, scheduler = client_with_dead_letters

        response = client.get("/scheduler/api/dead-letters?limit=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data["dead_letters"]) == 1
        assert data["total"] == 2  # Total count should still be 2

    def test_clear_dead_letters(self, client_with_dead_letters):
        """Test clearing dead letters via API"""
        client, scheduler = client_with_dead_letters

        # Verify we have dead letters
        assert len(scheduler.dead_letters) == 2

        response = client.delete("/scheduler/api/dead-letters")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["cleared"] == 2

        # Verify they're cleared
        assert len(scheduler.dead_letters) == 0

    def test_empty_dead_letters(self, client, scheduler):
        """Test getting dead letters when queue is empty"""
        response = client.get("/scheduler/api/dead-letters")
        assert response.status_code == 200
        data = response.json()
        assert data["dead_letters"] == []
        assert data["total"] == 0

