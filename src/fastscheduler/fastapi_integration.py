import asyncio
import importlib.resources
import json
import logging
from typing import TYPE_CHECKING, AsyncGenerator, Optional, List, Dict, Any

try:
    from fastapi import APIRouter, HTTPException, Body
    from fastapi.responses import HTMLResponse, StreamingResponse
    from pydantic import BaseModel, Field
except ImportError as e:
    raise ImportError(
        "FastAPI integration requires FastAPI. "
        "Install with: pip install fastscheduler[fastapi]"
    ) from e

if TYPE_CHECKING:
    from .main import FastScheduler

logger = logging.getLogger("fastscheduler")


# Pydantic models for API requests/responses
class JobCreateRequest(BaseModel):
    """Request model for creating a job."""

    func_name: str = Field(..., description="Function name (must be registered)")
    func_module: str = Field(..., description="Function module (must be registered)")
    schedule_type: str = Field(
        ..., description="Schedule type: interval/daily/weekly/hourly/cron/once"
    )
    schedule_config: Dict[str, Any] = Field(..., description="Schedule configuration")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    timeout: Optional[float] = Field(default=None, description="Maximum execution time in seconds")
    timezone: Optional[str] = Field(default=None, description="Timezone string (e.g., 'Asia/Shanghai')")
    enabled: bool = Field(default=True, description="Whether the job is enabled")
    args: List[Any] = Field(default_factory=list, description="Function arguments")
    kwargs: Dict[str, Any] = Field(default_factory=dict, description="Function keyword arguments")
    group: str = Field(default="default", description="Job group name for business isolation")


class JobUpdateRequest(BaseModel):
    """Request model for updating a job."""

    schedule_config: Optional[Dict[str, Any]] = Field(
        default=None, description="Updated schedule configuration"
    )
    max_retries: Optional[int] = Field(default=None, description="Updated max retries")
    timeout: Optional[float] = Field(default=None, description="Updated timeout")
    timezone: Optional[str] = Field(default=None, description="Updated timezone")
    enabled: Optional[bool] = Field(default=None, description="Whether job is enabled")


def _load_dashboard_template() -> str:
    """Load the dashboard HTML template from package resources."""
    try:
        # Python 3.9+ compatible way to read package resources
        files = importlib.resources.files("fastscheduler")
        template_path = files.joinpath("templates", "dashboard.html")
        return template_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"Failed to load dashboard template: {e}")
        # Return a minimal fallback template
        return """<!DOCTYPE html>
<html><head><title>FastScheduler</title></head>
<body style="background:#111;color:#fff;font-family:sans-serif;padding:40px;">
<h1>FastScheduler Dashboard</h1>
<p>Error loading template. Check logs for details.</p>
</body></html>"""


def create_scheduler_routes(scheduler: "FastScheduler", prefix: str = "/scheduler"):
    """
    Create FastAPI routes for scheduler management

    Usage:
        from fastapi import FastAPI
        from fastscheduler import FastScheduler
        from fastscheduler.fastapi_integration import create_scheduler_routes

        app = FastAPI()
        scheduler = FastScheduler()

        app.include_router(create_scheduler_routes(scheduler))

        scheduler.start()
    """
    router = APIRouter(prefix=prefix, tags=["scheduler"])

    async def interruptible_sleep(duration: float, check_interval: float = 0.1) -> bool:
        """
        Sleep for the specified duration, but check stop event periodically.
        Returns True if stopped, False if completed normally.
        """
        elapsed = 0.0
        while elapsed < duration:
            if scheduler._sse_stop_event.is_set():
                return True
            sleep_time = min(check_interval, duration - elapsed)
            await asyncio.sleep(sleep_time)
            elapsed += sleep_time
        return False

    async def event_generator() -> AsyncGenerator[str, None]:
        """Generate SSE events for real-time updates"""
        while True:
            try:
                # Check if scheduler has been stopped
                if scheduler._sse_stop_event.is_set():
                    logger.info("SSE connection closed due to scheduler stop")
                    break

                # Get current state
                stats = scheduler.get_statistics()
                jobs = scheduler.get_jobs()
                history = scheduler.get_history(limit=50)
                dead_letters = scheduler.get_dead_letters(limit=100)

                # Prepare data
                data = {
                    "running": scheduler.running,
                    "stats": stats,
                    "jobs": jobs,
                    "history": history,
                    "dead_letters": dead_letters,
                    "dead_letter_count": len(scheduler.dead_letters),
                }

                # Send as SSE event
                yield f"data: {json.dumps(data)}\n\n"

                # Use interruptible sleep to check stop event periodically
                stopped = await interruptible_sleep(1.0, check_interval=0.1)
                if stopped:
                    logger.info("SSE connection closed due to scheduler stop")
                    break

            except asyncio.CancelledError:
                # Clean shutdown - don't log as error
                logger.debug("SSE connection closed by client")
                break
            except Exception as e:
                # Log the actual error with context
                logger.error(
                    f"Error in SSE event generator: {type(e).__name__}: {e}",
                    exc_info=True,
                )
                # Check stop event before retrying
                if scheduler._sse_stop_event.is_set():
                    logger.info("SSE connection closed due to scheduler stop")
                    break
                # Use interruptible sleep for error recovery too
                stopped = await interruptible_sleep(1.0, check_interval=0.1)
                if stopped:
                    logger.info("SSE connection closed due to scheduler stop")
                    break

    @router.get("/events")
    async def events():
        """SSE endpoint for real-time updates"""
        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @router.get("/", response_class=HTMLResponse)
    async def dashboard():
        """Web dashboard for scheduler monitoring"""
        # Load and render template
        template = _load_dashboard_template()

        # Replace template variables
        html = template.replace("{{prefix}}", prefix)

        return html

    @router.get("/api/status")
    async def get_status():
        """Get scheduler status"""
        return {"running": scheduler.running, "statistics": scheduler.get_statistics()}

    @router.get("/api/jobs")
    async def get_jobs(group: Optional[str] = None):
        """Get all scheduled jobs, optionally filtered by group"""
        return {"jobs": scheduler.get_jobs(group=group)}

    @router.get("/api/jobs/{job_id}")
    async def get_job(job_id: str):
        """Get a specific job by ID"""
        job = scheduler.get_job(job_id)
        if job is None:
            return {"error": "Job not found", "job_id": job_id}
        return {"job": job}

    @router.post("/api/jobs/{job_id}/pause")
    async def pause_job(job_id: str):
        """Pause a scheduled job"""
        success = scheduler.pause_job(job_id)
        if success:
            return {"success": True, "message": f"Job {job_id} paused"}
        return {"success": False, "error": f"Job {job_id} not found"}

    @router.post("/api/jobs/{job_id}/resume")
    async def resume_job(job_id: str):
        """Resume a paused job"""
        success = scheduler.resume_job(job_id)
        if success:
            return {"success": True, "message": f"Job {job_id} resumed"}
        return {"success": False, "error": f"Job {job_id} not found"}

    @router.post("/api/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str):
        """Cancel and remove a scheduled job"""
        success = scheduler.cancel_job(job_id)
        if success:
            return {"success": True, "message": f"Job {job_id} cancelled"}
        return {"success": False, "error": f"Job {job_id} not found"}

    @router.post("/api/jobs/{job_id}/run")
    async def run_job_now(job_id: str):
        """Trigger immediate execution of a job"""
        success = scheduler.run_job_now(job_id)
        if success:
            return {"success": True, "message": f"Job {job_id} triggered"}
        return {"success": False, "error": f"Job {job_id} not found or already running"}

    @router.get("/api/history")
    async def get_history(func_name: Optional[str] = None, limit: int = 50):
        """Get job history"""
        return {"history": scheduler.get_history(func_name, limit)}

    @router.get("/api/dead-letters")
    async def get_dead_letters(limit: int = 100):
        """Get dead letter queue (failed jobs)"""
        return {
            "dead_letters": scheduler.get_dead_letters(limit),
            "total": len(scheduler.dead_letters),
        }

    @router.delete("/api/dead-letters")
    async def clear_dead_letters():
        """Clear all dead letter entries"""
        count = scheduler.clear_dead_letters()
        return {"success": True, "cleared": count}

    # ==================== Group Management API ====================

    @router.get("/api/groups")
    async def get_groups():
        """Get all job groups"""
        return {"groups": scheduler.get_groups()}

    @router.get("/api/groups/{group}/jobs")
    async def get_group_jobs(group: str):
        """Get all jobs in a specific group"""
        return {"jobs": scheduler.get_jobs_by_group(group), "group": group}

    @router.post("/api/groups/{group}/pause")
    async def pause_group_endpoint(group: str):
        """Pause all jobs in a group"""
        count = scheduler.pause_group(group)
        return {"success": True, "paused": count, "message": f"Paused {count} job(s) in group: {group}"}

    @router.post("/api/groups/{group}/resume")
    async def resume_group_endpoint(group: str):
        """Resume all paused jobs in a group"""
        count = scheduler.resume_group(group)
        return {"success": True, "resumed": count, "message": f"Resumed {count} job(s) in group: {group}"}

    @router.delete("/api/groups/{group}")
    async def cancel_group_endpoint(group: str):
        """Cancel all jobs in a group"""
        count = scheduler.cancel_group(group)
        return {"success": True, "cancelled": count, "message": f"Cancelled {count} job(s) in group: {group}"}

    # ==================== Task Management API ====================

    @router.post("/api/jobs", status_code=201)
    async def create_job(request: JobCreateRequest):
        """Create a new scheduled job"""
        try:
            job_id = scheduler.create_job(
                func_name=request.func_name,
                func_module=request.func_module,
                group=request.group,
                schedule_type=request.schedule_type,
                schedule_config=request.schedule_config,
                max_retries=request.max_retries,
                timeout=request.timeout,
                timezone=request.timezone,
                enabled=request.enabled,
                args=tuple(request.args),
                kwargs=request.kwargs,
            )

            if job_id:
                return {
                    "success": True,
                    "message": f"Job created successfully",
                    "job_id": job_id,
                }
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to create job. Make sure function {request.func_module}.{request.func_name} is registered.",
                )
        except Exception as e:
            logger.error(f"Error creating job: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to create job: {str(e)}")

    @router.put("/api/jobs/{job_id}")
    async def update_job(job_id: str, request: JobUpdateRequest):
        """Update an existing job"""
        try:
            success = scheduler.update_job(
                job_id=job_id,
                schedule_config=request.schedule_config,
                max_retries=request.max_retries,
                timeout=request.timeout,
                timezone=request.timezone,
                enabled=request.enabled,
            )

            if success:
                return {"success": True, "message": f"Job {job_id} updated successfully"}
            else:
                raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating job: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to update job: {str(e)}")

    @router.delete("/api/jobs/{job_id}")
    async def delete_job(job_id: str):
        """Delete a job (same as cancel, but clearer semantics)"""
        success = scheduler.cancel_job(job_id)
        if success:
            return {"success": True, "message": f"Job {job_id} deleted successfully"}
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    @router.post("/api/jobs/{job_id}/enable")
    async def enable_job(job_id: str):
        """Enable a job"""
        success = scheduler.enable_job(job_id)
        if success:
            return {"success": True, "message": f"Job {job_id} enabled"}
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    @router.post("/api/jobs/{job_id}/disable")
    async def disable_job(job_id: str):
        """Disable a job"""
        success = scheduler.disable_job(job_id)
        if success:
            return {"success": True, "message": f"Job {job_id} disabled"}
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return router
