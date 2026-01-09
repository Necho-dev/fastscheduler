import asyncio
import importlib.resources
import json
import logging
from typing import TYPE_CHECKING, AsyncGenerator, Optional

try:
    from fastapi import APIRouter
    from fastapi.responses import HTMLResponse, StreamingResponse
except ImportError as e:
    raise ImportError(
        "FastAPI integration requires FastAPI. "
        "Install with: pip install fastscheduler[fastapi]"
    ) from e

if TYPE_CHECKING:
    from .main import FastScheduler

logger = logging.getLogger("fastscheduler")


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

    async def event_generator() -> AsyncGenerator[str, None]:
        """Generate SSE events for real-time updates"""
        while True:
            try:
                # Get current state
                stats = scheduler.get_statistics()
                jobs = scheduler.get_jobs()
                history = scheduler.get_history(limit=50)

                # Prepare data
                data = {
                    "running": scheduler.running,
                    "stats": stats,
                    "jobs": jobs,
                    "history": history,
                }

                # Send as SSE event
                yield f"data: {json.dumps(data)}\n\n"

                # Update every second
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                # Clean shutdown - don't log as error
                logger.debug("SSE connection closed by client")
                break
            except Exception as e:
                # Log the actual error with context
                logger.error(
                    f"Error in SSE event generator: {type(e).__name__}: {e}",
                    exc_info=True
                )
                # Wait before retrying to prevent tight error loops
                await asyncio.sleep(1)

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
    async def get_jobs():
        """Get all scheduled jobs"""
        return {"jobs": scheduler.get_jobs()}

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

    @router.get("/api/history")
    async def get_history(func_name: Optional[str] = None, limit: int = 50):
        """Get job history"""
        return {"history": scheduler.get_history(func_name, limit)}

    return router
