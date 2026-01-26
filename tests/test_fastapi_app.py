"""
FastAPI test application for FastScheduler.

This application demonstrates:
- SQLAlchemy storage backend
- Heapq and Redis queue backends
- API-driven job creation
- Dashboard UI with i18n and theme switching
- Multiple scheduling types
"""

import asyncio
import logging
import sys
import time
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from fastscheduler import FastScheduler
    from fastscheduler.fastapi_integration import create_scheduler_routes
except ImportError:
    # Fallback: import from source
    from src.fastscheduler.main import FastScheduler
    from src.fastscheduler.fastapi_integration import create_scheduler_routes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="FastScheduler Test Application",
    description="Test application for FastScheduler with all new features",
    version="0.3.0"
)

# ==================== Test Task Functions ====================

def data_sync_task(task_name: str):
    """Data synchronization task."""
    logger.info(f"📊 [{task_name}] Data sync completed at {time.strftime('%H:%M:%S')}")


def notification_task(message: str):
    """Notification sending task."""
    logger.info(f"📧 [{message}] Notification sent at {time.strftime('%H:%M:%S')}")


def health_check_task():
    """System health check task."""
    logger.info(f"💚 Health check passed at {time.strftime('%H:%M:%S')}")


def backup_task(backup_type: str):
    """Backup task."""
    logger.info(f"💾 [{backup_type}] Backup completed at {time.strftime('%H:%M:%S')}")


def report_generation_task(report_name: str):
    """Report generation task."""
    logger.info(f"📄 [{report_name}] Report generated at {time.strftime('%H:%M:%S')}")


def cache_cleanup_task():
    """Cache cleanup task."""
    logger.info(f"🧹 Cache cleanup completed at {time.strftime('%H:%M:%S')}")


def metrics_collection_task():
    """Metrics collection task."""
    logger.info(f"📈 Metrics collected at {time.strftime('%H:%M:%S')}")


def email_digest_task(digest_type: str):
    """Email digest task."""
    logger.info(f"📬 [{digest_type}] Email digest sent at {time.strftime('%H:%M:%S')}")


def database_maintenance_task():
    """Database maintenance task."""
    logger.info(f"🔧 Database maintenance completed at {time.strftime('%H:%M:%S')}")


def api_monitoring_task(endpoint: str):
    """API monitoring task."""
    logger.info(f"🔍 [{endpoint}] API health check passed at {time.strftime('%H:%M:%S')}")


def log_rotation_task():
    """Log rotation task."""
    logger.info(f"📋 Log rotation completed at {time.strftime('%H:%M:%S')}")


def security_scan_task():
    """Security scan task."""
    logger.info(f"🔒 Security scan completed at {time.strftime('%H:%M:%S')}")


def one_time_task(task_name: str):
    """One-time execution task."""
    logger.info(f"🎯 [{task_name}] One-time task executed at {time.strftime('%H:%M:%S')}")


def delayed_cleanup_task():
    """Delayed cleanup task."""
    logger.info(f"🧹 Delayed cleanup completed at {time.strftime('%H:%M:%S')}")


def async_task_wrapper(delay: int = 1):
    """Async task wrapper."""
    async def _async():
        await asyncio.sleep(delay)
        logger.info(f"⚡ Async task completed after {delay} seconds")
    return _async()


def failing_task():
    """Task that fails for testing retries."""
    import random
    if random.random() < 0.7:  # 70% chance of failure
        raise ValueError("Random failure for testing retries")
    logger.info("Task succeeded!")


def long_running_task(duration: int = 5):
    """Long running task for testing."""
    logger.info(f"Starting long task (duration: {duration}s)")
    time.sleep(duration)
    logger.info("Long task completed")


# Initialize scheduler with SQLAlchemy storage
# Choose queue backend: "heapq" or "redis"
QUEUE_BACKEND = "heapq"  # Change to "redis" to test distributed queue
REDIS_URL = "redis://localhost:6379/0"

# Ensure tests directory exists
tests_dir = Path(__file__).parent
tests_dir.mkdir(exist_ok=True)
db_path = tests_dir / "test_scheduler.db"

if QUEUE_BACKEND == "redis":
    scheduler = FastScheduler(
        storage="sqlalchemy",
        database_url=f"sqlite:///{db_path.absolute()}",
        queue="redis",
        redis_url=REDIS_URL,
        queue_key="fastscheduler:test:queue",
        max_workers=5,
        quiet=False
    )
    logger.info(f"Using Redis queue backend: {REDIS_URL}")
else:
    scheduler = FastScheduler(
        storage="sqlalchemy",
        database_url=f"sqlite:///{db_path.absolute()}",
        queue="heapq",
        max_workers=5,
        quiet=False
    )
    logger.info("Using Heapq queue backend (in-memory)")


# Register all test functions
scheduler.register_function(data_sync_task)
scheduler.register_function(notification_task)
scheduler.register_function(health_check_task)
scheduler.register_function(backup_task)
scheduler.register_function(report_generation_task)
scheduler.register_function(cache_cleanup_task)
scheduler.register_function(metrics_collection_task)
scheduler.register_function(email_digest_task)
scheduler.register_function(database_maintenance_task)
scheduler.register_function(api_monitoring_task)
scheduler.register_function(log_rotation_task)
scheduler.register_function(security_scan_task)
scheduler.register_function(one_time_task)
scheduler.register_function(delayed_cleanup_task)
scheduler.register_function(failing_task)
scheduler.register_function(long_running_task)

# Register async function
async def async_task_wrapper(delay: int = 1):
    """Async task wrapper."""
    await asyncio.sleep(delay)
    logger.info(f"⚡ Async task completed after {delay} seconds")

scheduler.register_function(async_task_wrapper)

# Setup FastAPI routes
router = create_scheduler_routes(scheduler, prefix="/scheduler")
app.include_router(router)


@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with dashboard link."""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>FastScheduler Test Application</title>
        <meta charset="utf-8">
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                max-width: 800px;
                margin: 50px auto;
                padding: 20px;
                background: #f5f5f5;
            }}
            .container {{
                background: white;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            h1 {{ color: #333; }}
            .info {{ 
                background: #e3f2fd; 
                padding: 15px; 
                border-radius: 4px; 
                margin: 20px 0;
            }}
            .link {{
                display: inline-block;
                margin: 10px 0;
                padding: 10px 20px;
                background: #8b5cf6;
                color: white;
                text-decoration: none;
                border-radius: 4px;
            }}
            .link:hover {{ background: #7c3aed; }}
            code {{
                background: #f5f5f5;
                padding: 2px 6px;
                border-radius: 3px;
                font-family: 'Monaco', monospace;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>⚡ FastScheduler Test Application</h1>
            
            <div class="info">
                <h3>Configuration:</h3>
                <ul>
                    <li><strong>Storage Backend:</strong> <code>SQLAlchemy</code> (SQLite)</li>
                    <li><strong>Queue Backend:</strong> <code>{queue_backend}</code></li>
                    {redis_info}
                    <li><strong>Database:</strong> <code>tests/test_scheduler.db</code></li>
                </ul>
            </div>

            <h2>Quick Links:</h2>
            <a href="/scheduler/" class="link">📊 Dashboard (Real-time Monitoring)</a>
            <br>
            <a href="/docs" class="link">📚 API Documentation</a>
            <br>
            <a href="/redoc" class="link">📖 ReDoc Documentation</a>

            <h2>Features Tested:</h2>
            <ul>
                <li>✅ SQLAlchemy storage backend</li>
                <li>✅ {queue_backend} queue backend</li>
                <li>✅ API-driven job creation</li>
                <li>✅ Dashboard UI with i18n (English/中文)</li>
                <li>✅ Light/Dark theme switching</li>
                <li>✅ All scheduling types (interval, daily, weekly, hourly, cron, once)</li>
                <li>✅ Job management (pause, resume, cancel, run now)</li>
                <li>✅ <strong>Job groups</strong> for business isolation (data_sync, notification)</li>
                <li>✅ Group-based filtering and management</li>
            </ul>
            
            <h2>Test Coverage:</h2>
            <ul>
                <li><strong>Interval Scheduling</strong>: Multiple intervals (seconds, minutes, hours)</li>
                <li><strong>Daily Scheduling</strong>: Different times throughout the day</li>
                <li><strong>Weekly Scheduling</strong>: Various day combinations</li>
                <li><strong>Hourly Scheduling</strong>: Different minute offsets</li>
                <li><strong>Cron Scheduling</strong>: Complex cron expressions</li>
                <li><strong>Once Scheduling</strong>: One-time delayed execution tasks</li>
                <li><strong>Job Groups</strong>: Multiple groups with different tasks</li>
                <li><strong>Task Varieties</strong>: Different function names and parameters</li>
                <li><strong>Manual Execution</strong>: Test immediate job execution via API</li>
            </ul>
            
            <h2>Example Job Groups:</h2>
            <ul>
                <li><strong>data_sync</strong>: Data synchronization tasks
                    <ul>
                        <li>Interval: Every 30 seconds (data_sync_task)</li>
                        <li>Daily: 02:00 (backup_task)</li>
                        <li>Cron: Every 5 minutes (metrics_collection_task)</li>
                    </ul>
                </li>
                <li><strong>notification</strong>: Notification service tasks
                    <ul>
                        <li>Interval: Every 60 seconds (notification_task)</li>
                        <li>Hourly: At minute 0 (email_digest_task)</li>
                        <li>Weekly: Monday & Friday at 10:00 (report_generation_task)</li>
                    </ul>
                </li>
                <li><strong>maintenance</strong>: System maintenance tasks
                    <ul>
                        <li>Interval: Every 2 minutes (health_check_task)</li>
                        <li>Daily: 03:00 (cache_cleanup_task)</li>
                        <li>Daily: 04:00 (log_rotation_task)</li>
                        <li>Cron: Every 15 minutes (api_monitoring_task)</li>
                    </ul>
                </li>
                <li><strong>security</strong>: Security-related tasks
                    <ul>
                        <li>Daily: 01:00 (security_scan_task)</li>
                        <li>Weekly: Sunday at 00:00 (database_maintenance_task)</li>
                    </ul>
                </li>
                <li><strong>default</strong>: Default group tasks
                    <ul>
                        <li>Interval: Every 3 minutes (health_check_task)</li>
                        <li>Hourly: At minute 30 (metrics_collection_task)</li>
                        <li>Cron: Every hour at minute 45 (api_monitoring_task)</li>
                    </ul>
                </li>
                <li><strong>one_time</strong>: One-time execution tasks
                    <ul>
                        <li>Once: After 10 seconds (one_time_task)</li>
                        <li>Once: After 1 minute (delayed_cleanup_task)</li>
                        <li>Once: After 2 minutes (one_time_task)</li>
                    </ul>
                </li>
            </ul>

            <h2>Test Endpoints:</h2>
            <p>Use the API documentation at <a href="/docs">/docs</a> to:</p>
            <ul>
                <li>Create new scheduled jobs</li>
                <li>Update existing jobs</li>
                <li>Enable/disable jobs</li>
                <li>View job status and history</li>
            </ul>
        </div>
    </body>
    </html>
    """.format(
        queue_backend=QUEUE_BACKEND.upper(),
        redis_info=f'<li><strong>Redis URL:</strong> <code>{REDIS_URL}</code></li>' if QUEUE_BACKEND == "redis" else ""
    )


@app.on_event("startup")
async def startup_event():
    """Start the scheduler when the app starts."""
    logger.info("Starting FastScheduler...")
    scheduler.start()
    
    # Create comprehensive test jobs covering all scheduling types
    try:
        # ==================== Group 1: "data_sync" - 数据同步业务组 ====================
        logger.info("=" * 60)
        logger.info("Creating jobs for group: data_sync")
        logger.info("=" * 60)
        
        # Interval job - every 30 seconds
        scheduler.create_job(
            func_name="data_sync_task",
            func_module="tests.test_fastapi_app",
            schedule_type="interval",
            schedule_config={"interval": 30, "unit": "seconds"},
            args=("Real-time Sync",),
            max_retries=3,
            enabled=True,
            group="data_sync"
        )
        logger.info("✓ Interval: data_sync_task (every 30s)")
        
        # Daily job - at 02:00
        scheduler.create_job(
            func_name="backup_task",
            func_module="tests.test_fastapi_app",
            schedule_type="daily",
            schedule_config={"time": "02:00"},
            args=("Nightly Backup",),
            enabled=True,
            group="data_sync"
        )
        logger.info("✓ Daily: backup_task (02:00)")
        
        # Cron job - every 5 minutes
        scheduler.create_job(
            func_name="metrics_collection_task",
            func_module="tests.test_fastapi_app",
            schedule_type="cron",
            schedule_config={"expression": "*/5 * * * *"},
            enabled=True,
            group="data_sync"
        )
        logger.info("✓ Cron: metrics_collection_task (*/5 * * * *)")
        
        # ==================== Group 2: "notification" - 通知业务组 ====================
        logger.info("=" * 60)
        logger.info("Creating jobs for group: notification")
        logger.info("=" * 60)
        
        # Interval job - every 60 seconds
        scheduler.create_job(
            func_name="notification_task",
            func_module="tests.test_fastapi_app",
            schedule_type="interval",
            schedule_config={"interval": 60, "unit": "seconds"},
            args=("Push Notification",),
            max_retries=2,
            enabled=True,
            group="notification"
        )
        logger.info("✓ Interval: notification_task (every 60s)")
        
        # Hourly job - at minute 0
        scheduler.create_job(
            func_name="email_digest_task",
            func_module="tests.test_fastapi_app",
            schedule_type="hourly",
            schedule_config={"time": "0"},
            args=("Hourly Digest",),
            enabled=True,
            group="notification"
        )
        logger.info("✓ Hourly: email_digest_task (minute 0)")
        
        # Weekly job - Monday and Friday at 10:00
        scheduler.create_job(
            func_name="report_generation_task",
            func_module="tests.test_fastapi_app",
            schedule_type="weekly",
            schedule_config={"time": "10:00", "days": [0, 4]},  # Monday, Friday
            args=("Weekly Summary",),
            enabled=True,
            group="notification"
        )
        logger.info("✓ Weekly: report_generation_task (Mon, Fri 10:00)")
        
        # ==================== Group 3: "maintenance" - 系统维护组 ====================
        logger.info("=" * 60)
        logger.info("Creating jobs for group: maintenance")
        logger.info("=" * 60)
        
        # Interval job - every 2 minutes
        scheduler.create_job(
            func_name="health_check_task",
            func_module="tests.test_fastapi_app",
            schedule_type="interval",
            schedule_config={"interval": 2, "unit": "minutes"},
            enabled=True,
            group="maintenance"
        )
        logger.info("✓ Interval: health_check_task (every 2m)")
        
        # Daily job - at 03:00
        scheduler.create_job(
            func_name="cache_cleanup_task",
            func_module="tests.test_fastapi_app",
            schedule_type="daily",
            schedule_config={"time": "03:00"},
            enabled=True,
            group="maintenance"
        )
        logger.info("✓ Daily: cache_cleanup_task (03:00)")
        
        # Daily job - at 04:00
        scheduler.create_job(
            func_name="log_rotation_task",
            func_module="tests.test_fastapi_app",
            schedule_type="daily",
            schedule_config={"time": "04:00"},
            enabled=True,
            group="maintenance"
        )
        logger.info("✓ Daily: log_rotation_task (04:00)")
        
        # Cron job - every 15 minutes
        scheduler.create_job(
            func_name="api_monitoring_task",
            func_module="tests.test_fastapi_app",
            schedule_type="cron",
            schedule_config={"expression": "*/15 * * * *"},
            args=("/api/health",),
            enabled=True,
            group="maintenance"
        )
        logger.info("✓ Cron: api_monitoring_task (*/15 * * * *)")
        
        # ==================== Group 4: "security" - 安全组 ====================
        logger.info("=" * 60)
        logger.info("Creating jobs for group: security")
        logger.info("=" * 60)
        
        # Daily job - at 01:00
        scheduler.create_job(
            func_name="security_scan_task",
            func_module="tests.test_fastapi_app",
            schedule_type="daily",
            schedule_config={"time": "01:00"},
            enabled=True,
            group="security"
        )
        logger.info("✓ Daily: security_scan_task (01:00)")
        
        # Weekly job - Sunday at 00:00
        scheduler.create_job(
            func_name="database_maintenance_task",
            func_module="tests.test_fastapi_app",
            schedule_type="weekly",
            schedule_config={"time": "00:00", "days": [6]},  # Sunday
            enabled=True,
            group="security"
        )
        logger.info("✓ Weekly: database_maintenance_task (Sun 00:00)")
        
        # ==================== Group 5: "default" - 默认组 ====================
        logger.info("=" * 60)
        logger.info("Creating jobs for group: default")
        logger.info("=" * 60)
        
        # Interval job - every 3 minutes
        scheduler.create_job(
            func_name="health_check_task",
            func_module="tests.test_fastapi_app",
            schedule_type="interval",
            schedule_config={"interval": 3, "unit": "minutes"},
            enabled=True,
            group="default"
        )
        logger.info("✓ Interval: health_check_task (every 3m)")
        
        # Hourly job - at minute 30
        scheduler.create_job(
            func_name="metrics_collection_task",
            func_module="tests.test_fastapi_app",
            schedule_type="hourly",
            schedule_config={"time": "30"},
            enabled=True,
            group="default"
        )
        logger.info("✓ Hourly: metrics_collection_task (minute 30)")
        
        # Cron job - every hour at minute 45
        scheduler.create_job(
            func_name="api_monitoring_task",
            func_module="tests.test_fastapi_app",
            schedule_type="cron",
            schedule_config={"expression": "45 * * * *"},
            args=("/api/status",),
            enabled=True,
            group="default"
        )
        logger.info("✓ Cron: api_monitoring_task (45 * * * *)")
        
        # ==================== Group 6: "one_time" - 一次性任务组 ====================
        logger.info("=" * 60)
        logger.info("Creating jobs for group: one_time")
        logger.info("=" * 60)
        
        # Once job - execute after 10 seconds
        scheduler.create_job(
            func_name="one_time_task",
            func_module="tests.test_fastapi_app",
            schedule_type="once",
            schedule_config={"delay": 10, "unit": "seconds"},
            args=("Initial Setup",),
            enabled=True,
            group="one_time"
        )
        logger.info("✓ Once: one_time_task (after 10s)")
        
        # Once job - execute after 1 minute
        scheduler.create_job(
            func_name="delayed_cleanup_task",
            func_module="tests.test_fastapi_app",
            schedule_type="once",
            schedule_config={"delay": 1, "unit": "minutes"},
            enabled=True,
            group="one_time"
        )
        logger.info("✓ Once: delayed_cleanup_task (after 1m)")
        
        # Once job - execute after 2 minutes
        scheduler.create_job(
            func_name="one_time_task",
            func_module="tests.test_fastapi_app",
            schedule_type="once",
            schedule_config={"delay": 2, "unit": "minutes"},
            args=("Post-Startup Task",),
            enabled=True,
            group="one_time"
        )
        logger.info("✓ Once: one_time_task (after 2m)")
        
        # ==================== Summary ====================
        logger.info("=" * 60)
        groups = scheduler.get_groups()
        total_jobs = len(scheduler.get_jobs())
        logger.info(f"✓ Successfully created {total_jobs} jobs across {len(groups)} groups")
        logger.info(f"  Groups: {', '.join(groups)}")
        logger.info("=" * 60)
        
        # Log job breakdown by schedule type
        jobs = scheduler.get_jobs()
        schedule_types = {}
        for job in jobs:
            schedule_type = job.get('schedule', '').split()[0] if job.get('schedule') else 'unknown'
            schedule_types[schedule_type] = schedule_types.get(schedule_type, 0) + 1
        
        logger.info("Job breakdown by schedule type:")
        for sched_type, count in sorted(schedule_types.items()):
            logger.info(f"  - {sched_type}: {count} job(s)")
        
    except Exception as e:
        logger.error(f"Failed to create example jobs: {e}", exc_info=True)


@app.on_event("shutdown")
async def shutdown_event():
    """Stop the scheduler when the app shuts down."""
    logger.info("Stopping FastScheduler...")
    scheduler.stop(wait=True)
    logger.info("FastScheduler stopped")


if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting FastAPI test server...")
    logger.info(f"Queue backend: {QUEUE_BACKEND}")
    logger.info(f"Storage backend: SQLAlchemy (SQLite)")
    logger.info("Dashboard available at: http://localhost:8000/scheduler/")
    logger.info("API docs available at: http://localhost:8000/docs")
    
    uvicorn.run(
        "tests.test_fastapi_app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
