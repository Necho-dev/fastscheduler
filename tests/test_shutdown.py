#!/usr/bin/env python3
"""
Simple FastAPI test application to verify SSE graceful shutdown.

Usage:
    python tests/test_shutdown.py

Then:
    1. Open http://localhost:8001/scheduler/ in your browser
    2. Watch the SSE connection establish and data streaming
    3. Press Ctrl+C in the terminal
    4. Verify:
       - Browser console shows "Server is shutting down, closing connection..."
       - SSE connection closes immediately
       - Uvicorn completes shutdown within 2-3 seconds
"""

import asyncio
import logging
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    import anyio
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse
    
    from fastscheduler import FastScheduler
    from fastscheduler.fastapi_integration import create_scheduler_routes
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("Install with: pip install fastscheduler[fastapi]")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Test task functions
def simple_task():
    """Simple test task that runs every 10 seconds."""
    logger.info(f"✓ Simple task executed at {time.strftime('%H:%M:%S')}")


def heartbeat_task():
    """Heartbeat task every 5 seconds."""
    logger.info(f"💓 Heartbeat at {time.strftime('%H:%M:%S')}")


# Initialize scheduler
tests_dir = Path(__file__).parent
db_path = tests_dir / "test_shutdown.db"

scheduler = FastScheduler(
    storage="sqlalchemy",
    database_url=f"sqlite:///{db_path.absolute()}",
    queue="heapq",
    max_workers=2,
    quiet=False
)

# Register test functions
scheduler.register_function(simple_task)
scheduler.register_function(heartbeat_task)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan with proper SSE shutdown coordination."""
    logger.info("=" * 60)
    logger.info("Starting FastScheduler Test Application")
    logger.info("=" * 60)
    
    # Install signal handlers AFTER Uvicorn has installed its handlers
    # This ensures we can properly chain to Uvicorn's handler
    from fastscheduler.fastapi_integration import install_shutdown_handlers
    install_shutdown_handlers(scheduler)
    logger.info("✓ Signal handlers installed")
    
    # Start scheduler
    scheduler.start()
    logger.info("✓ FastScheduler started")
    
    # Create test jobs
    try:
        # Simple task every 10 seconds
        scheduler.create_job(
            func_name="simple_task",
            func_module="tests.test_shutdown",
            schedule_type="interval",
            schedule_config={"interval": 10, "unit": "seconds"},
            enabled=True,
            group="test"
        )
        logger.info("✓ Created simple_task (every 10s)")
        
        # Heartbeat task every 5 seconds
        scheduler.create_job(
            func_name="heartbeat_task",
            func_module="tests.test_shutdown",
            schedule_type="interval",
            schedule_config={"interval": 5, "unit": "seconds"},
            enabled=True,
            group="test"
        )
        logger.info("✓ Created heartbeat_task (every 5s)")
        
    except Exception as e:
        logger.error(f"Failed to create test jobs: {e}", exc_info=True)
    
    logger.info("=" * 60)
    logger.info("Dashboard: http://localhost:8001/scheduler/")
    logger.info("Press Ctrl+C to test graceful shutdown")
    logger.info("=" * 60)
    
    # Application running phase
    yield
    
    # === Shutdown phase ===
    logger.info("=" * 60)
    logger.info("Starting graceful shutdown...")
    logger.info("=" * 60)
    
    try:
        # Note: scheduler.shutdown_connection() is automatically called by the signal handler
        # installed in create_scheduler_routes(), so we don't need to call it here.
        # This happens BEFORE Uvicorn starts waiting for connections to close.
        
        # Give a moment for SSE shutdown event to be sent and frontend to close
        logger.info("Waiting briefly for SSE connections to close...")
        try:
            await anyio.sleep(0.5)
        except Exception:
            pass
        logger.info("✓ SSE shutdown grace period completed")
        
        # Stop scheduler resources (non-blocking)
        logger.info("Stopping scheduler resources...")
        scheduler.stop(wait=False)
        logger.info("✓ Scheduler stopped")
        
        logger.info("=" * 60)
        logger.info("Graceful shutdown completed successfully")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}", exc_info=True)


# Create FastAPI app
app = FastAPI(
    title="FastScheduler Shutdown Test",
    description="Test application for SSE graceful shutdown",
    version="0.1.0",
    lifespan=lifespan
)

# Register scheduler routes (don't install signal handlers yet - we'll do it in lifespan)
prefix = "/scheduler"
scheduler_router = create_scheduler_routes(scheduler, prefix=prefix, install_signal_handlers=False)
app.include_router(scheduler_router)


@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with test instructions."""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>FastScheduler Shutdown Test</title>
        <meta charset="utf-8">
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                max-width: 900px;
                margin: 50px auto;
                padding: 20px;
                background: #f5f5f5;
            }
            .container {
                background: white;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            h1 { color: #333; }
            .test-steps {
                background: #e3f2fd;
                padding: 20px;
                border-radius: 4px;
                margin: 20px 0;
            }
            .test-steps ol {
                margin: 10px 0;
                padding-left: 20px;
            }
            .test-steps li {
                margin: 8px 0;
            }
            .link {
                display: inline-block;
                margin: 10px 0;
                padding: 12px 24px;
                background: #8b5cf6;
                color: white;
                text-decoration: none;
                border-radius: 4px;
                font-weight: 500;
            }
            .link:hover { background: #7c3aed; }
            .expected {
                background: #f0f9ff;
                border-left: 4px solid #0ea5e9;
                padding: 15px;
                margin: 20px 0;
            }
            .expected h3 {
                margin-top: 0;
                color: #0369a1;
            }
            code {
                background: #f5f5f5;
                padding: 2px 6px;
                border-radius: 3px;
                font-family: 'Monaco', monospace;
                color: #e11d48;
            }
            .warning {
                background: #fef3c7;
                border-left: 4px solid #f59e0b;
                padding: 15px;
                margin: 20px 0;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🧪 FastScheduler SSE Graceful Shutdown Test</h1>
            
            <div class="test-steps">
                <h2>Test Steps:</h2>
                <ol>
                    <li>Click the Dashboard link below to open the monitoring page</li>
                    <li>Observe the SSE connection establish (status shows "Live")</li>
                    <li>Watch real-time data streaming (job status, statistics)</li>
                    <li>Open browser DevTools Console (F12)</li>
                    <li>Go back to the terminal where this app is running</li>
                    <li>Press <code>Ctrl+C</code> to trigger shutdown</li>
                </ol>
            </div>

            <a href="/scheduler/" class="link" target="_blank">
                📊 Open Dashboard (SSE Test Page)
            </a>

            <div class="expected">
                <h3>✅ Expected Behavior:</h3>
                <ul>
                    <li><strong>Browser Console:</strong> Should display "Server is shutting down, closing connection..."</li>
                    <li><strong>SSE Status:</strong> Should change from "Live" to "Offline" immediately</li>
                    <li><strong>No Reconnection:</strong> EventSource should NOT attempt to reconnect</li>
                    <li><strong>Terminal:</strong> Should show shutdown logs completing within 2-3 seconds</li>
                    <li><strong>Uvicorn:</strong> Should log "Application shutdown complete" quickly</li>
                </ul>
            </div>

            <div class="warning">
                <h3>⚠️ What We're Testing:</h3>
                <p>This test verifies that when <code>Ctrl+C</code> is pressed:</p>
                <ul>
                    <li>The scheduler sends a <code>shutdown</code> event via SSE</li>
                    <li>The frontend receives it and actively closes the EventSource</li>
                    <li>Uvicorn doesn't get stuck waiting for the SSE connection to timeout</li>
                    <li>The entire shutdown process completes quickly (< 3 seconds)</li>
                </ul>
            </div>

            <h2>Test Jobs:</h2>
            <ul>
                <li><code>simple_task</code> - Runs every 10 seconds</li>
                <li><code>heartbeat_task</code> - Runs every 5 seconds</li>
            </ul>

            <h2>Technical Details:</h2>
            <ul>
                <li><strong>Storage:</strong> SQLAlchemy (SQLite)</li>
                <li><strong>Queue:</strong> Heapq (in-memory)</li>
                <li><strong>SSE Endpoint:</strong> <code>/scheduler/events</code></li>
                <li><strong>Shutdown Signal:</strong> <code>event: shutdown</code></li>
            </ul>
        </div>
    </body>
    </html>
    """


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "scheduler_running": scheduler.running,
        "shutdown_requested": scheduler.is_shutdown_requested()
    }


if __name__ == "__main__":
    import uvicorn
    
    print("\n" + "=" * 60)
    print("🧪 FastScheduler SSE Graceful Shutdown Test")
    print("=" * 60)
    print("Starting test server on http://localhost:8001")
    print("Dashboard: http://localhost:8001/scheduler/")
    print("=" * 60)
    print("\nTest Instructions:")
    print("1. Open the dashboard in your browser")
    print("2. Wait for SSE connection to establish")
    print("3. Press Ctrl+C here in the terminal")
    print("4. Check browser console and observe shutdown behavior")
    print("=" * 60 + "\n")
    
    uvicorn.run(
        "tests.test_shutdown:app",
        host="0.0.0.0",
        port=8001,
        reload=False,  # Don't use reload for this test
        log_level="info"
    )
