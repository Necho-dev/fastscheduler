#!/usr/bin/env python3
"""
Quick start script for FastScheduler test application.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

if __name__ == "__main__":
    import uvicorn
    
    # Check for queue backend argument
    queue_backend = os.environ.get("QUEUE_BACKEND", "heapq")
    
    if "--redis" in sys.argv or queue_backend == "redis":
        print("🚀 Starting FastScheduler Test App with Redis queue backend...")
        print("   Make sure Redis is running: redis-server or docker run -d -p 6379:6379 redis")
    else:
        print("🚀 Starting FastScheduler Test App with Heapq queue backend...")
    
    print("\n📊 Dashboard: http://localhost:8000/scheduler/")
    print("📚 API Docs: http://localhost:8000/docs")
    print("🏠 Home: http://localhost:8000/")
    print("\n" + "="*60 + "\n")
    
    uvicorn.run(
        "tests.test_fastapi_app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
