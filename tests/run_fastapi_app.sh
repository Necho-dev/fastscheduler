#!/bin/bash
# Run FastScheduler test application

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -e ".[fastapi,database,all]"

# Check if Redis is needed
if [ "$1" == "redis" ]; then
    echo "Using Redis queue backend..."
    export QUEUE_BACKEND=redis
    # Check if Redis is running
    if ! redis-cli ping > /dev/null 2>&1; then
        echo "Warning: Redis is not running. Starting Redis with Docker..."
        docker run -d -p 6379:6379 --name fastscheduler-redis redis:latest 2>/dev/null || \
        echo "Please start Redis manually: docker run -d -p 6379:6379 redis:latest"
    fi
else
    echo "Using Heapq queue backend (in-memory)..."
    export QUEUE_BACKEND=heapq
fi

# Run the application
echo "Starting FastAPI application..."
python tests/run_fastapi_app.py
