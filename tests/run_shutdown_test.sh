#!/bin/bash
# Quick test script for SSE graceful shutdown

echo "=========================================="
echo "FastScheduler SSE Shutdown Test"
echo "=========================================="
echo ""
echo "This test will:"
echo "  1. Start a FastAPI server with FastScheduler"
echo "  2. Open SSE dashboard at http://localhost:8001/scheduler/"
echo "  3. Wait for you to press Ctrl+C"
echo "  4. Verify graceful shutdown completes quickly"
echo ""
echo "Expected: Shutdown should complete in < 3 seconds"
echo "=========================================="
echo ""

cd "$(dirname "$0")/.."

# Check if dependencies are installed
if ! python -c "import fastapi, anyio, fastscheduler" 2>/dev/null; then
    echo "❌ Missing dependencies. Installing..."
    pip install -e ".[fastapi]"
fi

echo "Starting test server..."
echo ""

python tests/test_shutdown.py
