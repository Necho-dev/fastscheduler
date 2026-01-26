"""Heapq-based in-memory queue backend."""

import heapq
import logging
from typing import List, Optional

from ..models import Job
from .base import QueueBackend

logger = logging.getLogger("fastscheduler")


class HeapqQueueBackend(QueueBackend):
    """
    In-memory priority queue backend using Python's heapq.
    
    This is the default queue backend, providing fast in-memory operations.
    Jobs are stored in a heap structure, ordered by next_run time.
    """

    def __init__(self, quiet: bool = False):
        """
        Initialize the heapq queue backend.
        
        Args:
            quiet: If True, suppress log messages
        """
        self.quiet = quiet
        self._heap: List[Job] = []
        self._job_map: dict[str, int] = {}  # job_id -> index in heap
        
        if not quiet:
            logger.info("Heapq queue backend initialized")

    def push(self, job: Job) -> None:
        """Add a job to the queue."""
        heapq.heappush(self._heap, job)
        # Update job map - note: heap doesn't maintain stable indices
        # So we rebuild the map after each push
        self._rebuild_job_map()

    def pop(self) -> Optional[Job]:
        """Remove and return the job with the smallest next_run time."""
        if not self._heap:
            return None
        
        job = heapq.heappop(self._heap)
        self._rebuild_job_map()
        return job

    def peek(self) -> Optional[Job]:
        """Return the job with the smallest next_run time without removing it."""
        if not self._heap:
            return None
        return self._heap[0]

    def remove(self, job_id: str) -> bool:
        """
        Remove a job by ID from the queue.
        
        Note: This is O(n) operation as we need to find and remove the job,
        then rebuild the heap.
        """
        if job_id not in self._job_map:
            return False
        
        # Find and remove the job
        self._heap = [j for j in self._heap if j.job_id != job_id]
        heapq.heapify(self._heap)
        self._rebuild_job_map()
        return True

    def get_all(self) -> List[Job]:
        """Get all jobs in the queue."""
        return list(self._heap)

    def clear(self) -> int:
        """Clear all jobs from the queue."""
        count = len(self._heap)
        self._heap.clear()
        self._job_map.clear()
        return count

    def size(self) -> int:
        """Return the number of jobs in the queue."""
        return len(self._heap)

    def update(self, job: Job) -> bool:
        """
        Update an existing job in the queue.
        
        Note: This is O(n) operation. We remove the old job and add the updated one.
        """
        if job.job_id not in self._job_map:
            return False
        
        # Remove old job
        self.remove(job.job_id)
        # Add updated job
        self.push(job)
        return True

    def close(self) -> None:
        """Close the queue backend (no-op for in-memory backend)."""
        pass

    def _rebuild_job_map(self) -> None:
        """Rebuild the job_id to index mapping."""
        self._job_map = {job.job_id: i for i, job in enumerate(self._heap)}

    def heapify(self) -> None:
        """Rebuild the heap structure (useful after bulk modifications)."""
        heapq.heapify(self._heap)
        self._rebuild_job_map()
