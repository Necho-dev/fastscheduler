"""Base queue backend interface."""

from abc import ABC, abstractmethod
from typing import List, Optional

from ..models import Job


class QueueBackend(ABC):
    """
    Abstract base class for queue backends.
    
    Queue backends are responsible for managing the priority queue of jobs.
    They must support:
    - Adding jobs to the queue
    - Removing jobs from the queue
    - Peeking at the next job
    - Getting all jobs
    - Clearing the queue
    """

    @abstractmethod
    def push(self, job: Job) -> None:
        """Add a job to the queue."""
        pass

    @abstractmethod
    def pop(self) -> Optional[Job]:
        """
        Remove and return the job with the smallest next_run time.
        Returns None if queue is empty.
        """
        pass

    @abstractmethod
    def peek(self) -> Optional[Job]:
        """
        Return the job with the smallest next_run time without removing it.
        Returns None if queue is empty.
        """
        pass

    @abstractmethod
    def remove(self, job_id: str) -> bool:
        """
        Remove a job by ID from the queue.
        Returns True if job was found and removed, False otherwise.
        """
        pass

    @abstractmethod
    def get_all(self) -> List[Job]:
        """Get all jobs in the queue (order not guaranteed)."""
        pass

    @abstractmethod
    def clear(self) -> int:
        """
        Clear all jobs from the queue.
        Returns the number of jobs removed.
        """
        pass

    @abstractmethod
    def size(self) -> int:
        """Return the number of jobs in the queue."""
        pass

    @abstractmethod
    def update(self, job: Job) -> bool:
        """
        Update an existing job in the queue.
        Returns True if job was found and updated, False otherwise.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the queue backend and release resources."""
        pass
