"""Redis-based distributed queue backend."""

import json
import logging
import time
from typing import List, Optional

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None  # type: ignore

from ..models import Job, JobStatus
from .base import QueueBackend

logger = logging.getLogger("fastscheduler")


class RedisQueueBackend(QueueBackend):
    """
    Redis-based distributed queue backend.
    
    This backend uses Redis sorted sets (ZSET) to maintain a priority queue
    across multiple scheduler instances. The score is the next_run timestamp.
    
    Features:
    - Distributed: Multiple scheduler instances can share the same queue
    - Persistent: Jobs survive scheduler restarts
    - Atomic operations: Redis ensures thread-safe operations
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        queue_key: str = "fastscheduler:queue",
        lock_key: str = "fastscheduler:lock",
        lock_timeout: int = 30,
        quiet: bool = False,
    ):
        """
        Initialize the Redis queue backend.
        
        Args:
            redis_url: Redis connection URL (e.g., "redis://localhost:6379/0")
            queue_key: Redis key for the sorted set storing jobs
            lock_key: Redis key for distributed locking
            lock_timeout: Lock timeout in seconds
            quiet: If True, suppress log messages
        """
        if not REDIS_AVAILABLE:
            raise ImportError(
                "Redis queue backend requires redis package. "
                "Install with: pip install fastscheduler[redis]"
            )
        
        self.redis_url = redis_url
        self.queue_key = queue_key
        self.lock_key = lock_key
        self.lock_timeout = lock_timeout
        self.quiet = quiet
        
        # Parse Redis URL
        try:
            self.redis_client = redis.from_url(redis_url, decode_responses=False)
            # Test connection
            self.redis_client.ping()
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Redis at {redis_url}: {e}") from e
        
        if not quiet:
            logger.info(f"Redis queue backend initialized: {self._safe_url()}")

    def _safe_url(self) -> str:
        """Return Redis URL with password masked."""
        url = self.redis_url
        if "@" in url and "://" in url:
            pre, post = url.split("@", 1)
            if ":" in pre:
                proto_user = pre.rsplit(":", 1)[0]
                return f"{proto_user}:***@{post}"
        return url

    def _serialize_job(self, job: Job) -> bytes:
        """Serialize a Job to JSON bytes."""
        job_dict = job.to_dict()
        return json.dumps(job_dict).encode('utf-8')

    def _deserialize_job(self, data: bytes) -> Job:
        """Deserialize JSON bytes to a Job."""
        job_dict = json.loads(data.decode('utf-8'))
        return Job.from_dict(job_dict)

    def push(self, job: Job) -> None:
        """Add a job to the queue."""
        serialized = self._serialize_job(job)
        score = job.next_run
        
        # Use ZADD to add/update job in sorted set
        self.redis_client.zadd(self.queue_key, {serialized: score})

    def pop(self) -> Optional[Job]:
        """
        Remove and return the job with the smallest next_run time.
        
        Uses ZPOPMIN for non-blocking atomic pop operation.
        """
        try:
            # ZPOPMIN returns the element with the smallest score (non-blocking)
            result = self.redis_client.zpopmin(self.queue_key, count=1)
            if not result:
                return None
            
            # result is a list of tuples: [(serialized_bytes, score), ...]
            serialized, _ = result[0]
            return self._deserialize_job(serialized)
        except Exception as e:
            logger.error(f"Error popping from Redis queue: {e}")
            return None

    def peek(self) -> Optional[Job]:
        """Return the job with the smallest next_run time without removing it."""
        try:
            # ZRANGE with BYSCORE to get the job with minimum score
            result = self.redis_client.zrange(
                self.queue_key, 0, 0, withscores=True
            )
            if not result:
                return None
            
            serialized, _ = result[0]
            return self._deserialize_job(serialized)
        except Exception as e:
            logger.error(f"Error peeking from Redis queue: {e}")
            return None

    def remove(self, job_id: str) -> bool:
        """Remove a job by ID from the queue."""
        # We need to find the job first by scanning the sorted set
        # This is O(n) but necessary since we can't index by job_id
        try:
            # Get all jobs
            all_jobs = self.redis_client.zrange(self.queue_key, 0, -1)
            removed = False
            
            for serialized in all_jobs:
                try:
                    job = self._deserialize_job(serialized)
                    if job.job_id == job_id:
                        self.redis_client.zrem(self.queue_key, serialized)
                        removed = True
                        break
                except Exception:
                    continue
            
            return removed
        except Exception as e:
            logger.error(f"Error removing job from Redis queue: {e}")
            return False

    def get_all(self) -> List[Job]:
        """Get all jobs in the queue."""
        try:
            all_jobs = []
            serialized_jobs = self.redis_client.zrange(self.queue_key, 0, -1)
            
            for serialized in serialized_jobs:
                try:
                    job = self._deserialize_job(serialized)
                    all_jobs.append(job)
                except Exception as e:
                    logger.warning(f"Failed to deserialize job: {e}")
                    continue
            
            return all_jobs
        except Exception as e:
            logger.error(f"Error getting all jobs from Redis queue: {e}")
            return []

    def clear(self) -> int:
        """Clear all jobs from the queue."""
        try:
            count = self.redis_client.zcard(self.queue_key)
            self.redis_client.delete(self.queue_key)
            return count
        except Exception as e:
            logger.error(f"Error clearing Redis queue: {e}")
            return 0

    def size(self) -> int:
        """Return the number of jobs in the queue."""
        try:
            return self.redis_client.zcard(self.queue_key)
        except Exception as e:
            logger.error(f"Error getting queue size: {e}")
            return 0

    def update(self, job: Job) -> bool:
        """Update an existing job in the queue."""
        # Remove old job and add updated one
        removed = self.remove(job.job_id)
        if removed:
            self.push(job)
        return removed

    def close(self) -> None:
        """Close the Redis connection."""
        try:
            if hasattr(self, 'redis_client'):
                self.redis_client.close()
        except Exception as e:
            logger.warning(f"Error closing Redis connection: {e}")
