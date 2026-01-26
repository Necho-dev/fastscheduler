"""Queue backends for FastScheduler."""

from .base import QueueBackend

__all__ = ["QueueBackend"]

# Lazy import for queue backends
def get_heapq_backend():
    """Get HeapqQueueBackend class."""
    from .heapq_backend import HeapqQueueBackend
    return HeapqQueueBackend

def get_redis_backend():
    """Get RedisQueueBackend class (requires redis package)."""
    try:
        from .redis_backend import RedisQueueBackend
        return RedisQueueBackend
    except ImportError as e:
        raise ImportError(
            "Redis queue backend requires redis package. "
            "Install with: pip install fastscheduler[redis]"
        ) from e
