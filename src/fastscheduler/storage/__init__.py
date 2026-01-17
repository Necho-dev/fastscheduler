"""Storage backends for FastScheduler persistence."""

from .base import StorageBackend
from .json_backend import JSONStorageBackend

__all__ = ["StorageBackend", "JSONStorageBackend"]


# Lazy import for SQLModel backend to avoid requiring sqlmodel as dependency
def get_sqlmodel_backend():
    """Get SQLModelStorageBackend class (requires sqlmodel package)."""
    try:
        from .sqlmodel_backend import SQLModelStorageBackend

        return SQLModelStorageBackend
    except ImportError as e:
        raise ImportError(
            "SQLModel storage backend requires sqlmodel. "
            "Install with: pip install fastscheduler[database]"
        ) from e
