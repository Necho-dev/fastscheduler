"""Storage backends for FastScheduler persistence."""

from .base import StorageBackend
from .json_backend import JSONStorageBackend

__all__ = ["StorageBackend", "JSONStorageBackend"]


# Lazy import for SQLAlchemy backend to avoid requiring sqlalchemy as dependency
def get_sqlalchemy_backend():
    """Get SQLAlchemyStorageBackend class (requires sqlalchemy package)."""
    try:
        from .sqlalchemy_backend import SQLAlchemyStorageBackend

        return SQLAlchemyStorageBackend
    except ImportError as e:
        raise ImportError(
            "SQLAlchemy storage backend requires sqlalchemy. "
            "Install with: pip install fastscheduler[database]"
        ) from e


# Backward compatibility: keep sqlmodel backend available
def get_sqlmodel_backend():
    """Get SQLModelStorageBackend class (requires sqlmodel package).
    
    Deprecated: Use get_sqlalchemy_backend() instead.
    """
    try:
        from .sqlmodel_backend import SQLModelStorageBackend

        return SQLModelStorageBackend
    except ImportError as e:
        raise ImportError(
            "SQLModel storage backend requires sqlmodel. "
            "Install with: pip install fastscheduler[database]"
        ) from e
