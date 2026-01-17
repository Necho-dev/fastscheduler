"""Abstract base class for storage backends."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class StorageBackend(ABC):
    """Base class for scheduler storage backends."""

    @abstractmethod
    def save_state(
        self,
        jobs: List[Dict[str, Any]],
        history: List[Dict[str, Any]],
        statistics: Dict[str, Any],
        job_counter: int,
        scheduler_running: bool,
    ) -> None:
        """Save scheduler state."""

    @abstractmethod
    def load_state(self) -> Optional[Dict[str, Any]]:
        """Load scheduler state. Returns None if no state exists."""

    @abstractmethod
    def save_dead_letters(
        self, dead_letters: List[Dict[str, Any]], max_dead_letters: int
    ) -> None:
        """Save dead letter queue entries."""

    @abstractmethod
    def load_dead_letters(self) -> List[Dict[str, Any]]:
        """Load dead letter queue entries."""

    @abstractmethod
    def clear_dead_letters(self) -> int:
        """Clear all dead letter entries. Returns count cleared."""

    def close(self) -> None:
        """Clean up resources. Override if needed."""
