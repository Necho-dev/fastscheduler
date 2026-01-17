"""JSON file-based storage backend."""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import StorageBackend

logger = logging.getLogger("fastscheduler")


class JSONStorageBackend(StorageBackend):
    """
    JSON file-based storage backend.
    
    Stores scheduler state in JSON files. This is the default backend,
    suitable for simple setups and development.
    
    Args:
        state_file: Path to the main state JSON file
        quiet: If True, suppress info log messages
    """
    
    def __init__(self, state_file: str = "fastscheduler_state.json", quiet: bool = False):
        self.state_file = Path(state_file)
        self._dead_letters_file = Path(
            str(self.state_file).replace(".json", "_dead_letters.json")
        )
        self.quiet = quiet
    
    def save_state(
        self,
        jobs: List[Dict[str, Any]],
        history: List[Dict[str, Any]],
        statistics: Dict[str, Any],
        job_counter: int,
        scheduler_running: bool,
    ) -> None:
        """Save state to JSON file."""
        try:
            state = {
                "version": "1.0",
                "metadata": {
                    "last_save": time.time(),
                    "last_save_readable": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "scheduler_running": scheduler_running,
                },
                "jobs": jobs,
                "history": history[-1000:],  # Limit history in file
                "statistics": statistics,
                "_job_counter": job_counter,
            }
            
            # Atomic write using temp file
            temp_file = self.state_file.with_suffix(".tmp")
            with open(temp_file, "w") as f:
                json.dump(state, f, indent=2)
            temp_file.replace(self.state_file)
            
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    def load_state(self) -> Optional[Dict[str, Any]]:
        """Load state from JSON file."""
        if not self.state_file.exists():
            if not self.quiet:
                logger.info("No previous state found, starting fresh")
            return None
        
        try:
            with open(self.state_file, "r") as f:
                state = json.load(f)
            return state
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return None
    
    def save_dead_letters(self, dead_letters: List[Dict[str, Any]], max_dead_letters: int) -> None:
        """Save dead letter queue to JSON file."""
        try:
            data = {
                "dead_letters": dead_letters,
                "max_dead_letters": max_dead_letters,
            }
            
            with open(self._dead_letters_file, "w") as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save dead letters: {e}")
    
    def load_dead_letters(self) -> List[Dict[str, Any]]:
        """Load dead letter queue from JSON file."""
        if not self._dead_letters_file.exists():
            return []
        
        try:
            with open(self._dead_letters_file, "r") as f:
                data = json.load(f)
            return data.get("dead_letters", [])
        except Exception as e:
            logger.error(f"Failed to load dead letters: {e}")
            return []
    
    def clear_dead_letters(self) -> int:
        """Clear all dead letter entries."""
        count = len(self.load_dead_letters())
        self.save_dead_letters([], 0)
        return count
    
    @property
    def dead_letters_file(self) -> Path:
        """Get the dead letters file path."""
        return self._dead_letters_file
