"""
Dead Letter Queue for Failed Tasks

Stores failed tasks for analysis, retry, or manual intervention.
Supports:
- Persistent storage (JSON files)
- TTL-based expiration
- Search and filtering
- Retry from DLQ
"""

import os
import json
import time
import uuid
import threading
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from pathlib import Path


@dataclass
class FailedTask:
    """
    Represents a task that failed after exhausting retries.
    
    Attributes:
        id: Unique identifier for this failed task
        task: Original task parameters
        error: Error message
        traceback: Full traceback if available
        handler: Handler that was supposed to process the task
        attempts: Number of retry attempts made
        first_failure: Timestamp of first failure
        last_failure: Timestamp of last failure
        agent_id: Agent that was processing the task
        metadata: Additional context
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task: Dict[str, Any] = field(default_factory=dict)
    error: str = ""
    traceback: Optional[str] = None
    handler: Optional[str] = None
    attempts: int = 0
    first_failure: float = field(default_factory=time.time)
    last_failure: float = field(default_factory=time.time)
    agent_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FailedTask':
        """Create from dictionary."""
        return cls(**data)
    
    def age_seconds(self) -> float:
        """Get age of this failed task."""
        return time.time() - self.first_failure


class DeadLetterQueue:
    """
    Dead Letter Queue for storing and managing failed tasks.
    
    Usage:
        dlq = DeadLetterQueue("/var/agent-cluster/dlq")
        
        # Add failed task
        dlq.add(task, error="Connection timeout", attempts=3)
        
        # Process later
        for failed in dlq.list_unprocessed():
            if should_retry(failed):
                retry_task(failed)
                dlq.mark_processed(failed.id)
    """
    
    def __init__(
        self,
        storage_dir: str = "/tmp/agent_cluster/dlq",
        max_size: int = 10000,
        ttl_seconds: float = 86400 * 7,  # 7 days
        on_add: Callable[[FailedTask], None] = None,
    ):
        """
        Initialize Dead Letter Queue.
        
        Args:
            storage_dir: Directory for persistent storage
            max_size: Maximum number of tasks in queue
            ttl_seconds: Time-to-live for tasks (auto-expire)
            on_add: Callback when task is added
        """
        self.storage_dir = Path(storage_dir)
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.on_add = on_add
        
        self._tasks: Dict[str, FailedTask] = {}
        self._processed: set = set()
        self._lock = threading.RLock()
        
        # Ensure storage directory exists
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing tasks
        self._load_from_disk()
    
    def add(
        self,
        task: Dict[str, Any],
        error: str,
        traceback: str = None,
        handler: str = None,
        attempts: int = 0,
        agent_id: str = None,
        metadata: Dict[str, Any] = None,
    ) -> FailedTask:
        """
        Add a failed task to the queue.
        
        Args:
            task: Original task parameters
            error: Error message
            traceback: Full traceback if available
            handler: Handler name
            attempts: Number of retry attempts
            agent_id: Agent that processed the task
            metadata: Additional context
            
        Returns:
            Created FailedTask
        """
        with self._lock:
            # Check size limit
            if len(self._tasks) >= self.max_size:
                self._evict_oldest()
            
            failed_task = FailedTask(
                task=task,
                error=error,
                traceback=traceback,
                handler=handler,
                attempts=attempts,
                agent_id=agent_id,
                metadata=metadata or {},
            )
            
            self._tasks[failed_task.id] = failed_task
            self._save_to_disk(failed_task)
            
            if self.on_add:
                self.on_add(failed_task)
            
            return failed_task
    
    def get(self, task_id: str) -> Optional[FailedTask]:
        """Get failed task by ID."""
        return self._tasks.get(task_id)
    
    def list_unprocessed(
        self,
        handler: str = None,
        agent_id: str = None,
        limit: int = None,
    ) -> List[FailedTask]:
        """
        List unprocessed failed tasks.
        
        Args:
            handler: Filter by handler name
            agent_id: Filter by agent ID
            limit: Maximum number to return
            
        Returns:
            List of unprocessed FailedTasks
        """
        with self._lock:
            results = []
            for task in self._tasks.values():
                if task.id in self._processed:
                    continue
                if handler and task.handler != handler:
                    continue
                if agent_id and task.agent_id != agent_id:
                    continue
                results.append(task)
            
            # Sort by failure time (oldest first)
            results.sort(key=lambda t: t.first_failure)
            
            if limit:
                results = results[:limit]
            
            return results
    
    def list_all(
        self,
        include_processed: bool = False,
        limit: int = 100,
    ) -> List[FailedTask]:
        """List all failed tasks."""
        with self._lock:
            tasks = list(self._tasks.values())
            if not include_processed:
                tasks = [t for t in tasks if t.id not in self._processed]
            tasks.sort(key=lambda t: t.first_failure, reverse=True)
            return tasks[:limit]
    
    def mark_processed(self, task_id: str) -> bool:
        """Mark task as processed."""
        with self._lock:
            if task_id in self._tasks:
                self._processed.add(task_id)
                self._save_processed()
                return True
            return False
    
    def retry(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get task for retry (removes from DLQ).
        
        Args:
            task_id: ID of failed task
            
        Returns:
            Original task parameters, or None if not found
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if task:
                self._remove_from_disk(task_id)
                del self._tasks[task_id]
                self._processed.discard(task_id)
                return task.task
            return None
    
    def purge_expired(self) -> int:
        """
        Remove expired tasks from queue.
        
        Returns:
            Number of tasks removed
        """
        with self._lock:
            expired = []
            now = time.time()
            
            for task_id, task in self._tasks.items():
                if now - task.first_failure > self.ttl_seconds:
                    expired.append(task_id)
            
            for task_id in expired:
                self._remove_from_disk(task_id)
                del self._tasks[task_id]
                self._processed.discard(task_id)
            
            return len(expired)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        with self._lock:
            handlers = {}
            agents = {}
            error_types = {}
            
            for task in self._tasks.values():
                if task.handler:
                    handlers[task.handler] = handlers.get(task.handler, 0) + 1
                if task.agent_id:
                    agents[task.agent_id] = agents.get(task.agent_id, 0) + 1
                # Simple error categorization
                error_type = task.error.split(':')[0] if ':' in task.error else task.error[:50]
                error_types[error_type] = error_types.get(error_type, 0) + 1
            
            return {
                'total_tasks': len(self._tasks),
                'processed': len(self._processed),
                'unprocessed': len(self._tasks) - len(self._processed),
                'handlers': handlers,
                'agents': agents,
                'error_types': error_types,
                'oldest_age': max((t.age_seconds() for t in self._tasks.values()), default=0),
            }
    
    def clear(self):
        """Clear all tasks from queue."""
        with self._lock:
            for task_id in list(self._tasks.keys()):
                self._remove_from_disk(task_id)
            self._tasks.clear()
            self._processed.clear()
            self._save_processed()
    
    def _evict_oldest(self):
        """Remove oldest tasks when at capacity."""
        sorted_tasks = sorted(
            self._tasks.items(),
            key=lambda x: x[1].first_failure
        )
        
        # Remove oldest 10% to make room
        to_remove = max(1, len(sorted_tasks) // 10)
        for task_id, _ in sorted_tasks[:to_remove]:
            self._remove_from_disk(task_id)
            del self._tasks[task_id]
    
    def _save_to_disk(self, task: FailedTask):
        """Save task to disk."""
        filename = f"{task.id}.json"
        filepath = self.storage_dir / filename
        with open(filepath, 'w') as f:
            json.dump(task.to_dict(), f, indent=2)
    
    def _remove_from_disk(self, task_id: str):
        """Remove task file from disk."""
        filename = f"{task_id}.json"
        filepath = self.storage_dir / filename
        if filepath.exists():
            filepath.unlink()
    
    def _load_from_disk(self):
        """Load existing tasks from disk."""
        for filepath in self.storage_dir.glob("*.json"):
            if filepath.name == "_processed.json":
                continue
            try:
                with open(filepath) as f:
                    data = json.load(f)
                task = FailedTask.from_dict(data)
                self._tasks[task.id] = task
            except (json.JSONDecodeError, KeyError):
                # Skip corrupted files
                continue
        
        # Load processed set
        processed_file = self.storage_dir / "_processed.json"
        if processed_file.exists():
            try:
                with open(processed_file) as f:
                    self._processed = set(json.load(f))
            except (json.JSONDecodeError, TypeError):
                self._processed = set()
    
    def _save_processed(self):
        """Save processed set to disk."""
        processed_file = self.storage_dir / "_processed.json"
        with open(processed_file, 'w') as f:
            json.dump(list(self._processed), f)
