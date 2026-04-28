#!/usr/bin/env python3
"""
Task Scheduler

Intelligent task scheduling for heterogeneous agents.
"""

import threading
import time
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from collections import defaultdict
import heapq


class SchedulePolicy(Enum):
    """Task scheduling policies."""
    FIFO = "fifo"               # First in, first out
    PRIORITY = "priority"       # Higher priority first
    ROUND_ROBIN = "round_robin" # Fair distribution
    CAPABILITY = "capability"   # Best capability match
    LOAD_BALANCE = "load"       # Lowest load agent


@dataclass
class ScheduledTask:
    """A task in the scheduler."""
    task_id: str
    task_type: str
    priority: int
    params: dict
    created_at: datetime = field(default_factory=datetime.now)
    target_agent: Optional[str] = None
    retries: int = 0
    max_retries: int = 3
    
    def __lt__(self, other):
        # For heap - higher priority = smaller number (processed first)
        return self.priority > other.priority


class TaskScheduler:
    """
    Intelligent task scheduler.
    
    Features:
    - Multiple scheduling policies
    - Priority queues
    - Retry handling
    - Deadline tracking
    - Load balancing
    """
    
    def __init__(self, policy: SchedulePolicy = SchedulePolicy.PRIORITY):
        self.policy = policy
        self.tasks: Dict[str, ScheduledTask] = {}
        self.pending: List[ScheduledTask] = []  # Heap
        self.assigned: Dict[str, str] = {}  # task_id -> agent_id
        self.completed: List[str] = []
        self.failed: List[str] = []
        
        self._lock = threading.Lock()
        
        # Callbacks
        self.on_assign: Optional[Callable] = None
        self.on_complete: Optional[Callable] = None
        self.on_fail: Optional[Callable] = None
        
        # Stats
        self.stats = defaultdict(int)
    
    def submit(
        self,
        task_id: str,
        task_type: str,
        priority: int = 0,
        params: dict = None,
        target_agent: str = None,
    ):
        """Submit a task for scheduling."""
        task = ScheduledTask(
            task_id=task_id,
            task_type=task_type,
            priority=priority,
            params=params or {},
            target_agent=target_agent,
        )
        
        with self._lock:
            self.tasks[task_id] = task
            heapq.heappush(self.pending, task)
            self.stats["submitted"] += 1
    
    def next_batch(self, n: int = 10) -> List[ScheduledTask]:
        """Get next N tasks to schedule."""
        with self._lock:
            batch = []
            for _ in range(min(n, len(self.pending))):
                if self.pending:
                    task = heapq.heappop(self.pending)
                    batch.append(task)
            return batch
    
    def assign(self, task_id: str, agent_id: str):
        """Mark task as assigned to agent."""
        with self._lock:
            self.assigned[task_id] = agent_id
            if task_id in self.tasks:
                self.tasks[task_id].target_agent = agent_id
            self.stats["assigned"] += 1
        
        if self.on_assign:
            self.on_assign(task_id, agent_id)
    
    def complete(self, task_id: str, result: dict = None):
        """Mark task as completed."""
        with self._lock:
            if task_id in self.assigned:
                del self.assigned[task_id]
            self.completed.append(task_id)
            self.stats["completed"] += 1
        
        if self.on_complete:
            self.on_complete(task_id, result)
    
    def fail(self, task_id: str, error: str, retry: bool = True):
        """Mark task as failed."""
        with self._lock:
            if task_id in self.assigned:
                del self.assigned[task_id]
            
            task = self.tasks.get(task_id)
            if task and retry and task.retries < task.max_retries:
                # Retry
                task.retries += 1
                heapq.heappush(self.pending, task)
                self.stats["retried"] += 1
            else:
                self.failed.append(task_id)
                self.stats["failed"] += 1
        
        if self.on_fail:
            self.on_fail(task_id, error)
    
    def get_status(self) -> dict:
        """Get scheduler status."""
        with self._lock:
            return {
                "pending": len(self.pending),
                "assigned": len(self.assigned),
                "completed": len(self.completed),
                "failed": len(self.failed),
                "stats": dict(self.stats),
            }
