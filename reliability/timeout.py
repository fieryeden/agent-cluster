"""
Timeout Handler for Task Execution

Provides configurable timeouts with:
- Hard limits on execution time
- Graceful shutdown on timeout
- Timeout statistics
- Context manager and decorator support
"""

import time
import threading
import signal
from dataclasses import dataclass
from typing import Callable, Any, Optional, Dict
from functools import wraps


class TimeoutError(Exception):
    """Raised when operation exceeds timeout."""
    pass


@dataclass
class TimeoutConfig:
    """Configuration for timeout behavior."""
    default_timeout: float = 30.0
    max_timeout: float = 300.0
    graceful_shutdown_seconds: float = 5.0
    enable_signals: bool = True


class TimeoutHandler:
    """
    Handles timeouts for task execution.
    
    Usage:
        handler = TimeoutHandler(default_timeout=30.0)
        
        # As context manager
        with handler.timeout(10.0):
            result = long_running_operation()
        
        # As decorator
        @handler.timeout(10.0)
        def my_function():
            ...
        
        # Execute with timeout
        result = handler.execute(lambda: risky_operation(), timeout=5.0)
    """
    
    def __init__(
        self,
        default_timeout: float = 30.0,
        max_timeout: float = 300.0,
        graceful_shutdown: float = 5.0,
    ):
        """
        Initialize timeout handler.
        
        Args:
            default_timeout: Default timeout in seconds
            max_timeout: Maximum allowed timeout
            graceful_shutdown: Time to wait for graceful shutdown
        """
        self.default_timeout = default_timeout
        self.max_timeout = max_timeout
        self.graceful_shutdown = graceful_shutdown
        
        self._stats = {
            'total_operations': 0,
            'timeouts': 0,
            'total_time': 0.0,
        }
        self._lock = threading.Lock()
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Get timeout statistics."""
        return self._stats.copy()
    
    def timeout(self, seconds: float = None):
        """
        Context manager for timeout.
        
        Args:
            seconds: Timeout in seconds (None for default)
            
        Yields:
            None
            
        Raises:
            TimeoutError: If operation exceeds timeout
        """
        return _TimeoutContext(
            seconds or self.default_timeout,
            self.max_timeout,
            self._stats,
            self._lock,
        )
    
    def execute(
        self,
        func: Callable[[], Any],
        timeout: float = None,
        on_timeout: Callable[[], Any] = None,
    ) -> Any:
        """
        Execute function with timeout.
        
        Args:
            func: Function to execute
            timeout: Timeout in seconds (None for default)
            on_timeout: Fallback function if timeout occurs
            
        Returns:
            Function result
            
        Raises:
            TimeoutError: If timeout and no fallback provided
        """
        timeout_val = min(timeout or self.default_timeout, self.max_timeout)
        result = [None]
        exception = [None]
        
        def worker():
            try:
                result[0] = func()
            except Exception as e:
                exception[0] = e
        
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        thread.join(timeout=timeout_val)
        
        with self._lock:
            self._stats['total_operations'] += 1
        
        if thread.is_alive():
            # Timeout occurred
            with self._lock:
                self._stats['timeouts'] += 1
            
            if on_timeout:
                return on_timeout()
            raise TimeoutError(f"Operation timed out after {timeout_val}s")
        
        if exception[0]:
            raise exception[0]
        
        return result[0]
    
    def __call__(self, timeout: float = None):
        """Decorator factory for timeout."""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                return self.execute(lambda: func(*args, **kwargs), timeout)
            return wrapper
        return decorator


class _TimeoutContext:
    """Internal context manager for timeout handling."""
    
    def __init__(
        self,
        timeout: float,
        max_timeout: float,
        stats: Dict[str, Any],
        lock: threading.Lock,
    ):
        self.timeout = min(timeout, max_timeout)
        self.stats = stats
        self.lock = lock
        self.start_time = None
        self._timer = None
        self._timed_out = False
    
    def __enter__(self):
        self.start_time = time.time()
        
        if threading.current_thread() is threading.main_thread():
            # Use signal-based timeout (only works in main thread)
            self._setup_signal_timeout()
        else:
            # Use timer-based timeout for non-main threads
            self._setup_thread_timeout()
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cancel_timeout()
        
        elapsed = time.time() - self.start_time
        with self.lock:
            self.stats['total_operations'] += 1
            self.stats['total_time'] += elapsed
        
        if self._timed_out:
            raise TimeoutError(f"Operation timed out after {self.timeout}s")
        
        return False
    
    def _setup_signal_timeout(self):
        """Setup signal-based timeout (Unix only, main thread)."""
        def timeout_handler(signum, frame):
            self._timed_out = True
        
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(int(self.timeout))
    
    def _setup_thread_timeout(self):
        """Setup thread-based timeout."""
        def on_timeout():
            self._timed_out = True
        
        self._timer = threading.Timer(self.timeout, on_timeout)
        self._timer.start()
    
    def _cancel_timeout(self):
        """Cancel active timeout."""
        if self._timer:
            self._timer.cancel()
            self._timer = None
        
        try:
            signal.alarm(0)
        except (ValueError, AttributeError):
            # Signal not available (non-Unix or non-main thread)
            pass


class TaskTimeoutManager:
    """
    Manages timeouts for multiple tasks.
    
    Usage:
        manager = TaskTimeoutManager()
        
        # Start task with timeout
        task_id = manager.start("my-task", timeout=30.0)
        
        # Check status
        if manager.is_timed_out(task_id):
            handle_timeout()
        
        # Mark complete
        manager.complete(task_id)
    """
    
    def __init__(self, default_timeout: float = 30.0):
        self.default_timeout = default_timeout
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
    
    def start(
        self,
        task_id: str,
        timeout: float = None,
        on_timeout: Callable[[str], None] = None,
    ) -> str:
        """
        Start tracking a task.
        
        Args:
            task_id: Unique task identifier
            timeout: Timeout in seconds
            on_timeout: Callback when timeout occurs
            
        Returns:
            Task ID
        """
        timeout = timeout or self.default_timeout
        start_time = time.time()
        
        def timeout_callback():
            with self._lock:
                if task_id in self._tasks:
                    self._tasks[task_id]['timed_out'] = True
                    if on_timeout:
                        on_timeout(task_id)
        
        timer = threading.Timer(timeout, timeout_callback)
        
        with self._lock:
            self._tasks[task_id] = {
                'start_time': start_time,
                'timeout': timeout,
                'timer': timer,
                'timed_out': False,
            }
        
        timer.start()
        return task_id
    
    def complete(self, task_id: str) -> Optional[float]:
        """
        Mark task as complete.
        
        Args:
            task_id: Task identifier
            
        Returns:
            Elapsed time, or None if task not found
        """
        with self._lock:
            task = self._tasks.pop(task_id, None)
            if task:
                task['timer'].cancel()
                return time.time() - task['start_time']
            return None
    
    def is_timed_out(self, task_id: str) -> bool:
        """Check if task has timed out."""
        with self._lock:
            task = self._tasks.get(task_id)
            return task['timed_out'] if task else False
    
    def get_remaining(self, task_id: str) -> Optional[float]:
        """Get remaining time before timeout."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task:
                elapsed = time.time() - task['start_time']
                remaining = task['timeout'] - elapsed
                return max(0, remaining)
            return None
    
    def cancel(self, task_id: str) -> bool:
        """Cancel a task timeout."""
        with self._lock:
            task = self._tasks.pop(task_id, None)
            if task:
                task['timer'].cancel()
                return True
            return False
    
    def cancel_all(self):
        """Cancel all task timeouts."""
        with self._lock:
            for task in self._tasks.values():
                task['timer'].cancel()
            self._tasks.clear()
    
    def get_active_count(self) -> int:
        """Get number of active tasks."""
        with self._lock:
            return len(self._tasks)
