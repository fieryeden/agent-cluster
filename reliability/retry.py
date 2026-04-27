"""
Retry Manager with Exponential Backoff

Provides configurable retry policies with:
- Exponential backoff with jitter
- Maximum retry limits
- Retry on specific exceptions
- Retry budgets (max total retries per time window)
"""

import time
import random
import threading
from dataclasses import dataclass, field
from typing import Callable, Type, Tuple, Any, Optional, List
from functools import wraps
from enum import Enum


class RetryStrategy(Enum):
    """Retry strategies."""
    FIXED = "fixed"           # Fixed delay between retries
    LINEAR = "linear"         # Linear increase: delay * attempt
    EXPONENTIAL = "exponential"  # Exponential backoff: delay * 2^attempt
    EXPONENTIAL_JITTER = "exponential_jitter"  # Exponential with random jitter


@dataclass
class RetryPolicy:
    """
    Configuration for retry behavior.
    
    Usage:
        policy = RetryPolicy(
            max_retries=3,
            base_delay=1.0,
            strategy=RetryStrategy.EXPONENTIAL_JITTER,
        )
    """
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_JITTER
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)
    retryable_status_codes: Tuple[int, ...] = (429, 500, 502, 503, 504)
    retry_on: Optional[Callable[[Exception], bool]] = None
    
    def should_retry(self, exception: Exception) -> bool:
        """Check if exception should trigger retry."""
        # Check custom predicate
        if self.retry_on and self.retry_on(exception):
            return True
        
        # Check exception types
        if isinstance(exception, self.retryable_exceptions):
            return True
        
        return False
    
    def get_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number."""
        if self.strategy == RetryStrategy.FIXED:
            delay = self.base_delay
        
        elif self.strategy == RetryStrategy.LINEAR:
            delay = self.base_delay * attempt
        
        elif self.strategy == RetryStrategy.EXPONENTIAL:
            delay = self.base_delay * (2 ** (attempt - 1))
        
        elif self.strategy == RetryStrategy.EXPONENTIAL_JITTER:
            # Full jitter: random between 0 and exponential delay
            exponential = self.base_delay * (2 ** (attempt - 1))
            delay = random.uniform(0, min(exponential, self.max_delay))
        
        else:
            delay = self.base_delay
        
        return min(delay, self.max_delay)


@dataclass
class RetryStats:
    """Statistics for retry manager."""
    total_attempts: int = 0
    successful_first_try: int = 0
    successful_after_retry: int = 0
    exhausted_retries: int = 0
    total_retry_time: float = 0.0


class RetryManager:
    """
    Manages retries with configurable policies.
    
    Usage:
        manager = RetryManager(policy=RetryPolicy(max_retries=3))
        
        # As context
        result = manager.execute(lambda: risky_operation())
        
        # As decorator
        @manager.retry
        def my_function():
            ...
    """
    
    def __init__(
        self,
        policy: RetryPolicy = None,
        on_retry: Callable[[int, Exception, float], None] = None,
    ):
        self.policy = policy or RetryPolicy()
        self.on_retry = on_retry
        self._stats = RetryStats()
        self._lock = threading.Lock()
    
    @property
    def stats(self) -> RetryStats:
        """Get retry statistics."""
        return self._stats
    
    def execute(
        self,
        func: Callable[[], Any],
        *args,
        **kwargs
    ) -> Any:
        """
        Execute function with retry logic.
        
        Args:
            func: Function to execute
            *args, **kwargs: Arguments for function
            
        Returns:
            Function result
            
        Raises:
            Exception: Last exception after exhausting retries
        """
        last_exception = None
        start_time = time.time()
        
        for attempt in range(1, self.policy.max_retries + 2):  # +1 for initial attempt
            try:
                with self._lock:
                    self._stats.total_attempts += 1
                
                result = func(*args, **kwargs)
                
                # Success
                with self._lock:
                    if attempt == 1:
                        self._stats.successful_first_try += 1
                    else:
                        self._stats.successful_after_retry += 1
                    self._stats.total_retry_time += time.time() - start_time
                
                return result
                
            except Exception as e:
                last_exception = e
                
                # Check if we should retry
                if not self.policy.should_retry(e):
                    raise
                
                # Check if we have retries left
                if attempt > self.policy.max_retries:
                    with self._lock:
                        self._stats.exhausted_retries += 1
                    raise
                
                # Calculate and apply delay
                delay = self.policy.get_delay(attempt)
                
                if self.on_retry:
                    self.on_retry(attempt, e, delay)
                
                time.sleep(delay)
        
        # Should not reach here, but just in case
        raise last_exception
    
    def retry(self, func: Callable) -> Callable:
        """Decorator to add retry logic to function."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.execute(lambda: func(*args, **kwargs))
        return wrapper
    
    def reset_stats(self):
        """Reset statistics."""
        with self._lock:
            self._stats = RetryStats()


class RetryBudget:
    """
    Limits total retries across multiple operations.
    
    Prevents retry storms by enforcing a budget on retries
    within a time window.
    
    Usage:
        budget = RetryBudget(max_retries_per_minute=100)
        
        if budget.acquire():
            result = manager.execute(func)
        else:
            # Retry budget exhausted, fail fast
            result = fallback()
    """
    
    def __init__(
        self,
        max_retries_per_minute: int = 100,
        max_retries_per_hour: int = 1000,
    ):
        self.max_retries_per_minute = max_retries_per_minute
        self.max_retries_per_hour = max_retries_per_hour
        
        self._minute_count = 0
        self._hour_count = 0
        self._minute_start = time.time()
        self._hour_start = time.time()
        self._lock = threading.Lock()
    
    def acquire(self) -> bool:
        """
        Acquire a retry slot.
        
        Returns:
            True if retry is allowed, False if budget exhausted
        """
        with self._lock:
            now = time.time()
            
            # Reset counters if windows expired
            if now - self._minute_start >= 60:
                self._minute_count = 0
                self._minute_start = now
            
            if now - self._hour_start >= 3600:
                self._hour_count = 0
                self._hour_start = now
            
            # Check limits
            if self._minute_count >= self.max_retries_per_minute:
                return False
            
            if self._hour_count >= self.max_retries_per_hour:
                return False
            
            # Acquire slot
            self._minute_count += 1
            self._hour_count += 1
            return True
    
    def get_status(self) -> dict:
        """Get current budget status."""
        with self._lock:
            now = time.time()
            return {
                'minute': {
                    'used': self._minute_count,
                    'limit': self.max_retries_per_minute,
                    'resets_in': max(0, 60 - (now - self._minute_start)),
                },
                'hour': {
                    'used': self._hour_count,
                    'limit': self.max_retries_per_hour,
                    'resets_in': max(0, 3600 - (now - self._hour_start)),
                },
            }


def with_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_JITTER,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """
    Decorator for quick retry configuration.
    
    Usage:
        @with_retry(max_retries=3, base_delay=1.0)
        def flaky_function():
            ...
    """
    policy = RetryPolicy(
        max_retries=max_retries,
        base_delay=base_delay,
        strategy=strategy,
        retryable_exceptions=exceptions,
    )
    manager = RetryManager(policy=policy)
    return manager.retry
