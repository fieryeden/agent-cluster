"""
Circuit Breaker Pattern Implementation

Prevents cascading failures by stopping requests to failing services.
States: CLOSED (normal) -> OPEN (failing) -> HALF_OPEN (testing)
"""

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Optional, Dict, Any, List
from enum import Enum
from datetime import datetime


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation, requests flow through
    OPEN = "open"          # Failing, requests are blocked
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitStats:
    """Statistics for circuit breaker."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    last_failure_time: Optional[float] = None
    last_failure_reason: Optional[str] = None
    consecutive_failures: int = 0
    state_changes: int = 0


class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures.
    
    Usage:
        breaker = CircuitBreaker(name="agent-1", failure_threshold=5)
        
        result = breaker.call(lambda: risky_operation())
        if result is None:
            print("Circuit is open, request blocked")
    
    Or as decorator:
        @breaker.protect
        def my_function():
            ...
    """
    
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 3,
        timeout_seconds: float = 60.0,
        half_open_max_calls: int = 3,
    ):
        """
        Initialize circuit breaker.
        
        Args:
            name: Identifier for this circuit
            failure_threshold: Failures before opening circuit
            success_threshold: Successes in half-open to close
            timeout_seconds: Time before transitioning to half-open
            half_open_max_calls: Max calls allowed in half-open state
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout_seconds = timeout_seconds
        self.half_open_max_calls = half_open_max_calls
        
        self._state = CircuitState.CLOSED
        self._stats = CircuitStats()
        self._half_open_successes = 0
        self._half_open_calls = 0
        self._last_state_change = time.time()
        self._lock = threading.RLock()
        
        # Event callbacks
        self._on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None
        self._on_failure: Optional[Callable[[Exception], None]] = None
    
    @property
    def state(self) -> CircuitState:
        """Get current state, with automatic timeout transition."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_state_change >= self.timeout_seconds:
                    self._transition_to(CircuitState.HALF_OPEN)
            return self._state
    
    @property
    def stats(self) -> CircuitStats:
        """Get circuit statistics."""
        return self._stats
    
    @property
    def is_closed(self) -> bool:
        """Check if circuit allows requests."""
        return self.state == CircuitState.CLOSED
    
    @property
    def is_open(self) -> bool:
        """Check if circuit blocks requests."""
        return self.state == CircuitState.OPEN
    
    @property
    def is_half_open(self) -> bool:
        """Check if circuit is testing recovery."""
        return self.state == CircuitState.HALF_OPEN
    
    def call(self, func: Callable[[], Any], *args, **kwargs) -> Any:
        """
        Execute function through circuit breaker.
        
        Args:
            func: Function to execute
            *args, **kwargs: Arguments for function
            
        Returns:
            Function result or None if circuit is open
            
        Raises:
            Exception: Re-raised after recording failure
        """
        state = self.state  # Get current state (may transition)
        
        if state == CircuitState.OPEN:
            return None  # Block request
        
        if state == CircuitState.HALF_OPEN:
            with self._lock:
                if self._half_open_calls >= self.half_open_max_calls:
                    return None  # Too many half-open calls
        
        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure(e)
            raise
    
    def protect(self, func: Callable) -> Callable:
        """Decorator to protect function with circuit breaker."""
        def wrapper(*args, **kwargs):
            return self.call(lambda: func(*args, **kwargs))
        return wrapper
    
    def _record_success(self):
        """Record successful request."""
        with self._lock:
            self._stats.total_requests += 1
            self._stats.successful_requests += 1
            self._stats.consecutive_failures = 0
            
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_successes += 1
                self._half_open_calls += 1
                
                if self._half_open_successes >= self.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
    
    def _record_failure(self, error: Exception):
        """Record failed request."""
        with self._lock:
            self._stats.total_requests += 1
            self._stats.failed_requests += 1
            self._stats.consecutive_failures += 1
            self._stats.last_failure_time = time.time()
            self._stats.last_failure_reason = str(error)
            
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
                self._transition_to(CircuitState.OPEN)
            elif self._stats.consecutive_failures >= self.failure_threshold:
                self._transition_to(CircuitState.OPEN)
            
            if self._on_failure:
                self._on_failure(error)
    
    def _transition_to(self, new_state: CircuitState):
        """Transition to new state."""
        old_state = self._state
        if old_state == new_state:
            return
        
        self._state = new_state
        self._last_state_change = time.time()
        self._stats.state_changes += 1
        
        # Reset half-open counters
        if new_state == CircuitState.HALF_OPEN:
            self._half_open_successes = 0
            self._half_open_calls = 0
        elif new_state == CircuitState.CLOSED:
            self._stats.consecutive_failures = 0
        
        if self._on_state_change:
            self._on_state_change(old_state, new_state)
    
    def reset(self):
        """Reset circuit breaker to closed state."""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
    
    def force_open(self):
        """Force circuit to open state (manual trip)."""
        with self._lock:
            self._transition_to(CircuitState.OPEN)
    
    def on_state_change(self, callback: Callable[[CircuitState, CircuitState], None]):
        """Register state change callback."""
        self._on_state_change = callback
    
    def on_failure(self, callback: Callable[[Exception], None]):
        """Register failure callback."""
        self._on_failure = callback
    
    def to_dict(self) -> Dict[str, Any]:
        """Export circuit state as dictionary."""
        return {
            'name': self.name,
            'state': self.state.value,
            'stats': {
                'total_requests': self._stats.total_requests,
                'successful_requests': self._stats.successful_requests,
                'failed_requests': self._stats.failed_requests,
                'consecutive_failures': self._stats.consecutive_failures,
                'last_failure_time': self._stats.last_failure_time,
                'last_failure_reason': self._stats.last_failure_reason,
            },
            'config': {
                'failure_threshold': self.failure_threshold,
                'success_threshold': self.success_threshold,
                'timeout_seconds': self.timeout_seconds,
            },
        }


class CircuitBreakerRegistry:
    """
    Registry for managing multiple circuit breakers.
    
    Usage:
        registry = CircuitBreakerRegistry()
        registry.get_or_create("agent-1")
        registry.get_or_create("agent-2", failure_threshold=10)
    """
    
    def __init__(self, default_config: Dict[str, Any] = None):
        self._circuits: Dict[str, CircuitBreaker] = {}
        self._default_config = default_config or {}
        self._lock = threading.RLock()
    
    def get_or_create(
        self,
        name: str,
        failure_threshold: int = None,
        success_threshold: int = None,
        timeout_seconds: float = None,
    ) -> CircuitBreaker:
        """Get existing circuit or create new one."""
        with self._lock:
            if name not in self._circuits:
                config = self._default_config.copy()
                if failure_threshold is not None:
                    config['failure_threshold'] = failure_threshold
                if success_threshold is not None:
                    config['success_threshold'] = success_threshold
                if timeout_seconds is not None:
                    config['timeout_seconds'] = timeout_seconds
                
                self._circuits[name] = CircuitBreaker(name=name, **config)
            return self._circuits[name]
    
    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get circuit by name."""
        return self._circuits.get(name)
    
    def reset_all(self):
        """Reset all circuits."""
        with self._lock:
            for circuit in self._circuits.values():
                circuit.reset()
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get stats for all circuits."""
        return {name: cb.to_dict() for name, cb in self._circuits.items()}
    
    def get_open_circuits(self) -> List[str]:
        """Get names of all open circuits."""
        return [name for name, cb in self._circuits.items() if cb.is_open]
