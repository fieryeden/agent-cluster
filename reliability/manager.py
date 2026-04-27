"""
Resilience Manager - Unified Reliability Orchestrator

Combines all reliability components into a unified interface:
- Circuit breaker
- Retry with backoff
- Dead letter queue
- Timeout handling
- Health checks
- Resource limits
"""

import time
import threading
from dataclasses import dataclass, field
from typing import Dict, Any, Callable, Optional, List
from functools import wraps

from reliability.circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerRegistry
from reliability.retry import RetryManager, RetryPolicy, RetryStrategy
from reliability.dead_letter import DeadLetterQueue, FailedTask
from reliability.timeout import TimeoutHandler, TimeoutError
from reliability.health import HealthChecker, HealthStatus


@dataclass
class ResilienceConfig:
    """Configuration for resilience manager."""
    # Circuit breaker
    circuit_failure_threshold: int = 5
    circuit_success_threshold: int = 3
    circuit_timeout_seconds: float = 60.0
    
    # Retry
    retry_max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_JITTER
    
    # Timeout
    default_timeout: float = 30.0
    max_timeout: float = 300.0
    
    # Dead letter queue
    dlq_storage_dir: str = "/tmp/agent_cluster/dlq"
    dlq_max_size: int = 10000
    dlq_ttl_seconds: float = 86400 * 7
    
    # Health check
    health_check_interval: float = 30.0


class ResilienceManager:
    """
    Unified resilience orchestrator.
    
    Usage:
        manager = ResilienceManager()
        
        # Execute with full resilience
        result = manager.execute(
            "agent-1",
            lambda: risky_operation(),
            timeout=30.0,
        )
        
        # As decorator
        @manager.protect("agent-1")
        def my_function():
            ...
    """
    
    def __init__(self, config: ResilienceConfig = None):
        self.config = config or ResilienceConfig()
        
        # Initialize components
        self._circuit_registry = CircuitBreakerRegistry({
            'failure_threshold': self.config.circuit_failure_threshold,
            'success_threshold': self.config.circuit_success_threshold,
            'timeout_seconds': self.config.circuit_timeout_seconds,
        })
        
        self._retry_manager = RetryManager(RetryPolicy(
            max_retries=self.config.retry_max_retries,
            base_delay=self.config.retry_base_delay,
            max_delay=self.config.retry_max_delay,
            strategy=self.config.retry_strategy,
        ))
        
        self._timeout_handler = TimeoutHandler(
            default_timeout=self.config.default_timeout,
            max_timeout=self.config.max_timeout,
        )
        
        self._dlq = DeadLetterQueue(
            storage_dir=self.config.dlq_storage_dir,
            max_size=self.config.dlq_max_size,
            ttl_seconds=self.config.dlq_ttl_seconds,
        )
        
        self._health_checker = HealthChecker(
            default_interval=self.config.health_check_interval,
        )
        
        self._stats = {
            'total_requests': 0,
            'successful': 0,
            'failed': 0,
            'circuit_rejected': 0,
            'retries': 0,
            'timeouts': 0,
        }
        self._lock = threading.Lock()
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Get resilience statistics."""
        return {
            **self._stats,
            'circuits': self._circuit_registry.get_all_stats(),
            'dlq': self._dlq.get_stats(),
            'retry': self._retry_manager.stats.__dict__,
        }
    
    def execute(
        self,
        circuit_name: str,
        func: Callable[[], Any],
        timeout: float = None,
        max_retries: int = None,
        on_circuit_open: Callable[[], Any] = None,
        on_failure: Callable[[Exception], Any] = None,
        task_metadata: Dict[str, Any] = None,
    ) -> Any:
        """
        Execute function with full resilience protection.
        
        Order of protection (outer to inner):
        1. Circuit breaker (blocks if circuit is open)
        2. Timeout (limits execution time)
        3. Retry (retries on failure)
        
        Args:
            circuit_name: Name for circuit breaker
            func: Function to execute
            timeout: Timeout in seconds
            max_retries: Max retry attempts
            on_circuit_open: Fallback when circuit is open
            on_failure: Fallback on failure after retries
            task_metadata: Metadata for DLQ
            
        Returns:
            Function result or fallback result
            
        Raises:
            Exception: If no fallback provided and all retries exhausted
        """
        with self._lock:
            self._stats['total_requests'] += 1
        
        # Get circuit breaker
        circuit = self._circuit_registry.get_or_create(circuit_name)
        
        # Check circuit state
        if circuit.is_open:
            with self._lock:
                self._stats['circuit_rejected'] += 1
            
            if on_circuit_open:
                return on_circuit_open()
            return None
        
        # Configure retry
        retry_policy = None
        if max_retries is not None:
            retry_policy = RetryPolicy(
                max_retries=max_retries,
                base_delay=self.config.retry_base_delay,
                max_delay=self.config.retry_max_delay,
                strategy=self.config.retry_strategy,
            )
        
        retry_manager = RetryManager(retry_policy or RetryPolicy())
        
        last_error = None
        
        # Execute with retry
        try:
            result = retry_manager.execute(
                lambda: self._execute_with_timeout(
                    circuit,
                    func,
                    timeout,
                )
            )
            
            with self._lock:
                self._stats['successful'] += 1
            
            return result
            
        except TimeoutError as e:
            last_error = e
            with self._lock:
                self._stats['timeouts'] += 1
                
        except Exception as e:
            last_error = e
        
        # Record failure in DLQ
        self._dlq.add(
            task=task_metadata or {},
            error=str(last_error),
            handler=circuit_name,
            attempts=retry_manager.stats.total_attempts,
        )
        
        with self._lock:
            self._stats['failed'] += 1
        
        if on_failure:
            return on_failure(last_error)
        
        raise last_error
    
    def _execute_with_timeout(
        self,
        circuit: CircuitBreaker,
        func: Callable[[], Any],
        timeout: float = None,
    ) -> Any:
        """Execute function with timeout through circuit breaker."""
        return circuit.call(
            lambda: self._timeout_handler.execute(
                func,
                timeout=timeout,
            )
        )
    
    def protect(
        self,
        circuit_name: str,
        timeout: float = None,
        max_retries: int = None,
    ):
        """Decorator for resilience protection."""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                return self.execute(
                    circuit_name,
                    lambda: func(*args, **kwargs),
                    timeout=timeout,
                    max_retries=max_retries,
                )
            return wrapper
        return decorator
    
    def get_circuit(self, name: str) -> CircuitBreaker:
        """Get circuit breaker by name."""
        return self._circuit_registry.get_or_create(name)
    
    def reset_circuit(self, name: str):
        """Reset a circuit breaker."""
        circuit = self._circuit_registry.get(name)
        if circuit:
            circuit.reset()
    
    def reset_all_circuits(self):
        """Reset all circuit breakers."""
        self._circuit_registry.reset_all()
    
    def get_dlq(self) -> DeadLetterQueue:
        """Get dead letter queue."""
        return self._dlq
    
    def get_health_checker(self) -> HealthChecker:
        """Get health checker."""
        return self._health_checker
    
    def add_health_check(
        self,
        name: str,
        check_type: str,
        endpoint: str = None,
        port: int = None,
        host: str = "localhost",
    ):
        """Add a health check."""
        if check_type == "http":
            self._health_checker.add_http_check(name, endpoint)
        elif check_type == "tcp":
            self._health_checker.add_tcp_check(name, host, port)
        else:
            raise ValueError(f"Unknown check type: {check_type}")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of all checks."""
        results = self._health_checker.check_all()
        return {
            name: {
                'status': result.status.value,
                'message': result.message,
                'latency_ms': result.latency_ms,
            }
            for name, result in results.items()
        }
    
    def start_health_monitoring(self):
        """Start background health monitoring."""
        self._health_checker.start_background()
    
    def stop_health_monitoring(self):
        """Stop background health monitoring."""
        self._health_checker.stop_background()
    
    def get_open_circuits(self) -> List[str]:
        """Get names of all open circuits."""
        return self._circuit_registry.get_open_circuits()
    
    def get_failed_tasks(self, limit: int = 10) -> List[FailedTask]:
        """Get recent failed tasks from DLQ."""
        return self._dlq.list_unprocessed(limit=limit)
    
    def retry_failed_task(self, task_id: str) -> bool:
        """Retry a failed task from DLQ."""
        task = self._dlq.retry(task_id)
        return task is not None
    
    def clear_dlq(self):
        """Clear all tasks from DLQ."""
        self._dlq.clear()
    
    def to_dict(self) -> Dict[str, Any]:
        """Export resilience state."""
        return {
            'stats': self.stats,
            'config': {
                'circuit_failure_threshold': self.config.circuit_failure_threshold,
                'retry_max_retries': self.config.retry_max_retries,
                'default_timeout': self.config.default_timeout,
            },
            'health': self.get_health_status(),
        }
