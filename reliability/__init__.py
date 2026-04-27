"""
Reliability Module for Agent Cluster

Provides production-grade reliability patterns:
- CircuitBreaker: Stop sending to failing agents
- RetryManager: Exponential backoff retries
- DeadLetterQueue: Store failed tasks for analysis
- TimeoutHandler: Configurable task timeouts
- HealthChecker: Monitor agent health
- ResourceManager: Resource limits (memory, CPU, file handles)
"""

from reliability.circuit_breaker import CircuitBreaker, CircuitState
from reliability.retry import RetryManager, RetryPolicy
from reliability.dead_letter import DeadLetterQueue, FailedTask
from reliability.timeout import TimeoutHandler, TimeoutError
from reliability.health import HealthChecker, HealthStatus
from reliability.manager import ResilienceManager

__all__ = [
    'CircuitBreaker',
    'CircuitState',
    'RetryManager',
    'RetryPolicy',
    'DeadLetterQueue',
    'FailedTask',
    'TimeoutHandler',
    'TimeoutError',
    'HealthChecker',
    'HealthStatus',
    'ResilienceManager',
]
