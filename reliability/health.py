"""
Health Checker for Agent Monitoring

Provides health checks with:
- HTTP endpoints
- TCP port checks
- Custom health functions
- Periodic background checking
- Auto-restart capabilities
"""

import time
import socket
import threading
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Callable, Dict, Any, List, Optional, Tuple
from enum import Enum
from concurrent.futures import ThreadPoolExecutor


class HealthStatus(Enum):
    """Health check status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    name: str
    status: HealthStatus
    message: str = ""
    latency_ms: float = 0.0
    timestamp: float = field(default_factory=time.time)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckConfig:
    """Configuration for a health check."""
    name: str
    check_type: str  # "http", "tcp", "custom"
    endpoint: Optional[str] = None  # URL for HTTP
    port: Optional[int] = None  # Port for TCP
    host: str = "localhost"
    timeout: float = 5.0
    interval: float = 30.0
    unhealthy_threshold: int = 3
    healthy_threshold: int = 2
    expected_status: int = 200  # For HTTP
    expected_content: Optional[str] = None
    check_func: Optional[Callable[[], Tuple[bool, str]]] = None  # For custom checks


class HealthChecker:
    """
    Performs health checks on agents and services.
    
    Usage:
        checker = HealthChecker()
        
        # Add HTTP check
        checker.add_http_check("api", "http://localhost:8080/health")
        
        # Add TCP check
        checker.add_tcp_check("redis", "localhost", 6379)
        
        # Add custom check
        checker.add_custom_check("memory", lambda: check_memory())
        
        # Run single check
        result = checker.check("api")
        
        # Run all checks
        results = checker.check_all()
        
        # Start background monitoring
        checker.start_background()
    """
    
    def __init__(
        self,
        default_interval: float = 30.0,
        default_timeout: float = 5.0,
    ):
        self.default_interval = default_interval
        self.default_timeout = default_timeout
        
        self._checks: Dict[str, HealthCheckConfig] = {}
        self._results: Dict[str, List[HealthCheckResult]] = {}
        self._consecutive_failures: Dict[str, int] = {}
        self._consecutive_successes: Dict[str, int] = {}
        self._callbacks: Dict[str, List[Callable[[HealthCheckResult], None]]] = {}
        
        self._background_thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.RLock()
    
    def add_http_check(
        self,
        name: str,
        url: str,
        timeout: float = None,
        interval: float = None,
        expected_status: int = 200,
        expected_content: str = None,
    ):
        """Add HTTP health check."""
        config = HealthCheckConfig(
            name=name,
            check_type="http",
            endpoint=url,
            timeout=timeout or self.default_timeout,
            interval=interval or self.default_interval,
            expected_status=expected_status,
            expected_content=expected_content,
        )
        self._checks[name] = config
        self._results[name] = []
    
    def add_tcp_check(
        self,
        name: str,
        host: str,
        port: int,
        timeout: float = None,
        interval: float = None,
    ):
        """Add TCP port health check."""
        config = HealthCheckConfig(
            name=name,
            check_type="tcp",
            host=host,
            port=port,
            timeout=timeout or self.default_timeout,
            interval=interval or self.default_interval,
        )
        self._checks[name] = config
        self._results[name] = []
    
    def add_custom_check(
        self,
        name: str,
        check_func: Callable[[], Tuple[bool, str]],
        timeout: float = None,
        interval: float = None,
    ):
        """
        Add custom health check.
        
        Args:
            name: Check name
            check_func: Function returning (healthy, message)
            timeout: Timeout in seconds
            interval: Check interval in seconds
        """
        config = HealthCheckConfig(
            name=name,
            check_type="custom",
            timeout=timeout or self.default_timeout,
            interval=interval or self.default_interval,
            check_func=check_func,  # Store check function directly
        )
        self._checks[name] = config
        self._results[name] = []
    
    def check(self, name: str) -> HealthCheckResult:
        """Run a single health check."""
        config = self._checks.get(name)
        if not config:
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNKNOWN,
                message="Check not found",
            )
        
        start_time = time.time()
        
        try:
            if config.check_type == "http":
                result = self._check_http(config)
            elif config.check_type == "tcp":
                result = self._check_tcp(config)
            elif config.check_type == "custom":
                result = self._check_custom(config)
            else:
                result = HealthCheckResult(
                    name=name,
                    status=HealthStatus.UNKNOWN,
                    message=f"Unknown check type: {config.check_type}",
                )
        except Exception as e:
            result = HealthCheckResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=str(e),
                latency_ms=(time.time() - start_time) * 1000,
            )
        
        result.latency_ms = (time.time() - start_time) * 1000
        
        # Update status based on thresholds
        self._update_status(name, result)
        
        # Store result
        with self._lock:
            self._results[name].append(result)
            # Keep last 100 results
            self._results[name] = self._results[name][-100:]
        
        # Run callbacks
        self._run_callbacks(name, result)
        
        return result
    
    def check_all(self) -> Dict[str, HealthCheckResult]:
        """Run all health checks."""
        results = {}
        for name in self._checks:
            results[name] = self.check(name)
        return results
    
    def check_parallel(self) -> Dict[str, HealthCheckResult]:
        """Run all health checks in parallel."""
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                name: executor.submit(self.check, name)
                for name in self._checks
            }
            return {name: future.result() for name, future in futures.items()}
    
    def get_status(self, name: str = None) -> HealthStatus:
        """Get overall health status."""
        if name:
            # Get status for specific check
            results = self._results.get(name, [])
            if not results:
                return HealthStatus.UNKNOWN
            return results[-1].status if results else HealthStatus.UNKNOWN
        
        # Get overall status
        if not self._checks:
            return HealthStatus.UNKNOWN
        
        statuses = [self.get_status(n) for n in self._checks]
        
        if any(s == HealthStatus.UNHEALTHY for s in statuses):
            return HealthStatus.UNHEALTHY
        if any(s == HealthStatus.DEGRADED for s in statuses):
            return HealthStatus.DEGRADED
        if all(s == HealthStatus.HEALTHY for s in statuses):
            return HealthStatus.HEALTHY
        return HealthStatus.UNKNOWN
    
    def get_results(self, name: str = None, limit: int = 10) -> List[HealthCheckResult]:
        """Get recent check results."""
        if name:
            return self._results.get(name, [])[-limit:]
        
        all_results = []
        for results in self._results.values():
            all_results.extend(results)
        
        all_results.sort(key=lambda r: r.timestamp, reverse=True)
        return all_results[:limit]
    
    def on_status_change(
        self,
        name: str,
        callback: Callable[[HealthCheckResult], None],
    ):
        """Register callback for status changes."""
        if name not in self._callbacks:
            self._callbacks[name] = []
        self._callbacks[name].append(callback)
    
    def start_background(self):
        """Start background health monitoring."""
        if self._running:
            return
        
        self._running = True
        self._background_thread = threading.Thread(target=self._background_loop, daemon=True)
        self._background_thread.start()
    
    def stop_background(self):
        """Stop background monitoring."""
        self._running = False
        if self._background_thread:
            self._background_thread.join(timeout=5)
            self._background_thread = None
    
    def _check_http(self, config: HealthCheckConfig) -> HealthCheckResult:
        """Perform HTTP health check."""
        try:
            start = time.time()
            req = urllib.request.Request(config.endpoint, method='GET')
            response = urllib.request.urlopen(req, timeout=config.timeout)
            
            status_code = response.status
            content = response.read().decode('utf-8', errors='replace')
            
            if status_code != config.expected_status:
                return HealthCheckResult(
                    name=config.name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Unexpected status: {status_code}",
                )
            
            if config.expected_content and config.expected_content not in content:
                return HealthCheckResult(
                    name=config.name,
                    status=HealthStatus.DEGRADED,
                    message="Expected content not found",
                )
            
            return HealthCheckResult(
                name=config.name,
                status=HealthStatus.HEALTHY,
                message="OK",
                latency_ms=(time.time() - start) * 1000,
            )
            
        except urllib.error.HTTPError as e:
            return HealthCheckResult(
                name=config.name,
                status=HealthStatus.UNHEALTHY,
                message=f"HTTP {e.code}: {e.reason}",
            )
        except urllib.error.URLError as e:
            return HealthCheckResult(
                name=config.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Connection error: {e.reason}",
            )
        except Exception as e:
            return HealthCheckResult(
                name=config.name,
                status=HealthStatus.UNHEALTHY,
                message=str(e),
            )
    
    def _check_tcp(self, config: HealthCheckConfig) -> HealthCheckResult:
        """Perform TCP port health check."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(config.timeout)
            
            start = time.time()
            result = sock.connect_ex((config.host, config.port))
            latency = (time.time() - start) * 1000
            sock.close()
            
            if result == 0:
                return HealthCheckResult(
                    name=config.name,
                    status=HealthStatus.HEALTHY,
                    message="Port open",
                    latency_ms=latency,
                )
            else:
                return HealthCheckResult(
                    name=config.name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Port closed (error {result})",
                )
                
        except socket.timeout:
            return HealthCheckResult(
                name=config.name,
                status=HealthStatus.UNHEALTHY,
                message="Connection timeout",
            )
        except Exception as e:
            return HealthCheckResult(
                name=config.name,
                status=HealthStatus.UNHEALTHY,
                message=str(e),
            )
    
    def _check_custom(self, config: HealthCheckConfig) -> HealthCheckResult:
        """Perform custom health check."""
        check_func = config.check_func
        if not check_func:
            return HealthCheckResult(
                name=config.name,
                status=HealthStatus.UNKNOWN,
                message="No check function configured",
            )
        
        try:
            healthy, message = check_func()
            return HealthCheckResult(
                name=config.name,
                status=HealthStatus.HEALTHY if healthy else HealthStatus.UNHEALTHY,
                message=message,
            )
        except Exception as e:
            return HealthCheckResult(
                name=config.name,
                status=HealthStatus.UNHEALTHY,
                message=str(e),
            )
    
    def _update_status(self, name: str, result: HealthCheckResult):
        """Update status based on thresholds."""
        with self._lock:
            if result.status == HealthStatus.HEALTHY:
                self._consecutive_successes[name] = self._consecutive_successes.get(name, 0) + 1
                self._consecutive_failures[name] = 0
            else:
                self._consecutive_failures[name] = self._consecutive_failures.get(name, 0) + 1
                self._consecutive_successes[name] = 0
            
            config = self._checks.get(name)
            if config:
                # Transition logic
                failures = self._consecutive_failures.get(name, 0)
                successes = self._consecutive_successes.get(name, 0)
                
                if failures >= config.unhealthy_threshold:
                    result.status = HealthStatus.UNHEALTHY
                elif successes >= config.healthy_threshold:
                    result.status = HealthStatus.HEALTHY
    
    def _run_callbacks(self, name: str, result: HealthCheckResult):
        """Run registered callbacks."""
        callbacks = self._callbacks.get(name, [])
        for callback in callbacks:
            try:
                callback(result)
            except Exception:
                pass  # Don't let callback errors propagate
    
    def _background_loop(self):
        """Background monitoring loop."""
        while self._running:
            # Group checks by interval
            intervals = set(c.interval for c in self._checks.values())
            
            for interval in intervals:
                # Run checks for this interval
                for name, config in self._checks.items():
                    if config.interval == interval:
                        self.check(name)
            
            # Sleep for shortest interval
            min_interval = min(intervals) if intervals else 30
            time.sleep(min_interval)
