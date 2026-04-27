"""
Tests for Reliability Module

Tests for circuit breaker, retry, dead letter queue, timeout, and health checking.
"""

import sys
import os
import time
import unittest
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from reliability import (
    CircuitBreaker,
    CircuitState,
    RetryManager,
    RetryPolicy,
    DeadLetterQueue,
    TimeoutHandler,
    HealthChecker,
    HealthStatus,
    ResilienceManager,
)


class TestCircuitBreaker(unittest.TestCase):
    """Test circuit breaker pattern."""

    def test_starts_closed(self):
        """Should start in closed state."""
        cb = CircuitBreaker(name="test-circuit")
        self.assertTrue(cb.is_closed)

    def test_opens_after_failures(self):
        """Should open after threshold failures."""
        cb = CircuitBreaker(
            name="test-circuit",
            failure_threshold=2,
        )

        # Simulate failures using call()
        for _ in range(3):
            try:
                def failing():
                    raise Exception("fail")
                cb.call(failing)
            except:
                pass

        self.assertTrue(cb.is_open)

    def test_allows_when_closed(self):
        """Should allow execution when closed."""
        cb = CircuitBreaker(name="test-circuit")
        
        def success():
            return "ok"
        
        result = cb.call(success)  # Use call() not protect()
        self.assertEqual(result, "ok")


class TestRetryManager(unittest.TestCase):
    """Test retry with backoff."""

    def test_retries_on_failure(self):
        """Should retry failed operations."""
        policy = RetryPolicy(max_retries=3, base_delay=0.01)
        rm = RetryManager(policy=policy)
        
        attempts = [0]
        
        def failing():
            attempts[0] += 1
            raise Exception("fail")
        
        with self.assertRaises(Exception):
            rm.execute(failing)
        
        self.assertEqual(attempts[0], 4)  # initial + 3 retries

    def test_succeeds_after_retry(self):
        """Should succeed if retry works."""
        policy = RetryPolicy(max_retries=3, base_delay=0.01)
        rm = RetryManager(policy=policy)
        
        attempts = [0]
        
        def eventually_succeeds():
            attempts[0] += 1
            if attempts[0] < 3:
                raise Exception("fail")
            return "success"
        
        result = rm.execute(eventually_succeeds)
        self.assertEqual(result, "success")


class TestDeadLetterQueue(unittest.TestCase):
    """Test dead letter queue."""

    def setUp(self):
        """Set up test DLQ."""
        self.test_dir = tempfile.mkdtemp(prefix="test_dlq_")
        self.dlq = DeadLetterQueue(storage_dir=self.test_dir)

    def tearDown(self):
        """Clean up test DLQ."""
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_add_failed_task(self):
        """Should add failed task to queue."""
        failed_task = self.dlq.add(
            task={"task_id": "test-1"},
            error="Test error"
        )
        self.assertIsNotNone(failed_task.id)

    def test_get_failed_task(self):
        """Should retrieve failed task."""
        task = self.dlq.add(
            task={"task_id": "test-2"},
            error="Test error"
        )
        retrieved = self.dlq.get(task.id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.id, task.id)

    def test_list_tasks(self):
        """Should list all failed tasks."""
        for i in range(5):
            self.dlq.add(
                task={"index": i},
                error=f"Error {i}"
            )
        tasks = self.dlq.list_all()
        self.assertEqual(len(tasks), 5)

    def test_clear_tasks(self):
        """Should clear all tasks."""
        self.dlq.add(task={"test": "data"}, error="Error")
        self.dlq.clear()
        tasks = self.dlq.list_all()
        self.assertEqual(len(tasks), 0)


class TestTimeoutHandler(unittest.TestCase):
    """Test timeout handling."""

    def test_completes_within_timeout(self):
        """Should complete if within timeout."""
        handler = TimeoutHandler(default_timeout=5.0)
        result = handler.execute(lambda: "done")
        self.assertEqual(result, "done")

    def test_times_out_slow_task(self):
        """Should raise timeout for slow tasks."""
        from reliability import TimeoutError as ReliabilityTimeoutError
        handler = TimeoutHandler(default_timeout=0.1)
        
        def slow():
            time.sleep(1.0)
            return "too late"
        
        with self.assertRaises(ReliabilityTimeoutError):
            handler.execute(slow)


class TestHealthChecker(unittest.TestCase):
    """Test health checking."""

    def test_add_custom_check(self):
        """Should add custom health check."""
        checker = HealthChecker()
        checker.add_custom_check("test", lambda: (True, "OK"))
        result = checker.check("test")
        self.assertEqual(result.status, HealthStatus.HEALTHY)

    def test_check_not_found(self):
        """Should return unknown for missing check."""
        checker = HealthChecker()
        result = checker.check("nonexistent")
        self.assertEqual(result.status, HealthStatus.UNKNOWN)

    def test_custom_check_failure(self):
        """Should handle failed health check."""
        checker = HealthChecker()
        checker.add_custom_check("failing", lambda: (False, "Error"))
        result = checker.check("failing")
        self.assertEqual(result.status, HealthStatus.UNHEALTHY)


class TestResilienceManager(unittest.TestCase):
    """Test unified resilience manager."""

    def test_initialization(self):
        """Should initialize with default config."""
        manager = ResilienceManager()
        self.assertIsNotNone(manager)

    def test_has_components(self):
        """Should have all resilience components."""
        manager = ResilienceManager()
        
        # Check that manager has the expected methods
        self.assertTrue(hasattr(manager, 'execute'))
        self.assertTrue(hasattr(manager, 'protect'))


if __name__ == '__main__':
    unittest.main()
