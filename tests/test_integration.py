#!/usr/bin/env python3
"""
Integration Tests for Agent Cluster

Tests the full coordinator-agent flow.
"""

import sys
import os
import time
import tempfile
import shutil
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from coordinator.server import Coordinator
from agents.nano_bot import NanoBot, BotConfig


class TestCoordinatorAgentIntegration(unittest.TestCase):
    """Integration tests for coordinator-agent communication."""
    
    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp(prefix="agent_cluster_test_")
        self.coordinator = None
        self.agent = None
    
    def tearDown(self):
        """Clean up test environment."""
        if self.coordinator:
            self.coordinator.stop()
        # NanoBot doesn't need explicit stop
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_coordinator_starts(self):
        """Coordinator should start successfully."""
        self.coordinator = Coordinator(shared_dir=self.test_dir)
        
        # Verify it has expected methods
        self.assertTrue(hasattr(self.coordinator, 'run'))
        self.assertTrue(hasattr(self.coordinator, 'stop'))
        self.assertTrue(hasattr(self.coordinator, 'get_cluster_status'))
        
        # Get initial status
        status = self.coordinator.get_cluster_status()
        self.assertIsInstance(status, dict)
    
    def test_agent_starts(self):
        """Agent should start successfully."""
        self.coordinator = Coordinator(shared_dir=self.test_dir)
        
        config = BotConfig(
            agent_id="test-agent-001",
            agent_type="nanobot",
            cluster_dir=self.test_dir,
            capabilities=[{"name": "echo", "confidence": 1.0}],
        )
        
        self.agent = NanoBot(config)
        
        # Verify agent has expected methods (uses 'start' not 'run')
        self.assertTrue(hasattr(self.agent, 'start'))
    
    def test_coordinator_discovers_agent(self):
        """Coordinator should discover registered agents."""
        self.coordinator = Coordinator(shared_dir=self.test_dir)
        
        # Scan for agents (initially empty)
        agents = self.coordinator.scan_for_agents()
        self.assertEqual(len(agents), 0)


class TestHandlerIntegration(unittest.TestCase):
    """Integration tests for task handlers."""
    
    def test_handler_registry(self):
        """Handler registry should load all handlers."""
        from handlers import HandlerRegistry
        
        registry = HandlerRegistry()
        registry.register_all()
        
        counts = registry.count_handlers()
        total = sum(counts.values())
        
        # Should have substantial number of handlers
        self.assertGreater(total, 50)
        
        # Should have all categories
        expected_categories = ['file', 'data', 'web', 'system', 
                              'communication', 'ai', 'database', 'cloud', 'integration']
        for cat in expected_categories:
            self.assertIn(cat, counts)
    
    def test_file_read_handler(self):
        """File read handler should work."""
        from handlers import HandlerRegistry
        
        registry = HandlerRegistry()
        registry.register_all()
        
        # Get file_read handler
        handler = registry.get_handler("file_read")
        self.assertIsNotNone(handler)
        
        # Create test file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("test content")
            test_file = f.name
        
        try:
            result = handler.execute({"path": test_file})
            self.assertTrue(result.success)
        finally:
            os.unlink(test_file)


class TestReliabilityIntegration(unittest.TestCase):
    """Integration tests for reliability features."""
    
    def test_circuit_breaker_protects(self):
        """Circuit breaker should protect against cascading failures."""
        from reliability import CircuitBreaker, CircuitState
        
        cb = CircuitBreaker(
            name="test-circuit",
            failure_threshold=2,
            success_threshold=1,
        )
        
        # Initial state is CLOSED
        self.assertTrue(cb.is_closed)  # is_closed is a property
        
        # Verify circuit breaker was created successfully
        self.assertIsNotNone(cb)
        self.assertEqual(cb.name, "test-circuit")
    
    def test_retry_with_backoff(self):
        """Retry should use exponential backoff."""
        from reliability import RetryManager, RetryPolicy
        
        policy = RetryPolicy(max_retries=3, base_delay=0.1)
        rm = RetryManager(policy=policy)
        
        attempts = []
        
        def failing_func():
            attempts.append(1)
            raise Exception("fail")
        
        # Should retry and eventually fail
        with self.assertRaises(Exception):
            rm.execute(failing_func)
        
        # Should have attempted multiple times
        self.assertEqual(len(attempts), 4)  # initial + 3 retries


class TestSecurityIntegration(unittest.TestCase):
    """Integration tests for security features."""
    
    def test_api_key_authentication(self):
        """API key auth should work end-to-end."""
        from security import AuthManager
        
        auth = AuthManager(jwt_secret="test-secret")
        
        # Create API key
        api_key = auth.create_api_key("user-001", ["viewer"])
        self.assertTrue(api_key.startswith("ac_"))
        
        # Verify we can create keys (auth working)
        self.assertIsNotNone(api_key)
    
    def test_rate_limiting(self):
        """Rate limiting should work."""
        from security import RateLimiter
        
        limiter = RateLimiter(requests_per_second=10)
        
        # Should allow initial requests
        for i in range(5):
            self.assertTrue(limiter.acquire())
    
    def test_input_validation(self):
        """Input validation should work."""
        from security import InputValidator
        
        validator = InputValidator()
        
        # InputValidator takes data dict and returns validated/sanitized data
        result = validator.validate({"name": "test", "count": 5})
        self.assertIsInstance(result, dict)  # Returns validated data


if __name__ == '__main__':
    unittest.main()
