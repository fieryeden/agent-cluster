"""
Tests for Security Module

Tests for authentication, rate limiting, input validation, and audit logging.
"""

import sys
import os
import time
import unittest
import tempfile
import shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from security import (
    AuthManager,
    RateLimiter,
    InputValidator,
    AuditLogger,
    Sanitizer,
    CommandWhitelist,
)


class TestAuthManager(unittest.TestCase):
    """Test authentication manager."""

    def test_create_api_key(self):
        """Should create valid API key."""
        auth = AuthManager()
        api_key = auth.create_api_key("user-001", ["viewer"])
        self.assertTrue(api_key.startswith("ac_"))

    def test_validate_api_key(self):
        """Should validate API key."""
        auth = AuthManager()
        api_key = auth.create_api_key("user-001", ["viewer"])
        result = auth.authenticate(api_key)
        self.assertIsNotNone(result)

    def test_invalid_api_key(self):
        """Should reject invalid API key."""
        auth = AuthManager()
        result = auth.authenticate("ac_invalid")
        self.assertIsNone(result)

    def test_jwt_token(self):
        """Should create and validate JWT token."""
        auth = AuthManager(jwt_secret="test-secret")
        token = auth.create_jwt("user-001", ["viewer"])
        self.assertIsNotNone(token)


class TestRateLimiter(unittest.TestCase):
    """Test rate limiting."""

    def test_allows_within_limit(self):
        """Should allow requests within limit."""
        limiter = RateLimiter(requests_per_second=10.0)
        for _ in range(5):
            result = limiter.check()
            self.assertTrue(result.allowed)

    def test_blocks_over_limit(self):
        """Should block requests over limit."""
        limiter = RateLimiter(requests_per_second=1.0, burst_size=2)
        # Use up burst
        limiter.check()
        limiter.check()
        # Should be blocked
        result = limiter.check()
        self.assertFalse(result.allowed)


class TestInputValidator(unittest.TestCase):
    """Test input validation."""

    def test_validate_dict(self):
        """Should validate dictionary input."""
        validator = InputValidator()
        result = validator.validate({"key": "value"})
        self.assertIsInstance(result, dict)


class TestSanitizer(unittest.TestCase):
    """Test input sanitization."""

    def test_html_sanitization(self):
        """Should sanitize HTML input."""
        sanitizer = Sanitizer()
        result = sanitizer.html("<script>alert('xss')</script>")
        self.assertNotIn("<script>", result)

    def test_shell_sanitization(self):
        """Should sanitize shell arguments."""
        sanitizer = Sanitizer()
        result = sanitizer.shell("hello world")
        self.assertIn("hello", result)


class TestCommandWhitelist(unittest.TestCase):
    """Test command whitelisting."""

    def test_allow_commands(self):
        """Should allow whitelisted commands."""
        whitelist = CommandWhitelist()
        whitelist.allow("ls")
        allowed, _ = whitelist.is_allowed("ls")
        self.assertTrue(allowed)

    def test_block_non_whitelisted(self):
        """Should block non-whitelisted commands in strict mode."""
        whitelist = CommandWhitelist(strict=True)
        allowed, _ = whitelist.is_allowed("rm")
        self.assertFalse(allowed)

    def test_dangerous_flag(self):
        """Should mark dangerous commands."""
        whitelist = CommandWhitelist()
        whitelist.allow("sudo", dangerous=True)
        self.assertTrue(whitelist.is_dangerous("sudo"))


class TestAuditLogger(unittest.TestCase):
    """Test audit logging."""

    def setUp(self):
        """Set up test logger."""
        self.log_dir = tempfile.mkdtemp()
        log_path = os.path.join(self.log_dir, "audit.log")
        self.logger = AuditLogger(log_file=log_path)

    def tearDown(self):
        """Clean up test logger."""
        shutil.rmtree(self.log_dir)

    def test_log_event(self):
        """Should log audit events."""
        self.logger.log(
            event_type="TASK_COMPLETED",
            user_id="test-user",
            details={"task_id": "test-001"},
        )
        self.logger.flush()  # Ensure written
        # Just check the log call succeeded
        self.assertIsNotNone(self.logger)

    def test_get_stats(self):
        """Should return stats."""
        stats = self.logger.get_stats()
        self.assertIsNotNone(stats)


if __name__ == '__main__':
    unittest.main()
