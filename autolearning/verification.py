#!/usr/bin/env python3
"""
Verification Module

Verifies that installed capabilities work correctly.

Three-level verification:
1. **Smoke Test**: Basic functionality check
2. **Integration Test**: Works with expected inputs
3. **Performance Test**: Meets performance requirements

If verification fails at any level, triggers rollback.
"""

import subprocess
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))


class VerificationLevel(Enum):
    """Levels of verification depth."""
    SMOKE = "smoke"          # Basic functionality
    INTEGRATION = "integration"  # Works with expected inputs
    PERFORMANCE = "performance"  # Meets performance requirements


class VerificationStatus(Enum):
    """Status of verification."""
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class VerificationTest:
    """A single verification test."""
    
    test_id: str
    test_name: str
    level: VerificationLevel
    command: str
    expected_output: Optional[str] = None
    expected_exit_code: int = 0
    timeout_seconds: int = 30
    required: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_id": self.test_id,
            "test_name": self.test_name,
            "level": self.level.value,
            "command": self.command,
            "expected_output": self.expected_output,
            "expected_exit_code": self.expected_exit_code,
            "timeout_seconds": self.timeout_seconds,
            "required": self.required,
        }


@dataclass
class VerificationResult:
    """Result of a verification test."""
    
    test_id: str
    test_name: str
    level: VerificationLevel
    status: VerificationStatus
    output: str = ""
    error: Optional[str] = None
    execution_time: float = 0.0
    passed: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_id": self.test_id,
            "test_name": self.test_name,
            "level": self.level.value,
            "status": self.status.value,
            "output": self.output,
            "error": self.error,
            "execution_time": self.execution_time,
            "passed": self.passed,
        }


@dataclass
class CapabilityVerification:
    """Complete verification of a capability."""
    
    verification_id: str
    capability_name: str
    agent_id: str
    tool_name: str
    smoke_result: Optional[VerificationResult] = None
    integration_result: Optional[VerificationResult] = None
    performance_result: Optional[VerificationResult] = None
    overall_status: VerificationStatus = VerificationStatus.PENDING
    verified_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "verification_id": self.verification_id,
            "capability_name": self.capability_name,
            "agent_id": self.agent_id,
            "tool_name": self.tool_name,
            "smoke_result": self.smoke_result.to_dict() if self.smoke_result else None,
            "integration_result": self.integration_result.to_dict() if self.integration_result else None,
            "performance_result": self.performance_result.to_dict() if self.performance_result else None,
            "overall_status": self.overall_status.value,
            "verified_at": self.verified_at.isoformat(),
        }
    
    @property
    def is_verified(self) -> bool:
        """Check if capability is verified at all levels."""
        return (
            self.smoke_result is not None and
            self.smoke_result.passed and
            self.integration_result is not None and
            self.integration_result.passed
        )


class VerificationManager:
    """
    Manages verification of installed capabilities.
    
    Workflow:
    1. Generate verification tests for capability
    2. Run tests at each level
    3. Collect results
    4. Report overall verification status
    5. Trigger rollback if critical tests fail
    """
    
    # Standard tests by capability type
    STANDARD_TESTS = {
        "python_package": {
            "smoke": "python -c 'import {package}'",
            "integration": "python -c 'import {package}; {package}.__version__'",
        },
        "cli_tool": {
            "smoke": "{tool} --version",
            "integration": "{tool} --help",
        },
        "system_package": {
            "smoke": "which {tool}",
            "integration": "{tool} --version || dpkg -l {tool}",
        },
    }
    
    def __init__(self, registry: Any = None):
        """
        Initialize verification manager.
        
        Args:
            registry: CapabilityRegistry for updating capability status
        """
        self.registry = registry
        self.verifications: Dict[str, CapabilityVerification] = {}
        self._rollback_handlers: Dict[str, Callable] = {}
    
    def generate_tests(
        self,
        capability_name: str,
        tool_name: str,
        tool_type: str,
    ) -> List[VerificationTest]:
        """
        Generate standard verification tests for a capability.
        
        Args:
            capability_name: Name of the capability
            tool_name: Name of the tool
            tool_type: Type (python_package, cli_tool, system_package)
        
        Returns:
            List of VerificationTest instances
        """
        import uuid
        
        tests = []
        template = self.STANDARD_TESTS.get(tool_type, {})
        
        # Smoke test
        if "smoke" in template:
            tests.append(VerificationTest(
                test_id=f"test-{uuid.uuid4().hex[:8]}",
                test_name=f"{tool_name}_smoke",
                level=VerificationLevel.SMOKE,
                command=template["smoke"].format(package=tool_name, tool=tool_name),
                timeout_seconds=10,
                required=True,
            ))
        
        # Integration test
        if "integration" in template:
            tests.append(VerificationTest(
                test_id=f"test-{uuid.uuid4().hex[:8]}",
                test_name=f"{tool_name}_integration",
                level=VerificationLevel.INTEGRATION,
                command=template["integration"].format(package=tool_name, tool=tool_name),
                timeout_seconds=30,
                required=True,
            ))
        
        return tests
    
    def run_test(self, test: VerificationTest) -> VerificationResult:
        """
        Run a single verification test.
        
        Args:
            test: VerificationTest to run
        
        Returns:
            VerificationResult
        """
        result = VerificationResult(
            test_id=test.test_id,
            test_name=test.test_name,
            level=test.level,
            status=VerificationStatus.RUNNING,
        )
        
        start_time = time.time()
        
        try:
            proc = subprocess.run(
                test.command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=test.timeout_seconds,
            )
            
            result.output = proc.stdout
            result.execution_time = time.time() - start_time
            result.passed = proc.returncode == test.expected_exit_code
            
            if test.expected_output and test.expected_output in proc.stdout:
                result.passed = True
            
            result.status = VerificationStatus.PASSED if result.passed else VerificationStatus.FAILED
            
            if not result.passed:
                result.error = proc.stderr or f"Exit code: {proc.returncode}"
                
        except subprocess.TimeoutExpired:
            result.status = VerificationStatus.FAILED
            result.error = f"Timeout after {test.timeout_seconds}s"
            result.execution_time = test.timeout_seconds
            
        except Exception as e:
            result.status = VerificationStatus.FAILED
            result.error = str(e)
        
        return result
    
    def verify_capability(
        self,
        capability_name: str,
        tool_name: str,
        tool_type: str,
        agent_id: str,
        custom_tests: Optional[List[VerificationTest]] = None,
    ) -> CapabilityVerification:
        """
        Verify an installed capability.
        
        Args:
            capability_name: Name of capability
            tool_name: Name of tool
            tool_type: Type of tool
            agent_id: Agent that installed the capability
            custom_tests: Optional custom tests
        
        Returns:
            CapabilityVerification with results
        """
        import uuid
        
        verification = CapabilityVerification(
            verification_id=f"verify-{uuid.uuid4().hex[:12]}",
            capability_name=capability_name,
            agent_id=agent_id,
            tool_name=tool_name,
        )
        
        # Generate or use custom tests
        tests = custom_tests or self.generate_tests(capability_name, tool_name, tool_type)
        
        # Run tests by level
        for test in tests:
            result = self.run_test(test)
            
            if test.level == VerificationLevel.SMOKE:
                verification.smoke_result = result
            elif test.level == VerificationLevel.INTEGRATION:
                verification.integration_result = result
            elif test.level == VerificationLevel.PERFORMANCE:
                verification.performance_result = result
            
            # Stop on required test failure
            if test.required and not result.passed:
                break
        
        # Determine overall status
        if verification.smoke_result and not verification.smoke_result.passed:
            verification.overall_status = VerificationStatus.FAILED
        elif verification.integration_result and not verification.integration_result.passed:
            verification.overall_status = VerificationStatus.FAILED
        else:
            verification.overall_status = VerificationStatus.PASSED
        
        self.verifications[verification.verification_id] = verification
        return verification
    
    def register_rollback_handler(
        self,
        capability_name: str,
        handler: Callable,
    ):
        """
        Register a rollback handler for a capability.
        
        Args:
            capability_name: Name of capability
            handler: Callable to execute for rollback
        """
        self._rollback_handlers[capability_name] = handler
    
    def trigger_rollback(
        self,
        verification: CapabilityVerification,
        reason: str,
    ) -> bool:
        """
        Trigger rollback for a failed verification.
        
        Args:
            verification: Failed verification
            reason: Reason for rollback
        
        Returns:
            True if rollback succeeded
        """
        handler = self._rollback_handlers.get(verification.capability_name)
        if handler:
            try:
                return handler(verification)
            except Exception:
                return False
        return False
    
    def create_verification_report(
        self,
        verification: CapabilityVerification,
    ) -> str:
        """
        Create a human-readable verification report.
        
        Args:
            verification: Verification to report on
        
        Returns:
            Report string
        """
        lines = [
            f"Verification Report: {verification.capability_name}",
            f"=" * 50,
            f"Agent: {verification.agent_id}",
            f"Tool: {verification.tool_name}",
            f"Status: {verification.overall_status.value}",
            f"",
        ]
        
        for level_name, result in [
            ("Smoke Test", verification.smoke_result),
            ("Integration Test", verification.integration_result),
            ("Performance Test", verification.performance_result),
        ]:
            if result:
                status_icon = "✓" if result.passed else "✗"
                lines.append(f"{status_icon} {level_name}: {result.status.value}")
                if result.error:
                    lines.append(f"  Error: {result.error}")
                lines.append(f"  Time: {result.execution_time:.2f}s")
            else:
                lines.append(f"○ {level_name}: skipped")
        
        return "\n".join(lines)


# Convenience functions

def quick_verify(package: str, package_type: str = "python_package") -> bool:
    """
    Quick verification of an installed package.
    
    Args:
        package: Package name
        package_type: Type of package
    
    Returns:
        True if verification passed
    """
    manager = VerificationManager()
    verification = manager.verify_capability(
        capability_name=package,
        tool_name=package,
        tool_type=package_type,
        agent_id="local",
    )
    return verification.is_verified
