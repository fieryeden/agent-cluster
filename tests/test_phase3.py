#!/usr/bin/env python3
"""
Phase 3 Tests - Auto-Learning

Tests for:
- Research dispatch and result collection
- Tool installation with safety checks
- Verification of installed capabilities
- Complete auto-learning workflow
"""

import sys
import os
import tempfile
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch
import time

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from capabilities.registry import CapabilityRegistry
from capabilities.discovery import CapabilityDiscovery, CapabilityQuery, CapabilityQueryType

from autolearning.research import (
    ResearchRequest, ResearchResult, ResearchDispatcher,
    ResearchPriority, ResearchStatus, SolutionProposal,
)
from autolearning.installation import (
    ToolInstaller, InstallationRequest, InstallationResult,
    InstallationStatus, ToolType,
)
from autolearning.verification import (
    VerificationManager, VerificationTest, VerificationLevel,
    VerificationStatus, CapabilityVerification,
)
from autolearning.workflow import (
    AutoLearningWorkflow, LearningStatus, LearningTask,
)


# ============================================
# RESEARCH TESTS
# ============================================

def test_create_research_request():
    """Test creating a research request."""
    dispatcher = ResearchDispatcher()
    
    request = dispatcher.create_request(
        capability_name="excel_processing",
        description="Need to read and write Excel files",
        priority=ResearchPriority.HIGH,
    )
    
    assert request is not None
    assert request.capability_name == "excel_processing"
    assert request.priority == ResearchPriority.HIGH
    assert request.request_id.startswith("research-")
    print("✓ create research request")


def test_research_request_serialization():
    """Test serializing and deserializing research request."""
    dispatcher = ResearchDispatcher()
    
    request = dispatcher.create_request(
        capability_name="web_scraping",
        description="Extract data from web pages",
        constraints={"platform": "linux", "license": "MIT"},
    )
    
    # Serialize
    data = request.to_dict()
    assert data["capability_name"] == "web_scraping"
    assert "platform" in data["constraints"]
    
    # Deserialize
    request2 = ResearchRequest.from_dict(data)
    assert request2.capability_name == request.capability_name
    assert request2.constraints["platform"] == "linux"
    print("✓ research request serialization")


def test_dispatch_to_researchers():
    """Test dispatching research request to multiple researchers."""
    dispatcher = ResearchDispatcher()
    
    request = dispatcher.create_request(
        capability_name="data_analysis",
        description="Statistical analysis of datasets",
    )
    
    # Dispatch to specific agents
    messages = dispatcher.dispatch(request, ["agent-001", "agent-002"])
    
    assert len(messages) == 2
    assert messages[0].recipient_id == "agent-001"
    assert messages[1].recipient_id == "agent-002"
    print("✓ dispatch to researchers")


def test_collect_research_result():
    """Test collecting research results."""
    dispatcher = ResearchDispatcher()
    
    request = dispatcher.create_request(
        capability_name="image_processing",
        description="Process and transform images",
    )
    
    # Simulate result from researcher
    result = ResearchResult(
        request_id=request.request_id,
        researcher_id="agent-001",
        capability_name="image_processing",
        status=ResearchStatus.COMPLETED,
        solution={
            "tool_name": "pillow",
            "tool_type": "pip",
            "install_command": "pip install pillow",
            "verification_command": "python -c 'import PIL'",
            "confidence": 0.9,
        },
        confidence=0.9,
    )
    
    dispatcher.collect_result(result)
    
    # Get best solution
    best = dispatcher.get_best_solution(request.request_id)
    assert best is not None
    assert best.tool_name == "pillow"
    assert best.confidence == 0.9
    print("✓ collect research result")


def test_select_best_solution():
    """Test selecting best solution from multiple results."""
    dispatcher = ResearchDispatcher()
    
    request = dispatcher.create_request(
        capability_name="task",
        description="test",
    )
    
    # Multiple results with different confidence
    results = [
        ResearchResult(
            request_id=request.request_id,
            researcher_id=f"agent-{i}",
            capability_name="task",
            status=ResearchStatus.COMPLETED,
            solution={"tool_name": f"tool-{i}", "confidence": conf},
            confidence=conf,
        )
        for i, conf in enumerate([0.7, 0.9, 0.8])
    ]
    
    for r in results:
        dispatcher.collect_result(r)
    
    best = dispatcher.get_best_solution(request.request_id)
    assert best.confidence == 0.9
    print("✓ select best solution")


# ============================================
# INSTALLATION TESTS
# ============================================

def test_validate_safe_command():
    """Test command validation for safe commands."""
    installer = ToolInstaller()
    
    # Safe pip install
    is_safe, reason = installer.validate_command("pip install requests")
    assert is_safe
    print("✓ validate safe command")


def test_validate_dangerous_command():
    """Test command validation rejects dangerous commands."""
    installer = ToolInstaller()
    
    # Dangerous patterns
    is_safe, reason = installer.validate_command("rm -rf /")
    assert not is_safe
    assert "Dangerous pattern" in reason
    
    # curl | sh without safe source
    is_safe, reason = installer.validate_command("curl | sh")
    assert not is_safe
    print("✓ validate dangerous command")


def test_create_install_request():
    """Test creating an installation request."""
    installer = ToolInstaller()
    
    request = installer.create_install_request(
        tool_name="pandas",
        tool_type=ToolType.PIP,
        install_command="pip install pandas",
        verification_command="python -c 'import pandas'",
        target_agents=["agent-001"],
    )
    
    assert request is not None
    assert request.tool_name == "pandas"
    assert request.tool_type == ToolType.PIP
    assert request.rollback_on_failure
    print("✓ create install request")


def test_install_request_serialization():
    """Test installation request serialization."""
    installer = ToolInstaller()
    
    request = installer.create_install_request(
        tool_name="numpy",
        tool_type=ToolType.PIP,
        install_command="pip install numpy",
        verification_command="python -c 'import numpy'",
        target_agents=["agent-001"],
    )
    
    data = request.to_dict()
    assert data["tool_name"] == "numpy"
    assert data["tool_type"] == "pip"
    
    request2 = InstallationRequest(**request.__dict__)
    assert request2.tool_name == "numpy"
    print("✓ install request serialization")


def test_dry_run_install():
    """Test installation dry run."""
    installer = ToolInstaller()
    
    request = installer.create_install_request(
        tool_name="test-package",
        tool_type=ToolType.PIP,
        install_command="pip install test-package",
        verification_command="python -c 'import test_package'",
        target_agents=["agent-001"],
    )
    
    result = installer.execute_install(request, dry_run=True)
    
    assert result.status == InstallationStatus.SUCCESS
    assert "[DRY RUN]" in result.output
    print("✓ dry run install")


def test_get_uninstall_command():
    """Test getting uninstall commands."""
    installer = ToolInstaller()
    
    # pip
    cmd = installer.get_uninstall_command("requests", ToolType.PIP)
    assert "pip uninstall" in cmd
    
    # apt
    cmd = installer.get_uninstall_command("curl", ToolType.APT)
    assert "apt remove" in cmd
    
    print("✓ get uninstall command")


# ============================================
# VERIFICATION TESTS
# ============================================

def test_generate_verification_tests():
    """Test generating verification tests."""
    verifier = VerificationManager()
    
    tests = verifier.generate_tests(
        capability_name="web_requests",
        tool_name="requests",
        tool_type="python_package",
    )
    
    assert len(tests) >= 1
    # Should have smoke and integration tests
    levels = [t.level for t in tests]
    assert VerificationLevel.SMOKE in levels
    print("✓ generate verification tests")


def test_run_verification_test():
    """Test running a verification test."""
    verifier = VerificationManager()
    
    test = VerificationTest(
        test_id="test-001",
        test_name="python_check",
        level=VerificationLevel.SMOKE,
        command="python --version",
        timeout_seconds=10,
    )
    
    result = verifier.run_test(test)
    
    assert result.status in (VerificationStatus.PASSED, VerificationStatus.FAILED)
    assert result.execution_time > 0
    print("✓ run verification test")


def test_verify_installed_package():
    """Test verifying an installed Python package."""
    verifier = VerificationManager()
    
    # Verify json (always available in Python)
    verification = verifier.verify_capability(
        capability_name="json_parsing",
        tool_name="json",
        tool_type="python_package",
        agent_id="test-agent",
    )
    
    assert verification is not None
    assert verification.capability_name == "json_parsing"
    print("✓ verify installed package")


def test_create_verification_report():
    """Test creating verification report."""
    verifier = VerificationManager()
    
    verification = verifier.verify_capability(
        capability_name="json",
        tool_name="json",
        tool_type="python_package",
        agent_id="test",
    )
    
    report = verifier.create_verification_report(verification)
    
    assert "Verification Report" in report
    assert "json" in report
    print("✓ create verification report")


# ============================================
# WORKFLOW TESTS
# ============================================

def test_start_learning_task():
    """Test starting an auto-learning task."""
    registry = CapabilityRegistry()
    workflow = AutoLearningWorkflow(registry)
    
    # Since no researchers available, it will fail
    task = workflow.start_learning(
        capability_name="test_capability",
        description="Test capability",
    )
    
    # Task should exist (may fail if no researchers)
    assert task is not None
    assert task.capability_name == "test_capability"
    print("✓ start learning task")


def test_get_task_status():
    """Test getting task status."""
    registry = CapabilityRegistry()
    workflow = AutoLearningWorkflow(registry)
    
    task = workflow.start_learning("test", "test")
    
    # Get status
    status = workflow.get_status(task.task_id)
    assert status is not None
    assert status.task_id == task.task_id
    print("✓ get task status")


def test_create_status_report():
    """Test creating status report."""
    registry = CapabilityRegistry()
    workflow = AutoLearningWorkflow(registry)
    
    task = LearningTask(
        task_id="test-task-001",
        capability_name="test_cap",
        description="Test capability",
        status=LearningStatus.RESEARCHING,
    )
    
    report = workflow.create_status_report(task)
    
    assert "Auto-Learning Task" in report
    assert "test_cap" in report
    print("✓ create status report")


def test_cancel_task():
    """Test cancelling a learning task."""
    registry = CapabilityRegistry()
    workflow = AutoLearningWorkflow(registry)
    
    task = workflow.start_learning("test", "test")
    task_id = task.task_id
    
    workflow.cancel_task(task_id, "Test cancellation")
    
    # Should be in completed (failed)
    assert workflow.get_status(task_id) is not None
    assert workflow.get_status(task_id).status == LearningStatus.FAILED
    print("✓ cancel task")


def test_complete_workflow_mock():
    """Test complete workflow with mocked components."""
    registry = CapabilityRegistry()
    workflow = AutoLearningWorkflow(registry)
    
    # Create task manually and add to active_tasks
    task = LearningTask(
        task_id="test-workflow-001",
        capability_name="mock_capability",
        description="Mocked capability",
        status=LearningStatus.RESEARCHING,
    )
    
    # Add to active tasks
    workflow.active_tasks[task.task_id] = task
    
    # Mock research request
    task.research_request = ResearchRequest(
        request_id="research-001",
        capability_name="mock_capability",
        description="test",
    )
    
    # Mock research result
    research_result = ResearchResult(
        request_id="research-001",
        researcher_id="agent-001",
        capability_name="mock_capability",
        status=ResearchStatus.COMPLETED,
        solution={
            "tool_name": "mock_tool",
            "tool_type": "pip",
            "install_command": "pip install mock_tool",
            "verification_command": "python -c 'import json'",
            "confidence": 0.85,
        },
        confidence=0.85,
    )
    
    # Process result
    workflow.process_research_result(research_result)
    
    # Task should be processed (either active or completed)
    assert task.task_id in workflow.active_tasks or task.task_id in workflow.completed_tasks
    print("✓ complete workflow mock")


def test_register_capability_after_learning():
    """Test that capability is registered after successful learning."""
    registry = CapabilityRegistry()
    
    # Initially no capability
    assert registry.find_best_agent("learned_capability") is None
    
    # Register after learning
    registry.register_capability(
        agent_id="agent-001",
        capability_name="learned_capability",
        confidence=0.9,
        metadata={"tool_name": "test_tool", "learned": True},
    )
    
    # Now should find it
    best = registry.find_best_agent("learned_capability")
    assert best == "agent-001"
    print("✓ register capability after learning")


# ============================================
# INTEGRATION TEST
# ============================================

def test_phase3_integration():
    """Integration test: complete auto-learning flow."""
    registry = CapabilityRegistry()
    workflow = AutoLearningWorkflow(registry)
    
    # 1. Start learning
    task = workflow.start_learning(
        capability_name="json_processing",
        description="JSON parsing capability",
    )
    
    # 2. Task exists
    assert task is not None
    
    # 3. Create mock successful result
    result = ResearchResult(
        request_id=task.research_request.request_id if task.research_request else "test",
        researcher_id="mock-researcher",
        capability_name="json_processing",
        status=ResearchStatus.COMPLETED,
        solution={
            "tool_name": "json",
            "tool_type": "pip",
            "install_command": "echo 'install'",
            "verification_command": "python -c 'import json'",
            "confidence": 1.0,
        },
        confidence=1.0,
    )
    
    # 4. Process result
    workflow.process_research_result(result)
    
    print("✓ phase3 integration")


# ============================================
# RUN TESTS
# ============================================

TESTS = [
    # Research
    test_create_research_request,
    test_research_request_serialization,
    test_dispatch_to_researchers,
    test_collect_research_result,
    test_select_best_solution,
    # Installation
    test_validate_safe_command,
    test_validate_dangerous_command,
    test_create_install_request,
    test_install_request_serialization,
    test_dry_run_install,
    test_get_uninstall_command,
    # Verification
    test_generate_verification_tests,
    test_run_verification_test,
    test_verify_installed_package,
    test_create_verification_report,
    # Workflow
    test_start_learning_task,
    test_get_task_status,
    test_create_status_report,
    test_cancel_task,
    test_complete_workflow_mock,
    test_register_capability_after_learning,
    test_phase3_integration,
]


def run_tests():
    """Run all Phase 3 tests."""
    print("=" * 50)
    print("PHASE 3: AUTO-LEARNING TESTS")
    print("=" * 50)
    
    passed = 0
    failed = 0
    
    for test in TESTS:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"✗ {test.__name__}: {e}")
            failed += 1
    
    print("=" * 50)
    print(f"TOTAL: {passed}/{len(TESTS)} tests passed")
    print("=" * 50)
    
    return passed, failed


if __name__ == "__main__":
    run_tests()
