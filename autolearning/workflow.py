#!/usr/bin/env python3
"""
Auto-Learning Workflow

Orchestrates the complete auto-learning flow:
1. Capability gap detected
2. Research dispatched
3. Solutions evaluated
4. Tool installed across cluster
5. Verification performed
6. Capability registered

This is the top-level coordinator for Phase 3.
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import BaseMessage, MessageType, create_message
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
    VerificationManager, VerificationStatus, CapabilityVerification,
)


class LearningStatus(Enum):
    """Status of an auto-learning workflow."""
    IDLE = "idle"
    RESEARCHING = "researching"
    INSTALLING = "installing"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class LearningTask:
    """A single auto-learning task."""
    
    task_id: str
    capability_name: str
    description: str
    status: LearningStatus = LearningStatus.IDLE
    research_request: Optional[ResearchRequest] = None
    research_results: List[ResearchResult] = field(default_factory=list)
    selected_solution: Optional[SolutionProposal] = None
    installation_requests: List[InstallationRequest] = field(default_factory=list)
    installation_results: List[InstallationResult] = field(default_factory=list)
    verification: Optional[CapabilityVerification] = None
    error: Optional[str] = None
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "capability_name": self.capability_name,
            "description": self.description,
            "status": self.status.value,
            "research_request": self.research_request.to_dict() if self.research_request else None,
            "research_results": [r.to_dict() for r in self.research_results],
            "selected_solution": self.selected_solution.to_dict() if self.selected_solution else None,
            "installation_results": [r.to_dict() for r in self.installation_results],
            "verification": self.verification.to_dict() if self.verification else None,
            "error": self.error,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class AutoLearningWorkflow:
    """
    Orchestrates the complete auto-learning workflow.
    
    Usage:
        workflow = AutoLearningWorkflow(registry, message_queue)
        
        # Start learning a new capability
        task = workflow.start_learning("excel_processing", "Need to read/write Excel files")
        
        # Process incoming messages
        workflow.process_message(incoming_message)
        
        # Check status
        status = workflow.get_status(task.task_id)
    """
    
    def __init__(
        self,
        registry: CapabilityRegistry,
        message_queue: Any = None,
        coordinator_id: str = "coordinator",
    ):
        """
        Initialize auto-learning workflow.
        
        Args:
            registry: CapabilityRegistry for tracking capabilities
            message_queue: Message queue for sending messages
            coordinator_id: ID of the coordinator agent
        """
        self.registry = registry
        self.message_queue = message_queue
        self.coordinator_id = coordinator_id
        
        self.research_dispatcher = ResearchDispatcher(registry)
        self.installer = ToolInstaller(coordinator_id)
        self.verifier = VerificationManager(registry)
        
        self.active_tasks: Dict[str, LearningTask] = {}
        self.completed_tasks: Dict[str, LearningTask] = {}
        
        # Callbacks for status updates
        self._status_callbacks: List[Callable] = []
    
    def register_status_callback(self, callback: Callable):
        """Register a callback for status updates."""
        self._status_callbacks.append(callback)
    
    def _notify_status(self, task: LearningTask):
        """Notify all registered callbacks of status update."""
        for callback in self._status_callbacks:
            try:
                callback(task)
            except Exception:
                pass
    
    def start_learning(
        self,
        capability_name: str,
        description: str,
        priority: ResearchPriority = ResearchPriority.NORMAL,
        target_agents: Optional[List[str]] = None,
    ) -> LearningTask:
        """
        Start an auto-learning workflow for a capability.
        
        Args:
            capability_name: Name of capability to learn
            description: What needs to be learned
            priority: Priority of the learning task
            target_agents: Specific agents to target (optional)
        
        Returns:
            LearningTask instance
        """
        import uuid
        
        task = LearningTask(
            task_id=f"learn-{uuid.uuid4().hex[:12]}",
            capability_name=capability_name,
            description=description,
            status=LearningStatus.RESEARCHING,
        )
        
        self.active_tasks[task.task_id] = task
        
        # Create research request
        research_request = self.research_dispatcher.create_request(
            capability_name=capability_name,
            description=description,
            priority=priority,
        )
        task.research_request = research_request
        
        # Find researchers
        researchers = target_agents or self.research_dispatcher.find_researchers()
        
        if not researchers:
            task.status = LearningStatus.FAILED
            task.error = "No agents available for research"
            self._complete_task(task)
            return task
        
        # Dispatch research request
        messages = self.research_dispatcher.dispatch(research_request, researchers)
        
        # Store messages for sending (if message_queue available)
        if self.message_queue:
            for msg in messages:
                self.message_queue.send(msg)
        
        self._notify_status(task)
        return task
    
    def process_research_result(self, result: ResearchResult):
        """
        Process an incoming research result.
        
        Args:
            result: ResearchResult from a researcher
        """
        # Find the corresponding task
        task = None
        for t in self.active_tasks.values():
            if t.research_request and t.research_request.request_id == result.request_id:
                task = t
                break
        
        if not task:
            return
        
        # Store result
        task.research_results.append(result)
        
        # Check if we have enough results
        successful_results = [
            r for r in task.research_results
            if r.status == ResearchStatus.COMPLETED
        ]
        
        # Evaluate solutions when we have enough data
        if len(successful_results) >= 1:
            self._evaluate_solutions(task)
    
    def _evaluate_solutions(self, task: LearningTask):
        """
        Evaluate research solutions and select best one.
        
        Args:
            task: LearningTask to evaluate
        """
        best_solution = self.research_dispatcher.get_best_solution(
            task.research_request.request_id
        )
        
        if not best_solution:
            task.status = LearningStatus.FAILED
            task.error = "No valid solutions found"
            self._complete_task(task)
            return
        
        task.selected_solution = best_solution
        task.status = LearningStatus.INSTALLING
        
        # Create installation request
        install_request = self.installer.create_install_request(
            tool_name=best_solution.tool_name,
            tool_type=ToolType(best_solution.tool_type),
            install_command=best_solution.install_command,
            verification_command=best_solution.verification_command,
            target_agents=[],  # Will be set based on which agents need this
        )
        task.installation_requests.append(install_request)
        
        self._notify_status(task)
        
        # Execute installation (local agent)
        result = self.installer.execute_install(install_request)
        task.installation_results.append(result)
        
        if result.status in (InstallationStatus.SUCCESS,):
            self._verify_installation(task)
        elif result.status in (InstallationStatus.FAILED, InstallationStatus.ROLLED_BACK):
            task.status = LearningStatus.FAILED
            task.error = f"Installation failed: {result.error}"
            self._complete_task(task)
    
    def _verify_installation(self, task: LearningTask):
        """
        Verify the installation.
        
        Args:
            task: LearningTask to verify
        """
        task.status = LearningStatus.VERIFYING
        self._notify_status(task)
        
        if not task.selected_solution or not task.installation_results:
            task.status = LearningStatus.FAILED
            task.error = "No installation to verify"
            self._complete_task(task)
            return
        
        solution = task.selected_solution
        install_result = task.installation_results[0]
        
        verification = self.verifier.verify_capability(
            capability_name=task.capability_name,
            tool_name=solution.tool_name,
            tool_type=solution.tool_type,
            agent_id=install_result.agent_id,
        )
        
        task.verification = verification
        
        if verification.is_verified:
            # Register the capability
            self.registry.register_capability(
                agent_id=install_result.agent_id,
                capability_name=task.capability_name,
                confidence=solution.confidence,
                metadata={
                    "tool_name": solution.tool_name,
                    "tool_type": solution.tool_type,
                    "learned_at": datetime.now().isoformat(),
                }
            )
            task.status = LearningStatus.COMPLETED
        else:
            task.status = LearningStatus.FAILED
            task.error = "Verification failed"
            
            # Trigger rollback
            self.verifier.trigger_rollback(verification, "Verification failed")
        
        self._complete_task(task)
    
    def _complete_task(self, task: LearningTask):
        """
        Mark task as complete and move to completed list.
        
        Args:
            task: Task to complete
        """
        task.completed_at = datetime.now()
        self._notify_status(task)
        
        if task.task_id in self.active_tasks:
            del self.active_tasks[task.task_id]
        
        self.completed_tasks[task.task_id] = task
    
    def get_status(self, task_id: str) -> Optional[LearningTask]:
        """
        Get status of a learning task.
        
        Args:
            task_id: ID of the task
        
        Returns:
            LearningTask or None
        """
        return self.active_tasks.get(task_id) or self.completed_tasks.get(task_id)
    
    def get_active_tasks(self) -> List[LearningTask]:
        """Get all active learning tasks."""
        return list(self.active_tasks.values())
    
    def cancel_task(self, task_id: str, reason: str = "Cancelled by coordinator"):
        """
        Cancel an active learning task.
        
        Args:
            task_id: ID of task to cancel
            reason: Reason for cancellation
        """
        task = self.active_tasks.get(task_id)
        if task:
            # Cancel research if in progress
            if task.research_request:
                self.research_dispatcher.cancel_request(
                    task.research_request.request_id, reason
                )
            
            task.status = LearningStatus.FAILED
            task.error = reason
            self._complete_task(task)
    
    def create_status_report(self, task: LearningTask) -> str:
        """
        Create a human-readable status report.
        
        Args:
            task: Task to report on
        
        Returns:
            Report string
        """
        lines = [
            f"Auto-Learning Task: {task.capability_name}",
            f"=" * 50,
            f"Task ID: {task.task_id}",
            f"Status: {task.status.value}",
            f"Description: {task.description}",
            f"",
        ]
        
        if task.research_request:
            lines.append(f"Research Request: {task.research_request.request_id}")
            lines.append(f"  Results: {len(task.research_results)}")
        
        if task.selected_solution:
            lines.append(f"Selected Solution: {task.selected_solution.tool_name}")
            lines.append(f"  Confidence: {task.selected_solution.confidence}")
        
        if task.installation_results:
            for result in task.installation_results:
                status_icon = "✓" if result.status == InstallationStatus.SUCCESS else "✗"
                lines.append(f"{status_icon} Installation: {result.status.value}")
        
        if task.verification:
            lines.append(self.verifier.create_verification_report(task.verification))
        
        if task.error:
            lines.append(f"Error: {task.error}")
        
        duration = (task.completed_at or datetime.now()) - task.started_at
        lines.append(f"Duration: {duration.total_seconds():.1f}s")
        
        return "\n".join(lines)


# Convenience function

def learn_capability(
    capability_name: str,
    description: str,
    registry: CapabilityRegistry,
) -> LearningTask:
    """
    Quick helper to start learning a capability.
    
    Args:
        capability_name: Name of capability
        description: Description of what to learn
        registry: CapabilityRegistry instance
    
    Returns:
        LearningTask (will be in RESEARCHING status)
    """
    workflow = AutoLearningWorkflow(registry)
    return workflow.start_learning(capability_name, description)
