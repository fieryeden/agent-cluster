#!/usr/bin/env python3
"""
Research Dispatch Protocol

When a capability gap is detected:
1. Create ResearchRequest with capability needed
2. Dispatch to research-capable agents
3. Collect ResearchResult with solution
4. Coordinator evaluates and selects best solution

Message Types:
- RESEARCH_REQUEST: "Figure out how to do X"
- RESEARCH_PROGRESS: "Working on X, found Y"
- RESEARCH_RESULT: "Here's how to do X"
"""

import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import BaseMessage, MessageType, create_message


class ResearchPriority(Enum):
    """Priority levels for research requests."""
    LOW = "low"          # Nice to have, no rush
    NORMAL = "normal"    # Standard priority
    HIGH = "high"        # Blocking current task
    URGENT = "urgent"    # Critical, drop everything


class ResearchStatus(Enum):
    """Status of a research request."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ResearchRequest:
    """Request for agents to research a capability."""
    
    request_id: str
    capability_name: str
    description: str
    priority: ResearchPriority = ResearchPriority.NORMAL
    timeout_seconds: int = 300  # 5 min default
    constraints: Dict[str, Any] = field(default_factory=dict)
    requester_id: str = "coordinator"
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "request_id": self.request_id,
            "capability_name": self.capability_name,
            "description": self.description,
            "priority": self.priority.value,
            "timeout_seconds": self.timeout_seconds,
            "constraints": self.constraints,
            "requester_id": self.requester_id,
            "created_at": self.created_at.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ResearchRequest":
        """Deserialize from dictionary."""
        return cls(
            request_id=data["request_id"],
            capability_name=data["capability_name"],
            description=data["description"],
            priority=ResearchPriority(data["priority"]),
            timeout_seconds=data["timeout_seconds"],
            constraints=data.get("constraints", {}),
            requester_id=data["requester_id"],
            created_at=datetime.fromisoformat(data["created_at"]),
        )


@dataclass
class ResearchResult:
    """Result of a research request."""
    
    request_id: str
    researcher_id: str
    capability_name: str
    status: ResearchStatus
    solution: Optional[Dict[str, Any]] = None
    confidence: float = 0.0
    alternatives: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None
    research_time_seconds: float = 0.0
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "request_id": self.request_id,
            "researcher_id": self.researcher_id,
            "capability_name": self.capability_name,
            "status": self.status.value,
            "solution": self.solution,
            "confidence": self.confidence,
            "alternatives": self.alternatives,
            "error": self.error,
            "research_time_seconds": self.research_time_seconds,
            "created_at": self.created_at.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ResearchResult":
        """Deserialize from dictionary."""
        return cls(
            request_id=data["request_id"],
            researcher_id=data["researcher_id"],
            capability_name=data["capability_name"],
            status=ResearchStatus(data["status"]),
            solution=data.get("solution"),
            confidence=data.get("confidence", 0.0),
            alternatives=data.get("alternatives", []),
            error=data.get("error"),
            research_time_seconds=data.get("research_time_seconds", 0.0),
            created_at=datetime.fromisoformat(data["created_at"]),
        )


@dataclass
class SolutionProposal:
    """A proposed solution for a capability gap."""
    
    tool_name: str
    tool_type: str  # "pip", "apt", "npm", "custom"
    install_command: str
    verification_command: str
    estimated_install_time: int  # seconds
    requirements: List[str]  # dependencies
    confidence: float
    pros: List[str] = field(default_factory=list)
    cons: List[str] = field(default_factory=list)
    source_url: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "tool_name": self.tool_name,
            "tool_type": self.tool_type,
            "install_command": self.install_command,
            "verification_command": self.verification_command,
            "estimated_install_time": self.estimated_install_time,
            "requirements": self.requirements,
            "confidence": self.confidence,
            "pros": self.pros,
            "cons": self.cons,
            "source_url": self.source_url,
        }


class ResearchDispatcher:
    """
    Dispatches research requests to capable agents.
    
    Workflow:
    1. Receive capability gap notification
    2. Create ResearchRequest
    3. Find agents with research capability
    4. Dispatch request
    5. Collect and evaluate results
    6. Return best solution
    """
    
    def __init__(self, registry: Any = None):
        """
        Initialize research dispatcher.
        
        Args:
            registry: CapabilityRegistry instance for finding researchers
        """
        self.registry = registry
        self.active_requests: Dict[str, ResearchRequest] = {}
        self.results: Dict[str, List[ResearchResult]] = {}
        self.result_handlers: Dict[str, Callable] = {}
    
    def create_request(
        self,
        capability_name: str,
        description: str,
        priority: ResearchPriority = ResearchPriority.NORMAL,
        timeout_seconds: int = 300,
        constraints: Optional[Dict[str, Any]] = None,
    ) -> ResearchRequest:
        """
        Create a new research request.
        
        Args:
            capability_name: Name of needed capability
            description: What needs to be researched
            priority: Priority level
            timeout_seconds: Max time for research
            constraints: Any constraints (platform, license, etc.)
        
        Returns:
            ResearchRequest instance
        """
        import uuid
        request_id = f"research-{uuid.uuid4().hex[:12]}"
        
        request = ResearchRequest(
            request_id=request_id,
            capability_name=capability_name,
            description=description,
            priority=priority,
            timeout_seconds=timeout_seconds,
            constraints=constraints or {},
        )
        
        self.active_requests[request_id] = request
        return request
    
    def dispatch(self, request: ResearchRequest, target_agents: List[str]) -> List[BaseMessage]:
        """
        Dispatch research request to specified agents.
        
        Args:
            request: ResearchRequest to dispatch
            target_agents: List of agent IDs to send to
        
        Returns:
            List of protocol messages to send
        """
        messages = []
        
        for agent_id in target_agents:
            msg = create_message(
                msg_type=MessageType.RESEARCH_REQUEST,
                sender_id=request.requester_id,
                recipient_id=agent_id,
                payload=request.to_dict(),
            )
            messages.append(msg)
        
        return messages
    
    def find_researchers(self) -> List[str]:
        """
        Find agents capable of research.
        
        Returns:
            List of agent IDs with research capability
        """
        if self.registry is None:
            return []
        
        researchers = []
        
        # Look for agents with research capability
        for capability in ["research", "web_search", "documentation_lookup", "capability_discovery"]:
            agent_id = self.registry.find_best_agent(capability)
            if agent_id and agent_id not in researchers:
                researchers.append(agent_id)
        
        return researchers
    
    def collect_result(self, result: ResearchResult):
        """
        Collect a research result.
        
        Args:
            result: ResearchResult from a researcher
        """
        if result.request_id not in self.results:
            self.results[result.request_id] = []
        
        self.results[result.request_id].append(result)
    
    def get_best_solution(self, request_id: str) -> Optional[SolutionProposal]:
        """
        Get the best solution from collected results.
        
        Args:
            request_id: ID of the research request
        
        Returns:
            Best SolutionProposal or None
        """
        results = self.results.get(request_id, [])
        
        if not results:
            return None
        
        # Filter successful results
        successful = [r for r in results if r.status == ResearchStatus.COMPLETED and r.solution]
        
        if not successful:
            return None
        
        # Sort by confidence, take best
        successful.sort(key=lambda r: r.confidence, reverse=True)
        best_result = successful[0]
        
        # Convert to SolutionProposal
        solution_data = best_result.solution
        return SolutionProposal(
            tool_name=solution_data.get("tool_name", "unknown"),
            tool_type=solution_data.get("tool_type", "pip"),
            install_command=solution_data.get("install_command", ""),
            verification_command=solution_data.get("verification_command", ""),
            estimated_install_time=solution_data.get("estimated_install_time", 60),
            requirements=solution_data.get("requirements", []),
            confidence=best_result.confidence,
            pros=solution_data.get("pros", []),
            cons=solution_data.get("cons", []),
            source_url=solution_data.get("source_url"),
        )
    
    def cancel_request(self, request_id: str, reason: str = "Cancelled by coordinator"):
        """
        Cancel an active research request.
        
        Args:
            request_id: ID of request to cancel
            reason: Cancellation reason
        """
        if request_id in self.active_requests:
            request = self.active_requests[request_id]
            # Create cancellation message for tracking
            result = ResearchResult(
                request_id=request_id,
                researcher_id="coordinator",
                capability_name=request.capability_name,
                status=ResearchStatus.CANCELLED,
                error=reason,
            )
            self.collect_result(result)
            del self.active_requests[request_id]


# Convenience functions

def create_research_message(
    capability_name: str,
    description: str,
    target_agents: List[str],
    priority: ResearchPriority = ResearchPriority.NORMAL,
) -> List[BaseMessage]:
    """
    Quick helper to create and dispatch a research request.
    
    Args:
        capability_name: Name of needed capability
        description: What to research
        target_agents: Agents to send to
        priority: Priority level
    
    Returns:
        List of protocol messages
    """
    dispatcher = ResearchDispatcher()
    request = dispatcher.create_request(
        capability_name=capability_name,
        description=description,
        priority=priority,
    )
    return dispatcher.dispatch(request, target_agents)


def parse_research_response(message: BaseMessage) -> ResearchResult:
    """
    Parse a research response message.
    
    Args:
        message: Protocol message with research result
    
    Returns:
        ResearchResult instance
    """
    return ResearchResult.from_dict(message.payload)
