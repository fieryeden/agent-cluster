"""
Agent Cluster Protocol - Message Types

File-based communication for MVP. All messages are JSON files in shared directories.
"""

import json
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any, List


class MessageType(Enum):
    """All supported message types for agent communication."""
    # Lifecycle
    HEARTBEAT = "heartbeat"              # Agent alive check
    REGISTER = "register"                 # Agent registration
    SHUTDOWN = "shutdown"                 # Graceful shutdown
    
    # Capability Discovery
    CAPABILITY_QUERY = "capability_query"     # "Can you do X?"
    CAPABILITY_RESPONSE = "capability_response"  # "Yes (conf: 0.8)" / "No"
    CAPABILITY_UPDATE = "capability_update"   # New capability learned
    
    # Task Management
    TASK_ASSIGN = "task_assign"           # "Do X with params"
    TASK_PROGRESS = "task_progress"       # "Working on X, 30%"
    TASK_COMPLETE = "task_complete"       # "X done, result: ..."
    TASK_FAILED = "task_failed"           # "X failed, reason: ..."
    
    # Tool Management
    TOOL_INSTALL = "tool_install"         # "Install this tool"
    TOOL_INSTALLED = "tool_installed"     # "Tool installed OK"
    TOOL_FAILED = "tool_failed"           # "Tool install failed"
    
    # Research
    RESEARCH_REQUEST = "research_request" # "Figure out how to do X"
    RESEARCH_RESULT = "research_result"   # "Here's how to do X"

    # Agent-to-Agent Communication
    PEER_REQUEST = "peer_request"        # Agent asks another agent directly
    PEER_RESPONSE = "peer_response"      # Agent responds to peer request
    PEER_NOTIFY = "peer_notify"          # Agent notifies another agent (no response needed)
    PEER_BROADCAST = "peer_broadcast"    # Agent broadcasts to all peers
    CONVERSATION_START = "conversation_start"  # Initiate a new conversation thread
    CONVERSATION_END = "conversation_end"      # Close a conversation thread

    # File Transfer
    FILE_SEND = "file_send"              # Agent sends a file to another agent
    FILE_REQUEST = "file_request"          # Agent requests a file from another agent
    FILE_SEND_RESPONSE = "file_send_response"  # Agent accepts/declines a file transfer

    # Task Delegation (agent-to-agent, not coordinator-assigned)
    TASK_DELEGATE = "task_delegate"            # Agent delegates a subtask to another agent
    TASK_DELEGATE_RESPONSE = "task_delegate_response"  # Agent accepts/declines/counter-offers

    # Status & Capability
    STATUS_QUERY = "status_query"              # Agent asks another agent for status
    STATUS_RESPONSE = "status_response"        # Agent responds with status info
    CAPABILITY_SHARE = "capability_share"       # Agent announces new capability to specific peers

    # Consensus
    CONSENSUS_REQUEST = "consensus_request"    # Agent proposes a decision for group vote
    CONSENSUS_VOTE = "consensus_vote"          # Agent casts a vote on a consensus proposal

    # Peer Health & Context
    HEARTBEAT_PEER = "heartbeat_peer"          # Direct peer-to-peer liveness check
    CONTEXT_SHARE = "context_share"            # Agent pushes working context to another agent
    ERROR_REPORT = "error_report"              # Agent reports an error/conflict to another agent

    # OTA Updates (coordinator → agents, no user consent needed)
    OTA_UPDATE_ANNOUNCE = "ota_update_announce"    # Coordinator announces an update is available
    OTA_UPDATE_PACKAGE = "ota_update_package"      # Coordinator sends the update payload
    OTA_UPDATE_ACK = "ota_update_ack"              # Agent acknowledges update received
    OTA_UPDATE_STATUS = "ota_update_status"        # Agent reports install progress/result
    OTA_UPDATE_ROLLBACK = "ota_update_rollback"    # Coordinator commands rollback of failed update


@dataclass
class AgentCapability:
    """A capability an agent has with confidence score."""
    name: str
    confidence: float  # 0.0 - 1.0
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class BaseMessage:
    """Base message structure for all agent communication."""
    msg_type: MessageType
    sender_id: str
    recipient_id: str  # "coordinator" or "broadcast" or specific agent
    message_id: str
    timestamp: str
    payload: Dict[str, Any]
    
    def to_dict(self) -> Dict:
        """Convert message to dictionary."""
        data = asdict(self)
        data['msg_type'] = self.msg_type.value
        return data
    
    def to_json(self) -> str:
        data = asdict(self)
        data['msg_type'] = self.msg_type.value
        return json.dumps(data, indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'BaseMessage':
        """Create message from dictionary."""
        data = dict(data)  # Copy
        data['msg_type'] = MessageType(data['msg_type'])
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'BaseMessage':
        data = json.loads(json_str)
        data['msg_type'] = MessageType(data['msg_type'])
        return cls(**data)


def create_message(
    msg_type: MessageType,
    sender_id: str,
    recipient_id: str,
    payload: Dict[str, Any]
) -> BaseMessage:
    """Factory function to create messages with auto-generated IDs and timestamps."""
    return BaseMessage(
        msg_type=msg_type,
        sender_id=sender_id,
        recipient_id=recipient_id,
        message_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc).isoformat() + "Z",
        payload=payload
    )


# === Specific Message Constructors ===

def heartbeat(agent_id: str, load: float = 0.0) -> BaseMessage:
    """Agent heartbeat to coordinator."""
    return create_message(
        MessageType.HEARTBEAT,
        sender_id=agent_id,
        recipient_id="coordinator",
        payload={"load": load, "status": "active"}
    )


def register_agent(
    agent_id: str,
    agent_type: str,
    capabilities: List[AgentCapability],
    device_info: Dict[str, Any]
) -> BaseMessage:
    """Register a new agent with the coordinator."""
    return create_message(
        MessageType.REGISTER,
        sender_id=agent_id,
        recipient_id="coordinator",
        payload={
            "agent_type": agent_type,
            "capabilities": [c.to_dict() for c in capabilities],
            "device_info": device_info
        }
    )


def capability_query(query: str, context: Optional[Dict] = None) -> BaseMessage:
    """Coordinator asks agents if they can handle something."""
    return create_message(
        MessageType.CAPABILITY_QUERY,
        sender_id="coordinator",
        recipient_id="broadcast",
        payload={"query": query, "context": context or {}}
    )


def capability_response(
    agent_id: str,
    query_id: str,
    can_handle: bool,
    confidence: float = 0.0,
    details: Optional[Dict] = None
) -> BaseMessage:
    """Agent responds to capability query."""
    return create_message(
        MessageType.CAPABILITY_RESPONSE,
        sender_id=agent_id,
        recipient_id="coordinator",
        payload={
            "query_id": query_id,
            "can_handle": can_handle,
            "confidence": confidence,
            "details": details or {}
        }
    )


def task_assign(
    task_id: str,
    agent_id: str,
    task_type: str,
    task_data: Dict[str, Any],
    priority: int = 0
) -> BaseMessage:
    """Coordinator assigns a task to an agent."""
    return create_message(
        MessageType.TASK_ASSIGN,
        sender_id="coordinator",
        recipient_id=agent_id,
        payload={
            "task_id": task_id,
            "task_type": task_type,
            "task_data": task_data,
            "priority": priority
        }
    )


def task_progress(
    agent_id: str,
    task_id: str,
    progress: float,
    status: str
) -> BaseMessage:
    """Agent reports progress on a task."""
    return create_message(
        MessageType.TASK_PROGRESS,
        sender_id=agent_id,
        recipient_id="coordinator",
        payload={
            "task_id": task_id,
            "progress": progress,
            "status": status
        }
    )


def task_complete(
    agent_id: str,
    task_id: str,
    result: Dict[str, Any]
) -> BaseMessage:
    """Agent reports task completion."""
    return create_message(
        MessageType.TASK_COMPLETE,
        sender_id=agent_id,
        recipient_id="coordinator",
        payload={
            "task_id": task_id,
            "result": result
        }
    )


def task_failed(
    agent_id: str,
    task_id: str,
    reason: str,
    error_details: Optional[Dict] = None
) -> BaseMessage:
    """Agent reports task failure."""
    return create_message(
        MessageType.TASK_FAILED,
        sender_id=agent_id,
        recipient_id="coordinator",
        payload={
            "task_id": task_id,
            "reason": reason,
            "error_details": error_details or {}
        }
    )


def research_request(
    request_id: str,
    topic: str,
    context: Optional[Dict] = None
) -> BaseMessage:
    """Coordinator dispatches research to research agent."""
    return create_message(
        MessageType.RESEARCH_REQUEST,
        sender_id="coordinator",
        recipient_id="research-agent",
        payload={
            "request_id": request_id,
            "topic": topic,
            "context": context or {}
        }
    )


def research_result(
    request_id: str,
    findings: Dict[str, Any],
    tools_recommended: List[str]
) -> BaseMessage:
    """Research agent returns findings."""
    return create_message(
        MessageType.RESEARCH_RESULT,
        sender_id="research-agent",
        recipient_id="coordinator",
        payload={
            "request_id": request_id,
            "findings": findings,
            "tools_recommended": tools_recommended
        }
    )




# === Agent-to-Agent Message Constructors ===

def peer_request(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    content: str,
    request_type: str = "general",
    context: Optional[Dict] = None,
) -> BaseMessage:
    """Agent sends a request directly to another agent."""
    return create_message(
        MessageType.PEER_REQUEST,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "content": content,
            "request_type": request_type,
            "context": context or {},
        }
    )


def peer_response(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    content: str,
    in_reply_to: str,
    response_type: str = "answer",
) -> BaseMessage:
    """Agent responds to a peer request."""
    return create_message(
        MessageType.PEER_RESPONSE,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "content": content,
            "in_reply_to": in_reply_to,
            "response_type": response_type,
        }
    )


def peer_notify(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    content: str,
    notification_type: str = "info",
) -> BaseMessage:
    """Agent notifies another agent (no response expected)."""
    return create_message(
        MessageType.PEER_NOTIFY,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "content": content,
            "notification_type": notification_type,
        }
    )


def peer_broadcast(
    sender_id: str,
    content: str,
    topic: str = "",
    exclude: Optional[List[str]] = None,
) -> BaseMessage:
    """Agent broadcasts a message to all peers."""
    return create_message(
        MessageType.PEER_BROADCAST,
        sender_id=sender_id,
        recipient_id="broadcast",
        payload={
            "content": content,
            "topic": topic,
            "exclude": exclude or [],
        }
    )


def conversation_start(
    sender_id: str,
    recipient_id: str,
    topic: str,
    initial_message: str = "",
) -> BaseMessage:
    """Initiate a new conversation thread between agents."""
    return create_message(
        MessageType.CONVERSATION_START,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "topic": topic,
            "initial_message": initial_message,
        }
    )


def conversation_end(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    summary: str = "",
) -> BaseMessage:
    """Close a conversation thread."""
    return create_message(
        MessageType.CONVERSATION_END,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "summary": summary,
        }
    )

def file_send(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    filename: str,
    file_data: str,
    file_type: str = "application/octet-stream",
    description: str = "",
    encoding: str = "base64",
    checksum: str = "",
) -> BaseMessage:
    """Agent sends a file to another agent.

    The file payload is base64-encoded in the message.
    For large files, agents should negotiate an out-of-band transfer.

    Args:
        sender_id: Sending agent ID
        recipient_id: Receiving agent ID
        conversation_id: Conversation this file belongs to
        filename: Original filename (e.g. "results.csv")
        file_data: Base64-encoded file contents
        file_type: MIME type (e.g. "text/csv", "application/json")
        description: Human-readable description of the file
        encoding: Encoding method (default: base64)
        checksum: SHA-256 checksum of the *original* (pre-encoding) file for integrity
    """
    return create_message(
        MessageType.FILE_SEND,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "filename": filename,
            "file_data": file_data,
            "file_type": file_type,
            "description": description,
            "encoding": encoding,
            "checksum": checksum,
        },
    )


def file_request(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    filename: str,
    description: str = "",
    file_type_hint: str = "",
) -> BaseMessage:
    """Agent requests a file from another agent.

    The recipient should respond with a FILE_SEND message containing the file.

    Args:
        sender_id: Requesting agent ID
        recipient_id: Agent that has the file
        conversation_id: Conversation this request belongs to
        filename: Name of the file being requested
        description: Why the file is needed
        file_type_hint: Expected MIME type (optional, helps recipient locate the right file)
    """
    return create_message(
        MessageType.FILE_REQUEST,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "filename": filename,
            "description": description,
            "file_type_hint": file_type_hint,
        },
    )


def file_send_response(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    file_message_id: str,
    accepted: bool,
    reason: str = "",
    save_as: str = "",
) -> BaseMessage:
    """Agent accepts or declines an incoming file transfer.

    Must be sent in response to a FILE_SEND before the recipient processes the file.
    If declined, the sender should not expect the file to be used.

    Args:
        sender_id: Responding agent ID (the intended file recipient)
        recipient_id: Original file sender ID
        conversation_id: Conversation this belongs to
        file_message_id: The message_id of the original FILE_SEND
        accepted: True to accept, False to decline
        reason: Why declined (or optional acceptance note)
        save_as: Alternative filename if the recipient wants to rename
    """
    return create_message(
        MessageType.FILE_SEND_RESPONSE,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "file_message_id": file_message_id,
            "accepted": accepted,
            "reason": reason,
            "save_as": save_as,
        },
    )


def task_delegate(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    task_description: str,
    task_params: Optional[Dict] = None,
    priority: str = "normal",
    deadline: str = "",
    context: Optional[Dict] = None,
) -> BaseMessage:
    """Agent delegates a subtask to another agent with full context.

    Unlike coordinator TASK_ASSIGN, this is agent-initiated delegation
    with negotiation — the recipient can accept, decline, or counter-offer.

    Args:
        sender_id: Delegating agent ID
        recipient_id: Agent being asked to do the task
        conversation_id: Conversation this delegation belongs to
        task_description: What needs to be done
        task_params: Parameters/input for the task
        priority: "low", "normal", "high", or "critical"
        deadline: ISO timestamp for when the task should be done
        context: Additional context (related task IDs, prior results, etc.)
    """
    return create_message(
        MessageType.TASK_DELEGATE,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "task_description": task_description,
            "task_params": task_params or {},
            "priority": priority,
            "deadline": deadline,
            "context": context or {},
        },
    )


def task_delegate_response(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    delegate_message_id: str,
    response: str,
    reason: str = "",
    counter_offer: Optional[Dict] = None,
    estimated_completion: str = "",
) -> BaseMessage:
    """Agent responds to a task delegation (accept, decline, or counter-offer).

    Args:
        sender_id: Responding agent ID
        recipient_id: Delegating agent ID
        conversation_id: Conversation this belongs to
        delegate_message_id: The message_id of the TASK_DELEGATE
        response: "accept", "decline", or "counter_offer"
        reason: Why declined, or notes on acceptance
        counter_offer: If counter_offer, proposed modifications to the task
        estimated_completion: When the task will be done (ISO timestamp)
    """
    return create_message(
        MessageType.TASK_DELEGATE_RESPONSE,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "delegate_message_id": delegate_message_id,
            "response": response,
            "reason": reason,
            "counter_offer": counter_offer or {},
            "estimated_completion": estimated_completion,
        },
    )


def status_query(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    query: str = "",
    about_task: str = "",
) -> BaseMessage:
    """Agent asks another agent for a lightweight status check.

    More conversational than formal task progress — think "hey, how's it going?"
    rather than a formal report.

    Args:
        sender_id: Asking agent
        recipient_id: Agent being asked
        conversation_id: Conversation this belongs to
        query: Free-text status question
        about_task: Optional specific task ID to ask about
    """
    return create_message(
        MessageType.STATUS_QUERY,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "query": query,
            "about_task": about_task,
        },
    )


def status_response(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    status: str,
    details: str = "",
    task_status: Optional[Dict] = None,
    availability: str = "available",
) -> BaseMessage:
    """Agent responds with status information.

    Args:
        sender_id: Responding agent
        recipient_id: Agent that asked
        conversation_id: Conversation this belongs to
        status: Brief status summary ("busy", "idle", "working on X", etc.)
        details: Longer description of current state
        task_status: Dict of task_id → status for specific tasks
        availability: "available", "busy", "offline", "overloaded"
    """
    return create_message(
        MessageType.STATUS_RESPONSE,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "status": status,
            "details": details,
            "task_status": task_status or {},
            "availability": availability,
        },
    )


def capability_share(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    capability_name: str,
    capability_description: str = "",
    confidence: float = 0.0,
    metadata: Optional[Dict] = None,
) -> BaseMessage:
    """Agent announces a new or updated capability to specific peers.

    Unlike CAPABILITY_UPDATE (which goes to the coordinator registry),
    this is a targeted peer notification — "hey, I can do X now."

    Args:
        sender_id: Agent sharing the capability
        recipient_id: Peer being notified
        conversation_id: Conversation this belongs to
        capability_name: Name of the capability (e.g. "sentiment_analysis")
        capability_description: What it does
        confidence: Self-assessed confidence (0.0-1.0)
        metadata: Extra info (model used, version, limitations, etc.)
    """
    return create_message(
        MessageType.CAPABILITY_SHARE,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "capability_name": capability_name,
            "capability_description": capability_description,
            "confidence": confidence,
            "metadata": metadata or {},
        },
    )


def consensus_request(
    sender_id: str,
    proposal: str,
    options: Optional[List[str]] = None,
    voters: Optional[List[str]] = None,
    deadline: str = "",
    quorum: int = 0,
    topic: str = "",
) -> BaseMessage:
    """Agent proposes a decision for group vote.

    Sent to all voting agents. Each responds with CONSENSUS_VOTE.
    The coordinator tallies votes and announces the result.

    Args:
        sender_id: Proposing agent
        proposal: What is being decided
        options: Choices to vote on (e.g. ["retry", "abort", "escalate"])
        voters: List of agent IDs that should vote (empty = all)
        deadline: ISO timestamp for when voting closes
        quorum: Minimum votes needed for a valid decision (0 = all)
        topic: Topic/category for the proposal
    """
    return create_message(
        MessageType.CONSENSUS_REQUEST,
        sender_id=sender_id,
        recipient_id="broadcast",
        payload={
            "proposal": proposal,
            "options": options or ["accept", "reject"],
            "voters": voters or [],
            "deadline": deadline,
            "quorum": quorum,
            "topic": topic,
        },
    )


def consensus_vote(
    sender_id: str,
    recipient_id: str,
    consensus_message_id: str,
    vote: str,
    reasoning: str = "",
    conditions: Optional[Dict] = None,
) -> BaseMessage:
    """Agent casts a vote on a consensus proposal.

    Args:
        sender_id: Voting agent
        recipient_id: Proposing agent (or coordinator)
        consensus_message_id: The message_id of the CONSENSUS_REQUEST
        vote: The chosen option (must match one of the proposed options)
        reasoning: Why this vote
        conditions: Optional conditions on the vote (e.g. "accept if timeout > 60s")
    """
    return create_message(
        MessageType.CONSENSUS_VOTE,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "consensus_message_id": consensus_message_id,
            "vote": vote,
            "reasoning": reasoning,
            "conditions": conditions or {},
        },
    )


def heartbeat_peer(
    sender_id: str,
    recipient_id: str,
) -> BaseMessage:
    """Direct peer-to-peer liveness check.

    Lighter than the coordinator-managed heartbeat — this is an agent
    directly checking if another agent is alive and responsive.

    Args:
        sender_id: Pinging agent
        recipient_id: Agent being checked
    """
    return create_message(
        MessageType.HEARTBEAT_PEER,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "ping_timestamp": datetime.now(timezone.utc).isoformat() + "Z",
        },
    )


def context_share(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    context_type: str,
    context_data: Dict,
    description: str = "",
) -> BaseMessage:
    """Agent pushes working context to another agent.

    Lighter than FILE_SEND — structured key-value context rather than
    raw file data. Useful for syncing state, sharing configs, or
    bringing another agent up to speed.

    Args:
        sender_id: Sharing agent
        recipient_id: Receiving agent
        conversation_id: Conversation this belongs to
        context_type: Category of context ("config", "state", "reference", "docs")
        context_data: Structured context payload (dict of key-value pairs)
        description: What this context is about
    """
    return create_message(
        MessageType.CONTEXT_SHARE,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "context_type": context_type,
            "context_data": context_data,
            "description": description,
        },
    )


def error_report(
    sender_id: str,
    recipient_id: str,
    conversation_id: str,
    error_type: str,
    error_message: str,
    related_task: str = "",
    related_message_id: str = "",
    severity: str = "error",
    suggested_fix: str = "",
) -> BaseMessage:
    """Agent reports an error or conflict to another agent.

    Important for debugging multi-agent chains — "you gave me bad data"
    or "the output format doesn't match what I expected."

    Args:
        sender_id: Agent reporting the error
        recipient_id: Agent that caused or should know about the error
        conversation_id: Conversation this belongs to
        error_type: Category ("data_format", "timeout", "invalid_input", "conflict", "crash")
        error_message: What went wrong
        related_task: Task ID where the error occurred
        related_message_id: Message ID that triggered the error
        severity: "warning", "error", or "critical"
        suggested_fix: What the reporting agent thinks should be done
    """
    return create_message(
        MessageType.ERROR_REPORT,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "conversation_id": conversation_id,
            "error_type": error_type,
            "error_message": error_message,
            "related_task": related_task,
            "related_message_id": related_message_id,
            "severity": severity,
            "suggested_fix": suggested_fix,
        },
    )


def ota_update_announce(
    sender_id: str,
    version: str,
    changelog: str = "",
    priority: str = "normal",
    deadline: str = "",
    checksum: str = "",
    size_bytes: int = 0,
    requires_restart: bool = False,
) -> BaseMessage:
    """Coordinator announces an OTA update is available.

    Sent to all connected agents. Agents should respond with OTA_UPDATE_ACK
    and then the coordinator will send the actual package.

    No user consent is required — updates are installed automatically.

    Args:
        sender_id: Coordinator ID
        version: New version string (e.g. "1.5.0")
        changelog: What changed in this release
        priority: "low", "normal", "high", or "critical" (critical = immediate install)
        deadline: ISO timestamp — agents must install by this time (empty = no deadline)
        checksum: SHA-256 of the update package for verification
        size_bytes: Size of the update package
        requires_restart: Whether the agent needs to restart after install
    """
    return create_message(
        MessageType.OTA_UPDATE_ANNOUNCE,
        sender_id=sender_id,
        recipient_id="broadcast",
        payload={
            "version": version,
            "changelog": changelog,
            "priority": priority,
            "deadline": deadline,
            "checksum": checksum,
            "size_bytes": size_bytes,
            "requires_restart": requires_restart,
        },
    )


def ota_update_package(
    sender_id: str,
    recipient_id: str,
    version: str,
    package_data: str,
    package_type: str = "tar.gz",
    checksum: str = "",
    install_script: str = "",
    pre_install: str = "",
    post_install: str = "",
    rollback_script: str = "",
) -> BaseMessage:
    """Coordinator sends the OTA update payload to an agent.

    The package_data is base64-encoded. Upon receipt, the agent
    automatically installs — no user consent needed.

    Args:
        sender_id: Coordinator ID
        recipient_id: Target agent ID
        version: Version being installed
        package_data: Base64-encoded update package
        package_type: Archive format ("tar.gz", "zip", "whl")
        checksum: SHA-256 of the decoded package for integrity verification
        install_script: Shell commands to run for installation
        pre_install: Commands to run before installation (e.g. backup)
        post_install: Commands to run after installation (e.g. restart service)
        rollback_script: Commands to rollback if install fails
    """
    return create_message(
        MessageType.OTA_UPDATE_PACKAGE,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "version": version,
            "package_data": package_data,
            "package_type": package_type,
            "checksum": checksum,
            "install_script": install_script,
            "pre_install": pre_install,
            "post_install": post_install,
            "rollback_script": rollback_script,
        },
    )


def ota_update_ack(
    sender_id: str,
    recipient_id: str,
    version: str,
    announce_message_id: str,
    ready: bool = True,
    current_version: str = "",
) -> BaseMessage:
    """Agent acknowledges an OTA update announcement.

    Sent in response to OTA_UPDATE_ANNOUNCE. If ready=True, coordinator
    will send the OTA_UPDATE_PACKAGE. If ready=False, agent is not
    in a state to accept updates (e.g. mid-task).

    Args:
        sender_id: Acknowledging agent ID
        recipient_id: Coordinator ID
        version: Version being acknowledged
        announce_message_id: The message_id of the OTA_UPDATE_ANNOUNCE
        ready: Whether the agent is ready to receive the package now
        current_version: Agent's current version (for coordinator tracking)
    """
    return create_message(
        MessageType.OTA_UPDATE_ACK,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "version": version,
            "announce_message_id": announce_message_id,
            "ready": ready,
            "current_version": current_version,
        },
    )


def ota_update_status(
    sender_id: str,
    recipient_id: str,
    version: str,
    status: str,
    stage: str = "",
    message: str = "",
    previous_version: str = "",
    rollback_available: bool = True,
) -> BaseMessage:
    """Agent reports OTA update installation progress or result.

    Status values: "downloading", "verifying", "pre_install", "installing",
    "post_install", "success", "failed", "rolled_back"

    Args:
        sender_id: Reporting agent ID
        recipient_id: Coordinator ID
        version: Version being installed
        status: Current installation status
        stage: More specific stage if applicable
        message: Human-readable detail (error message on failure, etc.)
        previous_version: Version that was running before the update
        rollback_available: Whether a rollback is possible on failure
    """
    return create_message(
        MessageType.OTA_UPDATE_STATUS,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "version": version,
            "status": status,
            "stage": stage,
            "message": message,
            "previous_version": previous_version,
            "rollback_available": rollback_available,
        },
    )


def ota_update_rollback(
    sender_id: str,
    recipient_id: str,
    version: str,
    reason: str = "",
    target_version: str = "",
) -> BaseMessage:
    """Coordinator commands an agent to rollback a failed update.

    Sent when an agent reports a failed install, or when the coordinator
    determines a version is problematic and needs to be reverted fleet-wide.

    No user consent needed — rollback is automatic.

    Args:
        sender_id: Coordinator ID
        recipient_id: Agent to rollback
        version: Version to rollback FROM
        reason: Why the rollback is being commanded
        target_version: Version to rollback TO (empty = previous version)
    """
    return create_message(
        MessageType.OTA_UPDATE_ROLLBACK,
        sender_id=sender_id,
        recipient_id=recipient_id,
        payload={
            "version": version,
            "reason": reason,
            "target_version": target_version,
        },
    )


# === File-based Message Queue ===

class MessageQueue:
    """
    File-based message queue for MVP.
    
    Directory structure:
        inbox/     - Messages received by this agent
        outbox/    - Messages sent by this agent
        processed/ - Processed messages (archive)
    """
    
    def __init__(self, base_dir: str, agent_id: str):
        self.base_dir = base_dir
        self.agent_id = agent_id
        self.inbox_dir = f"{base_dir}/agents/{agent_id}/inbox"
        self.outbox_dir = f"{base_dir}/agents/{agent_id}/outbox"
        self.processed_dir = f"{base_dir}/agents/{agent_id}/processed"
        self.broadcast_dir = f"{base_dir}/broadcast"
        self.coordinator_dir = f"{base_dir}/coordinator/inbox"
    
    def send(self, message: BaseMessage) -> str:
        """Write message to appropriate outbox."""
        import os
        
        # Determine target directory
        if message.recipient_id == "broadcast":
            target_dir = self.broadcast_dir
        elif message.recipient_id == "coordinator":
            target_dir = self.coordinator_dir
        else:
            target_dir = f"{self.base_dir}/agents/{message.recipient_id}/inbox"
        
        # Ensure directory exists
        os.makedirs(target_dir, exist_ok=True)
        
        # Write message file
        filename = f"{message.timestamp.replace(':', '-')}_{message.message_id}.json"
        filepath = f"{target_dir}/{filename}"
        
        with open(filepath, 'w') as f:
            f.write(message.to_json())
        
        return filepath
    
    def receive(self, include_broadcast: bool = True) -> List[tuple]:
        """Read all pending messages from inbox."""
        import os
        import glob
        
        messages = []
        
        # Coordinator reads from coordinator_dir, agents read from inbox_dir
        if self.agent_id == "coordinator":
            inbox_files = glob.glob(f"{self.coordinator_dir}/*.json")
        else:
            inbox_files = glob.glob(f"{self.inbox_dir}/*.json")
        
        for filepath in inbox_files:
            with open(filepath, 'r') as f:
                msg = BaseMessage.from_json(f.read())
                messages.append((filepath, msg))
        
        # Check broadcast inbox
        if include_broadcast:
            broadcast_files = glob.glob(f"{self.broadcast_dir}/*.json")
            for filepath in broadcast_files:
                with open(filepath, 'r') as f:
                    msg = BaseMessage.from_json(f.read())
                    messages.append((filepath, msg))
        
        return messages
    
    def mark_processed(self, filepath: str):
        """Move message to processed archive."""
        import os
        import shutil
        
        filename = os.path.basename(filepath)
        os.makedirs(self.processed_dir, exist_ok=True)
        shutil.move(filepath, f"{self.processed_dir}/{filename}")


if __name__ == "__main__":
    # Test message creation
    print("=== Agent Cluster Protocol Test ===\n")
    
    # Test heartbeat
    hb = heartbeat("test-agent-001")
    print(f"Heartbeat:\n{hb.to_json()}\n")
    
    # Test capability query
    cq = capability_query("spreadsheet analysis", {"file": "data.xlsx"})
    print(f"Capability Query:\n{cq.to_json()}\n")
    
    # Test task assign
    ta = task_assign(
        task_id="task-123",
        agent_id="bookkeeper-001",
        task_type="analyze_spreadsheet",
        task_data={"file": "/data/financials.xlsx"},
        priority=1
    )
    print(f"Task Assign:\n{ta.to_json()}\n")
    
    print("✓ Protocol module OK")
