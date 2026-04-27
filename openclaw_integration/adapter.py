#!/usr/bin/env python3
"""
OpenClaw Agent Adapter

Adapts an OpenClaw session to act as an agent in the cluster.
Handles message translation between OpenClaw session messages
and cluster protocol messages.

Key features:
- Translates OpenClaw session messages → cluster protocol messages
- Manages agent lifecycle (register, heartbeat, deregister)
- Handles OTA updates received via OpenClaw sessions
- Tracks agent state and capabilities
"""

import json
import os
import time
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path
import uuid

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import (
    BaseMessage, MessageType, create_message,
    peer_request, peer_response, peer_notify,
    status_query, status_response,
    heartbeat_peer, context_share, error_report,
    ota_update_ack, ota_update_status,
)
from protocol.ota_manager import AgentOTAInstaller


class OpenClawAgentAdapter:
    """
    Adapts an OpenClaw session to participate in the agent cluster.

    This adapter:
    1. Registers the agent with the cluster coordinator
    2. Sends periodic heartbeats via OpenClaw sessions
    3. Translates incoming OpenClaw messages to cluster protocol
    4. Handles OTA update installation automatically
    5. Reports status back through OpenClaw sessions
    """

    def __init__(
        self,
        agent_id: str,
        session_key: str,
        capabilities: List[str] = None,
        install_dir: str = None,
        current_version: str = "1.0.0",
        coordinator_session: str = None,
    ):
        """
        Initialize the agent adapter.

        Args:
            agent_id: Unique agent identifier
            session_key: OpenClaw session key for this agent
            capabilities: List of capability names
            install_dir: Installation directory for OTA updates
            current_version: Current software version
            coordinator_session: Session key of the coordinator
        """
        self.agent_id = agent_id
        self.session_key = session_key
        self.capabilities = capabilities or []
        self.current_version = current_version
        self.coordinator_session = coordinator_session

        # State
        self._status = "initialized"
        self._last_heartbeat = None
        self._running = False
        self._heartbeat_thread: Optional[threading.Thread] = None

        # OTA installer (no consent needed — automatic install)
        self.install_dir = install_dir or f"/tmp/agent-{agent_id}"
        self.ota_installer = AgentOTAInstaller(
            agent_id=agent_id,
            install_dir=self.install_dir,
            current_version=current_version,
        )

        # Message handlers by type
        self._handlers: Dict[MessageType, Callable] = {
            MessageType.OTA_UPDATE_ANNOUNCE: self._handle_ota_announce,
            MessageType.OTA_UPDATE_PACKAGE: self._handle_ota_package,
            MessageType.OTA_UPDATE_ROLLBACK: self._handle_ota_rollback,
            MessageType.PEER_REQUEST: self._handle_peer_request,
            MessageType.PEER_RESPONSE: self._handle_peer_response,
            MessageType.PEER_NOTIFY: self._handle_peer_notify,
            MessageType.STATUS_QUERY: self._handle_status_query,
            MessageType.HEARTBEAT_PEER: self._handle_peer_heartbeat,
            MessageType.CONSENSUS_REQUEST: self._handle_consensus_request,
            MessageType.CONSENSUS_VOTE: self._handle_consensus_vote,
            MessageType.TASK_DELEGATE: self._handle_task_delegate,
            MessageType.FILE_SEND: self._handle_file_send,
            MessageType.CONTEXT_SHARE: self._handle_context_share,
            MessageType.ERROR_REPORT: self._handle_error_report,
        }

        # Custom handler hooks
        self._custom_handlers: Dict[str, Callable] = {}

        # Stats
        self.adapter_stats = {
            "messages_received": 0,
            "messages_sent": 0,
            "ota_updates_installed": 0,
            "ota_rollbacks": 0,
            "tasks_delegated_received": 0,
            "errors_handled": 0,
            "uptime_seconds": 0,
        }
        self._start_time = None

        # Thread safety
        self._lock = threading.RLock()

    @property
    def status(self) -> str:
        return self._status

    def register(self) -> Dict[str, Any]:
        """
        Register this agent with the cluster coordinator.

        Returns:
            Registration result
        """
        self._status = "registering"
        self._start_time = time.time()

        # In live OpenClaw, this would send a REGISTER message
        # via sessions_send to the coordinator session
        self._status = "registered"

        return {
            "status": "registered",
            "agent_id": self.agent_id,
            "capabilities": self.capabilities,
            "version": self.current_version,
        }

    def deregister(self) -> Dict[str, Any]:
        """
        Deregister this agent from the cluster.

        Returns:
            Deregistration result
        """
        self._status = "deregistering"
        self.stop_heartbeat()
        self._status = "deregistered"

        return {"status": "deregistered", "agent_id": self.agent_id}

    def handle_message(self, raw_message: str) -> Optional[Dict[str, Any]]:
        """
        Handle an incoming OpenClaw session message.

        Translates the raw JSON message to a cluster protocol message
        and dispatches to the appropriate handler.

        Args:
            raw_message: JSON string from OpenClaw session

        Returns:
            Handler result dict, or None if unhandled
        """
        self.adapter_stats["messages_received"] += 1

        try:
            msg_data = json.loads(raw_message)
        except json.JSONDecodeError:
            self.adapter_stats["errors_handled"] += 1
            return {"status": "error", "reason": "invalid_json"}

        msg_type_str = msg_data.get("type", "")
        try:
            msg_type = MessageType(msg_type_str)
        except ValueError:
            # Check custom handlers
            custom_handler = self._custom_handlers.get(msg_type_str)
            if custom_handler:
                return custom_handler(msg_data)
            return {"status": "unhandled", "type": msg_type_str}

        handler = self._handlers.get(msg_type)
        if handler:
            return handler(msg_data)

        return {"status": "unhandled", "type": msg_type_str}

    def add_capability(self, capability: str):
        """Add a capability to this agent."""
        with self._lock:
            if capability not in self.capabilities:
                self.capabilities.append(capability)

    def remove_capability(self, capability: str):
        """Remove a capability from this agent."""
        with self._lock:
            self.capabilities = [c for c in self.capabilities if c != capability]

    def register_custom_handler(self, msg_type: str, handler: Callable):
        """Register a custom message handler for non-standard types."""
        self._custom_handlers[msg_type] = handler

    def get_status(self) -> Dict[str, Any]:
        """
        Get agent status for STATUS_QUERY responses.

        Returns:
            Agent status dict
        """
        uptime = 0
        if self._start_time:
            uptime = int(time.time() - self._start_time)

        return {
            "agent_id": self.agent_id,
            "status": self._status,
            "version": self.current_version,
            "capabilities": list(self.capabilities),
            "uptime_seconds": uptime,
            "last_heartbeat": self._last_heartbeat,
            "ota_updates_installed": self.adapter_stats["ota_updates_installed"],
            "stats": dict(self.adapter_stats),
            "install_history": self.ota_installer.get_install_history(),
        }

    def start_heartbeat(self, interval_seconds: int = 30):
        """
        Start periodic heartbeat via OpenClaw sessions.

        Args:
            interval_seconds: Heartbeat interval
        """
        if self._running:
            return

        self._running = True

        def _heartbeat_loop():
            while self._running:
                self._last_heartbeat = datetime.now(timezone.utc).isoformat()
                # In live OpenClaw, would call sessions_send
                time.sleep(interval_seconds)

        self._heartbeat_thread = threading.Thread(
            target=_heartbeat_loop, daemon=True, name=f"agent-{self.agent_id}-heartbeat"
        )
        self._heartbeat_thread.start()

    def stop_heartbeat(self):
        """Stop the heartbeat thread."""
        self._running = False
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5)
            self._heartbeat_thread = None

    # ─── OTA Handlers (no consent — automatic) ─────────────────────

    def _handle_ota_announce(self, msg_data: Dict) -> Dict[str, Any]:
        """
        Handle OTA_UPDATE_ANNOUNCE — automatically accept.

        No user consent needed. Agent acknowledges readiness immediately.
        """
        version = msg_data.get("payload", {}).get("version", "")
        priority = msg_data.get("payload", {}).get("priority", "normal")
        announce_msg_id = msg_data.get("message_id", "")

        # Always ready — no consent gate
        ack = ota_update_ack(
            sender_id=self.agent_id,
            recipient_id=msg_data.get("sender_id", "coordinator"),
            version=version,
            announce_message_id=announce_msg_id,
            ready=True,
            current_version=self.current_version,
        )

        return {
            "status": "acknowledged",
            "version": version,
            "ready": True,
            "action": "accept_ota",
        }

    def _handle_ota_package(self, msg_data: Dict) -> Dict[str, Any]:
        """
        Handle OTA_UPDATE_PACKAGE — install automatically.

        No user consent. Installation triggers on receipt.
        Reports progress back via STATUS messages.
        """
        payload = msg_data.get("payload", {})
        version = payload.get("version", "")

        # Auto-install (no consent check)
        result = self.ota_installer.install_update(payload)

        if result["status"] == "success":
            self.current_version = version
            self.adapter_stats["ota_updates_installed"] += 1
            self._status = "updated"

        # Report status back
        status_msg = ota_update_status(
            sender_id=self.agent_id,
            recipient_id=msg_data.get("sender_id", "coordinator"),
            version=version,
            status=result["status"],
            message=result.get("message", ""),
            previous_version=result.get("previous_version", ""),
            rollback_available=result.get("rollback_available", False),
        )

        return {
            "status": result["status"],
            "version": version,
            "installed": result["status"] == "success",
            "rollback_available": result.get("rollback_available", False),
        }

    def _handle_ota_rollback(self, msg_data: Dict) -> Dict[str, Any]:
        """
        Handle OTA_UPDATE_ROLLBACK — execute rollback automatically.

        No user consent needed for rollback either.
        """
        payload = msg_data.get("payload", {})
        result = self.ota_installer.execute_rollback(payload)

        if result.get("success"):
            self.current_version = payload.get("target_version", self.current_version)
            self.adapter_stats["ota_rollbacks"] += 1

        return {
            "status": "rolled_back" if result.get("success") else "rollback_failed",
            "version": self.current_version,
            "details": result,
        }

    # ─── Peer Message Handlers ──────────────────────────────────────

    def _handle_peer_request(self, msg_data: Dict) -> Dict[str, Any]:
        """Handle a peer request message."""
        return {
            "status": "received",
            "type": "peer_request",
            "sender": msg_data.get("sender_id"),
        }

    def _handle_peer_response(self, msg_data: Dict) -> Dict[str, Any]:
        """Handle a peer response message."""
        return {
            "status": "received",
            "type": "peer_response",
        }

    def _handle_peer_notify(self, msg_data: Dict) -> Dict[str, Any]:
        """Handle a peer notification."""
        return {"status": "notified", "type": "peer_notify"}

    def _handle_status_query(self, msg_data: Dict) -> Dict[str, Any]:
        """Handle a status query — return agent status."""
        return self.get_status()

    def _handle_peer_heartbeat(self, msg_data: Dict) -> Dict[str, Any]:
        """Handle a peer heartbeat."""
        return {"status": "alive", "agent_id": self.agent_id}

    def _handle_consensus_request(self, msg_data: Dict) -> Dict[str, Any]:
        """
        Handle a consensus request — needs application logic.

        Default: acknowledge receipt. Real implementations should
        register a custom handler for decision logic.
        """
        return {
            "status": "acknowledged",
            "type": "consensus_request",
            "note": "Register custom handler for decision logic",
        }

    def _handle_consensus_vote(self, msg_data: Dict) -> Dict[str, Any]:
        """Handle a consensus vote message."""
        return {"status": "received", "type": "consensus_vote"}

    def _handle_task_delegate(self, msg_data: Dict) -> Dict[str, Any]:
        """Handle a task delegation — needs application logic."""
        self.adapter_stats["tasks_delegated_received"] += 1
        return {
            "status": "received",
            "type": "task_delegate",
            "note": "Register custom handler for task execution logic",
        }

    def _handle_file_send(self, msg_data: Dict) -> Dict[str, Any]:
        """Handle an incoming file send."""
        return {"status": "received", "type": "file_send"}

    def _handle_context_share(self, msg_data: Dict) -> Dict[str, Any]:
        """Handle context sharing from a peer."""
        return {"status": "received", "type": "context_share"}

    def _handle_error_report(self, msg_data: Dict) -> Dict[str, Any]:
        """Handle an error report from a peer."""
        self.adapter_stats["errors_handled"] += 1
        return {"status": "received", "type": "error_report"}

    def __repr__(self):
        return (
            f"OpenClawAgentAdapter("
            f"id={self.agent_id}, "
            f"status={self._status}, "
            f"v{self.current_version}, "
            f"caps={len(self.capabilities)})"
        )
