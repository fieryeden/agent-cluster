#!/usr/bin/env python3
"""
Network Coordinator

Extends the base Coordinator to use network transport.
Replaces file-based messaging with TCP/WebSocket connections.

Now supports agent-to-agent communication with full conversation logging.
The coordinator acts as a relay/broker — agents send peer messages through
the coordinator, which routes them and logs every exchange.
"""

import os
import socket
import threading
import time
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path
import uuid

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import BaseMessage, MessageType, create_message
from capabilities.registry import CapabilityRegistry
from capabilities.discovery import CapabilityDiscovery, CapabilityQuery, CapabilityQueryType
from coordinator.coordinator import Coordinator
from network.transport import NetworkServer, ConnectionInfo


class NetworkCoordinator(Coordinator):
    """
    Network-based coordinator with agent-to-agent messaging.

    Extends file-based coordinator with:
    - Network transport (TCP)
    - Agent-to-agent message relay
    - Conversation logging (all peer exchanges persisted)
    - Conversation management (start, close, search, export)

    Usage:
        coordinator = NetworkCoordinator(port=7890)
        coordinator.start()

        # Agents can now talk to each other through the coordinator
        # All conversations are logged to SQLite
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = NetworkServer.DEFAULT_PORT,
        cluster_dir: str = None,
        conversation_db: str = None,
    ):
        """
        Initialize network coordinator.

        Args:
            host: Host to bind to
            port: Port to listen on
            cluster_dir: Optional cluster directory for file fallback
            conversation_db: Path to conversation log database
        """
        super().__init__(cluster_dir=cluster_dir or "/tmp/agent_cluster")
        self.host = host
        self.port = port

        # Network components
        self.network_server = NetworkServer(
            host=host,
            port=port,
            message_handler=self._handle_network_message,
        )

        # Map agent IDs to connections
        self.agent_connections: Dict[str, ConnectionInfo] = {}

        # Conversation log
        from protocol.conversation_log import ConversationLog
        from protocol.ota_manager import CoordinatorOTAManager
        _cluster_dir = cluster_dir or '/tmp/agent_cluster'
        db_path = conversation_db or os.path.join(_cluster_dir, 'conversations.db')
        self.conversation_log = ConversationLog(db_path)

        # OTA update manager
        self.ota_manager = CoordinatorOTAManager(
            install_dir=os.path.join(_cluster_dir, 'ota_updates')
        )

        # Active conversation threads (conversation_id -> metadata)
        self.active_conversations: Dict[str, Dict[str, Any]] = {}

        # Stats
        self.network_stats = {
            "messages_received": 0,
            "messages_sent": 0,
            "bytes_received": 0,
            "bytes_sent": 0,
            "peer_messages_relayed": 0,
            "conversations_started": 0,
        }

    def start(self):
        """Start the coordinator."""
        super().start()
        self.network_server.start()
        print(f"Network coordinator started on {self.host}:{self.port}")

    def stop(self):
        """Stop the coordinator."""
        self.network_server.stop()
        super().stop()
        if self.conversation_log:
            self.conversation_log.close()
        print("Network coordinator stopped")

    # ─── Network Message Handler ────────────────────────────────────

    def _handle_network_message(self, message: BaseMessage, conn: ConnectionInfo):
        """Handle a message received over network."""
        self.network_stats["messages_received"] += 1
        self.network_stats["bytes_received"] += conn.bytes_received

        if message.sender_id and message.sender_id not in self.agent_connections:
            self.agent_connections[message.sender_id] = conn

        # Peer-to-peer messages (agent-to-agent via coordinator relay)
        if message.msg_type == MessageType.PEER_REQUEST:
            self._handle_peer_request(message)
        elif message.msg_type == MessageType.PEER_RESPONSE:
            self._handle_peer_response(message)
        elif message.msg_type == MessageType.PEER_NOTIFY:
            self._handle_peer_notify(message)
        elif message.msg_type == MessageType.PEER_BROADCAST:
            self._handle_peer_broadcast(message)
        elif message.msg_type == MessageType.CONVERSATION_START:
            self._handle_conversation_start(message)
        elif message.msg_type == MessageType.CONVERSATION_END:
            self._handle_conversation_end(message)
        elif message.msg_type == MessageType.FILE_SEND:
            self._handle_file_send(message)
        elif message.msg_type == MessageType.FILE_REQUEST:
            self._handle_file_request(message)
        elif message.msg_type == MessageType.FILE_SEND_RESPONSE:
            self._handle_file_send_response(message)
        elif message.msg_type == MessageType.TASK_DELEGATE:
            self._handle_task_delegate(message)
        elif message.msg_type == MessageType.TASK_DELEGATE_RESPONSE:
            self._handle_task_delegate_response(message)
        elif message.msg_type == MessageType.STATUS_QUERY:
            self._handle_status_query(message)
        elif message.msg_type == MessageType.STATUS_RESPONSE:
            self._handle_status_response(message)
        elif message.msg_type == MessageType.CAPABILITY_SHARE:
            self._handle_capability_share(message)
        elif message.msg_type == MessageType.CONSENSUS_REQUEST:
            self._handle_consensus_request(message)
        elif message.msg_type == MessageType.CONSENSUS_VOTE:
            self._handle_consensus_vote(message)
        elif message.msg_type == MessageType.HEARTBEAT_PEER:
            self._handle_heartbeat_peer(message)
        elif message.msg_type == MessageType.CONTEXT_SHARE:
            self._handle_context_share(message)
        elif message.msg_type == MessageType.ERROR_REPORT:
            self._handle_error_report(message)
        elif message.msg_type == MessageType.OTA_UPDATE_ANNOUNCE:
            self._handle_ota_update_announce(message)
        elif message.msg_type == MessageType.OTA_UPDATE_ACK:
            self._handle_ota_update_ack(message)
        elif message.msg_type == MessageType.OTA_UPDATE_STATUS:
            self._handle_ota_update_status(message)
        elif message.msg_type == MessageType.OTA_UPDATE_ROLLBACK:
            self._handle_ota_update_rollback(message)

        # Original coordinator messages
        elif message.msg_type == MessageType.REGISTER:
            self._handle_registration(message, conn)
        elif message.msg_type == MessageType.HEARTBEAT:
            self._handle_heartbeat(message, conn)
        elif message.msg_type == MessageType.TASK_COMPLETE:
            self._handle_task_complete(message)
        elif message.msg_type == MessageType.TASK_FAILED:
            self._handle_task_failed(message)
        elif message.msg_type == MessageType.CAPABILITY_QUERY:
            self._handle_capability_query(message, conn)
        elif message.msg_type == MessageType.CAPABILITY_RESPONSE:
            self._handle_capability_response(message)
        else:
            # Generic relay for unknown types
            if message.recipient_id and message.recipient_id != "coordinator":
                self.route_message(message.recipient_id, message)

    # ─── Agent-to-Agent Message Handlers ────────────────────────────

    def _handle_peer_request(self, message: BaseMessage):
        """
        Relay a peer request from one agent to another and log it.

        Payload: {conversation_id, content, request_type, context}
        """
        conversation_id = message.payload.get("conversation_id", "")
        if not conversation_id:
            conversation_id = f"conv-{uuid.uuid4().hex[:8]}"
            message.payload["conversation_id"] = conversation_id

        # Log the message
        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="peer_request",
            content=message.payload.get("content", ""),
            metadata={
                "request_type": message.payload.get("request_type", "general"),
                "context": message.payload.get("context", {}),
            },
        )

        # Relay to target agent
        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)

        self.network_stats["peer_messages_relayed"] += 1
        print(f"[RELAY] {message.sender_id} → {message.recipient_id}: peer_request ({conversation_id}) [{status}]")

    def _handle_peer_response(self, message: BaseMessage):
        """
        Relay a peer response and log it.

        Payload: {conversation_id, content, in_reply_to, response_type}
        """
        conversation_id = message.payload.get("conversation_id", "")

        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="peer_response",
            content=message.payload.get("content", ""),
            metadata={
                "in_reply_to": message.payload.get("in_reply_to", ""),
                "response_type": message.payload.get("response_type", "answer"),
            },
        )

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)

        self.network_stats["peer_messages_relayed"] += 1
        print(f"[RELAY] {message.sender_id} → {message.recipient_id}: peer_response ({conversation_id}) [{status}]")

    def _handle_peer_notify(self, message: BaseMessage):
        """
        Relay a notification (no response expected) and log it.

        Payload: {conversation_id, content, notification_type}
        """
        conversation_id = message.payload.get("conversation_id", "")

        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="peer_notify",
            content=message.payload.get("content", ""),
            metadata={
                "notification_type": message.payload.get("notification_type", "info"),
            },
        )

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)

        self.network_stats["peer_messages_relayed"] += 1
        print(f"[RELAY] {message.sender_id} → {message.recipient_id}: peer_notify ({conversation_id}) [{status}]")

    def _handle_peer_broadcast(self, message: BaseMessage):
        """
        Broadcast a message from one agent to all other agents and log it.

        Payload: {content, topic, exclude}
        """
        topic = message.payload.get("topic", "")
        exclude = message.payload.get("exclude", [])

        # Generate conversation ID for the broadcast thread
        conversation_id = f"broadcast-{uuid.uuid4().hex[:8]}"

        # Log the broadcast
        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id="broadcast",
            msg_type="peer_broadcast",
            content=message.payload.get("content", ""),
            metadata={"topic": topic, "exclude": exclude},
        )

        # Broadcast to all connected agents (excluding sender and excluded list)
        all_exclude = [message.sender_id] + exclude
        count = self.broadcast_message(message, exclude=all_exclude)

        self.conversation_log.update_delivery_status(message.message_id, "delivered")
        self.network_stats["peer_messages_relayed"] += count

        print(f"[BROADCAST] {message.sender_id} → all ({count} agents): {topic or 'no topic'}")

    def _handle_conversation_start(self, message: BaseMessage):
        """
        Initiate a new conversation thread.

        Payload: {topic, initial_message}
        Creates a conversation_id, logs the start, and notifies the target agent.
        """
        conversation_id = f"conv-{uuid.uuid4().hex[:12]}"
        topic = message.payload.get("topic", "")

        # Register the conversation
        self.active_conversations[conversation_id] = {
            "topic": topic,
            "initiator": message.sender_id,
            "participant": message.recipient_id,
            "started_at": datetime.now(timezone.utc).isoformat() + "Z",
            "status": "active",
        }

        # Set topic in conversation log
        self.conversation_log.set_conversation_topic(conversation_id, topic)

        # Log the initial message if provided
        initial_content = message.payload.get("initial_message", "")
        if initial_content:
            self.conversation_log.log_message(
                conversation_id=conversation_id,
                message_id=message.message_id,
                sender_id=message.sender_id,
                recipient_id=message.recipient_id,
                msg_type="conversation_start",
                content=initial_content,
                metadata={"topic": topic},
            )

        # Send conversation_start to target agent with the generated conversation_id
        start_msg = create_message(
            msg_type=MessageType.CONVERSATION_START,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            payload={
                "conversation_id": conversation_id,
                "topic": topic,
                "initial_message": initial_content,
            },
        )
        success = self.route_message(message.recipient_id, start_msg)

        self.network_stats["conversations_started"] += 1
        print(f"[CONV START] {conversation_id}: {message.sender_id} ↔ {message.recipient_id} topic='{topic}' [{conversation_id}]")

        # Also send conversation_id back to the initiator
        ack = create_message(
            msg_type=MessageType.CONVERSATION_START,
            sender_id="coordinator",
            recipient_id=message.sender_id,
            payload={
                "conversation_id": conversation_id,
                "topic": topic,
                "status": "started",
            },
        )
        self.route_message(message.sender_id, ack)

    def _handle_conversation_end(self, message: BaseMessage):
        """
        Close a conversation thread.

        Payload: {conversation_id, summary}
        """
        conversation_id = message.payload.get("conversation_id", "")
        summary = message.payload.get("summary", "")

        # Log the end message
        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="conversation_end",
            content=summary,
            metadata={"summary": summary},
        )

        # Close the conversation
        self.conversation_log.close_conversation(conversation_id)
        if conversation_id in self.active_conversations:
            self.active_conversations[conversation_id]["status"] = "closed"

        # Notify the other agent
        end_msg = create_message(
            msg_type=MessageType.CONVERSATION_END,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            payload={
                "conversation_id": conversation_id,
                "summary": summary,
                "status": "closed",
            },
        )
        self.route_message(message.recipient_id, end_msg)

        print(f"[CONV END] {conversation_id}: closed by {message.sender_id}")

    # ─── File Transfer Handlers ──────────────────────────────────────

    def _handle_file_send(self, message: BaseMessage):
        """
        Relay a file from one agent to another and log the transfer.

        The file data (base64-encoded) is carried in the message payload.
        Coordinator logs the metadata (filename, type, size, checksum) but
        does NOT store the file data itself — it just relays the message.

        Payload: {conversation_id, filename, file_data, file_type, description, encoding, checksum}
        """
        conversation_id = message.payload.get("conversation_id", "")
        filename = message.payload.get("filename", "unknown")
        file_type = message.payload.get("file_type", "application/octet-stream")
        file_data = message.payload.get("file_data", "")
        checksum = message.payload.get("checksum", "")
        description = message.payload.get("description", "")

        # Approximate size from base64 (base64 is ~4/3 of raw)
        import base64
        try:
            raw_size = len(base64.b64decode(file_data)) if file_data else 0
        except Exception:
            raw_size = 0

        # Log the file transfer (metadata only, not the file data itself)
        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="file_send",
            content=f"[FILE] {filename} ({raw_size} bytes, {file_type}) — {description}",
            metadata={
                "filename": filename,
                "file_type": file_type,
                "file_size": raw_size,
                "checksum": checksum,
                "encoding": message.payload.get("encoding", "base64"),
                "description": description,
            },
        )

        # Relay the full message (including file_data) to the recipient
        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)

        self.network_stats["peer_messages_relayed"] += 1
        print(
            f"[FILE SEND] {message.sender_id} → {message.recipient_id}: "
            f"{filename} ({raw_size} bytes, {file_type}) [{status}]"
        )

    def _handle_file_request(self, message: BaseMessage):
        """
        Relay a file request from one agent to another and log it.

        The recipient should respond with a FILE_SEND message containing the file.

        Payload: {conversation_id, filename, description, file_type_hint}
        """
        conversation_id = message.payload.get("conversation_id", "")
        filename = message.payload.get("filename", "")

        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="file_request",
            content=f"[FILE REQUEST] {filename} — {message.payload.get('description', '')}",
            metadata={
                "filename": filename,
                "file_type_hint": message.payload.get("file_type_hint", ""),
                "description": message.payload.get("description", ""),
            },
        )

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)

        self.network_stats["peer_messages_relayed"] += 1
        print(
            f"[FILE REQUEST] {message.sender_id} → {message.recipient_id}: "
            f"{filename} [{status}]"
        )

    # ─── File Transfer Accept/Decline ────────────────────────────────

    def _handle_file_send_response(self, message: BaseMessage):
        """Relay file transfer acceptance/decline back to the sender."""
        conversation_id = message.payload.get("conversation_id", "")
        accepted = message.payload.get("accepted", False)
        file_msg_id = message.payload.get("file_message_id", "")

        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="file_send_response",
            content=f"[FILE {'ACCEPTED' if accepted else 'DECLINED'}] {file_msg_id}",
            metadata={
                "file_message_id": file_msg_id,
                "accepted": accepted,
                "reason": message.payload.get("reason", ""),
                "save_as": message.payload.get("save_as", ""),
            },
        )

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)

        action = "ACCEPTED" if accepted else "DECLINED"
        print(f"[FILE {action}] {message.sender_id} → {message.recipient_id}: {file_msg_id}")

    # ─── Task Delegation Handlers ─────────────────────────────────────

    def _handle_task_delegate(self, message: BaseMessage):
        """Relay task delegation from one agent to another and log it."""
        conversation_id = message.payload.get("conversation_id", "")
        task_desc = message.payload.get("task_description", "")
        priority = message.payload.get("priority", "normal")

        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="task_delegate",
            content=f"[DELEGATE] {task_desc[:80]} (priority: {priority})",
            metadata={
                "task_description": task_desc,
                "priority": priority,
                "deadline": message.payload.get("deadline", ""),
                "context": message.payload.get("context", {}),
            },
        )

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)
        self.network_stats["peer_messages_relayed"] += 1
        print(f"[DELEGATE] {message.sender_id} → {message.recipient_id}: {task_desc[:50]} [{status}]")

    def _handle_task_delegate_response(self, message: BaseMessage):
        """Relay delegation response (accept/decline/counter) back to delegator."""
        conversation_id = message.payload.get("conversation_id", "")
        response = message.payload.get("response", "")
        delegate_msg_id = message.payload.get("delegate_message_id", "")

        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="task_delegate_response",
            content=f"[DELEGATE {response.upper()}] for {delegate_msg_id}",
            metadata={
                "delegate_message_id": delegate_msg_id,
                "response": response,
                "reason": message.payload.get("reason", ""),
                "counter_offer": message.payload.get("counter_offer", {}),
                "estimated_completion": message.payload.get("estimated_completion", ""),
            },
        )

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)
        self.network_stats["peer_messages_relayed"] += 1
        print(f"[DELEGATE {response.upper()}] {message.sender_id} → {message.recipient_id} [{status}]")

    # ─── Status & Capability Handlers ─────────────────────────────────

    def _handle_status_query(self, message: BaseMessage):
        """Relay status query to target agent."""
        conversation_id = message.payload.get("conversation_id", "")
        query = message.payload.get("query", "")

        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="status_query",
            content=f"[STATUS?] {query}",
            metadata=message.payload,
        )

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)
        self.network_stats["peer_messages_relayed"] += 1
        print(f"[STATUS?] {message.sender_id} → {message.recipient_id}: {query[:50]} [{status}]")

    def _handle_status_response(self, message: BaseMessage):
        """Relay status response back to querying agent."""
        conversation_id = message.payload.get("conversation_id", "")
        agent_status = message.payload.get("status", "")
        availability = message.payload.get("availability", "unknown")

        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="status_response",
            content=f"[STATUS] {agent_status} (availability: {availability})",
            metadata=message.payload,
        )

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)
        self.network_stats["peer_messages_relayed"] += 1
        print(f"[STATUS] {message.sender_id}: {agent_status} ({availability}) [{status}]")

    def _handle_capability_share(self, message: BaseMessage):
        """Relay capability announcement to specific peer and log it."""
        conversation_id = message.payload.get("conversation_id", "")
        cap_name = message.payload.get("capability_name", "")
        confidence = message.payload.get("confidence", 0.0)

        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="capability_share",
            content=f"[CAP] {cap_name} (confidence: {confidence})",
            metadata=message.payload,
        )

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)
        self.network_stats["peer_messages_relayed"] += 1
        print(f"[CAP SHARE] {message.sender_id} → {message.recipient_id}: {cap_name} [{status}]")

    # ─── Consensus Handlers ───────────────────────────────────────────

    def __init_consensus_if_needed(self):
        """Lazy-init the consensus tracking dict."""
        if not hasattr(self, "_consensus_votes"):
            self._consensus_votes = {}  # consensus_msg_id → {voter: vote}
            self._consensus_proposals = {}  # consensus_msg_id → proposal details

    def _handle_consensus_request(self, message: BaseMessage):
        """Broadcast consensus proposal to voters and start tracking votes."""
        self.__init_consensus_if_needed()

        proposal = message.payload.get("proposal", "")
        options = message.payload.get("options", ["accept", "reject"])
        voters = message.payload.get("voters", [])
        quorum = message.payload.get("quorum", 0)

        # Track this proposal
        self._consensus_proposals[message.message_id] = {
            "proposer": message.sender_id,
            "proposal": proposal,
            "options": options,
            "voters": voters,
            "quorum": quorum,
            "votes": {},
            "created_at": datetime.now(timezone.utc).isoformat() + "Z",
        }
        self._consensus_votes[message.message_id] = {}

        # Log the proposal
        self.conversation_log.log_message(
            conversation_id=f"consensus-{message.message_id[:8]}",
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id="broadcast",
            msg_type="consensus_request",
            content=f"[CONSENSUS?] {proposal[:80]}",
            metadata=message.payload,
        )

        # Relay to all voters (or broadcast if no specific voters)
        targets = voters if voters else list(self.agent_connections.keys())
        for voter_id in targets:
            if voter_id != message.sender_id:  # Don't send back to proposer
                self.route_message(voter_id, message)

        self.network_stats["peer_messages_relayed"] += 1
        self.network_stats.setdefault("consensus_proposals", 0)
        self.network_stats["consensus_proposals"] += 1
        print(f"[CONSENSUS?] {message.sender_id} proposes: {proposal[:50]} → {len(targets)} voters")

    def _handle_consensus_vote(self, message: BaseMessage):
        """Record a vote and relay to the proposer. Announce result if quorum reached."""
        self.__init_consensus_if_needed()

        consensus_msg_id = message.payload.get("consensus_message_id", "")
        vote = message.payload.get("vote", "")
        reasoning = message.payload.get("reasoning", "")

        # Record the vote
        if consensus_msg_id in self._consensus_votes:
            self._consensus_votes[consensus_msg_id][message.sender_id] = {
                "vote": vote,
                "reasoning": reasoning,
                "conditions": message.payload.get("conditions", {}),
            }

        # Log the vote
        self.conversation_log.log_message(
            conversation_id=f"consensus-{consensus_msg_id[:8]}",
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="consensus_vote",
            content=f"[VOTE] {vote} — {reasoning[:50]}",
            metadata=message.payload,
        )

        # Relay to proposer
        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)
        self.network_stats["peer_messages_relayed"] += 1

        # Check if quorum reached
        proposal = self._consensus_proposals.get(consensus_msg_id, {})
        votes = self._consensus_votes.get(consensus_msg_id, {})
        voters = proposal.get("voters", [])
        quorum = proposal.get("quorum", 0)

        if quorum > 0 and len(votes) >= quorum:
            # Tally and announce
            tally = {}
            for v in votes.values():
                choice = v["vote"]
                tally[choice] = tally.get(choice, 0) + 1
            winner = max(tally, key=tally.get) if tally else None
            print(f"[CONSENSUS RESULT] {consensus_msg_id[:8]}: {tally} → winner: {winner}")

        print(f"[VOTE] {message.sender_id} votes {vote} on {consensus_msg_id[:8]} [{status}]")

    # ─── Peer Health & Context Handlers ───────────────────────────────

    def _handle_heartbeat_peer(self, message: BaseMessage):
        """Relay peer heartbeat and log it."""
        self.conversation_log.log_message(
            conversation_id=f"heartbeat-{message.sender_id}-{message.recipient_id}",
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="heartbeat_peer",
            content="[PEER PING]",
            metadata=message.payload,
        )

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)
        print(f"[PEER PING] {message.sender_id} → {message.recipient_id} [{status}]")

    def _handle_context_share(self, message: BaseMessage):
        """Relay context data to target agent and log it."""
        conversation_id = message.payload.get("conversation_id", "")
        context_type = message.payload.get("context_type", "")
        description = message.payload.get("description", "")

        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="context_share",
            content=f"[CONTEXT] {context_type}: {description[:60]}",
            metadata={
                "context_type": context_type,
                "description": description,
                "context_data_keys": list(message.payload.get("context_data", {}).keys()),
            },
        )

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)
        self.network_stats["peer_messages_relayed"] += 1
        print(f"[CONTEXT] {message.sender_id} → {message.recipient_id}: {context_type} [{status}]")

    def _handle_error_report(self, message: BaseMessage):
        """Relay error report to target agent and log it with severity tracking."""
        conversation_id = message.payload.get("conversation_id", "")
        error_type = message.payload.get("error_type", "")
        error_msg = message.payload.get("error_message", "")
        severity = message.payload.get("severity", "error")

        self.conversation_log.log_message(
            conversation_id=conversation_id,
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type="error_report",
            content=f"[ERROR/{severity.upper()}] {error_type}: {error_msg[:60]}",
            metadata=message.payload,
        )

        # Track error count
        self.network_stats.setdefault("errors_reported", 0)
        self.network_stats["errors_reported"] += 1

        success = self.route_message(message.recipient_id, message)
        status = "delivered" if success else "failed"
        self.conversation_log.update_delivery_status(message.message_id, status)

        print(f"[ERROR/{severity.upper()}] {message.sender_id} → {message.recipient_id}: {error_type} [{status}]")



    # ─── OTA Update Handlers ─────────────────────────────────────────

    def _handle_ota_update_announce(self, message):
        """Handle OTA_UPDATE_ANNOUNCE — coordinator broadcasting update availability.

        This is typically sent BY the coordinator, but if an agent sends it
        (e.g. a sub-coordinator), we relay to the fleet.
        """
        version = message.payload.get("version", "unknown")
        self.ota_manager.mark_announced(version, count=1)
        self.conversation_log.log_message(
            conversation_id=f"ota-{version}",
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id="broadcast",
            msg_type=MessageType.OTA_UPDATE_ANNOUNCE.value,
            content=f"OTA update announced: v{version}",
            metadata=message.payload,
        )
        self.network_stats["messages_sent"] += 1
        self._log_info(f"OTA update v{version} announced by {message.sender_id}")

    def _handle_ota_update_ack(self, message):
        """Handle OTA_UPDATE_ACK — agent acknowledging update announcement.

        If agent is ready, coordinator sends the package automatically.
        No user consent — this is automatic.
        """
        version = message.payload.get("version", "unknown")
        ready = message.payload.get("ready", True)
        announce_msg_id = message.payload.get("announce_message_id", "")
        current_version = message.payload.get("current_version", "")

        self.ota_manager.record_ack(version, message.sender_id, ready, current_version)

        self.conversation_log.log_message(
            conversation_id=f"ota-{version}",
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type=MessageType.OTA_UPDATE_ACK.value,
            content=f"OTA ACK: v{version} ready={ready}",
            metadata=message.payload,
        )

        # If agent is ready, send the package immediately
        if ready:
            package_payload = self.ota_manager.get_package_message(version, message.sender_id)
            if package_payload:
                from protocol.messages import ota_update_package
                pkg_msg = ota_update_package(
                    sender_id="coordinator",
                    recipient_id=message.sender_id,
                    version=version,
                    **{k: v for k, v in package_payload.items() if k != "version"},
                )
                self._send_to_agent(message.sender_id, pkg_msg)
                self.network_stats["messages_sent"] += 1
                self._log_info(f"OTA package v{version} sent to {message.sender_id}")

    def _handle_ota_update_status(self, message):
        """Handle OTA_UPDATE_STATUS — agent reporting install progress/result.

        Track fleet-wide status. If a failure is reported and rollback
        is available, coordinator can command rollback automatically.
        """
        version = message.payload.get("version", "unknown")
        status = message.payload.get("status", "unknown")
        status_message = message.payload.get("message", "")

        self.ota_manager.record_status(version, message.sender_id, status, status_message)

        self.conversation_log.log_message(
            conversation_id=f"ota-{version}",
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type=MessageType.OTA_UPDATE_STATUS.value,
            content=f"OTA status: v{version} → {status} ({status_message})",
            metadata=message.payload,
        )

        # Auto-rollback on failure if rollback is available
        if status == "failed" and message.payload.get("rollback_available", False):
            from protocol.messages import ota_update_rollback
            rollback_msg = ota_update_rollback(
                sender_id="coordinator",
                recipient_id=message.sender_id,
                version=version,
                reason=f"Auto-rollback: install failed ({status_message})",
            )
            self._send_to_agent(message.sender_id, rollback_msg)
            self.network_stats["messages_sent"] += 1
            self._log_warning(f"OTA v{version} failed on {message.sender_id} — auto-rollback commanded")

    def _handle_ota_update_rollback(self, message):
        """Handle OTA_UPDATE_ROLLBACK — coordinator commanding rollback.

        Log the rollback command. The agent's OTA installer will handle
        the actual rollback execution.
        """
        version = message.payload.get("version", "unknown")
        reason = message.payload.get("reason", "")

        self.conversation_log.log_message(
            conversation_id=f"ota-{version}",
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            msg_type=MessageType.OTA_UPDATE_ROLLBACK.value,
            content=f"OTA rollback commanded: v{version} ({reason})",
            metadata=message.payload,
        )
        self._log_info(f"OTA rollback for v{version} commanded — reason: {reason}")

    # ─── Conversation Query API ─────────────────────────────────────

    def get_conversation_log(self, conversation_id: str, limit: int = 100) -> List[Dict]:
        """Get all messages in a conversation."""
        entries = self.conversation_log.get_conversation(conversation_id, limit=limit)
        return [e.to_dict() for e in entries]

    def get_agent_conversations(self, agent_id: str, limit: int = 50) -> List[Dict]:
        """Get all conversations involving an agent."""
        return self.conversation_log.get_agent_conversations(agent_id, limit=limit)

    def search_conversations(self, query: str, limit: int = 20) -> List[Dict]:
        """Full-text search across all conversations."""
        entries = self.conversation_log.search_conversations(query, limit=limit)
        return [e.to_dict() for e in entries]

    def get_conversation_stats(self) -> Dict[str, Any]:
        """Get conversation statistics."""
        return self.conversation_log.get_stats()

    def export_conversation(self, conversation_id: str, format: str = "json") -> str:
        """Export a conversation as JSON or markdown."""
        return self.conversation_log.export_conversation(conversation_id, format=format)

    def export_all_conversations(self, format: str = "json") -> str:
        """Export all conversations."""
        return self.conversation_log.export_all(format=format)

    # ─── Original Coordinator Handlers (unchanged) ───────────────────

    def _handle_registration(self, message: BaseMessage, conn: ConnectionInfo):
        """Handle agent registration."""
        agent_id = message.sender_id
        capabilities = message.payload.get("capabilities", {})

        self.register_agent(agent_id, capabilities)

        conn.agent_id = agent_id
        self.agent_connections[agent_id] = conn

        ack = create_message(
            msg_type=MessageType.REGISTER,
            sender_id="coordinator",
            recipient_id=agent_id,
            payload={"status": "registered", "agent_id": agent_id},
        )
        self.network_server.send_message(agent_id, ack)
        print(f"Agent registered: {agent_id} from {conn.address}")

    def _handle_heartbeat(self, message: BaseMessage, conn: ConnectionInfo):
        """Handle heartbeat from agent."""
        agent_id = message.sender_id

        if agent_id in self.agents:
            self.agents[agent_id].last_heartbeat = datetime.now()
        conn.last_heartbeat = datetime.now()

        pong = create_message(
            msg_type=MessageType.HEARTBEAT,
            sender_id="coordinator",
            recipient_id=agent_id,
            payload={"pong": True},
        )
        self.network_server.send_message(agent_id, pong)

    def _handle_capability_query(self, message: BaseMessage, conn: ConnectionInfo):
        """Handle capability query."""
        query_type = message.payload.get("query_type")
        capability_name = message.payload.get("capability_name")

        discovery = CapabilityDiscovery(self.capability_registry)

        if query_type == "can_do":
            result = discovery.can_do(agent_id=message.sender_id, capability=capability_name)
        elif query_type == "list_all":
            result = discovery.list_all(agent_id=message.sender_id)
        elif query_type == "best_match":
            result = discovery.best_match(capability=capability_name)
        else:
            result = {"error": f"Unknown query type: {query_type}"}

        response = create_message(
            msg_type=MessageType.CAPABILITY_RESPONSE,
            sender_id="coordinator",
            recipient_id=message.sender_id,
            payload=result,
        )
        self.network_server.send_message(message.sender_id, response)

    def _handle_capability_response(self, message: BaseMessage):
        """Handle capability response."""
        pass

    def _handle_task_complete(self, message: BaseMessage):
        """Handle task completion."""
        task_id = message.payload.get("task_id")
        result = message.payload.get("result")
        if task_id in self.tasks:
            self.tasks[task_id].status = "completed"
            self.tasks[task_id].result = result

    def _handle_task_failed(self, message: BaseMessage):
        """Handle task failure."""
        task_id = message.payload.get("task_id")
        error = message.payload.get("error")
        if task_id in self.tasks:
            self.tasks[task_id].status = "failed"
            self.tasks[task_id].error = error

    # ─── Routing ────────────────────────────────────────────────────

    def route_message(self, recipient_id: str, message: BaseMessage) -> bool:
        """Route a message to an agent."""
        success = self.network_server.send_message(recipient_id, message)
        if success:
            self.network_stats["messages_sent"] += 1
        return success

    def broadcast_message(self, message: BaseMessage, exclude: List[str] = None) -> int:
        """Broadcast a message to all agents."""
        return self.network_server.broadcast_message(message, exclude)

    def assign_task_network(self, task_type: str, params: Dict[str, Any], target_agent: str = None) -> str:
        """Assign a task over the network."""
        task_id = f"task-{uuid.uuid4().hex[:8]}"

        if not target_agent:
            target_agent = self.capability_registry.find_best_agent(task_type)
        if not target_agent:
            print(f"No agent available for task: {task_type}")
            return None

        message = create_message(
            msg_type=MessageType.TASK_ASSIGN,
            sender_id="coordinator",
            recipient_id=target_agent,
            payload={
                "task_id": task_id,
                "task_type": task_type,
                "params": params,
            },
        )

        if self.route_message(target_agent, message):
            self.tasks[task_id] = type('Task', (), {
                'task_id': task_id,
                'task_type': task_type,
                'params': params,
                'status': 'pending',
                'assigned_to': target_agent,
                'result': None,
                'error': None,
            })()
            print(f"Task assigned: {task_id} → {target_agent}")
            return task_id
        return None

    def get_network_status(self) -> Dict[str, Any]:
        """Get network-specific status including conversation stats."""
        conv_stats = {}
        if self.conversation_log:
            conv_stats = self.conversation_log.get_stats()

        return {
            "host": self.host,
            "port": self.port,
            "connections": self.network_server.get_connection_stats(),
            "stats": self.network_stats,
            "conversations": conv_stats,
            "active_conversations": len(self.active_conversations),
            "agents": {
                agent_id: conn.to_dict()
                for agent_id, conn in self.agent_connections.items()
            },
        }


# Convenience functions
def create_network_coordinator(port: int = NetworkServer.DEFAULT_PORT) -> NetworkCoordinator:
    """Create a network coordinator."""
    return NetworkCoordinator(port=port)
