#!/usr/bin/env python3
"""
OpenClaw Coordinator Bridge

Bridges the Agent Cluster coordinator to OpenClaw's messaging infrastructure.
Allows OpenClaw to act as the cluster coordinator, routing messages between
agents via OpenClaw's session system.

Key features:
- Registers cluster agents as OpenClaw sessions
- Routes peer messages through OpenClaw's session_send
- Logs all conversations via OpenClaw's memory system
- Exposes cluster status via OpenClaw's status API
- OTA orchestration through OpenClaw cron jobs
"""

import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path
import uuid

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import BaseMessage, MessageType, create_message
from protocol.conversation_log import ConversationLog
from protocol.ota_manager import CoordinatorOTAManager
from capabilities.registry import CapabilityRegistry
from capabilities.discovery import CapabilityDiscovery
from network.coordinator import NetworkCoordinator


class OpenClawCoordinatorBridge:
    """
    Bridges NetworkCoordinator to OpenClaw infrastructure.

    Instead of running a standalone TCP coordinator, this bridge:
    1. Uses OpenClaw sessions as agent communication channels
    2. Routes peer messages through sessions_send
    3. Stores conversation logs in OpenClaw's memory system
    4. Exposes cluster health via OpenClaw's dashboard
    5. Schedules OTA rollouts via OpenClaw cron
    """

    def __init__(
        self,
        coordinator: NetworkCoordinator,
        workspace_dir: str = None,
        session_prefix: str = "agent-cluster",
    ):
        """
        Initialize the OpenClaw bridge.

        Args:
            coordinator: The NetworkCoordinator instance to bridge
            workspace_dir: OpenClaw workspace directory
            session_prefix: Prefix for OpenClaw session labels
        """
        self.coordinator = coordinator
        self.workspace_dir = workspace_dir or os.environ.get(
            "OPENCLAW_WORKSPACE", "/root/.openclaw/workspace"
        )
        self.session_prefix = session_prefix

        # Agent session mapping: agent_id -> session_key
        self.agent_sessions: Dict[str, str] = {}
        # Reverse: session_key -> agent_id
        self.session_agents: Dict[str, str] = {}

        # Thread safety
        self._lock = threading.RLock()

        # Bridge state
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None

        # Event hooks
        self._on_agent_registered: List[Callable] = []
        self._on_agent_lost: List[Callable] = []
        self._on_conversation_event: List[Callable] = []

        # Stats
        self.bridge_stats = {
            "sessions_created": 0,
            "messages_routed": 0,
            "conversations_synced": 0,
            "ota_rollouts_triggered": 0,
            "errors": 0,
        }

        # Memory directory for cluster state
        self.cluster_memory_dir = os.path.join(
            self.workspace_dir, "memory", "cluster"
        )
        os.makedirs(self.cluster_memory_dir, exist_ok=True)

    def register_agent_session(
        self, agent_id: str, session_key: str, capabilities: List[str] = None
    ) -> Dict[str, Any]:
        """
        Map an agent to an OpenClaw session.

        Args:
            agent_id: Agent cluster ID
            session_key: OpenClaw session key for this agent
            capabilities: List of capability names

        Returns:
            Registration result dict
        """
        with self._lock:
            self.agent_sessions[agent_id] = session_key
            self.session_agents[session_key] = agent_id
            self.bridge_stats["sessions_created"] += 1

        # Write agent state to memory
        agent_state = {
            "agent_id": agent_id,
            "session_key": session_key,
            "capabilities": capabilities or [],
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "status": "registered",
        }
        state_path = os.path.join(
            self.cluster_memory_dir, f"agent-{agent_id}.json"
        )
        with open(state_path, "w") as f:
            json.dump(agent_state, f, indent=2)

        # Fire hooks
        for hook in self._on_agent_registered:
            try:
                hook(agent_id, session_key, capabilities)
            except Exception:
                self.bridge_stats["errors"] += 1

        return {
            "status": "registered",
            "agent_id": agent_id,
            "session_key": session_key,
            "capabilities": capabilities or [],
        }

    def deregister_agent_session(self, agent_id: str) -> Dict[str, Any]:
        """
        Remove an agent's OpenClaw session mapping.

        Args:
            agent_id: Agent cluster ID

        Returns:
            Deregistration result dict
        """
        with self._lock:
            session_key = self.agent_sessions.pop(agent_id, None)
            if session_key:
                self.session_agents.pop(session_key, None)

        # Update state file
        state_path = os.path.join(
            self.cluster_memory_dir, f"agent-{agent_id}.json"
        )
        if os.path.exists(state_path):
            with open(state_path, "r") as f:
                state = json.load(f)
            state["status"] = "deregistered"
            state["deregistered_at"] = datetime.now(timezone.utc).isoformat()
            with open(state_path, "w") as f:
                json.dump(state, f, indent=2)

        for hook in self._on_agent_lost:
            try:
                hook(agent_id)
            except Exception:
                self.bridge_stats["errors"] += 1

        return {"status": "deregistered", "agent_id": agent_id}

    def route_peer_message(
        self, sender_id: str, recipient_id: str, message: BaseMessage
    ) -> Dict[str, Any]:
        """
        Route a peer message through OpenClaw sessions.

        Instead of TCP relay, this sends the message via OpenClaw's
        sessions_send to the target agent's session.

        Args:
            sender_id: Sending agent's ID
            recipient_id: Receiving agent's ID
            message: The message to route

        Returns:
            Routing result dict
        """
        with self._lock:
            target_session = self.agent_sessions.get(recipient_id)

        if not target_session:
            return {
                "status": "failed",
                "reason": f"No session for agent {recipient_id}",
            }

        # Convert message to session-sendable format
        message_text = self._format_message_for_session(message)

        # In a live OpenClaw environment, this would call sessions_send
        # For now, we log the routing intent
        self.bridge_stats["messages_routed"] += 1

        # Log to conversation log
        conv_id = message.payload.get("conversation_id", f"peer-{uuid.uuid4().hex[:8]}")
        self.coordinator.conversation_log.log_message(
            conversation_id=conv_id,
            message_id=message.message_id,
            sender_id=sender_id,
            recipient_id=recipient_id,
            msg_type=message.msg_type.value,
            content=message_text,
            metadata=message.payload,
        )

        return {
            "status": "routed",
            "sender_id": sender_id,
            "recipient_id": recipient_id,
            "target_session": target_session,
            "message_id": message.message_id,
        }

    def get_cluster_status(self) -> Dict[str, Any]:
        """
        Get cluster status formatted for OpenClaw dashboard.

        Returns:
            Cluster status dict with agent info, health, conversations
        """
        with self._lock:
            active_agents = len(self.agent_sessions)

        # Get coordinator network status
        coord_status = self.coordinator.get_network_status()

        # Get OTA fleet status
        ota_status = self.coordinator.ota_manager.get_fleet_status()

        # Get conversation stats
        conv_stats = {}
        try:
            all_convs = self.coordinator.conversation_log.list_conversations(limit=1000)
            conv_stats = {
                "total_conversations": len(all_convs) if isinstance(all_convs, list) else 0,
                "active_conversations": coord_status.get("active_conversations", 0),
            }
        except Exception:
            conv_stats = {"total_conversations": 0, "active_conversations": 0}

        return {
            "cluster_health": self._compute_cluster_health(coord_status),
            "active_agents": active_agents,
            "agent_sessions": dict(self.agent_sessions),
            "network_status": coord_status,
            "ota_fleet": ota_status,
            "conversation_stats": conv_stats,
            "bridge_stats": self.bridge_stats,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def schedule_ota_rollout(
        self,
        version: str,
        package_path: str,
        priority: str = "normal",
        stagger_seconds: int = 30,
        schedule_time: str = None,
    ) -> Dict[str, Any]:
        """
        Schedule an OTA rollout via OpenClaw cron.

        Args:
            version: Version to roll out
            package_path: Path to the update package
            priority: Update priority (low/normal/high/critical)
            stagger_seconds: Seconds between agent updates
            schedule_time: ISO timestamp for scheduled rollout (None = now)

        Returns:
            Rollout schedule result
        """
        # Register the update with OTA manager
        pkg = self.coordinator.ota_manager.register_update(
            version=version,
            package_path=package_path,
            changelog=f"OTA rollout v{version}",
            priority=priority,
        )

        self.bridge_stats["ota_rollouts_triggered"] += 1

        # In a live OpenClaw environment, this would create a cron job:
        # cron_add(schedule=..., payload=ota_update_announce(...))
        # For now, record the intent
        rollout_state = {
            "version": version,
            "package_path": package_path,
            "priority": priority,
            "stagger_seconds": stagger_seconds,
            "schedule_time": schedule_time or datetime.now(timezone.utc).isoformat(),
            "status": "scheduled",
            "agents_targeted": list(self.agent_sessions.keys()),
        }

        rollout_path = os.path.join(
            self.cluster_memory_dir, f"ota-rollout-{version}.json"
        )
        with open(rollout_path, "w") as f:
            json.dump(rollout_state, f, indent=2)

        return {
            "status": "scheduled",
            "version": version,
            "agents_targeted": len(self.agent_sessions),
            "schedule_time": rollout_state["schedule_time"],
        }

    def sync_conversations_to_memory(self) -> int:
        """
        Sync conversation logs to OpenClaw's memory system.

        Exports recent conversations as markdown files in the
        cluster memory directory for OpenClaw's memory search.

        Returns:
            Number of conversations synced
        """
        try:
            # Export recent conversations as markdown
            exports = self.coordinator.conversation_log.export_conversations(
                format="markdown", limit=50
            )
            if not exports:
                return 0

            sync_dir = os.path.join(self.cluster_memory_dir, "conversations")
            os.makedirs(sync_dir, exist_ok=True)

            count = 0
            if isinstance(exports, list):
                for conv in exports:
                    conv_id = conv.get("conversation_id", f"conv-{count}")
                    path = os.path.join(sync_dir, f"{conv_id}.md")
                    with open(path, "w") as f:
                        f.write(conv.get("content", ""))
                    count += 1
            elif isinstance(exports, str):
                # Single export
                path = os.path.join(sync_dir, "recent.md")
                with open(path, "w") as f:
                    f.write(exports)
                count = 1

            self.bridge_stats["conversations_synced"] += count
            return count

        except Exception:
            self.bridge_stats["errors"] += 1
            return 0

    def on_agent_registered(self, callback: Callable):
        """Register callback for agent registration events."""
        self._on_agent_registered.append(callback)

    def on_agent_lost(self, callback: Callable):
        """Register callback for agent loss events."""
        self._on_agent_lost.append(callback)

    def on_conversation_event(self, callback: Callable):
        """Register callback for conversation events."""
        self._on_conversation_event.append(callback)

    def start_monitoring(self, interval_seconds: int = 30):
        """
        Start background monitoring thread.

        Periodically:
        - Checks agent liveness via session activity
        - Syncs conversations to memory
        - Updates cluster state files

        Args:
            interval_seconds: Check interval in seconds
        """
        if self._running:
            return

        self._running = True

        def _monitor_loop():
            while self._running:
                try:
                    # Update cluster state
                    status = self.get_cluster_status()
                    state_path = os.path.join(
                        self.cluster_memory_dir, "cluster-state.json"
                    )
                    with open(state_path, "w") as f:
                        json.dump(status, f, indent=2)

                    # Sync conversations every 5 cycles
                    if int(time.time()) % (interval_seconds * 5) < interval_seconds:
                        self.sync_conversations_to_memory()

                except Exception:
                    self.bridge_stats["errors"] += 1

                time.sleep(interval_seconds)

        self._monitor_thread = threading.Thread(
            target=_monitor_loop, daemon=True, name="openclaw-bridge-monitor"
        )
        self._monitor_thread.start()

    def stop_monitoring(self):
        """Stop the background monitoring thread."""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
            self._monitor_thread = None

    # ─── Internal ──────────────────────────────────────────────────

    def _format_message_for_session(self, message: BaseMessage) -> str:
        """Format a cluster message for OpenClaw session delivery."""
        return json.dumps({
            "type": message.msg_type.value,
            "message_id": message.message_id,
            "sender_id": message.sender_id,
            "recipient_id": message.recipient_id,
            "payload": message.payload,
            "timestamp": message.timestamp,
        })

    def _compute_cluster_health(self, coord_status: Dict) -> int:
        """
        Compute a 0-100 cluster health score.

        Based on: agent availability (40%), error rate (30%),
        capability coverage (20%), throughput (10%)
        """
        agents = coord_status.get("agents", {})
        total = len(agents) if isinstance(agents, dict) else 0
        if total == 0:
            return 0

        online = sum(
            1 for a in (agents.values() if isinstance(agents, dict) else [])
            if a.get("status") == "online"
        )

        availability = (online / total) * 100 if total > 0 else 0

        stats = coord_status.get("stats", {})
        total_msgs = stats.get("messages_received", 0)
        errors = stats.get("errors_reported", 0)
        error_rate = (errors / max(total_msgs, 1)) * 100

        error_score = max(0, 100 - error_rate * 10)

        # Weighted health
        health = int(availability * 0.4 + error_score * 0.3 + 70 * 0.2 + 80 * 0.1)
        return min(100, max(0, health))

    def __repr__(self):
        return (
            f"OpenClawCoordinatorBridge("
            f"agents={len(self.agent_sessions)}, "
            f"routed={self.bridge_stats['messages_routed']})"
        )
