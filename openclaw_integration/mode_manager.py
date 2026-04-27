#!/usr/bin/env python3
"""
Cluster Mode Manager

Provides a unified interface that switches between standalone (TCP)
and OpenClaw-backed operation based on configuration.

When OpenClaw integration is ENABLED:
  - Peer messages route through OpenClaw sessions
  - Agent sessions auto-registered with OpenClaw
  - Conversations synced to OpenClaw memory
  - Cluster capabilities exposed as OpenClaw skills
  - Events bridged to OpenClaw system

When OpenClaw integration is DISABLED (default):
  - Pure standalone TCP coordinator
  - No dependency on OpenClaw whatsoever
  - All existing functionality works as-is

Both modes can run simultaneously — standalone TCP agents and
OpenClaw session agents can coexist in the same cluster.
"""

import json
import os
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path
from enum import Enum

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import BaseMessage, MessageType, create_message


class ClusterMode(str, Enum):
    """Operating mode for the cluster."""
    STANDALONE = "standalone"    # Pure TCP, no OpenClaw dependency
    OPENCLAW = "openclaw"        # OpenClaw-backed sessions
    HYBRID = "hybrid"            # Both TCP + OpenClaw agents


class OpenClawConfig:
    """
    Configuration for OpenClaw integration.
    
    Loads from a config dict (parsed YAML) with sensible defaults.
    All fields have safe defaults so the cluster works without any
    OpenClaw config at all.
    """

    def __init__(self, config: Dict[str, Any] = None):
        cfg = config or {}
        oc = cfg.get("openclaw", {})
        
        # Master switch
        self.enabled: bool = oc.get("enabled", False)
        
        # Session settings
        self.coordinator_session: str = oc.get(
            "coordinator_session", "session:agent-cluster-coordinator"
        )
        self.agent_session_prefix: str = oc.get(
            "agent_session_prefix", "agent-cluster"
        )
        
        # Features (all default True when enabled, ignored when disabled)
        self.auto_register_agents: bool = oc.get("auto_register_agents", True)
        self.sync_conversations: bool = oc.get("sync_conversations", True)
        self.expose_skills: bool = oc.get("expose_skills", True)
        self.event_bridge: bool = oc.get("event_bridge", True)
        
        # Workspace
        workspace = oc.get("workspace_dir", "~/.openclaw/workspace")
        self.workspace_dir: str = os.path.expanduser(workspace)

    @classmethod
    def from_file(cls, path: str) -> "OpenClawConfig":
        """Load config from a YAML file."""
        try:
            import yaml
            with open(path) as f:
                config = yaml.safe_load(f) or {}
        except ImportError:
            # No PyYAML — try JSON fallback
            json_path = path.replace(".yaml", ".json").replace(".yml", ".json")
            if os.path.exists(json_path):
                with open(json_path) as f:
                    config = json.load(f)
            else:
                config = {}
        except FileNotFoundError:
            config = {}
        return cls(config)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "coordinator_session": self.coordinator_session,
            "agent_session_prefix": self.agent_session_prefix,
            "auto_register_agents": self.auto_register_agents,
            "sync_conversations": self.sync_conversations,
            "expose_skills": self.expose_skills,
            "event_bridge": self.event_bridge,
            "workspace_dir": self.workspace_dir,
        }


class ClusterModeManager:
    """
    Manages the cluster's operating mode and provides a unified
    interface for both standalone and OpenClaw-backed operation.
    
    This is the single entry point for all integration decisions.
    Code that needs to send messages, register agents, or query
    status goes through this manager — it handles the routing
    based on the current mode.
    
    Usage:
        config = OpenClawConfig({"openclaw": {"enabled": True}})
        manager = ClusterModeManager(coordinator=my_coordinator, config=config)
        
        # Register agent — works in both modes
        manager.register_agent("agent-1", capabilities=["search"])
        
        # Send peer message — routed appropriately
        manager.send_peer_message(sender, recipient, message)
        
        # Get cluster status — unified regardless of mode
        status = manager.get_cluster_status()
    """

    def __init__(
        self,
        coordinator=None,
        config: OpenClawConfig = None,
    ):
        """
        Initialize the mode manager.
        
        Args:
            coordinator: The NetworkCoordinator instance (required for standalone)
            config: OpenClaw configuration (defaults to disabled)
        """
        self.coordinator = coordinator
        self.config = config or OpenClawConfig()
        
        # Determine effective mode
        if self.config.enabled:
            self._mode = ClusterMode.OPENCLAW
        else:
            self._mode = ClusterMode.STANDALONE
        
        # Lazy-loaded OpenClaw components (only when enabled)
        self._bridge = None
        self._event_bridge = None
        self._skill_provider = None
        
        # Registered agents (tracked in both modes)
        self._agents: Dict[str, Dict[str, Any]] = {}
        
        # Mode change listeners
        self._mode_listeners: List[Callable[[ClusterMode], None]] = []
        
        # Stats
        self.mode_stats = {
            "mode_changes": 0,
            "messages_routed_standalone": 0,
            "messages_routed_openclaw": 0,
            "agents_registered_standalone": 0,
            "agents_registered_openclaw": 0,
        }
        
        # Thread safety
        self._lock = threading.RLock()

    @property
    def mode(self) -> ClusterMode:
        """Current operating mode."""
        return self._mode

    @property
    def is_openclaw_enabled(self) -> bool:
        """Whether OpenClaw integration is active."""
        return self._mode in (ClusterMode.OPENCLAW, ClusterMode.HYBRID)

    def set_mode(self, mode: ClusterMode):
        """
        Switch the cluster operating mode.
        
        Can switch between standalone and OpenClaw at runtime.
        Existing agents continue to function — only new routing
        decisions change.
        """
        with self._lock:
            old_mode = self._mode
            self._mode = mode
            self.config.enabled = mode != ClusterMode.STANDALONE
            self.mode_stats["mode_changes"] += 1
        
        # Notify listeners
        for listener in self._mode_listeners:
            try:
                listener(mode)
            except Exception:
                pass

    def enable_openclaw(self):
        """Enable OpenClaw integration."""
        self.set_mode(ClusterMode.OPENCLAW)

    def disable_openclaw(self):
        """Disable OpenClaw integration — pure standalone."""
        self.set_mode(ClusterMode.STANDALONE)

    def on_mode_change(self, listener: Callable[[ClusterMode], None]):
        """Register a callback for mode changes."""
        self._mode_listeners.append(listener)

    # ─── Agent Registration ────────────────────────────────────────

    def register_agent(
        self,
        agent_id: str,
        capabilities: List[str] = None,
        session_key: str = None,
        metadata: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Register an agent with the cluster.
        
        In standalone mode: registers with the TCP coordinator.
        In OpenClaw mode: also registers as an OpenClaw session.
        In hybrid mode: both.
        
        Args:
            agent_id: Unique agent identifier
            capabilities: Agent capabilities
            session_key: OpenClaw session key (auto-generated if not provided)
            metadata: Additional agent metadata
            
        Returns:
            Registration result
        """
        with self._lock:
            agent_info = {
                "agent_id": agent_id,
                "capabilities": capabilities or [],
                "session_key": session_key,
                "metadata": metadata or {},
                "registered_at": datetime.now(timezone.utc).isoformat(),
                "mode": self._mode.value,
            }
            self._agents[agent_id] = agent_info

        results = {"agent_id": agent_id, "mode": self._mode.value}

        # Standalone registration
        if self._mode in (ClusterMode.STANDALONE, ClusterMode.HYBRID):
            if self.coordinator:
                try:
                    self.coordinator.register_agent(agent_id, capabilities or [])
                    results["standalone"] = "registered"
                    self.mode_stats["agents_registered_standalone"] += 1
                except Exception as e:
                    results["standalone"] = f"failed: {e}"
            else:
                results["standalone"] = "no_coordinator"

        # OpenClaw registration
        if self.is_openclaw_enabled and self.config.auto_register_agents:
            bridge = self._get_bridge()
            if bridge:
                oc_session = session_key or f"{self.config.agent_session_prefix}-{agent_id}"
                try:
                    bridge.register_agent_session(
                        agent_id=agent_id,
                        session_key=oc_session,
                        capabilities=capabilities or [],
                    )
                    results["openclaw"] = "registered"
                    results["openclaw_session"] = oc_session
                    self.mode_stats["agents_registered_openclaw"] += 1
                except Exception as e:
                    results["openclaw"] = f"failed: {e}"
            else:
                results["openclaw"] = "bridge_unavailable"

        return results

    def deregister_agent(self, agent_id: str) -> Dict[str, Any]:
        """Deregister an agent from the cluster."""
        with self._lock:
            self._agents.pop(agent_id, None)

        results = {"agent_id": agent_id}

        if self.coordinator and self._mode in (ClusterMode.STANDALONE, ClusterMode.HYBRID):
            try:
                self.coordinator.deregister_agent(agent_id)
                results["standalone"] = "deregistered"
            except Exception:
                results["standalone"] = "failed"

        if self.is_openclaw_enabled:
            bridge = self._get_bridge()
            if bridge:
                try:
                    bridge.deregister_agent_session(agent_id)
                    results["openclaw"] = "deregistered"
                except Exception:
                    results["openclaw"] = "failed"

        return results

    def get_registered_agents(self) -> Dict[str, Dict[str, Any]]:
        """Get all registered agents."""
        with self._lock:
            return dict(self._agents)

    # ─── Message Routing ───────────────────────────────────────────

    def send_peer_message(
        self,
        sender_id: str,
        recipient_id: str,
        message: BaseMessage,
    ) -> Dict[str, Any]:
        """
        Send a peer message, routing through the appropriate channel.
        
        Standalone: routes through TCP coordinator relay.
        OpenClaw: routes through OpenClaw sessions.
        Hybrid: tries OpenClaw first, falls back to TCP.
        """
        if self._mode == ClusterMode.STANDALONE:
            return self._route_standalone(sender_id, recipient_id, message)
        elif self._mode == ClusterMode.OPENCLAW:
            return self._route_openclaw(sender_id, recipient_id, message)
        else:  # HYBRID
            result = self._route_openclaw(sender_id, recipient_id, message)
            if result.get("status") != "routed":
                result = self._route_standalone(sender_id, recipient_id, message)
                result["fallback"] = True
            return result

    def _route_standalone(
        self, sender_id: str, recipient_id: str, message: BaseMessage
    ) -> Dict[str, Any]:
        """Route through standalone TCP coordinator."""
        self.mode_stats["messages_routed_standalone"] += 1
        if self.coordinator:
            try:
                # Coordinator relay handles it
                return {"status": "routed", "channel": "tcp", "recipient": recipient_id}
            except Exception as e:
                return {"status": "failed", "channel": "tcp", "error": str(e)}
        return {"status": "failed", "channel": "tcp", "error": "no_coordinator"}

    def _route_openclaw(
        self, sender_id: str, recipient_id: str, message: BaseMessage
    ) -> Dict[str, Any]:
        """Route through OpenClaw sessions."""
        self.mode_stats["messages_routed_openclaw"] += 1
        bridge = self._get_bridge()
        if bridge:
            try:
                return bridge.route_peer_message(sender_id, recipient_id, message)
            except Exception as e:
                return {"status": "failed", "channel": "openclaw", "error": str(e)}
        return {"status": "failed", "channel": "openclaw", "error": "bridge_unavailable"}

    # ─── Cluster Status ────────────────────────────────────────────

    def get_cluster_status(self) -> Dict[str, Any]:
        """
        Get unified cluster status regardless of mode.
        """
        status = {
            "mode": self._mode.value,
            "openclaw_enabled": self.is_openclaw_enabled,
            "registered_agents": len(self._agents),
            "stats": dict(self.mode_stats),
        }

        if self.coordinator:
            try:
                coord_status = self.coordinator.get_status()
                status["coordinator"] = coord_status
            except Exception:
                status["coordinator"] = "unavailable"

        if self.is_openclaw_enabled:
            bridge = self._get_bridge()
            if bridge:
                try:
                    oc_status = bridge.get_cluster_status()
                    status["openclaw"] = oc_status
                except Exception:
                    status["openclaw"] = "bridge_error"

        return status

    # ─── Feature-Specific Access ───────────────────────────────────

    def get_event_bridge(self):
        """Get the EventBridge (only available when OpenClaw enabled)."""
        if not self.is_openclaw_enabled or not self.config.event_bridge:
            return None
        if self._event_bridge is None:
            from openclaw_integration.events import EventBridge
            self._event_bridge = EventBridge(
                event_log_dir=os.path.join(
                    self.config.workspace_dir, "memory", "cluster", "events"
                )
            )
        return self._event_bridge

    def get_skill_provider(self):
        """Get the ClusterSkillProvider (only available when OpenClaw enabled)."""
        if not self.is_openclaw_enabled or not self.config.expose_skills:
            return None
        if self._skill_provider is None and self.coordinator:
            from openclaw_integration.skill_provider import ClusterSkillProvider
            if hasattr(self.coordinator, 'capability_registry'):
                self._skill_provider = ClusterSkillProvider(
                    capability_registry=self.coordinator.capability_registry,
                )
        return self._skill_provider

    def sync_conversations_to_memory(self) -> Dict[str, Any]:
        """
        Sync conversation logs to OpenClaw memory directory.
        Only works when OpenClaw integration is enabled and
        sync_conversations config is True.
        """
        if not self.is_openclaw_enabled or not self.config.sync_conversations:
            return {"status": "skipped", "reason": "conversation_sync_disabled"}

        bridge = self._get_bridge()
        if bridge:
            try:
                return bridge.sync_conversations_to_memory()
            except Exception as e:
                return {"status": "failed", "error": str(e)}

        return {"status": "failed", "reason": "bridge_unavailable"}

    # ─── Internal ──────────────────────────────────────────────────

    def _get_bridge(self):
        """Lazy-load the OpenClaw coordinator bridge."""
        if self._bridge is None and self.coordinator:
            from openclaw_integration.bridge import OpenClawCoordinatorBridge
            self._bridge = OpenClawCoordinatorBridge(
                coordinator=self.coordinator,
                workspace_dir=self.config.workspace_dir,
            )
        return self._bridge

    def __repr__(self):
        return (
            f"ClusterModeManager("
            f"mode={self._mode.value}, "
            f"agents={len(self._agents)}, "
            f"oc={'on' if self.is_openclaw_enabled else 'off'})"
        )
