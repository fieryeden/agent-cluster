#!/usr/bin/env python3
"""
Event Bridge

Translates agent cluster events to OpenClaw system events and vice versa.
Enables the cluster to participate in OpenClaw's event-driven architecture.

Event types mapped:
- Agent registration/deregistration → OpenClaw session events
- Conversation events → OpenClaw memory events
- OTA status changes → OpenClaw notification events
- Capability changes → OpenClaw skill events
- Error events → OpenClaw alert events
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path
from enum import Enum

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


class ClusterEventType(str, Enum):
    """Cluster-side event types."""
    AGENT_REGISTERED = "cluster.agent.registered"
    AGENT_DEREGISTERED = "cluster.agent.deregistered"
    AGENT_HEARTBEAT = "cluster.agent.heartbeat"
    AGENT_LOST = "cluster.agent.lost"
    CONVERSATION_STARTED = "cluster.conversation.started"
    CONVERSATION_CLOSED = "cluster.conversation.closed"
    CONVERSATION_MESSAGE = "cluster.conversation.message"
    CAPABILITY_ADDED = "cluster.capability.added"
    CAPABILITY_REMOVED = "cluster.capability.removed"
    OTA_ANNOUNCED = "cluster.ota.announced"
    OTA_INSTALLING = "cluster.ota.installing"
    OTA_SUCCESS = "cluster.ota.success"
    OTA_FAILED = "cluster.ota.failed"
    OTA_ROLLED_BACK = "cluster.ota.rolled_back"
    ERROR_REPORTED = "cluster.error.reported"
    TASK_DELEGATED = "cluster.task.delegated"
    TASK_COMPLETED = "cluster.task.completed"
    TASK_FAILED = "cluster.task.failed"
    CONSENSUS_STARTED = "cluster.consensus.started"
    CONSENSUS_COMPLETED = "cluster.consensus.completed"
    FILE_TRANSFERRED = "cluster.file.transferred"
    HEALTH_CHANGED = "cluster.health.changed"


class OpenClawEventType(str, Enum):
    """OpenClaw-side event types."""
    SESSION_CREATED = "openclaw.session.created"
    SESSION_CLOSED = "openclaw.session.closed"
    MEMORY_UPDATE = "openclaw.memory.update"
    NOTIFICATION_SEND = "openclaw.notification.send"
    SKILL_REGISTERED = "openclaw.skill.registered"
    ALERT_TRIGGERED = "openclaw.alert.triggered"
    CRON_SCHEDULED = "openclaw.cron.scheduled"
    STATUS_CHANGE = "openclaw.status.change"


class EventBridge:
    """
    Bidirectional event translator between cluster and OpenClaw.

    Maps cluster events to OpenClaw events and vice versa,
    enabling seamless integration of the two systems.
    """

    # Cluster → OpenClaw mapping
    CLUSTER_TO_OPENCLAW = {
        ClusterEventType.AGENT_REGISTERED: OpenClawEventType.SESSION_CREATED,
        ClusterEventType.AGENT_DEREGISTERED: OpenClawEventType.SESSION_CLOSED,
        ClusterEventType.CONVERSATION_MESSAGE: OpenClawEventType.MEMORY_UPDATE,
        ClusterEventType.OTA_ANNOUNCED: OpenClawEventType.NOTIFICATION_SEND,
        ClusterEventType.OTA_FAILED: OpenClawEventType.ALERT_TRIGGERED,
        ClusterEventType.ERROR_REPORTED: OpenClawEventType.ALERT_TRIGGERED,
        ClusterEventType.HEALTH_CHANGED: OpenClawEventType.STATUS_CHANGE,
        ClusterEventType.CAPABILITY_ADDED: OpenClawEventType.SKILL_REGISTERED,
        ClusterEventType.CONSENSUS_COMPLETED: OpenClawEventType.NOTIFICATION_SEND,
    }

    def __init__(self, event_log_dir: str = None):
        """
        Initialize the event bridge.

        Args:
            event_log_dir: Directory for event log files
        """
        self.event_log_dir = event_log_dir or "/tmp/cluster_events"
        os.makedirs(self.event_log_dir, exist_ok=True)

        # Event listeners
        self._cluster_listeners: Dict[ClusterEventType, List[Callable]] = {}
        self._openclaw_listeners: Dict[OpenClawEventType, List[Callable]] = {}

        # Event history (recent events for replay)
        self._event_history: List[Dict] = []
        self._max_history = 1000

        # Stats
        self.bridge_stats = {
            "events_translated": 0,
            "cluster_events": 0,
            "openclaw_events": 0,
            "listeners_called": 0,
            "errors": 0,
        }

    def translate_cluster_event(
        self, cluster_event: ClusterEventType, payload: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Translate a cluster event to OpenClaw format.

        Args:
            cluster_event: The cluster event type
            payload: Event payload data

        Returns:
            Translated OpenClaw event dict, or None if no mapping
        """
        self.bridge_stats["cluster_events"] += 1

        oc_event_type = self.CLUSTER_TO_OPENCLAW.get(cluster_event)
        if not oc_event_type:
            # No mapping — log but don't translate
            self._record_event(cluster_event.value, "cluster", payload)
            return None

        oc_event = {
            "event_type": oc_event_type.value,
            "source": "agent-cluster",
            "original_type": cluster_event.value,
            "payload": self._transform_payload(cluster_event, payload),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_id": f"evt-{id(self)}-{len(self._event_history)}",
        }

        self.bridge_stats["events_translated"] += 1
        self._record_event(oc_event_type.value, "openclaw", oc_event)

        # Notify OpenClaw listeners
        for listener in self._openclaw_listeners.get(oc_event_type, []):
            try:
                listener(oc_event)
                self.bridge_stats["listeners_called"] += 1
            except Exception:
                self.bridge_stats["errors"] += 1

        # Notify cluster listeners too
        for listener in self._cluster_listeners.get(cluster_event, []):
            try:
                listener(payload)
                self.bridge_stats["listeners_called"] += 1
            except Exception:
                self.bridge_stats["errors"] += 1

        return oc_event

    def translate_openclaw_event(
        self, oc_event_type: OpenClawEventType, payload: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Translate an OpenClaw event to cluster format.

        Reverse mapping: find cluster event(s) that map to this OpenClaw event.

        Args:
            oc_event_type: The OpenClaw event type
            payload: Event payload data

        Returns:
            Translated cluster event dict, or None if no mapping
        """
        self.bridge_stats["openclaw_events"] += 1

        # Reverse lookup
        cluster_events = [
            ce for ce, oe in self.CLUSTER_TO_OPENCLAW.items()
            if oe == oc_event_type
        ]

        if not cluster_events:
            self._record_event(oc_event_type.value, "openclaw", payload)
            return None

        # Use first mapping
        cluster_event = cluster_events[0]
        result = {
            "event_type": cluster_event.value,
            "source": "openclaw",
            "original_type": oc_event_type.value,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        self.bridge_stats["events_translated"] += 1
        self._record_event(cluster_event.value, "cluster", result)

        # Notify cluster listeners
        for listener in self._cluster_listeners.get(cluster_event, []):
            try:
                listener(payload)
                self.bridge_stats["listeners_called"] += 1
            except Exception:
                self.bridge_stats["errors"] += 1

        return result

    def on_cluster_event(
        self, event_type: ClusterEventType, listener: Callable
    ):
        """Register a listener for a cluster event."""
        if event_type not in self._cluster_listeners:
            self._cluster_listeners[event_type] = []
        self._cluster_listeners[event_type].append(listener)

    def on_openclaw_event(
        self, event_type: OpenClawEventType, listener: Callable
    ):
        """Register a listener for an OpenClaw event."""
        if event_type not in self._openclaw_listeners:
            self._openclaw_listeners[event_type] = []
        self._openclaw_listeners[event_type].append(listener)

    def get_event_history(
        self, source: str = None, event_type: str = None, limit: int = 100
    ) -> List[Dict]:
        """
        Get recent event history.

        Args:
            source: Filter by source ("cluster" or "openclaw")
            event_type: Filter by event type string
            limit: Maximum events to return

        Returns:
            List of event dicts
        """
        events = self._event_history
        if source:
            events = [e for e in events if e.get("source") == source]
        if event_type:
            events = [e for e in events if e.get("event_type") == event_type]
        return events[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Get bridge statistics."""
        return dict(self.bridge_stats)

    # ─── Internal ──────────────────────────────────────────────────

    def _transform_payload(
        self, cluster_event: ClusterEventType, payload: Dict
    ) -> Dict:
        """Transform cluster payload to OpenClaw-friendly format."""
        if cluster_event == ClusterEventType.OTA_ANNOUNCED:
            return {
                "title": f"OTA Update Available: v{payload.get('version', '?')}",
                "body": payload.get("changelog", ""),
                "priority": payload.get("priority", "normal"),
                "target_agents": payload.get("target_agents", []),
            }
        elif cluster_event == ClusterEventType.OTA_FAILED:
            return {
                "severity": "high",
                "title": f"OTA Update Failed: v{payload.get('version', '?')}",
                "body": payload.get("message", "Unknown error"),
                "agent_id": payload.get("agent_id"),
            }
        elif cluster_event == ClusterEventType.ERROR_REPORTED:
            return {
                "severity": payload.get("severity", "medium"),
                "title": f"Cluster Error: {payload.get('error_type', 'unknown')}",
                "body": payload.get("description", ""),
                "agent_id": payload.get("agent_id"),
            }
        elif cluster_event == ClusterEventType.HEALTH_CHANGED:
            return {
                "health_score": payload.get("health_score", 0),
                "previous_score": payload.get("previous_score", 0),
                "change": payload.get("health_score", 0) - payload.get("previous_score", 0),
            }
        return payload

    def _record_event(self, event_type: str, source: str, data: Dict):
        """Record an event in history and optionally to disk."""
        record = {
            "event_type": event_type,
            "source": source,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }
        self._event_history.append(record)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

    def __repr__(self):
        return (
            f"EventBridge("
            f"translated={self.bridge_stats['events_translated']}, "
            f"history={len(self._event_history)})"
        )
