#!/usr/bin/env python3
"""
Tests for OpenClaw Integration Module

Tests the bridge, adapter, events, and skill provider components
that connect the agent cluster to OpenClaw infrastructure.
"""

import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import (
    BaseMessage, MessageType, create_message,
    ota_update_announce, ota_update_package, ota_update_ack,
    ota_update_status, ota_update_rollback,
    peer_request, status_query,
)
from protocol.conversation_log import ConversationLog
from protocol.ota_manager import AgentOTAInstaller
from capabilities.registry import CapabilityRegistry

try:
    from capabilities.discovery import CapabilityDiscovery
except ImportError:
    CapabilityDiscovery = None

from openclaw_integration.bridge import OpenClawCoordinatorBridge
from openclaw_integration.adapter import OpenClawAgentAdapter
from openclaw_integration.events import EventBridge, ClusterEventType, OpenClawEventType
from openclaw_integration.skill_provider import ClusterSkillProvider


class TestOpenClawBridge(unittest.TestCase):
    """Test the coordinator bridge to OpenClaw."""

    def setUp(self):
        from network.coordinator import NetworkCoordinator
        self.tmpdir = tempfile.mkdtemp(prefix="oc_bridge_")
        self.coordinator = NetworkCoordinator(
            port=0,
            cluster_dir=self.tmpdir,
        )
        self.bridge = OpenClawCoordinatorBridge(
            coordinator=self.coordinator,
            workspace_dir=self.tmpdir,
        )

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_register_agent_session(self):
        result = self.bridge.register_agent_session(
            agent_id="agent-alpha",
            session_key="sess-alpha-001",
            capabilities=["web_search", "code_gen"],
        )
        self.assertEqual(result["status"], "registered")
        self.assertIn("agent-alpha", self.bridge.agent_sessions)
        self.assertEqual(self.bridge.agent_sessions["agent-alpha"], "sess-alpha-001")

    def test_deregister_agent_session(self):
        self.bridge.register_agent_session("agent-beta", "sess-beta-001")
        result = self.bridge.deregister_agent_session("agent-beta")
        self.assertEqual(result["status"], "deregistered")
        self.assertNotIn("agent-beta", self.bridge.agent_sessions)

    def test_route_peer_message(self):
        self.bridge.register_agent_session("agent-alpha", "sess-alpha")
        self.bridge.register_agent_session("agent-beta", "sess-beta")
        msg = peer_request(
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            conversation_id="conv-test",
            request_type="data_query",
            content="What's the weather?",
        )
        result = self.bridge.route_peer_message("agent-alpha", "agent-beta", msg)
        self.assertEqual(result["status"], "routed")
        self.assertEqual(result["target_session"], "sess-beta")

    def test_route_to_unknown_agent_fails(self):
        msg = peer_request(
            sender_id="agent-alpha",
            recipient_id="agent-unknown",
            conversation_id="conv-test",
            request_type="query",
            content="Hello?",
        )
        result = self.bridge.route_peer_message("agent-alpha", "agent-unknown", msg)
        self.assertEqual(result["status"], "failed")

    def test_cluster_status(self):
        self.bridge.register_agent_session("agent-alpha", "sess-alpha")
        status = self.bridge.get_cluster_status()
        self.assertIn("cluster_health", status)
        self.assertIn("active_agents", status)
        self.assertEqual(status["active_agents"], 1)

    def test_schedule_ota_rollout(self):
        self.bridge.register_agent_session("agent-alpha", "sess-alpha")
        # Create a dummy package
        import tarfile, io
        pkg_path = os.path.join(self.tmpdir, "update.tar.gz")
        with tarfile.open(pkg_path, "w:gz") as tar:
            info = tarfile.TarInfo(name="VERSION")
            info.size = len(b"2.0.0")
            tar.addfile(info, io.BytesIO(b"2.0.0"))

        result = self.bridge.schedule_ota_rollout(
            version="2.0.0",
            package_path=pkg_path,
            priority="high",
        )
        self.assertEqual(result["status"], "scheduled")
        self.assertEqual(result["agents_targeted"], 1)

    def test_agent_registration_hook(self):
        events = []
        self.bridge.on_agent_registered(lambda aid, sid, caps: events.append(aid))
        self.bridge.register_agent_session("agent-gamma", "sess-gamma")
        self.assertEqual(events, ["agent-gamma"])

    def test_agent_lost_hook(self):
        events = []
        self.bridge.register_agent_session("agent-delta", "sess-delta")
        self.bridge.on_agent_lost(lambda aid: events.append(aid))
        self.bridge.deregister_agent_session("agent-delta")
        self.assertEqual(events, ["agent-delta"])

    def test_agent_state_persisted(self):
        self.bridge.register_agent_session("agent-epsilon", "sess-epsilon", ["cap1"])
        state_path = os.path.join(
            self.bridge.cluster_memory_dir, "agent-agent-epsilon.json"
        )
        self.assertTrue(os.path.exists(state_path))
        with open(state_path) as f:
            state = json.load(f)
        self.assertEqual(state["agent_id"], "agent-epsilon")
        self.assertEqual(state["capabilities"], ["cap1"])


class TestOpenClawAgentAdapter(unittest.TestCase):
    """Test the agent adapter."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="oc_adapter_")
        os.makedirs(os.path.join(self.tmpdir, "install"), exist_ok=True)
        with open(os.path.join(self.tmpdir, "install", "VERSION"), "w") as f:
            f.write("1.0.0")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_register(self):
        adapter = OpenClawAgentAdapter(
            agent_id="agent-alpha",
            session_key="sess-alpha",
            capabilities=["search", "translate"],
            install_dir=os.path.join(self.tmpdir, "install"),
            current_version="1.0.0",
        )
        result = adapter.register()
        self.assertEqual(result["status"], "registered")
        self.assertEqual(result["capabilities"], ["search", "translate"])

    def test_deregister(self):
        adapter = OpenClawAgentAdapter(
            agent_id="agent-beta",
            session_key="sess-beta",
            install_dir=os.path.join(self.tmpdir, "install"),
        )
        adapter.register()
        result = adapter.deregister()
        self.assertEqual(result["status"], "deregistered")

    def test_handle_ota_announce_auto_accept(self):
        """OTA announce is auto-accepted — no consent."""
        adapter = OpenClawAgentAdapter(
            agent_id="agent-alpha",
            session_key="sess-alpha",
            install_dir=os.path.join(self.tmpdir, "install"),
            current_version="1.0.0",
        )
        msg = json.dumps({
            "type": "ota_update_announce",
            "message_id": "msg-001",
            "sender_id": "coordinator",
            "payload": {"version": "1.5.0", "priority": "high"},
        })
        result = adapter.handle_message(msg)
        self.assertEqual(result["status"], "acknowledged")
        self.assertTrue(result["ready"])  # Auto-accept

    def test_handle_ota_package_auto_install(self):
        """OTA package is auto-installed — no consent."""
        import tarfile, io, base64, hashlib
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            content = b"1.5.0"
            info = tarfile.TarInfo(name="VERSION")
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
        raw = buf.getvalue()

        adapter = OpenClawAgentAdapter(
            agent_id="agent-alpha",
            session_key="sess-alpha",
            install_dir=os.path.join(self.tmpdir, "install"),
            current_version="1.0.0",
        )
        adapter.register()

        msg = json.dumps({
            "type": "ota_update_package",
            "sender_id": "coordinator",
            "payload": {
                "version": "1.5.0",
                "package_data": base64.b64encode(raw).decode(),
                "package_type": "tar.gz",
                "checksum": hashlib.sha256(raw).hexdigest(),
                "install_script": "",
                "pre_install": "",
                "post_install": "",
                "rollback_script": "",
            },
        })
        result = adapter.handle_message(msg)
        self.assertEqual(result["status"], "success")
        self.assertEqual(adapter.current_version, "1.5.0")

    def test_handle_status_query(self):
        adapter = OpenClawAgentAdapter(
            agent_id="agent-alpha",
            session_key="sess-alpha",
            capabilities=["search"],
            install_dir=os.path.join(self.tmpdir, "install"),
        )
        adapter.register()
        msg = json.dumps({
            "type": "status_query",
            "sender_id": "coordinator",
            "payload": {},
        })
        result = adapter.handle_message(msg)
        self.assertEqual(result["agent_id"], "agent-alpha")
        self.assertIn("search", result["capabilities"])

    def test_handle_invalid_json(self):
        adapter = OpenClawAgentAdapter(
            agent_id="agent-alpha",
            session_key="sess-alpha",
            install_dir=os.path.join(self.tmpdir, "install"),
        )
        result = adapter.handle_message("not json {{{")
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["reason"], "invalid_json")

    def test_handle_unknown_type(self):
        adapter = OpenClawAgentAdapter(
            agent_id="agent-alpha",
            session_key="sess-alpha",
            install_dir=os.path.join(self.tmpdir, "install"),
        )
        msg = json.dumps({"type": "custom_unknown_type", "payload": {}})
        result = adapter.handle_message(msg)
        self.assertEqual(result["status"], "unhandled")

    def test_custom_handler(self):
        adapter = OpenClawAgentAdapter(
            agent_id="agent-alpha",
            session_key="sess-alpha",
            install_dir=os.path.join(self.tmpdir, "install"),
        )
        adapter.register_custom_handler(
            "custom_task", lambda data: {"status": "custom_ok", "data": data}
        )
        msg = json.dumps({"type": "custom_task", "payload": {"key": "val"}})
        result = adapter.handle_message(msg)
        self.assertEqual(result["status"], "custom_ok")

    def test_add_remove_capability(self):
        adapter = OpenClawAgentAdapter(
            agent_id="agent-alpha",
            session_key="sess-alpha",
            capabilities=["search"],
            install_dir=os.path.join(self.tmpdir, "install"),
        )
        adapter.add_capability("translate")
        self.assertIn("translate", adapter.capabilities)
        adapter.remove_capability("search")
        self.assertNotIn("search", adapter.capabilities)

    def test_heartbeat_start_stop(self):
        adapter = OpenClawAgentAdapter(
            agent_id="agent-alpha",
            session_key="sess-alpha",
            install_dir=os.path.join(self.tmpdir, "install"),
        )
        adapter.start_heartbeat(interval_seconds=1)
        self.assertTrue(adapter._running)
        adapter.stop_heartbeat()
        self.assertFalse(adapter._running)

    def test_rollback_auto_accepted(self):
        """OTA rollback is automatic — no consent needed."""
        import tarfile, io, base64, hashlib
        install_dir = os.path.join(self.tmpdir, "install")
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            content = b"1.5.0"
            info = tarfile.TarInfo(name="VERSION")
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
        raw = buf.getvalue()

        adapter = OpenClawAgentAdapter(
            agent_id="agent-alpha",
            session_key="sess-alpha",
            install_dir=install_dir,
            current_version="1.0.0",
        )
        adapter.register()

        # Install 1.5.0 first
        pkg_msg = json.dumps({
            "type": "ota_update_package",
            "sender_id": "coordinator",
            "payload": {
                "version": "1.5.0",
                "package_data": base64.b64encode(raw).decode(),
                "package_type": "tar.gz",
                "checksum": hashlib.sha256(raw).hexdigest(),
                "install_script": "", "pre_install": "", "post_install": "", "rollback_script": "",
            },
        })
        adapter.handle_message(pkg_msg)
        self.assertEqual(adapter.current_version, "1.5.0")

        # Rollback
        rb_msg = json.dumps({
            "type": "ota_update_rollback",
            "sender_id": "coordinator",
            "payload": {"version": "1.5.0", "target_version": "1.0.0"},
        })
        result = adapter.handle_message(rb_msg)
        self.assertEqual(result["status"], "rolled_back")
        self.assertEqual(adapter.current_version, "1.0.0")


class TestEventBridge(unittest.TestCase):
    """Test the event translation bridge."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="oc_events_")
        self.bridge = EventBridge(event_log_dir=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_translate_agent_registered(self):
        result = self.bridge.translate_cluster_event(
            ClusterEventType.AGENT_REGISTERED,
            {"agent_id": "agent-alpha", "capabilities": ["search"]},
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["event_type"], "openclaw.session.created")

    def test_translate_ota_announced(self):
        result = self.bridge.translate_cluster_event(
            ClusterEventType.OTA_ANNOUNCED,
            {"version": "2.0.0", "changelog": "Big update", "priority": "high"},
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["event_type"], "openclaw.notification.send")
        self.assertIn("title", result["payload"])

    def test_translate_ota_failed_as_alert(self):
        result = self.bridge.translate_cluster_event(
            ClusterEventType.OTA_FAILED,
            {"version": "2.0.0", "message": "Checksum failed", "agent_id": "a1"},
        )
        self.assertEqual(result["event_type"], "openclaw.alert.triggered")
        self.assertEqual(result["payload"]["severity"], "high")

    def test_translate_error_as_alert(self):
        result = self.bridge.translate_cluster_event(
            ClusterEventType.ERROR_REPORTED,
            {"error_type": "timeout", "description": "Agent timed out", "severity": "critical"},
        )
        self.assertEqual(result["event_type"], "openclaw.alert.triggered")
        self.assertEqual(result["payload"]["severity"], "critical")

    def test_unmapped_cluster_event(self):
        result = self.bridge.translate_cluster_event(
            ClusterEventType.AGENT_HEARTBEAT,
            {"agent_id": "agent-alpha"},
        )
        self.assertIsNone(result)  # No OpenClaw mapping for heartbeat

    def test_reverse_translate_session_created(self):
        result = self.bridge.translate_openclaw_event(
            OpenClawEventType.SESSION_CREATED,
            {"session_key": "sess-001"},
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["event_type"], "cluster.agent.registered")

    def test_event_history(self):
        self.bridge.translate_cluster_event(
            ClusterEventType.AGENT_REGISTERED, {"agent_id": "a1"}
        )
        self.bridge.translate_cluster_event(
            ClusterEventType.ERROR_REPORTED, {"error_type": "test"}
        )
        history = self.bridge.get_event_history()
        # Two events: one translated (AGENT_REGISTERED), one translated (ERROR_REPORTED)
        # Plus their unmapped cluster source records
        self.assertGreaterEqual(len(history), 2)

    def test_cluster_event_listener(self):
        events = []
        self.bridge.on_cluster_event(
            ClusterEventType.AGENT_REGISTERED,
            lambda p: events.append(p),
        )
        self.bridge.translate_cluster_event(
            ClusterEventType.AGENT_REGISTERED, {"agent_id": "a1"}
        )
        self.assertEqual(len(events), 1)

    def test_openclaw_event_listener(self):
        events = []
        self.bridge.on_openclaw_event(
            OpenClawEventType.ALERT_TRIGGERED,
            lambda p: events.append(p),
        )
        self.bridge.translate_cluster_event(
            ClusterEventType.ERROR_REPORTED, {"error_type": "test", "severity": "high"}
        )
        self.assertEqual(len(events), 1)

    def test_health_changed_payload(self):
        result = self.bridge.translate_cluster_event(
            ClusterEventType.HEALTH_CHANGED,
            {"health_score": 85, "previous_score": 92},
        )
        self.assertEqual(result["payload"]["change"], -7)

    def test_stats(self):
        self.bridge.translate_cluster_event(
            ClusterEventType.AGENT_REGISTERED, {}
        )
        stats = self.bridge.get_stats()
        self.assertGreater(stats["events_translated"], 0)


class TestClusterSkillProvider(unittest.TestCase):
    """Test the skill provider."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="oc_skills_")
        self.registry = CapabilityRegistry()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_generate_skills_empty(self):
        provider = ClusterSkillProvider(
            capability_registry=self.registry,
            skill_output_dir=os.path.join(self.tmpdir, "skills"),
        )
        skills = provider.generate_skills()
        self.assertEqual(len(skills), 0)

    def test_generate_skills_with_capabilities(self):
        self.registry.define_capability("web_search", "Search the web", category="information")
        self.registry.register_capability("agent-alpha", "web_search", confidence=0.95)
        provider = ClusterSkillProvider(
            capability_registry=self.registry,
            skill_output_dir=os.path.join(self.tmpdir, "skills"),
        )
        skills = provider.generate_skills()
        self.assertEqual(len(skills), 1)
        self.assertEqual(skills[0]["name"], "cluster.web_search")

    def test_invoke_skill(self):
        self.registry.define_capability("translate", "Translate text", category="language")
        self.registry.register_capability("agent-beta", "translate", confidence=0.9)
        provider = ClusterSkillProvider(
            capability_registry=self.registry,
            skill_output_dir=os.path.join(self.tmpdir, "skills"),
        )
        provider.generate_skills()
        result = provider.invoke_skill("cluster.translate", {"text": "hello", "target": "es"})
        self.assertEqual(result["status"], "routed")
        self.assertEqual(result["agent_id"], "agent-beta")

    def test_invoke_unknown_skill(self):
        provider = ClusterSkillProvider(
            capability_registry=self.registry,
            skill_output_dir=os.path.join(self.tmpdir, "skills"),
        )
        result = provider.invoke_skill("cluster.nonexistent")
        self.assertEqual(result["status"], "failed")

    def test_fleet_skills_summary(self):
        self.registry.define_capability("search", "Search", category="info")
        self.registry.register_capability("a1", "search", confidence=0.9)
        self.registry.register_capability("a2", "search", confidence=0.7)
        provider = ClusterSkillProvider(
            capability_registry=self.registry,
            skill_output_dir=os.path.join(self.tmpdir, "skills"),
        )
        summary = provider.get_fleet_skills_summary()
        self.assertEqual(summary["total_skills"], 1)
        self.assertEqual(summary["agents_with_skills"], 2)

    def test_skill_invocation_hook(self):
        self.registry.define_capability("compute", "Compute", category="math")
        self.registry.register_capability("a1", "compute", confidence=1.0)
        provider = ClusterSkillProvider(
            capability_registry=self.registry,
            skill_output_dir=os.path.join(self.tmpdir, "skills"),
        )
        provider.generate_skills()
        invocations = []
        provider.on_skill_invoked(lambda r: invocations.append(r))
        provider.invoke_skill("cluster.compute")
        self.assertEqual(len(invocations), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
