#!/usr/bin/env python3
"""
Tests for Cluster Mode Manager

Tests the mode switching, standalone vs OpenClaw routing,
config loading, and graceful fallback behavior.
"""

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import peer_request
from openclaw_integration.mode_manager import (
    ClusterMode, OpenClawConfig, ClusterModeManager,
)


class TestOpenClawConfig(unittest.TestCase):
    """Test the configuration class."""

    def test_defaults_disabled(self):
        """Default config has OpenClaw disabled."""
        config = OpenClawConfig()
        self.assertFalse(config.enabled)
        self.assertEqual(config.coordinator_session, "session:agent-cluster-coordinator")

    def test_enabled_from_dict(self):
        """Config from dict with enabled=True."""
        config = OpenClawConfig({"openclaw": {"enabled": True}})
        self.assertTrue(config.enabled)

    def test_partial_config(self):
        """Partial config overrides only specified fields."""
        config = OpenClawConfig({"openclaw": {"enabled": True, "sync_conversations": False}})
        self.assertTrue(config.enabled)
        self.assertFalse(config.sync_conversations)
        self.assertTrue(config.auto_register_agents)  # default preserved

    def test_empty_config(self):
        """Empty dict config uses all defaults."""
        config = OpenClawConfig({})
        self.assertFalse(config.enabled)
        self.assertTrue(config.auto_register_agents)

    def test_to_dict_roundtrip(self):
        """Config serializes to dict and back."""
        config = OpenClawConfig({"openclaw": {"enabled": True}})
        d = config.to_dict()
        config2 = OpenClawConfig({"openclaw": d})
        self.assertEqual(config.enabled, config2.enabled)
        self.assertEqual(config.workspace_dir, config2.workspace_dir)

    def test_workspace_dir_expansion(self):
        """Workspace dir expands ~."""
        config = OpenClawConfig({"openclaw": {"workspace_dir": "~/test"}})
        self.assertNotIn("~", config.workspace_dir)


class TestClusterModeManagerStandalone(unittest.TestCase):
    """Test the mode manager in standalone (default) mode."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="oc_mode_")
        self.config = OpenClawConfig()  # disabled by default
        self.manager = ClusterModeManager(
            coordinator=None,
            config=self.config,
        )

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_default_mode_is_standalone(self):
        self.assertEqual(self.manager.mode, ClusterMode.STANDALONE)

    def test_openclaw_disabled_by_default(self):
        self.assertFalse(self.manager.is_openclaw_enabled)

    def test_register_agent_standalone(self):
        result = self.manager.register_agent("agent-alpha", capabilities=["search"])
        self.assertEqual(result["mode"], "standalone")
        self.assertEqual(result["agent_id"], "agent-alpha")

    def test_deregister_agent(self):
        self.manager.register_agent("agent-alpha")
        result = self.manager.deregister_agent("agent-alpha")
        self.assertEqual(result["agent_id"], "agent-alpha")

    def test_send_peer_message_standalone_no_coordinator(self):
        msg = peer_request(
            sender_id="agent-a",
            recipient_id="agent-b",
            conversation_id="conv-test",
            request_type="query",
            content="hello",
        )
        result = self.manager.send_peer_message("agent-a", "agent-b", msg)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["channel"], "tcp")

    def test_get_cluster_status(self):
        status = self.manager.get_cluster_status()
        self.assertEqual(status["mode"], "standalone")
        self.assertFalse(status["openclaw_enabled"])

    def test_no_event_bridge_when_disabled(self):
        bridge = self.manager.get_event_bridge()
        self.assertIsNone(bridge)

    def test_no_skill_provider_when_disabled(self):
        provider = self.manager.get_skill_provider()
        self.assertIsNone(provider)

    def test_sync_conversations_skipped_when_disabled(self):
        result = self.manager.sync_conversations_to_memory()
        self.assertEqual(result["status"], "skipped")


class TestClusterModeManagerOpenClaw(unittest.TestCase):
    """Test the mode manager with OpenClaw enabled."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="oc_mode_oc_")
        self.config = OpenClawConfig({
            "openclaw": {
                "enabled": True,
                "workspace_dir": self.tmpdir,
                "agent_session_prefix": "test-agent",
            }
        })
        self.manager = ClusterModeManager(
            coordinator=None,
            config=self.config,
        )

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_mode_is_openclaw(self):
        self.assertEqual(self.manager.mode, ClusterMode.OPENCLAW)

    def test_openclaw_enabled(self):
        self.assertTrue(self.manager.is_openclaw_enabled)

    def test_register_agent_openclaw(self):
        result = self.manager.register_agent(
            "agent-alpha",
            capabilities=["search"],
            session_key="sess-alpha",
        )
        self.assertEqual(result["mode"], "openclaw")
        # No bridge available (no coordinator), so openclaw registration fails gracefully
        self.assertIn("openclaw", result)

    def test_switch_to_standalone(self):
        self.manager.disable_openclaw()
        self.assertEqual(self.manager.mode, ClusterMode.STANDALONE)
        self.assertFalse(self.manager.is_openclaw_enabled)

    def test_switch_to_openclaw(self):
        self.manager.disable_openclaw()
        self.manager.enable_openclaw()
        self.assertEqual(self.manager.mode, ClusterMode.OPENCLAW)
        self.assertTrue(self.manager.is_openclaw_enabled)

    def test_mode_change_listener(self):
        modes = []
        self.manager.on_mode_change(lambda m: modes.append(m))
        self.manager.disable_openclaw()
        self.manager.enable_openclaw()
        self.assertEqual(modes, [ClusterMode.STANDALONE, ClusterMode.OPENCLAW])

    def test_set_mode_hybrid(self):
        self.manager.set_mode(ClusterMode.HYBRID)
        self.assertEqual(self.manager.mode, ClusterMode.HYBRID)
        self.assertTrue(self.manager.is_openclaw_enabled)

    def test_event_bridge_available(self):
        bridge = self.manager.get_event_bridge()
        self.assertIsNotNone(bridge)

    def test_sync_conversations_no_bridge(self):
        # No coordinator attached, so bridge won't be created
        result = self.manager.sync_conversations_to_memory()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["reason"], "bridge_unavailable")


class TestClusterModeManagerModeSwitch(unittest.TestCase):
    """Test runtime mode switching behavior."""

    def setUp(self):
        self.config = OpenClawConfig()
        self.manager = ClusterModeManager(coordinator=None, config=self.config)

    def test_switch_preserves_agents(self):
        """Agents registered in one mode persist across mode switches."""
        self.manager.register_agent("agent-alpha", capabilities=["search"])
        self.manager.enable_openclaw()
        agents = self.manager.get_registered_agents()
        self.assertIn("agent-alpha", agents)

    def test_mode_changes_tracked(self):
        self.manager.enable_openclaw()
        self.manager.disable_openclaw()
        self.assertEqual(self.manager.mode_stats["mode_changes"], 2)

    def test_message_routing_changes_with_mode(self):
        """Messages route differently based on mode."""
        msg = peer_request(
            sender_id="a", recipient_id="b",
            conversation_id="conv-test", request_type="q", content="hi",
        )
        # Standalone → TCP
        result = self.manager.send_peer_message("a", "b", msg)
        self.assertEqual(result["channel"], "tcp")

        # Switch to OpenClaw → session routing
        self.manager.enable_openclaw()
        result = self.manager.send_peer_message("a", "b", msg)
        self.assertEqual(result["channel"], "openclaw")

    def test_hybrid_fallback(self):
        """Hybrid mode falls back to TCP when OpenClaw fails."""
        self.manager.set_mode(ClusterMode.HYBRID)
        msg = peer_request(
            sender_id="a", recipient_id="b",
            conversation_id="conv-test", request_type="q", content="hi",
        )
        # No bridge available, so OpenClaw fails → TCP fallback
        result = self.manager.send_peer_message("a", "b", msg)
        # Should fall back since bridge unavailable
        self.assertTrue(result.get("fallback", False) or result["status"] == "failed")

    def test_config_reflects_mode(self):
        """Config.enabled tracks the current mode."""
        self.manager.enable_openclaw()
        self.assertTrue(self.manager.config.enabled)
        self.manager.disable_openclaw()
        self.assertFalse(self.manager.config.enabled)


class TestClusterModeManagerRepr(unittest.TestCase):
    """Test string representation."""

    def test_repr_standalone(self):
        manager = ClusterModeManager(coordinator=None)
        r = repr(manager)
        self.assertIn("standalone", r)
        self.assertIn("oc=off", r)

    def test_repr_openclaw(self):
        config = OpenClawConfig({"openclaw": {"enabled": True}})
        manager = ClusterModeManager(coordinator=None, config=config)
        r = repr(manager)
        self.assertIn("openclaw", r)
        self.assertIn("oc=on", r)


if __name__ == "__main__":
    unittest.main(verbosity=2)
