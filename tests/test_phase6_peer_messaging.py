#!/usr/bin/env python3
"""
Tests for Agent-to-Agent Communication (Phase 6)

Tests conversation logging, peer message relay, conversation management,
search, export, and integration with the network coordinator.
"""

import hashlib
import json
import os
import sys
import tempfile
import time
import threading
import unittest
from pathlib import Path
from datetime import datetime

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import (
    BaseMessage, MessageType, create_message,
    peer_request, peer_response, peer_notify,
    peer_broadcast, conversation_start, conversation_end,
    file_send, file_request, file_send_response,
    task_delegate, task_delegate_response,
    status_query, status_response,
    capability_share,
    consensus_request, consensus_vote,
    heartbeat_peer, context_share, error_report,
    ota_update_announce, ota_update_package, ota_update_ack,
    ota_update_status, ota_update_rollback,
)
from protocol.conversation_log import ConversationLog, ConversationEntry


class TestConversationLog(unittest.TestCase):
    """Test the ConversationLog module."""

    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.log = ConversationLog(self.db_path)

    def tearDown(self):
        self.log.close()
        os.close(self.db_fd)
        os.unlink(self.db_path)

    def test_log_single_message(self):
        """Log a single message and verify it's stored."""
        entry_id = self.log.log_message(
            conversation_id="conv-001",
            message_id="msg-001",
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            msg_type="peer_request",
            content="Can you verify the compliance output?",
        )
        self.assertGreater(entry_id, 0)

    def test_log_conversation_thread(self):
        """Log a multi-message conversation and retrieve it."""
        self.log.log_message(
            conversation_id="conv-002",
            message_id="msg-010",
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            msg_type="peer_request",
            content="Hey beta, verify this output?",
        )
        self.log.log_message(
            conversation_id="conv-002",
            message_id="msg-011",
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            msg_type="peer_response",
            content="Sure, checking now.",
            delivery_status="delivered",
        )
        self.log.log_message(
            conversation_id="conv-002",
            message_id="msg-012",
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            msg_type="peer_response",
            content="Verified — 3 findings confirmed.",
            delivery_status="delivered",
        )

        entries = self.log.get_conversation("conv-002")
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0].sender_id, "agent-alpha")
        self.assertEqual(entries[1].sender_id, "agent-beta")
        self.assertEqual(entries[2].content, "Verified — 3 findings confirmed.")

    def test_duplicate_message_id_ignored(self):
        """Duplicate message_id should be silently ignored."""
        self.log.log_message(
            conversation_id="conv-003",
            message_id="msg-dup",
            sender_id="a",
            recipient_id="b",
            msg_type="peer_request",
            content="first",
        )
        self.log.log_message(
            conversation_id="conv-003",
            message_id="msg-dup",
            sender_id="a",
            recipient_id="b",
            msg_type="peer_request",
            content="second",
        )
        entries = self.log.get_conversation("conv-003")
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].content, "first")

    def test_delivery_status_update(self):
        """Update delivery status of a message."""
        self.log.log_message(
            conversation_id="conv-004",
            message_id="msg-status",
            sender_id="a",
            recipient_id="b",
            msg_type="peer_request",
            content="test",
            delivery_status="pending",
        )
        self.log.update_delivery_status("msg-status", "delivered")

        entries = self.log.get_conversation("conv-004")
        self.assertEqual(entries[0].delivery_status, "delivered")

    def test_get_agent_conversations(self):
        """Get all conversations involving a specific agent."""
        # Agent alpha in 2 conversations
        self.log.log_message("conv-a1", "m-a1", "alpha", "beta", "peer_request", "msg1")
        self.log.log_message("conv-a2", "m-a2", "gamma", "alpha", "peer_request", "msg2")

        convs = self.log.get_agent_conversations("alpha")
        self.assertEqual(len(convs), 2)

    def test_get_messages_between(self):
        """Get all messages between two specific agents."""
        self.log.log_message("conv-ab", "m-ab1", "alpha", "beta", "peer_request", "msg1")
        self.log.log_message("conv-ab", "m-ab2", "beta", "alpha", "peer_response", "msg2")
        self.log.log_message("conv-ac", "m-ac1", "alpha", "gamma", "peer_request", "other")

        messages = self.log.get_messages_between("alpha", "beta")
        self.assertEqual(len(messages), 2)
        # Should NOT include the alpha→gamma message

    def test_full_text_search(self):
        """Search across conversation content."""
        self.log.log_message("conv-s1", "m-s1", "a", "b", "peer_request", "compliance check needed")
        self.log.log_message("conv-s2", "m-s2", "c", "d", "peer_request", "data enrichment pipeline")

        results = self.log.search_conversations("compliance")
        self.assertEqual(len(results), 1)
        self.assertIn("compliance", results[0].content)

    def test_conversation_topic(self):
        """Set and retrieve conversation topic."""
        self.log.log_message("conv-topic", "m-t1", "a", "b", "peer_request", "hello")
        self.log.set_conversation_topic("conv-topic", "Compliance Verification")

        convs = self.log.get_all_conversations()
        topic_conv = [c for c in convs if c["conversation_id"] == "conv-topic"]
        self.assertEqual(len(topic_conv), 1)
        self.assertEqual(topic_conv[0]["topic"], "Compliance Verification")

    def test_close_conversation(self):
        """Close a conversation and verify status."""
        self.log.log_message("conv-close", "m-c1", "a", "b", "peer_request", "hi")
        self.log.close_conversation("conv-close")

        convs = self.log.get_all_conversations(status="closed")
        closed_ids = [c["conversation_id"] for c in convs]
        self.assertIn("conv-close", closed_ids)

    def test_get_stats(self):
        """Get conversation statistics."""
        self.log.log_message("conv-stats1", "m-st1", "alpha", "beta", "peer_request", "hello")
        self.log.log_message("conv-stats1", "m-st2", "beta", "alpha", "peer_response", "hi")
        self.log.log_message("conv-stats2", "m-st3", "gamma", "delta", "peer_notify", "fyi")

        stats = self.log.get_stats()
        self.assertEqual(stats["total_messages"], 3)
        self.assertEqual(stats["total_conversations"], 2)

    def test_export_json(self):
        """Export a conversation as JSON."""
        self.log.log_message("conv-export", "m-ex1", "a", "b", "peer_request", "test content")
        exported = self.log.export_conversation("conv-export", format="json")
        data = json.loads(exported)
        self.assertEqual(data["conversation_id"], "conv-export")
        self.assertEqual(len(data["messages"]), 1)

    def test_export_markdown(self):
        """Export a conversation as Markdown."""
        self.log.log_message("conv-md", "m-md1", "alpha", "beta", "peer_request", "check this")
        md = self.log.export_conversation("conv-md", format="markdown")
        self.assertIn("alpha", md)
        self.assertIn("beta", md)
        self.assertIn("check this", md)

    def test_export_all(self):
        """Export all conversations."""
        self.log.log_message("conv-all1", "m-a1", "a", "b", "peer_request", "msg1")
        self.log.log_message("conv-all2", "m-a2", "c", "d", "peer_notify", "msg2")
        exported = self.log.export_all(format="json")
        data = json.loads(exported)
        self.assertIn("conv-all1", data)
        self.assertIn("conv-all2", data)

    def test_prune_old_conversations(self):
        """Prune old conversations (smoke test — no data should be pruned since all are new)."""
        self.log.log_message("conv-prune", "m-p1", "a", "b", "peer_request", "fresh")
        # Nothing should be pruned (all entries are from today)
        self.log.prune(older_than_days=30)
        entries = self.log.get_conversation("conv-prune")
        self.assertEqual(len(entries), 1)

    def test_concurrent_writes(self):
        """Multiple threads writing simultaneously."""
        errors = []
        barrier = threading.Barrier(4)

        def writer(thread_id):
            try:
                barrier.wait(timeout=5)
                for i in range(10):
                    self.log.log_message(
                        f"conv-thread-{thread_id}",
                        f"msg-t{thread_id}-{i}",
                        f"agent-{thread_id}",
                        f"agent-{(thread_id + 1) % 4}",
                        "peer_request",
                        f"thread {thread_id} msg {i}",
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(len(errors), 0, f"Concurrent write errors: {errors}")
        stats = self.log.get_stats()
        self.assertEqual(stats["total_messages"], 40)


class TestPeerMessageTypes(unittest.TestCase):
    """Test new peer message types in the protocol."""

    def test_peer_request_creation(self):
        """Create a peer_request message."""
        msg = peer_request(
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            conversation_id="conv-test",
            content="Can you verify this?",
            request_type="verification",
        )
        self.assertEqual(msg.msg_type, MessageType.PEER_REQUEST)
        self.assertEqual(msg.sender_id, "agent-alpha")
        self.assertEqual(msg.recipient_id, "agent-beta")
        self.assertEqual(msg.payload["conversation_id"], "conv-test")
        self.assertEqual(msg.payload["content"], "Can you verify this?")

    def test_peer_response_creation(self):
        """Create a peer_response message."""
        msg = peer_response(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            conversation_id="conv-test",
            content="Verified — looks good!",
            in_reply_to="msg-001",
        )
        self.assertEqual(msg.msg_type, MessageType.PEER_RESPONSE)
        self.assertEqual(msg.payload["in_reply_to"], "msg-001")

    def test_peer_notify_creation(self):
        """Create a peer_notify message."""
        msg = peer_notify(
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            conversation_id="conv-notify",
            content="Task completed, results incoming.",
            notification_type="info",
        )
        self.assertEqual(msg.msg_type, MessageType.PEER_NOTIFY)
        self.assertEqual(msg.payload["notification_type"], "info")

    def test_peer_broadcast_creation(self):
        """Create a peer_broadcast message."""
        msg = peer_broadcast(
            sender_id="agent-alpha",
            content="New capability available: sentiment_analysis",
            topic="capability_update",
            exclude=["agent-gamma"],
        )
        self.assertEqual(msg.msg_type, MessageType.PEER_BROADCAST)
        self.assertEqual(msg.recipient_id, "broadcast")
        self.assertEqual(msg.payload["exclude"], ["agent-gamma"])

    def test_conversation_start_creation(self):
        """Create a conversation_start message."""
        msg = conversation_start(
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            topic="Cross-verify compliance module",
            initial_message="Hey, I need help verifying the compliance checker output.",
        )
        self.assertEqual(msg.msg_type, MessageType.CONVERSATION_START)
        self.assertEqual(msg.payload["topic"], "Cross-verify compliance module")

    def test_conversation_end_creation(self):
        """Create a conversation_end message."""
        msg = conversation_end(
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            conversation_id="conv-123",
            summary="Verification complete, 2 issues found.",
        )
        self.assertEqual(msg.msg_type, MessageType.CONVERSATION_END)
        self.assertEqual(msg.payload["conversation_id"], "conv-123")

    def test_message_roundtrip_json(self):
        """Serialize and deserialize a peer message through JSON."""
        original = peer_request(
            sender_id="a",
            recipient_id="b",
            conversation_id="conv-rt",
            content="test roundtrip",
        )
        json_str = original.to_json()
        restored = BaseMessage.from_json(json_str)
        self.assertEqual(restored.msg_type, MessageType.PEER_REQUEST)
        self.assertEqual(restored.sender_id, "a")
        self.assertEqual(restored.payload["content"], "test roundtrip")


class TestFileTransfer(unittest.TestCase):
    """Test file_send and file_request message types and constructors."""

    def test_file_send_creation(self):
        """Create a file_send message with base64 data."""
        import base64
        file_content = b"id,name,score\n1,alice,95\n2,bob,87"
        encoded = base64.b64encode(file_content).decode()

        msg = file_send(
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            conversation_id="conv-file-1",
            filename="results.csv",
            file_data=encoded,
            file_type="text/csv",
            description="Experiment results from run #42",
            checksum="abc123sha256",
        )
        self.assertEqual(msg.msg_type, MessageType.FILE_SEND)
        self.assertEqual(msg.sender_id, "agent-alpha")
        self.assertEqual(msg.payload["filename"], "results.csv")
        self.assertEqual(msg.payload["file_type"], "text/csv")
        self.assertEqual(msg.payload["file_data"], encoded)
        self.assertEqual(msg.payload["checksum"], "abc123sha256")

    def test_file_send_decodes_correctly(self):
        """Verify base64 payload can be decoded back to original."""
        import base64
        original = b"Hello, this is a test file!"
        encoded = base64.b64encode(original).decode()

        msg = file_send(
            sender_id="a",
            recipient_id="b",
            conversation_id="conv-decode",
            filename="test.txt",
            file_data=encoded,
        )
        decoded = base64.b64decode(msg.payload["file_data"])
        self.assertEqual(decoded, original)

    def test_file_request_creation(self):
        """Create a file_request message."""
        msg = file_request(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            conversation_id="conv-file-1",
            filename="results.csv",
            description="Need the experiment results for analysis",
            file_type_hint="text/csv",
        )
        self.assertEqual(msg.msg_type, MessageType.FILE_REQUEST)
        self.assertEqual(msg.sender_id, "agent-beta")
        self.assertEqual(msg.payload["filename"], "results.csv")
        self.assertEqual(msg.payload["file_type_hint"], "text/csv")

    def test_file_send_json_roundtrip(self):
        """Serialize and deserialize a file_send message."""
        import base64
        data = base64.b64encode(b"binary data here").decode()
        original = file_send(
            sender_id="a",
            recipient_id="b",
            conversation_id="conv-rt",
            filename="data.bin",
            file_data=data,
        )
        json_str = original.to_json()
        restored = BaseMessage.from_json(json_str)
        self.assertEqual(restored.msg_type, MessageType.FILE_SEND)
        self.assertEqual(restored.payload["filename"], "data.bin")

    def test_file_request_json_roundtrip(self):
        """Serialize and deserialize a file_request message."""
        original = file_request(
            sender_id="a",
            recipient_id="b",
            conversation_id="conv-rt",
            filename="model.pkl",
        )
        json_str = original.to_json()
        restored = BaseMessage.from_json(json_str)
        self.assertEqual(restored.msg_type, MessageType.FILE_REQUEST)
        self.assertEqual(restored.payload["filename"], "model.pkl")

    def test_file_send_with_empty_data(self):
        """File send with empty data should not crash."""
        msg = file_send(
            sender_id="a",
            recipient_id="b",
            conversation_id="conv-empty",
            filename="empty.txt",
            file_data="",
        )
        self.assertEqual(msg.payload["file_data"], "")


        self.assertEqual(msg.payload["file_data"], "")


class TestFileSendResponse(unittest.TestCase):
    """Test file_send_response message type."""

    def test_accept_file(self):
        msg = file_send_response(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            conversation_id="conv-file-1",
            file_message_id="msg-file-001",
            accepted=True,
        )
        self.assertEqual(msg.msg_type, MessageType.FILE_SEND_RESPONSE)
        self.assertTrue(msg.payload["accepted"])

    def test_decline_file_with_reason(self):
        msg = file_send_response(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            conversation_id="conv-file-1",
            file_message_id="msg-file-001",
            accepted=False,
            reason="File too large — max 10MB per transfer",
            save_as="",
        )
        self.assertFalse(msg.payload["accepted"])
        self.assertEqual(msg.payload["reason"], "File too large — max 10MB per transfer")

    def test_accept_with_rename(self):
        msg = file_send_response(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            conversation_id="conv-file-1",
            file_message_id="msg-file-001",
            accepted=True,
            save_as="renamed_results.csv",
        )
        self.assertEqual(msg.payload["save_as"], "renamed_results.csv")


class TestTaskDelegation(unittest.TestCase):
    """Test task_delegate and task_delegate_response messages."""

    def test_delegate_task(self):
        msg = task_delegate(
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            conversation_id="conv-delegate",
            task_description="Run sentiment analysis on the customer feedback dataset",
            priority="high",
            deadline="2026-04-28T12:00:00Z",
            context={"dataset": "feedback_2026.csv", "model": "distilbert"},
        )
        self.assertEqual(msg.msg_type, MessageType.TASK_DELEGATE)
        self.assertEqual(msg.payload["priority"], "high")
        self.assertEqual(msg.payload["context"]["dataset"], "feedback_2026.csv")

    def test_accept_delegation(self):
        msg = task_delegate_response(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            conversation_id="conv-delegate",
            delegate_message_id="msg-del-001",
            response="accept",
            estimated_completion="2026-04-28T10:00:00Z",
        )
        self.assertEqual(msg.msg_type, MessageType.TASK_DELEGATE_RESPONSE)
        self.assertEqual(msg.payload["response"], "accept")

    def test_decline_delegation(self):
        msg = task_delegate_response(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            conversation_id="conv-delegate",
            delegate_message_id="msg-del-001",
            response="decline",
            reason="I don't have the sentiment_analysis capability yet",
        )
        self.assertEqual(msg.payload["response"], "decline")

    def test_counter_offer_delegation(self):
        msg = task_delegate_response(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            conversation_id="conv-delegate",
            delegate_message_id="msg-del-001",
            response="counter_offer",
            counter_offer={"priority": "normal", "deadline": "2026-04-29T12:00:00Z"},
            reason="Can do it but need more time — tomorrow instead of today",
        )
        self.assertEqual(msg.payload["response"], "counter_offer")
        self.assertEqual(msg.payload["counter_offer"]["priority"], "normal")


class TestStatusQuery(unittest.TestCase):
    """Test status_query and status_response messages."""

    def test_status_query(self):
        msg = status_query(
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            conversation_id="conv-status",
            query="How's the analysis going?",
            about_task="task-001",
        )
        self.assertEqual(msg.msg_type, MessageType.STATUS_QUERY)
        self.assertEqual(msg.payload["about_task"], "task-001")

    def test_status_response(self):
        msg = status_response(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            conversation_id="conv-status",
            status="Working on it — 60% done",
            availability="busy",
            task_status={"task-001": "in_progress"},
        )
        self.assertEqual(msg.msg_type, MessageType.STATUS_RESPONSE)
        self.assertEqual(msg.payload["availability"], "busy")


class TestCapabilityShare(unittest.TestCase):
    """Test capability_share message."""

    def test_share_capability(self):
        msg = capability_share(
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            conversation_id="conv-cap",
            capability_name="sentiment_analysis",
            capability_description="Fine-tuned DistilBERT for customer feedback",
            confidence=0.85,
            metadata={"model": "distilbert-base-uncased", "accuracy": 0.89},
        )
        self.assertEqual(msg.msg_type, MessageType.CAPABILITY_SHARE)
        self.assertEqual(msg.payload["capability_name"], "sentiment_analysis")
        self.assertAlmostEqual(msg.payload["confidence"], 0.85)


class TestConsensus(unittest.TestCase):
    """Test consensus_request and consensus_vote messages."""

    def test_consensus_request(self):
        msg = consensus_request(
            sender_id="agent-alpha",
            proposal="Should we retry the failed pipeline or abort?",
            options=["retry", "abort", "escalate"],
            voters=["agent-beta", "agent-gamma"],
            quorum=2,
            deadline="2026-04-28T10:00:00Z",
        )
        self.assertEqual(msg.msg_type, MessageType.CONSENSUS_REQUEST)
        self.assertEqual(msg.recipient_id, "broadcast")
        self.assertEqual(msg.payload["options"], ["retry", "abort", "escalate"])

    def test_consensus_vote(self):
        msg = consensus_vote(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            consensus_message_id="msg-consensus-001",
            vote="retry",
            reasoning="The error looks transient — worth trying again",
        )
        self.assertEqual(msg.msg_type, MessageType.CONSENSUS_VOTE)
        self.assertEqual(msg.payload["vote"], "retry")

    def test_conditional_vote(self):
        msg = consensus_vote(
            sender_id="agent-gamma",
            recipient_id="agent-alpha",
            consensus_message_id="msg-consensus-001",
            vote="retry",
            reasoning="OK if we increase timeout",
            conditions={"max_retries": 2, "timeout_seconds": 120},
        )
        self.assertEqual(msg.payload["conditions"]["timeout_seconds"], 120)


class TestPeerHealthContext(unittest.TestCase):
    """Test heartbeat_peer, context_share, and error_report messages."""

    def test_heartbeat_peer(self):
        msg = heartbeat_peer(
            sender_id="agent-alpha",
            recipient_id="agent-beta",
        )
        self.assertEqual(msg.msg_type, MessageType.HEARTBEAT_PEER)
        self.assertIn("ping_timestamp", msg.payload)

    def test_context_share(self):
        msg = context_share(
            sender_id="agent-alpha",
            recipient_id="agent-beta",
            conversation_id="conv-ctx",
            context_type="config",
            context_data={"model": "gpt-4", "temperature": 0.7, "max_tokens": 4096},
            description="Current generation config for the analysis pipeline",
        )
        self.assertEqual(msg.msg_type, MessageType.CONTEXT_SHARE)
        self.assertEqual(msg.payload["context_type"], "config")
        self.assertEqual(msg.payload["context_data"]["temperature"], 0.7)

    def test_error_report(self):
        msg = error_report(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            conversation_id="conv-err",
            error_type="data_format",
            error_message="Expected CSV but received JSON array",
            related_task="task-001",
            severity="error",
            suggested_fix="Re-export the dataset as CSV with headers",
        )
        self.assertEqual(msg.msg_type, MessageType.ERROR_REPORT)
        self.assertEqual(msg.payload["error_type"], "data_format")
        self.assertEqual(msg.payload["severity"], "error")
        self.assertEqual(msg.payload["suggested_fix"], "Re-export the dataset as CSV with headers")

    def test_critical_error_report(self):
        msg = error_report(
            sender_id="agent-beta",
            recipient_id="agent-alpha",
            conversation_id="conv-err",
            error_type="crash",
            error_message="OOM during model inference",
            severity="critical",
        )
        self.assertEqual(msg.payload["severity"], "critical")


class TestNetworkCoordinatorPeerMessages(unittest.TestCase):
    """Test that the NetworkCoordinator handles peer messages correctly."""

    def test_coordinator_has_conversation_log(self):
        """Verify coordinator initializes with conversation log."""
        from network.coordinator import NetworkCoordinator
        coord = NetworkCoordinator(port=0)  # port 0 = don't actually bind
        self.assertIsNotNone(coord.conversation_log)
        self.assertIn("peer_messages_relayed", coord.network_stats)
        self.assertIn("conversations_started", coord.network_stats)

    def test_get_conversation_log_method(self):
        """Verify conversation query methods exist."""
        from network.coordinator import NetworkCoordinator
        coord = NetworkCoordinator(port=0)
        self.assertTrue(hasattr(coord, "get_conversation_log"))
        self.assertTrue(hasattr(coord, "get_agent_conversations"))
        self.assertTrue(hasattr(coord, "search_conversations"))
        self.assertTrue(hasattr(coord, "get_conversation_stats"))
        self.assertTrue(hasattr(coord, "export_conversation"))
        self.assertTrue(hasattr(coord, "export_all_conversations"))

    def test_network_status_includes_conversations(self):
        """Verify network status includes conversation stats."""
        from network.coordinator import NetworkCoordinator
        coord = NetworkCoordinator(port=0)
        status = coord.get_network_status()
        self.assertIn("conversations", status)
        self.assertIn("active_conversations", status)


if __name__ == "__main__":
    unittest.main(verbosity=2)


# ─── OTA Update Tests ──────────────────────────────────────────────

class TestOTAMessageTypes(unittest.TestCase):
    """Test OTA update message constructors."""

    def test_ota_update_announce(self):
        msg = ota_update_announce(
            sender_id="coordinator",
            version="1.5.0",
            changelog="Added consensus voting, OTA updates, file transfer",
            priority="high",
            deadline="2026-04-28T00:00:00Z",
            requires_restart=True,
        )
        self.assertEqual(msg.msg_type, MessageType.OTA_UPDATE_ANNOUNCE)
        self.assertEqual(msg.recipient_id, "broadcast")
        self.assertEqual(msg.payload["version"], "1.5.0")
        self.assertEqual(msg.payload["priority"], "high")
        self.assertTrue(msg.payload["requires_restart"])

    def test_ota_update_package(self):
        import base64
        pkg_data = base64.b64encode(b"fake_package_content").decode()
        msg = ota_update_package(
            sender_id="coordinator",
            recipient_id="agent-alpha",
            version="1.5.0",
            package_data=pkg_data,
            package_type="tar.gz",
            checksum=hashlib.sha256(b"fake_package_content").hexdigest(),
            install_script="pip install -e .",
            pre_install="pip check",
            post_install="systemctl restart agent",
            rollback_script="pip install agent-cluster==1.4.0",
        )
        self.assertEqual(msg.msg_type, MessageType.OTA_UPDATE_PACKAGE)
        self.assertEqual(msg.payload["version"], "1.5.0")
        self.assertTrue(msg.payload["package_data"])
        self.assertEqual(msg.payload["install_script"], "pip install -e .")

    def test_ota_update_ack_ready(self):
        msg = ota_update_ack(
            sender_id="agent-alpha",
            recipient_id="coordinator",
            version="1.5.0",
            announce_message_id="msg-announce-001",
            ready=True,
            current_version="1.4.0",
        )
        self.assertEqual(msg.msg_type, MessageType.OTA_UPDATE_ACK)
        self.assertTrue(msg.payload["ready"])
        self.assertEqual(msg.payload["current_version"], "1.4.0")

    def test_ota_update_ack_not_ready(self):
        msg = ota_update_ack(
            sender_id="agent-beta",
            recipient_id="coordinator",
            version="1.5.0",
            announce_message_id="msg-announce-001",
            ready=False,
            current_version="1.4.0",
        )
        self.assertFalse(msg.payload["ready"])

    def test_ota_update_status_success(self):
        msg = ota_update_status(
            sender_id="agent-alpha",
            recipient_id="coordinator",
            version="1.5.0",
            status="success",
            message="Updated to 1.5.0",
            previous_version="1.4.0",
            rollback_available=True,
        )
        self.assertEqual(msg.msg_type, MessageType.OTA_UPDATE_STATUS)
        self.assertEqual(msg.payload["status"], "success")

    def test_ota_update_status_failed(self):
        msg = ota_update_status(
            sender_id="agent-beta",
            recipient_id="coordinator",
            version="1.5.0",
            status="failed",
            message="Checksum mismatch",
            previous_version="1.4.0",
        )
        self.assertEqual(msg.payload["status"], "failed")

    def test_ota_update_rollback(self):
        msg = ota_update_rollback(
            sender_id="coordinator",
            recipient_id="agent-beta",
            version="1.5.0",
            reason="Fleet-wide rollback: critical bug in 1.5.0",
            target_version="1.4.0",
        )
        self.assertEqual(msg.msg_type, MessageType.OTA_UPDATE_ROLLBACK)
        self.assertEqual(msg.payload["target_version"], "1.4.0")


class TestCoordinatorOTAManager(unittest.TestCase):
    """Test coordinator-side OTA management."""

    def setUp(self):
        from protocol.ota_manager import CoordinatorOTAManager
        self.ota = CoordinatorOTAManager(install_dir="/tmp/test_ota_coord")
        # Create a fake package file
        self.pkg_path = "/tmp/test_ota_package.tar.gz"
        import tarfile, io
        with tarfile.open(self.pkg_path, "w:gz") as tar:
            info = tarfile.TarInfo(name="VERSION")
            info.size = len(b"1.5.0")
            tar.addfile(info, io.BytesIO(b"1.5.0"))

    def tearDown(self):
        import shutil
        shutil.rmtree("/tmp/test_ota_coord", ignore_errors=True)
        os.unlink(self.pkg_path)

    def test_register_update(self):
        pkg = self.ota.register_update(
            version="1.5.0",
            package_path=self.pkg_path,
            changelog="New features",
            priority="high",
        )
        self.assertEqual(pkg.version, "1.5.0")
        self.assertTrue(pkg.checksum)
        self.assertTrue(pkg.package_data)

    def test_get_announce_message(self):
        self.ota.register_update(version="1.5.0", package_path=self.pkg_path)
        announce = self.ota.get_announce_message("1.5.0")
        self.assertEqual(announce["version"], "1.5.0")
        self.assertIn("checksum", announce)
        self.assertGreater(announce["size_bytes"], 0)

    def test_get_package_message(self):
        self.ota.register_update(version="1.5.0", package_path=self.pkg_path)
        package = self.ota.get_package_message("1.5.0", "agent-alpha")
        self.assertIn("package_data", package)
        self.assertIn("checksum", package)

    def test_record_ack(self):
        self.ota.register_update(version="1.5.0", package_path=self.pkg_path)
        self.ota.mark_announced("1.5.0", count=2)
        self.ota.record_ack("1.5.0", "agent-alpha", ready=True, current_version="1.4.0")
        self.ota.record_ack("1.5.0", "agent-beta", ready=False, current_version="1.4.0")
        status = self.ota.get_fleet_status("1.5.0")
        self.assertEqual(status["rollout"]["acked"], 1)  # only agent-alpha was ready

    def test_record_install_status(self):
        self.ota.register_update(version="1.5.0", package_path=self.pkg_path)
        self.ota.record_ack("1.5.0", "agent-alpha", ready=True, current_version="1.4.0")
        self.ota.record_status("1.5.0", "agent-alpha", "success", "Updated")
        status = self.ota.get_fleet_status("1.5.0")
        self.assertEqual(status["rollout"]["installed"], 1)

    def test_fleet_status(self):
        self.ota.register_update(version="1.5.0", package_path=self.pkg_path)
        fleet = self.ota.get_fleet_status()
        self.assertIn("1.5.0", fleet["packages"])
        self.assertIn("1.5.0", fleet["rollouts"])

    def test_nonexistent_version(self):
        self.assertIsNone(self.ota.get_announce_message("9.9.9"))
        self.assertIsNone(self.ota.get_package_message("9.9.9", "agent-alpha"))


class TestAgentOTAInstaller(unittest.TestCase):
    """Test agent-side OTA installer."""

    def setUp(self):
        from protocol.ota_manager import AgentOTAInstaller
        self.install_dir = "/tmp/test_agent_install"
        self.backup_dir = "/tmp/test_agent_backups"
        os.makedirs(self.install_dir, exist_ok=True)
        # Write a "current version" file
        with open(os.path.join(self.install_dir, "VERSION"), "w") as f:
            f.write("1.4.0")
        self.installer = AgentOTAInstaller(
            agent_id="agent-alpha",
            install_dir=self.install_dir,
            current_version="1.4.0",
            backup_dir=self.backup_dir,
        )

    def tearDown(self):
        import shutil
        shutil.rmtree(self.install_dir, ignore_errors=True)
        shutil.rmtree(self.backup_dir, ignore_errors=True)

    def _make_package(self, version="1.5.0"):
        """Create a test OTA package payload."""
        import tarfile, io, base64, hashlib
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            content = version.encode()
            info = tarfile.TarInfo(name="VERSION")
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
        raw = buf.getvalue()
        return {
            "version": version,
            "package_data": base64.b64encode(raw).decode(),
            "package_type": "tar.gz",
            "checksum": hashlib.sha256(raw).hexdigest(),
            "install_script": "",
            "pre_install": "",
            "post_install": "",
            "rollback_script": "",
        }

    def test_successful_install(self):
        payload = self._make_package()
        result = self.installer.install_update(payload)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["version"], "1.5.0")
        self.assertEqual(result["previous_version"], "1.4.0")
        # Version file should be updated
        with open(os.path.join(self.install_dir, "VERSION")) as f:
            self.assertEqual(f.read(), "1.5.0")
        # Version tracked in installer
        self.assertEqual(self.installer.current_version, "1.5.0")

    def test_checksum_verification_passes(self):
        payload = self._make_package()
        result = self.installer.install_update(payload)
        self.assertEqual(result["status"], "success")

    def test_checksum_verification_fails(self):
        payload = self._make_package()
        payload["checksum"] = "0000000000000000"
        result = self.installer.install_update(payload)
        self.assertIn(result["status"], ["failed", "rolled_back"])

    def test_install_creates_backup(self):
        payload = self._make_package()
        self.installer.install_update(payload)
        history = self.installer.get_install_history()
        self.assertEqual(len(history), 1)
        self.assertTrue(os.path.exists(history[0]["backup_path"]))

    def test_rollback_on_failure(self):
        payload = self._make_package()
        payload["install_script"] = "exit 1"  # Force failure
        result = self.installer.install_update(payload)
        self.assertIn(result["status"], ["failed", "rolled_back"])

    def test_install_history(self):
        payload1 = self._make_package("1.5.0")
        self.installer.install_update(payload1)
        payload2 = self._make_package("1.6.0")
        self.installer.install_update(payload2)
        history = self.installer.get_install_history()
        self.assertEqual(len(history), 2)

    def test_rollback_command(self):
        # Install 1.5.0 successfully first
        payload = self._make_package("1.5.0")
        self.installer.install_update(payload)
        self.assertEqual(self.installer.current_version, "1.5.0")

        # Now rollback
        rollback_result = self.installer.execute_rollback({
            "version": "1.5.0",
            "target_version": "1.4.0",
        })
        self.assertTrue(rollback_result["success"])
        self.assertEqual(self.installer.current_version, "1.4.0")

    def test_no_consent_needed(self):
        """Verify install happens automatically — no consent check in code path."""
        payload = self._make_package()
        result = self.installer.install_update(payload)
        # No consent prompt, no confirmation step — direct install
        self.assertEqual(result["status"], "success")
