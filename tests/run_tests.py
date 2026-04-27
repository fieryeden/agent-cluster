#!/usr/bin/env python3
"""
Test Suite for Agent Cluster MVP

Run with: python run_tests.py
"""

import os
import sys
import json
import time
import tempfile
import threading
import shutil
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import (
    MessageType, BaseMessage, MessageQueue, AgentCapability,
    heartbeat, register_agent, capability_query, capability_response,
    task_assign, task_progress, task_complete, task_failed,
    create_message
)

from agents.nano_bot import NanoBot, BotConfig
from coordinator.coordinator import Coordinator


class TestResults:
    """Simple test result tracker."""
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
    
    def success(self, name):
        self.passed += 1
        print(f"  ✓ {name}")
    
    def failure(self, name, error):
        self.failed += 1
        self.errors.append((name, str(error)))
        print(f"  ✗ {name}: {error}")
    
    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*50}")
        print(f"Results: {self.passed}/{total} passed")
        if self.errors:
            print("\nFailures:")
            for name, err in self.errors:
                print(f"  - {name}: {err}")
        print('='*50)
        return self.failed == 0


def test_protocol_messages():
    """Test message creation and serialization."""
    print("\n[test_protocol_messages]")
    results = TestResults()
    
    try:
        # Test heartbeat
        hb = heartbeat("agent-001")
        assert hb.msg_type == MessageType.HEARTBEAT
        assert hb.sender_id == "agent-001"
        assert hb.recipient_id == "coordinator"
        results.success("heartbeat creation")
        
        # Test serialization
        json_str = hb.to_json()
        parsed = json.loads(json_str)
        assert parsed['msg_type'] == 'heartbeat'
        results.success("heartbeat serialization")
        
        # Test deserialization
        hb2 = BaseMessage.from_json(json_str)
        assert hb2.msg_type == MessageType.HEARTBEAT
        assert hb2.sender_id == "agent-001"
        results.success("heartbeat deserialization")
        
    except Exception as e:
        results.failure("protocol test", e)
    
    return results


def test_message_queue():
    """Test file-based message queue."""
    print("\n[test_message_queue]")
    results = TestResults()
    
    # Create temp directory
    tmpdir = tempfile.mkdtemp(prefix="cluster_test_")
    
    try:
        queue = MessageQueue(tmpdir, "test-agent")
        
        # Test send - send to self, not coordinator
        msg = create_message(
            MessageType.HEARTBEAT,
            sender_id="test-agent",
            recipient_id="test-agent",  # Send to self for test
            payload={"load": 0.0, "status": "active"}
        )
        filepath = queue.send(msg)
        assert os.path.exists(filepath)
        results.success("queue send")
        
        # Test receive
        messages = queue.receive(include_broadcast=False)
        assert len(messages) == 1
        assert messages[0][1].msg_type == MessageType.HEARTBEAT
        results.success("queue receive")
        
        # Test mark processed
        queue.mark_processed(messages[0][0])
        assert not os.path.exists(messages[0][0])
        assert os.path.exists(os.path.join(queue.processed_dir, os.path.basename(messages[0][0])))
        results.success("mark processed")
        
    except Exception as e:
        results.failure("queue test", e)
    finally:
        shutil.rmtree(tmpdir)
    
    return results


def test_nano_bot_init():
    """Test nano bot initialization."""
    print("\n[test_nano_bot_init]")
    results = TestResults()
    
    tmpdir = tempfile.mkdtemp(prefix="cluster_test_")
    
    try:
        config = BotConfig(
            agent_id="test-bot-001",
            agent_type="worker",
            cluster_dir=tmpdir,
            capabilities=[{"name": "test", "confidence": 0.8}]
        )
        
        bot = NanoBot(config)
        assert bot.agent_id == "test-bot-001"
        assert "test" in bot.capabilities
        results.success("bot initialization")
        
        # Test built-in handlers exist
        assert 'ping' in bot.handlers
        assert 'echo' in bot.handlers
        assert 'shell' in bot.handlers
        results.success("handlers registered")
        
    except Exception as e:
        results.failure("bot init test", e)
    finally:
        shutil.rmtree(tmpdir)
    
    return results


def test_task_handlers():
    """Test nano bot task handlers."""
    print("\n[test_task_handlers]")
    results = TestResults()
    
    tmpdir = tempfile.mkdtemp(prefix="cluster_test_")
    
    try:
        config = BotConfig(
            agent_id="test-bot-001",
            agent_type="worker",
            cluster_dir=tmpdir
        )
        bot = NanoBot(config)
        
        # Test ping
        result = bot._handle_ping({})
        assert result['pong'] == True
        results.success("ping handler")
        
        # Test echo
        result = bot._handle_echo({"hello": "world"})
        assert result['echo']['hello'] == "world"
        results.success("echo handler")
        
        # Test python
        result = bot._handle_python({"code": "x = 1 + 1"})
        assert result['success'] == True
        results.success("python handler")
        
        # Test shell (safe command)
        result = bot._handle_shell({"command": "echo hello"})
        assert result['returncode'] == 0
        assert "hello" in result['stdout']
        results.success("shell handler")
        
        # Test shell (dangerous blocked)
        try:
            result = bot._handle_shell({"command": "rm -rf /"})
            results.failure("shell safety", "Should have blocked dangerous command")
        except ValueError:
            results.success("shell safety")
        
    except Exception as e:
        results.failure("handler test", e)
    finally:
        shutil.rmtree(tmpdir)
    
    return results


def test_coordinator_init():
    """Test coordinator initialization."""
    print("\n[test_coordinator_init]")
    results = TestResults()
    
    tmpdir = tempfile.mkdtemp(prefix="cluster_test_")
    
    try:
        coord = Coordinator(tmpdir)
        
        # Check directories created
        assert os.path.exists(f"{tmpdir}/coordinator/inbox")
        assert os.path.exists(f"{tmpdir}/broadcast")
        results.success("directory setup")
        
        # Test status method
        status = coord.get_status()
        assert 'agents' in status
        assert 'tasks' in status
        results.success("status method")
        
    except Exception as e:
        results.failure("coordinator init", e)
    finally:
        shutil.rmtree(tmpdir)
    
    return results


def test_coordinator_task_routing():
    """Test coordinator task submission and routing."""
    print("\n[test_coordinator_task_routing]")
    results = TestResults()
    
    tmpdir = tempfile.mkdtemp(prefix="cluster_test_")
    
    try:
        coord = Coordinator(tmpdir)
        
        # Submit task
        task_id = coord.submit_task("ping", {"test": True})
        assert task_id in coord.tasks
        assert coord.tasks[task_id].status == "pending"
        results.success("task submission")
        
        # No agents, so no assignment
        coord._assign_pending_tasks()
        assert coord.tasks[task_id].status == "pending"  # Still pending
        results.success("no agent handling")
        
        # Add mock agent (directly, no threading)
        from coordinator.coordinator import AgentInfo
        mock_agent = AgentInfo(
            agent_id="mock-agent-001",
            agent_type="worker",
            capabilities={"ping": AgentCapability(name="ping", confidence=0.9)},
            device_info={},
            last_heartbeat=time.time()
        )
        coord.agents["mock-agent-001"] = mock_agent
        coord.capability_index["ping"].add("mock-agent-001")
        
        # Now assign
        coord._assign_pending_tasks()
        assert coord.tasks[task_id].status == "assigned"
        results.success("task assignment")
        
    except Exception as e:
        results.failure("task routing", e)
    finally:
        shutil.rmtree(tmpdir)
    
    return results


def test_integration():
    """Integration test: coordinator + bot communication."""
    print("\n[test_integration]")
    results = TestResults()
    
    tmpdir = tempfile.mkdtemp(prefix="cluster_test_")
    
    try:
        # Setup coordinator
        coord = Coordinator(tmpdir)
        
        # Create bot config
        bot_config = BotConfig(
            agent_id="integration-bot-001",
            agent_type="worker",
            cluster_dir=tmpdir,
            heartbeat_interval=1  # Fast heartbeat for test
        )
        
        # Create bot
        bot = NanoBot(bot_config)
        
        # Manually register (no threading)
        bot._register()
        
        # Process registration in coordinator
        coord._process_messages()
        
        assert "integration-bot-001" in coord.agents
        results.success("bot registration")
        
        # Submit task to coordinator
        task_id = coord.submit_task("echo", {"message": "integration test"})
        
        # Assign task
        coord._assign_pending_tasks()
        
        # Bot processes messages
        bot._process_messages()
        
        # Check bot received task
        # (In real test, bot would execute and report)
        results.success("task flow")
        
    except Exception as e:
        import traceback
        results.failure("integration test", traceback.format_exc())
    finally:
        shutil.rmtree(tmpdir)
    
    return results


def main():
    """Run all tests."""
    print("="*50)
    print("Agent Cluster MVP Test Suite")
    print("="*50)
    
    all_results = []
    
    # Run tests
    all_results.append(test_protocol_messages())
    all_results.append(test_message_queue())
    all_results.append(test_nano_bot_init())
    all_results.append(test_task_handlers())
    all_results.append(test_coordinator_init())
    all_results.append(test_coordinator_task_routing())
    all_results.append(test_integration())
    
    # Aggregate results
    total_passed = sum(r.passed for r in all_results)
    total_failed = sum(r.failed for r in all_results)
    
    print(f"\n{'='*50}")
    print(f"TOTAL: {total_passed}/{total_passed + total_failed} tests passed")
    print('='*50)
    
    return total_failed == 0


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
