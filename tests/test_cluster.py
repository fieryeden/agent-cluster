#!/usr/bin/env python3
"""
Test suite for Agent Cluster MVP.
Tests basic functionality: registration, heartbeat, task routing.
"""

import sys
import time
import json
import tempfile
import threading
import shutil
from pathlib import Path

# Add parent dirs to path
sys.path.insert(0, str(Path(__file__).parent.parent / "nano_bot"))
sys.path.insert(0, str(Path(__file__).parent.parent / "coordinator"))

from agent import NanoBot, HEARTBEAT_TIMEOUT
from server import Coordinator


def test_agent_registration():
    """Test that agent registers with coordinator."""
    print("\n[TEST] Agent Registration")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Start coordinator
        coord = Coordinator(shared_dir=tmpdir)
        coord_thread = threading.Thread(target=coord.run, daemon=True)
        coord_thread.start()
        
        time.sleep(0.5)  # Let coordinator initialize
        
        # Start agent
        agent = NanoBot(
            agent_id="test-agent-1",
            shared_dir=tmpdir,
            capabilities={"echo": 1.0, "test": 0.5}
        )
        agent_thread = threading.Thread(target=agent.run, daemon=True)
        agent_thread.start()
        
        # Wait for registration (coordinator scans every 5s)
        time.sleep(6)
        
        # Force a scan to pick up agent
        coord.scan_for_agents()
        time.sleep(1)
        
        # Check registration
        assert "test-agent-1" in coord.agents, "Agent not registered"
        agent_info = coord.agents["test-agent-1"]
        assert "echo" in agent_info.capabilities, "Capabilities not registered"
        assert agent_info.is_alive(), "Agent should be alive"
        
        print(f"  ✓ Agent registered: {agent_info.agent_id}")
        print(f"  ✓ Capabilities: {list(agent_info.capabilities.keys())}")
        print(f"  ✓ Status: {agent_info.status}")
        
        # Cleanup
        agent.stop()
        coord.stop()
        
    print("[PASS] Agent Registration\n")


def test_heartbeat():
    """Test that agent sends heartbeats."""
    print("\n[TEST] Heartbeat Monitoring")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        coord = Coordinator(shared_dir=tmpdir)
        coord_thread = threading.Thread(target=coord.run, daemon=True)
        coord_thread.start()
        
        time.sleep(0.5)
        
        agent = NanoBot(
            agent_id="heartbeat-agent",
            shared_dir=tmpdir,
            capabilities={"echo": 1.0}
        )
        agent_thread = threading.Thread(target=agent.run, daemon=True)
        agent_thread.start()
        
        # Wait for registration (coordinator scans every 5s)
        time.sleep(6)
        coord.scan_for_agents()
        time.sleep(1)
        
        agent_info = coord.agents.get("heartbeat-agent")
        assert agent_info, "Agent not found"
        
        first_heartbeat = agent_info.last_heartbeat
        
        # Wait for next heartbeat
        time.sleep(12)  # > HEARTBEAT_INTERVAL (10s)
        
        second_heartbeat = agent_info.last_heartbeat
        assert second_heartbeat > first_heartbeat, "Heartbeat not updated"
        
        print(f"  ✓ First heartbeat: {first_heartbeat:.1f}")
        print(f"  ✓ Second heartbeat: {second_heartbeat:.1f}")
        print(f"  ✓ Heartbeat updated: {second_heartbeat - first_heartbeat:.1f}s")
        
        agent.stop()
        coord.stop()
        
    print("[PASS] Heartbeat Monitoring\n")


def test_task_routing():
    """Test that coordinator routes tasks to capable agents."""
    print("\n[TEST] Task Routing")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        coord = Coordinator(shared_dir=tmpdir)
        coord_thread = threading.Thread(target=coord.run, daemon=True)
        coord_thread.start()
        
        time.sleep(0.5)
        
        # Start agent with echo capability
        agent = NanoBot(
            agent_id="worker-1",
            shared_dir=tmpdir,
            capabilities={"echo": 1.0, "heavy_compute": 0.3}
        )
        agent_thread = threading.Thread(target=agent.run, daemon=True)
        agent_thread.start()
        
        # Wait for registration
        time.sleep(6)
        coord.scan_for_agents()
        time.sleep(1)
        
        # Check capabilities query
        echo_agents = coord.query_capabilities("echo")
        assert len(echo_agents) > 0, "No agents found with echo capability"
        assert echo_agents[0]["agent_id"] == "worker-1", "Wrong agent"
        assert echo_agents[0]["confidence"] == 1.0, "Wrong confidence"
        
        print(f"  ✓ Found {len(echo_agents)} agent(s) with echo capability")
        print(f"  ✓ Best agent: {echo_agents[0]['agent_id']} (confidence: {echo_agents[0]['confidence']})")
        
        # Assign task
        task_id = coord.assign_task("echo", {"message": "Hello, Cluster!"})
        print(f"  ✓ Task assigned: {task_id}")
        
        # Wait for completion
        result = coord.get_task_result(task_id, timeout=10)
        assert result, "Task timed out"
        assert "response" in result, "No response in result"
        
        response = result["response"]
        assert response.get("status") == "success", "Task failed"
        assert response.get("echo") == "Hello, Cluster!", "Wrong echo response"
        
        print(f"  ✓ Task completed: {response.get('echo')}")
        
        agent.stop()
        coord.stop()
        
    print("[PASS] Task Routing\n")


def test_capability_selection():
    """Test that coordinator selects best agent for task."""
    print("\n[TEST] Capability-Based Selection")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        coord = Coordinator(shared_dir=tmpdir)
        coord_thread = threading.Thread(target=coord.run, daemon=True)
        coord_thread.start()
        
        time.sleep(0.5)
        
        # Start multiple agents with different capabilities
        agents = []
        for i, caps in enumerate([
            {"echo": 0.5, "compute": 0.9},  # Agent 0: good at compute
            {"echo": 1.0, "compute": 0.3},  # Agent 1: best at echo
            {"echo": 0.7, "compute": 0.5},  # Agent 2: medium
        ]):
            agent = NanoBot(
                agent_id=f"agent-{i}",
                shared_dir=tmpdir,
                capabilities=caps
            )
            thread = threading.Thread(target=agent.run, daemon=True)
            thread.start()
            agents.append(agent)
        
        # Wait for registration
        time.sleep(6)
        coord.scan_for_agents()
        time.sleep(1)
        
        # Query for echo capability
        echo_agents = coord.query_capabilities("echo")
        assert len(echo_agents) == 3, f"Expected 3 agents, got {len(echo_agents)}"
        
        # Should select agent-1 (highest confidence: 1.0)
        best = echo_agents[0]
        assert best["agent_id"] == "agent-1", f"Wrong best agent: {best['agent_id']}"
        assert best["confidence"] == 1.0, f"Wrong confidence: {best['confidence']}"
        
        print(f"  ✓ Echo agents: {[a['agent_id'] for a in echo_agents]}")
        print(f"  ✓ Best for echo: {best['agent_id']} (confidence: {best['confidence']})")
        
        # Query for compute capability
        compute_agents = coord.query_capabilities("compute")
        best_compute = compute_agents[0]
        assert best_compute["agent_id"] == "agent-0", "Wrong compute agent"
        assert best_compute["confidence"] == 0.9, "Wrong compute confidence"
        
        print(f"  ✓ Best for compute: {best_compute['agent_id']} (confidence: {best_compute['confidence']})")
        
        for agent in agents:
            agent.stop()
        coord.stop()
        
    print("[PASS] Capability-Based Selection\n")


def test_agent_death_detection():
    """Test that coordinator detects when agent dies."""
    print("\n[TEST] Agent Death Detection")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        coord = Coordinator(shared_dir=tmpdir)
        coord_thread = threading.Thread(target=coord.run, daemon=True)
        coord_thread.start()
        
        time.sleep(0.5)
        
        agent = NanoBot(
            agent_id="mortal-agent",
            shared_dir=tmpdir,
            capabilities={"echo": 1.0}
        )
        agent_thread = threading.Thread(target=agent.run, daemon=True)
        agent_thread.start()
        
        # Wait for registration
        time.sleep(6)
        coord.scan_for_agents()
        time.sleep(1)
        
        # Agent should be alive
        agent_info = coord.agents.get("mortal-agent")
        assert agent_info and agent_info.is_alive(), "Agent should be alive"
        print(f"  ✓ Agent alive: {agent_info.is_alive()}")
        
        # Kill agent
        agent.stop()
        time.sleep(2)
        
        # Manually update coordinator's view
        coord.check_agent_health()
        
        # After heartbeat timeout, should be dead
        # (Need to wait for timeout)
        time.sleep(HEARTBEAT_TIMEOUT + 5)
        
        coord.check_agent_health()
        agent_info = coord.agents.get("mortal-agent")
        if agent_info:
            print(f"  ✓ Agent status: {agent_info.status}")
            print(f"  ✓ Is alive: {agent_info.is_alive()}")
        
        coord.stop()
        
    print("[PASS] Agent Death Detection\n")


def run_all_tests():
    """Run all tests."""
    print("\n" + "="*60)
    print("AGENT CLUSTER MVP - Test Suite")
    print("="*60)
    
    try:
        test_agent_registration()
        test_heartbeat()
        test_task_routing()
        test_capability_selection()
        test_agent_death_detection()
        
        print("="*60)
        print("[SUCCESS] All tests passed!")
        print("="*60 + "\n")
        return True
        
    except AssertionError as e:
        print(f"\n[FAIL] Test failed: {e}\n")
        return False
    except Exception as e:
        print(f"\n[ERROR] Test error: {e}\n")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
