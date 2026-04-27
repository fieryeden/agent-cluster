#!/usr/bin/env python3
"""
Orchestration Tests

Tests for unified orchestration layer.
"""

import sys
import tempfile
import shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from orchestration import (
    ClusterOrchestrator,
    BotType,
    BotConfig,
    TaskScheduler,
    SchedulePolicy,
    MessageRouter,
    RoutingRule,
)
from protocol.messages import MessageType, create_message


def test_orchestrator_init():
    """Test orchestrator initialization."""
    print("\n[test_orchestrator_init]")
    
    orch = ClusterOrchestrator()
    assert len(orch.agents) == 0
    assert len(orch.pending_tasks) == 0
    print("  ✓ init")
    return True


def test_register_agent():
    """Test agent registration."""
    print("\n[test_register_agent]")
    
    orch = ClusterOrchestrator()
    
    config = BotConfig(
        bot_type=BotType.NANOBOT,
        agent_id="nano-001",
        capabilities=["ping", "echo"],
    )
    
    success = orch.register_agent(config)
    assert success
    assert "nano-001" in orch.agents
    assert "ping" in orch.capability_agents
    assert "nano-001" in orch.capability_agents["ping"]
    print("  ✓ register nanobot")
    
    # Register OpenClaw agent
    config2 = BotConfig(
        bot_type=BotType.OPENCLAW,
        agent_id="claw-001",
        capabilities=["research", "code"],
        connection_type="tcp",
        address="localhost:7891",
    )
    
    success = orch.register_agent(config2)
    assert success
    assert "claw-001" in orch.agents
    print("  ✓ register openclaw")
    
    return True


def test_task_submit():
    """Test task submission."""
    print("\n[test_task_submit]")
    
    orch = ClusterOrchestrator()
    
    # Register agent first
    config = BotConfig(
        bot_type=BotType.NANOBOT,
        agent_id="nano-001",
        capabilities=["echo"],
    )
    orch.register_agent(config)
    
    # Submit task
    task_id = orch.submit_task("echo", {"message": "hello"})
    assert task_id.startswith("task-")
    assert task_id in orch.tasks
    assert task_id in orch.pending_tasks
    print("  ✓ submit task")
    
    return True


def test_task_assign():
    """Test task assignment."""
    print("\n[test_task_assign]")
    
    orch = ClusterOrchestrator()
    
    # Register agent
    config = BotConfig(
        bot_type=BotType.NANOBOT,
        agent_id="nano-001",
        capabilities=["echo"],
    )
    orch.register_agent(config)
    
    # Submit task
    task_id = orch.submit_task("echo", {"message": "hello"})
    
    # Assign
    assigned = orch.assign_tasks()
    assert len(assigned) == 1
    assert task_id in assigned
    assert task_id not in orch.pending_tasks
    print("  ✓ assign task")
    
    return True


def test_multi_agent_assign():
    """Test assignment with multiple agents."""
    print("\n[test_multi_agent_assign]")
    
    orch = ClusterOrchestrator()
    
    # Register agents
    for i in range(3):
        config = BotConfig(
            bot_type=BotType.NANOBOT,
            agent_id=f"nano-{i:03d}",
            capabilities=["echo"],
        )
        orch.register_agent(config)
    
    # Submit tasks
    task_ids = []
    for i in range(6):
        task_id = orch.submit_task("echo", {"message": f"task-{i}"})
        task_ids.append(task_id)
    
    # Assign
    assigned = orch.assign_tasks()
    assert len(assigned) == 6  # 6 tasks submitted
    print("  ✓ multi-agent assign")
    
    return True


def test_scheduler():
    """Test task scheduler."""
    print("\n[test_scheduler]")
    
    scheduler = TaskScheduler(policy=SchedulePolicy.PRIORITY)
    
    # Submit tasks with different priorities
    scheduler.submit("low", "echo", priority=1)
    scheduler.submit("high", "echo", priority=10)
    scheduler.submit("medium", "echo", priority=5)
    
    # Get batch - should come in priority order
    batch = scheduler.next_batch(3)
    assert batch[0].task_id == "high"
    assert batch[1].task_id == "medium"
    assert batch[2].task_id == "low"
    print("  ✓ priority ordering")
    
    return True


def test_router():
    """Test message router."""
    print("\n[test_router]")
    
    tmpdir = tempfile.mkdtemp(prefix="router_test_")
    
    try:
        router = MessageRouter(base_dir=tmpdir)
        
        # Register route
        agent_dir = Path(tmpdir) / "agents" / "nano-001"
        router.register_route("nano-001", str(agent_dir), "file")
        
        # Create message
        msg = create_message(
            MessageType.HEARTBEAT,
            sender_id="coordinator",
            recipient_id="nano-001",
            payload={"status": "ok"},
        )
        
        # Route
        success = router.route(msg)
        assert success
        print("  ✓ file routing")
        
        return True
    finally:
        shutil.rmtree(tmpdir)


def test_status():
    """Test status reporting."""
    print("\n[test_status]")
    
    orch = ClusterOrchestrator()
    
    # Register mixed agents
    orch.register_agent(BotConfig(BotType.NANOBOT, "nano-001", ["echo"]))
    orch.register_agent(BotConfig(BotType.OPENCLAW, "claw-001", ["research"]))
    orch.register_agent(BotConfig(BotType.EXTENSION, "ext-001", ["ui"]))
    
    status = orch.get_status()
    
    assert status["by_type"]["nanobot"] == 1
    assert status["by_type"]["openclaw"] == 1
    assert status["by_type"]["extension"] == 1
    print("  ✓ status by type")
    
    return True


def main():
    """Run all tests."""
    print("=" * 50)
    print("ORCHESTRATION TESTS")
    print("=" * 50)
    
    tests = [
        test_orchestrator_init,
        test_register_agent,
        test_task_submit,
        test_task_assign,
        test_multi_agent_assign,
        test_scheduler,
        test_router,
        test_status,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            failed += 1
            print(f"  ✗ {test.__name__}: {e}")
    
    print("\n" + "=" * 50)
    print(f"TOTAL: {passed}/{passed + failed} tests passed")
    print("=" * 50)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    import traceback
    try:
        exit(main())
    except Exception as e:
        traceback.print_exc()
        exit(1)
