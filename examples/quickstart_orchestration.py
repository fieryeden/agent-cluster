#!/usr/bin/env python3
"""
Quick Start - Orchestration Layer

Minimal example showing how to use the unified orchestration.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from orchestration import ClusterOrchestrator, BotType, BotConfig


def main():
    print("=== Agent Cluster Orchestrator ===\n")
    
    # 1. Create orchestrator
    orch = ClusterOrchestrator(config_dir="/tmp/my_cluster")
    orch.start()
    
    # 2. Register a NanoBot (file-based)
    nano_config = BotConfig(
        bot_type=BotType.NANOBOT,
        agent_id="nano-worker-001",
        capabilities=["echo", "ping", "python"],
        connection_type="file",
    )
    orch.register_agent(nano_config)
    
    # 3. Register an OpenClaw agent (network-based)
    claw_config = BotConfig(
        bot_type=BotType.OPENCLAW,
        agent_id="claw-researcher-001",
        capabilities=["research", "analysis", "web_search"],
        connection_type="tcp",
        address="localhost:7891",
    )
    orch.register_agent(claw_config)
    
    # 4. Submit tasks
    print("\n--- Submitting Tasks ---")
    
    # Task for NanoBot
    task1 = orch.submit_task("echo", {"message": "Hello from orchestrator!"})
    print(f"Submitted: {task1}")
    
    # Task for OpenClaw
    task2 = orch.submit_task("research", {"topic": "AI agent orchestration patterns"})
    print(f"Submitted: {task2}")
    
    # 5. Assign tasks to agents
    print("\n--- Assigning Tasks ---")
    assigned = orch.assign_tasks()
    print(f"Assigned: {assigned}")
    
    # 6. Check status
    print("\n--- Cluster Status ---")
    status = orch.get_status()
    print(f"Agents: {len(status['agents'])}")
    print(f"By type: {status['by_type']}")
    print(f"Pending tasks: {status['tasks']['pending']}")
    
    # 7. Simulate task completion
    print("\n--- Completing Task ---")
    orch.complete_task(task1, {"result": "ECHO: Hello from orchestrator!"}, "nano-worker-001")
    
    # Final status
    print("\n--- Final Status ---")
    status = orch.get_status()
    for agent_id, agent_data in status['agents'].items():
        print(f"{agent_id}: {agent_data['completed']} completed, {agent_data['failed']} failed")
    
    orch.stop()
    print("\n=== Done ===")


if __name__ == "__main__":
    main()
