#!/usr/bin/env python3
"""
Demo: Agent Cluster MVP in action.
Shows basic usage: start coordinator, start agents, assign tasks.
"""

import sys
import time
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "nano_bot"))
sys.path.insert(0, str(Path(__file__).parent.parent / "coordinator"))

from agent import NanoBot
from server import Coordinator


def demo():
    """Run a simple demo of the agent cluster."""
    print("\n" + "="*60)
    print("AGENT CLUSTER MVP - Demo")
    print("="*60 + "\n")
    
    # Use a temp directory for this demo
    import tempfile
    tmpdir = tempfile.mkdtemp(prefix="agent_cluster_demo_")
    print(f"[SETUP] Shared directory: {tmpdir}\n")
    
    # Start coordinator
    print("[SETUP] Starting coordinator...")
    coord = Coordinator(shared_dir=tmpdir)
    coord_thread = threading.Thread(target=coord.run, daemon=True)
    coord_thread.start()
    time.sleep(1)
    
    # Start multiple agents
    print("[SETUP] Starting agents...")
    agents = []
    agent_configs = [
        ("worker-alpha", {"echo": 1.0, "shell": 0.9, "data_process": 0.7}),
        ("worker-beta", {"echo": 1.0, "shell": 0.5, "math": 0.9}),
        ("worker-gamma", {"echo": 0.8, "special_task": 1.0}),
    ]
    
    for agent_id, caps in agent_configs:
        agent = NanoBot(agent_id=agent_id, shared_dir=tmpdir, capabilities=caps)
        thread = threading.Thread(target=agent.run, daemon=True)
        thread.start()
        agents.append(agent)
        print(f"  → Started {agent_id}: {list(caps.keys())}")
    
    time.sleep(2)  # Let agents register
    print()
    
    # Show cluster status
    print("[STATUS] Cluster Status:")
    status = coord.get_cluster_status()
    print(f"  Total agents: {status['total_agents']}")
    print(f"  Alive agents: {status['alive_agents']}")
    print(f"  Pending tasks: {status['pending_tasks']}")
    print()
    
    # Demo 1: Echo task
    print("[TASK] Assigning echo task...")
    task1 = coord.assign_task("echo", {"message": "Hello from coordinator!"})
    print(f"  → Task ID: {task1}")
    
    result1 = coord.get_task_result(task1, timeout=10)
    if result1:
        print(f"  → Result: {result1['response'].get('echo')}")
    print()
    
    # Demo 2: Shell task
    print("[TASK] Assigning shell task (hostname)...")
    task2 = coord.assign_task("shell", {"command": "hostname"})
    print(f"  → Task ID: {task2}")
    
    result2 = coord.get_task_result(task2, timeout=10)
    if result2:
        print(f"  → Result: {result2['response'].get('stdout', '').strip()}")
    print()
    
    # Demo 3: Multiple tasks
    print("[TASK] Assigning multiple tasks...")
    tasks = []
    for i in range(3):
        task = coord.assign_task("echo", {"message": f"Parallel task {i+1}"})
        tasks.append(task)
        print(f"  → Task {i+1}: {task}")
    
    # Collect results
    print("\n[WAIT] Collecting results...")
    for i, task in enumerate(tasks):
        result = coord.get_task_result(task, timeout=10)
        if result:
            print(f"  → Task {i+1} result: {result['response'].get('echo')}")
    print()
    
    # Show final status
    print("[STATUS] Final Cluster Status:")
    status = coord.get_cluster_status()
    print(f"  Completed tasks: {status['completed_tasks']}")
    print()
    
    # Cleanup
    print("[CLEANUP] Stopping agents...")
    for agent in agents:
        agent.stop()
    coord.stop()
    
    # Clean up temp dir
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)
    
    print("\n" + "="*60)
    print("[DONE] Demo completed successfully!")
    print("="*60 + "\n")


if __name__ == "__main__":
    demo()
