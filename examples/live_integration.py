#!/usr/bin/env python3
"""
Live Agent Integration

Shows how to integrate actual NanoBots with the orchestrator.
"""

import sys
import time
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from orchestration import ClusterOrchestrator, BotType, BotConfig as OrchBotConfig
from agents.nano_bot import NanoBot, BotConfig as NanoBotConfig


def main():
    print("=== Live Agent Integration ===\n")
    
    # Create temp directory for file-based messaging
    cluster_dir = tempfile.mkdtemp(prefix="live_cluster_")
    print(f"Cluster dir: {cluster_dir}")
    
    # 1. Create orchestrator
    orch = ClusterOrchestrator(config_dir=cluster_dir)
    orch.start()
    
    # 2. Create a real NanoBot
    nano_config = NanoBotConfig(
        agent_id="nano-live-001",
        agent_type="worker",
        cluster_dir=cluster_dir,
        capabilities=[{"name": "echo", "confidence": 1.0}],
    )
    bot = NanoBot(nano_config)
    
    # 3. Register with orchestrator
    orch_config = OrchBotConfig(
        bot_type=BotType.NANOBOT,
        agent_id="nano-live-001",
        capabilities=["echo"],
        connection_type="file",
    )
    orch.register_agent(orch_config)
    
    # 4. Bot registers with coordinator (file-based)
    print("\n--- Bot Registration ---")
    bot._register()
    
    # 5. Orchestrator picks up registration
    print("\n--- Submitting Task ---")
    task_id = orch.submit_task("echo", {"message": "Live test!"})
    orch.assign_tasks()
    
    # 6. Bot processes task (poll loop)
    print("\n--- Bot Processing ---")
    bot._process_messages()  # Check for messages once
    
    # Bot executes the task
    print("Bot received and processing task...")
    
    # Give it a moment
    time.sleep(0.1)
    
    # Simulate completion (in real scenario, bot would complete it)
    orch.complete_task(task_id, {"output": "ECHO: Live test!"}, "nano-live-001")
    
    # 6. Check final state
    print("\n--- Final State ---")
    status = orch.get_status()
    print(f"Completed tasks: {len(orch.tasks)}")
    
    for agent_id, agent_data in status['agents'].items():
        print(f"{agent_id}: {agent_data}")
    
    orch.stop()
    print("\n=== Done ===")
    
    # Cleanup
    import shutil
    shutil.rmtree(cluster_dir)


if __name__ == "__main__":
    main()
