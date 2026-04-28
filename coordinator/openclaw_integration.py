#!/usr/bin/env python3
"""
OpenClaw Coordinator Integration

Wires OpenClaw as the cluster coordinator.
"""

import sys
import json
import time
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from orchestration import ClusterOrchestrator, BotType, BotConfig
from protocol.messages import BaseMessage, MessageType, create_message


class OpenClawCoordinator:
    """
    OpenClaw as cluster coordinator.
    
    Responsibilities:
    - Manage agent connections
    - Route tasks to capable agents
    - Monitor agent health
    - Handle external requests (from OpenClaw sessions)
    """
    
    def __init__(self, cluster_dir: str = None):
        self.cluster_dir = cluster_dir or "/tmp/openclaw_cluster"
        
        # Create orchestrator
        self.orchestrator = ClusterOrchestrator(config_dir=self.cluster_dir)
        
        # OpenClaw-specific handlers
        self.pending_requests: Dict[str, Dict] = {}  # request_id -> request
        self.results: Dict[str, Any] = {}  # request_id -> result
        
        # Callbacks for OpenClaw integration
        self.on_agent_joined: Optional[callable] = None
        self.on_agent_left: Optional[callable] = None
        self.on_task_result: Optional[callable] = None
        
        # State
        self.running = False
        self._poll_thread: Optional[threading.Thread] = None
    
    def start(self):
        """Start the coordinator."""
        self.orchestrator.start()
        self.running = True
        
        # Start poll thread
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        
        print("[OpenClaw-Coordinator] Started")
    
    def stop(self):
        """Stop the coordinator."""
        self.running = False
        self.orchestrator.stop()
        print("[OpenClaw-Coordinator] Stopped")
    
    # === Agent Management ===
    
    def register_agent(
        self,
        agent_id: str,
        bot_type: BotType,
        capabilities: List[str],
        config: Dict = None,
    ) -> bool:
        """
        Register an agent with the cluster.
        
        Args:
            agent_id: Unique agent identifier
            bot_type: Type of bot (NANOBOT, OPENCLAW, etc.)
            capabilities: List of capability names
            config: Optional additional config
        
        Returns:
            True if registered successfully
        """
        bot_config = BotConfig(
            bot_type=bot_type,
            agent_id=agent_id,
            capabilities=capabilities,
            connection_type=config.get("connection_type", "file") if config else "file",
            address=config.get("address") if config else None,
            config=config or {},
        )
        
        success = self.orchestrator.register_agent(bot_config)
        
        if success and self.on_agent_joined:
            self.on_agent_joined(agent_id, bot_type, capabilities)
        
        return success
    
    def unregister_agent(self, agent_id: str):
        """Unregister an agent."""
        self.orchestrator.unregister_agent(agent_id)
        
        if self.on_agent_left:
            self.on_agent_left(agent_id)
    
    def get_agents(self) -> List[Dict]:
        """Get all registered agents."""
        status = self.orchestrator.get_status()
        return list(status["agents"].values())
    
    # === Task Execution ===
    
    def execute_task(
        self,
        task_type: str,
        params: Dict,
        target_agent: str = None,
        priority: int = 0,
        timeout: float = 60.0,
    ) -> str:
        """
        Submit a task for execution.
        
        Args:
            task_type: Type of task (capability name)
            params: Task parameters
            target_agent: Specific agent (or None for auto-assign)
            priority: Task priority
            timeout: Timeout in seconds
        
        Returns:
            Request ID (use to get result)
        """
        import uuid
        request_id = f"req-{uuid.uuid4().hex[:8]}"
        
        # Submit to orchestrator
        task_id = self.orchestrator.submit_task(
            task_type=task_type,
            params=params,
            target_agent=target_agent,
            priority=priority,
        )
        
        # Track request
        self.pending_requests[request_id] = {
            "task_id": task_id,
            "task_type": task_type,
            "params": params,
            "timeout": timeout,
            "submitted_at": datetime.now(),
        }
        
        # Assign immediately
        self.orchestrator.assign_tasks()
        
        print(f"[OpenClaw-Coordinator] Task submitted: {task_type} ({request_id})")
        
        return request_id
    
    def get_result(self, request_id: str, timeout: float = None) -> Optional[Dict]:
        """
        Get result of a task.
        
        Args:
            request_id: Request ID from execute_task
            timeout: Max seconds to wait (None = no wait)
        
        Returns:
            Result dict or None if not ready
        """
        if request_id in self.results:
            return self.results.pop(request_id)
        
        if timeout:
            start = time.time()
            while time.time() - start < timeout:
                if request_id in self.results:
                    return self.results.pop(request_id)
                time.sleep(0.1)
        
        return None
    
    def wait_result(self, request_id: str, timeout: float = 60.0) -> Dict:
        """
        Wait for task result.
        
        Args:
            request_id: Request ID
            timeout: Timeout in seconds
        
        Returns:
            Result dict
        
        Raises:
            TimeoutError if not complete
        """
        result = self.get_result(request_id, timeout=timeout)
        if result is None:
            raise TimeoutError(f"Task {request_id} timed out")
        return result
    
    # === Status ===
    
    def get_status(self) -> Dict:
        """Get cluster status."""
        return {
            "orchestrator": self.orchestrator.get_status(),
            "pending_requests": len(self.pending_requests),
            "completed_results": len(self.results),
        }
    
    # === Internal ===
    
    def _poll_loop(self):
        """Background poll for completed tasks."""
        while self.running:
            try:
                # Check for completed tasks in orchestrator
                for task_id, task in list(self.orchestrator.tasks.items()):
                    if task["status"] == "completed":
                        # Find matching request
                        for req_id, req in list(self.pending_requests.items()):
                            if req["task_id"] == task_id:
                                # Move to results
                                self.results[req_id] = {
                                    "task_id": task_id,
                                    "task_type": task["task_type"],
                                    "result": task["result"],
                                    "agent_id": task["assigned_to"],
                                    "completed_at": task["completed_at"].isoformat() if task["completed_at"] else None,
                                }
                                del self.pending_requests[req_id]
                                
                                if self.on_task_result:
                                    self.on_task_result(req_id, self.results[req_id])
                                
                                print(f"[OpenClaw-Coordinator] Task complete: {req_id}")
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"[OpenClaw-Coordinator] Poll error: {e}")
                time.sleep(1)


# === OpenClaw Integration Helper ===

def create_openclaw_coordinator() -> OpenClawCoordinator:
    """
    Create an OpenClaw coordinator ready for use.
    
    Usage in OpenClaw session:
        from coordinator.openclaw_integration import create_openclaw_coordinator
        
        coord = create_openclaw_coordinator()
        coord.start()
        
        # Submit task
        req_id = coord.execute_task("research", {"topic": "AI agents"})
        result = coord.wait_result(req_id)
    """
    coordinator = OpenClawCoordinator()
    return coordinator


# === CLI for testing ===

def main():
    """Test the OpenClaw coordinator."""
    print("=== OpenClaw Coordinator Test ===\n")
    
    coord = create_openclaw_coordinator()
    coord.start()
    
    # Register a test agent
    coord.register_agent(
        agent_id="test-nano-001",
        bot_type=BotType.NANOBOT,
        capabilities=["echo", "ping"],
    )
    
    # Submit a task
    req_id = coord.execute_task("echo", {"message": "Hello from OpenClaw!"})
    print(f"Submitted: {req_id}")
    
    # Simulate completion (in real scenario, agent would do this)
    # coord.orchestrator.complete_task(task_id, result, agent_id)
    
    # Check status
    status = coord.get_status()
    print(f"\nStatus: {json.dumps(status, indent=2, default=str)}")
    
    # Wait a bit
    time.sleep(2)
    
    coord.stop()
    print("\n=== Done ===")


if __name__ == "__main__":
    main()
