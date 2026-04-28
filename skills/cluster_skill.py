#!/usr/bin/env python3
"""
Cluster Skill for OpenClaw

Allows OpenClaw to manage and use the agent cluster.
"""

from typing import Dict, List, Optional, Any
import sys
from pathlib import Path

# Add project to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from orchestration import BotType
from coordinator.openclaw_integration import OpenClawCoordinator


# Global coordinator instance
_coordinator: Optional[OpenClawCoordinator] = None


def get_coordinator() -> OpenClawCoordinator:
    """Get or create the coordinator."""
    global _coordinator
    if _coordinator is None:
        _coordinator = OpenClawCoordinator()
        _coordinator.start()
    return _coordinator


def cluster_status() -> Dict:
    """
    Get cluster status.
    
    Returns:
        Status dict with agents, tasks, and capabilities
    """
    coord = get_coordinator()
    return coord.get_status()


def list_agents() -> List[Dict]:
    """
    List all registered agents.
    
    Returns:
        List of agent info dicts
    """
    coord = get_coordinator()
    return coord.get_agents()


def register_agent(
    agent_id: str,
    bot_type: str = "nanobot",
    capabilities: List[str] = None,
) -> bool:
    """
    Register an agent with the cluster.
    
    Args:
        agent_id: Unique agent identifier
        bot_type: Type (nanobot, openclaw, extension, custom)
        capabilities: List of capabilities
    
    Returns:
        True if registered successfully
    """
    coord = get_coordinator()
    
    # Map string to enum
    type_map = {
        "nanobot": BotType.NANOBOT,
        "openclaw": BotType.OPENCLAW,
        "extension": BotType.EXTENSION,
        "custom": BotType.CUSTOM,
    }
    
    bot_type_enum = type_map.get(bot_type.lower(), BotType.CUSTOM)
    
    return coord.register_agent(
        agent_id=agent_id,
        bot_type=bot_type_enum,
        capabilities=capabilities or [],
    )


def submit_task(
    task_type: str,
    params: Dict,
    target_agent: str = None,
    priority: int = 0,
) -> str:
    """
    Submit a task for execution.
    
    Args:
        task_type: Type of task (capability name)
        params: Task parameters
        target_agent: Specific agent (or None for auto-assign)
        priority: Task priority
    
    Returns:
        Request ID
    """
    coord = get_coordinator()
    return coord.execute_task(
        task_type=task_type,
        params=params,
        target_agent=target_agent,
        priority=priority,
    )


def get_task_result(request_id: str, timeout: float = 60.0) -> Optional[Dict]:
    """
    Get result of a submitted task.
    
    Args:
        request_id: Request ID from submit_task
        timeout: Seconds to wait (None = no wait)
    
    Returns:
        Result dict or None if not ready
    """
    coord = get_coordinator()
    return coord.wait_result(request_id, timeout=timeout)


def execute_on_cluster(
    task_type: str,
    params: Dict,
    timeout: float = 60.0,
) -> Dict:
    """
    Execute a task and wait for result.
    
    Args:
        task_type: Type of task
        params: Task parameters
        timeout: Timeout in seconds
    
    Returns:
        Result dict
    
    Raises:
        TimeoutError if task times out
    """
    coord = get_coordinator()
    request_id = coord.execute_task(task_type, params)
    return coord.wait_result(request_id, timeout=timeout)


# === Tool Functions for OpenClaw ===

def cluster_dispatch(
    task_type: str,
    params: Dict,
    wait: bool = True,
    timeout: float = 60.0,
) -> Dict:
    """
    Dispatch a task to the agent cluster.
    
    OpenClaw tool function.
    
    Args:
        task_type: Type of task (capability name)
        params: Task parameters
        wait: Wait for result
        timeout: Timeout if waiting
    
    Returns:
        Result dict with task_id and optionally result
    """
    request_id = submit_task(task_type, params)
    
    if wait:
        try:
            result = get_task_result(request_id, timeout=timeout)
            return {
                "success": True,
                "request_id": request_id,
                "result": result,
            }
        except TimeoutError:
            return {
                "success": False,
                "request_id": request_id,
                "error": "timeout",
            }
    else:
        return {
            "success": True,
            "request_id": request_id,
            "status": "submitted",
        }


# Example usage
if __name__ == "__main__":
    print("=== Cluster Skill Test ===\n")
    
    # Register agent
    register_agent("test-agent-001", "nanobot", ["echo", "ping"])
    
    # Check status
    status = cluster_status()
    print(f"Agents: {len(status['orchestrator']['agents'])}")
    
    # Submit task (won't complete without actual agent running)
    req_id = submit_task("echo", {"message": "test"})
    print(f"Submitted: {req_id}")
    
    print("\n=== Done ===")
