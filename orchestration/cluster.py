#!/usr/bin/env python3
"""
Cluster Orchestrator

Unified orchestration for heterogeneous agent clusters.
Supports multiple bot types and connection methods.
"""

import json
import threading
import time
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import BaseMessage, MessageType, create_message
from capabilities.registry import CapabilityRegistry
from network.transport import NetworkServer, NetworkClient


class BotType(Enum):
    """Supported bot types."""
    NANOBOT = "nanobot"         # Python nano-bot (original)
    OPENCLAW = "openclaw"       # OpenClaw agent
    EXTENSION = "extension"     # Browser extension agent
    CUSTOM = "custom"           # Custom agent type


@dataclass
class BotConfig:
    """Configuration for a bot instance."""
    bot_type: BotType
    agent_id: str
    capabilities: List[str] = field(default_factory=list)
    
    # Connection settings
    connection_type: str = "file"  # file | tcp | websocket
    address: Optional[str] = None  # host:port for network
    
    # Bot-specific config
    config: Dict[str, Any] = field(default_factory=dict)
    
    # Limits
    max_concurrent_tasks: int = 3
    heartbeat_interval: int = 30


@dataclass
class AgentConnection:
    """Active connection to an agent."""
    agent_id: str
    bot_type: BotType
    connection: Any  # NetworkClient, file path, etc.
    capabilities: List[str]
    status: str = "active"
    last_heartbeat: datetime = field(default_factory=datetime.now)
    current_tasks: List[str] = field(default_factory=list)
    total_tasks_completed: int = 0
    total_tasks_failed: int = 0
    
    def to_dict(self) -> Dict:
        return {
            "agent_id": self.agent_id,
            "bot_type": self.bot_type.value,
            "capabilities": self.capabilities,
            "status": self.status,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "current_tasks": len(self.current_tasks),
            "completed": self.total_tasks_completed,
            "failed": self.total_tasks_failed,
        }


class ClusterOrchestrator:
    """
    Unified orchestrator for heterogeneous agent clusters.
    
    Manages multiple bot types:
    - NanoBot: Lightweight Python agents (file/network)
    - OpenClaw: Full OpenClaw agents (network)
    - Extension: Browser-based agents (websocket)
    - Custom: User-defined agent types
    
    Features:
    - Unified message routing across bot types
    - Capability-based task assignment
    - Health monitoring for all agent types
    - Automatic failover and rebalancing
    """
    
    def __init__(self, config_dir: str = None):
        """
        Initialize cluster orchestrator.
        
        Args:
            config_dir: Directory for config files
        """
        self.config_dir = config_dir or "/tmp/cluster_orchestrator"
        
        # Agent registry
        self.agents: Dict[str, AgentConnection] = {}
        self.bot_configs: Dict[str, BotConfig] = {}
        
        # Capability tracking
        self.capability_registry = CapabilityRegistry()
        self.capability_agents: Dict[str, List[str]] = {}  # cap -> agent_ids
        
        # Task management
        self.tasks: Dict[str, Dict] = {}
        self.pending_tasks: List[str] = []
        
        # Network (optional)
        self.network_server: Optional[NetworkServer] = None
        self.network_port: int = 7890
        
        # State
        self.running = False
        self._lock = threading.Lock()
        
        # Callbacks
        self.on_agent_connect: Optional[Callable] = None
        self.on_agent_disconnect: Optional[Callable] = None
        self.on_task_complete: Optional[Callable] = None
    
    # === Agent Management ===
    
    def register_agent(self, config: BotConfig) -> bool:
        """
        Register a new agent.
        
        Args:
            config: Bot configuration
        
        Returns:
            True if registered successfully
        """
        with self._lock:
            agent_id = config.agent_id
            
            if agent_id in self.agents:
                print(f"[Orchestrator] Agent {agent_id} already registered")
                return False
            
            # Create connection
            conn = AgentConnection(
                agent_id=agent_id,
                bot_type=config.bot_type,
                connection=None,  # Set by connection method
                capabilities=config.capabilities,
            )
            
            self.agents[agent_id] = conn
            self.bot_configs[agent_id] = config
            
            # Update capability index
            for cap in config.capabilities:
                self.capability_registry.register_capability(agent_id, cap, confidence=1.0)
                if cap not in self.capability_agents:
                    self.capability_agents[cap] = []
                self.capability_agents[cap].append(agent_id)
            
            print(f"[Orchestrator] Registered {config.bot_type.value}: {agent_id}")
            print(f"[Orchestrator] Capabilities: {config.capabilities}")
            
            if self.on_agent_connect:
                self.on_agent_connect(agent_id, config)
            
            return True
    
    def unregister_agent(self, agent_id: str):
        """Unregister an agent."""
        with self._lock:
            if agent_id not in self.agents:
                return
            
            agent = self.agents[agent_id]
            
            # Remove from capability index
            for cap in agent.capabilities:
                if cap in self.capability_agents:
                    if agent_id in self.capability_agents[cap]:
                        self.capability_agents[cap].remove(agent_id)
            
            # Reassign tasks
            for task_id in agent.current_tasks:
                self.pending_tasks.append(task_id)
            
            del self.agents[agent_id]
            del self.bot_configs[agent_id]
            
            print(f"[Orchestrator] Unregistered: {agent_id}")
            
            if self.on_agent_disconnect:
                self.on_agent_disconnect(agent_id)
    
    def get_agent(self, agent_id: str) -> Optional[AgentConnection]:
        """Get agent by ID."""
        return self.agents.get(agent_id)
    
    def list_agents(self, bot_type: BotType = None) -> List[AgentConnection]:
        """List all agents, optionally filtered by type."""
        agents = list(self.agents.values())
        if bot_type:
            agents = [a for a in agents if a.bot_type == bot_type]
        return agents
    
    # === Task Management ===
    
    def submit_task(
        self,
        task_type: str,
        params: Dict[str, Any],
        target_agent: str = None,
        priority: int = 0,
    ) -> str:
        """
        Submit a task for execution.
        
        Args:
            task_type: Type of task (capability name)
            params: Task parameters
            target_agent: Specific agent (or None for auto-assign)
            priority: Task priority (higher = more important)
        
        Returns:
            Task ID
        """
        import uuid
        
        task_id = f"task-{uuid.uuid4().hex[:8]}"
        
        task = {
            "task_id": task_id,
            "task_type": task_type,
            "params": params,
            "target_agent": target_agent,
            "priority": priority,
            "status": "pending",
            "assigned_to": None,
            "created_at": datetime.now(),
            "started_at": None,
            "completed_at": None,
            "result": None,
            "error": None,
        }
        
        with self._lock:
            self.tasks[task_id] = task
            self.pending_tasks.append(task_id)
        
        print(f"[Orchestrator] Task submitted: {task_type} ({task_id})")
        
        return task_id
    
    def assign_tasks(self) -> List[str]:
        """
        Assign pending tasks to available agents.
        
        Returns:
            List of assigned task IDs
        """
        assigned = []
        
        with self._lock:
            # Sort pending by priority (descending)
            pending = sorted(
                self.pending_tasks,
                key=lambda t: self.tasks[t]["priority"],
                reverse=True
            )
            
            for task_id in pending:
                task = self.tasks[task_id]
                
                # Find best agent
                agent_id = self._find_best_agent(
                    task["task_type"],
                    task["target_agent"],
                )
                
                if agent_id:
                    # Assign
                    task["status"] = "assigned"
                    task["assigned_to"] = agent_id
                    task["started_at"] = datetime.now()
                    
                    self.agents[agent_id].current_tasks.append(task_id)
                    assigned.append(task_id)
                    
                    print(f"[Orchestrator] Assigned {task_id} → {agent_id}")
        
        # Remove assigned from pending
        for task_id in assigned:
            if task_id in self.pending_tasks:
                self.pending_tasks.remove(task_id)
        
        return assigned
    
    def _find_best_agent(
        self,
        capability: str,
        target_agent: str = None,
    ) -> Optional[str]:
        """Find best agent for a capability."""
        # Target agent specified
        if target_agent:
            if target_agent in self.agents:
                agent = self.agents[target_agent]
                if len(agent.current_tasks) < self.bot_configs[target_agent].max_concurrent_tasks:
                    return target_agent
            return None
        
        # Find by capability
        if capability not in self.capability_agents:
            return None
        
        candidates = self.capability_agents[capability]
        
        # Score by load
        best = None
        best_score = float('inf')
        
        for agent_id in candidates:
            if agent_id not in self.agents:
                continue
            
            agent = self.agents[agent_id]
            config = self.bot_configs[agent_id]
            
            # Check capacity
            if len(agent.current_tasks) >= config.max_concurrent_tasks:
                continue
            
            # Score: fewer tasks = better
            score = len(agent.current_tasks)
            
            if score < best_score:
                best_score = score
                best = agent_id
        
        return best
    
    def complete_task(self, task_id: str, result: Any, agent_id: str):
        """Mark task as complete."""
        with self._lock:
            if task_id not in self.tasks:
                return
            
            task = self.tasks[task_id]
            task["status"] = "completed"
            task["completed_at"] = datetime.now()
            task["result"] = result
            
            if agent_id in self.agents:
                agent = self.agents[agent_id]
                if task_id in agent.current_tasks:
                    agent.current_tasks.remove(task_id)
                agent.total_tasks_completed += 1
        
        print(f"[Orchestrator] Task completed: {task_id}")
        
        if self.on_task_complete:
            self.on_task_complete(task_id, result)
    
    def fail_task(self, task_id: str, error: str, agent_id: str):
        """Mark task as failed."""
        with self._lock:
            if task_id not in self.tasks:
                return
            
            task = self.tasks[task_id]
            task["status"] = "failed"
            task["completed_at"] = datetime.now()
            task["error"] = error
            
            if agent_id in self.agents:
                agent = self.agents[agent_id]
                if task_id in agent.current_tasks:
                    agent.current_tasks.remove(task_id)
                agent.total_tasks_failed += 1
            
            # Requeue for retry
            self.pending_tasks.append(task_id)
        
        print(f"[Orchestrator] Task failed: {task_id} - {error}")
    
    # === Health Monitoring ===
    
    def update_heartbeat(self, agent_id: str, status: str = "active"):
        """Update agent heartbeat."""
        with self._lock:
            if agent_id not in self.agents:
                return
            
            self.agents[agent_id].last_heartbeat = datetime.now()
            self.agents[agent_id].status = status
    
    def check_health(self, timeout: int = 90) -> List[str]:
        """
        Check agent health.
        
        Args:
            timeout: Seconds without heartbeat = unhealthy
        
        Returns:
            List of unhealthy agent IDs
        """
        unhealthy = []
        now = datetime.now()
        
        with self._lock:
            for agent_id, agent in self.agents.items():
                age = (now - agent.last_heartbeat).total_seconds()
                if age > timeout:
                    unhealthy.append(agent_id)
        
        return unhealthy
    
    # === Status ===
    
    def get_status(self) -> Dict:
        """Get cluster status."""
        with self._lock:
            return {
                "agents": {
                    aid: agent.to_dict()
                    for aid, agent in self.agents.items()
                },
                "tasks": {
                    "pending": len(self.pending_tasks),
                    "total": len(self.tasks),
                },
                "capabilities": dict(self.capability_agents),
                "by_type": {
                    bot.value: len([a for a in self.agents.values() if a.bot_type == bot])
                    for bot in BotType
                },
            }
    
    # === Lifecycle ===
    
    def start(self):
        """Start the orchestrator."""
        self.running = True
        print("[Orchestrator] Started")
    
    def stop(self):
        """Stop the orchestrator."""
        self.running = False
        print("[Orchestrator] Stopped")


# Convenience functions

def create_orchestrator(network_port: int = None) -> ClusterOrchestrator:
    """Create a cluster orchestrator."""
    orchestrator = ClusterOrchestrator()
    if network_port:
        orchestrator.network_port = network_port
    return orchestrator
