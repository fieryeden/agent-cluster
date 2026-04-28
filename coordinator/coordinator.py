#!/usr/bin/env python3
"""
Coordinator - Agent Cluster Orchestrator

The central brain that:
1. Manages agent registry (who's available, capabilities, load)
2. Routes tasks to appropriate agents
3. Dispatches research when capabilities missing
4. Monitors agent health via heartbeats
5. Aggregates results

Usage:
    python coordinator.py --cluster-dir ./cluster_data --port 8080
"""

import os
import sys
import json
import time
import uuid
import signal
import argparse
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

# Add protocol to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from protocol.messages import (
    MessageType, BaseMessage, MessageQueue, AgentCapability,
    capability_query, task_assign, research_request,
    create_message
)


@dataclass
class AgentInfo:
    """Information about a registered agent."""
    agent_id: str
    agent_type: str
    capabilities: Dict[str, AgentCapability]
    device_info: Dict[str, Any]
    last_heartbeat: datetime
    current_load: float = 0.0
    status: str = "active"
    assigned_tasks: List[str] = field(default_factory=list)


@dataclass
class TaskInfo:
    """Information about a task being processed."""
    task_id: str
    task_type: str
    task_data: Dict[str, Any]
    status: str  # pending, assigned, running, complete, failed
    assigned_to: Optional[str] = None
    result: Optional[Dict] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    priority: int = 0


class Coordinator:
    """
    The orchestration layer that manages agent clusters.
    """
    
    def __init__(self, cluster_dir: str, heartbeat_timeout: int = 90):
        self.cluster_dir = cluster_dir
        self.heartbeat_timeout = heartbeat_timeout  # seconds
        self.running = False
        
        # Agent registry
        self.agents: Dict[str, AgentInfo] = {}
        self.capability_index: Dict[str, Set[str]] = defaultdict(set)  # cap_name -> agent_ids
        
        # Task management
        self.tasks: Dict[str, TaskInfo] = {}
        self.pending_tasks: List[str] = []  # task_ids waiting for assignment
        
        # Message queue
        self.queue = MessageQueue(cluster_dir, "coordinator")
        
        # Capability query tracking
        self.pending_queries: Dict[str, Dict] = {}  # query_id -> {query, responses, task_id}
        
        # Ensure directories exist
        self._setup_directories()
        
        # Signal handlers
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def _setup_directories(self):
        """Create necessary directory structure."""
        dirs = [
            f"{self.cluster_dir}/coordinator/inbox",
            f"{self.cluster_dir}/coordinator/processed",
            f"{self.cluster_dir}/broadcast",
            f"{self.cluster_dir}/agents",
        ]
        for d in dirs:
            os.makedirs(d, exist_ok=True)
    
    def start(self):
        """Main coordinator loop."""
        print("[Coordinator] Starting...")
        print(f"[Coordinator] Cluster dir: {self.cluster_dir}")
        
        self.running = True
        
        while self.running:
            try:
                self._process_messages()
                self._check_timeouts()
                self._assign_pending_tasks()
                time.sleep(1)
            except Exception as e:
                print(f"[Coordinator] Error: {e}")
                time.sleep(5)
    
    def _process_messages(self):
        """Process incoming messages."""
        messages = self.queue.receive(include_broadcast=False)
        
        for filepath, msg in messages:
            self._handle_message(msg)
            self.queue.mark_processed(filepath)
    
    def _handle_message(self, msg: BaseMessage):
        """Route message to appropriate handler."""
        handler_map = {
            MessageType.HEARTBEAT: self._handle_heartbeat,
            MessageType.REGISTER: self._handle_register,
            MessageType.CAPABILITY_RESPONSE: self._handle_capability_response,
            MessageType.TASK_PROGRESS: self._handle_task_progress,
            MessageType.TASK_COMPLETE: self._handle_task_complete,
            MessageType.TASK_FAILED: self._handle_task_failed,
            MessageType.RESEARCH_RESULT: self._handle_research_result,
        }
        
        handler = handler_map.get(msg.msg_type)
        if handler:
            handler(msg)
        else:
            print(f"[Coordinator] Unknown message type: {msg.msg_type.value}")
    
    # === Agent Management ===
    
    def _handle_register(self, msg: BaseMessage):
        """Register a new agent."""
        agent_id = msg.sender_id
        agent_type = msg.payload.get('agent_type', 'unknown')
        capabilities = msg.payload.get('capabilities', [])
        device_info = msg.payload.get('device_info', {})
        
        # Build capability dict
        cap_dict = {}
        for cap_data in capabilities:
            cap = AgentCapability(
                name=cap_data['name'],
                confidence=cap_data.get('confidence', 0.8),
                metadata=cap_data.get('metadata')
            )
            cap_dict[cap.name] = cap
            # Index by capability
            self.capability_index[cap.name].add(agent_id)
        
        # Create agent info
        agent = AgentInfo(
            agent_id=agent_id,
            agent_type=agent_type,
            capabilities=cap_dict,
            device_info=device_info,
            last_heartbeat=datetime.utcnow()
        )
        
        self.agents[agent_id] = agent
        print(f"[Coordinator] Agent registered: {agent_id} ({agent_type})")
        print(f"[Coordinator]   Capabilities: {list(cap_dict.keys())}")
    
    def _handle_heartbeat(self, msg: BaseMessage):
        """Update agent heartbeat."""
        agent_id = msg.sender_id
        load = msg.payload.get('load', 0.0)
        status = msg.payload.get('status', 'active')
        
        if agent_id in self.agents:
            self.agents[agent_id].last_heartbeat = datetime.utcnow()
            self.agents[agent_id].current_load = load
            self.agents[agent_id].status = status
            # Silent heartbeat (only log issues)
    
    def _check_timeouts(self):
        """Check for timed-out agents."""
        now = datetime.utcnow()
        timeout_threshold = timedelta(seconds=self.heartbeat_timeout)
        
        dead_agents = []
        for agent_id, agent in self.agents.items():
            if now - agent.last_heartbeat > timeout_threshold:
                print(f"[Coordinator] Agent timeout: {agent_id}")
                dead_agents.append(agent_id)
        
        for agent_id in dead_agents:
            self._remove_agent(agent_id)
    
    def _remove_agent(self, agent_id: str):
        """Remove agent from registry."""
        if agent_id in self.agents:
            agent = self.agents.pop(agent_id)
            # Remove from capability index
            for cap_name in agent.capabilities:
                self.capability_index[cap_name].discard(agent_id)
            
            # Reassign any pending tasks
            for task_id in agent.assigned_tasks:
                if task_id in self.tasks:
                    self.tasks[task_id].status = "pending"
                    self.tasks[task_id].assigned_to = None
                    self.pending_tasks.append(task_id)
            
            print(f"[Coordinator] Removed agent: {agent_id}")
    
    # === Task Routing ===
    
    def submit_task(self, task_type: str, task_data: Dict, priority: int = 0) -> str:
        """
        Submit a task for execution.
        This is the main entry point for external systems.
        """
        task_id = f"task-{uuid.uuid4().hex[:8]}"
        
        task = TaskInfo(
            task_id=task_id,
            task_type=task_type,
            task_data=task_data,
            status="pending",
            priority=priority
        )
        
        self.tasks[task_id] = task
        self.pending_tasks.append(task_id)
        
        print(f"[Coordinator] Task submitted: {task_type} ({task_id})")
        return task_id
    
    def _assign_pending_tasks(self):
        """Try to assign pending tasks to available agents."""
        if not self.pending_tasks:
            return
        
        # Sort by priority (highest first)
        self.pending_tasks.sort(key=lambda t: self.tasks[t].priority, reverse=True)
        
        assigned = []
        for task_id in self.pending_tasks:
            task = self.tasks[task_id]
            
            # Find capable agent with lowest load
            agent_id = self._find_agent_for_task(task.task_type)
            
            if agent_id:
                self._assign_task(task_id, agent_id)
                assigned.append(task_id)
        
        # Remove assigned tasks from pending
        for task_id in assigned:
            self.pending_tasks.remove(task_id)
    
    def _find_agent_for_task(self, task_type: str) -> Optional[str]:
        """Find best agent for a task type."""
        candidates = []
        
        # Check capability index
        for agent_id in self.capability_index.get(task_type, set()):
            if agent_id in self.agents:
                agent = self.agents[agent_id]
                if agent.status == "active" and agent.current_load < 1.0:
                    candidates.append((agent_id, agent.current_load))
        
        # Also check by task type matching capability names
        for agent_id, agent in self.agents.items():
            if agent.status != "active" or agent.current_load >= 1.0:
                continue
            for cap_name in agent.capabilities:
                if task_type.lower() in cap_name.lower():
                    candidates.append((agent_id, agent.current_load))
                    break
        
        if candidates:
            # Sort by load (lowest first)
            candidates.sort(key=lambda x: x[1])
            return candidates[0][0]
        
        return None
    
    def _assign_task(self, task_id: str, agent_id: str):
        """Assign task to agent."""
        task = self.tasks[task_id]
        agent = self.agents[agent_id]
        
        task.status = "assigned"
        task.assigned_to = agent_id
        agent.assigned_tasks.append(task_id)
        
        # Send task assignment
        msg = task_assign(
            task_id=task_id,
            agent_id=agent_id,
            task_type=task.task_type,
            task_data=task.task_data,
            priority=task.priority
        )
        self.queue.send(msg)
        
        print(f"[Coordinator] Assigned {task_id} to {agent_id}")
    
    # === Task Lifecycle ===
    
    def _handle_task_progress(self, msg: BaseMessage):
        """Handle task progress update."""
        task_id = msg.payload.get('task_id')
        progress = msg.payload.get('progress')
        status = msg.payload.get('status')
        
        if task_id in self.tasks:
            print(f"[Coordinator] Progress: {task_id} - {progress*100:.0f}% ({status})")
    
    def _handle_task_complete(self, msg: BaseMessage):
        """Handle task completion."""
        task_id = msg.payload.get('task_id')
        result = msg.payload.get('result')
        
        if task_id in self.tasks:
            task = self.tasks[task_id]
            task.status = "complete"
            task.result = result
            
            # Remove from agent's assigned tasks
            if task.assigned_to and task.assigned_to in self.agents:
                self.agents[task.assigned_to].assigned_tasks.remove(task_id)
            
            print(f"[Coordinator] Task complete: {task_id}")
            print(f"[Coordinator] Result: {json.dumps(result, indent=2)[:200]}")
    
    def _handle_task_failed(self, msg: BaseMessage):
        """Handle task failure."""
        task_id = msg.payload.get('task_id')
        reason = msg.payload.get('reason')
        
        if task_id in self.tasks:
            task = self.tasks[task_id]
            task.status = "failed"
            task.result = {"error": reason}
            
            # Remove from agent
            if task.assigned_to and task.assigned_to in self.agents:
                self.agents[task.assigned_to].assigned_tasks.remove(task_id)
            
            print(f"[Coordinator] Task failed: {task_id} - {reason}")
            
            # TODO: Could retry or reassign here
    
    # === Capability Discovery ===
    
    def _handle_capability_response(self, msg: BaseMessage):
        """Handle capability query response."""
        query_id = msg.payload.get('query_id')
        can_handle = msg.payload.get('can_handle')
        confidence = msg.payload.get('confidence', 0.0)
        
        if query_id in self.pending_queries:
            query_info = self.pending_queries[query_id]
            query_info['responses'].append({
                'agent_id': msg.sender_id,
                'can_handle': can_handle,
                'confidence': confidence
            })
            
            # Check if all agents responded
            if len(query_info['responses']) >= len(self.agents):
                self._resolve_capability_query(query_id)
    
    def query_capability(self, query: str, task_id: Optional[str] = None) -> str:
        """Broadcast capability query to all agents."""
        query_id = str(uuid.uuid4())
        
        self.pending_queries[query_id] = {
            'query': query,
            'responses': [],
            'task_id': task_id
        }
        
        msg = capability_query(query, {'query_id': query_id})
        self.queue.send(msg)
        
        print(f"[Coordinator] Capability query: {query}")
        return query_id
    
    def _resolve_capability_query(self, query_id: str):
        """Process capability query results and assign task."""
        query_info = self.pending_queries.pop(query_id)
        responses = query_info['responses']
        
        # Find best agent
        valid_responses = [r for r in responses if r['can_handle']]
        
        if valid_responses:
            # Sort by confidence
            valid_responses.sort(key=lambda x: x['confidence'], reverse=True)
            best = valid_responses[0]
            
            task_id = query_info.get('task_id')
            if task_id and task_id in self.tasks:
                self._assign_task(task_id, best['agent_id'])
        else:
            # No agent can handle - dispatch research
            print(f"[Coordinator] No agent for: {query_info['query']}")
            self._dispatch_research(query_info['query'])
    
    # === Research Dispatch ===
    
    def _dispatch_research(self, topic: str):
        """Dispatch research request to research agent."""
        request_id = str(uuid.uuid4())
        
        msg = research_request(
            request_id=request_id,
            topic=topic
        )
        self.queue.send(msg)
        
        print(f"[Coordinator] Research dispatched: {topic}")
    
    def _handle_research_result(self, msg: BaseMessage):
        """Handle research result from research agent."""
        request_id = msg.payload.get('request_id')
        findings = msg.payload.get('findings', {})
        tools_recommended = msg.payload.get('tools_recommended', [])
        
        print(f"[Coordinator] Research result: {request_id}")
        print(f"[Coordinator] Findings: {findings}")
        print(f"[Coordinator] Tools recommended: {tools_recommended}")
        
        # TODO: Install tools across cluster
        # TODO: Update agent capabilities
    
    def _shutdown(self, signum, frame):
        """Graceful shutdown."""
        print("\n[Coordinator] Shutting down...")
        self.running = False
    
    # === API Methods ===
    
    def get_status(self) -> Dict:
        """Get cluster status summary."""
        return {
            "agents": {
                agent_id: {
                    "type": agent.agent_type,
                    "status": agent.status,
                    "load": agent.current_load,
                    "capabilities": list(agent.capabilities.keys()),
                    "last_heartbeat": agent.last_heartbeat.isoformat()
                }
                for agent_id, agent in self.agents.items()
            },
            "tasks": {
                task_id: {
                    "type": task.task_type,
                    "status": task.status,
                    "assigned_to": task.assigned_to
                }
                for task_id, task in self.tasks.items()
            },
            "pending_tasks": len(self.pending_tasks),
            "capability_index": {
                k: list(v) for k, v in self.capability_index.items()
            }
        }


def main():
    parser = argparse.ArgumentParser(description='Agent Cluster Coordinator')
    parser.add_argument('--cluster-dir', required=True, help='Cluster data directory')
    parser.add_argument('--heartbeat-timeout', type=int, default=90, help='Agent timeout in seconds')
    
    args = parser.parse_args()
    
    coordinator = Coordinator(
        cluster_dir=args.cluster_dir,
        heartbeat_timeout=args.heartbeat_timeout
    )
    coordinator.start()


if __name__ == '__main__':
    main()
