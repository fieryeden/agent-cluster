#!/usr/bin/env python3
"""
Coordinator Server - Orchestration Layer
Manages distributed agent clusters, assigns tasks, aggregates results.

Core responsibilities:
1. Maintain agent registry
2. Route tasks to appropriate agents
3. Monitor agent health (heartbeats)
4. Aggregate and report results
"""

import json
import time
import uuid
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set

# Configuration
DEFAULT_SHARED_DIR = "/tmp/agent_cluster"
HEARTBEAT_TIMEOUT = 30  # seconds


def _now_iso() -> str:
    """Return current UTC time as ISO string (timezone-aware)."""
    return datetime.now(timezone.utc).isoformat()


class AgentInfo:
    """Information about a registered agent."""

    def __init__(self, agent_id: str, capabilities: Dict[str, float] = None,
                 status: str = "unknown", last_heartbeat: float = None):
        self.agent_id = agent_id
        self.capabilities: Dict[str, float] = capabilities or {}
        self.load: float = 0.0
        self.last_heartbeat: float = last_heartbeat or time.time()
        self.status: str = status
        self.inbox_dir: Optional[Path] = None
        self.outbox_dir: Optional[Path] = None
        self.delivery_method: str = "file"  # "file" or "http"

    def is_alive(self) -> bool:
        """Check if agent is still responding."""
        return time.time() - self.last_heartbeat < HEARTBEAT_TIMEOUT

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for reporting."""
        return {
            "agent_id": self.agent_id,
            "capabilities": self.capabilities,
            "load": self.load,
            "last_heartbeat": self.last_heartbeat,
            "status": self.status,
            "is_alive": self.is_alive(),
            "delivery_method": self.delivery_method
        }


class Coordinator:
    """Central coordinator for agent cluster."""

    def __init__(self, shared_dir: str = DEFAULT_SHARED_DIR):
        self.shared_dir = Path(shared_dir)
        self.agents: Dict[str, AgentInfo] = {}
        self.running = True

        # Setup directories
        self.agents_dir = self.shared_dir / "agents"
        self.tasks_dir = self.shared_dir / "tasks"
        self.results_dir = self.shared_dir / "results"
        for d in [self.agents_dir, self.tasks_dir, self.results_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Task tracking
        self.pending_tasks: Dict[str, Dict[str, Any]] = {}
        self.active_tasks: Dict[str, Dict[str, Any]] = {}
        self.completed_tasks: Dict[str, Dict[str, Any]] = {}

        print(f"[INFO] Coordinator initialized at {self.shared_dir}")

    def scan_for_agents(self) -> List[str]:
        """Scan shared directory for registered agents."""
        found = []
        if self.agents_dir.exists():
            for agent_dir in self.agents_dir.iterdir():
                if agent_dir.is_dir():
                    agent_id = agent_dir.name
                    if agent_id not in self.agents:
                        self.agents[agent_id] = AgentInfo(agent_id)
                        self.agents[agent_id].inbox_dir = agent_dir / "inbox"
                        self.agents[agent_id].outbox_dir = agent_dir / "outbox"
                        self.agents[agent_id].status = "discovered"
                        print(f"[INFO] Discovered agent: {agent_id}")
                    found.append(agent_id)
        return found

    def check_agent_messages(self, agent_id: str) -> List[Dict[str, Any]]:
        """Check outbox of specific agent for messages."""
        if agent_id not in self.agents:
            return []
        agent = self.agents[agent_id]
        if not agent.outbox_dir or not agent.outbox_dir.exists():
            return []
        messages = []
        for msg_file in agent.outbox_dir.glob("*.json"):
            try:
                with open(msg_file) as f:
                    msg = json.load(f)
                messages.append(msg)
                msg_file.unlink()
            except Exception as e:
                print(f"[WARN] Failed to read {msg_file}: {e}")
        return messages

    def process_agent_message(self, msg: Dict[str, Any], agent_id: str):
        """Process message from agent."""
        msg_type = msg.get("type", "")
        data = msg.get("data", {})

        if agent_id not in self.agents:
            print(f"[WARN] Unknown agent: {agent_id}")
            return

        agent = self.agents[agent_id]

        if msg_type == "registration":
            agent.capabilities = data.get("capabilities", {})
            agent.status = "ready"
            print(f"[INFO] Agent {agent_id} registered with capabilities: {list(agent.capabilities.keys())}")
        elif msg_type == "heartbeat":
            agent.last_heartbeat = time.time()
            agent.load = data.get("load", 0.0)
            agent.capabilities = data.get("capabilities", agent.capabilities)
            agent.status = "alive"
        elif msg_type == "shutdown":
            agent.status = "offline"
            print(f"[INFO] Agent {agent_id} shutting down")
        elif msg_type.endswith("_response"):
            request_id = data.get("request_id")
            response = data.get("response", {})
            if request_id in self.pending_tasks:
                task = self.pending_tasks.pop(request_id)
                task["response"] = response
                task["completed_at"] = _now_iso()
                self.completed_tasks[request_id] = task
                print(f"[INFO] Task {request_id} completed by {agent_id}")
            elif request_id in self.active_tasks:
                task = self.active_tasks.pop(request_id)
                task["response"] = response
                task["completed_at"] = _now_iso()
                self.completed_tasks[request_id] = task
                print(f"[INFO] Task {request_id} completed by {agent_id}")
            else:
                print(f"[DEBUG] Unknown request_id: {request_id}")
        else:
            print(f"[DEBUG] Unknown message type: {msg_type}")

    def send_to_agent(self, agent_id: str, msg_type: str, data: Dict[str, Any]) -> str:
        """Send message to specific agent."""
        if agent_id not in self.agents:
            raise ValueError(f"Unknown agent: {agent_id}")
        agent = self.agents[agent_id]
        if not agent.inbox_dir:
            raise ValueError(f"Agent {agent_id} has no inbox")
        msg_id = f"msg-{uuid.uuid4().hex[:8]}"
        message = {
            "id": msg_id,
            "type": msg_type,
            "coordinator_id": "coordinator",
            "timestamp": _now_iso(),
            "data": data
        }
        msg_file = agent.inbox_dir / f"{msg_id}.json"
        with open(msg_file, "w") as f:
            json.dump(message, f, indent=2)
        return msg_id

    def query_capabilities(self, capability: str) -> List[Dict[str, Any]]:
        """Query all agents for a specific capability."""
        results = []
        for agent_id, agent in self.agents.items():
            if not agent.is_alive():
                continue
            confidence = agent.capabilities.get(capability, 0.0)
            if confidence > 0:
                results.append({
                    "agent_id": agent_id,
                    "confidence": confidence,
                    "load": agent.load
                })
        results.sort(key=lambda x: (-x["confidence"], x["load"]))
        return results

    def assign_task(self, capability: str, task_data: Dict[str, Any],
                    agent_id: Optional[str] = None) -> str:
        """Assign task to best available agent (or specific agent)."""
        if not agent_id:
            candidates = self.query_capabilities(capability)
            if not candidates:
                raise ValueError(f"No agents with capability: {capability}")
            agent_id = candidates[0]["agent_id"]

        task_id = f"task-{uuid.uuid4().hex[:8]}"
        task_msg = {
            "task_id": task_id,
            "capability": capability,
            "params": task_data
        }

        # Try file-based delivery first
        try:
            self.send_to_agent(agent_id, "task_assign", task_msg)
        except ValueError:
            pass  # HTTP-only agent - store in pending for agent to poll

        self.pending_tasks[task_id] = {
            "task_id": task_id,
            "capability": capability,
            "agent_id": agent_id,
            "params": task_data,
            "assigned_at": _now_iso()
        }
        print(f"[INFO] Task {task_id} assigned to {agent_id} ({capability})")
        return task_id

    def claim_task(self, task_id: str, agent_id: str) -> Optional[Dict[str, Any]]:
        """Agent claims a pending task, moving it to active."""
        if task_id in self.active_tasks:
            return {"status": "already_claimed", "task": self.active_tasks[task_id]}
        if task_id not in self.pending_tasks:
            return None
        task = self.pending_tasks.pop(task_id)
        task["status"] = "claimed"
        task["claimed_by"] = agent_id
        task["claimed_at"] = _now_iso()
        self.active_tasks[task_id] = task
        return task

    def complete_task(self, task_id: str, agent_id: str,
                      result_status: str = "completed",
                      result_data: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Move task from active (or pending) to completed with result."""
        task = None
        if task_id in self.active_tasks:
            task = self.active_tasks.pop(task_id)
        elif task_id in self.pending_tasks:
            task = self.pending_tasks.pop(task_id)
        if not task:
            return None
        task["status"] = result_status
        task["completed_by"] = agent_id
        task["completed_at"] = _now_iso()
        task["result"] = result_data or {}
        self.completed_tasks[task_id] = task
        return task

    def get_task_result(self, task_id: str, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """Wait for task result with timeout."""
        start = time.time()
        while time.time() - start < timeout:
            if task_id in self.completed_tasks:
                return self.completed_tasks.pop(task_id)
            time.sleep(0.5)
        return None

    def check_agent_health(self):
        """Check health of all agents and update status."""
        now = time.time()
        for agent_id, agent in self.agents.items():
            if agent.is_alive():
                if agent.status == "offline":
                    agent.status = "recovered"
                    print(f"[INFO] Agent {agent_id} recovered")
            else:
                if agent.status not in ["offline", "dead"]:
                    agent.status = "dead"
                    print(f"[WARN] Agent {agent_id} not responding "
                          f"(last heartbeat {int(now - agent.last_heartbeat)}s ago)")

    def get_cluster_status(self) -> Dict[str, Any]:
        """Get status of entire cluster."""
        self.check_agent_health()
        alive_count = sum(1 for a in self.agents.values() if a.is_alive())
        return {
            "total_agents": len(self.agents),
            "alive_agents": alive_count,
            "dead_agents": len(self.agents) - alive_count,
            "pending_tasks": len(self.pending_tasks),
            "active_tasks": len(self.active_tasks),
            "completed_tasks": len(self.completed_tasks),
            "agents": [a.to_dict() for a in self.agents.values()]
        }

    def run(self, port: int = None):
        """Main coordinator loop."""
        print(f"[INFO] Coordinator starting...")
        if port:
            self._start_http_server(port)
        last_scan = 0
        while self.running:
            if time.time() - last_scan > 5:
                self.scan_for_agents()
                last_scan = time.time()
            for agent_id in list(self.agents.keys()):
                messages = self.check_agent_messages(agent_id)
                for msg in messages:
                    self.process_agent_message(msg, agent_id)
            self.check_agent_health()
            time.sleep(1)

    def register_agent_http(self, agent_id: str, capabilities=None):
        """Register an agent via HTTP API."""
        caps_dict = {}
        if capabilities:
            if isinstance(capabilities, dict):
                caps_dict = capabilities
            elif isinstance(capabilities, list):
                for c in capabilities:
                    if isinstance(c, dict):
                        caps_dict[c.get('name', 'unknown')] = c.get('confidence', 1.0)
                    else:
                        caps_dict[str(c)] = 1.0
        agent = AgentInfo(
            agent_id=agent_id,
            capabilities=caps_dict,
            status="alive",
            last_heartbeat=time.time(),
        )
        agent.delivery_method = "http"
        self.agents[agent_id] = agent
        print(f"[INFO] Agent {agent_id} registered via HTTP with capabilities: {list(caps_dict.keys())}")
        return agent.to_dict()

    def _start_http_server(self, port: int):
        """Start HTTP API server for network-based agents."""
        try:
            from http.server import HTTPServer, BaseHTTPRequestHandler

            coord_ref = self

            class CoordinatorHTTPHandler(BaseHTTPRequestHandler):
                def log_message(self, format, *args):
                    pass

                def do_GET(self):
                    path = self.path.rstrip('/')
                    if path in ('/status', '/api/status'):
                        self._json(coord_ref.get_cluster_status())
                    elif path in ('/agents', '/api/agents'):
                        self._json({"agents": [a.to_dict() for a in coord_ref.agents.values()]})
                    elif path in ('/tasks', '/api/tasks'):
                        self._json({
                            "pending": coord_ref.pending_tasks,
                            "active_count": len(coord_ref.active_tasks),
                            "completed_count": len(coord_ref.completed_tasks)
                        })
                    elif path in ('/tasks/pending', '/api/tasks/pending'):
                        self._json({"tasks": list(coord_ref.pending_tasks.values())})
                    elif path in ('/tasks/active', '/api/tasks/active'):
                        self._json({"tasks": list(coord_ref.active_tasks.values())})
                    elif path in ('/tasks/completed', '/api/tasks/completed'):
                        self._json({"tasks": list(coord_ref.completed_tasks.values())})
                    else:
                        self._json({"status": "ok", "coordinator": "agent-cluster", "version": "0.9.0"})

                def do_POST(self):
                    path = self.path.rstrip('/')
                    body = self._read_body()

                    if path in ('/register', '/api/register'):
                        agent_id = body.get('agent_id', f'agent-{uuid.uuid4().hex[:8]}')
                        result = coord_ref.register_agent_http(agent_id, body.get('capabilities'))
                        self._json(result)

                    elif path in ('/heartbeat', '/api/heartbeat'):
                        agent_id = body.get('agent_id')
                        if agent_id in coord_ref.agents:
                            coord_ref.agents[agent_id].last_heartbeat = time.time()
                            coord_ref.agents[agent_id].load = body.get('load', 0.0)
                            coord_ref.agents[agent_id].status = "alive"
                            self._json({"status": "ok"})
                        else:
                            self._json({"error": "unknown agent"}, 404)

                    elif path in ('/unregister', '/api/unregister'):
                        agent_id = body.get('agent_id')
                        if agent_id and agent_id in coord_ref.agents:
                            del coord_ref.agents[agent_id]
                            self._json({"status": "unregistered", "agent_id": agent_id})
                        else:
                            self._json({"error": "unknown agent"}, 404)

                    elif path in ('/assign', '/api/assign'):
                        try:
                            task_id = coord_ref.assign_task(
                                body.get('capability'),
                                body.get('task_data', body.get('params', {})),
                                body.get('agent_id')
                            )
                            self._json({"status": "assigned", "task_id": task_id})
                        except ValueError as e:
                            self._json({"error": str(e)}, 400)

                    elif path.startswith('/tasks/') and path.endswith('/claim') and path.count('/') == 3:
                        # POST /tasks/:id/claim — Agent claims a pending task
                        task_id = path.split('/')[-2]
                        agent_id = body.get('agent_id', 'unknown')
                        result = coord_ref.claim_task(task_id, agent_id)
                        if result is None:
                            self._json({"error": "task not found"}, 404)
                        elif isinstance(result, dict) and result.get("status") == "already_claimed":
                            self._json(result)
                        else:
                            self._json({"status": "claimed", "task": result})

                    elif path in ('/tasks/result', '/api/tasks/result'):
                        # POST /tasks/result — Agent submits task result
                        task_id = body.get('task_id')
                        agent_id = body.get('agent_id', 'unknown')
                        result_status = body.get('status', 'completed')
                        result_data = body.get('result', {})
                        task = coord_ref.complete_task(task_id, agent_id, result_status, result_data)
                        if task is None:
                            self._json({"error": "task not found"}, 404)
                        else:
                            self._json({"status": "accepted", "task_id": task_id})

                    else:
                        self._json({"error": "not found"}, 404)

                def do_OPTIONS(self):
                    self.send_response(200)
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
                    self.send_header('Access-Control-Allow-Headers', 'Content-Type')
                    self.end_headers()

                def _read_body(self):
                    length = int(self.headers.get('Content-Length', 0))
                    if length > 0:
                        try:
                            return json.loads(self.rfile.read(length).decode())
                        except Exception:
                            pass
                    return {}

                def _json(self, data, status=200):
                    body_bytes = json.dumps(data, default=str).encode()
                    self.send_response(status)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.send_header('Content-Length', str(len(body_bytes)))
                    self.end_headers()
                    self.wfile.write(body_bytes)

            server = HTTPServer(('0.0.0.0', port), CoordinatorHTTPHandler)
            self._http_server = server
            tcp_thread = threading.Thread(target=server.serve_forever, daemon=True)
            tcp_thread.start()
            print(f"[INFO] HTTP server listening on 0.0.0.0:{port}")
            print(f"[INFO] Endpoints:")
            print(f"[INFO]   GET  /status           - Cluster status")
            print(f"[INFO]   GET  /agents           - Agent list")
            print(f"[INFO]   GET  /tasks            - Task summary")
            print(f"[INFO]   GET  /tasks/pending    - Pending tasks")
            print(f"[INFO]   GET  /tasks/active     - Active (claimed) tasks")
            print(f"[INFO]   GET  /tasks/completed   - Completed tasks")
            print(f"[INFO]   POST /register         - Register agent")
            print(f"[INFO]   POST /heartbeat        - Agent heartbeat")
            print(f"[INFO]   POST /unregister       - Unregister agent")
            print(f"[INFO]   POST /assign           - Assign task")
            print(f"[INFO]   POST /tasks/:id/claim  - Claim a pending task")
            print(f"[INFO]   POST /tasks/result     - Submit task result")

        except Exception as e:
            print(f"[WARN] HTTP server failed to start: {e}")
            print(f"[INFO] Continuing in file-only mode")

    def stop(self):
        """Stop the coordinator."""
        self.running = False
        if hasattr(self, '_http_server') and self._http_server:
            self._http_server.shutdown()
            print(f"[INFO] HTTP server stopped")
        print(f"[INFO] Coordinator stopped")


def main():
    """Entry point for standalone coordinator."""
    import argparse
    parser = argparse.ArgumentParser(description="Coordinator Server")
    parser.add_argument("--shared-dir", help="Shared directory", default=DEFAULT_SHARED_DIR)
    parser.add_argument("--port", type=int, help="HTTP API port", default=8080)
    args = parser.parse_args()
    coordinator = Coordinator(shared_dir=args.shared_dir)
    try:
        coordinator.run(port=args.port)
    except KeyboardInterrupt:
        coordinator.stop()


if __name__ == "__main__":
    main()
