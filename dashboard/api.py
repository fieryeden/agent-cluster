#!/usr/bin/env python3
"""
Dashboard API Server

RESTful API for the dashboard frontend.

Endpoints:
- GET /api/overview - Cluster overview
- GET /api/agents - All agents
- GET /api/agents/:id - Single agent
- GET /api/tasks - Tasks (query param: status)
- GET /api/tasks/queue - Task queue by status
- GET /api/capabilities - Capability registry
- GET /api/gaps - Capability gaps
- GET /api/alerts - Active alerts
- GET /api/trends - Metric trends (query param: hours)
- GET /api/topology - Network topology
- GET /api/export/json - Full JSON export
- GET /api/export/html - HTML dashboard
- POST /api/alerts/:id/acknowledge - Acknowledge alert
- POST /api/alerts/:id/resolve - Resolve alert

Resume Portal Endpoints:
- GET /api/resumes - Browse agents in labor market
- GET /api/resumes/:id - Get full agent resume
- POST /api/resumes/:id/review - Add review to agent
- POST /api/resumes/:id/task - Record task completion
- GET /api/marketplace - Get marketplace HTML page
- GET /api/marketplace/:id - Get agent resume HTML page
"""

import json
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler
    from urllib.parse import urlparse, parse_qs
    HTTP_AVAILABLE = True
except ImportError:
    HTTP_AVAILABLE = False

from dashboard.monitor import ClusterDashboard, DashboardConfig
from dashboard.resume_portal import AgentResumePortal


class DashboardAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler for dashboard API."""
    dashboard: ClusterDashboard = None
    resume_portal: AgentResumePortal = None

    def log_message(self, format, *args):
        """Suppress default logging."""
        pass

    def _send_json(self, data: Any, status: int = 200):
        """Send JSON response."""
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())

    def _send_html(self, html: str, status: int = 200):
        """Send HTML response."""
        self.send_response(status)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html.encode())

    def _parse_path(self) -> tuple:
        """Parse request path and query params."""
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/')
        params = parse_qs(parsed.query)
        # Convert single-value params
        params = {k: v[0] if len(v) == 1 else v for k, v in params.items()}
        return path, params

    def _read_json_body(self) -> Dict:
        """Read and parse JSON body."""
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length > 0:
            body = self.rfile.read(content_length)
            return json.loads(body.decode())
        return {}

    def do_GET(self):
        """Handle GET requests."""
        path, params = self._parse_path()

        # Route handlers for standard dashboard
        routes = {
            '/api/overview': lambda: self.dashboard.get_overview(),
            '/api/agents': lambda: self.dashboard.get_agents(),
            '/api/tasks': lambda: self.dashboard.get_tasks(params.get('status')),
            '/api/tasks/queue': lambda: self.dashboard.get_task_queue(),
            '/api/capabilities': lambda: self.dashboard.get_capabilities(),
            '/api/gaps': lambda: self.dashboard.get_capability_gaps(),
            '/api/alerts': lambda: self.dashboard.get_alerts(params.get('include_resolved', 'false').lower() == 'true'),
            '/api/trends': lambda: self.dashboard.get_trends(int(params.get('hours', 1))),
            '/api/topology': lambda: self.dashboard.get_network_topology(),
            '/api/export/json': lambda: json.loads(self.dashboard.export_json()),
            '/api/export/html': lambda: self.dashboard.export_html(),
        }

        # Root path serves the HTML dashboard
        if path == '/' or path == '':
            try:
                self._send_html(self.dashboard.export_html())
            except Exception as e:
                self._send_json({'error': str(e)}, 500)
            return

        # Check exact match
        if path in routes:
            try:
                data = routes[path]()
                if path == '/api/export/html':
                    self._send_html(data)
                else:
                    self._send_json(data)
            except Exception as e:
                self._send_json({'error': str(e)}, 500)
            return

        # Check /api/agents/:id
        if path.startswith('/api/agents/'):
            agent_id = path.split('/')[-1]
            data = self.dashboard.get_agent(agent_id)
            if data:
                self._send_json(data)
            else:
                self._send_json({'error': 'Agent not found'}, 404)
            return

        # Check /api/gaps/:name (mark as learning/resolved)
        if path.startswith('/api/gaps/'):
            self._send_json({'error': 'Not implemented'}, 501)
            return

        # ========== RESUME PORTAL ROUTES ==========

        # GET /api/resumes - Browse agents in labor market
        if path == '/api/resumes':
            if self.resume_portal:
                agents = self.resume_portal.browse_agents(
                    specialization=params.get('specialization'),
                    min_rating=float(params.get('min_rating', 0)),
                    capability=params.get('capability'),
                    sort_by=params.get('sort_by', 'rating'),
                    limit=int(params.get('limit', 50))
                )
                self._send_json({'agents': agents, 'total': len(agents)})
            else:
                self._send_json({'error': 'Resume portal not initialized'}, 503)
            return

        # GET /api/resumes/:id - Get full agent resume
        if path.startswith('/api/resumes/') and path.count('/') == 3:
            agent_id = path.split('/')[-1]
            if self.resume_portal:
                resume = self.resume_portal.get_resume(agent_id)
                if resume:
                    self._send_json(resume)
                else:
                    self._send_json({'error': 'Agent not found'}, 404)
            else:
                self._send_json({'error': 'Resume portal not initialized'}, 503)
            return

        # GET /api/marketplace - Get marketplace HTML page
        if path == '/api/marketplace':
            if self.resume_portal:
                html = self.resume_portal.generate_marketplace_html()
                self._send_html(html)
            else:
                self._send_html('<h1>Resume portal not initialized</h1>')
            return

        # GET /api/marketplace/:id - Get agent resume HTML page
        if path.startswith('/api/marketplace/') and path.count('/') == 3:
            agent_id = path.split('/')[-1]
            if self.resume_portal:
                html = self.resume_portal.generate_resume_html(agent_id)
                self._send_html(html)
            else:
                self._send_html('<h1>Resume portal not initialized</h1>')
            return

        # Unknown path
        self._send_json({'error': 'Not found'}, 404)

    def do_POST(self):
        """Handle POST requests."""
        path, params = self._parse_path()

        # Acknowledge alert
        if path.startswith('/api/alerts/') and path.endswith('/acknowledge'):
            alert_id = path.split('/')[-2]
            self.dashboard.collector.acknowledge_alert(alert_id)
            self._send_json({'status': 'acknowledged'})
            return

        # Resolve alert
        if path.startswith('/api/alerts/') and path.endswith('/resolve'):
            alert_id = path.split('/')[-2]
            self.dashboard.collector.resolve_alert(alert_id)
            self._send_json({'status': 'resolved'})
            return

        # ========== RESUME PORTAL POST ROUTES ==========

        # POST /api/resumes/:id/review - Add review to agent
        if path.startswith('/api/resumes/') and path.endswith('/review'):
            agent_id = path.split('/')[-2]
            if self.resume_portal:
                try:
                    data = self._read_json_body()
                    self.resume_portal.add_review(
                        agent_id=agent_id,
                        rating=data['rating'],
                        comment=data['comment'],
                        reviewer_id=data['reviewer_id'],
                        task_type=data['task_type']
                    )
                    self._send_json({'success': True})
                except Exception as e:
                    self._send_json({'error': str(e)}, 400)
            else:
                self._send_json({'error': 'Resume portal not initialized'}, 503)
            return

        # POST /api/resumes/:id/task - Record task completion
        if path.startswith('/api/resumes/') and path.endswith('/task'):
            agent_id = path.split('/')[-2]
            if self.resume_portal:
                try:
                    data = self._read_json_body()
                    self.resume_portal.record_task(
                        agent_id=agent_id,
                        success=data.get('success', True),
                        duration_seconds=data.get('duration', 0)
                    )
                    self._send_json({'success': True})
                except Exception as e:
                    self._send_json({'error': str(e)}, 400)
            else:
                self._send_json({'error': 'Resume portal not initialized'}, 503)
            return

        # ========== TASK ASSIGNMENT ROUTE ==========
        # POST /api/tasks/assign - Assign task to cluster via coordinator
        if path == '/api/tasks/assign':
            try:
                data = self._read_json_body()
                capability = data.get('capability', '')
                task_data = data.get('task_data', {})
                agent_id = data.get('agent_id')
                # Forward to coordinator if URL available
                coord_url = getattr(self, '_coordinator_url', None)
                if coord_url:
                    import urllib.request
                    payload = json.dumps({'capability': capability, 'task_data': task_data, 'agent_id': agent_id}).encode()
                    req = urllib.request.Request(f'{coord_url}/assign', data=payload, headers={'Content-Type': 'application/json'})
                    resp = urllib.request.urlopen(req, timeout=10)
                    result = json.loads(resp.read())
                    self._send_json(result)
                else:
                    self._send_json({'error': 'No coordinator URL configured'}, 503)
            except Exception as e:
                self._send_json({'error': str(e)}, 500)
            return

        self._send_json({'error': 'Not found'}, 404)

    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()



class DashboardAPI:
    """Dashboard API server wrapper."""

    def __init__(self, dashboard: ClusterDashboard, port: int = 8080,
                 resume_portal: AgentResumePortal = None,
                 coordinator_url: str = None):
        """
        Initialize API server.

        Args:
            dashboard: ClusterDashboard instance
            port: Port to listen on
            resume_portal: Optional AgentResumePortal instance
            coordinator_url: Optional coordinator URL to sync agents from (e.g. http://localhost:8080)
        """
        if not HTTP_AVAILABLE:
            raise ImportError("HTTP server not available")
        self.dashboard = dashboard
        self.port = port
        self.resume_portal = resume_portal or AgentResumePortal()
        self.coordinator_url = coordinator_url
        self._server: Optional[ThreadingHTTPServer] = None
        self._sync_thread = None

    def _sync_from_coordinator(self):
        """Background thread: sync agent and task data from coordinator."""
        from dashboard.monitor import AgentMetrics, TaskMetrics
        from datetime import datetime as dt
        import urllib.request
        import time as _time

        while True:
            try:
                resp = urllib.request.urlopen(
                    f'{self.coordinator_url}/status', timeout=5)
                data = json.loads(resp.read())

                for a in data.get('agents', []):
                    aid = a['agent_id']
                    raw_caps = a.get('capabilities', {})
                    if isinstance(raw_caps, dict):
                        caps = list(raw_caps.keys())
                    elif isinstance(raw_caps, list):
                        caps = [c.get('name', str(c)) if isinstance(c, dict) else str(c) for c in raw_caps]
                    else:
                        caps = []
                    status = 'online' if a.get('is_alive') else 'offline'

                    if aid not in self.dashboard.agent_metrics:
                        m = AgentMetrics(
                            agent_id=aid,
                            bot_type='nanobot',
                            status=status,
                            capabilities=caps,
                            capability_count=len(caps),
                            last_heartbeat=dt.now(),
                        )
                        self.dashboard.agent_metrics[aid] = m
                    else:
                        m = self.dashboard.agent_metrics[aid]
                        m.status = status
                        m.capabilities = caps
                        m.capability_count = len(caps)
                        m.last_heartbeat = dt.now()

                # Sync completed tasks from coordinator
                try:
                    resp2 = urllib.request.urlopen(
                        f'{self.coordinator_url}/tasks/completed', timeout=5)
                    tdata = json.loads(resp2.read())
                    for t in tdata.get('tasks', []):
                        tid = t['task_id']
                        if tid not in self.dashboard.tasks:
                            tm = TaskMetrics(
                                task_id=tid,
                                task_type=t.get('capability', 'unknown'),
                                status='completed',
                                created_at=dt.now(),
                            )
                            # Use real timestamps from coordinator
                            from datetime import timezone
                            try:
                                if t.get('completed_at'):
                                    tm.completed_at = dt.fromisoformat(t['completed_at'])
                                else:
                                    tm.completed_at = dt.now()
                                if t.get('claimed_at'):
                                    tm.started_at = dt.fromisoformat(t['claimed_at'])
                            except Exception:
                                tm.completed_at = dt.now()
                            # Calculate duration from execution_time or timestamps
                            exec_time = (t.get('result') or {}).get('execution_time', 0)
                            if exec_time:
                                tm.duration_seconds = exec_time
                            elif tm.started_at and tm.completed_at:
                                tm.duration_seconds = (tm.completed_at - tm.started_at).total_seconds()
                            claimed_by = t.get('claimed_by') or t.get('completed_by', 'unknown')
                            tm.assigned_to = claimed_by
                            self.dashboard.tasks[tid] = tm
                            if claimed_by in self.dashboard.agent_metrics:
                                self.dashboard.agent_metrics[claimed_by].tasks_completed += 1
                            # Also add to task_history for snapshot metrics
                            if tm not in self.dashboard.task_history:
                                self.dashboard.task_history.append(tm)
                except Exception:
                    pass

                # Create snapshot so performance metrics are populated
                try:
                    self.dashboard._create_snapshot()
                except Exception as e:
                    print(f"[Dashboard] Snapshot error: {e}")
                    import traceback
                    traceback.print_exc()

            except Exception:
                pass

            _time.sleep(5)

    def start(self, blocking: bool = True):
        """
        Start the API server.

        Args:
            blocking: If True, block until server stops
        """
        DashboardAPIHandler.dashboard = self.dashboard
        DashboardAPIHandler.resume_portal = self.resume_portal
        DashboardAPIHandler._coordinator_url = self.coordinator_url
        self._server = ThreadingHTTPServer(('0.0.0.0', self.port), DashboardAPIHandler)

        # Start coordinator sync if URL provided
        if self.coordinator_url:
            import threading
            self._sync_thread = threading.Thread(target=self._sync_from_coordinator, daemon=True)
            self._sync_thread.start()
            print(f"[Dashboard API] Syncing from coordinator at {self.coordinator_url}")

        print(f"[Dashboard API] Server running on http://0.0.0.0:{self.port}")
        print(f"[Dashboard API] Endpoints:")
        print(f" - GET /api/overview")
        print(f" - GET /api/agents")
        print(f" - GET /api/tasks")
        print(f" - GET /api/capabilities")
        print(f" - GET /api/gaps")
        print(f" - GET /api/alerts")
        print(f" - GET /api/trends")
        print(f" - GET /api/topology")
        print(f" - GET /api/export/html")
        print(f"")
        print(f"[Resume Portal] Endpoints:")
        print(f" - GET /api/resumes")
        print(f" - GET /api/resumes/:id")
        print(f" - POST /api/resumes/:id/review")
        print(f" - POST /api/resumes/:id/task")
        print(f" - GET /api/marketplace (HTML)")
        print(f" - GET /api/marketplace/:id (HTML)")
        if blocking:
            self._server.serve_forever()
        else:
            import threading
            thread = threading.Thread(target=self._server.serve_forever, daemon=True)
            thread.start()

    def stop(self):
        """Stop the server."""
        if self._server:
            self._server.shutdown()

def create_api_server(
    orchestrator,
    registry,
    port: int = 8080,
    config: DashboardConfig = None,
    resume_portal: AgentResumePortal = None,
) -> tuple:
    """
    Create a complete dashboard with API server.

    Args:
        orchestrator: ClusterOrchestrator instance
        registry: CapabilityRegistry instance
        port: API server port
        config: Dashboard configuration
        resume_portal: Optional AgentResumePortal instance

    Returns:
        Tuple of (Dashboard, API server)
    """
    dashboard = ClusterDashboard(
        orchestrator=orchestrator,
        registry=registry,
        config=config or DashboardConfig(),
    )
    api = DashboardAPI(dashboard, port=port, resume_portal=resume_portal)
    return dashboard, api


# CLI entry point
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Agent Cluster Dashboard')
    parser.add_argument('--port', type=int, default=8080, help='API server port')
    parser.add_argument('--mock', action='store_true', help='Use mock data')
    args = parser.parse_args()

    if args.mock:
        # Create mock dashboard for testing
        from orchestration.cluster import ClusterOrchestrator, BotConfig, BotType

        orch = ClusterOrchestrator()
        reg = orch.capability_registry

        # Register some mock agents
        orch.register_agent(BotConfig(
            bot_type=BotType.NANOBOT,
            agent_id="agent-001",
            capabilities=["echo", "shell", "compute"],
            max_concurrent_tasks=3,
        ))
        orch.register_agent(BotConfig(
            bot_type=BotType.NANOBOT,
            agent_id="agent-002",
            capabilities=["echo", "data_processing"],
            max_concurrent_tasks=2,
        ))
        orch.register_agent(BotConfig(
            bot_type=BotType.OPENCLAW,
            agent_id="openclaw-main",
            capabilities=["research", "analysis", "generation", "echo"],
            max_concurrent_tasks=5,
        ))

        # Submit some tasks
        orch.submit_task("echo", {"message": "Hello"})
        orch.submit_task("shell", {"command": "ls -la"})
        orch.submit_task("research", {"topic": "test"})

        dashboard = ClusterDashboard(orchestrator=orch, registry=reg)
        dashboard.start()

        # Add some mock gaps
        dashboard.collector.record_capability_gap("excel_processing", "agent-001")
        dashboard.collector.record_capability_gap("excel_processing", "agent-002")
        dashboard.collector.record_capability_gap("image_generation", "openclaw-main")

        # Create resume portal with demo data
        resume_portal = AgentResumePortal()
        resume_portal.get_or_create_resume('bookkeeper-v2.3', ['data', 'file'])
        resume_portal.get_or_create_resume('web-scraper-01', ['web', 'data'])
        resume_portal.get_or_create_resume('ai-assistant-04', ['ai', 'communication'])
        for _ in range(50):
            resume_portal.record_task('bookkeeper-v2.3', success=True)
            resume_portal.record_task('web-scraper-01', success=True)
        resume_portal.add_review('bookkeeper-v2.3', 5, 'Excellent work!', 'client-001', 'data_processing')

    else:
        # Real dashboard - would need actual cluster connection
        print("Error: No cluster connection. Use --mock for testing.")
        exit(1)

    api = DashboardAPI(dashboard, port=args.port, resume_portal=resume_portal if args.mock else None)

    print(f"\nDashboard available at: http://localhost:{args.port}/api/export/html")
    print(f"Marketplace available at: http://localhost:{args.port}/api/marketplace\n")

    try:
        api.start(blocking=True)
    except KeyboardInterrupt:
        print("\nShutting down...")
        dashboard.stop()
        api.stop()
