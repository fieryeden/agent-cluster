#!/usr/bin/env python3
"""
Dashboard CLI - Command-line interface for the agent cluster dashboard.

Usage:
    python -m dashboard.cli serve --port 8080
    python -m dashboard.cli status
    python -m dashboard.cli export --format html --output dashboard.html
"""

import argparse
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dashboard.monitor import ClusterDashboard, DashboardConfig
from dashboard.api import DashboardAPI


def cmd_serve(args):
    """Start the dashboard API server."""
    from orchestration.cluster import ClusterOrchestrator, BotConfig, BotType
    
    # Create orchestrator
    orch = ClusterOrchestrator(config_dir=args.shared_dir or "/tmp/agent_cluster")
    
    # Check for existing agents file
    config_file = Path(orch.config_dir) / "config.json"
    if config_file.exists():
        import json
        with open(config_file) as f:
            config = json.load(f)
            for agent_id, agent_config in config.get('agents', {}).items():
                orch.register_agent(BotConfig(
                    bot_type=BotType(agent_config.get('type', 'nanobot')),
                    agent_id=agent_id,
                    capabilities=agent_config.get('capabilities', []),
                ))
    
    # Create dashboard
    config = DashboardConfig(
        metrics_interval=args.interval,
        heartbeat_timeout_seconds=args.heartbeat_timeout,
    )
    
    dashboard = ClusterDashboard(
        orchestrator=orch,
        registry=orch.capability_registry,
        config=config,
    )
    
    dashboard.start()
    
    # Create API server
    api = DashboardAPI(dashboard, port=args.port)
    
    print(f"\n{'='*60}")
    print(f"  Agent Cluster Dashboard")
    print(f"{'='*60}")
    print(f"\n  Dashboard: http://localhost:{args.port}/api/export/html")
    print(f"  API:       http://localhost:{args.port}/api/overview")
    print(f"\n  Press Ctrl+C to stop")
    print(f"{'='*60}\n")
    
    try:
        api.start(blocking=True)
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        dashboard.stop()
        api.stop()


def cmd_status(args):
    """Show current cluster status."""
    from orchestration.cluster import ClusterOrchestrator
    import json
    
    config_dir = args.shared_dir or "/tmp/agent_cluster"
    orch = ClusterOrchestrator(config_dir=config_dir)
    
    # Load existing state
    status_file = Path(config_dir) / "status.json"
    if status_file.exists():
        with open(status_file) as f:
            status = json.load(f)
    else:
        status = orch.get_status()
    
    # Print status
    print("\n📊 Agent Cluster Status\n")
    
    agents = status.get('agents', {})
    print(f"{'Agents:':<15} {len(agents)} registered")
    
    online = sum(1 for a in agents.values() if a.get('status') == 'active')
    print(f"{'  Online:':<15} {online}")
    print(f"{'  Offline:':<15} {len(agents) - online}")
    
    tasks = status.get('tasks', {})
    print(f"\n{'Tasks:':<15}")
    print(f"{'  Pending:':<15} {tasks.get('pending', 0)}")
    print(f"{'  Total:':<15} {tasks.get('total', 0)}")
    
    caps = status.get('capabilities', {})
    print(f"\n{'Capabilities:':<15} {len(caps)} unique")
    for cap, agents_list in list(caps.items())[:5]:
        print(f"  - {cap}: {len(agents_list)} agent(s)")
    if len(caps) > 5:
        print(f"  ... and {len(caps) - 5} more")
    
    print()


def cmd_export(args):
    """Export dashboard to file."""
    from orchestration.cluster import ClusterOrchestrator
    
    config_dir = args.shared_dir or "/tmp/agent_cluster"
    orch = ClusterOrchestrator(config_dir=config_dir)
    
    dashboard = ClusterDashboard(
        orchestrator=orch,
        registry=orch.capability_registry,
    )
    
    if args.format == 'html':
        content = dashboard.export_html()
        output = args.output or 'dashboard.html'
    else:
        content = dashboard.export_json()
        output = args.output or 'dashboard.json'
    
    with open(output, 'w') as f:
        f.write(content)
    
    print(f"Exported dashboard to {output}")


def cmd_mock(args):
    """Run dashboard with mock data (for testing)."""
    from orchestration.cluster import ClusterOrchestrator, BotConfig, BotType
    
    orch = ClusterOrchestrator()
    
    # Register mock agents
    orch.register_agent(BotConfig(
        bot_type=BotType.NANOBOT,
        agent_id="agent-001",
        capabilities=["echo", "shell", "compute", "data_fetch"],
        max_concurrent_tasks=3,
    ))
    orch.register_agent(BotConfig(
        bot_type=BotType.NANOBOT,
        agent_id="agent-002",
        capabilities=["echo", "data_processing", "validation"],
        max_concurrent_tasks=2,
    ))
    orch.register_agent(BotConfig(
        bot_type=BotType.OPENCLAW,
        agent_id="openclaw-main",
        capabilities=["research", "analysis", "generation", "code_review"],
        max_concurrent_tasks=5,
    ))
    orch.register_agent(BotConfig(
        bot_type=BotType.EXTENSION,
        agent_id="browser-agent",
        capabilities=["web_automation", "scraping", "testing"],
        max_concurrent_tasks=2,
    ))
    
    # Submit mock tasks
    orch.submit_task("echo", {"message": "Hello world"}, priority=2)
    orch.submit_task("shell", {"command": "ls -la"}, priority=1)
    orch.submit_task("research", {"topic": "AI agents"}, priority=3)
    orch.submit_task("data_processing", {"file": "data.csv"}, priority=1)
    orch.submit_task("web_automation", {"url": "https://example.com"}, priority=2)
    
    # Assign some tasks
    orch.assign_tasks()
    
    # Create dashboard
    dashboard = ClusterDashboard(
        orchestrator=orch,
        registry=orch.capability_registry,
    )
    
    # Add mock capability gaps
    dashboard.collector.record_capability_gap("excel_processing", "agent-001")
    dashboard.collector.record_capability_gap("excel_processing", "agent-002")
    dashboard.collector.record_capability_gap("excel_processing", "openclaw-main")
    dashboard.collector.record_capability_gap("image_generation", "openclaw-main")
    dashboard.collector.record_capability_gap("pdf_rendering", "agent-001")
    dashboard.collector.record_capability_gap("pdf_rendering", "browser-agent")
    dashboard.collector.record_capability_gap("ml_training", "agent-001")
    
    dashboard.start()
    
    # Create API server
    api = DashboardAPI(dashboard, port=args.port)
    
    print(f"\n{'='*60}")
    print(f"  Agent Cluster Dashboard (MOCK MODE)")
    print(f"{'='*60}")
    print(f"\n  Dashboard: http://localhost:{args.port}/api/export/html")
    print(f"  API:       http://localhost:{args.port}/api/overview")
    print(f"\n  Mock Data:")
    print(f"    - 4 agents (1 offline for testing)")
    print(f"    - 5 tasks (mix of pending/active)")
    print(f"    - 3 capability gaps")
    print(f"\n  Press Ctrl+C to stop")
    print(f"{'='*60}\n")
    
    try:
        api.start(blocking=True)
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        dashboard.stop()
        api.stop()


def main():
    parser = argparse.ArgumentParser(
        description='Agent Cluster Dashboard',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python -m dashboard.cli serve --port 8080
  python -m dashboard.cli status
  python -m dashboard.cli mock --port 8080
  python -m dashboard.cli export --format html
'''
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # serve command
    serve_parser = subparsers.add_parser('serve', help='Start dashboard server')
    serve_parser.add_argument('--port', type=int, default=8080, help='Server port')
    serve_parser.add_argument('--shared-dir', help='Cluster shared directory')
    serve_parser.add_argument('--interval', type=float, default=5.0, help='Update interval (seconds)')
    serve_parser.add_argument('--heartbeat-timeout', type=int, default=90, help='Heartbeat timeout (seconds)')
    
    # status command
    status_parser = subparsers.add_parser('status', help='Show cluster status')
    status_parser.add_argument('--shared-dir', help='Cluster shared directory')
    
    # export command
    export_parser = subparsers.add_parser('export', help='Export dashboard')
    export_parser.add_argument('--format', choices=['html', 'json'], default='html', help='Export format')
    export_parser.add_argument('--output', '-o', help='Output file')
    export_parser.add_argument('--shared-dir', help='Cluster shared directory')
    
    # mock command
    mock_parser = subparsers.add_parser('mock', help='Run with mock data')
    mock_parser.add_argument('--port', type=int, default=8080, help='Server port')
    
    args = parser.parse_args()
    
    if args.command == 'serve':
        cmd_serve(args)
    elif args.command == 'status':
        cmd_status(args)
    elif args.command == 'export':
        cmd_export(args)
    elif args.command == 'mock':
        cmd_mock(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
