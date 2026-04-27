#!/usr/bin/env python3
"""
Agent Cluster - Main Entry Point

Usage:
    python -m agent_cluster coordinator --port 8080
    python -m agent_cluster agent --coordinator http://localhost:8080
    python -m agent_cluster doctor
"""

import sys
import argparse
import logging
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

__version__ = '1.0.0'


def main():
    parser = argparse.ArgumentParser(
        description='Agent Cluster - Distributed AI Agent System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Start coordinator:
    python -m agent_cluster coordinator --port 8080
  
  Start agent:
    python -m agent_cluster agent --coordinator /tmp/agent_cluster
  
  Run diagnostics:
    python -m agent_cluster doctor
"""
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Coordinator command
    coord_parser = subparsers.add_parser('coordinator', help='Start coordinator server')
    coord_parser.add_argument('--port', type=int, default=8080, help='Port to listen on')
    coord_parser.add_argument('--host', default='0.0.0.0', help='Host to bind')
    coord_parser.add_argument('--shared-dir', default='/tmp/agent_cluster', help='Shared directory for messaging')
    coord_parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    # Agent command
    agent_parser = subparsers.add_parser('agent', help='Start agent process')
    agent_parser.add_argument('--shared-dir', required=True, help='Shared directory (coordinator location)')
    agent_parser.add_argument('--agent-id', help='Agent ID (auto-generated if not set)')
    agent_parser.add_argument('--capabilities', nargs='+', default=['echo', 'shell', 'compute'], help='Agent capabilities')
    
    # Dashboard command
    dash_parser = subparsers.add_parser('dashboard', help='Start dashboard web UI')
    dash_parser.add_argument('--port', type=int, default=8081, help='Dashboard port')
    dash_parser.add_argument('--shared-dir', default='/tmp/agent_cluster', help='Shared directory')
    dash_parser.add_argument('--coordinator-url', default=None, help='Coordinator URL for agent sync (e.g. http://localhost:8080)')
    
    # Doctor command
    subparsers.add_parser('doctor', help='Run diagnostics')
    
    # Version command
    subparsers.add_parser('version', help='Show version')
    
    # Handler command
    handler_parser = subparsers.add_parser('handlers', help='List available handlers')
    handler_parser.add_argument('--category', help='Filter by category')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'version':
        print(f"agent-cluster version {__version__}")
        sys.exit(0)
    
    if args.command == 'doctor':
        run_diagnostics()
        sys.exit(0)
    
    if args.command == 'handlers':
        list_handlers(args.category)
        sys.exit(0)
    
    if args.command == 'coordinator':
        start_coordinator(args)
    
    elif args.command == 'agent':
        start_agent(args)
    
    elif args.command == 'dashboard':
        start_dashboard(args)


def start_coordinator(args):
    """Start the coordinator server."""
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print(f"Agent Cluster v{__version__}")
    print(f"Starting Coordinator on {args.host}:{args.port}")
    print(f"Shared directory: {args.shared_dir}")
    print("")
    
    # Create shared directory
    Path(args.shared_dir).mkdir(parents=True, exist_ok=True)
    
    # Import and run coordinator
    from coordinator.server import Coordinator
    
    coordinator = Coordinator(shared_dir=args.shared_dir)
    
    print("Coordinator ready. Press Ctrl+C to stop.")
    print("")
    
    try:
        coordinator.run(port=args.port)
    except KeyboardInterrupt:
        print("\nShutting down...")
        coordinator.stop()


def start_agent(args):
    """Start an agent process."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [%(name)s] %(levelname)s - %(message)s'
    )
    
    print(f"Agent Cluster v{__version__}")
    print(f"Starting Agent")
    print(f"Shared directory: {args.shared_dir}")
    print(f"Capabilities: {args.capabilities}")
    print("")
    
    from agents.nano_bot import NanoBot, BotConfig
    
    # Convert capabilities to expected format
    capabilities = [{"name": cap, "confidence": 1.0} for cap in args.capabilities]
    
    config = BotConfig(
        agent_id=args.agent_id or f"agent-{Path(args.shared_dir).name}",
        agent_type="nanobot",
        cluster_dir=args.shared_dir,
        capabilities=capabilities,
    )
    
    agent = NanoBot(config)
    
    print("Agent ready. Press Ctrl+C to stop.")
    print("")
    
    try:
        agent.run()
    except KeyboardInterrupt:
        print("\nStopping agent...")
        agent.stop()


def start_dashboard(args):
    """Start the dashboard web UI."""
    print(f"Starting Dashboard on port {args.port}")
    print(f"Shared directory: {args.shared_dir}")

    # Dashboard is optional - check if available
    try:
        from dashboard.monitor import ClusterDashboard, DashboardConfig
        from dashboard.api import DashboardAPI
        from capabilities.registry import CapabilityRegistry
        from orchestration.cluster import ClusterOrchestrator

        config = DashboardConfig()
        registry = CapabilityRegistry()
        orchestrator = ClusterOrchestrator()
        dashboard = ClusterDashboard(orchestrator=orchestrator, registry=registry, config=config)

        # Connect to coordinator if running on same host
        coordinator_url = f"http://localhost:{args.port - 1}"  # Default: coordinator on port-1
        if hasattr(args, 'coordinator_url') and args.coordinator_url:
            coordinator_url = args.coordinator_url

        print(f"\nDashboard API running at http://localhost:{args.port}")
        print(f"Coordinator sync: {coordinator_url}")
        print("Endpoints:")
        print(f" - http://localhost:{args.port}/api/overview")
        print(f" - http://localhost:{args.port}/api/agents")
        print(f" - http://localhost:{args.port}/api/export/html")
        print("\nPress Ctrl+C to stop.")

        # Start the API server with coordinator sync
        api = DashboardAPI(dashboard=dashboard, port=args.port, coordinator_url=coordinator_url)
        api.start()

    except ImportError as e:
        print(f"Dashboard not available: {e}")
        print("Install with: pip install aiohttp websockets")
        sys.exit(1)



def run_diagnostics():
    """Run system diagnostics."""
    print(f"Agent Cluster v{__version__} - Diagnostics")
    print("=" * 50)
    print("")
    
    issues = []
    
    # Check Python version
    py_version = sys.version_info
    print(f"✓ Python {py_version.major}.{py_version.minor}.{py_version.micro}")
    if py_version < (3, 8):
        issues.append("Python 3.8+ required")
    
    # Check imports
    print("\nModule checks:")
    modules = [
        ('handlers', 'Task Handlers'),
        ('reliability', 'Reliability Module'),
        ('security', 'Security Module'),
        ('deployment', 'Deployment Module'),
        ('coordinator', 'Coordinator'),
        ('agents', 'Agent Framework'),
    ]
    
    for mod_name, desc in modules:
        try:
            __import__(mod_name)
            print(f"  ✓ {desc}")
        except ImportError as e:
            print(f"  ✗ {desc}: {e}")
            issues.append(f"Cannot import {mod_name}")
    
    # Check handlers
    print("\nHandler registry:")
    try:
        from handlers import HandlerRegistry
        registry = HandlerRegistry()
        registry.register_all()
        counts = registry.count_handlers()
        total = sum(counts.values())
        print(f"  ✓ {total} handlers registered")
        for cat, count in sorted(counts.items()):
            print(f"    - {cat}: {count}")
    except Exception as e:
        print(f"  ✗ Handler registration failed: {e}")
        issues.append("Handler registration failed")
    
    # Check security
    print("\nSecurity module:")
    try:
        from security import AuthManager
        auth = AuthManager()
        key = auth.create_api_key("test", ["viewer"])
        print(f"  ✓ API key generation works")
    except Exception as e:
        print(f"  ✗ Auth check failed: {e}")
        issues.append("Auth module failed")
    
    # Check reliability
    print("\nReliability module:")
    try:
        from reliability import CircuitBreaker, RetryManager
        cb = CircuitBreaker(name="test-circuit")
        print(f"  ✓ CircuitBreaker initialized")
        rm = RetryManager()
        print(f"  ✓ RetryManager initialized")
    except Exception as e:
        print(f"  ✗ Reliability check failed: {e}")
        issues.append("Reliability module failed")
    
    # Summary
    print("\n" + "=" * 50)
    if issues:
        print(f"❌ ISSUES FOUND ({len(issues)}):")
        for issue in issues:
            print(f"  - {issue}")
        sys.exit(1)
    else:
        print("✅ All diagnostics passed!")
        print("\nReady to start. Try:")
        print("  python -m agent_cluster coordinator --port 8080")
        print("  python -m agent_cluster agent --shared-dir /tmp/agent_cluster")
        sys.exit(0)


def list_handlers(category=None):
    """List available handlers."""
    from handlers import HandlerRegistry
    registry = HandlerRegistry()
    registry.register_all()
    
    counts = registry.count_handlers()
    total = sum(counts.values())
    
    print(f"Agent Cluster v{__version__} - Available Handlers ({total} total)")
    print("=" * 50)
    
    if category:
        if category in counts:
            print(f"\n{category}: {counts[category]} handlers")
            # Would need to extend HandlerRegistry to list handler names
        else:
            print(f"\nCategory '{category}' not found.")
            print(f"Available: {', '.join(sorted(counts.keys()))}")
    else:
        print("\nBy category:")
        for cat, count in sorted(counts.items()):
            print(f"  {cat}: {count} handlers")


if __name__ == '__main__':
    main()
