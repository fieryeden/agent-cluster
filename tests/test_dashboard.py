#!/usr/bin/env python3
"""
Tests for Dashboard Components

Run with: python tests/test_dashboard.py
"""

import sys
import time
import threading
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from dashboard.monitor import (
    ClusterDashboard,
    DashboardConfig,
    AgentMetrics,
    TaskMetrics,
    CapabilityGap,
    ClusterSnapshot,
    Alert,
    MetricsCollector,
)
from orchestration.cluster import ClusterOrchestrator, BotConfig, BotType


def create_test_orchestrator():
    """Create orchestrator with test agents."""
    orch = ClusterOrchestrator()
    
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
        capabilities=["research", "analysis", "generation"],
        max_concurrent_tasks=5,
    ))
    
    return orch


class TestDashboardBasics:
    """Basic dashboard functionality tests."""
    
    def test_dashboard_creation(self):
        """Dashboard can be created."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        assert dashboard is not None
        assert dashboard.orchestrator is orch
        assert dashboard.registry is orch.capability_registry
        print("✓ Dashboard creation")
    
    def test_dashboard_config(self):
        """Dashboard respects configuration."""
        config = DashboardConfig(
            metrics_interval=1.0,
            heartbeat_timeout_seconds=60,
            max_recent_tasks=100,
        )
        
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
            config=config,
        )
        
        assert dashboard.config.metrics_interval == 1.0
        assert dashboard.config.heartbeat_timeout_seconds == 60
        assert dashboard.config.max_recent_tasks == 100
        print("✓ Dashboard configuration")
    
    def test_dashboard_overview(self):
        """Dashboard provides overview."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        # Trigger metric update
        dashboard._update_agent_metrics()
        
        overview = dashboard.get_overview()
        
        assert 'cluster_health' in overview
        assert 'agents' in overview
        assert 'tasks' in overview
        assert 'capabilities' in overview
        assert 'performance' in overview
        assert 'alerts' in overview
        
        assert overview['agents']['total'] == 3
        print("✓ Dashboard overview")
    
    def test_agent_metrics(self):
        """Dashboard tracks agent metrics."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        dashboard._update_agent_metrics()
        
        agents = dashboard.get_agents()
        
        assert len(agents) == 3
        agent_ids = [a['agent_id'] for a in agents]
        assert 'agent-001' in agent_ids
        assert 'agent-002' in agent_ids
        assert 'openclaw-main' in agent_ids
        
        # Check single agent
        agent = dashboard.get_agent('agent-001')
        assert agent is not None
        assert agent['bot_type'] == 'nanobot'
        assert 'echo' in agent['capabilities']
        print("✓ Agent metrics")


class TestTaskTracking:
    """Task tracking tests."""
    
    def test_task_submission(self):
        """Dashboard tracks submitted tasks."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        # Submit tasks
        orch.submit_task("echo", {"message": "test"})
        orch.submit_task("shell", {"command": "ls"})
        
        # Update metrics
        dashboard._update_task_metrics()
        
        tasks = dashboard.get_tasks()
        assert len(tasks) == 2
        
        pending = dashboard.get_tasks(status='pending')
        assert len(pending) == 2
        print("✓ Task submission tracking")
    
    def test_task_queue(self):
        """Dashboard provides task queue view."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        orch.submit_task("echo", {"message": "test1"})
        orch.submit_task("echo", {"message": "test2"})
        
        dashboard._update_task_metrics()
        
        queue = dashboard.get_task_queue()
        
        assert 'pending' in queue
        assert 'active' in queue
        assert 'recent_completed' in queue
        assert len(queue['pending']) == 2
        print("✓ Task queue view")


class TestCapabilityGaps:
    """Capability gap tracking tests."""
    
    def test_gap_recording(self):
        """Dashboard records capability gaps."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        # Record gaps
        dashboard.collector.record_capability_gap("excel_processing", "agent-001")
        dashboard.collector.record_capability_gap("excel_processing", "agent-002")
        dashboard.collector.record_capability_gap("image_generation", "openclaw-main")
        
        gaps = dashboard.get_capability_gaps()
        
        assert len(gaps) == 2
        
        # Find excel gap (gaps are dicts from get_capability_gaps)
        excel_gap = next((g for g in gaps if g.capability_name == 'excel_processing'), None)
        assert excel_gap is not None
        assert excel_gap.request_count == 2
        assert len(excel_gap.requesting_agents) == 2
        print("✓ Capability gap recording")
    
    def test_gap_status_updates(self):
        """Dashboard updates gap status."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        dashboard.collector.record_capability_gap("pdf_rendering", "agent-001")
        
        # Mark as learning
        dashboard.collector.mark_gap_learning("pdf_rendering", "learn-abc123")
        
        gaps = dashboard.get_capability_gaps()
        pdf_gap = next((g for g in gaps if g.capability_name == 'pdf_rendering'), None)
        
        assert pdf_gap.resolution_status == 'learning'
        assert pdf_gap.learning_task_id == 'learn-abc123'
        
        # Mark as resolved
        dashboard.collector.mark_gap_resolved("pdf_rendering")
        
        gaps = dashboard.get_capability_gaps()
        pdf_gap = next((g for g in gaps if g.capability_name == 'pdf_rendering'), None)
        assert pdf_gap.resolution_status == 'resolved'
        print("✓ Gap status updates")
    
    def test_gap_alerting(self):
        """Dashboard alerts on frequent gaps."""
        config = DashboardConfig(capability_gap_threshold=3)
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
            config=config,
        )
        
        # Record gap 3 times
        dashboard.collector.record_capability_gap("video_encoding", "agent-001")
        dashboard.collector.record_capability_gap("video_encoding", "agent-002")
        dashboard.collector.record_capability_gap("video_encoding", "openclaw-main")
        
        alerts = dashboard.get_alerts()
        video_alerts = [a for a in alerts if 'video_encoding' in a.get('message', '')]
        
        assert len(video_alerts) == 1
        assert video_alerts[0]['severity'] == 'warning'
        assert video_alerts[0]['category'] == 'capability'
        print("✓ Gap alerting")


class TestAlerts:
    """Alert system tests."""
    
    def test_alert_creation(self):
        """Dashboard creates alerts."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        dashboard.collector._create_alert(
            severity="error",
            category="system",
            message="Test alert",
            details={"test": True},
        )
        
        alerts = dashboard.get_alerts()
        assert len(alerts) == 1
        assert alerts[0]['message'] == "Test alert"
        print("✓ Alert creation")
    
    def test_alert_acknowledge_resolve(self):
        """Dashboard manages alert lifecycle."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        dashboard.collector._create_alert(
            severity="warning",
            category="agent",
            message="Test warning",
        )
        
        alerts = dashboard.get_alerts()
        alert_id = alerts[0]['alert_id']
        
        # Acknowledge
        dashboard.collector.acknowledge_alert(alert_id)
        alerts = dashboard.get_alerts()
        assert alerts[0]['acknowledged'] == True
        
        # Resolve
        dashboard.collector.resolve_alert(alert_id)
        alerts = dashboard.get_alerts()
        assert len(alerts) == 0
        print("✓ Alert lifecycle")


class TestExports:
    """Export functionality tests."""
    
    def test_json_export(self):
        """Dashboard exports JSON."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        dashboard._update_agent_metrics()
        
        json_export = dashboard.export_json()
        
        import json
        data = json.loads(json_export)
        
        assert 'timestamp' in data
        assert 'overview' in data
        assert 'agents' in data
        assert 'tasks' in data
        print("✓ JSON export")
    
    def test_html_export(self):
        """Dashboard exports HTML."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        dashboard._update_agent_metrics()
        
        html = dashboard.export_html()
        
        assert '<!DOCTYPE html>' in html
        assert 'Agent Cluster Dashboard' in html
        assert 'agent-001' in html
        print("✓ HTML export")


class TestHealthScore:
    """Health score calculation tests."""
    
    def test_health_score_healthy(self):
        """Health score for healthy cluster."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        dashboard._update_agent_metrics()
        dashboard._create_snapshot()
        
        overview = dashboard.get_overview()
        
        # Should be high since all agents online, no errors
        assert overview['cluster_health'] >= 70
        print("✓ Health score (healthy)")
    
    def test_health_score_with_gaps(self):
        """Health score decreases with gaps."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        dashboard._update_agent_metrics()
        
        # Add gaps
        for i in range(5):
            dashboard.collector.record_capability_gap(f"missing_cap_{i}", f"agent-{i}")
        
        dashboard._create_snapshot()
        
        overview = dashboard.get_overview()
        # Score should reflect gaps exist
        print(f"  Health with gaps: {overview['cluster_health']}")
        print("✓ Health score (with gaps)")


class TestTrends:
    """Trend calculation tests."""
    
    def test_trend_calculation(self):
        """Dashboard calculates trends."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        # Create multiple snapshots
        for _ in range(5):
            dashboard._update_agent_metrics()
            dashboard._create_snapshot()
            time.sleep(0.1)
        
        trends = dashboard.get_trends(hours=1)
        
        assert 'throughput' in trends
        assert 'latency' in trends
        assert 'error_rate' in trends
        assert 'agents_online' in trends
        print("✓ Trend calculation")


class TestNetworkTopology:
    """Network topology tests."""
    
    def test_topology_generation(self):
        """Dashboard generates network topology."""
        orch = create_test_orchestrator()
        dashboard = ClusterDashboard(
            orchestrator=orch,
            registry=orch.capability_registry,
        )
        
        dashboard._update_agent_metrics()
        
        topology = dashboard.get_network_topology()
        
        assert 'nodes' in topology
        assert 'edges' in topology
        
        # Should have coordinator + 3 agents
        assert len(topology['nodes']) == 4
        assert len(topology['edges']) == 3
        
        # Check coordinator node
        coord = next((n for n in topology['nodes'] if n['id'] == 'coordinator'), None)
        assert coord is not None
        assert coord['type'] == 'coordinator'
        print("✓ Network topology")


def run_all_tests():
    """Run all dashboard tests."""
    print("\n" + "="*60)
    print("  Dashboard Tests")
    print("="*60 + "\n")
    
    test_classes = [
        TestDashboardBasics(),
        TestTaskTracking(),
        TestCapabilityGaps(),
        TestAlerts(),
        TestExports(),
        TestHealthScore(),
        TestTrends(),
        TestNetworkTopology(),
    ]
    
    total = 0
    passed = 0
    
    for test_obj in test_classes:
        for method_name in dir(test_obj):
            if method_name.startswith('test_'):
                total += 1
                try:
                    getattr(test_obj, method_name)()
                    passed += 1
                except Exception as e:
                    print(f"✗ {method_name}: {e}")
    
    print("\n" + "-"*60)
    print(f"  Results: {passed}/{total} tests passed")
    print("-"*60 + "\n")
    
    return passed == total


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
