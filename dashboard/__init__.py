# Agent Cluster Dashboard
"""
Real-time monitoring dashboard for the Agent Cluster MVP.

Components:
- ClusterHealthMonitor: Overall cluster health
- AgentStatusPanel: Per-agent status and metrics
- TaskQueueVisualizer: Pending/active/completed tasks
- CapabilityGapTracker: Missing capabilities with frequency
- AutoLearningProgress: Active learning workflows
- NetworkTopologyView: Agent connections and latency
"""

from dashboard.monitor import ClusterDashboard, DashboardConfig
from dashboard.api import DashboardAPI

__all__ = ['ClusterDashboard', 'DashboardConfig', 'DashboardAPI']
