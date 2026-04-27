#!/usr/bin/env python3
"""
Cluster Dashboard Monitor
Central monitoring system for the agent cluster.

Features:
- Real-time agent status (online/offline/busy)
- Task queue visualization (pending/active/completed/failed)
- Capability gap tracking with frequency
- Auto-learning workflow progress
- Performance metrics (latency, throughput, error rates)
- Historical data and trends
- Alert system for anomalies
"""

import json
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field, asdict
from collections import defaultdict
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from capabilities.registry import CapabilityRegistry
from orchestration.cluster import ClusterOrchestrator
from autolearning.workflow import AutoLearningWorkflow, LearningStatus


@dataclass
class DashboardConfig:
    """Dashboard configuration."""
    # Update intervals (seconds)
    metrics_interval: float = 5.0
    history_interval: float = 60.0
    
    # Retention periods
    metrics_retention_hours: int = 24
    task_history_hours: int = 168  # 7 days
    
    # Alert thresholds
    heartbeat_timeout_seconds: int = 90
    task_stuck_minutes: int = 30
    error_rate_threshold: float = 0.15  # 15% errors
    capability_gap_threshold: int = 3  # Alert if requested 3+ times
    
    # Display settings
    max_recent_tasks: int = 50
    max_capability_gaps: int = 20
    top_agents_by_load: int = 10


@dataclass
class AgentMetrics:
    """Metrics for a single agent."""
    agent_id: str
    bot_type: str
    status: str  # online, offline, busy, error
    
    # Task metrics
    tasks_completed: int = 0
    tasks_failed: int = 0
    tasks_active: int = 0
    total_execution_time: float = 0.0
    
    # Performance
    avg_task_time: float = 0.0
    success_rate: float = 1.0
    
    # Health
    last_heartbeat: Optional[datetime] = None
    heartbeat_age_seconds: float = 0.0
    connection_latency_ms: float = 0.0
    
    # Capabilities
    capabilities: List[str] = field(default_factory=list)
    capability_count: int = 0
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['last_heartbeat'] = self.last_heartbeat.isoformat() if self.last_heartbeat else None
        return d


@dataclass
class TaskMetrics:
    """Metrics for a single task."""
    task_id: str
    task_type: str
    status: str  # pending, active, completed, failed
    assigned_to: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0.0
    priority: int = 0
    retries: int = 0
    error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['created_at'] = self.created_at.isoformat()
        d['started_at'] = self.started_at.isoformat() if self.started_at else None
        d['completed_at'] = self.completed_at.isoformat() if self.completed_at else None
        return d


@dataclass
class CapabilityGap:
    """A missing capability that was requested."""
    capability_name: str
    request_count: int = 0
    first_requested: Optional[datetime] = None
    last_requested: Optional[datetime] = None
    requesting_agents: Set[str] = field(default_factory=set)
    resolution_status: str = "open"  # open, learning, resolved
    learning_task_id: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            'capability_name': self.capability_name,
            'request_count': self.request_count,
            'first_requested': self.first_requested.isoformat() if self.first_requested else None,
            'last_requested': self.last_requested.isoformat() if self.last_requested else None,
            'requesting_agents': list(self.requesting_agents),
            'resolution_status': self.resolution_status,
            'learning_task_id': self.learning_task_id,
        }


@dataclass
class ClusterSnapshot:
    """Point-in-time snapshot of cluster state."""
    timestamp: datetime
    total_agents: int
    online_agents: int
    busy_agents: int
    offline_agents: int
    
    pending_tasks: int
    active_tasks: int
    completed_tasks_24h: int
    failed_tasks_24h: int
    
    total_capabilities: int
    capability_gaps: int
    
    throughput_tps: float  # Tasks per second
    avg_latency_seconds: float
    error_rate: float
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        return d


@dataclass
class Alert:
    """Dashboard alert."""
    alert_id: str
    severity: str  # info, warning, error, critical
    category: str  # agent, task, capability, system
    message: str
    details: Dict[str, Any]
    created_at: datetime
    acknowledged: bool = False
    resolved_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['created_at'] = self.created_at.isoformat()
        d['resolved_at'] = self.resolved_at.isoformat() if self.resolved_at else None
        return d


class MetricsCollector:
    """Collects and aggregates metrics from cluster components."""
    
    def __init__(self, config: DashboardConfig):
        self.config = config
        self._lock = threading.Lock()
        
        # Time series data
        self.snapshots: List[ClusterSnapshot] = []
        self.agent_history: Dict[str, List[AgentMetrics]] = defaultdict(list)
        
        # Capability gap tracking
        self.capability_gaps: Dict[str, CapabilityGap] = {}
        
        # Alert tracking
        self.alerts: List[Alert] = []
        self.active_alerts: Dict[str, Alert] = {}
        
    def record_capability_gap(self, capability_name: str, requesting_agent: str):
        """Record a capability gap request."""
        with self._lock:
            if capability_name not in self.capability_gaps:
                self.capability_gaps[capability_name] = CapabilityGap(
                    capability_name=capability_name,
                    first_requested=datetime.now(),
                )
            
            gap = self.capability_gaps[capability_name]
            gap.request_count += 1
            gap.last_requested = datetime.now()
            gap.requesting_agents.add(requesting_agent)
            
            # Check if should alert
            if gap.request_count >= self.config.capability_gap_threshold and gap.resolution_status == "open":
                self._create_alert(
                    severity="warning",
                    category="capability",
                    message=f"Capability '{capability_name}' requested {gap.request_count} times but not available",
                    details=gap.to_dict()
                )
    
    def mark_gap_learning(self, capability_name: str, task_id: str):
        """Mark a capability gap as being learned."""
        with self._lock:
            if capability_name in self.capability_gaps:
                gap = self.capability_gaps[capability_name]
                gap.resolution_status = "learning"
                gap.learning_task_id = task_id
    
    def mark_gap_resolved(self, capability_name: str):
        """Mark a capability gap as resolved."""
        with self._lock:
            if capability_name in self.capability_gaps:
                self.capability_gaps[capability_name].resolution_status = "resolved"
    
    def _create_alert(self, severity: str, category: str, message: str, details: Dict = None):
        """Create a new alert."""
        import uuid
        alert = Alert(
            alert_id=f"alert-{uuid.uuid4().hex[:8]}",
            severity=severity,
            category=category,
            message=message,
            details=details or {},
            created_at=datetime.now(),
        )
        self.alerts.append(alert)
        self.active_alerts[alert.alert_id] = alert
    
    def acknowledge_alert(self, alert_id: str):
        """Acknowledge an alert."""
        with self._lock:
            if alert_id in self.active_alerts:
                self.active_alerts[alert_id].acknowledged = True
    
    def resolve_alert(self, alert_id: str):
        """Resolve an alert."""
        with self._lock:
            if alert_id in self.active_alerts:
                self.active_alerts[alert_id].resolved_at = datetime.now()
                del self.active_alerts[alert_id]
    
    def add_snapshot(self, snapshot: ClusterSnapshot):
        """Add a cluster snapshot."""
        with self._lock:
            self.snapshots.append(snapshot)
            # Trim old snapshots
            cutoff = datetime.now() - timedelta(hours=self.config.metrics_retention_hours)
            self.snapshots = [s for s in self.snapshots if s.timestamp > cutoff]
    
    def get_trends(self, hours: int = 1) -> Dict[str, Any]:
        """Get metric trends over time."""
        with self._lock:
            cutoff = datetime.now() - timedelta(hours=hours)
            recent = [s for s in self.snapshots if s.timestamp > cutoff]
            
            if not recent:
                return {}
            
            return {
                'time_range_hours': hours,
                'sample_count': len(recent),
                'throughput': {
                    'min': min(s.throughput_tps for s in recent),
                    'max': max(s.throughput_tps for s in recent),
                    'avg': sum(s.throughput_tps for s in recent) / len(recent),
                    'current': recent[-1].throughput_tps if recent else 0,
                },
                'latency': {
                    'min': min(s.avg_latency_seconds for s in recent),
                    'max': max(s.avg_latency_seconds for s in recent),
                    'avg': sum(s.avg_latency_seconds for s in recent) / len(recent),
                    'current': recent[-1].avg_latency_seconds if recent else 0,
                },
                'error_rate': {
                    'min': min(s.error_rate for s in recent),
                    'max': max(s.error_rate for s in recent),
                    'avg': sum(s.error_rate for s in recent) / len(recent),
                    'current': recent[-1].error_rate if recent else 0,
                },
                'agents_online': {
                    'min': min(s.online_agents for s in recent),
                    'max': max(s.online_agents for s in recent),
                    'avg': sum(s.online_agents for s in recent) / len(recent),
                    'current': recent[-1].online_agents if recent else 0,
                },
            }
    
    def get_top_capability_gaps(self, limit: int = 10) -> List[CapabilityGap]:
        """Get top capability gaps by request count."""
        with self._lock:
            sorted_gaps = sorted(
                self.capability_gaps.values(),
                key=lambda g: g.request_count,
                reverse=True
            )
            return sorted_gaps[:limit]


class ClusterDashboard:
    """
    Main dashboard for monitoring the agent cluster.
    
    Integrates with:
    - ClusterOrchestrator: Agent and task status
    - CapabilityRegistry: Capability information
    - AutoLearningWorkflow: Learning progress
    
    Provides:
    - Real-time status updates
    - Historical metrics and trends
    - Alert management
    - Export capabilities (JSON, HTML)
    """
    
    def __init__(
        self,
        orchestrator: ClusterOrchestrator,
        registry: CapabilityRegistry,
        config: DashboardConfig = None,
    ):
        self.orchestrator = orchestrator
        self.registry = registry
        self.config = config or DashboardConfig()
        self.collector = MetricsCollector(self.config)
        
        self._running = False
        self._update_thread: Optional[threading.Thread] = None
        
        # Task tracking
        self.tasks: Dict[str, TaskMetrics] = {}
        self.task_history: List[TaskMetrics] = []
        
        # Agent tracking
        self.agent_metrics: Dict[str, AgentMetrics] = {}
        
        # Learning tracking
        self.active_learning: Dict[str, Dict] = {}
        
        # Register callbacks
        self._setup_callbacks()
    
    def _setup_callbacks(self):
        """Setup callbacks for real-time updates."""
        if self.orchestrator is not None:
            self.orchestrator.on_agent_connect = self._on_agent_connect
            self.orchestrator.on_agent_disconnect = self._on_agent_disconnect
            self.orchestrator.on_task_complete = self._on_task_complete
    
    def _on_agent_connect(self, agent_id: str, config):
        """Handle agent connection."""
        self._update_agent_metrics()
    
    def _on_agent_disconnect(self, agent_id: str):
        """Handle agent disconnection."""
        self._update_agent_metrics()
    
    def _on_task_complete(self, task_id: str, result: Any):
        """Handle task completion."""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            task.status = "completed"
            task.completed_at = datetime.now()
            if task.started_at:
                task.duration_seconds = (task.completed_at - task.started_at).total_seconds()
            
            # Move to history
            self.task_history.append(task)
            del self.tasks[task_id]
            
            # Trim history
            if len(self.task_history) > self.config.max_recent_tasks:
                self.task_history = self.task_history[-self.config.max_recent_tasks:]
    
    def start(self):
        """Start the dashboard monitoring."""
        if self._running:
            return
        
        self._running = True
        self._update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self._update_thread.start()
    
    def stop(self):
        """Stop the dashboard."""
        self._running = False
        if self._update_thread:
            self._update_thread.join(timeout=5)
    
    def _update_loop(self):
        """Main update loop."""
        while self._running:
            try:
                self._update_metrics()
                self._check_alerts()
            except Exception as e:
                print(f"[Dashboard] Update error: {e}")
            
            time.sleep(self.config.metrics_interval)
    
    def _update_metrics(self):
        """Update all metrics."""
        try:
            self._update_agent_metrics()
        except Exception:
            pass  # Orchestrator may be unavailable
        try:
            self._update_task_metrics()
        except Exception:
            pass
        try:
            self._create_snapshot()
        except Exception:
            pass
    
    def _update_agent_metrics(self):
        """Update agent metrics from orchestrator."""
        status = self.orchestrator.get_status()
        now = datetime.now()
        
        for agent_id, agent_data in status.get('agents', {}).items():
            if agent_id not in self.agent_metrics:
                self.agent_metrics[agent_id] = AgentMetrics(
                    agent_id=agent_id,
                    bot_type=agent_data.get('bot_type', 'unknown'),
                    status='online',
                )
            
            metrics = self.agent_metrics[agent_id]
            metrics.status = agent_data.get('status', 'unknown')
            metrics.tasks_completed = agent_data.get('completed', 0)
            metrics.tasks_failed = agent_data.get('failed', 0)
            metrics.tasks_active = agent_data.get('current_tasks', 0)
            metrics.capability_count = len(agent_data.get('capabilities', []))
            metrics.capabilities = agent_data.get('capabilities', [])
            
            # Calculate success rate
            total = metrics.tasks_completed + metrics.tasks_failed
            metrics.success_rate = metrics.tasks_completed / total if total > 0 else 1.0
            
            # Calculate avg task time
            if metrics.tasks_completed > 0:
                # Would need actual timing data from orchestrator
                pass
            
            # Heartbeat age
            last_hb = agent_data.get('last_heartbeat')
            if last_hb:
                try:
                    if isinstance(last_hb, str):
                        metrics.last_heartbeat = datetime.fromisoformat(last_hb)
                    else:
                        metrics.last_heartbeat = last_hb
                    metrics.heartbeat_age_seconds = (now - metrics.last_heartbeat).total_seconds()
                except:
                    pass
            
            # Determine status
            if metrics.heartbeat_age_seconds > self.config.heartbeat_timeout_seconds:
                metrics.status = 'offline'
            elif metrics.tasks_active > 0:
                metrics.status = 'busy'
            else:
                metrics.status = 'online'
    
    def _update_task_metrics(self):
        """Update task metrics from orchestrator."""
        # Get pending tasks
        for task_id in self.orchestrator.pending_tasks:
            if task_id not in self.tasks:
                task_data = self.orchestrator.tasks.get(task_id, {})
                self.tasks[task_id] = TaskMetrics(
                    task_id=task_id,
                    task_type=task_data.get('task_type', 'unknown'),
                    status='pending',
                    created_at=task_data.get('created_at', datetime.now()),
                    priority=task_data.get('priority', 0),
                )
        
        # Update assigned tasks
        for task_id, task in self.orchestrator.tasks.items():
            if task_id in self.tasks:
                self.tasks[task_id].assigned_to = task.get('assigned_to')
                if task.get('started_at') and not self.tasks[task_id].started_at:
                    self.tasks[task_id].started_at = task.get('started_at')
                    self.tasks[task_id].status = 'active'
    
    def _create_snapshot(self):
        """Create a cluster snapshot."""
        now = datetime.now()
        
        # Calculate metrics
        online = sum(1 for m in self.agent_metrics.values() if m.status in ('online', 'busy'))
        busy = sum(1 for m in self.agent_metrics.values() if m.status == 'busy')
        offline = sum(1 for m in self.agent_metrics.values() if m.status == 'offline')
        
        pending = sum(1 for t in self.tasks.values() if t.status == 'pending')
        active = sum(1 for t in self.tasks.values() if t.status == 'active')
        
        # Combine task_history and self.tasks for metrics
        all_completed = list(self.task_history)
        for t in self.tasks.values():
            if t.status == 'completed' and t not in all_completed:
                all_completed.append(t)

        # Calculate throughput with timezone-safe comparison
        def _age(t):
            if not t.completed_at:
                return float('inf')
            ca = t.completed_at
            n = now
            if ca.tzinfo is not None and n.tzinfo is None:
                from datetime import timezone
                n = n.replace(tzinfo=timezone.utc)
            elif ca.tzinfo is None and n.tzinfo is not None:
                from datetime import timezone
                ca = ca.replace(tzinfo=timezone.utc)
            return (n - ca).total_seconds()

        recent_completed = [t for t in all_completed if _age(t) < 3600]
        window = 3600
        if not recent_completed:
            recent_completed = [t for t in all_completed if _age(t) < 86400]
            window = 86400
        throughput = len(recent_completed) / float(window) if recent_completed else 0

        # Calculate average latency
        completed_with_duration = [t for t in recent_completed if t.duration_seconds > 0]
        avg_latency = sum(t.duration_seconds for t in completed_with_duration) / len(completed_with_duration) if completed_with_duration else 0

        # Calculate error rate
        recent_failed = [t for t in all_completed if t.status == 'failed' and _age(t) < 86400]
        total_recent = len(recent_completed) + len(recent_failed)
        error_rate = len(recent_failed) / total_recent if total_recent > 0 else 0
        
        snapshot = ClusterSnapshot(
            timestamp=now,
            total_agents=len(self.agent_metrics),
            online_agents=online,
            busy_agents=busy,
            offline_agents=offline,
            pending_tasks=pending,
            active_tasks=active,
            completed_tasks_24h=sum(1 for t in all_completed if _age(t) < 86400),
            failed_tasks_24h=sum(1 for t in all_completed if t.status == 'failed' and _age(t) < 86400),
            total_capabilities=len(self.registry.list_all_capabilities()),
            capability_gaps=len(self.collector.capability_gaps),
            throughput_tps=throughput,
            avg_latency_seconds=avg_latency,
            error_rate=error_rate,
        )
        
        self.collector.add_snapshot(snapshot)
    
    def _check_alerts(self):
        """Check for alert conditions."""
        now = datetime.now()
        
        # Check for offline agents
        for agent_id, metrics in self.agent_metrics.items():
            if metrics.status == 'offline':
                alert_key = f"agent_offline_{agent_id}"
                if alert_key not in self.collector.active_alerts:
                    self.collector._create_alert(
                        severity="warning",
                        category="agent",
                        message=f"Agent '{agent_id}' is offline",
                        details={'agent_id': agent_id, 'offline_since': metrics.last_heartbeat.isoformat() if metrics.last_heartbeat else None}
                    )
        
        # Check for stuck tasks
        for task_id, task in self.tasks.items():
            if task.status == 'pending':
                age_minutes = (now - task.created_at).total_seconds() / 60
                if age_minutes > self.config.task_stuck_minutes:
                    alert_key = f"task_stuck_{task_id}"
                    if alert_key not in self.collector.active_alerts:
                        self.collector._create_alert(
                            severity="warning",
                            category="task",
                            message=f"Task '{task_id}' stuck in pending for {age_minutes:.0f} minutes",
                            details={'task_id': task_id, 'task_type': task.task_type, 'age_minutes': age_minutes}
                        )
        
        # Check error rate
        recent = self.collector.snapshots[-1] if self.collector.snapshots else None
        if recent and recent.error_rate > self.config.error_rate_threshold:
            alert_key = "high_error_rate"
            if alert_key not in self.collector.active_alerts:
                self.collector._create_alert(
                    severity="error",
                    category="system",
                    message=f"Error rate elevated: {recent.error_rate:.1%}",
                    details={'error_rate': recent.error_rate, 'threshold': self.config.error_rate_threshold}
                )
    
    # === Public API ===
    
    def get_overview(self) -> Dict[str, Any]:
        """Get cluster overview."""
        snapshot = self.collector.snapshots[-1] if self.collector.snapshots else None
        
        return {
            'cluster_health': self._calculate_health_score(),
            'agents': {
                'total': len(self.agent_metrics),
                'online': sum(1 for m in self.agent_metrics.values() if m.status in ('online', 'busy')),
                'busy': sum(1 for m in self.agent_metrics.values() if m.status == 'busy'),
                'offline': sum(1 for m in self.agent_metrics.values() if m.status == 'offline'),
            },
            'tasks': {
                'pending': sum(1 for t in self.tasks.values() if t.status == 'pending'),
                'active': sum(1 for t in self.tasks.values() if t.status == 'active'),
                'completed_24h': (snapshot.completed_tasks_24h if snapshot else 0) or sum(1 for t in self.tasks.values() if t.status == 'completed'),
                'failed_24h': (snapshot.failed_tasks_24h if snapshot else 0) or sum(1 for t in self.tasks.values() if t.status == 'failed'),
            },
            'capabilities': {
                'total': len(self.registry.list_all_capabilities()),
                'gaps': len(self.collector.capability_gaps),
            },
            'performance': {
                'throughput_tps': snapshot.throughput_tps if snapshot else 0,
                'avg_latency_s': snapshot.avg_latency_seconds if snapshot else 0,
                'error_rate': snapshot.error_rate if snapshot else 0,
            },
            'alerts': {
                'active': len(self.collector.active_alerts),
                'unacknowledged': sum(1 for a in self.collector.active_alerts.values() if not a.acknowledged),
            },
        }
    
    def _calculate_health_score(self) -> float:
        """Calculate overall cluster health score (0-100)."""
        if not self.agent_metrics:
            return 0.0
        
        scores = []
        
        # Agent availability (40% weight)
        online_ratio = sum(1 for m in self.agent_metrics.values() if m.status in ('online', 'busy')) / len(self.agent_metrics)
        scores.append(online_ratio * 40)
        
        # Error rate (30% weight)
        snapshot = self.collector.snapshots[-1] if self.collector.snapshots else None
        if snapshot:
            error_penalty = min(snapshot.error_rate * 100, 30)
            scores.append(30 - error_penalty)
        else:
            scores.append(30)
        
        # Capability coverage (20% weight)
        total_caps = len(self.registry.list_all_capabilities())
        gaps = len(self.collector.capability_gaps)
        if total_caps > 0:
            coverage = (total_caps - gaps) / total_caps
            scores.append(coverage * 20)
        else:
            scores.append(10)  # Partial credit if no caps defined
        
        # Task throughput (10% weight)
        if snapshot and snapshot.throughput_tps > 0:
            scores.append(10)
        else:
            scores.append(5)
        
        return sum(scores)
    
    def get_agents(self) -> List[Dict[str, Any]]:
        """Get all agent details."""
        return [m.to_dict() for m in self.agent_metrics.values()]
    
    def get_agent(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get single agent details."""
        if agent_id in self.agent_metrics:
            return self.agent_metrics[agent_id].to_dict()
        return None
    
    def get_tasks(self, status: str = None) -> List[Dict[str, Any]]:
        """Get tasks, optionally filtered by status."""
        tasks = list(self.tasks.values())
        if status:
            tasks = [t for t in tasks if t.status == status]
        return [t.to_dict() for t in tasks]
    
    def get_task_queue(self) -> Dict[str, List[Dict]]:
        """Get task queue by status."""
        return {
            'pending': [t.to_dict() for t in self.tasks.values() if t.status == 'pending'],
            'active': [t.to_dict() for t in self.tasks.values() if t.status == 'active'],
            'recent_completed': [t.to_dict() for t in self.task_history[-20:]],
        }
    
    def get_capability_gaps(self) -> List[Dict[str, Any]]:
        """Get capability gaps."""
        return self.collector.get_top_capability_gaps(self.config.max_capability_gaps)
    
    def get_capabilities(self) -> Dict[str, Any]:
        """Get capability registry overview."""
        all_caps = self.registry.list_all_capabilities()
        
        # Group by category
        by_category = {}
        for cap in all_caps:
            # This would need actual category tracking from registry
            by_category.setdefault('general', []).append(cap)
        
        # Get agents per capability
        cap_agents = {}
        for cap in all_caps:
            agents = self.registry.get_capability_agents(cap)
            cap_agents[cap] = [a.agent_id for a in agents]
        
        return {
            'total_capabilities': len(all_caps),
            'capabilities': all_caps,
            'by_category': by_category,
            'agents_per_capability': cap_agents,
            'gaps': self.get_capability_gaps(),
        }
    
    def get_learning_status(self, workflow: AutoLearningWorkflow = None) -> Dict[str, Any]:
        """Get auto-learning workflow status."""
        if not workflow:
            return {'active_tasks': [], 'recent_completed': []}
        
        active = workflow.get_active_tasks()
        
        return {
            'active_tasks': [t.to_dict() for t in active],
            'recent_completed': [t.to_dict() for t in list(workflow.completed_tasks.values())[-10:]],
            'statistics': {
                'total_active': len(active),
                'total_completed': len(workflow.completed_tasks),
            }
        }
    
    def get_alerts(self, include_resolved: bool = False) -> List[Dict[str, Any]]:
        """Get alerts."""
        alerts = list(self.collector.active_alerts.values())
        if include_resolved:
            alerts.extend([a for a in self.collector.alerts if a.resolved_at])
        return [a.to_dict() for a in sorted(alerts, key=lambda a: a.created_at, reverse=True)]
    
    def get_trends(self, hours: int = 1) -> Dict[str, Any]:
        """Get metric trends."""
        return self.collector.get_trends(hours)
    
    def get_network_topology(self) -> Dict[str, Any]:
        """Get network connection topology."""
        # This would integrate with network layer
        nodes = []
        edges = []
        
        # Add coordinator node
        nodes.append({
            'id': 'coordinator',
            'type': 'coordinator',
            'status': 'active',
        })
        
        # Add agent nodes
        for agent_id, metrics in self.agent_metrics.items():
            nodes.append({
                'id': agent_id,
                'type': 'agent',
                'status': metrics.status,
                'capabilities': metrics.capability_count,
            })
            edges.append({
                'source': 'coordinator',
                'target': agent_id,
                'status': 'active' if metrics.status != 'offline' else 'inactive',
            })
        
        return {
            'nodes': nodes,
            'edges': edges,
        }
    
    def export_json(self) -> str:
        """Export full dashboard state as JSON."""
        return json.dumps({
            'timestamp': datetime.now().isoformat(),
            'overview': self.get_overview(),
            'agents': self.get_agents(),
            'tasks': self.get_task_queue(),
            'capabilities': self.get_capabilities(),
            'gaps': self.get_capability_gaps(),
            'alerts': self.get_alerts(),
            'trends': self.get_trends(),
            'topology': self.get_network_topology(),
        }, indent=2)
    

    def _render_task_assignment_html(self) -> str:
        """Render task assignment form HTML (not inside f-string, so braces are literal)."""
        return """
 <!-- Task Assignment -->
 <div class="card" style="margin-top: 20px;">
 <h2>Assign Task to Cluster</h2>
 <div style="display: flex; gap: 10px; flex-wrap: wrap; align-items: end;">
 <div style="flex: 1; min-width: 200px;">
 <label style="font-size: 12px; color: #666;">Capability</label><br>
 <select id="task-capability" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
 <option value="web">Web</option>
 <option value="file">File</option>
 <option value="data">Data</option>
 <option value="ai">AI</option>
 <option value="legal">Legal</option>
 <option value="research">Research</option>
 </select>
 </div>
 <div style="flex: 2; min-width: 200px;">
 <label style="font-size: 12px; color: #666;">Task Description</label><br>
 <input id="task-desc" type="text" placeholder="Describe the task..." style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
 </div>
 <div style="flex: 1; min-width: 150px;">
 <label style="font-size: 12px; color: #666;">Assign to (optional)</label><br>
 <input id="task-agent" type="text" placeholder="Any available" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
 </div>
 <button onclick="assignTask()" style="padding: 8px 20px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 14px;">Assign</button>
 </div>
 <div id="task-result" style="margin-top: 10px; font-size: 13px;"></div>
 </div>
 <script>
 function assignTask() {
   var cap = document.getElementById('task-capability').value;
   var desc = document.getElementById('task-desc').value;
   var agent = document.getElementById('task-agent').value;
   var resultDiv = document.getElementById('task-result');
   resultDiv.innerHTML = '<em>Submitting...</em>';
   fetch('/api/tasks/assign', {
     method: 'POST',
     headers: {'Content-Type': 'application/json'},
     body: JSON.stringify({capability: cap, task_data: {description: desc}, agent_id: agent || null})
   })
   .then(function(r) { return r.json(); })
   .then(function(d) {
     if (d.error) { resultDiv.innerHTML = '<span style="color: #ef4444;">Error: ' + d.error + '</span>'; }
     else { resultDiv.innerHTML = '<span style="color: #22c55e;">Assigned! Task ID: ' + (d.task_id || 'N/A') + '</span>'; document.getElementById('task-desc').value = ''; }
   })
   .catch(function(e) { resultDiv.innerHTML = '<span style="color: #ef4444;">Failed: ' + e + '</span>'; });
 }
 </script>
"""

    def export_html(self) -> str:
        """Export dashboard as standalone HTML."""
        data = json.loads(self.export_json())
        
        html = f'''<!DOCTYPE html>
<html>
<head>
    <title>Agent Cluster Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; }}
        .dashboard {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        h1 {{ margin-bottom: 20px; color: #333; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
        .card {{ background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .card h2 {{ font-size: 14px; text-transform: uppercase; color: #666; margin-bottom: 10px; }}
        .metric {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #eee; }}
        .metric:last-child {{ border-bottom: none; }}
        .metric-value {{ font-weight: 600; }}
        .health-score {{ font-size: 48px; font-weight: bold; text-align: center; margin: 20px 0; }}
        .health-excellent {{ color: #22c55e; }}
        .health-good {{ color: #84cc16; }}
        .health-warning {{ color: #eab308; }}
        .health-critical {{ color: #ef4444; }}
        .status-badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 500; }}
        .status-online {{ background: #dcfce7; color: #166534; }}
        .status-busy {{ background: #fef3c7; color: #92400e; }}
        .status-offline {{ background: #fee2e2; color: #991b1b; }}
        .alert {{ padding: 10px; margin: 5px 0; border-radius: 4px; border-left: 4px solid; }}
        .alert-warning {{ background: #fef3c7; border-color: #f59e0b; }}
        .alert-error {{ background: #fee2e2; border-color: #ef4444; }}
        .alert-critical {{ background: #fecaca; border-color: #dc2626; }}
        .gap-item {{ display: flex; justify-content: space-between; align-items: center; padding: 8px 0; }}
        .gap-count {{ background: #dbeafe; color: #1e40af; padding: 2px 8px; border-radius: 12px; font-size: 12px; }}
        .timestamp {{ color: #999; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="dashboard">
        <h1>🎯 Agent Cluster Dashboard</h1>
        <p class="timestamp">Last updated: {data['timestamp']}</p>
        
        <div class="grid">
            <!-- Health Score -->
            <div class="card">
                <h2>Cluster Health</h2>
                <div class="health-score {self._health_class(data['overview']['cluster_health'])}">{data['overview']['cluster_health']:.0f}</div>
                <p style="text-align: center; color: #666;">Overall health score</p>
            </div>
            
            <!-- Agents Overview -->
            <div class="card">
                <h2>Agents</h2>
                <div class="metric"><span>Total Agents</span><span class="metric-value">{data['overview']['agents']['total']}</span></div>
                <div class="metric"><span>Online</span><span class="metric-value" style="color: #22c55e;">{data['overview']['agents']['online']}</span></div>
                <div class="metric"><span>Busy</span><span class="metric-value" style="color: #eab308;">{data['overview']['agents']['busy']}</span></div>
                <div class="metric"><span>Offline</span><span class="metric-value" style="color: #ef4444;">{data['overview']['agents']['offline']}</span></div>
            </div>
            
            <!-- Tasks Overview -->
            <div class="card">
                <h2>Tasks</h2>
                <div class="metric"><span>Pending</span><span class="metric-value">{data['overview']['tasks']['pending']}</span></div>
                <div class="metric"><span>Active</span><span class="metric-value">{data['overview']['tasks']['active']}</span></div>
                <div class="metric"><span>Completed (24h)</span><span class="metric-value" style="color: #22c55e;">{data['overview']['tasks']['completed_24h']}</span></div>
                <div class="metric"><span>Failed (24h)</span><span class="metric-value" style="color: #ef4444;">{data['overview']['tasks']['failed_24h']}</span></div>
            </div>
            
            <!-- Performance -->
            <div class="card">
                <h2>Performance</h2>
                <div class="metric"><span>Throughput</span><span class="metric-value">{data['overview']['performance']['throughput_tps']:.2f} tps</span></div>
                <div class="metric"><span>Avg Latency</span><span class="metric-value">{data['overview']['performance']['avg_latency_s']:.2f}s</span></div>
                <div class="metric"><span>Error Rate</span><span class="metric-value">{data['overview']['performance']['error_rate']*100:.1f}%</span></div>
            </div>
            
            <!-- Capability Gaps -->
            <div class="card">
                <h2>Capability Gaps ({len(data['gaps'])})</h2>
                {self._render_gaps_html(data['gaps'][:5])}
            </div>
            
            <!-- Active Alerts -->
            <div class="card">
                <h2>Active Alerts ({data['overview']['alerts']['active']})</h2>
                {self._render_alerts_html(data['alerts'][:5])}
            </div>
        </div>
        
        <!-- Agent List -->
        <div class="card" style="margin-top: 20px;">
            <h2>Agent Details</h2>
            {self._render_agents_html(data['agents'])}
        </div>
    </div>
    
    <script>
        // Auto-refresh every 30 seconds
        setTimeout(() => location.reload(), 30000);
    </script>
</body>
</html>'''
        return html
    
    def _health_class(self, score: float) -> str:
        if score >= 80: return 'health-excellent'
        if score >= 60: return 'health-good'
        if score >= 40: return 'health-warning'
        return 'health-critical'
    
    def _render_gaps_html(self, gaps: List[Dict]) -> str:
        if not gaps:
            return '<p style="color: #999;">No capability gaps</p>'
        html = ''
        for gap in gaps:
            html += f'''<div class="gap-item">
                <span>{gap['capability_name']}</span>
                <span class="gap-count">{gap['request_count']} requests</span>
            </div>'''
        return html
    
    def _render_alerts_html(self, alerts: List[Dict]) -> str:
        if not alerts:
            return '<p style="color: #999;">No active alerts</p>'
        html = ''
        for alert in alerts:
            html += f'''<div class="alert alert-{alert['severity']}">
                <strong>{alert['severity'].upper()}:</strong> {alert['message']}
            </div>'''
        return html
    
    def _render_agents_html(self, agents: List[Dict]) -> str:
        if not agents:
            return '<p style="color: #999;">No agents registered</p>'
        html = '<table style="width: 100%; border-collapse: collapse;"><thead><tr>'
        html += '<th style="text-align: left; padding: 8px;">Agent ID</th>'
        html += '<th style="text-align: left; padding: 8px;">Type</th>'
        html += '<th style="text-align: left; padding: 8px;">Status</th>'
        html += '<th style="text-align: right; padding: 8px;">Completed</th>'
        html += '<th style="text-align: right; padding: 8px;">Failed</th>'
        html += '<th style="text-align: right; padding: 8px;">Capabilities</th>'
        html += '</tr></thead><tbody>'
        for agent in agents:
            status_class = f"status-{agent['status']}"
            html += f'''<tr>
                <td style="padding: 8px;">{agent['agent_id']}</td>
                <td style="padding: 8px;">{agent['bot_type']}</td>
                <td style="padding: 8px;"><span class="status-badge {status_class}">{agent['status']}</span></td>
                <td style="text-align: right; padding: 8px;">{agent['tasks_completed']}</td>
                <td style="text-align: right; padding: 8px;">{agent['tasks_failed']}</td>
                <td style="text-align: right; padding: 8px;">{agent['capability_count']}</td>
            </tr>'''
        html += '</tbody></table>'
        return html
