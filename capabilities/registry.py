#!/usr/bin/env python3
"""
Capability Registry - Central tracking of all agent capabilities.

Features:
- Track capabilities across all agents
- Capability metadata (requirements, performance stats)
- Search/query capabilities
- Capability versioning
"""

import json
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field, asdict
from collections import defaultdict
from pathlib import Path


@dataclass
class CapabilityMetadata:
    """Metadata about a specific capability."""
    name: str
    description: str = ""
    version: str = "1.0.0"
    requirements: List[str] = field(default_factory=list)  # packages, tools needed
    inputs: List[str] = field(default_factory=list)  # expected input types
    outputs: List[str] = field(default_factory=list)  # output types
    avg_execution_time: float = 0.0  # seconds
    success_rate: float = 1.0
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['last_updated'] = self.last_updated.isoformat()
        return d


@dataclass
class AgentCapabilityRecord:
    """Record of an agent's capability with performance metrics."""
    agent_id: str
    capability_name: str
    confidence: float
    executions: int = 0
    successes: int = 0
    avg_time: float = 0.0
    last_used: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def record_execution(self, success: bool, execution_time: float):
        """Record a capability execution."""
        self.executions += 1
        if success:
            self.successes += 1
        # Rolling average
        self.avg_time = (self.avg_time * (self.executions - 1) + execution_time) / self.executions
        self.last_used = datetime.now(timezone.utc)
    
    @property
    def success_rate(self) -> float:
        if self.executions == 0:
            return 1.0
        return self.successes / self.executions
    
    def to_dict(self) -> Dict:
        return {
            'agent_id': self.agent_id,
            'capability_name': self.capability_name,
            'confidence': self.confidence,
            'executions': self.executions,
            'successes': self.successes,
            'avg_time': self.avg_time,
            'success_rate': self.success_rate,
            'last_used': self.last_used.isoformat() if self.last_used else None,
            'metadata': self.metadata
        }


class CapabilityRegistry:
    """
    Central registry for all agent capabilities.
    
    Features:
    - Register/deregister capabilities
    - Query by capability name, type, or semantic search
    - Track performance metrics per agent per capability
    - Capability versioning and dependencies
    """
    
    def __init__(self, storage_path: Optional[str] = None):
        self.storage_path = Path(storage_path) if storage_path else None
        self._lock = threading.RLock()
        
        # Capability definitions (what the capability is)
        self.capability_definitions: Dict[str, CapabilityMetadata] = {}
        
        # Agent capability records (who has what, with performance)
        self.agent_capabilities: Dict[str, AgentCapabilityRecord] = {}  # f"{agent_id}:{cap_name}" -> record
        
        # Indexes for fast lookup
        self._by_agent: Dict[str, Set[str]] = defaultdict(set)  # agent_id -> capability names
        self._by_capability: Dict[str, Set[str]] = defaultdict(set)  # cap_name -> agent_ids
        
        # Capability categories (for semantic grouping)
        self.categories: Dict[str, Set[str]] = defaultdict(set)  # category -> capability names
        
        # Load from storage if available
        if self.storage_path and self.storage_path.exists():
            self._load()
    
    def register_capability(
        self,
        agent_id: str,
        capability_name: str,
        confidence: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> AgentCapabilityRecord:
        """Register a capability for an agent."""
        with self._lock:
            key = f"{agent_id}:{capability_name}"
            
            if key in self.agent_capabilities:
                # Update existing
                record = self.agent_capabilities[key]
                record.confidence = confidence
                if metadata:
                    record.metadata.update(metadata)
            else:
                # Create new
                record = AgentCapabilityRecord(
                    agent_id=agent_id,
                    capability_name=capability_name,
                    confidence=confidence,
                    metadata=metadata or {}
                )
                self.agent_capabilities[key] = record
            
            # Update indexes
            self._by_agent[agent_id].add(capability_name)
            self._by_capability[capability_name].add(agent_id)
            
            self._save()
            return record
    
    def deregister_capability(self, agent_id: str, capability_name: str) -> bool:
        """Remove a capability from an agent."""
        with self._lock:
            key = f"{agent_id}:{capability_name}"
            if key not in self.agent_capabilities:
                return False
            
            del self.agent_capabilities[key]
            self._by_agent[agent_id].discard(capability_name)
            self._by_capability[capability_name].discard(agent_id)
            
            # Clean up empty sets
            if not self._by_agent[agent_id]:
                del self._by_agent[agent_id]
            if not self._by_capability[capability_name]:
                del self._by_capability[capability_name]
            
            self._save()
            return True
    
    def deregister_agent(self, agent_id: str) -> List[str]:
        """Remove all capabilities for an agent. Returns removed capability names."""
        with self._lock:
            if agent_id not in self._by_agent:
                return []
            
            removed = list(self._by_agent[agent_id])
            for cap_name in removed:
                key = f"{agent_id}:{cap_name}"
                self.agent_capabilities.pop(key, None)
                self._by_capability[cap_name].discard(agent_id)
            
            del self._by_agent[agent_id]
            self._save()
            return removed
    
    def get_agent_capabilities(self, agent_id: str) -> List[AgentCapabilityRecord]:
        """Get all capabilities for an agent."""
        with self._lock:
            return [
                self.agent_capabilities[f"{agent_id}:{cap}"]
                for cap in self._by_agent.get(agent_id, set())
                if f"{agent_id}:{cap}" in self.agent_capabilities
            ]
    
    def get_capability_agents(self, capability_name: str) -> List[AgentCapabilityRecord]:
        """Get all agents that have a capability."""
        with self._lock:
            return [
                self.agent_capabilities[f"{agent}:{capability_name}"]
                for agent in self._by_capability.get(capability_name, set())
                if f"{agent}:{capability_name}" in self.agent_capabilities
            ]
    
    def find_best_agent(self, capability_name: str, exclude: Optional[Set[str]] = None) -> Optional[str]:
        """Find the best agent for a capability based on confidence and performance."""
        with self._lock:
            exclude = exclude or set()
            candidates = self.get_capability_agents(capability_name)
            
            if not candidates:
                return None
            
            # Score: confidence * success_rate * (1 - load_factor)
            # Higher is better
            best_agent = None
            best_score = -1
            
            for record in candidates:
                if record.agent_id in exclude:
                    continue
                
                # Score combines confidence and historical success
                score = record.confidence * record.success_rate
                if score > best_score:
                    best_score = score
                    best_agent = record.agent_id
            
            return best_agent
    
    def query_capabilities(self, query: str, top_k: int = 10) -> List[AgentCapabilityRecord]:
        """
        Query capabilities by name (fuzzy match).
        Returns records sorted by relevance.
        """
        with self._lock:
            query_lower = query.lower()
            results = []
            
            for key, record in self.agent_capabilities.items():
                # Simple fuzzy match
                if query_lower in record.capability_name.lower():
                    results.append(record)
            
            # Sort by confidence then success rate
            results.sort(key=lambda r: (r.confidence * r.success_rate), reverse=True)
            return results[:top_k]
    
    def record_execution(
        self,
        agent_id: str,
        capability_name: str,
        success: bool,
        execution_time: float
    ):
        """Record a capability execution for performance tracking."""
        with self._lock:
            key = f"{agent_id}:{capability_name}"
            if key in self.agent_capabilities:
                self.agent_capabilities[key].record_execution(success, execution_time)
                self._save()
    
    def define_capability(
        self,
        name: str,
        description: str = "",
        version: str = "1.0.0",
        requirements: Optional[List[str]] = None,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
        category: Optional[str] = None
    ):
        """Define a capability type (what it is, not who has it)."""
        with self._lock:
            metadata = CapabilityMetadata(
                name=name,
                description=description,
                version=version,
                requirements=requirements or [],
                inputs=inputs or [],
                outputs=outputs or []
            )
            self.capability_definitions[name] = metadata
            
            if category:
                self.categories[category].add(name)
            
            self._save()
    
    def get_capability_definition(self, name: str) -> Optional[CapabilityMetadata]:
        """Get capability definition by name."""
        return self.capability_definitions.get(name)
    
    def get_capabilities_by_category(self, category: str) -> List[str]:
        """Get all capabilities in a category."""
        return list(self.categories.get(category, set()))
    
    def list_all_capabilities(self) -> List[str]:
        """List all known capability names."""
        with self._lock:
            return list(set(
                cap for cap in self._by_capability.keys()
            ))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics."""
        with self._lock:
            return {
                'total_agents': len(self._by_agent),
                'total_capabilities': len(self._by_capability),
                'total_records': len(self.agent_capabilities),
                'capability_definitions': len(self.capability_definitions),
                'categories': len(self.categories)
            }
    
    def _save(self):
        """Persist registry to storage."""
        if not self.storage_path:
            return
        
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            'capability_definitions': {
                name: cap.to_dict()
                for name, cap in self.capability_definitions.items()
            },
            'agent_capabilities': {
                key: record.to_dict()
                for key, record in self.agent_capabilities.items()
            },
            'categories': {
                cat: list(caps)
                for cat, caps in self.categories.items()
            },
            'saved_at': datetime.now(timezone.utc).isoformat()
        }
        
        with open(self.storage_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def _load(self):
        """Load registry from storage."""
        if not self.storage_path or not self.storage_path.exists():
            return
        
        try:
            with open(self.storage_path, 'r') as f:
                data = json.load(f)
            
            # Load capability definitions
            for name, cap_data in data.get('capability_definitions', {}).items():
                cap_data['last_updated'] = datetime.fromisoformat(cap_data['last_updated'])
                self.capability_definitions[name] = CapabilityMetadata(**cap_data)
            
            # Load agent capabilities
            for key, record_data in data.get('agent_capabilities', {}).items():
                if record_data.get('last_used'):
                    record_data['last_used'] = datetime.fromisoformat(record_data['last_used'])
                record = AgentCapabilityRecord(
                    agent_id=record_data['agent_id'],
                    capability_name=record_data['capability_name'],
                    confidence=record_data['confidence'],
                    executions=record_data.get('executions', 0),
                    successes=record_data.get('successes', 0),
                    avg_time=record_data.get('avg_time', 0.0),
                    last_used=record_data.get('last_used'),
                    metadata=record_data.get('metadata', {})
                )
                self.agent_capabilities[key] = record
                
                # Rebuild indexes
                agent_id = record.agent_id
                cap_name = record.capability_name
                self._by_agent[agent_id].add(cap_name)
                self._by_capability[cap_name].add(agent_id)
            
            # Load categories
            for cat, caps in data.get('categories', {}).items():
                self.categories[cat] = set(caps)
                
        except Exception as e:
            print(f"[CapabilityRegistry] Load error: {e}")
    
    def export_dict(self) -> Dict:
        """Export registry as dictionary (for serialization)."""
        with self._lock:
            return {
                'capability_definitions': {
                    name: cap.to_dict()
                    for name, cap in self.capability_definitions.items()
                },
                'agent_capabilities': {
                    key: record.to_dict()
                    for key, record in self.agent_capabilities.items()
                },
                'stats': self.get_stats()
            }


# Pre-defined capability categories for small business automation
DEFAULT_CAPABILITIES = {
    'data_processing': [
        'data_ingestion',
        'data_cleaning',
        'data_transformation',
        'data_validation',
        'report_generation'
    ],
    'communication': [
        'email_handling',
        'sms_sending',
        'notification_dispatch',
        'customer_response',
        'meeting_scheduling'
    ],
    'research': [
        'web_search',
        'document_analysis',
        'competitor_monitoring',
        'market_research',
        'trend_analysis'
    ],
    'automation': [
        'task_scheduling',
        'workflow_orchestration',
        'file_management',
        'backup_operations',
        'system_monitoring'
    ],
    'analysis': [
        'sentiment_analysis',
        'financial_analysis',
        'performance_metrics',
        'anomaly_detection',
        'forecasting'
    ]
}


def setup_default_capabilities(registry: CapabilityRegistry):
    """Populate registry with default capability definitions."""
    for category, capabilities in DEFAULT_CAPABILITIES.items():
        for cap in capabilities:
            registry.define_capability(
                name=cap,
                description=f"{cap.replace('_', ' ').title()} capability",
                category=category
            )
