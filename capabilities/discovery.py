#!/usr/bin/env python3
"""
Capability Discovery - Query/response protocol for capability negotiation.

Features:
- CAPABILITY_QUERY: "Can you do X?"
- CAPABILITY_RESPONSE: Confidence + metadata
- CAPABILITY_LIST: "What can you do?"
- CAPABILITY_UPDATE: Dynamic add/remove
"""

import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import BaseMessage, MessageType, create_message


class CapabilityQueryType(Enum):
    """Types of capability queries."""
    CAN_DO = "can_do"           # "Can you do X?" -> returns confidence
    LIST_ALL = "list_all"       # "What can you do?" -> returns all capabilities
    BEST_MATCH = "best_match"   # "Who can do X best?" -> returns ranked agents
    REQUIREMENTS = "requirements"  # "What do you need for X?" -> returns requirements


@dataclass
class CapabilityQuery:
    """Query about capabilities."""
    query_type: CapabilityQueryType
    capability_name: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    target_agent: Optional[str] = None
    exclude_agents: Optional[List[str]] = None
    min_confidence: float = 0.5
    
    def to_dict(self) -> Dict:
        return {
            'query_type': self.query_type.value,
            'capability_name': self.capability_name,
            'parameters': self.parameters,
            'target_agent': self.target_agent,
            'exclude_agents': self.exclude_agents,
            'min_confidence': self.min_confidence
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CapabilityQuery':
        return cls(
            query_type=CapabilityQueryType(data['query_type']),
            capability_name=data.get('capability_name'),
            parameters=data.get('parameters'),
            target_agent=data.get('target_agent'),
            exclude_agents=data.get('exclude_agents'),
            min_confidence=data.get('min_confidence', 0.5)
        )


@dataclass
class CapabilityResponse:
    """Response to capability query."""
    success: bool
    query_type: CapabilityQueryType
    agent_id: Optional[str] = None
    capability_name: Optional[str] = None
    confidence: float = 0.0
    available: bool = False
    metadata: Dict[str, Any] = None
    alternatives: List[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)
        if self.metadata is None:
            self.metadata = {}
        if self.alternatives is None:
            self.alternatives = []
    
    def to_dict(self) -> Dict:
        return {
            'success': self.success,
            'query_type': self.query_type.value,
            'agent_id': self.agent_id,
            'capability_name': self.capability_name,
            'confidence': self.confidence,
            'available': self.available,
            'metadata': self.metadata,
            'alternatives': self.alternatives,
            'error': self.error,
            'timestamp': self.timestamp.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CapabilityResponse':
        return cls(
            success=data['success'],
            query_type=CapabilityQueryType(data['query_type']),
            agent_id=data.get('agent_id'),
            capability_name=data.get('capability_name'),
            confidence=data.get('confidence', 0.0),
            available=data.get('available', False),
            metadata=data.get('metadata', {}),
            alternatives=data.get('alternatives', []),
            error=data.get('error'),
            timestamp=datetime.fromisoformat(data['timestamp']) if data.get('timestamp') else None
        )


@dataclass
class CapabilityUpdate:
    """Dynamic capability update request."""
    agent_id: str
    action: str  # 'add', 'remove', 'update'
    capability_name: str
    confidence: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict:
        return {
            'agent_id': self.agent_id,
            'action': self.action,
            'capability_name': self.capability_name,
            'confidence': self.confidence,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CapabilityUpdate':
        return cls(
            agent_id=data['agent_id'],
            action=data['action'],
            capability_name=data['capability_name'],
            confidence=data.get('confidence'),
            metadata=data.get('metadata')
        )


class CapabilityDiscovery:
    """
    Capability discovery handler.
    
    Handles:
    - Capability queries (can you do X?)
    - Capability listing (what can you do?)
    - Best match finding (who's best for X?)
    - Dynamic updates (I can do X now)
    """
    
    def __init__(self, registry, agent_id: str = None):
        """
        Initialize discovery handler.
        
        Args:
            registry: CapabilityRegistry instance
            agent_id: This agent's ID (for responses)
        """
        from .registry import CapabilityRegistry
        self.registry: CapabilityRegistry = registry
        self.agent_id = agent_id
    
    def query(self, query: CapabilityQuery) -> CapabilityResponse:
        """
        Process a capability query.
        
        Args:
            query: The capability query
        
        Returns:
            CapabilityResponse with results
        """
        if query.query_type == CapabilityQueryType.CAN_DO:
            return self._handle_can_do(query)
        elif query.query_type == CapabilityQueryType.LIST_ALL:
            return self._handle_list_all(query)
        elif query.query_type == CapabilityQueryType.BEST_MATCH:
            return self._handle_best_match(query)
        elif query.query_type == CapabilityQueryType.REQUIREMENTS:
            return self._handle_requirements(query)
        else:
            return CapabilityResponse(
                success=False,
                query_type=query.query_type,
                error=f"Unknown query type: {query.query_type}"
            )
    
    def _handle_can_do(self, query: CapabilityQuery) -> CapabilityResponse:
        """Handle 'Can you do X?' query."""
        cap_name = query.capability_name
        if not cap_name:
            return CapabilityResponse(
                success=False,
                query_type=query.query_type,
                error="No capability name provided"
            )
        
        # If target_agent specified, check that agent
        if query.target_agent:
            agent_caps = self.registry.get_agent_capabilities(query.target_agent)
            for cap in agent_caps:
                if cap.capability_name == cap_name and cap.confidence >= query.min_confidence:
                    return CapabilityResponse(
                        success=True,
                        query_type=query.query_type,
                        agent_id=query.target_agent,
                        capability_name=cap_name,
                        confidence=cap.confidence,
                        available=True,
                        metadata={
                            'executions': cap.executions,
                            'success_rate': cap.success_rate,
                            'avg_time': cap.avg_time
                        }
                    )
            
            return CapabilityResponse(
                success=True,
                query_type=query.query_type,
                agent_id=query.target_agent,
                capability_name=cap_name,
                confidence=0.0,
                available=False
            )
        
        # Otherwise find any agent with the capability
        agents = self.registry.get_capability_agents(cap_name)
        valid_agents = [
            cap for cap in agents
            if cap.confidence >= query.min_confidence
            and (not query.exclude_agents or cap.agent_id not in query.exclude_agents)
        ]
        
        if valid_agents:
            # Return the best one
            best = max(valid_agents, key=lambda c: c.confidence * c.success_rate)
            return CapabilityResponse(
                success=True,
                query_type=query.query_type,
                agent_id=best.agent_id,
                capability_name=cap_name,
                confidence=best.confidence,
                available=True,
                metadata={
                    'executions': best.executions,
                    'success_rate': best.success_rate,
                    'avg_time': best.avg_time
                },
                alternatives=[
                    {'agent_id': c.agent_id, 'confidence': c.confidence}
                    for c in sorted(valid_agents, key=lambda x: x.confidence, reverse=True)[1:4]
                ]
            )
        
        return CapabilityResponse(
            success=True,
            query_type=query.query_type,
            capability_name=cap_name,
            confidence=0.0,
            available=False
        )
    
    def _handle_list_all(self, query: CapabilityQuery) -> CapabilityResponse:
        """Handle 'What can you do?' query."""
        target = query.target_agent or self.agent_id
        if not target:
            return CapabilityResponse(
                success=False,
                query_type=query.query_type,
                error="No agent specified"
            )
        
        agent_caps = self.registry.get_agent_capabilities(target)
        capabilities = [
            {
                'name': cap.capability_name,
                'confidence': cap.confidence,
                'executions': cap.executions,
                'success_rate': cap.success_rate
            }
            for cap in agent_caps
        ]
        
        return CapabilityResponse(
            success=True,
            query_type=query.query_type,
            agent_id=target,
            metadata={'capabilities': capabilities, 'total': len(capabilities)}
        )
    
    def _handle_best_match(self, query: CapabilityQuery) -> CapabilityResponse:
        """Handle 'Who can do X best?' query."""
        cap_name = query.capability_name
        if not cap_name:
            return CapabilityResponse(
                success=False,
                query_type=query.query_type,
                error="No capability name provided"
            )
        
        exclude = set(query.exclude_agents or [])
        best_agent = self.registry.find_best_agent(cap_name, exclude=exclude)
        
        if best_agent:
            cap_record = self.registry.agent_capabilities.get(f"{best_agent}:{cap_name}")
            return CapabilityResponse(
                success=True,
                query_type=query.query_type,
                agent_id=best_agent,
                capability_name=cap_name,
                confidence=cap_record.confidence if cap_record else 0.0,
                available=True,
                metadata={
                    'success_rate': cap_record.success_rate if cap_record else 0,
                    'executions': cap_record.executions if cap_record else 0
                }
            )
        
        return CapabilityResponse(
            success=True,
            query_type=query.query_type,
            capability_name=cap_name,
            confidence=0.0,
            available=False
        )
    
    def _handle_requirements(self, query: CapabilityQuery) -> CapabilityResponse:
        """Handle 'What do you need for X?' query."""
        cap_name = query.capability_name
        if not cap_name:
            return CapabilityResponse(
                success=False,
                query_type=query.query_type,
                error="No capability name provided"
            )
        
        cap_def = self.registry.get_capability_definition(cap_name)
        if cap_def:
            return CapabilityResponse(
                success=True,
                query_type=query.query_type,
                capability_name=cap_name,
                metadata={
                    'description': cap_def.description,
                    'version': cap_def.version,
                    'requirements': cap_def.requirements,
                    'inputs': cap_def.inputs,
                    'outputs': cap_def.outputs
                }
            )
        
        return CapabilityResponse(
            success=False,
            query_type=query.query_type,
            capability_name=cap_name,
            error=f"Capability '{cap_name}' not defined"
        )
    
    def apply_update(self, update: CapabilityUpdate) -> bool:
        """
        Apply a capability update.
        
        Args:
            update: The capability update request
        
        Returns:
            True if successful
        """
        if update.action == 'add':
            self.registry.register_capability(
                agent_id=update.agent_id,
                capability_name=update.capability_name,
                confidence=update.confidence or 1.0,
                metadata=update.metadata
            )
            return True
        
        elif update.action == 'remove':
            return self.registry.deregister_capability(
                agent_id=update.agent_id,
                capability_name=update.capability_name
            )
        
        elif update.action == 'update':
            self.registry.register_capability(
                agent_id=update.agent_id,
                capability_name=update.capability_name,
                confidence=update.confidence or 1.0,
                metadata=update.metadata
            )
            return True
        
        return False

    def create_query_message(self, query: CapabilityQuery, target_agent: str = None) -> BaseMessage:
        """Create a protocol message for a capability query."""
        return create_message(
            msg_type=MessageType.CAPABILITY_QUERY,
            sender_id=self.agent_id,
            recipient_id=target_agent or "broadcast",
            payload=query.to_dict()
        )

    def create_response_message(self, response: CapabilityResponse, target_agent: str = None) -> BaseMessage:
        """Create a protocol message for a capability response."""
        return create_message(
            msg_type=MessageType.CAPABILITY_RESPONSE,
            sender_id=self.agent_id,
            recipient_id=target_agent or "coordinator",
            payload=response.to_dict()
        )
# Convenience functions

def query_capability(registry, capability_name: str, min_confidence: float = 0.5) -> Optional[str]:
    """Quick query: find an agent for a capability."""
    discovery = CapabilityDiscovery(registry)
    response = discovery.query(CapabilityQuery(
        query_type=CapabilityQueryType.CAN_DO,
        capability_name=capability_name,
        min_confidence=min_confidence
    ))
    return response.agent_id if response.available else None


def list_agent_capabilities(registry, agent_id: str) -> List[Dict]:
    """Quick query: list an agent's capabilities."""
    discovery = CapabilityDiscovery(registry)
    response = discovery.query(CapabilityQuery(
        query_type=CapabilityQueryType.LIST_ALL,
        target_agent=agent_id
    ))
    return response.metadata.get('capabilities', []) if response.success else []


from pathlib import Path
