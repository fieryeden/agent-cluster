"""
Capabilities module - Capability discovery and management.

Components:
- registry.py: Central capability tracking
- discovery.py: Capability query/response protocol
- updates.py: Dynamic capability updates
"""

from .registry import (
    CapabilityRegistry,
    CapabilityMetadata,
    AgentCapabilityRecord,
    setup_default_capabilities
)

__all__ = [
    'CapabilityRegistry',
    'CapabilityMetadata',
    'AgentCapabilityRecord',
    'setup_default_capabilities'
]
