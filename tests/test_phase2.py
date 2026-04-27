#!/usr/bin/env python3
"""
Phase 2 Tests - Capability Discovery

Tests for:
- CapabilityRegistry: tracking capabilities across agents
- CapabilityDiscovery: query/response protocol
- Dynamic Capability Updates: add/remove capabilities at runtime
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from capabilities.registry import (
    CapabilityRegistry, CapabilityMetadata, AgentCapabilityRecord
)
from capabilities.discovery import (
    CapabilityQuery, CapabilityQueryType, CapabilityResponse, CapabilityDiscovery
)
from capabilities.updates import (
    UpdateType, UpdateStatus, CapabilityUpdateRequest, CapabilityUpdater
)


def test_registry_init():
    """Test CapabilityRegistry initialization."""
    registry = CapabilityRegistry()
    assert registry is not None
    stats = registry.get_stats()
    assert stats["total_agents"] == 0
    assert stats["total_capabilities"] == 0
    print("✓ registry init")


def test_register_capability():
    """Test registering a capability with an agent."""
    registry = CapabilityRegistry()
    
    # Register a capability
    registry.register_capability(
        agent_id="agent-001",
        capability_name="excel_analysis",
        confidence=0.85
    )
    
    stats = registry.get_stats()
    assert stats["total_agents"] == 1
    assert stats["total_capabilities"] == 1
    print("✓ register capability")


def test_find_agents_for_capability():
    """Test finding agents that have a capability."""
    registry = CapabilityRegistry()
    
    # Register multiple agents with same capability
    registry.register_capability("agent-001", "excel_analysis", 0.9)
    registry.register_capability("agent-002", "excel_analysis", 0.7)
    registry.register_capability("agent-003", "python_scripting", 0.95)
    
    # Find agents with excel_analysis
    agents = registry.get_capability_agents("excel_analysis")
    assert len(agents) == 2
    print("✓ find agents for capability")


def test_best_agent_for_capability():
    """Test finding the best agent for a capability."""
    registry = CapabilityRegistry()
    
    registry.register_capability("agent-001", "data_processing", 0.8)
    registry.register_capability("agent-002", "data_processing", 0.95)
    registry.register_capability("agent-003", "data_processing", 0.6)
    
    best = registry.find_best_agent("data_processing")
    assert best == "agent-002"  # Highest confidence
    print("✓ best agent for capability")


def test_capability_metadata():
    """Test capability metadata tracking."""
    registry = CapabilityRegistry()
    
    # Register capability
    registry.register_capability(
        agent_id="agent-001",
        capability_name="web_scraping",
        confidence=0.9
    )
    
    # Retrieve capability record
    caps = registry.get_agent_capabilities("agent-001")
    assert len(caps) == 1
    assert caps[0].capability_name == "web_scraping"
    print("✓ capability metadata")


def test_discovery_query_can_do():
    """Test CAN_DO query type."""
    registry = CapabilityRegistry()
    registry.register_capability("agent-001", "excel_analysis", 0.9)
    
    discovery = CapabilityDiscovery(registry, "coordinator")
    
    query = CapabilityQuery(
        query_type=CapabilityQueryType.CAN_DO,
        capability_name="excel_analysis"
    )
    
    response = discovery.query(query)
    assert response.success
    assert response.available
    assert response.agent_id == "agent-001"
    print("✓ discovery query can_do")


def test_discovery_query_list_all():
    """Test LIST_ALL query type."""
    registry = CapabilityRegistry()
    registry.register_capability("agent-001", "excel_analysis", 0.9)
    registry.register_capability("agent-001", "python_scripting", 0.85)
    
    discovery = CapabilityDiscovery(registry, "coordinator")
    
    query = CapabilityQuery(
        query_type=CapabilityQueryType.LIST_ALL,
        target_agent="agent-001"
    )
    
    response = discovery.query(query)
    assert response.success
    # Capabilities stored in metadata['capabilities']
    assert "capabilities" in response.metadata
    assert len(response.metadata["capabilities"]) == 2
    print("✓ discovery query list_all")


def test_discovery_best_match():
    """Test BEST_MATCH query type."""
    registry = CapabilityRegistry()
    registry.register_capability("agent-001", "data_processing", 0.7)
    registry.register_capability("agent-002", "data_processing", 0.95)
    registry.register_capability("agent-003", "data_processing", 0.85)
    
    discovery = CapabilityDiscovery(registry, "coordinator")
    
    query = CapabilityQuery(
        query_type=CapabilityQueryType.BEST_MATCH,
        capability_name="data_processing"
    )
    
    response = discovery.query(query)
    assert response.success
    assert response.agent_id == "agent-002"  # Highest confidence
    assert response.confidence == 0.95
    print("✓ discovery best_match")


def test_discovery_min_confidence():
    """Test query with minimum confidence filter."""
    registry = CapabilityRegistry()
    registry.register_capability("agent-001", "ml_inference", 0.6)
    registry.register_capability("agent-002", "ml_inference", 0.9)
    registry.register_capability("agent-003", "ml_inference", 0.4)
    
    discovery = CapabilityDiscovery(registry, "coordinator")
    
    # Query with min_confidence 0.5
    query = CapabilityQuery(
        query_type=CapabilityQueryType.CAN_DO,
        capability_name="ml_inference",
        min_confidence=0.5
    )
    
    response = discovery.query(query)
    assert response.success
    # Should return highest that meets threshold
    assert response.agent_id == "agent-002"
    print("✓ discovery min_confidence")


def test_update_acquire():
    """Test acquiring a new capability."""
    registry = CapabilityRegistry()
    
    # Agent initially has no capabilities
    assert registry.find_best_agent("new_skill") is None
    
    # Acquire capability
    registry.register_capability(
        agent_id="agent-001",
        capability_name="new_skill",
        confidence=0.5
    )
    
    assert registry.find_best_agent("new_skill") == "agent-001"
    print("✓ update acquire")


def test_remove_capability():
    """Test removing a capability."""
    registry = CapabilityRegistry()
    
    registry.register_capability("agent-001", "skill", 0.9)
    assert registry.find_best_agent("skill") == "agent-001"
    
    # Remove capability
    registry.deregister_capability("agent-001", "skill")
    
    assert registry.find_best_agent("skill") is None
    print("✓ remove capability")


def test_capability_execution_tracking():
    """Test tracking capability executions."""
    registry = CapabilityRegistry()
    
    registry.register_capability("agent-001", "task_runner", 0.8)
    
    # Record some executions
    registry.record_execution("agent-001", "task_runner", success=True, execution_time=1.5)
    registry.record_execution("agent-001", "task_runner", success=True, execution_time=1.2)
    registry.record_execution("agent-001", "task_runner", success=False, execution_time=2.0)
    
    caps = registry.get_agent_capabilities("agent-001")
    record = caps[0]
    
    assert record.executions == 3
    assert record.successes == 2
    assert abs(record.success_rate - 0.667) < 0.01
    print("✓ capability execution tracking")


def test_registry_export():
    """Test exporting registry data."""
    registry = CapabilityRegistry()
    
    registry.register_capability("agent-001", "skill_a", 0.9)
    registry.register_capability("agent-002", "skill_b", 0.8)
    
    data = registry.export_dict()
    assert "agent_capabilities" in data
    assert len(data["agent_capabilities"]) == 2
    print("✓ registry export")


def test_query_message_creation():
    """Test creating protocol messages for queries."""
    registry = CapabilityRegistry()
    registry.register_capability("agent-001", "test_skill", 0.9)
    
    discovery = CapabilityDiscovery(registry, agent_id="coordinator")
    
    query = CapabilityQuery(
        query_type=CapabilityQueryType.CAN_DO,
        capability_name="test_skill"
    )
    
    msg = discovery.create_query_message(query)
    assert msg is not None
    assert msg.msg_type.value == "capability_query"
    assert msg.sender_id == "coordinator"
    print("✓ query message creation")


def test_response_message_creation():
    """Test creating protocol messages for responses."""
    registry = CapabilityRegistry()
    registry.register_capability("agent-001", "test_skill", 0.9)
    
    discovery = CapabilityDiscovery(registry, agent_id="agent-001")
    
    response = CapabilityResponse(
        success=True,
        query_type=CapabilityQueryType.CAN_DO,
        agent_id="agent-001",
        capability_name="test_skill",
        confidence=0.9,
        available=True
    )
    
    msg = discovery.create_response_message(response)
    assert msg is not None
    assert msg.msg_type.value == "capability_response"
    assert msg.sender_id == "agent-001"
    print("✓ response message creation")


def test_deregister_agent():
    """Test deregistering an agent entirely."""
    registry = CapabilityRegistry()
    
    registry.register_capability("agent-001", "skill_a", 0.9)
    registry.register_capability("agent-001", "skill_b", 0.8)
    registry.register_capability("agent-002", "skill_a", 0.7)
    
    # Deregister agent-001
    registry.deregister_agent("agent-001")
    
    # Should only have agent-002
    agents = registry.get_capability_agents("skill_a")
    assert len(agents) == 1
    # Returns AgentCapabilityRecord, not strings
    assert agents[0].agent_id == "agent-002"
    print("✓ deregister agent")


def test_capability_definition():
    """Test defining capability metadata."""
    registry = CapabilityRegistry()
    
    # Define a capability
    registry.define_capability(
        name="web_scraping",
        description="Extract data from web pages",
        requirements=["beautifulsoup4", "requests"],
        inputs=["url"],
        outputs=["html", "text", "structured_data"]
    )
    
    # Get definition
    definition = registry.get_capability_definition("web_scraping")
    assert definition is not None
    assert definition.name == "web_scraping"
    assert "beautifulsoup4" in definition.requirements
    print("✓ capability definition")


# ============================================
# RUN TESTS
# ============================================

TESTS = [
    test_registry_init,
    test_register_capability,
    test_find_agents_for_capability,
    test_best_agent_for_capability,
    test_capability_metadata,
    test_discovery_query_can_do,
    test_discovery_query_list_all,
    test_discovery_best_match,
    test_discovery_min_confidence,
    test_update_acquire,
    test_remove_capability,
    test_capability_execution_tracking,
    test_registry_export,
    test_query_message_creation,
    test_response_message_creation,
    test_deregister_agent,
    test_capability_definition,
]


def run_tests():
    """Run all Phase 2 tests."""
    print("=" * 50)
    print("PHASE 2: CAPABILITY DISCOVERY TESTS")
    print("=" * 50)
    
    passed = 0
    failed = 0
    
    for test in TESTS:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"✗ {test.__name__}: {e}")
            failed += 1
    
    print("=" * 50)
    print(f"TOTAL: {passed}/{len(TESTS)} tests passed")
    print("=" * 50)
    
    return passed, failed


if __name__ == "__main__":
    run_tests()
