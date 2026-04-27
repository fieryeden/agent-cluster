#!/usr/bin/env python3
"""
Cluster Skill Provider

Exposes agent cluster capabilities as OpenClaw skills.
Allows OpenClaw to discover and invoke cluster agent capabilities
through the skill system.

Key features:
- Dynamic skill generation from cluster capabilities
- Skill invocation routed to best-available agent
- Capability gap detection surfaced as skill requests
- Fleet-wide skill aggregation
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from capabilities.registry import CapabilityRegistry
from capabilities.discovery import CapabilityDiscovery


class ClusterSkillProvider:
    """
    Provides agent cluster capabilities as OpenClaw skills.

    This bridges the capability registry to OpenClaw's skill system,
    allowing:
    1. Cluster capabilities auto-appear as available skills
    2. Skill invocations route to the best agent
    3. Capability gaps surface as skill requests
    4. Fleet-wide skill aggregation for dashboard visibility
    """

    def __init__(
        self,
        capability_registry: CapabilityRegistry,
        capability_discovery: CapabilityDiscovery = None,
        skill_output_dir: str = None,
    ):
        """
        Initialize the skill provider.

        Args:
            capability_registry: The cluster's capability registry
            capability_discovery: Optional discovery for finding agents
            skill_output_dir: Directory to write skill definitions
        """
        self.registry = capability_registry
        self.discovery = capability_discovery
        self.skill_output_dir = skill_output_dir or "/tmp/cluster_skills"
        os.makedirs(self.skill_output_dir, exist_ok=True)

        # Skill → capability mapping
        self._skill_map: Dict[str, Dict[str, Any]] = {}

        # Invocation hooks
        self._invocation_hooks: List[Callable] = []

        # Stats
        self.skill_stats = {
            "skills_generated": 0,
            "skills_invoked": 0,
            "invocations_routed": 0,
            "invocations_failed": 0,
            "gaps_detected": 0,
        }

    def generate_skills(self) -> List[Dict[str, Any]]:
        """
        Generate OpenClaw skill definitions from cluster capabilities.

        Returns:
            List of skill definition dicts
        """
        skills = []

        # Get all registered capabilities
        all_cap_names = self.registry.list_all_capabilities()

        for cap_name in (all_cap_names or []):
            # Find agents with this capability
            agents = self.registry.get_capability_agents(cap_name)
            if not agents:
                continue

            cap_def = self.registry.get_capability_definition(cap_name)
            desc = cap_def.description if cap_def and hasattr(cap_def, 'description') else f"Execute {cap_name} via cluster agent"

            skill = {
                "name": f"cluster.{cap_name}",
                "display_name": f"Cluster: {cap_name.replace('_', ' ').title()}",
                "description": desc,
                "source": "agent-cluster",
                "capability": cap_name,
                "available_agents": [
                    {
                        "agent_id": a.agent_id if hasattr(a, 'agent_id') else str(a),
                        "confidence": a.confidence if hasattr(a, 'confidence') else 1.0,
                    }
                    for a in agents
                ],
                "best_agent": self.registry.find_best_agent(cap_name),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

            skills.append(skill)
            self._skill_map[f"cluster.{cap_name}"] = skill

            # Write skill definition file
            skill_path = os.path.join(
                self.skill_output_dir, f"cluster-{cap_name}.json"
            )
            with open(skill_path, "w") as f:
                json.dump(skill, f, indent=2)

        self.skill_stats["skills_generated"] = len(skills)
        return skills

    def invoke_skill(
        self, skill_name: str, params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Invoke a cluster skill by routing to the best agent.

        Args:
            skill_name: Skill name (e.g. "cluster.web_search")
            params: Skill parameters

        Returns:
            Invocation result dict
        """
        self.skill_stats["skills_invoked"] += 1

        skill = self._skill_map.get(skill_name)
        if not skill:
            # Try generating skills first
            self.generate_skills()
            skill = self._skill_map.get(skill_name)

        if not skill:
            self.skill_stats["invocations_failed"] += 1
            return {
                "status": "failed",
                "reason": f"Skill {skill_name} not found in cluster",
                "available_skills": list(self._skill_map.keys()),
            }

        best_agent = skill.get("best_agent")
        if not best_agent:
            self.skill_stats["invocations_failed"] += 1
            return {
                "status": "failed",
                "reason": f"No available agent for {skill_name}",
            }

        # Route invocation
        self.skill_stats["invocations_routed"] += 1

        result = {
            "status": "routed",
            "skill": skill_name,
            "agent_id": best_agent,
            "params": params or {},
            "capability": skill.get("capability"),
        }

        # Fire invocation hooks
        for hook in self._invocation_hooks:
            try:
                hook(result)
            except Exception:
                pass

        return result

    def detect_capability_gaps(self) -> List[Dict[str, Any]]:
        """
        Detect capability gaps and surface as skill requests.

        Returns:
            List of gap dicts with requested capabilities
        """
        gaps = []

        # Get gaps from discovery if available
        if self.discovery:
            try:
                if hasattr(self.discovery, 'get_unresolved_gaps'):
                    discovery_gaps = self.discovery.get_unresolved_gaps()
                elif hasattr(self.discovery, 'gap_tracker'):
                    discovery_gaps = self.discovery.gap_tracker.get_unresolved_gaps()
                else:
                    discovery_gaps = []
                for gap in (discovery_gaps or []):
                    if isinstance(gap, dict):
                        gaps.append({
                            "capability": gap.get("capability", ""),
                            "request_count": gap.get("request_count", 0),
                            "first_requested": gap.get("first_requested", ""),
                            "status": gap.get("status", "unresolved"),
                            "skill_request": f"cluster.{gap.get('capability', 'unknown')}",
                        })
            except Exception:
                pass

        self.skill_stats["gaps_detected"] = len(gaps)
        return gaps

    def get_fleet_skills_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all fleet skills.

        Returns:
            Summary dict with skill counts, agents, gaps
        """
        if not self._skill_map:
            self.generate_skills()

        total_skills = len(self._skill_map)
        agent_counts: Dict[str, int] = {}

        for skill_name, skill_data in self._skill_map.items():
            for agent_info in skill_data.get("available_agents", []):
                aid = agent_info.get("agent_id", "unknown")
                agent_counts[aid] = agent_counts.get(aid, 0) + 1

        gaps = self.detect_capability_gaps()

        return {
            "total_skills": total_skills,
            "skill_names": list(self._skill_map.keys()),
            "agents_with_skills": len(agent_counts),
            "agent_skill_counts": agent_counts,
            "capability_gaps": len(gaps),
            "gap_details": gaps[:10],  # Top 10 gaps
        }

    def on_skill_invoked(self, callback: Callable):
        """Register a callback for skill invocations."""
        self._invocation_hooks.append(callback)

    def _get_best_agent(self, agents) -> Optional[str]:
        """Select the best agent for a capability."""
        if not agents:
            return None
        best = max(
            agents,
            key=lambda a: a.confidence if hasattr(a, 'confidence') else 1.0
        )
        return best.agent_id if hasattr(best, 'agent_id') else str(best)

    def __repr__(self):
        return (
            f"ClusterSkillProvider("
            f"skills={len(self._skill_map)}, "
            f"invoked={self.skill_stats['skills_invoked']})"
        )
