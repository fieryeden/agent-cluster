"""
OpenClaw Integration Module

Bridges the Agent Cluster to OpenClaw's infrastructure — with a switch.

When OpenClaw integration is ENABLED:
  - Peer messages route through OpenClaw sessions
  - Agent sessions auto-registered with OpenClaw
  - Conversations synced to OpenClaw memory
  - Cluster capabilities exposed as OpenClaw skills
  - Events bridged to OpenClaw system

When DISABLED (default):
  - Pure standalone TCP coordinator, zero OpenClaw dependency
  - All existing functionality works unchanged

Both modes can coexist — standalone TCP agents and OpenClaw session
agents can participate in the same cluster simultaneously.
"""

from openclaw_integration.mode_manager import ClusterMode, OpenClawConfig, ClusterModeManager
from openclaw_integration.bridge import OpenClawCoordinatorBridge
from openclaw_integration.adapter import OpenClawAgentAdapter
from openclaw_integration.events import EventBridge
from openclaw_integration.skill_provider import ClusterSkillProvider

__all__ = [
    "ClusterMode",
    "OpenClawConfig",
    "ClusterModeManager",
    "OpenClawCoordinatorBridge",
    "OpenClawAgentAdapter",
    "EventBridge",
    "ClusterSkillProvider",
]
