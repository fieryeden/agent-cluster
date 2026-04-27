#!/usr/bin/env python3
"""
Auto-Learning Module

Phase 3 of the Agent Cluster MVP.

Enables agents to:
1. Research new capabilities when needed
2. Install tools across the cluster
3. Verify installation and rollback on failure
"""

from .research import ResearchRequest, ResearchResult, ResearchDispatcher
from .installation import ToolInstaller, InstallationResult, InstallationStatus
from .verification import VerificationManager, VerificationResult
from .workflow import AutoLearningWorkflow, LearningStatus

__all__ = [
    # Research
    "ResearchRequest",
    "ResearchResult", 
    "ResearchDispatcher",
    # Installation
    "ToolInstaller",
    "InstallationResult",
    "InstallationStatus",
    # Verification
    "VerificationManager",
    "VerificationResult",
    # Workflow
    "AutoLearningWorkflow",
    "LearningStatus",
]
