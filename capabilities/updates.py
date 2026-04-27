#!/usr/bin/env python3
"""
Dynamic Capability Updates - Agents add/remove capabilities at runtime.

Features:
- Capability acquisition (agent learns new skill)
- Capability removal (agent loses access)
- Confidence updates (skill improves/degrades)
- Batch updates (multiple capabilities at once)
- Verification (test before announcing)
"""

import json
import subprocess
import importlib
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict, field
from pathlib import Path
from enum import Enum
import threading


class UpdateType(Enum):
    """Types of capability updates."""
    ACQUIRE = "acquire"      # Agent gains new capability
    REMOVE = "remove"        # Agent loses capability
    UPDATE = "update"        # Capability metadata/confidence change
    VERIFY = "verify"        # Verify capability still works
    BATCH = "batch"          # Multiple updates at once


class UpdateStatus(Enum):
    """Status of capability update."""
    PENDING = "pending"
    VERIFIED = "verified"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class CapabilityVerification:
    """Result of capability verification."""
    capability_name: str
    success: bool
    error: Optional[str] = None
    execution_time: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict:
        return {
            'capability_name': self.capability_name,
            'success': self.success,
            'error': self.error,
            'execution_time': self.execution_time,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class CapabilityUpdateRequest:
    """Request to update capabilities."""
    agent_id: str
    update_type: UpdateType
    capability_name: str
    confidence: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None
    verify_before: bool = True
    rollback_on_failure: bool = True
    
    def to_dict(self) -> Dict:
        return {
            'agent_id': self.agent_id,
            'update_type': self.update_type.value,
            'capability_name': self.capability_name,
            'confidence': self.confidence,
            'metadata': self.metadata,
            'verify_before': self.verify_before,
            'rollback_on_failure': self.rollback_on_failure
        }


@dataclass
class CapabilityUpdateResult:
    """Result of capability update."""
    request: CapabilityUpdateRequest
    status: UpdateStatus
    previous_state: Optional[Dict[str, Any]] = None
    verification: Optional[CapabilityVerification] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict:
        return {
            'request': self.request.to_dict(),
            'status': self.status.value,
            'previous_state': self.previous_state,
            'verification': self.verification.to_dict() if self.verification else None,
            'error': self.error,
            'timestamp': self.timestamp.isoformat()
        }


class CapabilityUpdater:
    """
    Handles dynamic capability updates for agents.
    
    Features:
    - Verify capability before announcing
    - Track update history
    - Rollback on failure
    - Batch updates with transaction semantics
    """
    
    def __init__(self, registry, agent_id: str):
        """
        Initialize updater.
        
        Args:
            registry: CapabilityRegistry instance
            agent_id: This agent's ID
        """
        from .registry import CapabilityRegistry
        self.registry: CapabilityRegistry = registry
        self.agent_id = agent_id
        
        # Verification functions per capability type
        self._verifiers: Dict[str, Callable[[], bool]] = {}
        
        # Update history
        self._history: List[CapabilityUpdateResult] = []
        self._history_lock = threading.Lock()
        
        # Pending updates (for rollback)
        self._pending: Dict[str, Dict] = {}
    
    def register_verifier(self, capability_name: str, verifier: Callable[[], bool]):
        """
        Register a verification function for a capability.
        
        The verifier should:
        - Return True if capability works
        - Return False if capability fails
        - Raise exception on unexpected error
        """
        self._verifiers[capability_name] = verifier
    
    def acquire_capability(
        self,
        capability_name: str,
        confidence: float = 1.0,
        metadata: Optional[Dict] = None,
        verify: bool = True,
        verifier: Optional[Callable[[], bool]] = None
    ) -> CapabilityUpdateResult:
        """
        Acquire a new capability.
        
        Args:
            capability_name: Name of capability to acquire
            confidence: Initial confidence (0.0-1.0)
            metadata: Additional capability metadata
            verify: Whether to verify before announcing
            verifier: Optional custom verifier function
        
        Returns:
            CapabilityUpdateResult with outcome
        """
        request = CapabilityUpdateRequest(
            agent_id=self.agent_id,
            update_type=UpdateType.ACQUIRE,
            capability_name=capability_name,
            confidence=confidence,
            metadata=metadata,
            verify_before=verify
        )
        
        # Check if already have it
        existing = self.registry.agent_capabilities.get(f"{self.agent_id}:{capability_name}")
        previous_state = existing.to_dict() if existing else None
        
        # Verify if requested
        verification = None
        if verify:
            verifier_func = verifier or self._verifiers.get(capability_name)
            if verifier_func:
                try:
                    import time
                    start = time.time()
                    success = verifier_func()
                    execution_time = time.time() - start
                    
                    verification = CapabilityVerification(
                        capability_name=capability_name,
                        success=success,
                        execution_time=execution_time
                    )
                    
                    if not success:
                        return CapabilityUpdateResult(
                            request=request,
                            status=UpdateStatus.FAILED,
                            previous_state=previous_state,
                            verification=verification,
                            error="Verification failed"
                        )
                except Exception as e:
                    verification = CapabilityVerification(
                        capability_name=capability_name,
                        success=False,
                        error=str(e)
                    )
                    return CapabilityUpdateResult(
                        request=request,
                        status=UpdateStatus.FAILED,
                        previous_state=previous_state,
                        verification=verification,
                        error=f"Verification error: {e}"
                    )
        
        # Register the capability
        self.registry.register_capability(
            agent_id=self.agent_id,
            capability_name=capability_name,
            confidence=confidence,
            metadata=metadata
        )
        
        result = CapabilityUpdateResult(
            request=request,
            status=UpdateStatus.VERIFIED if verification and verification.success else UpdateStatus.PENDING,
            previous_state=previous_state,
            verification=verification
        )
        
        self._record_history(result)
        return result
    
    def remove_capability(
        self,
        capability_name: str,
        verify: bool = True
    ) -> CapabilityUpdateResult:
        """
        Remove a capability.
        
        Args:
            capability_name: Name of capability to remove
            verify: Whether to verify removal was successful
        
        Returns:
            CapabilityUpdateResult with outcome
        """
        request = CapabilityUpdateRequest(
            agent_id=self.agent_id,
            update_type=UpdateType.REMOVE,
            capability_name=capability_name,
            verify_before=verify
        )
        
        # Get previous state for potential rollback
        existing = self.registry.agent_capabilities.get(f"{self.agent_id}:{capability_name}")
        previous_state = existing.to_dict() if existing else None
        
        if not existing:
            return CapabilityUpdateResult(
                request=request,
                status=UpdateStatus.FAILED,
                error="Capability not found"
            )
        
        # Remove the capability
        success = self.registry.deregister_capability(
            agent_id=self.agent_id,
            capability_name=capability_name
        )
        
        result = CapabilityUpdateResult(
            request=request,
            status=UpdateStatus.VERIFIED if success else UpdateStatus.FAILED,
            previous_state=previous_state,
            error=None if success else "Failed to remove capability"
        )
        
        self._record_history(result)
        return result
    
    def update_confidence(
        self,
        capability_name: str,
        new_confidence: float,
        metadata_updates: Optional[Dict] = None
    ) -> CapabilityUpdateResult:
        """
        Update capability confidence/metadata.
        
        Args:
            capability_name: Name of capability
            new_confidence: New confidence value (0.0-1.0)
            metadata_updates: Optional metadata to merge
        
        Returns:
            CapabilityUpdateResult with outcome
        """
        request = CapabilityUpdateRequest(
            agent_id=self.agent_id,
            update_type=UpdateType.UPDATE,
            capability_name=capability_name,
            confidence=new_confidence,
            metadata=metadata_updates
        )
        
        existing = self.registry.agent_capabilities.get(f"{self.agent_id}:{capability_name}")
        previous_state = existing.to_dict() if existing else None
        
        if not existing:
            return CapabilityUpdateResult(
                request=request,
                status=UpdateStatus.FAILED,
                error="Capability not found"
            )
        
        # Update the capability
        self.registry.register_capability(
            agent_id=self.agent_id,
            capability_name=capability_name,
            confidence=new_confidence,
            metadata=metadata_updates
        )
        
        result = CapabilityUpdateResult(
            request=request,
            status=UpdateStatus.VERIFIED,
            previous_state=previous_state
        )
        
        self._record_history(result)
        return result
    
    def verify_capability(self, capability_name: str) -> CapabilityVerification:
        """
        Verify a capability still works.
        
        Args:
            capability_name: Name of capability to verify
        
        Returns:
            CapabilityVerification result
        """
        verifier = self._verifiers.get(capability_name)
        
        if not verifier:
            return CapabilityVerification(
                capability_name=capability_name,
                success=False,
                error="No verifier registered"
            )
        
        try:
            import time
            start = time.time()
            success = verifier()
            execution_time = time.time() - start
            
            return CapabilityVerification(
                capability_name=capability_name,
                success=success,
                execution_time=execution_time
            )
        except Exception as e:
            return CapabilityVerification(
                capability_name=capability_name,
                success=False,
                error=str(e)
            )
    
    def batch_update(
        self,
        updates: List[CapabilityUpdateRequest],
        atomic: bool = True
    ) -> List[CapabilityUpdateResult]:
        """
        Apply multiple updates atomically.
        
        Args:
            updates: List of update requests
            atomic: If True, rollback all on any failure
        
        Returns:
            List of results
        """
        results = []
        applied = []
        
        for update in updates:
            if update.update_type == UpdateType.ACQUIRE:
                result = self.acquire_capability(
                    capability_name=update.capability_name,
                    confidence=update.confidence or 1.0,
                    metadata=update.metadata,
                    verify=update.verify_before
                )
            elif update.update_type == UpdateType.REMOVE:
                result = self.remove_capability(
                    capability_name=update.capability_name,
                    verify=update.verify_before
                )
            elif update.update_type == UpdateType.UPDATE:
                result = self.update_confidence(
                    capability_name=update.capability_name,
                    new_confidence=update.confidence,
                    metadata_updates=update.metadata
                )
            else:
                result = CapabilityUpdateResult(
                    request=update,
                    status=UpdateStatus.FAILED,
                    error=f"Unknown update type: {update.update_type}"
                )
            
            results.append(result)
            
            if result.status == UpdateStatus.VERIFIED:
                applied.append(result)
            elif atomic:
                # Rollback all applied updates
                for r in reversed(applied):
                    if r.request.update_type == UpdateType.ACQUIRE:
                        self.registry.deregister_capability(
                            agent_id=self.agent_id,
                            capability_name=r.request.capability_name
                        )
                    elif r.request.update_type == UpdateType.REMOVE and r.previous_state:
                        self.registry.register_capability(
                            agent_id=self.agent_id,
                            capability_name=r.request.capability_name,
                            confidence=r.previous_state.get('confidence', 1.0),
                            metadata=r.previous_state.get('metadata')
                        )
                
                # Mark remaining as rolled back
                for r in results[len(applied):]:
                    r.status = UpdateStatus.ROLLED_BACK
                
                break
        
        return results
    
    def rollback(self, result: CapabilityUpdateResult) -> bool:
        """
        Rollback a capability update.
        
        Args:
            result: Previous update result to rollback
        
        Returns:
            True if rollback successful
        """
        if result.request.update_type == UpdateType.ACQUIRE:
            # Rollback: remove the capability
            return self.registry.deregister_capability(
                agent_id=self.agent_id,
                capability_name=result.request.capability_name
            )
        
        elif result.request.update_type == UpdateType.REMOVE and result.previous_state:
            # Rollback: restore the capability
            self.registry.register_capability(
                agent_id=self.agent_id,
                capability_name=result.request.capability_name,
                confidence=result.previous_state.get('confidence', 1.0),
                metadata=result.previous_state.get('metadata')
            )
            return True
        
        elif result.request.update_type == UpdateType.UPDATE and result.previous_state:
            # Rollback: restore previous confidence
            self.registry.register_capability(
                agent_id=self.agent_id,
                capability_name=result.request.capability_name,
                confidence=result.previous_state.get('confidence', 1.0),
                metadata=result.previous_state.get('metadata')
            )
            return True
        
        return False
    
    def _record_history(self, result: CapabilityUpdateResult):
        """Record update in history."""
        with self._history_lock:
            self._history.append(result)
            # Keep last 1000 updates
            if len(self._history) > 1000:
                self._history = self._history[-1000:]
    
    def get_history(self, limit: int = 100) -> List[CapabilityUpdateResult]:
        """Get update history."""
        with self._history_lock:
            return list(self._history[-limit:])
    
    def get_last_update(self, capability_name: str) -> Optional[CapabilityUpdateResult]:
        """Get last update for a specific capability."""
        with self._history_lock:
            for result in reversed(self._history):
                if result.request.capability_name == capability_name:
                    return result
        return None


# Standard verifiers for common capabilities

def create_python_import_verifier(module_name: str) -> Callable[[], bool]:
    """Create a verifier that checks if a Python module can be imported."""
    def verifier() -> bool:
        try:
            importlib.import_module(module_name)
            return True
        except ImportError:
            return False
    return verifier


def create_command_verifier(command: List[str]) -> Callable[[], bool]:
    """Create a verifier that checks if a command runs successfully."""
    def verifier() -> bool:
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                timeout=10
            )
            return result.returncode == 0
        except Exception:
            return False
    return verifier


def create_file_verifier(file_path: str) -> Callable[[], bool]:
    """Create a verifier that checks if a file exists."""
    def verifier() -> bool:
        return Path(file_path).exists()
    return verifier


# Example capability acquisition workflow

class CapabilityAcquisitionWorkflow:
    """
    Workflow for acquiring new capabilities.
    
    Steps:
    1. Install dependencies
    2. Load/learn capability
    3. Verify capability works
    4. Register with coordinator
    """
    
    def __init__(self, updater: CapabilityUpdater, coordinator_communicator=None):
        self.updater = updater
        self.coordinator_communicator = coordinator_communicator
    
    def acquire_with_dependencies(
        self,
        capability_name: str,
        dependencies: List[str],
        setup_commands: Optional[List[List[str]]] = None,
        confidence: float = 1.0
    ) -> CapabilityUpdateResult:
        """
        Acquire capability with dependency installation.
        
        Args:
            capability_name: Capability to acquire
            dependencies: Python packages to install
            setup_commands: Shell commands to run
            confidence: Initial confidence
        
        Returns:
            CapabilityUpdateResult
        """
        # Install dependencies
        for dep in dependencies:
            try:
                subprocess.run(
                    ['pip', 'install', dep],
                    capture_output=True,
                    timeout=60,
                    check=True
                )
            except subprocess.CalledProcessError as e:
                return CapabilityUpdateResult(
                    request=CapabilityUpdateRequest(
                        agent_id=self.updater.agent_id,
                        update_type=UpdateType.ACQUIRE,
                        capability_name=capability_name
                    ),
                    status=UpdateStatus.FAILED,
                    error=f"Failed to install {dep}: {e.stderr.decode()}"
                )
        
        # Run setup commands
        if setup_commands:
            for cmd in setup_commands:
                try:
                    subprocess.run(cmd, capture_output=True, timeout=60, check=True)
                except subprocess.CalledProcessError as e:
                    return CapabilityUpdateResult(
                        request=CapabilityUpdateRequest(
                            agent_id=self.updater.agent_id,
                            update_type=UpdateType.ACQUIRE,
                            capability_name=capability_name
                        ),
                        status=UpdateStatus.FAILED,
                        error=f"Setup command failed: {e.stderr.decode()}"
                    )
        
        # Verify by importing dependencies
        verifier = lambda: all(
            create_python_import_verifier(d.replace('-', '_'))()
            for d in dependencies
        )
        
        return self.updater.acquire_capability(
            capability_name=capability_name,
            confidence=confidence,
            verify=True,
            verifier=verifier
        )
