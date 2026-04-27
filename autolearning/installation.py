#!/usr/bin/env python3
"""
Tool Installation Module

Handles installation of tools/packages across the agent cluster.

Workflow:
1. Receive SolutionProposal from research phase
2. Validate installation command
3. Execute installation with safety checks
4. Verify installation succeeded
5. Rollback on failure

Supports:
- pip (Python packages)
- apt (system packages)
- npm (Node packages)
- custom (shell scripts)
"""

import subprocess
import json
import sys
import shutil
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import threading
import queue
import time


class InstallationStatus(Enum):
    """Status of a tool installation."""
    PENDING = "pending"
    VALIDATING = "validating"
    INSTALLING = "installing"
    VERIFYING = "verifying"
    SUCCESS = "success"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class ToolType(Enum):
    """Types of tools that can be installed."""
    PIP = "pip"
    APT = "apt"
    NPM = "npm"
    CUSTOM = "custom"
    HOMEBREW = "brew"
    GIT = "git"


@dataclass
class InstallationRequest:
    """Request to install a tool."""
    
    installation_id: str
    tool_name: str
    tool_type: ToolType
    install_command: str
    verification_command: str
    target_agents: List[str]
    requester_id: str = "coordinator"
    timeout_seconds: int = 300
    rollback_on_failure: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "installation_id": self.installation_id,
            "tool_name": self.tool_name,
            "tool_type": self.tool_type.value,
            "install_command": self.install_command,
            "verification_command": self.verification_command,
            "target_agents": self.target_agents,
            "requester_id": self.requester_id,
            "timeout_seconds": self.timeout_seconds,
            "rollback_on_failure": self.rollback_on_failure,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class InstallationResult:
    """Result of a tool installation."""
    
    installation_id: str
    agent_id: str
    tool_name: str
    status: InstallationStatus
    output: str = ""
    error: Optional[str] = None
    install_time_seconds: float = 0.0
    verified: bool = False
    rolled_back: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "installation_id": self.installation_id,
            "agent_id": self.agent_id,
            "tool_name": self.tool_name,
            "status": self.status.value,
            "output": self.output,
            "error": self.error,
            "install_time_seconds": self.install_time_seconds,
            "verified": self.verified,
            "rolled_back": self.rolled_back,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class RollbackInfo:
    """Information needed to rollback an installation."""
    
    tool_name: str
    tool_type: ToolType
    uninstall_command: str
    backup_location: Optional[str] = None
    pre_install_state: Optional[Dict[str, Any]] = None


class ToolInstaller:
    """
    Handles tool installation with safety and rollback.
    
    Features:
    - Command validation (no rm -rf, etc.)
    - Timeout enforcement
    - Automatic rollback on failure
    - Parallel installation to multiple agents
    """
    
    # Dangerous patterns in commands
    DANGEROUS_PATTERNS = [
        "rm -rf /",
        "rm -rf ~",
        "> /dev/sd",
        "mkfs",
        "dd if=",
        ":(){ :|:& };:",  # Fork bomb
        "chmod -R 777 /",
        "curl | sh",  # Blind curl to shell
        "wget | sh",
    ]
    
    # Safe curl/wget patterns (with checksums or known sources)
    SAFE_SOURCES = [
        "https://pypi.org",
        "https://files.pythonhosted.org",
        "https://github.com/",
        "https://apt.",
        "https://deb.",
    ]
    
    def __init__(self, agent_id: str = "coordinator"):
        """
        Initialize tool installer.
        
        Args:
            agent_id: ID of this agent (for result attribution)
        """
        self.agent_id = agent_id
        self.installations: Dict[str, InstallationResult] = {}
        self.rollback_info: Dict[str, RollbackInfo] = {}
        self.safety_checks_enabled = True
        self._install_lock = threading.Lock()
    
    def validate_command(self, command: str) -> Tuple[bool, str]:
        """
        Validate an installation command for safety.
        
        Args:
            command: The command to validate
        
        Returns:
            Tuple of (is_safe, reason)
        """
        if not self.safety_checks_enabled:
            return True, "Safety checks disabled"
        
        # Check for dangerous patterns
        for pattern in self.DANGEROUS_PATTERNS:
            if pattern in command:
                return False, f"Dangerous pattern detected: {pattern}"
        
        # Check for privilege escalation
        if "sudo" in command and "apt" not in command and "pip" not in command:
            return False, "Unapproved sudo usage"
        
        return True, "Command validated"
    
    def get_uninstall_command(self, tool_name: str, tool_type: ToolType) -> str:
        """
        Get the uninstall command for a tool.
        
        Args:
            tool_name: Name of the tool
            tool_type: Type of the tool
        
        Returns:
            Uninstall command string
        """
        uninstall_commands = {
            ToolType.PIP: f"pip uninstall -y {tool_name}",
            ToolType.APT: f"apt remove -y {tool_name}",
            ToolType.NPM: f"npm uninstall {tool_name}",
            ToolType.HOMEBREW: f"brew uninstall {tool_name}",
            ToolType.GIT: f"rm -rf {tool_name}",
            ToolType.CUSTOM: f"# Custom uninstall not specified for {tool_name}",
        }
        return uninstall_commands.get(tool_type, "# Unknown tool type")
    
    def prepare_rollback(self, request: InstallationRequest) -> RollbackInfo:
        """
        Prepare rollback information before installation.
        
        Args:
            request: Installation request
        
        Returns:
            RollbackInfo for potential rollback
        """
        rollback = RollbackInfo(
            tool_name=request.tool_name,
            tool_type=request.tool_type,
            uninstall_command=self.get_uninstall_command(
                request.tool_name, request.tool_type
            ),
        )
        self.rollback_info[request.installation_id] = rollback
        return rollback
    
    def execute_install(
        self,
        request: InstallationRequest,
        dry_run: bool = False,
    ) -> InstallationResult:
        """
        Execute a tool installation.
        
        Args:
            request: Installation request
            dry_run: If True, simulate without executing
        
        Returns:
            InstallationResult
        """
        import uuid
        
        result = InstallationResult(
            installation_id=request.installation_id,
            agent_id=self.agent_id,
            tool_name=request.tool_name,
            status=InstallationStatus.PENDING,
        )
        
        # Validate command
        result.status = InstallationStatus.VALIDATING
        is_safe, reason = self.validate_command(request.install_command)
        
        if not is_safe:
            result.status = InstallationStatus.FAILED
            result.error = f"Validation failed: {reason}"
            self.installations[request.installation_id] = result
            return result
        
        # Prepare rollback
        self.prepare_rollback(request)
        
        # Execute installation
        result.status = InstallationStatus.INSTALLING
        start_time = time.time()
        
        if dry_run:
            result.output = f"[DRY RUN] Would execute: {request.install_command}"
            result.status = InstallationStatus.SUCCESS
            result.install_time_seconds = 0.0
        else:
            try:
                proc = subprocess.run(
                    request.install_command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=request.timeout_seconds,
                )
                
                result.output = proc.stdout
                result.install_time_seconds = time.time() - start_time
                
                if proc.returncode != 0:
                    result.status = InstallationStatus.FAILED
                    result.error = proc.stderr or f"Exit code: {proc.returncode}"
                    
                    # Rollback if enabled
                    if request.rollback_on_failure:
                        self.rollback(request.installation_id)
                        result.rolled_back = True
                        result.status = InstallationStatus.ROLLED_BACK
                else:
                    result.status = InstallationStatus.SUCCESS
                    
            except subprocess.TimeoutExpired:
                result.status = InstallationStatus.FAILED
                result.error = f"Timeout after {request.timeout_seconds}s"
                result.install_time_seconds = request.timeout_seconds
                
                if request.rollback_on_failure:
                    self.rollback(request.installation_id)
                    result.rolled_back = True
                    result.status = InstallationStatus.ROLLED_BACK
                    
            except Exception as e:
                result.status = InstallationStatus.FAILED
                result.error = str(e)
        
        self.installations[request.installation_id] = result
        return result
    
    def verify_installation(
        self,
        request: InstallationRequest,
        result: InstallationResult,
    ) -> bool:
        """
        Verify that installation succeeded.
        
        Args:
            request: Original installation request
            result: Installation result to verify
        
        Returns:
            True if verification passed
        """
        if result.status not in (InstallationStatus.SUCCESS, InstallationStatus.INSTALLING):
            return False
        
        try:
            proc = subprocess.run(
                request.verification_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            
            verified = proc.returncode == 0
            result.verified = verified
            return verified
            
        except Exception as e:
            result.error = f"Verification failed: {e}"
            return False
    
    def rollback(self, installation_id: str) -> bool:
        """
        Rollback a failed installation.
        
        Args:
            installation_id: ID of installation to rollback
        
        Returns:
            True if rollback succeeded
        """
        rollback = self.rollback_info.get(installation_id)
        if not rollback:
            return False
        
        try:
            proc = subprocess.run(
                rollback.uninstall_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
            return proc.returncode == 0
        except Exception:
            return False
    
    def create_install_request(
        self,
        tool_name: str,
        tool_type: ToolType,
        install_command: str,
        verification_command: str,
        target_agents: List[str],
    ) -> InstallationRequest:
        """
        Create an installation request.
        
        Args:
            tool_name: Name of tool to install
            tool_type: Type of tool
            install_command: Command to install
            verification_command: Command to verify installation
            target_agents: Agents to install on
        
        Returns:
            InstallationRequest
        """
        import uuid
        
        return InstallationRequest(
            installation_id=f"install-{uuid.uuid4().hex[:12]}",
            tool_name=tool_name,
            tool_type=tool_type,
            install_command=install_command,
            verification_command=verification_command,
            target_agents=target_agents,
        )
    
    def broadcast_install(
        self,
        request: InstallationRequest,
        message_queue: Any,
    ) -> List[Any]:
        """
        Create messages to broadcast installation request.
        
        Args:
            request: Installation request
            message_queue: Message queue for creating messages
        
        Returns:
            List of messages to send
        """
        from protocol.messages import create_message, MessageType
        
        messages = []
        for agent_id in request.target_agents:
            msg = create_message(
                msg_type=MessageType.TOOL_INSTALL,
                sender_id=self.agent_id,
                recipient_id=agent_id,
                payload=request.to_dict(),
            )
            messages.append(msg)
        
        return messages


# Convenience functions

def install_pip_package(package: str, target_agents: List[str]) -> InstallationRequest:
    """Create request to install a pip package."""
    installer = ToolInstaller()
    return installer.create_install_request(
        tool_name=package,
        tool_type=ToolType.PIP,
        install_command=f"pip install {package}",
        verification_command=f"python -c 'import {package.replace('-', '_')}'",
        target_agents=target_agents,
    )


def install_apt_package(package: str, target_agents: List[str]) -> InstallationRequest:
    """Create request to install an apt package."""
    installer = ToolInstaller()
    return installer.create_install_request(
        tool_name=package,
        tool_type=ToolType.APT,
        install_command=f"apt install -y {package}",
        verification_command=f"which {package}",
        target_agents=target_agents,
    )
