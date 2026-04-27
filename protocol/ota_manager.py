"""
OTA Update Manager for Agent Cluster.

Handles the full lifecycle of Over-The-Air updates:
- Coordinator side: announce, package, track fleet-wide status, command rollbacks
- Agent side: receive, verify, install, report status, execute rollbacks

No user consent is required — updates install automatically upon receipt.
"""

import base64
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import zipfile
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class OTAUpdatePackage:
    """Represents an OTA update package with metadata."""

    def __init__(
        self,
        version: str,
        changelog: str = "",
        priority: str = "normal",
        deadline: str = "",
        requires_restart: bool = False,
        package_path: Optional[str] = None,
        package_data: Optional[str] = None,  # base64
        package_type: str = "tar.gz",
        checksum: str = "",
        install_script: str = "",
        pre_install: str = "",
        post_install: str = "",
        rollback_script: str = "",
    ):
        self.version = version
        self.changelog = changelog
        self.priority = priority
        self.deadline = deadline
        self.requires_restart = requires_restart
        self.package_path = package_path
        self.package_data = package_data
        self.package_type = package_type
        self.checksum = checksum
        self.install_script = install_script
        self.pre_install = pre_install
        self.post_install = post_install
        self.rollback_script = rollback_script
        self.created_at = datetime.now(timezone.utc).isoformat() + "Z"

    def compute_checksum(self) -> str:
        """Compute SHA-256 checksum of the package data."""
        if self.package_data:
            raw = base64.b64decode(self.package_data)
            return hashlib.sha256(raw).hexdigest()
        elif self.package_path and os.path.exists(self.package_path):
            h = hashlib.sha256()
            with open(self.package_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    h.update(chunk)
            return h.hexdigest()
        return ""

    def verify_checksum(self) -> bool:
        """Verify the package checksum matches the stored checksum."""
        if not self.checksum:
            return True  # No checksum to verify
        computed = self.compute_checksum()
        return computed == self.checksum

    def to_dict(self) -> Dict:
        return {
            "version": self.version,
            "changelog": self.changelog,
            "priority": self.priority,
            "deadline": self.deadline,
            "requires_restart": self.requires_restart,
            "package_type": self.package_type,
            "checksum": self.checksum,
            "install_script": self.install_script,
            "pre_install": self.pre_install,
            "post_install": self.post_install,
            "rollback_script": self.rollback_script,
            "created_at": self.created_at,
        }


class CoordinatorOTAManager:
    """
    Coordinator-side OTA update management.

    Tracks update packages, fleet-wide deployment status, and rollback state.
    Announces updates to all agents and sends packages on ACK.
    """

    def __init__(self, install_dir: str = "/tmp/agent_cluster_ota"):
        self.install_dir = install_dir
        os.makedirs(install_dir, exist_ok=True)

        self._lock = threading.Lock()

        # Available updates: version → OTAUpdatePackage
        self._packages: Dict[str, OTAUpdatePackage] = {}

        # Fleet status: version → {agent_id: OTAUpdateStatus}
        self._fleet_status: Dict[str, Dict[str, Dict]] = {}

        # Active rollout: version → {announced, acked, packaged, installed, failed, rolled_back}
        self._rollouts: Dict[str, Dict] = {}

        # Rollback info: version → {available, target_version, rollback_script}
        self._rollback_info: Dict[str, Dict] = {}

    def register_update(
        self,
        version: str,
        package_path: str,
        changelog: str = "",
        priority: str = "normal",
        deadline: str = "",
        requires_restart: bool = False,
        install_script: str = "",
        pre_install: str = "",
        post_install: str = "",
        rollback_script: str = "",
    ) -> OTAUpdatePackage:
        """Register a new OTA update package for deployment."""
        with self._lock:
            # Read and encode the package
            with open(package_path, "rb") as f:
                raw = f.read()
            package_data = base64.b64encode(raw).decode()

            pkg = OTAUpdatePackage(
                version=version,
                changelog=changelog,
                priority=priority,
                deadline=deadline,
                requires_restart=requires_restart,
                package_data=package_data,
                package_type=self._detect_type(package_path),
                checksum=hashlib.sha256(raw).hexdigest(),
                install_script=install_script,
                pre_install=pre_install,
                post_install=post_install,
                rollback_script=rollback_script,
            )

            self._packages[version] = pkg
            self._fleet_status[version] = {}
            self._rollouts[version] = {
                "announced": 0,
                "acked": 0,
                "packaged": 0,
                "installed": 0,
                "failed": 0,
                "rolled_back": 0,
                "started_at": datetime.now(timezone.utc).isoformat() + "Z",
                "status": "pending",
            }
            self._rollback_info[version] = {
                "available": bool(rollback_script),
                "target_version": "",
                "rollback_script": rollback_script,
            }

            return pkg

    def _detect_type(self, path: str) -> str:
        """Detect archive type from file extension."""
        if path.endswith(".tar.gz") or path.endswith(".tgz"):
            return "tar.gz"
        elif path.endswith(".zip"):
            return "zip"
        elif path.endswith(".whl"):
            return "whl"
        return "tar.gz"

    def get_announce_message(self, version: str) -> Optional[Dict]:
        """Get the payload for an OTA_UPDATE_ANNOUNCE message."""
        with self._lock:
            pkg = self._packages.get(version)
            if not pkg:
                return None
            return {
                "version": pkg.version,
                "changelog": pkg.changelog,
                "priority": pkg.priority,
                "deadline": pkg.deadline,
                "checksum": pkg.checksum,
                "size_bytes": len(base64.b64decode(pkg.package_data)) if pkg.package_data else 0,
                "requires_restart": pkg.requires_restart,
            }

    def get_package_message(self, version: str, agent_id: str) -> Optional[Dict]:
        """Get the payload for an OTA_UPDATE_PACKAGE message to a specific agent."""
        with self._lock:
            pkg = self._packages.get(version)
            if not pkg:
                return None
            return {
                "version": pkg.version,
                "package_data": pkg.package_data,
                "package_type": pkg.package_type,
                "checksum": pkg.checksum,
                "install_script": pkg.install_script,
                "pre_install": pkg.pre_install,
                "post_install": pkg.post_install,
                "rollback_script": pkg.rollback_script,
            }

    def record_ack(self, version: str, agent_id: str, ready: bool, current_version: str = ""):
        """Record an agent's ACK response to an update announcement."""
        with self._lock:
            if version in self._fleet_status:
                self._fleet_status[version][agent_id] = {
                    "status": "acked_ready" if ready else "acked_not_ready",
                    "current_version": current_version,
                    "acked_at": datetime.now(timezone.utc).isoformat() + "Z",
                }
                if ready:
                    self._rollouts[version]["acked"] += 1

    def record_status(self, version: str, agent_id: str, status: str, message: str = ""):
        """Record an agent's installation status update."""
        with self._lock:
            if version in self._fleet_status:
                entry = self._fleet_status[version].get(agent_id, {})
                entry["status"] = status
                entry["message"] = message
                entry["updated_at"] = datetime.now(timezone.utc).isoformat() + "Z"
                self._fleet_status[version][agent_id] = entry

                # Update rollout counters
                rollout = self._rollouts.get(version, {})
                if status == "success":
                    rollout["installed"] = rollout.get("installed", 0) + 1
                elif status == "failed":
                    rollout["failed"] = rollout.get("failed", 0) + 1
                elif status == "rolled_back":
                    rollout["rolled_back"] = rollout.get("rolled_back", 0) + 1
                elif status == "downloading":
                    rollout["packaged"] = rollout.get("packaged", 0) + 1

    def get_fleet_status(self, version: Optional[str] = None) -> Dict:
        """Get fleet-wide update status."""
        with self._lock:
            if version:
                return {
                    "version": version,
                    "rollout": self._rollouts.get(version, {}),
                    "agents": self._fleet_status.get(version, {}),
                }
            return {
                "packages": {v: p.to_dict() for v, p in self._packages.items()},
                "rollouts": self._rollouts,
                "fleet": self._fleet_status,
            }

    def get_rollback_info(self, version: str) -> Dict:
        """Get rollback information for a version."""
        with self._lock:
            return self._rollback_info.get(version, {"available": False})

    def mark_announced(self, version: str, count: int = 1):
        """Track how many agents were announced to."""
        with self._lock:
            if version in self._rollouts:
                self._rollouts[version]["announced"] += count
                self._rollouts[version]["status"] = "announced"


class AgentOTAInstaller:
    """
    Agent-side OTA update installer.

    Receives update packages, verifies checksums, installs automatically
    (no user consent needed), and reports status back to coordinator.
    Supports rollback on failure.
    """

    # Valid status values
    STATUS_DOWNLOADING = "downloading"
    STATUS_VERIFYING = "verifying"
    STATUS_PRE_INSTALL = "pre_install"
    STATUS_INSTALLING = "installing"
    STATUS_POST_INSTALL = "post_install"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"
    STATUS_ROLLED_BACK = "rolled_back"

    def __init__(
        self,
        agent_id: str,
        install_dir: str,
        current_version: str = "0.0.0",
        backup_dir: Optional[str] = None,
        on_restart_needed: Optional[callable] = None,
    ):
        self.agent_id = agent_id
        self.install_dir = install_dir
        self.current_version = current_version
        self.backup_dir = backup_dir or os.path.join(install_dir, "..", "ota_backups")
        self.on_restart_needed = on_restart_needed

        self._lock = threading.Lock()
        self._install_history: List[Dict] = []
        self._current_install: Optional[Dict] = None

        os.makedirs(self.backup_dir, exist_ok=True)

    def install_update(self, package_payload: Dict) -> Dict:
        """
        Full OTA install pipeline. No user consent — installs automatically.

        Steps:
        1. Decode package
        2. Verify checksum
        3. Pre-install hook (backup)
        4. Extract + install
        5. Post-install hook
        6. Report success/failure

        Returns status dict suitable for OTA_UPDATE_STATUS message.
        """
        version = package_payload.get("version", "unknown")
        package_data = package_payload.get("package_data", "")
        checksum = package_payload.get("checksum", "")
        package_type = package_payload.get("package_type", "tar.gz")
        install_script = package_payload.get("install_script", "")
        pre_install = package_payload.get("pre_install", "")
        post_install = package_payload.get("post_install", "")
        rollback_script = package_payload.get("rollback_script", "")

        previous_version = self.current_version

        with self._lock:
            self._current_install = {
                "version": version,
                "started_at": datetime.now(timezone.utc).isoformat() + "Z",
                "status": self.STATUS_DOWNLOADING,
            }

        try:
            # 1. Decode package
            raw_data = base64.b64decode(package_data)
            self._update_status(self.STATUS_DOWNLOADING, "Package decoded")

            # 2. Verify checksum
            computed = hashlib.sha256(raw_data).hexdigest()
            if checksum and computed != checksum:
                raise ValueError(
                    f"Checksum mismatch: expected {checksum}, got {computed}"
                )
            self._update_status(self.STATUS_VERIFYING, "Checksum verified")

            # 3. Pre-install (backup current installation)
            backup_path = self._create_backup(previous_version)
            self._update_status(self.STATUS_PRE_INSTALL, f"Backup created at {backup_path}")

            # Run pre-install script if provided
            if pre_install:
                result = subprocess.run(
                    pre_install, shell=True, capture_output=True, text=True, timeout=60
                )
                if result.returncode != 0:
                    raise RuntimeError(f"Pre-install script failed: {result.stderr}")

            # 4. Extract and install
            staging_dir = tempfile.mkdtemp(prefix=f"ota_{version}_")
            try:
                self._extract_package(raw_data, package_type, staging_dir)
                self._update_status(self.STATUS_INSTALLING, "Package extracted")

                # Run install script if provided
                if install_script:
                    result = subprocess.run(
                        install_script, shell=True, capture_output=True, text=True,
                        timeout=300, cwd=staging_dir,
                    )
                    if result.returncode != 0:
                        raise RuntimeError(f"Install script failed: {result.stderr}")

                # Copy files to install dir
                self._deploy_files(staging_dir, self.install_dir)
                self._update_status(self.STATUS_INSTALLING, "Files deployed")
            finally:
                shutil.rmtree(staging_dir, ignore_errors=True)

            # 5. Post-install
            if post_install:
                result = subprocess.run(
                    post_install, shell=True, capture_output=True, text=True, timeout=60
                )
                if result.returncode != 0:
                    # Post-install failure is a warning, not fatal
                    self._update_status(self.STATUS_POST_INSTALL, f"Post-install warning: {result.stderr}")
                else:
                    self._update_status(self.STATUS_POST_INSTALL, "Post-install completed")

            # 6. Success
            self.current_version = version
            self._update_status(self.STATUS_SUCCESS, f"Updated to {version}")

            # Record in history
            self._install_history.append({
                "version": version,
                "previous_version": previous_version,
                "backup_path": backup_path,
                "rollback_script": rollback_script,
                "status": self.STATUS_SUCCESS,
                "completed_at": datetime.now(timezone.utc).isoformat() + "Z",
            })

            return {
                "version": version,
                "status": self.STATUS_SUCCESS,
                "message": f"Successfully updated to {version}",
                "previous_version": previous_version,
                "rollback_available": True,
            }

        except Exception as e:
            # Attempt automatic rollback
            rollback_result = self._auto_rollback(previous_version, str(e))

            self._install_history.append({
                "version": version,
                "previous_version": previous_version,
                "status": self.STATUS_FAILED,
                "error": str(e),
                "rollback_result": rollback_result,
                "completed_at": datetime.now(timezone.utc).isoformat() + "Z",
            })

            final_status = self.STATUS_ROLLED_BACK if rollback_result.get("success") else self.STATUS_FAILED
            self._update_status(final_status, str(e))

            return {
                "version": version,
                "status": final_status,
                "message": f"Install failed: {e}. Rollback: {rollback_result.get('message', 'N/A')}",
                "previous_version": previous_version,
                "rollback_available": rollback_result.get("success", False),
            }

    def execute_rollback(self, rollback_payload: Dict) -> Dict:
        """Execute a coordinator-commanded rollback."""
        version = rollback_payload.get("version", "")
        target_version = rollback_payload.get("target_version", "")
        reason = rollback_payload.get("reason", "")

        # Find the most recent successful install for the target version
        target_backup = None
        for entry in reversed(self._install_history):
            if entry.get("status") == self.STATUS_SUCCESS and (
                not target_version or entry.get("previous_version") == target_version
            ):
                target_backup = entry.get("backup_path")
                break

        if not target_backup or not os.path.exists(target_backup):
            return {"success": False, "message": f"No backup found for rollback to {target_version or 'previous'}"}

        try:
            self._deploy_files(target_backup, self.install_dir)
            self.current_version = target_version or self.current_version
            return {"success": True, "message": f"Rolled back to {target_version or 'previous'}"}
        except Exception as e:
            return {"success": False, "message": f"Rollback failed: {e}"}

    def _auto_rollback(self, previous_version: str, error: str) -> Dict:
        """Attempt automatic rollback on install failure."""
        # Find backup for previous version
        for entry in reversed(self._install_history):
            if entry.get("previous_version") == previous_version and entry.get("backup_path"):
                backup_path = entry["backup_path"]
                if os.path.exists(backup_path):
                    try:
                        self._deploy_files(backup_path, self.install_dir)
                        self.current_version = previous_version
                        return {"success": True, "message": f"Auto-rolled back to {previous_version}"}
                    except Exception as e:
                        return {"success": False, "message": f"Auto-rollback failed: {e}"}

        # Try rollback script from the failed install's package
        for entry in reversed(self._install_history):
            if entry.get("rollback_script"):
                try:
                    result = subprocess.run(
                        entry["rollback_script"], shell=True, capture_output=True,
                        text=True, timeout=60,
                    )
                    if result.returncode == 0:
                        self.current_version = previous_version
                        return {"success": True, "message": "Rollback script executed"}
                    return {"success": False, "message": f"Rollback script failed: {result.stderr}"}
                except Exception as e:
                    return {"success": False, "message": f"Rollback script error: {e}"}

        return {"success": False, "message": "No rollback method available"}

    def _create_backup(self, version: str) -> str:
        """Create a backup of the current installation."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(
            self.backup_dir, f"backup_{version}_{timestamp}"
        )
        if os.path.exists(self.install_dir):
            shutil.copytree(self.install_dir, backup_path)
        else:
            os.makedirs(backup_path, exist_ok=True)
        return backup_path

    def _extract_package(self, raw_data: bytes, package_type: str, target_dir: str):
        """Extract package data to target directory."""
        if package_type == "tar.gz":
            with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
                tmp.write(raw_data)
                tmp_path = tmp.name
            try:
                with tarfile.open(tmp_path, "r:gz") as tar:
                    tar.extractall(path=target_dir)
            finally:
                os.unlink(tmp_path)
        elif package_type == "zip":
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
                tmp.write(raw_data)
                tmp_path = tmp.name
            try:
                with zipfile.ZipFile(tmp_path, "r") as zf:
                    zf.extractall(path=target_dir)
            finally:
                os.unlink(tmp_path)
        elif package_type == "whl":
            # Wheel is just a zip
            with tempfile.NamedTemporaryFile(suffix=".whl", delete=False) as tmp:
                tmp.write(raw_data)
                tmp_path = tmp.name
            try:
                with zipfile.ZipFile(tmp_path, "r") as zf:
                    zf.extractall(path=target_dir)
            finally:
                os.unlink(tmp_path)
        else:
            # Raw — just write the bytes
            with open(os.path.join(target_dir, "update_payload"), "wb") as f:
                f.write(raw_data)

    def _deploy_files(self, source_dir: str, target_dir: str):
        """Deploy files from source to target directory."""
        if not os.path.exists(source_dir):
            return
        for item in os.listdir(source_dir):
            src = os.path.join(source_dir, item)
            dst = os.path.join(target_dir, item)
            if os.path.isdir(src):
                if os.path.exists(dst):
                    shutil.rmtree(dst)
                shutil.copytree(src, dst)
            else:
                shutil.copy2(src, dst)

    def _update_status(self, status: str, message: str = ""):
        """Update current install status."""
        with self._lock:
            if self._current_install:
                self._current_install["status"] = status
                self._current_install["message"] = message

    def get_install_history(self) -> List[Dict]:
        """Return the full install history."""
        with self._lock:
            return list(self._install_history)

    def get_current_install_status(self) -> Optional[Dict]:
        """Return current in-progress install status."""
        with self._lock:
            return dict(self._current_install) if self._current_install else None
