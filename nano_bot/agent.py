#!/usr/bin/env python3
"""
Nano Bot - Minimal Agent (Target: ~7.8KB)
Runs on any Python 3.6+ system (Android 5+, Linux, Windows, macOS)

Core responsibilities:
1. Register with coordinator
2. Respond to capability queries
3. Execute assigned tasks
4. Report progress/results
"""

import json
import time
import uuid
import threading
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Callable

# Configuration
DEFAULT_SHARED_DIR = "/tmp/agent_cluster"
HEARTBEAT_INTERVAL = 10  # seconds
HEARTBEAT_TIMEOUT = 30  # seconds (coordinator considers agent dead)


def _now_iso() -> str:
    """Return current UTC time as ISO string (timezone-aware)."""
    return datetime.now(timezone.utc).isoformat()


class NanoBot:
    """Minimal agent that can execute tasks and report results."""

    def __init__(self,
                 agent_id: str = None,
                 shared_dir: str = DEFAULT_SHARED_DIR,
                 capabilities: Dict[str, float] = None):
        """
        Initialize nano bot.

        Args:
            agent_id: Unique agent identifier (auto-generated if None)
            shared_dir: Directory for file-based communication
            capabilities: Dict of capability -> confidence (0.0-1.0)
        """
        self.agent_id = agent_id or f"agent-{uuid.uuid4().hex[:8]}"
        self.shared_dir = Path(shared_dir)
        self.capabilities = capabilities or {"echo": 1.0, "shell": 0.8}
        self.current_load = 0.0
        self.running = True
        self.last_heartbeat = time.time()

        # Setup directories
        self.inbox_dir = self.shared_dir / "agents" / self.agent_id / "inbox"
        self.outbox_dir = self.shared_dir / "agents" / self.agent_id / "outbox"
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.outbox_dir.mkdir(parents=True, exist_ok=True)

        # Task handlers registry
        self.handlers: Dict[str, Callable] = {
            "echo": self._handle_echo,
            "shell": self._handle_shell,
            "capability_query": self._handle_capability_query,
        }

    def _handle_echo(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Simple echo handler for testing."""
        return {"status": "success", "echo": params.get("message", "")}

    def _handle_shell(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute shell command (with safety limits)."""
        command = params.get("command", "")

        # Safety: block dangerous commands
        dangerous = ["rm -rf", "sudo", "chmod", "mkfs", "dd if="]
        if any(d in command for d in dangerous):
            return {"status": "error", "error": "Command blocked for safety"}

        try:
            result = subprocess.run(
                command, shell=True, capture_output=True, text=True, timeout=30
            )
            return {
                "status": "success",
                "stdout": result.stdout,
                "stderr": result.stderr,
                "returncode": result.returncode
            }
        except subprocess.TimeoutExpired:
            return {"status": "error", "error": "Command timeout (30s)"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def _handle_capability_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Respond to capability query from coordinator."""
        query = params.get("capability", "")
        confidence = self.capabilities.get(query, 0.0)
        return {
            "status": "success",
            "capability": query,
            "confidence": confidence,
            "load": self.current_load,
            "agent_id": self.agent_id
        }

    def send_message(self, msg_type: str, data: Dict[str, Any]) -> str:
        """Send message to coordinator via file-based comm."""
        msg_id = f"msg-{uuid.uuid4().hex[:8]}"
        message = {
            "id": msg_id,
            "type": msg_type,
            "agent_id": self.agent_id,
            "timestamp": _now_iso(),
            "data": data
        }
        msg_file = self.outbox_dir / f"{msg_id}.json"
        with open(msg_file, "w") as f:
            json.dump(message, f, indent=2)
        return msg_id

    def check_inbox(self) -> List[Dict[str, Any]]:
        """Check inbox for new messages."""
        messages = []
        for msg_file in self.inbox_dir.glob("*.json"):
            try:
                with open(msg_file) as f:
                    messages.append(json.load(f))
                # Delete after reading (acknowledge receipt)
                msg_file.unlink()
            except Exception as e:
                print(f"[WARN] Failed to read {msg_file}: {e}")

        # Sort by timestamp
        messages.sort(key=lambda m: m.get("timestamp", ""))
        return messages

    def process_message(self, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process incoming message and return response."""
        msg_type = msg.get("type", "")
        params = msg.get("data", {})

        # For task_assign messages, route to the capability handler
        if msg_type == "task_assign":
            capability = params.get("capability", "")
            handler = self.handlers.get(capability)
            if handler:
                task_params = params.get("params", params)
                return handler(task_params)
            return {"status": "error", "error": f"No handler for capability: {capability}"}

        # Direct message type lookup
        handler = self.handlers.get(msg_type)
        if handler:
            return handler(params)
        else:
            return {"status": "error", "error": f"Unknown message type: {msg_type}"}

    def send_heartbeat(self):
        """Send heartbeat to coordinator."""
        self.send_message("heartbeat", {
            "load": self.current_load,
            "capabilities": self.capabilities,
            "status": "alive"
        })
        self.last_heartbeat = time.time()

    def register(self):
        """Register with coordinator."""
        self.send_message("registration", {
            "agent_id": self.agent_id,
            "capabilities": self.capabilities,
            "status": "ready"
        })
        print(f"[INFO] Agent {self.agent_id} registered")

    def run(self):
        """Main agent loop."""
        print(f"[INFO] Nano bot {self.agent_id} starting...")
        print(f"[INFO] Capabilities: {list(self.capabilities.keys())}")
        print(f"[INFO] Shared dir: {self.shared_dir}")

        # Register with coordinator
        self.register()
        last_heartbeat = time.time()

        while self.running:
            # Check inbox for new messages
            messages = self.check_inbox()
            for msg in messages:
                print(f"[RECV] {msg.get('type')}: {msg.get('id')}")

                # Process message
                response = self.process_message(msg)

                # Send response if needed
                if response:
                    # Use task_id from data if available, fall back to message id
                    request_id = msg.get("data", {}).get("task_id") or msg.get("id")
                    self.send_message(f"{msg.get('type')}_response", {
                        "request_id": request_id,
                        "response": response
                    })

            # Send heartbeat periodically
            if time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
                self.send_heartbeat()
                last_heartbeat = time.time()

            # Sleep to avoid busy loop
            time.sleep(1)

    def stop(self):
        """Stop the agent."""
        self.running = False
        self.send_message("shutdown", {"reason": "agent_stopped"})
        print(f"[INFO] Agent {self.agent_id} stopped")


def main():
    """Entry point for standalone agent."""
    import argparse
    parser = argparse.ArgumentParser(description="Nano Bot - Minimal Agent")
    parser.add_argument("--id", help="Agent ID", default=None)
    parser.add_argument("--shared-dir", help="Shared directory", default=DEFAULT_SHARED_DIR)
    parser.add_argument("--capabilities", help="JSON file with capabilities", default=None)
    args = parser.parse_args()

    # Load capabilities if provided
    caps = {"echo": 1.0, "shell": 0.8}
    if args.capabilities:
        try:
            with open(args.capabilities) as f:
                caps = json.load(f)
        except Exception as e:
            print(f"[WARN] Failed to load capabilities: {e}")

    # Create and run agent
    agent = NanoBot(
        agent_id=args.id,
        shared_dir=args.shared_dir,
        capabilities=caps
    )
    try:
        agent.run()
    except KeyboardInterrupt:
        agent.stop()


if __name__ == "__main__":
    main()
