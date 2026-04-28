#!/usr/bin/env python3
"""
Nano Bot - Minimal Agent for Agent Cluster MVP

Target: ~7.8KB Python script
Runs on: Any Python 3.7+ system (Android 5+, Linux, macOS, Windows)

Usage:
    python nano_bot.py --id my-agent-001 --type worker --coordinator ./cluster_data
"""

import os
import sys
import json
import time
import uuid
import signal
import argparse
import threading
import subprocess
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from pathlib import Path

# === Minimal Imports (stdlib only) ===

# Add protocol to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from protocol.messages import (
    MessageType, BaseMessage, MessageQueue, AgentCapability,
    heartbeat, register_agent, capability_response,
    task_progress, task_complete, task_failed,
    create_message
)


@dataclass
class BotConfig:
    """Nano bot configuration."""
    agent_id: str
    agent_type: str
    cluster_dir: str
    heartbeat_interval: float = 30.0  # seconds
    poll_interval: float = 1.0  # seconds
    capabilities: List[Dict] = None
    max_tasks: int = 3  # concurrent tasks
    
    def __post_init__(self):
        if self.capabilities is None:
            self.capabilities = []


class NanoBot:
    """
    Minimal agent that can:
    1. Register with coordinator
    2. Respond to capability queries
    3. Execute assigned tasks
    4. Report progress and results
    """
    
    def __init__(self, config: BotConfig):
        self.config = config
        self.agent_id = config.agent_id
        self.agent_type = config.agent_type
        self.running = False
        self.current_load = 0.0
        self.current_tasks: Dict[str, threading.Thread] = {}
        
        # Message queue
        self.queue = MessageQueue(config.cluster_dir, config.agent_id)
        
        # Capability registry
        self.capabilities: Dict[str, AgentCapability] = {}
        for cap in config.capabilities:
            self.capabilities[cap['name']] = AgentCapability(
                name=cap['name'],
                confidence=cap.get('confidence', 0.8),
                metadata=cap.get('metadata')
            )
        
        # Task handlers (extendable)
        self.handlers: Dict[str, Callable] = {
            'ping': self._handle_ping,
            'echo': self._handle_echo,
            'shell': self._handle_shell,
            'python': self._handle_python,
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def start(self):
        """Start the agent main loop."""
        print(f"[{self.agent_id}] Starting nano bot...")
        print(f"[{self.agent_id}] Type: {self.agent_type}")
        print(f"[{self.agent_id}] Capabilities: {list(self.capabilities.keys())}")
        
        self.running = True
        
        # Register with coordinator
        self._register()
        
        # Main loop
        while self.running:
            try:
                self._process_messages()
                self._send_heartbeat()
                time.sleep(self.config.poll_interval)
            except Exception as e:
                print(f"[{self.agent_id}] Error: {e}")
                time.sleep(5)
    
    def _register(self):
        """Send registration to coordinator."""
        device_info = {
            "hostname": os.uname().nodename if hasattr(os, 'uname') else "unknown",
            "platform": sys.platform,
            "python_version": sys.version,
            "pid": os.getpid(),
        }
        
        caps_list = list(self.capabilities.values())
        msg = register_agent(
            agent_id=self.agent_id,
            agent_type=self.agent_type,
            capabilities=caps_list,
            device_info=device_info
        )
        
        self.queue.send(msg)
        print(f"[{self.agent_id}] Registration sent")
    
    def _send_heartbeat(self):
        """Periodic heartbeat to coordinator."""
        # Only send if enough time has passed
        if not hasattr(self, '_last_heartbeat'):
            self._last_heartbeat = 0
        
        now = time.time()
        if now - self._last_heartbeat >= self.config.heartbeat_interval:
            msg = heartbeat(self.agent_id, self.current_load)
            self.queue.send(msg)
            self._last_heartbeat = now
            # Silent heartbeat (only log if debugging)
    
    def _process_messages(self):
        """Check inbox for new messages."""
        messages = self.queue.receive(include_broadcast=True)
        
        for filepath, msg in messages:
            try:
                self._handle_message(msg)
                self.queue.mark_processed(filepath)
            except Exception as e:
                print(f"[{self.agent_id}] Failed to process {msg.msg_type.value}: {e}")
    
    def _handle_message(self, msg: BaseMessage):
        """Route message to appropriate handler."""
        handler_map = {
            MessageType.CAPABILITY_QUERY: self._handle_capability_query,
            MessageType.TASK_ASSIGN: self._handle_task_assign,
            MessageType.TOOL_INSTALL: self._handle_tool_install,
            MessageType.SHUTDOWN: self._handle_shutdown_msg,
        }
        
        handler = handler_map.get(msg.msg_type)
        if handler:
            handler(msg)
        else:
            print(f"[{self.agent_id}] Unknown message type: {msg.msg_type.value}")
    
    def _handle_capability_query(self, msg: BaseMessage):
        """Respond to capability query from coordinator."""
        query = msg.payload.get('query', '').lower()
        query_id = msg.message_id
        
        # Check if we have matching capability
        can_handle = False
        confidence = 0.0
        details = {}
        
        for cap_name, cap in self.capabilities.items():
            if query in cap_name.lower() or cap_name.lower() in query:
                can_handle = True
                confidence = cap.confidence
                details = cap.metadata or {}
                break
        
        # Also check if load allows
        if can_handle and self.current_load >= 1.0:
            can_handle = False
            confidence = 0.0
            details['reason'] = "Agent at max load"
        
        response = capability_response(
            agent_id=self.agent_id,
            query_id=query_id,
            can_handle=can_handle,
            confidence=confidence,
            details=details
        )
        self.queue.send(response)
    
    def _handle_task_assign(self, msg: BaseMessage):
        """Execute assigned task."""
        task_id = msg.payload.get('task_id')
        task_type = msg.payload.get('task_type')
        task_data = msg.payload.get('task_data', {})
        
        print(f"[{self.agent_id}] Task assigned: {task_type} ({task_id})")
        
        # Check if we have handler
        handler = self.handlers.get(task_type)
        if not handler:
            # Send failure
            fail_msg = task_failed(
                agent_id=self.agent_id,
                task_id=task_id,
                reason=f"No handler for task type: {task_type}"
            )
            self.queue.send(fail_msg)
            return
        
        # Execute in thread (non-blocking)
        thread = threading.Thread(
            target=self._execute_task,
            args=(task_id, task_type, task_data, handler)
        )
        thread.start()
        self.current_tasks[task_id] = thread
    
    def _execute_task(self, task_id: str, task_type: str, task_data: Dict, handler: Callable):
        """Execute task and report result."""
        try:
            # Report start
            self.current_load += 0.3
            progress_msg = task_progress(
                agent_id=self.agent_id,
                task_id=task_id,
                progress=0.0,
                status="started"
            )
            self.queue.send(progress_msg)
            
            # Execute
            result = handler(task_data)
            
            # Report completion
            complete_msg = task_complete(
                agent_id=self.agent_id,
                task_id=task_id,
                result=result
            )
            self.queue.send(complete_msg)
            
        except Exception as e:
            # Report failure
            fail_msg = task_failed(
                agent_id=self.agent_id,
                task_id=task_id,
                reason=str(e),
                error_details={"exception": type(e).__name__}
            )
            self.queue.send(fail_msg)
        
        finally:
            self.current_load = max(0, self.current_load - 0.3)
            self.current_tasks.pop(task_id, None)
    
    def _handle_tool_install(self, msg: BaseMessage):
        """Install a new tool/capability."""
        tool_name = msg.payload.get('tool_name')
        install_cmd = msg.payload.get('install_cmd')
        
        print(f"[{self.agent_id}] Installing tool: {tool_name}")
        
        try:
            if install_cmd:
                result = subprocess.run(
                    install_cmd,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                if result.returncode != 0:
                    raise Exception(result.stderr)
            
            # Add capability
            self.capabilities[tool_name] = AgentCapability(
                name=tool_name,
                confidence=0.7,
                metadata={"installed": datetime.utcnow().isoformat()}
            )
            
            # Report success
            response = create_message(
                MessageType.TOOL_INSTALLED,
                sender_id=self.agent_id,
                recipient_id="coordinator",
                payload={"tool_name": tool_name, "success": True}
            )
            self.queue.send(response)
            print(f"[{self.agent_id}] Tool installed: {tool_name}")
            
        except Exception as e:
            response = create_message(
                MessageType.TOOL_FAILED,
                sender_id=self.agent_id,
                recipient_id="coordinator",
                payload={"tool_name": tool_name, "error": str(e)}
            )
            self.queue.send(response)
    
    def _handle_shutdown_msg(self, msg: BaseMessage):
        """Graceful shutdown request."""
        print(f"[{self.agent_id}] Shutdown requested")
        self.running = False
    
    # === Built-in Task Handlers ===
    
    def _handle_ping(self, data: Dict) -> Dict:
        """Simple ping handler."""
        return {"pong": True, "timestamp": datetime.utcnow().isoformat()}
    
    def _handle_echo(self, data: Dict) -> Dict:
        """Echo handler for testing."""
        return {"echo": data}
    
    def _handle_shell(self, data: Dict) -> Dict:
        """Execute shell command (limited)."""
        cmd = data.get('command')
        if not cmd:
            raise ValueError("No command provided")
        
        # Safety: block dangerous commands
        dangerous = ['rm -rf', 'mkfs', 'dd if=', '>:']
        if any(d in cmd for d in dangerous):
            raise ValueError(f"Blocked dangerous command")
        
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        return {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode
        }
    
    def _handle_python(self, data: Dict) -> Dict:
        """Execute Python code (sandboxed)."""
        code = data.get('code')
        if not code:
            raise ValueError("No code provided")
        
        # Very basic sandbox - restrict builtins
        safe_builtins = {
            'print': print,
            'len': len,
            'range': range,
            'str': str,
            'int': int,
            'float': float,
            'list': list,
            'dict': dict,
            'True': True,
            'False': False,
            'None': None,
        }
        
        local_vars = {}
        try:
            exec(code, {"__builtins__": safe_builtins}, local_vars)
            return {"result": local_vars, "success": True}
        except Exception as e:
            return {"error": str(e), "success": False}
    
    def _shutdown(self, signum, frame):
        """Signal handler for shutdown."""
        print(f"\n[{self.agent_id}] Shutting down...")
        self.running = False


def main():
    parser = argparse.ArgumentParser(description='Nano Bot - Minimal Agent')
    parser.add_argument('--id', required=True, help='Agent unique ID')
    parser.add_argument('--type', default='worker', help='Agent type')
    parser.add_argument('--coordinator', required=True, help='Cluster directory path')
    parser.add_argument('--capabilities', default='', help='Comma-separated capabilities')
    
    args = parser.parse_args()
    
    # Parse capabilities
    capabilities = []
    if args.capabilities:
        for cap in args.capabilities.split(','):
            capabilities.append({
                'name': cap.strip(),
                'confidence': 0.8
            })
    
    # Default capabilities based on type
    if not capabilities:
        type_caps = {
            'worker': ['shell', 'python', 'ping'],
            'bookkeeper': ['spreadsheet', 'financial', 'calc'],
            'researcher': ['search', 'python', 'analysis'],
            'legal': ['document', 'search', 'analysis'],
        }
        for cap in type_caps.get(args.type, ['ping']):
            capabilities.append({'name': cap, 'confidence': 0.8})
    
    config = BotConfig(
        agent_id=args.id,
        agent_type=args.type,
        cluster_dir=args.coordinator,
        capabilities=capabilities
    )
    
    bot = NanoBot(config)
    bot.start()


if __name__ == '__main__':
    main()
