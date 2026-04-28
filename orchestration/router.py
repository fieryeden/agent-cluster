#!/usr/bin/env python3
"""
Message Router

Routes messages between agents and orchestrator.
Supports multiple transport types.
"""

import json
import threading
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import BaseMessage, MessageType, create_message


@dataclass
class RoutingRule:
    """Rule for message routing."""
    source_type: str = "*"      # Bot type filter
    target_type: str = "*"      # Target bot type
    message_type: str = "*"     # Message type filter
    handler: Optional[Callable] = None
    priority: int = 0


class MessageRouter:
    """
    Unified message routing for heterogeneous agents.
    
    Handles:
    - File-based messaging (NanoBot)
    - Network messaging (OpenClaw, remote agents)
    - WebSocket messaging (browser extensions)
    
    Routes messages based on:
    - Recipient ID
    - Message type
    - Custom rules
    """
    
    def __init__(self, base_dir: str = None):
        self.base_dir = base_dir or "/tmp/message_router"
        
        self.routes: Dict[str, Any] = {}  # agent_id -> connection
        self.rules: List[RoutingRule] = []
        self.handlers: Dict[MessageType, Callable] = {}
        
        self._lock = threading.Lock()
        
        # Message queues by transport
        self.file_queue: Dict[str, List[BaseMessage]] = {}
        self.network_queue: Dict[str, List[BaseMessage]] = {}
        self.ws_queue: Dict[str, List[BaseMessage]] = {}
    
    def register_route(
        self,
        agent_id: str,
        connection: Any,
        transport: str = "file",
    ):
        """
        Register an agent route.
        
        Args:
            agent_id: Agent identifier
            connection: Connection object (path, socket, etc.)
            transport: Transport type (file, tcp, websocket)
        """
        with self._lock:
            self.routes[agent_id] = {
                "connection": connection,
                "transport": transport,
            }
    
    def unregister_route(self, agent_id: str):
        """Unregister an agent route."""
        with self._lock:
            if agent_id in self.routes:
                del self.routes[agent_id]
    
    def add_rule(self, rule: RoutingRule):
        """Add a routing rule."""
        with self._lock:
            self.rules.append(rule)
            self.rules.sort(key=lambda r: r.priority, reverse=True)
    
    def set_handler(self, msg_type: MessageType, handler: Callable):
        """Set handler for message type."""
        self.handlers[msg_type] = handler
    
    def route(self, message: BaseMessage) -> bool:
        """
        Route a message to destination.
        
        Args:
            message: Message to route
        
        Returns:
            True if routed successfully
        """
        recipient = message.recipient_id
        
        # Check custom rules first
        for rule in self.rules:
            if self._match_rule(rule, message):
                if rule.handler:
                    rule.handler(message)
                    return True
        
        # Direct route
        if recipient in self.routes:
            route = self.routes[recipient]
            return self._send_via_transport(message, route)
        
        # Broadcast
        if recipient == "broadcast":
            return self._broadcast(message)
        
        # Coordinator messages
        if recipient == "coordinator":
            return self._to_coordinator(message)
        
        # Unknown recipient
        print(f"[Router] Unknown recipient: {recipient}")
        return False
    
    def _match_rule(self, rule: RoutingRule, message: BaseMessage) -> bool:
        """Check if rule matches message."""
        if rule.message_type != "*" and rule.message_type != message.msg_type.value:
            return False
        return True
    
    def _send_via_transport(self, message: BaseMessage, route: dict) -> bool:
        """Send message via appropriate transport."""
        transport = route["transport"]
        conn = route["connection"]
        
        if transport == "file":
            # Write to agent's inbox
            inbox = Path(conn) / "inbox"
            inbox.mkdir(parents=True, exist_ok=True)
            
            filename = f"{datetime.now().isoformat()}_{message.message_id}.json"
            filepath = inbox / filename
            
            with open(filepath, 'w') as f:
                f.write(message.to_json())
            
            return True
        
        elif transport == "tcp":
            # Network connection
            if hasattr(conn, 'send'):
                conn.send(message.to_json())
                return True
            return False
        
        elif transport == "websocket":
            # WebSocket connection
            if hasattr(conn, 'send'):
                conn.send(message.to_json())
                return True
            return False
        
        return False
    
    def _broadcast(self, message: BaseMessage) -> bool:
        """Broadcast to all agents."""
        success = True
        for agent_id, route in self.routes.items():
            if not self._send_via_transport(message, route):
                success = False
        return success
    
    def _to_coordinator(self, message: BaseMessage) -> bool:
        """Route to coordinator."""
        # Coordinator handles its own inbox
        return True
