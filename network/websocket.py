#!/usr/bin/env python3
"""
WebSocket Transport

WebSocket implementation for browser/JavaScript compatibility.
Enables web-based agent UIs and dashboard.

Note: Requires 'websockets' package. Install with: pip install websockets
"""

import json
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import sys

# Optional import - falls back to stub if not available
try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    websockets = None

sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import BaseMessage, MessageType, create_message


class WSState(Enum):
    """WebSocket connection state."""
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATED = "authenticated"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass
class WSConnection:
    """WebSocket connection info."""
    
    websocket: Any  # websockets.WebSocketServerProtocol when available
    agent_id: str = ""
    state: "WSState" = None  # type: ignore
    connected_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        if self.state is None:
            from network.websocket import WSState
            self.state = WSState.CONNECTED
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "state": self.state.value,
            "connected_at": self.connected_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
        }


class WebSocketServer:
    """
    WebSocket server for coordinator.
    
    Enables web-based agent connections.
    
    Features:
    - Async/await architecture
    - JSON message protocol
    - Heartbeat monitoring
    - Broadcasting
    
    Requires: pip install websockets
    """
    
    DEFAULT_PORT = 8765
    HEARTBEAT_INTERVAL = 30
    
    def __init__(
        self,
        port: int = DEFAULT_PORT,
        message_handler: Optional[Callable] = None,
    ):
        """
        Initialize WebSocket server.
        
        Args:
            port: Port to listen on
            message_handler: Callback for incoming messages
        """
        if not WEBSOCKETS_AVAILABLE:
            raise ImportError("websockets package required. Install with: pip install websockets")
        
        self.port = port
        self.message_handler = message_handler
        
        self.connections: Dict[str, WSConnection] = {}
        self._server: Optional[websockets.WebSocketServer] = None
        self._running = False
    
    async def start(self):
        """Start the WebSocket server."""
        self._running = True
        self._server = await websockets.serve(
            self._handle_connection,
            "0.0.0.0",
            self.port,
        )
    
    async def stop(self):
        """Stop the server."""
        self._running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()
    
    async def _handle_connection(self, websocket: Any, path: str):
        """Handle a WebSocket connection."""
        conn_id = f"ws-{id(websocket)}"
        conn = WSConnection(websocket=websocket)
        self.connections[conn_id] = conn
        
        try:
            async for message in websocket:
                conn.last_activity = datetime.now()
                
                try:
                    data = json.loads(message)
                    msg = BaseMessage.from_dict(data)
                    
                    # Handle registration
                    if msg.msg_type == MessageType.REGISTER:
                        agent_id = msg.sender_id
                        conn.agent_id = agent_id
                        conn.state = WSState.AUTHENTICATED
                        
                        # Re-index by agent_id
                        if conn_id in self.connections:
                            del self.connections[conn_id]
                            self.connections[agent_id] = conn
                        
                        # Send ack
                        ack = create_message(
                            msg_type=MessageType.REGISTER,
                            sender_id="coordinator",
                            recipient_id=agent_id,
                            payload={"status": "registered"},
                        )
                        await websocket.send(json.dumps(ack.to_dict()))
                    
                    # Handle heartbeat
                    elif msg.msg_type == MessageType.HEARTBEAT:
                        pong = create_message(
                            msg_type=MessageType.HEARTBEAT,
                            sender_id="coordinator",
                            recipient_id=conn.agent_id,
                            payload={"pong": True},
                        )
                        await websocket.send(json.dumps(pong.to_dict()))
                    
                    # Other messages
                    elif self.message_handler:
                        await self.message_handler(msg, conn)
                        
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({"error": "Invalid JSON"}))
                except Exception as e:
                    await websocket.send(json.dumps({"error": str(e)}))
                    
        except websockets.ConnectionClosed:
            pass
        finally:
            # Clean up
            for key in list(self.connections.keys()):
                if key == conn_id or key == conn.agent_id:
                    del self.connections[key]
    
    async def send_message(self, agent_id: str, message: BaseMessage) -> bool:
        """Send a message to a specific agent."""
        conn = self.connections.get(agent_id)
        if not conn:
            return False
        
        try:
            await conn.websocket.send(json.dumps(message.to_dict()))
            return True
        except Exception:
            return False
    
    async def broadcast(self, message: BaseMessage, exclude: Set[str] = None) -> int:
        """Broadcast to all connected agents."""
        exclude = exclude or set()
        success = 0
        
        for agent_id, conn in list(self.connections.items()):
            if agent_id not in exclude and conn.agent_id:
                if await self.send_message(agent_id, message):
                    success += 1
        
        return success
    
    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        return {
            "port": self.port,
            "connections": len(self.connections),
            "agents": [c.to_dict() for c in self.connections.values() if c.agent_id],
        }


class WebSocketClient:
    """
    WebSocket client for agents.
    
    Usage:
        client = WebSocketClient("agent-001", "ws://localhost:8765")
        await client.connect()
        await client.send(message)
    """
    
    def __init__(
        self,
        agent_id: str,
        uri: str = "ws://localhost:8765",
        message_handler: Optional[Callable] = None,
    ):
        """
        Initialize WebSocket client.
        
        Args:
            agent_id: This agent's ID
            uri: WebSocket URI
            message_handler: Callback for incoming messages
        """
        self.agent_id = agent_id
        self.uri = uri
        self.message_handler = message_handler
        
        self.websocket: Optional[Any] = None  # websockets.WebSocketClientProtocol
        self.state = WSState.CLOSED
        self._receive_task: Optional[asyncio.Task] = None
    
    async def connect(self) -> bool:
        """Connect to the server."""
        try:
            self.websocket = await websockets.connect(self.uri)
            self.state = WSState.CONNECTED
            
            # Send registration
            reg = create_message(
                msg_type=MessageType.REGISTER,
                sender_id=self.agent_id,
                recipient_id="coordinator",
                payload={"agent_id": self.agent_id},
            )
            await self.websocket.send(json.dumps(reg.to_dict()))
            
            # Wait for ack
            ack_data = await self.websocket.recv()
            ack = json.loads(ack_data)
            
            self.state = WSState.AUTHENTICATED
            
            # Start receive loop
            self._receive_task = asyncio.create_task(self._receive_loop())
            
            return True
            
        except Exception as e:
            self.state = WSState.CLOSED
            return False
    
    async def disconnect(self):
        """Disconnect from server."""
        self.state = WSState.CLOSING
        
        if self._receive_task:
            self._receive_task.cancel()
        
        if self.websocket:
            await self.websocket.close()
        
        self.state = WSState.CLOSED
    
    async def _receive_loop(self):
        """Receive messages from server."""
        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    msg = BaseMessage.from_dict(data)
                    
                    if self.message_handler:
                        await self.message_handler(msg)
                        
                except json.JSONDecodeError:
                    pass
                except Exception as e:
                    print(f"Message handling error: {e}")
                    
        except websockets.ConnectionClosed:
            self.state = WSState.CLOSED
        except asyncio.CancelledError:
            pass
    
    async def send(self, message: BaseMessage) -> bool:
        """Send a message."""
        if self.state != WSState.AUTHENTICATED:
            return False
        
        try:
            await self.websocket.send(json.dumps(message.to_dict()))
            return True
        except Exception:
            return False
    
    async def send_heartbeat(self) -> bool:
        """Send a heartbeat."""
        msg = create_message(
            msg_type=MessageType.HEARTBEAT,
            sender_id=self.agent_id,
            recipient_id="coordinator",
            payload={"timestamp": datetime.now().isoformat()},
        )
        return await self.send(msg)


# Sync wrapper for non-async code (requires websockets)

if WEBSOCKETS_AVAILABLE:
    import threading
    
    class SyncWebSocketClient:
        """
        Synchronous wrapper for WebSocket client.
        
        Usage:
            client = SyncWebSocketClient("agent-001")
            client.connect()
            client.send(message)
        """
        
        def __init__(self, agent_id: str, uri: str = "ws://localhost:8765"):
            self.agent_id = agent_id
            self.uri = uri
            self._client = WebSocketClient(agent_id, uri)
            self._loop: Optional[asyncio.AbstractEventLoop] = None
            self._thread: Optional[threading.Thread] = None
        
        def connect(self) -> bool:
            """Connect synchronously."""
            result = [False]
            
            def run():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self._loop = loop
                
                async def do_connect():
                    connected = await self._client.connect()
                    result[0] = connected
                    if connected:
                        # Keep loop running
                        while self._client.state == WSState.AUTHENTICATED:
                            await asyncio.sleep(1)
                
                loop.run_until_complete(do_connect())
            
            self._thread = threading.Thread(target=run, daemon=True)
            self._thread.start()
            self._thread.join(timeout=5)
            
            return result[0]
        
        def send(self, message: BaseMessage) -> bool:
            """Send a message synchronously."""
            if not self._loop:
                return False
            
            future = asyncio.run_coroutine_threadsafe(
                self._client.send(message),
                self._loop,
            )
            return future.result(timeout=5)
else:
    class SyncWebSocketClient:
        """Stub - websockets not available."""
        def __init__(self, *args, **kwargs):
            raise ImportError("websockets package required. Install with: pip install websockets")


# Convenience functions

async def create_ws_server(port: int = WebSocketServer.DEFAULT_PORT, handler: Callable = None) -> WebSocketServer:
    """Create and start a WebSocket server."""
    server = WebSocketServer(port=port, message_handler=handler)
    await server.start()
    return server


def create_ws_client(agent_id: str, uri: str = "ws://localhost:8765") -> WebSocketClient:
    """Create a WebSocket client."""
    return WebSocketClient(agent_id=agent_id, uri=uri)
