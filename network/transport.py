#!/usr/bin/env python3
"""
Network Transport Layer

Replaces file-based messaging with TCP/WebSocket connections.
Enables agents to connect from anywhere on the network.

Components:
- TCP Server: Coordinator listens for agent connections
- TCP Client: Agents connect to coordinator
- Message Protocol: Framing, serialization, heartbeat
"""

import socket
import threading
import json
import time
import select
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import sys
import struct

sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol.messages import BaseMessage, MessageType, create_message


class ConnectionState(Enum):
    """State of a network connection."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATING = "authenticating"
    READY = "ready"
    CLOSING = "closing"


@dataclass
class ConnectionInfo:
    """Information about a network connection."""
    
    agent_id: str
    socket: socket.socket
    address: tuple
    state: ConnectionState = ConnectionState.CONNECTED
    connected_at: datetime = field(default_factory=datetime.now)
    last_heartbeat: datetime = field(default_factory=datetime.now)
    bytes_received: int = 0
    bytes_sent: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "address": f"{self.address[0]}:{self.address[1]}",
            "state": self.state.value,
            "connected_at": self.connected_at.isoformat(),
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "bytes_received": self.bytes_received,
            "bytes_sent": self.bytes_sent,
        }


class MessageFramer:
    """
    Frames messages for TCP transmission.
    
    Frame format:
    - 4 bytes: Length (big-endian uint32)
    - N bytes: JSON message
    
    This ensures clean message boundaries over TCP streams.
    """
    
    HEADER_SIZE = 4
    MAX_MESSAGE_SIZE = 10 * 1024 * 1024  # 10MB max
    
    @staticmethod
    def frame_message(message: BaseMessage) -> bytes:
        """
        Frame a message for transmission.
        
        Args:
            message: Message to frame
        
        Returns:
            Framed message as bytes
        """
        json_str = json.dumps(message.to_dict())
        json_bytes = json_str.encode('utf-8')
        
        length = len(json_bytes)
        if length > MessageFramer.MAX_MESSAGE_SIZE:
            raise ValueError(f"Message too large: {length} bytes")
        
        # Length prefix (4 bytes, big-endian)
        header = struct.pack('>I', length)
        
        return header + json_bytes
    
    @staticmethod
    def unframe_message(data: bytes) -> Tuple[Optional[BaseMessage], bytes]:
        """
        Unframe a message from received data.
        
        Args:
            data: Received data buffer
        
        Returns:
            Tuple of (message, remaining_data)
        """
        if len(data) < MessageFramer.HEADER_SIZE:
            return None, data
        
        # Read length
        length = struct.unpack('>I', data[:MessageFramer.HEADER_SIZE])[0]
        
        if length > MessageFramer.MAX_MESSAGE_SIZE:
            raise ValueError(f"Message too large: {length} bytes")
        
        # Check if we have the full message
        total_size = MessageFramer.HEADER_SIZE + length
        if len(data) < total_size:
            return None, data
        
        # Extract message
        json_bytes = data[MessageFramer.HEADER_SIZE:total_size]
        remaining = data[total_size:]
        
        try:
            json_str = json_bytes.decode('utf-8')
            msg_dict = json.loads(json_str)
            message = BaseMessage.from_dict(msg_dict)
            return message, remaining
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid message: {e}")


class NetworkServer:
    """
    TCP server for coordinator.
    
    Listens for agent connections and manages message routing.
    
    Features:
    - Async connection handling
    - Message framing
    - Heartbeat monitoring
    - Connection management
    """
    
    DEFAULT_PORT = 7890
    HEARTBEAT_INTERVAL = 30  # seconds
    HEARTBEAT_TIMEOUT = 90   # seconds (3 missed heartbeats)
    
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = DEFAULT_PORT,
        message_handler: Optional[Callable] = None,
    ):
        """
        Initialize network server.
        
        Args:
            host: Host to bind to
            port: Port to listen on
            message_handler: Callback for incoming messages
        """
        self.host = host
        self.port = port
        self.message_handler = message_handler
        
        self.server_socket: Optional[socket.socket] = None
        self.connections: Dict[str, ConnectionInfo] = {}
        self.running = False
        
        self._lock = threading.Lock()
        self._threads: List[threading.Thread] = []
    
    def start(self):
        """Start the server."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        
        self.running = True
        
        # Accept thread
        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        accept_thread.start()
        self._threads.append(accept_thread)
        
        # Heartbeat monitor thread
        heartbeat_thread = threading.Thread(target=self._heartbeat_monitor, daemon=True)
        heartbeat_thread.start()
        self._threads.append(heartbeat_thread)
    
    def stop(self):
        """Stop the server."""
        self.running = False
        
        # Close all connections
        with self._lock:
            for conn in self.connections.values():
                try:
                    conn.socket.close()
                except:
                    pass
            self.connections.clear()
        
        # Close server socket
        if self.server_socket:
            self.server_socket.close()
    
    def _accept_loop(self):
        """Accept incoming connections."""
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                
                # Create temporary connection info
                conn_id = f"conn-{address[0]}:{address[1]}"
                conn = ConnectionInfo(
                    agent_id=conn_id,
                    socket=client_socket,
                    address=address,
                    state=ConnectionState.CONNECTED,
                )
                
                with self._lock:
                    self.connections[conn_id] = conn
                
                # Start handler thread
                handler_thread = threading.Thread(
                    target=self._handle_connection,
                    args=(conn_id,),
                    daemon=True,
                )
                handler_thread.start()
                
            except Exception as e:
                if self.running:
                    print(f"Accept error: {e}")
    
    def _handle_connection(self, conn_id: str):
        """Handle a single connection."""
        with self._lock:
            conn = self.connections.get(conn_id)
        
        if not conn:
            return
        
        buffer = b''
        
        try:
            while self.running and conn.state in (ConnectionState.CONNECTED, ConnectionState.READY):
                # Receive data
                data = conn.socket.recv(65536)
                if not data:
                    break
                
                conn.bytes_received += len(data)
                buffer += data
                
                # Process messages
                while True:
                    try:
                        message, buffer = MessageFramer.unframe_message(buffer)
                        if message is None:
                            break
                        
                        # Update agent ID if registration
                        if message.msg_type == MessageType.REGISTER:
                            agent_id = message.sender_id
                            conn.agent_id = agent_id
                            conn.state = ConnectionState.READY
                            
                            # Re-index connection
                            with self._lock:
                                if conn_id in self.connections:
                                    del self.connections[conn_id]
                                    self.connections[agent_id] = conn
                        
                        # Handle message
                        if self.message_handler:
                            self.message_handler(message, conn)
                        
                    except ValueError as e:
                        print(f"Message error: {e}")
                        break
                    
        except Exception as e:
            print(f"Connection error: {e}")
        
        finally:
            # Remove connection
            with self._lock:
                if conn.agent_id in self.connections:
                    del self.connections[conn.agent_id]
                elif conn_id in self.connections:
                    del self.connections[conn_id]
            
            try:
                conn.socket.close()
            except:
                pass
    
    def _heartbeat_monitor(self):
        """Monitor connections for heartbeat timeout."""
        while self.running:
            time.sleep(self.HEARTBEAT_INTERVAL)
            
            now = datetime.now()
            timed_out = []
            
            with self._lock:
                for agent_id, conn in self.connections.items():
                    elapsed = (now - conn.last_heartbeat).total_seconds()
                    if elapsed > self.HEARTBEAT_TIMEOUT:
                        timed_out.append(agent_id)
            
            # Close timed out connections
            for agent_id in timed_out:
                self.close_connection(agent_id)
    
    def send_message(self, agent_id: str, message: BaseMessage) -> bool:
        """
        Send a message to an agent.
        
        Args:
            agent_id: Target agent ID
            message: Message to send
        
        Returns:
            True if sent successfully
        """
        with self._lock:
            conn = self.connections.get(agent_id)
        
        if not conn:
            return False
        
        try:
            framed = MessageFramer.frame_message(message)
            conn.socket.sendall(framed)
            conn.bytes_sent += len(framed)
            return True
        except Exception as e:
            print(f"Send error: {e}")
            return False
    
    def broadcast_message(self, message: BaseMessage, exclude: List[str] = None) -> int:
        """
        Broadcast a message to all connected agents.
        
        Args:
            message: Message to broadcast
            exclude: Agent IDs to exclude
        
        Returns:
            Number of successful sends
        """
        exclude = exclude or []
        success = 0
        
        with self._lock:
            agent_ids = list(self.connections.keys())
        
        for agent_id in agent_ids:
            if agent_id not in exclude:
                if self.send_message(agent_id, message):
                    success += 1
        
        return success
    
    def close_connection(self, agent_id: str):
        """Close a connection."""
        with self._lock:
            conn = self.connections.get(agent_id)
        
        if conn:
            try:
                conn.socket.close()
            except:
                pass
            
            with self._lock:
                if agent_id in self.connections:
                    del self.connections[agent_id]
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        with self._lock:
            return {
                "total_connections": len(self.connections),
                "connections": {
                    agent_id: conn.to_dict()
                    for agent_id, conn in self.connections.items()
                },
            }


class NetworkClient:
    """
    TCP client for agents.
    
    Connects to coordinator and handles message sending/receiving.
    """
    
    RECONNECT_DELAY = 5  # seconds
    RECONNECT_MAX_DELAY = 60  # seconds
    
    def __init__(
        self,
        agent_id: str,
        host: str = "localhost",
        port: int = NetworkServer.DEFAULT_PORT,
        message_handler: Optional[Callable] = None,
    ):
        """
        Initialize network client.
        
        Args:
            agent_id: This agent's ID
            host: Coordinator host
            port: Coordinator port
            message_handler: Callback for incoming messages
        """
        self.agent_id = agent_id
        self.host = host
        self.port = port
        self.message_handler = message_handler
        
        self.socket: Optional[socket.socket] = None
        self.state = ConnectionState.DISCONNECTED
        self.running = False
        
        self._lock = threading.Lock()
        self._receive_thread: Optional[threading.Thread] = None
    
    def connect(self) -> bool:
        """
        Connect to the coordinator.
        
        Returns:
            True if connected successfully
        """
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.state = ConnectionState.CONNECTED
            self.running = True
            
            # Send registration
            reg_msg = create_message(
                msg_type=MessageType.REGISTER,
                sender_id=self.agent_id,
                recipient_id="coordinator",
                payload={"agent_id": self.agent_id},
            )
            self._send_raw(reg_msg)
            
            # Start receive thread
            self._receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
            self._receive_thread.start()
            
            self.state = ConnectionState.READY
            return True
            
        except Exception as e:
            print(f"Connect error: {e}")
            self.state = ConnectionState.DISCONNECTED
            return False
    
    def disconnect(self):
        """Disconnect from coordinator."""
        self.running = False
        self.state = ConnectionState.DISCONNECTED
        
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
    
    def _receive_loop(self):
        """Receive messages from coordinator."""
        buffer = b''
        
        while self.running:
            try:
                data = self.socket.recv(65536)
                if not data:
                    break
                
                buffer += data
                
                # Process messages
                while True:
                    try:
                        message, buffer = MessageFramer.unframe_message(buffer)
                        if message is None:
                            break
                        
                        # Update heartbeat
                        if message.msg_type == MessageType.HEARTBEAT:
                            # Send heartbeat response
                            self.send_heartbeat()
                        elif self.message_handler:
                            self.message_handler(message)
                        
                    except ValueError:
                        break
                        
            except Exception as e:
                if self.running:
                    print(f"Receive error: {e}")
                break
        
        self.state = ConnectionState.DISCONNECTED
    
    def _send_raw(self, message: BaseMessage) -> bool:
        """Send a message without locking."""
        try:
            framed = MessageFramer.frame_message(message)
            self.socket.sendall(framed)
            return True
        except Exception as e:
            print(f"Send error: {e}")
            return False
    
    def send_message(self, message: BaseMessage) -> bool:
        """
        Send a message to coordinator.
        
        Args:
            message: Message to send
        
        Returns:
            True if sent successfully
        """
        with self._lock:
            if self.state != ConnectionState.READY:
                return False
            return self._send_raw(message)
    
    def send_heartbeat(self) -> bool:
        """Send a heartbeat message."""
        msg = create_message(
            msg_type=MessageType.HEARTBEAT,
            sender_id=self.agent_id,
            recipient_id="coordinator",
            payload={"timestamp": datetime.now().isoformat()},
        )
        return self.send_message(msg)
    
    def connect_with_retry(self, max_retries: int = -1) -> bool:
        """
        Connect with automatic retry.
        
        Args:
            max_retries: Maximum retries (-1 for infinite)
        
        Returns:
            True if connected
        """
        delay = self.RECONNECT_DELAY
        retries = 0
        
        while max_retries < 0 or retries < max_retries:
            if self.connect():
                return True
            
            print(f"Connection failed, retrying in {delay}s...")
            time.sleep(delay)
            
            # Exponential backoff
            delay = min(delay * 2, self.RECONNECT_MAX_DELAY)
            retries += 1
        
        return False


# Convenience functions

def create_server(port: int = NetworkServer.DEFAULT_PORT, handler: Callable = None) -> NetworkServer:
    """Create a network server."""
    return NetworkServer(port=port, message_handler=handler)


def create_client(agent_id: str, host: str = "localhost", port: int = NetworkServer.DEFAULT_PORT, handler: Callable = None) -> NetworkClient:
    """Create a network client."""
    return NetworkClient(agent_id=agent_id, host=host, port=port, message_handler=handler)
