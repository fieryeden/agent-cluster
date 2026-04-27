"""
Network Module

TCP/WebSocket transport for agent communication.
"""

from network.transport import (
    NetworkServer, NetworkClient, ConnectionInfo, ConnectionState,
    MessageFramer,
)
from network.websocket import (
    WebSocketServer, WebSocketClient, WSConnection, WSState,
)
from network.coordinator import NetworkCoordinator, create_network_coordinator

__all__ = [
    # TCP Transport
    "NetworkServer",
    "NetworkClient",
    "ConnectionInfo",
    "ConnectionState",
    "MessageFramer",
    # WebSocket
    "WebSocketServer",
    "WebSocketClient",
    "WSConnection",
    "WSState",
    # Coordinator
    "NetworkCoordinator",
    "create_network_coordinator",
]
