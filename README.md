# Agent Cluster MVP

A decentralized agent coordination framework with capability discovery, auto-learning, peer messaging, OTA updates, and optional OpenClaw integration.

## Features

- **Agent Lifecycle Management** — Register, deregister, heartbeat monitoring
- **Task Queue** — Priority-based task assignment with negotiation
- **Capability Discovery** — Registry, query protocol, dynamic updates
- **Auto-Learning** — Gap detection, research dispatch, tool installation, verification
- **Network Transport** — TCP + WebSocket with message framing
- **Peer Messaging** — Agent-to-agent conversations with full logging (SQLite + FTS5)
- **File Transfer** — Base64-encoded with request/accept handshake
- **Task Delegation** — Agent-initiated with accept/decline/counter-offer
- **Consensus Protocol** — Coordinator-tracked voting with quorum
- **OTA Updates** — Automatic rollout, no user consent required, auto-rollback on failure
- **Security** — Authentication, rate limiting, input validation, audit logging
- **Reliability** — Circuit breaker, retry with backoff, dead letter queue, timeout management
- **Dashboard** — Real-time monitoring with health scoring (0-100)
- **OpenClaw Integration** — Optional bridge to OpenClaw sessions, events, and skills

## Quick Start

```bash
# Install
pip install -e .

# Run standalone coordinator
python -m network.server --port 8080

# Run with dashboard
python -m dashboard.cli mock --port 8080

# Run tests
python -m pytest tests/ -q
```

## OpenClaw Integration

OpenClaw integration is **disabled by default**. Enable it in config:

```yaml
openclaw:
  enabled: true
  coordinator_session: "session:agent-cluster-coordinator"
  sync_conversations: true
  expose_skills: true
  event_bridge: true
```

Or toggle at runtime:

```python
from openclaw_integration import ClusterModeManager, OpenClawConfig

manager = ClusterModeManager(coordinator=my_coord, config=OpenClawConfig())
manager.enable_openclaw()   # → routes through OpenClaw sessions
manager.disable_openclaw()  # → back to pure TCP
```

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  Agent A     │◄───►│ Coordinator  │◄───►│  Agent B     │
│  (TCP/WS)   │     │  (Relay)     │     │  (OpenClaw) │
└─────────────┘     └──────┬───────┘     └─────────────┘
                          │
              ┌───────────┼───────────┐
              │           │           │
        ┌─────▼──┐  ┌────▼────┐  ┌──▼──────┐
        │Capability│  │Conversation│  │OTA      │
        │Registry  │  │Log (SQLite)│  │Manager  │
        └─────────┘  └───────────┘  └─────────┘
```

## Test Status

217 tests passing across 11 phases. No external dependencies — stdlib only.

## License

MIT
