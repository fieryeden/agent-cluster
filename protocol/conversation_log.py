#!/usr/bin/env python3
"""
Agent Conversation Log

Persistent, queryable log of all agent-to-agent conversations.
SQLite-backed with JSON export support.

Every message between agents is recorded with:
- sender, recipient, conversation thread ID
- message content, metadata
- timestamp, delivery status
"""

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict, field


@dataclass
class ConversationEntry:
    """A single message in the conversation log."""
    entry_id: Optional[int] = None
    conversation_id: str = ""
    message_id: str = ""
    sender_id: str = ""
    recipient_id: str = ""
    msg_type: str = ""
    content: str = ""
    metadata: str = "{}"  # JSON
    timestamp: str = ""
    delivery_status: str = "pending"  # pending | delivered | failed

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if d.get('metadata') and isinstance(d['metadata'], str):
            try:
                d['metadata'] = json.loads(d['metadata'])
            except json.JSONDecodeError:
                pass
        return d


class ConversationLog:
    """
    Persistent conversation log for agent-to-agent messaging.

    Features:
    - SQLite storage (fast, concurrent-safe)
    - Conversation threading (messages grouped by conversation_id)
    - Full-text search on content
    - JSON export for any conversation or the entire log
    - Statistics (message counts, active conversations, agent activity)
    - Retention policy (auto-prune old conversations)
    """

    def __init__(self, db_path: str = None):
        """
        Initialize the conversation log.

        Args:
            db_path: Path to SQLite database file. Defaults to
                     /tmp/agent_cluster/conversations.db
        """
        if db_path is None:
            db_path = "/tmp/agent_cluster/conversations.db"

        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._conn = None
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")

        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS conversation_log (
                entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                message_id TEXT NOT NULL UNIQUE,
                sender_id TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                msg_type TEXT NOT NULL,
                content TEXT NOT NULL DEFAULT '',
                metadata TEXT NOT NULL DEFAULT '{}',
                timestamp TEXT NOT NULL,
                delivery_status TEXT NOT NULL DEFAULT 'pending'
            );

            CREATE INDEX IF NOT EXISTS idx_conv_id
                ON conversation_log(conversation_id);
            CREATE INDEX IF NOT EXISTS idx_sender
                ON conversation_log(sender_id);
            CREATE INDEX IF NOT EXISTS idx_recipient
                ON conversation_log(recipient_id);
            CREATE INDEX IF NOT EXISTS idx_timestamp
                ON conversation_log(timestamp);
            CREATE INDEX IF NOT EXISTS idx_msg_type
                ON conversation_log(msg_type);

            CREATE TABLE IF NOT EXISTS conversation_meta (
                conversation_id TEXT PRIMARY KEY,
                topic TEXT NOT NULL DEFAULT '',
                participants TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active'
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS conversation_search
                USING FTS5(content, content=conversation_log, content_rowid=entry_id);

            CREATE TRIGGER IF NOT EXISTS conversation_log_ai AFTER INSERT ON conversation_log
                BEGIN
                    INSERT INTO conversation_search(rowid, content)
                    VALUES (new.entry_id, new.content);
                END;

            CREATE TRIGGER IF NOT EXISTS conversation_log_ad AFTER DELETE ON conversation_log
                BEGIN
                    INSERT INTO conversation_search(conversation_search, rowid, content)
                    VALUES ('delete', old.entry_id, old.content);
                END;
        """)
        self._conn.commit()

    def log_message(
        self,
        conversation_id: str,
        message_id: str,
        sender_id: str,
        recipient_id: str,
        msg_type: str,
        content: str,
        metadata: Dict[str, Any] = None,
        delivery_status: str = "pending",
    ) -> int:
        """
        Log a message to the conversation log.

        Args:
            conversation_id: Thread/conversation ID
            message_id: Unique message ID
            sender_id: Sending agent ID
            recipient_id: Receiving agent ID
            msg_type: Message type string
            content: Message content
            metadata: Optional metadata dict
            delivery_status: pending | delivered | failed

        Returns:
            entry_id of the logged message
        """
        now = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        meta_json = json.dumps(metadata or {})

        with self._lock:
            try:
                cursor = self._conn.execute(
                    """INSERT OR IGNORE INTO conversation_log
                       (conversation_id, message_id, sender_id, recipient_id,
                        msg_type, content, metadata, timestamp, delivery_status)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (conversation_id, message_id, sender_id, recipient_id,
                     msg_type, content, meta_json, now, delivery_status)
                )
                self._conn.commit()

                entry_id = cursor.lastrowid

                # Update conversation metadata
                self._update_conversation_meta(
                    conversation_id, sender_id, recipient_id, now
                )

                return entry_id
            except sqlite3.IntegrityError:
                # Duplicate message_id — skip
                return 0

    def _update_conversation_meta(
        self,
        conversation_id: str,
        sender_id: str,
        recipient_id: str,
        timestamp: str,
    ):
        """Update or create conversation metadata."""
        # Check if conversation exists
        row = self._conn.execute(
            "SELECT participants FROM conversation_meta WHERE conversation_id = ?",
            (conversation_id,)
        ).fetchone()

        if row:
            participants = json.loads(row["participants"])
            for aid in [sender_id, recipient_id]:
                if aid not in participants:
                    participants.append(aid)
            self._conn.execute(
                """UPDATE conversation_meta
                   SET participants = ?, updated_at = ?, message_count = message_count + 1
                   WHERE conversation_id = ?""",
                (json.dumps(participants), timestamp, conversation_id)
            )
        else:
            participants = list(set([sender_id, recipient_id]))
            self._conn.execute(
                """INSERT INTO conversation_meta
                   (conversation_id, participants, created_at, updated_at, message_count, status)
                   VALUES (?, ?, ?, ?, 1, 'active')""",
                (conversation_id, json.dumps(participants), timestamp, timestamp)
            )
        self._conn.commit()

    def update_delivery_status(self, message_id: str, status: str):
        """Update the delivery status of a message."""
        with self._lock:
            self._conn.execute(
                "UPDATE conversation_log SET delivery_status = ? WHERE message_id = ?",
                (status, message_id)
            )
            self._conn.commit()

    def get_conversation(
        self,
        conversation_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ConversationEntry]:
        """
        Get all messages in a conversation.

        Args:
            conversation_id: Conversation thread ID
            limit: Max messages to return
            offset: Offset for pagination

        Returns:
            List of ConversationEntry objects
        """
        rows = self._conn.execute(
            """SELECT * FROM conversation_log
               WHERE conversation_id = ?
               ORDER BY timestamp ASC
               LIMIT ? OFFSET ?""",
            (conversation_id, limit, offset)
        ).fetchall()

        return [self._row_to_entry(r) for r in rows]

    def get_agent_conversations(
        self,
        agent_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get all conversations involving an agent.

        Returns list of conversation summaries with:
        - conversation_id, participants, last message, message count
        """
        rows = self._conn.execute(
            """SELECT cm.* FROM conversation_meta cm
               WHERE cm.participants LIKE ?
               ORDER BY cm.updated_at DESC
               LIMIT ?""",
            (f'%{agent_id}%', limit)
        ).fetchall()

        results = []
        for row in rows:
            participants = json.loads(row["participants"])
            if agent_id in participants:
                # Get last message
                last = self._conn.execute(
                    """SELECT * FROM conversation_log
                       WHERE conversation_id = ?
                       ORDER BY timestamp DESC LIMIT 1""",
                    (row["conversation_id"],)
                ).fetchone()

                results.append({
                    "conversation_id": row["conversation_id"],
                    "topic": row["topic"],
                    "participants": participants,
                    "message_count": row["message_count"],
                    "status": row["status"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "last_message": self._row_to_entry(last).to_dict() if last else None,
                })

        return results

    def search_conversations(
        self,
        query: str,
        limit: int = 20,
    ) -> List[ConversationEntry]:
        """
        Full-text search across all conversation content.

        Args:
            query: Search query string
            limit: Max results

        Returns:
            List of matching ConversationEntry objects
        """
        rows = self._conn.execute(
            """SELECT cl.* FROM conversation_log cl
               JOIN conversation_search cs ON cl.entry_id = cs.rowid
               WHERE conversation_search MATCH ?
               ORDER BY cl.timestamp DESC
               LIMIT ?""",
            (query, limit)
        ).fetchall()

        return [self._row_to_entry(r) for r in rows]

    def get_all_conversations(
        self,
        status: str = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get all conversations with metadata.

        Args:
            status: Filter by status (active, closed, archived)
            limit: Max results

        Returns:
            List of conversation summary dicts
        """
        if status:
            rows = self._conn.execute(
                """SELECT * FROM conversation_meta
                   WHERE status = ?
                   ORDER BY updated_at DESC LIMIT ?""",
                (status, limit)
            ).fetchall()
        else:
            rows = self._conn.execute(
                """SELECT * FROM conversation_meta
                   ORDER BY updated_at DESC LIMIT ?""",
                (limit,)
            ).fetchall()

        results = []
        for row in rows:
            results.append({
                "conversation_id": row["conversation_id"],
                "topic": row["topic"],
                "participants": json.loads(row["participants"]),
                "message_count": row["message_count"],
                "status": row["status"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            })
        return results

    def get_messages_between(
        self,
        agent_a: str,
        agent_b: str,
        limit: int = 100,
    ) -> List[ConversationEntry]:
        """Get all messages exchanged between two agents."""
        rows = self._conn.execute(
            """SELECT * FROM conversation_log
               WHERE (sender_id = ? AND recipient_id = ?)
                  OR (sender_id = ? AND recipient_id = ?)
               ORDER BY timestamp ASC
               LIMIT ?""",
            (agent_a, agent_b, agent_b, agent_a, limit)
        ).fetchall()

        return [self._row_to_entry(r) for r in rows]

    def set_conversation_topic(self, conversation_id: str, topic: str):
        """Set or update the topic of a conversation."""
        with self._lock:
            self._conn.execute(
                "UPDATE conversation_meta SET topic = ? WHERE conversation_id = ?",
                (topic, conversation_id)
            )
            self._conn.commit()

    def close_conversation(self, conversation_id: str):
        """Mark a conversation as closed."""
        with self._lock:
            self._conn.execute(
                "UPDATE conversation_meta SET status = 'closed' WHERE conversation_id = ?",
                (conversation_id,)
            )
            self._conn.commit()

    def get_stats(self) -> Dict[str, Any]:
        """Get conversation log statistics."""
        total_msgs = self._conn.execute(
            "SELECT COUNT(*) as c FROM conversation_log"
        ).fetchone()["c"]

        total_convs = self._conn.execute(
            "SELECT COUNT(*) as c FROM conversation_meta"
        ).fetchone()["c"]

        active_convs = self._conn.execute(
            "SELECT COUNT(*) as c FROM conversation_meta WHERE status = 'active'"
        ).fetchone()["c"]

        # Most active agents
        agent_rows = self._conn.execute(
            """SELECT sender_id, COUNT(*) as msg_count
               FROM conversation_log
               GROUP BY sender_id
               ORDER BY msg_count DESC
               LIMIT 10"""
        ).fetchall()

        # Messages by type
        type_rows = self._conn.execute(
            """SELECT msg_type, COUNT(*) as msg_count
               FROM conversation_log
               GROUP BY msg_type
               ORDER BY msg_count DESC"""
        ).fetchall()

        # Delivery status breakdown
        status_rows = self._conn.execute(
            """SELECT delivery_status, COUNT(*) as msg_count
               FROM conversation_log
               GROUP BY delivery_status"""
        ).fetchall()

        return {
            "total_messages": total_msgs,
            "total_conversations": total_convs,
            "active_conversations": active_convs,
            "most_active_senders": [
                {"agent_id": r["sender_id"], "messages": r["msg_count"]}
                for r in agent_rows
            ],
            "messages_by_type": {
                r["msg_type"]: r["msg_count"] for r in type_rows
            },
            "delivery_status": {
                r["delivery_status"]: r["msg_count"] for r in status_rows
            },
        }

    def export_conversation(
        self,
        conversation_id: str,
        format: str = "json",
    ) -> str:
        """
        Export a single conversation as JSON or markdown.

        Args:
            conversation_id: Conversation to export
            format: 'json' or 'markdown'

        Returns:
            Formatted string
        """
        entries = self.get_conversation(conversation_id, limit=10000)
        meta_row = self._conn.execute(
            "SELECT * FROM conversation_meta WHERE conversation_id = ?",
            (conversation_id,)
        ).fetchone()

        meta = dict(meta_row) if meta_row else {}
        if meta.get("participants"):
            meta["participants"] = json.loads(meta["participants"])

        if format == "markdown":
            return self._export_markdown(entries, meta)
        else:
            return json.dumps({
                "conversation_id": conversation_id,
                "metadata": meta,
                "messages": [e.to_dict() for e in entries],
            }, indent=2)

    def export_all(self, format: str = "json") -> str:
        """Export the entire conversation log."""
        convs = self.get_all_conversations(limit=10000)
        result = {}
        for conv in convs:
            cid = conv["conversation_id"]
            entries = self.get_conversation(cid, limit=10000)
            result[cid] = {
                "metadata": conv,
                "messages": [e.to_dict() for e in entries],
            }

        if format == "markdown":
            parts = ["# Complete Conversation Log\n"]
            for cid, data in result.items():
                parts.append(self._export_markdown(
                    [ConversationEntry(**m) for m in data["messages"]],
                    data["metadata"]
                ))
                parts.append("\n---\n")
            return "\n".join(parts)
        else:
            return json.dumps(result, indent=2)

    def prune(self, older_than_days: int = 30):
        """
        Prune conversations older than N days.

        Args:
            older_than_days: Delete conversations with no activity in this many days
        """
        cutoff = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        # SQLite date math
        with self._lock:
            self._conn.execute(
                """DELETE FROM conversation_log
                   WHERE conversation_id IN (
                       SELECT conversation_id FROM conversation_meta
                       WHERE updated_at < datetime('now', ?)
                   )""",
                (f"-{older_than_days} days",)
            )
            self._conn.execute(
                """DELETE FROM conversation_meta
                   WHERE updated_at < datetime('now', ?)""",
                (f"-{older_than_days} days",)
            )
            self._conn.commit()

    def _export_markdown(
        self,
        entries: List[ConversationEntry],
        meta: Dict[str, Any],
    ) -> str:
        """Export conversation as readable markdown."""
        lines = [
            f"# Conversation: {meta.get('conversation_id', 'unknown')}",
            f"**Topic:** {meta.get('topic', '(no topic)')}",
            f"**Participants:** {', '.join(meta.get('participants', []))}",
            f"**Messages:** {meta.get('message_count', 0)}",
            f"**Status:** {meta.get('status', 'unknown')}",
            "",
            "---",
            "",
        ]

        for entry in entries:
            ts = entry.timestamp.split("T")[1].split(".")[0] if "T" in entry.timestamp else entry.timestamp
            lines.append(f"**[{ts}] {entry.sender_id} → {entry.recipient_id}** (`{entry.msg_type}`)")
            lines.append(f"{entry.content}")
            if entry.delivery_status != "delivered":
                lines.append(f"*Status: {entry.delivery_status}*")
            lines.append("")

        return "\n".join(lines)

    def _row_to_entry(self, row) -> ConversationEntry:
        """Convert a database row to a ConversationEntry."""
        if row is None:
            return ConversationEntry()
        return ConversationEntry(
            entry_id=row["entry_id"],
            conversation_id=row["conversation_id"],
            message_id=row["message_id"],
            sender_id=row["sender_id"],
            recipient_id=row["recipient_id"],
            msg_type=row["msg_type"],
            content=row["content"],
            metadata=row["metadata"],
            timestamp=row["timestamp"],
            delivery_status=row["delivery_status"],
        )

    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()


if __name__ == "__main__":
    # Quick self-test
    print("=== Conversation Log Self-Test ===\n")

    log = ConversationLog("/tmp/test_conversations.db")

    # Log some test messages
    log.log_message(
        conversation_id="conv-test-001",
        message_id="msg-001",
        sender_id="agent-alpha",
        recipient_id="agent-beta",
        msg_type="peer_request",
        content="Hey beta, can you verify the compliance check output?",
    )
    log.log_message(
        conversation_id="conv-test-001",
        message_id="msg-002",
        sender_id="agent-beta",
        recipient_id="agent-alpha",
        msg_type="peer_response",
        content="Sure, I'll run the verification now. Give me 30 seconds.",
        delivery_status="delivered",
    )
    log.log_message(
        conversation_id="conv-test-001",
        message_id="msg-003",
        sender_id="agent-beta",
        recipient_id="agent-alpha",
        msg_type="peer_response",
        content="Verified — 3 findings confirmed, 1 false positive. Sending details.",
        delivery_status="delivered",
    )

    # Get conversation
    conv = log.get_conversation("conv-test-001")
    print(f"Conversation has {len(conv)} messages:")
    for entry in conv:
        print(f"  [{entry.msg_type}] {entry.sender_id} → {entry.recipient_id}: {entry.content[:60]}...")

    # Stats
    stats = log.get_stats()
    print(f"\nStats: {stats['total_messages']} messages, {stats['total_conversations']} conversations")

    # Export as markdown
    md = log.export_conversation("conv-test-001", format="markdown")
    print(f"\nMarkdown export:\n{md[:500]}...")

    # Search
    results = log.search_conversations("compliance")
    print(f"\nSearch 'compliance': {len(results)} results")

    log.close()
    print("\n✓ Conversation log module OK")
