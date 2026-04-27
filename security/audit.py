"""
Audit Logging Module

Provides comprehensive audit logging for:
- User actions
- System events
- Security events
- Data access
- Task execution
"""

import os
import json
import time
import socket
import threading
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
from pathlib import Path
from enum import Enum
import hashlib


class AuditLevel(Enum):
    """Audit event severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AuditEventType(Enum):
    """Types of audit events."""
    # Authentication
    AUTH_SUCCESS = "auth_success"
    AUTH_FAILURE = "auth_failure"
    AUTH_LOGOUT = "auth_logout"
    AUTH_TOKEN_REFRESH = "auth_token_refresh"
    
    # Authorization
    PERMIT_GRANTED = "permit_granted"
    PERMIT_DENIED = "permit_denied"
    
    # Task operations
    TASK_CREATED = "task_created"
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_CANCELLED = "task_cancelled"
    
    # Agent operations
    AGENT_REGISTERED = "agent_registered"
    AGENT_DEREGISTERED = "agent_deregistered"
    AGENT_HEALTH_CHECK = "agent_health_check"
    
    # Data operations
    DATA_READ = "data_read"
    DATA_WRITE = "data_write"
    DATA_DELETE = "data_delete"
    
    # System operations
    SYSTEM_START = "system_start"
    SYSTEM_STOP = "system_stop"
    SYSTEM_CONFIG_CHANGE = "system_config_change"
    
    # Security events
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"
    CIRCUIT_BREAKER_TRIPPED = "circuit_breaker_tripped"
    INVALID_INPUT = "invalid_input"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    
    # Handler operations
    HANDLER_EXECUTED = "handler_executed"
    HANDLER_ERROR = "handler_error"


@dataclass
class AuditEvent:
    """Single audit event."""
    event_type: str
    timestamp: float = field(default_factory=time.time)
    level: str = "info"
    
    # Context
    user_id: Optional[str] = None
    agent_id: Optional[str] = None
    task_id: Optional[str] = None
    handler: Optional[str] = None
    
    # Details
    action: str = ""
    resource: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    
    # Result
    success: bool = True
    error: Optional[str] = None
    
    # Source
    source_ip: Optional[str] = None
    source_host: Optional[str] = None
    
    # Timing
    duration_ms: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())
    
    @property
    def datetime(self) -> datetime:
        """Get event as datetime."""
        return datetime.fromtimestamp(self.timestamp)
    
    @property
    def iso_timestamp(self) -> str:
        """Get ISO format timestamp."""
        return self.datetime.isoformat()


class AuditLogger:
    """
    Comprehensive audit logger.
    
    Usage:
        logger = AuditLogger("/var/log/agent-cluster/audit.log")
        
        # Log event
        logger.log(
            AuditEventType.TASK_COMPLETED,
            user_id="user-1",
            task_id="task-123",
            success=True,
            details={"result": "success"},
        )
        
        # Context manager for timing
        with logger.audit("handler_execute", handler="file_read") as audit:
            # ... do work ...
            audit.details = {"bytes_read": 1024}
    """
    
    def __init__(
        self,
        log_file: str = "/tmp/agent_cluster/audit.log",
        max_size_mb: float = 100,
        max_files: int = 10,
        flush_interval: float = 1.0,
        include_hostname: bool = True,
    ):
        """
        Initialize audit logger.
        
        Args:
            log_file: Path to audit log file
            max_size_mb: Maximum log file size in MB
            max_files: Maximum number of rotated files
            flush_interval: Seconds between flushes
            include_hostname: Include hostname in events
        """
        self.log_file = Path(log_file)
        self.max_size_bytes = int(max_size_mb * 1024 * 1024)
        self.max_files = max_files
        self.flush_interval = flush_interval
        self.include_hostname = include_hostname
        
        self._buffer: List[AuditEvent] = []
        self._lock = threading.RLock()
        self._last_flush = time.time()
        
        # Event callbacks
        self._callbacks: List[Callable[[AuditEvent], None]] = []
        
        # Ensure log directory exists
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
        self._hostname = socket.gethostname()
        
        # Start flush thread
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
    
    def log(
        self,
        event_type: str,
        level: str = "info",
        user_id: str = None,
        agent_id: str = None,
        task_id: str = None,
        handler: str = None,
        action: str = "",
        resource: str = None,
        details: Dict[str, Any] = None,
        success: bool = True,
        error: str = None,
        source_ip: str = None,
        duration_ms: float = None,
    ):
        """Log an audit event."""
        event = AuditEvent(
            event_type=event_type.value if isinstance(event_type, AuditEventType) else event_type,
            level=level.value if isinstance(level, AuditLevel) else level,
            user_id=user_id,
            agent_id=agent_id,
            task_id=task_id,
            handler=handler,
            action=action,
            resource=resource,
            details=details or {},
            success=success,
            error=error,
            source_ip=source_ip,
            source_host=self._hostname if self.include_hostname else None,
            duration_ms=duration_ms,
        )
        
        self._add_event(event)
    
    def _add_event(self, event: AuditEvent):
        """Add event to buffer."""
        with self._lock:
            self._buffer.append(event)
            
            # Flush if buffer is large or interval elapsed
            if len(self._buffer) >= 100 or time.time() - self._last_flush >= self.flush_interval:
                self._flush()
        
        # Run callbacks
        for callback in self._callbacks:
            try:
                callback(event)
            except Exception:
                pass
    
    def _flush(self):
        """Flush buffer to file."""
        with self._lock:
            if not self._buffer:
                return
            
            events = self._buffer.copy()
            self._buffer.clear()
            self._last_flush = time.time()
        
        # Write to file
        self._write_events(events)
    
    def _write_events(self, events: List[AuditEvent]):
        """Write events to log file."""
        # Check for rotation
        if self.log_file.exists() and self.log_file.stat().st_size >= self.max_size_bytes:
            self._rotate()
        
        try:
            with open(self.log_file, 'a') as f:
                for event in events:
                    f.write(event.to_json() + '\n')
        except Exception as e:
            # Fallback to stderr
            import sys
            sys.stderr.write(f"Audit log error: {e}\n")
    
    def _rotate(self):
        """Rotate log files."""
        # Remove oldest file
        oldest = self.log_file.with_suffix(f'.{self.max_files}')
        if oldest.exists():
            oldest.unlink()
        
        # Rotate existing files
        for i in range(self.max_files - 1, 0, -1):
            src = self.log_file.with_suffix(f'.{i}')
            dst = self.log_file.with_suffix(f'.{i + 1}')
            if src.exists():
                src.rename(dst)
        
        # Rotate current file
        self.log_file.rename(self.log_file.with_suffix('.1'))
    
    def _flush_loop(self):
        """Background flush loop."""
        while True:
            time.sleep(self.flush_interval)
            self._flush()
    
    def audit(
        self,
        event_type: str,
        **kwargs,
    ) -> 'AuditContext':
        """Create audit context for timing."""
        return AuditContext(self, event_type, **kwargs)
    
    def on_event(self, callback: Callable[[AuditEvent], None]):
        """Register event callback."""
        self._callbacks.append(callback)
    
    def search(
        self,
        event_type: str = None,
        user_id: str = None,
        start_time: float = None,
        end_time: float = None,
        success: bool = None,
        limit: int = 100,
    ) -> List[AuditEvent]:
        """Search audit log."""
        events = []
        
        if not self.log_file.exists():
            return events
        
        try:
            with open(self.log_file) as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        event = AuditEvent(**data)
                        
                        # Apply filters
                        if event_type and event.event_type != event_type:
                            continue
                        if user_id and event.user_id != user_id:
                            continue
                        if start_time and event.timestamp < start_time:
                            continue
                        if end_time and event.timestamp > end_time:
                            continue
                        if success is not None and event.success != success:
                            continue
                        
                        events.append(event)
                        
                        if len(events) >= limit:
                            break
                    except (json.JSONDecodeError, TypeError):
                        continue
        except Exception:
            pass
        
        return events
    
    def get_stats(self, hours: float = 24) -> Dict[str, Any]:
        """Get audit statistics."""
        cutoff = time.time() - (hours * 3600)
        events = self.search(start_time=cutoff, limit=10000)
        
        by_type: Dict[str, int] = {}
        by_user: Dict[str, int] = {}
        failures = 0
        
        for event in events:
            by_type[event.event_type] = by_type.get(event.event_type, 0) + 1
            if event.user_id:
                by_user[event.user_id] = by_user.get(event.user_id, 0) + 1
            if not event.success:
                failures += 1
        
        return {
            'total_events': len(events),
            'by_type': by_type,
            'by_user': by_user,
            'failures': failures,
            'hours_covered': hours,
        }
    
    def flush(self):
        """Force flush of buffer."""
        self._flush()


class AuditContext:
    """Context manager for timed audit events."""
    
    def __init__(
        self,
        logger: AuditLogger,
        event_type: str,
        **kwargs,
    ):
        self.logger = logger
        self.event_type = event_type
        self.kwargs = kwargs
        self.start_time = None
        self.details = kwargs.get('details', {})
        self.success = True
        self.error = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000
        
        if exc_type:
            self.success = False
            self.error = str(exc_val)
        
        self.logger.log(
            self.event_type,
            duration_ms=duration_ms,
            success=self.success,
            error=self.error,
            details=self.details,
            **{k: v for k, v in self.kwargs.items() if k != 'details'},
        )
        
        return False  # Don't suppress exceptions


def create_audit_event(
    event_type: AuditEventType,
    user_id: str = None,
    **kwargs,
) -> AuditEvent:
    """Helper to create audit event."""
    return AuditEvent(
        event_type=event_type.value,
        user_id=user_id,
        **kwargs,
    )
