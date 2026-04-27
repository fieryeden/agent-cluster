"""
Authentication Module

Provides API key and JWT authentication with:
- Role-based access control (RBAC)
- Permission checks
- Token management
- Session handling
"""

import time
import secrets
import hashlib
import hmac
import threading
import base64
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any, Set
from enum import Enum
from datetime import datetime


class Permission(Enum):
    """System permissions."""
    # Task operations
    TASK_READ = "task:read"
    TASK_WRITE = "task:write"
    TASK_DELETE = "task:delete"
    TASK_EXECUTE = "task:execute"
    
    # Agent operations
    AGENT_READ = "agent:read"
    AGENT_WRITE = "agent:write"
    AGENT_DELETE = "agent:delete"
    AGENT_REGISTER = "agent:register"
    
    # System operations
    SYSTEM_ADMIN = "system:admin"
    SYSTEM_CONFIG = "system:config"
    SYSTEM_MONITOR = "system:monitor"
    
    # Data operations
    DATA_READ = "data:read"
    DATA_WRITE = "data:write"
    DATA_DELETE = "data:delete"
    
    # Handler-specific
    HANDLER_EXECUTE = "handler:execute"
    HANDLER_DANGEROUS = "handler:dangerous"


@dataclass
class Role:
    """User role with permissions."""
    name: str
    permissions: Set[Permission] = field(default_factory=set)
    inherits: List[str] = field(default_factory=list)
    
    def has_permission(self, permission: Permission) -> bool:
        """Check if role has permission."""
        return permission in self.permissions


# Default roles
ROLES = {
    'admin': Role(
        name='admin',
        permissions={
            Permission.TASK_READ, Permission.TASK_WRITE, Permission.TASK_DELETE, Permission.TASK_EXECUTE,
            Permission.AGENT_READ, Permission.AGENT_WRITE, Permission.AGENT_DELETE, Permission.AGENT_REGISTER,
            Permission.SYSTEM_ADMIN, Permission.SYSTEM_CONFIG, Permission.SYSTEM_MONITOR,
            Permission.DATA_READ, Permission.DATA_WRITE, Permission.DATA_DELETE,
            Permission.HANDLER_EXECUTE, Permission.HANDLER_DANGEROUS,
        },
    ),
    'operator': Role(
        name='operator',
        permissions={
            Permission.TASK_READ, Permission.TASK_WRITE, Permission.TASK_EXECUTE,
            Permission.AGENT_READ,
            Permission.SYSTEM_MONITOR,
            Permission.DATA_READ, Permission.DATA_WRITE,
            Permission.HANDLER_EXECUTE,
        },
    ),
    'viewer': Role(
        name='viewer',
        permissions={
            Permission.TASK_READ,
            Permission.AGENT_READ,
            Permission.SYSTEM_MONITOR,
            Permission.DATA_READ,
        },
    ),
    'agent': Role(
        name='agent',
        permissions={
            Permission.TASK_READ, Permission.TASK_EXECUTE,
            Permission.AGENT_REGISTER,
            Permission.HANDLER_EXECUTE,
        },
    ),
}


@dataclass
class User:
    """Authenticated user."""
    id: str
    name: str
    roles: List[str] = field(default_factory=list)
    api_key: Optional[str] = None
    created: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def has_permission(self, permission: Permission) -> bool:
        """Check if user has permission through any role."""
        for role_name in self.roles:
            role = ROLES.get(role_name)
            if role and role.has_permission(permission):
                return True
        return False
    
    def get_all_permissions(self) -> Set[Permission]:
        """Get all permissions from all roles."""
        permissions = set()
        for role_name in self.roles:
            role = ROLES.get(role_name)
            if role:
                permissions.update(role.permissions)
        return permissions


class APIKeyAuth:
    """
    API Key authentication.
    
    Usage:
        auth = APIKeyAuth()
        
        # Generate key
        api_key = auth.generate_key("user-1", ["operator"])
        
        # Validate
        user = auth.validate(api_key)
        if user and user.has_permission(Permission.TASK_EXECUTE):
            # Allow execution
    """
    
    def __init__(
        self,
        key_length: int = 32,
        prefix: str = "ac_",
        keys_file: str = None,
    ):
        self.key_length = key_length
        self.prefix = prefix
        
        self._keys: Dict[str, User] = {}
        self._users: Dict[str, User] = {}
        self._lock = threading.RLock()
        
        if keys_file:
            self._load_keys(keys_file)
    
    def generate_key(
        self,
        user_id: str,
        roles: List[str],
        name: str = None,
        metadata: Dict[str, Any] = None,
    ) -> str:
        """Generate new API key for user."""
        # Generate random key
        raw_key = secrets.token_hex(self.key_length)
        api_key = f"{self.prefix}{raw_key}"
        
        # Create user
        user = User(
            id=user_id,
            name=name or user_id,
            roles=roles,
            api_key=api_key,
            metadata=metadata or {},
        )
        
        with self._lock:
            self._keys[api_key] = user
            self._users[user_id] = user
        
        return api_key
    
    def validate(self, api_key: str) -> Optional[User]:
        """Validate API key and return user."""
        with self._lock:
            user = self._keys.get(api_key)
            if user:
                user.last_active = time.time()
            return user
    
    def revoke(self, api_key: str) -> bool:
        """Revoke an API key."""
        with self._lock:
            if api_key in self._keys:
                user = self._keys.pop(api_key)
                self._users.pop(user.id, None)
                return True
            return False
    
    def revoke_user(self, user_id: str) -> List[str]:
        """Revoke all keys for a user."""
        with self._lock:
            user = self._users.pop(user_id, None)
            if user and user.api_key:
                self._keys.pop(user.api_key, None)
                return [user.api_key]
            return []
    
    def list_keys(self) -> List[Dict[str, Any]]:
        """List all API keys (masked)."""
        with self._lock:
            return [
                {
                    'user_id': user.id,
                    'name': user.name,
                    'key_prefix': user.api_key[:8] + '...' if user.api_key else None,
                    'roles': user.roles,
                    'created': user.created,
                    'last_active': user.last_active,
                }
                for user in self._users.values()
            ]
    
    def _load_keys(self, filepath: str):
        """Load keys from file."""
        import os
        if os.path.exists(filepath):
            with open(filepath) as f:
                data = json.load(f)
            for key_data in data.get('keys', []):
                user = User(**key_data)
                if user.api_key:
                    self._keys[user.api_key] = user
                    self._users[user.id] = user


class JWTAuth:
    """
    JWT authentication (HMAC-SHA256).
    
    Usage:
        auth = JWTAuth(secret="your-secret-key")
        
        # Create token
        token = auth.create_token("user-1", ["operator"])
        
        # Validate
        user = auth.validate(token)
    """
    
    def __init__(
        self,
        secret: str = None,
        issuer: str = "agent-cluster",
        expiry_seconds: int = 3600,
        algorithm: str = "HS256",
    ):
        self.secret = secret or secrets.token_hex(32)
        self.issuer = issuer
        self.expiry_seconds = expiry_seconds
        self.algorithm = algorithm
        
        self._blacklist: Set[str] = set()
        self._lock = threading.Lock()
    
    def create_token(
        self,
        user_id: str,
        roles: List[str],
        name: str = None,
        expiry: int = None,
        metadata: Dict[str, Any] = None,
    ) -> str:
        """Create JWT token."""
        now = time.time()
        expiry = expiry or self.expiry_seconds
        
        header = {"alg": self.algorithm, "typ": "JWT"}
        payload = {
            "sub": user_id,
            "iss": self.issuer,
            "iat": now,
            "exp": now + expiry,
            "roles": roles,
            "name": name or user_id,
        }
        if metadata:
            payload["metadata"] = metadata
        
        # Encode header and payload
        header_b64 = base64.urlsafe_b64encode(json.dumps(header).encode()).decode().rstrip('=')
        payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')
        
        # Sign
        signing_input = f"{header_b64}.{payload_b64}"
        signature = hmac.new(
            self.secret.encode(),
            signing_input.encode(),
            hashlib.sha256
        ).digest()
        signature_b64 = base64.urlsafe_b64encode(signature).decode().rstrip('=')
        
        return f"{signing_input}.{signature_b64}"
    
    def validate(self, token: str) -> Optional[User]:
        """Validate JWT token."""
        with self._lock:
            if token in self._blacklist:
                return None
        
        try:
            parts = token.split('.')
            if len(parts) != 3:
                return None
            
            header_b64, payload_b64, signature_b64 = parts
            
            # Verify signature
            signing_input = f"{header_b64}.{payload_b64}"
            expected_sig = hmac.new(
                self.secret.encode(),
                signing_input.encode(),
                hashlib.sha256
            ).digest()
            expected_b64 = base64.urlsafe_b64encode(expected_sig).decode().rstrip('=')
            
            if not hmac.compare_digest(signature_b64, expected_b64):
                return None
            
            # Decode payload
            payload_json = base64.urlsafe_b64decode(payload_b64 + '==').decode()
            payload = json.loads(payload_json)
            
            # Check expiry
            if payload.get('exp', 0) < time.time():
                return None
            
            # Check issuer
            if payload.get('iss') != self.issuer:
                return None
            
            return User(
                id=payload['sub'],
                name=payload.get('name', payload['sub']),
                roles=payload.get('roles', []),
                metadata=payload.get('metadata', {}),
                last_active=time.time(),
            )
            
        except Exception:
            return None
    
    def revoke(self, token: str):
        """Revoke a token (add to blacklist)."""
        with self._lock:
            self._blacklist.add(token)
    
    def cleanup_blacklist(self):
        """Remove expired tokens from blacklist."""
        with self._lock:
            # In production, would parse and check expiry
            # For simplicity, clear all
            self._blacklist.clear()


class AuthManager:
    """
    Unified authentication manager.
    
    Usage:
        auth = AuthManager()
        
        # Add API key auth
        api_key = auth.create_api_key("user-1", ["operator"])
        
        # Create JWT token
        jwt_token = auth.create_jwt("user-1", ["operator"])
        
        # Authenticate request
        user = auth.authenticate(request.headers.get('Authorization'))
        if user and auth.check_permission(user, Permission.TASK_EXECUTE):
            # Allow
    """
    
    def __init__(
        self,
        jwt_secret: str = None,
        api_key_prefix: str = "ac_",
    ):
        self.api_key_auth = APIKeyAuth(prefix=api_key_prefix)
        self.jwt_auth = JWTAuth(secret=jwt_secret)
        self._lock = threading.RLock()
    
    def create_api_key(
        self,
        user_id: str,
        roles: List[str],
        name: str = None,
    ) -> str:
        """Create API key for user."""
        return self.api_key_auth.generate_key(user_id, roles, name)
    
    def create_jwt(
        self,
        user_id: str,
        roles: List[str],
        name: str = None,
        expiry: int = None,
    ) -> str:
        """Create JWT token for user."""
        return self.jwt_auth.create_token(user_id, roles, name, expiry)
    
    def authenticate(
        self,
        auth_header: str,
        token: str = None,
    ) -> Optional[User]:
        """
        Authenticate from Authorization header or token.
        
        Supports:
        - Bearer <jwt>
        - ApiKey <key>
        - Direct key string
        """
        credential = auth_header or token
        if not credential:
            return None
        
        # Parse header
        if ' ' in credential:
            scheme, value = credential.split(' ', 1)
            scheme = scheme.lower()
        else:
            scheme = None
            value = credential
        
        # Try JWT
        if scheme == 'bearer' or (scheme is None and '.' in value):
            return self.jwt_auth.validate(value)
        
        # Try API key
        if scheme == 'apikey' or (scheme is None and value.startswith(self.api_key_auth.prefix)):
            return self.api_key_auth.validate(value)
        
        return None
    
    def check_permission(
        self,
        user: User,
        permission: Permission,
    ) -> bool:
        """Check if user has permission."""
        return user.has_permission(permission)
    
    def require_permission(
        self,
        user: User,
        permission: Permission,
    ) -> None:
        """Require permission or raise exception."""
        if not user.has_permission(permission):
            raise PermissionError(f"Permission denied: {permission.value}")
    
    def revoke_api_key(self, api_key: str) -> bool:
        """Revoke API key."""
        return self.api_key_auth.revoke(api_key)
    
    def revoke_jwt(self, token: str):
        """Revoke JWT token."""
        self.jwt_auth.revoke(token)
    
    def revoke_user(self, user_id: str):
        """Revoke all credentials for user."""
        self.api_key_auth.revoke_user(user_id)
    
    def list_users(self) -> List[Dict[str, Any]]:
        """List all users."""
        return self.api_key_auth.list_keys()
