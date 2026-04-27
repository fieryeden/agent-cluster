"""
Secret Management Module

Provides secure handling of secrets:
- Environment variable provider
- Encrypted file provider
- Secret rotation
- Secret masking in logs
"""

import os
import json
import time
import threading
import hashlib
import base64
import secrets as py_secrets
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Callable, List
from pathlib import Path
from abc import ABC, abstractmethod


@dataclass
class Secret:
    """Represents a secret value."""
    name: str
    value: str
    source: str
    created: float = field(default_factory=time.time)
    expires: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_expired(self) -> bool:
        """Check if secret has expired."""
        if self.expires is None:
            return False
        return time.time() > self.expires
    
    def mask(self, visible_chars: int = 4) -> str:
        """Get masked version of secret."""
        if len(self.value) <= visible_chars:
            return '*' * len(self.value)
        return self.value[:visible_chars] + '*' * (len(self.value) - visible_chars)


class SecretProvider(ABC):
    """Base class for secret providers."""
    
    @abstractmethod
    def get(self, name: str) -> Optional[Secret]:
        """Get secret by name."""
        pass
    
    @abstractmethod
    def list_secrets(self) -> List[str]:
        """List available secret names."""
        pass
    
    @abstractmethod
    def refresh(self) -> bool:
        """Refresh secrets from source."""
        pass


class EnvironmentProvider(SecretProvider):
    """
    Secrets from environment variables.
    
    Usage:
        provider = EnvironmentProvider(prefix="CLUSTER_")
        
        secret = provider.get("API_KEY")
        # Looks for CLUSTER_API_KEY in environment
    """
    
    def __init__(self, prefix: str = ""):
        self.prefix = prefix
        self._cache: Dict[str, Secret] = {}
    
    def get(self, name: str) -> Optional[Secret]:
        """Get secret from environment."""
        env_name = f"{self.prefix}{name}"
        value = os.environ.get(env_name)
        
        if value is None:
            return None
        
        # Cache the secret
        if name not in self._cache or self._cache[name].value != value:
            self._cache[name] = Secret(
                name=name,
                value=value,
                source=f"env:{env_name}",
            )
        
        return self._cache[name]
    
    def list_secrets(self) -> List[str]:
        """List available secrets from environment."""
        secrets = []
        for key in os.environ:
            if self.prefix and key.startswith(self.prefix):
                secrets.append(key[len(self.prefix):])
            elif not self.prefix:
                secrets.append(key)
        return secrets
    
    def refresh(self) -> bool:
        """Refresh cache from environment."""
        self._cache.clear()
        return True


class FileProvider(SecretProvider):
    """
    Secrets from JSON file with optional encryption.
    
    Usage:
        provider = FileProvider("/etc/cluster/secrets.json")
        
        # Or encrypted
        provider = FileProvider("/etc/cluster/secrets.enc", encrypted=True)
    """
    
    def __init__(
        self,
        filepath: str,
        encrypted: bool = False,
        encryption_key: str = None,
    ):
        self.filepath = Path(filepath)
        self.encrypted = encrypted
        self.encryption_key = encryption_key or os.environ.get('CLUSTER_SECRET_KEY')
        
        self._secrets: Dict[str, Secret] = {}
        self._last_load = 0
        self._lock = threading.Lock()
        
        self._load()
    
    def get(self, name: str) -> Optional[Secret]:
        """Get secret by name."""
        with self._lock:
            self._check_refresh()
            return self._secrets.get(name)
    
    def list_secrets(self) -> List[str]:
        """List available secret names."""
        with self._lock:
            self._check_refresh()
            return list(self._secrets.keys())
    
    def refresh(self) -> bool:
        """Force refresh from file."""
        with self._lock:
            return self._load()
    
    def _check_refresh(self):
        """Check if file has been modified and reload."""
        if not self.filepath.exists():
            return
        
        mtime = self.filepath.stat().st_mtime
        if mtime > self._last_load:
            self._load()
    
    def _load(self) -> bool:
        """Load secrets from file."""
        if not self.filepath.exists():
            return False
        
        try:
            with open(self.filepath, 'rb') as f:
                data = f.read()
            
            if self.encrypted:
                data = self._decrypt(data)
            
            secrets_data = json.loads(data)
            
            self._secrets = {}
            for name, value in secrets_data.items():
                if isinstance(value, dict):
                    self._secrets[name] = Secret(
                        name=name,
                        value=value.get('value', ''),
                        source=f"file:{self.filepath}",
                        expires=value.get('expires'),
                        metadata=value.get('metadata', {}),
                    )
                else:
                    self._secrets[name] = Secret(
                        name=name,
                        value=str(value),
                        source=f"file:{self.filepath}",
                    )
            
            self._last_load = time.time()
            return True
            
        except Exception:
            return False
    
    def _decrypt(self, data: bytes) -> bytes:
        """Decrypt data (simple XOR for demo, use proper crypto in production)."""
        if not self.encryption_key:
            raise ValueError("Encryption key required for encrypted secrets")
        
        key = self.encryption_key.encode()
        result = bytearray(len(data))
        
        for i, byte in enumerate(data):
            result[i] = byte ^ key[i % len(key)]
        
        return bytes(result)
    
    @staticmethod
    def encrypt_file(
        source: str,
        dest: str,
        key: str,
    ):
        """Encrypt a secrets file."""
        with open(source, 'rb') as f:
            data = f.read()
        
        key_bytes = key.encode()
        result = bytearray(len(data))
        
        for i, byte in enumerate(data):
            result[i] = byte ^ key_bytes[i % len(key_bytes)]
        
        with open(dest, 'wb') as f:
            f.write(result)


class SecretManager:
    """
    Unified secret manager with multiple providers.
    
    Usage:
        manager = SecretManager()
        manager.add_provider(EnvironmentProvider(prefix="CLUSTER_"))
        manager.add_provider(FileProvider("/etc/cluster/secrets.json"))
        
        # Get secret
        api_key = manager.get("API_KEY")
        if api_key:
            print(api_key.mask())  # Show masked version
            value = api_key.value  # Get actual value
    """
    
    def __init__(
        self,
        mask_in_logs: bool = True,
        cache_ttl: float = 300,
    ):
        self.mask_in_logs = mask_in_logs
        self.cache_ttl = cache_ttl
        
        self._providers: List[SecretProvider] = []
        self._cache: Dict[str, Secret] = {}
        self._masked_values: set = set()
        self._lock = threading.RLock()
        
        # Callbacks
        self._on_rotation: Optional[Callable[[str, str], None]] = None
    
    def add_provider(self, provider: SecretProvider, priority: int = 0):
        """
        Add a secret provider.
        
        Args:
            provider: Secret provider
            priority: Higher priority checked first
        """
        with self._lock:
            self._providers.insert(priority, provider)
    
    def get(self, name: str) -> Optional[str]:
        """Get secret value by name."""
        secret = self.get_secret(name)
        return secret.value if secret else None
    
    def get_secret(self, name: str) -> Optional[Secret]:
        """Get full secret object by name."""
        with self._lock:
            # Check cache
            cached = self._cache.get(name)
            if cached and not cached.is_expired():
                return cached
            
            # Try each provider
            for provider in self._providers:
                secret = provider.get(name)
                if secret and not secret.is_expired():
                    self._cache[name] = secret
                    self._masked_values.add(secret.value)
                    return secret
            
            return None
    
    def get_or_default(self, name: str, default: str = "") -> str:
        """Get secret value or return default."""
        value = self.get(name)
        return value if value is not None else default
    
    def require(self, name: str) -> str:
        """Get secret value or raise error."""
        value = self.get(name)
        if value is None:
            raise KeyError(f"Required secret '{name}' not found")
        return value
    
    def mask(self, text: str) -> str:
        """Mask any secret values in text."""
        if not self.mask_in_logs:
            return text
        
        result = text
        for secret_value in self._masked_values:
            if secret_value in result:
                # Mask most of the secret
                visible = min(4, len(secret_value) // 4)
                masked = secret_value[:visible] + '***'
                result = result.replace(secret_value, masked)
        
        return result
    
    def set(self, name: str, value: str, ttl: float = None):
        """Set a runtime secret (not persisted)."""
        with self._lock:
            expires = time.time() + ttl if ttl else None
            secret = Secret(
                name=name,
                value=value,
                source="runtime",
                expires=expires,
            )
            self._cache[name] = secret
            self._masked_values.add(value)
    
    def rotate(self, name: str, new_value: str):
        """Rotate a secret value."""
        with self._lock:
            old = self._cache.get(name)
            if old:
                self._masked_values.discard(old.value)
            
            self.set(name, new_value)
            
            if self._on_rotation:
                self._on_rotation(name, new_value)
    
    def on_rotation(self, callback: Callable[[str, str], None]):
        """Register rotation callback."""
        self._on_rotation = callback
    
    def refresh_all(self):
        """Refresh secrets from all providers."""
        with self._lock:
            self._cache.clear()
            for provider in self._providers:
                provider.refresh()
    
    def list_available(self) -> List[str]:
        """List all available secret names."""
        names = set()
        for provider in self._providers:
            names.update(provider.list_secrets())
        return list(names)
    
    def export_masked(self) -> Dict[str, str]:
        """Export all secrets with masked values."""
        result = {}
        for name in self.list_available():
            secret = self.get_secret(name)
            if secret:
                result[name] = secret.mask()
        return result
    
    def generate_key(self, length: int = 32) -> str:
        """Generate a secure random key."""
        return py_secrets.token_hex(length)
    
    def hash_secret(self, value: str, salt: str = None) -> str:
        """Hash a secret value."""
        salt = salt or py_secrets.token_hex(16)
        hashed = hashlib.sha256((salt + value).encode()).hexdigest()
        return f"{salt}${hashed}"
    
    def verify_hash(self, value: str, hashed: str) -> bool:
        """Verify a secret against its hash."""
        try:
            salt, hash_value = hashed.split('$', 1)
            computed = hashlib.sha256((salt + value).encode()).hexdigest()
            return hash_value == computed
        except (ValueError, AttributeError):
            return False


def setup_default_secrets() -> SecretManager:
    """Set up secret manager with default providers."""
    manager = SecretManager()
    
    # Add environment provider
    manager.add_provider(EnvironmentProvider(prefix="CLUSTER_"))
    
    # Add file provider if exists
    secrets_file = Path("/etc/agent-cluster/secrets.json")
    if secrets_file.exists():
        manager.add_provider(FileProvider(str(secrets_file)))
    
    return manager
