"""
Security Module for Agent Cluster

Provides production-grade security:
- API key and JWT authentication
- TLS encryption for network traffic
- Rate limiting per agent/coordinator
- Input validation and sanitization
- Command whitelist for shell handler
- Audit logging
- Secret management
"""

from security.auth import (
    AuthManager,
    APIKeyAuth,
    JWTAuth,
    User,
    Permission,
)

from security.tls_config import TLSConfig, TLSCertificate

from security.rate_limiter import (
    RateLimiter,
    RateLimitConfig,
    TokenBucket,
    LeakyBucket,
)

from security.input_validation import (
    InputValidator,
    ValidationError,
    Sanitizer,
    CommandWhitelist,
)

from security.audit import (
    AuditLogger,
    AuditEvent,
    AuditLevel,
)

from security.secrets import (
    SecretManager,
    SecretProvider,
    EnvironmentProvider,
    FileProvider,
)

__all__ = [
    # Auth
    'AuthManager',
    'APIKeyAuth',
    'JWTAuth',
    'User',
    'Permission',
    
    # TLS
    'TLSConfig',
    'TLSCertificate',
    
    # Rate limiting
    'RateLimiter',
    'RateLimitConfig',
    'TokenBucket',
    'LeakyBucket',
    
    # Input validation
    'InputValidator',
    'ValidationError',
    'Sanitizer',
    'CommandWhitelist',
    
    # Audit
    'AuditLogger',
    'AuditEvent',
    'AuditLevel',
    
    # Secrets
    'SecretManager',
    'SecretProvider',
    'EnvironmentProvider',
    'FileProvider',
]
