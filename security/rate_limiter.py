"""
Rate Limiting Module

Provides rate limiting with:
- Token bucket algorithm
- Leaky bucket algorithm
- Per-client limits
- Sliding window limits
- Distributed rate limiting
"""

import time
import threading
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Callable
from collections import defaultdict
from enum import Enum


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    requests_per_second: float = 10.0
    burst_size: int = None  # Max burst, defaults to 2x rate
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET
    
    # Window settings (for window-based strategies)
    window_size: float = 1.0  # seconds
    
    def __post_init__(self):
        if self.burst_size is None:
            self.burst_size = int(self.requests_per_second * 2)


@dataclass
class RateLimitResult:
    """Result of rate limit check."""
    allowed: bool
    remaining: float
    reset_after: float
    retry_after: float = 0.0


class TokenBucket:
    """
    Token bucket rate limiter.
    
    Tokens are added at a constant rate up to a maximum capacity.
    Each request consumes one token.
    
    Usage:
        bucket = TokenBucket(rate=10, capacity=20)
        
        if bucket.consume():
            # Request allowed
        else:
            # Rate limited
    """
    
    def __init__(
        self,
        rate: float,
        capacity: int = None,
    ):
        """
        Initialize token bucket.
        
        Args:
            rate: Tokens added per second
            capacity: Maximum tokens (defaults to 2x rate)
        """
        self.rate = rate
        self.capacity = capacity or int(rate * 2)
        
        self._tokens = self.capacity
        self._last_update = time.time()
        self._lock = threading.Lock()
    
    def consume(self, tokens: int = 1) -> RateLimitResult:
        """
        Try to consume tokens.
        
        Args:
            tokens: Number of tokens to consume
            
        Returns:
            RateLimitResult with allowed status and metadata
        """
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            
            # Add tokens based on elapsed time
            self._tokens = min(
                self.capacity,
                self._tokens + elapsed * self.rate
            )
            self._last_update = now
            
            if self._tokens >= tokens:
                # Allow request
                self._tokens -= tokens
                return RateLimitResult(
                    allowed=True,
                    remaining=self._tokens,
                    reset_after=0,
                )
            else:
                # Rate limited
                tokens_needed = tokens - self._tokens
                retry_after = tokens_needed / self.rate
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_after=retry_after,
                    retry_after=retry_after,
                )
    
    def peek(self) -> float:
        """Get current token count without consuming."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            return min(self.capacity, self._tokens + elapsed * self.rate)
    
    def reset(self):
        """Reset bucket to full capacity."""
        with self._lock:
            self._tokens = self.capacity
            self._last_update = time.time()


class LeakyBucket:
    """
    Leaky bucket rate limiter.
    
    Requests are added to a queue and processed at a constant rate.
    
    Usage:
        bucket = LeakyBucket(rate=10, capacity=20)
        
        if bucket.try_add():
            # Request allowed
        else:
            # Rate limited
    """
    
    def __init__(
        self,
        rate: float,
        capacity: int,
    ):
        """
        Initialize leaky bucket.
        
        Args:
            rate: Requests processed per second
            capacity: Maximum queue size
        """
        self.rate = rate
        self.capacity = capacity
        
        self._queue_level = 0
        self._last_update = time.time()
        self._lock = threading.Lock()
    
    def try_add(self) -> RateLimitResult:
        """Try to add a request to the bucket."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            
            # Drain the bucket
            drained = elapsed * self.rate
            self._queue_level = max(0, self._queue_level - drained)
            self._last_update = now
            
            if self._queue_level < self.capacity:
                # Allow request
                self._queue_level += 1
                return RateLimitResult(
                    allowed=True,
                    remaining=self.capacity - self._queue_level,
                    reset_after=0,
                )
            else:
                # Rate limited
                retry_after = 1.0 / self.rate
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_after=retry_after,
                    retry_after=retry_after,
                )
    
    def get_level(self) -> int:
        """Get current queue level."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            drained = elapsed * self.rate
            return max(0, int(self._queue_level - drained))


class SlidingWindowCounter:
    """
    Sliding window rate limiter.
    
    Counts requests in a sliding time window.
    """
    
    def __init__(
        self,
        max_requests: int,
        window_seconds: float,
    ):
        """
        Initialize sliding window counter.
        
        Args:
            max_requests: Maximum requests in window
            window_seconds: Window size in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        
        self._timestamps: list = []
        self._lock = threading.Lock()
    
    def try_acquire(self) -> RateLimitResult:
        """Try to acquire a slot in the window."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds
            
            # Remove old timestamps
            self._timestamps = [t for t in self._timestamps if t > cutoff]
            
            if len(self._timestamps) < self.max_requests:
                # Allow request
                self._timestamps.append(now)
                return RateLimitResult(
                    allowed=True,
                    remaining=self.max_requests - len(self._timestamps),
                    reset_after=self.window_seconds,
                )
            else:
                # Rate limited
                oldest = min(self._timestamps)
                retry_after = oldest - cutoff + 0.001
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_after=retry_after,
                    retry_after=retry_after,
                )
    
    def get_count(self) -> int:
        """Get current request count in window."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds
            return sum(1 for t in self._timestamps if t > cutoff)


class RateLimiter:
    """
    Unified rate limiter with per-client tracking.
    
    Usage:
        limiter = RateLimiter(
            requests_per_second=100,
            per_client_limit=10,
        )
        
        # Check rate limit
        result = limiter.check("client-1")
        if result.allowed:
            # Process request
        else:
            # Return 429 with retry_after
    """
    
    def __init__(
        self,
        requests_per_second: float = 100.0,
        burst_size: int = None,
        per_client_limit: float = None,
        strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET,
    ):
        self.global_rate = requests_per_second
        self.global_burst = burst_size or int(requests_per_second * 2)
        self.per_client_limit = per_client_limit
        self.strategy = strategy
        
        self._global_limiter = self._create_limiter(
            requests_per_second,
            self.global_burst,
        )
        
        self._client_limiters: Dict[str, Any] = {}
        self._lock = threading.RLock()
        
        # Callbacks
        self._on_limit: Optional[Callable[[str], None]] = None
    
    def _create_limiter(self, rate: float, burst: int):
        """Create appropriate limiter based on strategy."""
        if self.strategy == RateLimitStrategy.TOKEN_BUCKET:
            return TokenBucket(rate, burst)
        elif self.strategy == RateLimitStrategy.LEAKY_BUCKET:
            return LeakyBucket(rate, burst)
        elif self.strategy == RateLimitStrategy.SLIDING_WINDOW:
            return SlidingWindowCounter(burst, 1.0)
        else:
            return TokenBucket(rate, burst)
    
    def check(
        self,
        client_id: str = None,
        tokens: int = 1,
    ) -> RateLimitResult:
        """
        Check if request is allowed.
        
        Checks both global and per-client limits.
        
        Args:
            client_id: Client identifier (optional)
            tokens: Number of tokens to consume
            
        Returns:
            RateLimitResult with allowed status
        """
        # Check global limit first
        global_result = self._global_limiter.consume(tokens)
        if not global_result.allowed:
            if self._on_limit:
                self._on_limit("global")
            return global_result
        
        # Check per-client limit
        if client_id and self.per_client_limit:
            with self._lock:
                if client_id not in self._client_limiters:
                    self._client_limiters[client_id] = self._create_limiter(
                        self.per_client_limit,
                        int(self.per_client_limit * 2),
                    )
                
                client_result = self._client_limiters[client_id].consume(tokens)
                if not client_result.allowed:
                    if self._on_limit:
                        self._on_limit(client_id)
                    return client_result
        
        return global_result
    
    def acquire(self, client_id: str = None) -> bool:
        """Simple check if request is allowed."""
        return self.check(client_id).allowed
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        with self._lock:
            return {
                'global': {
                    'rate': self.global_rate,
                    'burst': self.global_burst,
                    'current_tokens': self._global_limiter.peek() if hasattr(self._global_limiter, 'peek') else None,
                },
                'per_client_limit': self.per_client_limit,
                'active_clients': len(self._client_limiters),
                'strategy': self.strategy.value,
            }
    
    def on_limit_exceeded(self, callback: Callable[[str], None]):
        """Register callback for limit exceeded events."""
        self._on_limit = callback
    
    def reset_client(self, client_id: str):
        """Reset rate limit for a client."""
        with self._lock:
            if client_id in self._client_limiters:
                limiter = self._client_limiters[client_id]
                if hasattr(limiter, 'reset'):
                    limiter.reset()
                else:
                    del self._client_limiters[client_id]
    
    def reset_all(self):
        """Reset all rate limits."""
        with self._lock:
            self._global_limiter.reset()
            self._client_limiters.clear()


class RateLimitMiddleware:
    """
    Rate limiting middleware for request handling.
    
    Usage:
        middleware = RateLimitMiddleware(limiter)
        
        # In request handler
        result = middleware.process(request)
        if not result.allowed:
            return Response(429, {"retry_after": result.retry_after})
    """
    
    def __init__(
        self,
        limiter: RateLimiter,
        key_extractor: Callable[[Any], str] = None,
    ):
        self.limiter = limiter
        self.key_extractor = key_extractor or self._default_key_extractor
    
    def _default_key_extractor(self, request: Any) -> str:
        """Extract client key from request."""
        # Default: try common attributes
        for attr in ['client_id', 'user_id', 'remote_addr', 'ip']:
            if hasattr(request, attr):
                return getattr(request, attr)
        
        # Check headers
        if hasattr(request, 'headers'):
            for header in ['X-Client-ID', 'X-Forwarded-For', 'X-Real-IP']:
                if header in request.headers:
                    return request.headers[header]
        
        return 'default'
    
    def process(self, request: Any) -> RateLimitResult:
        """Process request through rate limiter."""
        key = self.key_extractor(request)
        return self.limiter.check(key)
    
    def add_rate_limit_headers(
        self,
        response: Any,
        result: RateLimitResult,
    ) -> Any:
        """Add rate limit headers to response."""
        headers = {
            'X-RateLimit-Remaining': str(int(result.remaining)),
            'X-RateLimit-Reset': str(int(result.reset_after)),
        }
        if not result.allowed:
            headers['Retry-After'] = str(int(result.retry_after))
        
        if hasattr(response, 'headers'):
            for k, v in headers.items():
                response.headers[k] = v
        
        return response
