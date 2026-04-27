# Agent Cluster - Production Dockerfile
# Multi-stage build for minimal image size

# ============================================
# Stage 1: Builder
# ============================================
FROM python:3.11-slim as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY setup.py .
COPY agent_cluster/ ./agent_cluster/
COPY handlers/ ./handlers/
COPY reliability/ ./reliability/
COPY security/ ./security/
COPY deployment/ ./deployment/
COPY coordinator/ ./coordinator/
COPY agents/ ./agents/
COPY dashboard/ ./dashboard/
COPY capabilities/ ./capabilities/
COPY orchestration/ ./orchestration/
COPY autolearning/ ./autolearning/
COPY protocol/ ./protocol/
COPY network/ ./network/

# Build wheel
RUN pip wheel --no-deps --wheel-dir /wheels .

# ============================================
# Stage 2: Runtime
# ============================================
FROM python:3.11-slim as runtime

# Security: Create non-root user
RUN groupadd -r agentcluster && \
    useradd -r -g agentcluster -d /app -s /sbin/nologin agentcluster

WORKDIR /app

# Copy built wheel from builder
COPY --from=builder /wheels /wheels

# Install (no external deps, just our package)
RUN pip install --no-cache-dir /wheels/*.whl && \
    rm -rf /wheels

# Create directories
RUN mkdir -p /tmp/agent_cluster /var/log/agent-cluster && \
    chown -R agentcluster:agentcluster /app /tmp/agent_cluster /var/log/agent-cluster

# Copy entrypoint
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copy default config
COPY config.example.yaml /app/config.yaml

# Environment
ENV PYTHONUNBUFFERED=1 \
    AGENT_CLUSTER_CONFIG=/app/config.yaml \
    AGENT_CLUSTER_LOG_LEVEL=INFO

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from agent_cluster import HealthChecker; h=HealthChecker(); print('healthy')" || exit 1

# Expose ports
EXPOSE 8080 3000

# Switch to non-root user
USER agentcluster

# Entry point
ENTRYPOINT ["/entrypoint.sh"]

# Default: start coordinator
CMD ["coordinator", "--port", "8080"]
