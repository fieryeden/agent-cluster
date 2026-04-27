#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
# Agent Cluster Bootstrap Installer
# ═══════════════════════════════════════════════════════════════
# 
# One-time script to install/update Agent Cluster on a machine
# that doesn't have OTA capability yet.
#
# After running this, the agent will have full OTA support and
# all future updates can be pushed from the coordinator.
#
# Usage:
#   curl -sL <url>/bootstrap-agent-cluster.sh | bash
#   OR
#   bash bootstrap-agent-cluster.sh /opt/agent-cluster
#
# ═══════════════════════════════════════════════════════════════

set -euo pipefail

# ─── Configuration ───────────────────────────────────────────
DEFAULT_INSTALL_DIR="/opt/agent-cluster"
REPO_URL="https://github.com/fieryeden/agent-cluster.git"
BRANCH="main"
AGENT_ID="${AGENT_ID:-}"
COORDINATOR_HOST="${COORDINATOR_HOST:-}"
COORDINATOR_PORT="${COORDINATOR_PORT:-8080}"

# ─── Colors ──────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log()   { echo -e "${BLUE}[AGENT-CLUSTER]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ─── Pre-flight Checks ──────────────────────────────────────
check_prerequisites() {
    log "Checking prerequisites..."
    
    if ! command -v python3 &>/dev/null; then
        err "Python 3.8+ is required but not found."
        exit 1
    fi
    
    PY_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
    PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)
    
    if [ "$PY_MAJOR" -lt 3 ] || ([ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -lt 8 ]); then
        err "Python 3.8+ required, found $PY_VERSION"
        exit 1
    fi
    
    ok "Python $PY_VERSION found"
    
    if ! command -v git &>/dev/null; then
        warn "git not found — will download tarball instead"
        USE_GIT=false
    else
        USE_GIT=true
        ok "git found"
    fi
    
    if [ "$(id -u)" -ne 0 ]; then
        warn "Not running as root — may need sudo for install dir"
    fi
}

# ─── Install Methods ────────────────────────────────────────
install_from_git() {
    local install_dir="$1"
    log "Cloning from $REPO_URL (branch: $BRANCH)..."
    
    if [ -d "$install_dir/.git" ]; then
        log "Existing repo found at $install_dir — updating..."
        cd "$install_dir"
        git fetch origin "$BRANCH"
        git reset --hard "origin/$BRANCH"
    else
        git clone -b "$BRANCH" "$REPO_URL" "$install_dir"
        cd "$install_dir"
    fi
    
    ok "Code retrieved at $(git rev-parse --short HEAD)"
}

install_from_tarball() {
    local install_dir="$1"
    local tar_url="${REPO_URL%/}.git/archive/${BRANCH}.tar.gz"
    local tmp_tar="/tmp/agent-cluster-latest.tar.gz"
    
    log "Downloading tarball from GitHub..."
    
    if command -v curl &>/dev/null; then
        curl -sL "$tar_url" -o "$tmp_tar"
    elif command -v wget &>/dev/null; then
        wget -q "$tar_url" -O "$tmp_tar"
    else
        err "Need curl or wget to download"
        exit 1
    fi
    
    mkdir -p "$install_dir"
    tar -xzf "$tmp_tar" -C "$install_dir" --strip-components=1
    rm -f "$tmp_tar"
    
    ok "Code extracted to $install_dir"
}

# ─── Setup ───────────────────────────────────────────────────
setup_virtualenv() {
    local install_dir="$1"
    log "Setting up virtual environment..."
    
    if [ ! -d "$install_dir/venv" ]; then
        python3 -m venv "$install_dir/venv"
        ok "Virtual environment created"
    else
        ok "Virtual environment already exists"
    fi
    
    source "$install_dir/venv/bin/activate"
    pip install -q --upgrade pip
    
    # Install in development mode (editable)
    cd "$install_dir"
    pip install -q -e .
    
    ok "Package installed (editable mode)"
}

install_config() {
    local install_dir="$1"
    local config_file="$install_dir/config.yaml"
    
    if [ -f "$config_file" ]; then
        ok "Config already exists at $config_file — preserving"
        return
    fi
    
    log "Creating default config..."
    cp "$install_dir/config.example.yaml" "$config_file"
    
    # Set agent ID if provided
    if [ -n "$AGENT_ID" ]; then
        python3 -c "
import yaml
with open('$config_file') as f:
    cfg = yaml.safe_load(f) or {}
cfg['agent_id'] = '$AGENT_ID'
with open('$config_file', 'w') as f:
    yaml.dump(cfg, f, default_flow_style=False)
" 2>/dev/null || warn "Could not set agent_id in config (pyyaml not installed)"
    fi
    
    ok "Config created at $config_file"
}

register_ota_capability() {
    local install_dir="$1"
    log "Registering OTA capability..."
    
    # Create a marker file that the coordinator can check
    mkdir -p "$install_dir/shared"
    cat > "$install_dir/shared/capabilities.json" <<EOF
{
  "agent_id": "${AGENT_ID:-$(hostname)}",
  "capabilities": [
    "ota_install",
    "ota_rollback",
    "peer_messaging",
    "file_transfer",
    "task_delegation",
    "consensus_voting",
    "status_query",
    "context_share"
  ],
  "version": "0.12.0",
  "installed_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
    ok "OTA capability registered"
}

install_systemd_service() {
    local install_dir="$1"
    local agent_name="${AGENT_ID:-$(hostname)}"
    
    if [ ! -d /etc/systemd/system ]; then
        warn "systemd not found — skipping service installation"
        return
    fi
    
    log "Installing systemd service..."
    
    cat > /etc/systemd/system/agent-cluster.service <<EOF
[Unit]
Description=Agent Cluster - ${agent_name}
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=${install_dir}
ExecStart=${install_dir}/venv/bin/python -m agent_cluster
Restart=on-failure
RestartSec=10
Environment=AGENT_ID=${agent_name}
Environment=COORDINATOR_HOST=${COORDINATOR_HOST}
Environment=COORDINATOR_PORT=${COORDINATOR_PORT}

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable agent-cluster
    ok "systemd service installed (agent-cluster.service)"
}

# ─── Main ────────────────────────────────────────────────────
main() {
    local install_dir="${1:-$DEFAULT_INSTALL_DIR}"
    
    echo ""
    echo "═══════════════════════════════════════════════════════"
    echo "  Agent Cluster Bootstrap Installer"
    echo "  Target: $install_dir"
    echo "═══════════════════════════════════════════════════════"
    echo ""
    
    check_prerequisites
    
    # Get the code
    if [ "$USE_GIT" = true ]; then
        install_from_git "$install_dir"
    else
        install_from_tarball "$install_dir"
    fi
    
    # Setup environment
    setup_virtualenv "$install_dir"
    install_config "$install_dir"
    
    # Register capabilities (including OTA)
    register_ota_capability "$install_dir"
    
    # Install as service (Linux only)
    install_systemd_service "$install_dir"
    
    echo ""
    echo "═══════════════════════════════════════════════════════"
    ok "Bootstrap complete!"
    echo ""
    echo "  Install dir:   $install_dir"
    echo "  Agent ID:      ${AGENT_ID:-$(hostname)}"
    echo "  Version:       0.12.0"
    echo "  OTA capable:   YES ✓"
    echo ""
    echo "  Start agent:   systemctl start agent-cluster"
    echo "  View logs:     journalctl -u agent-cluster -f"
    echo ""
    echo "  Future updates will be pushed via OTA from"
    echo "  the coordinator — no manual intervention needed."
    echo "═══════════════════════════════════════════════════════"
    echo ""
}

main "$@"
