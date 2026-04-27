"""
Installation Script Generator

Creates installation scripts for:
- Linux (apt, yum, pacman)
- macOS (brew)
- Windows (chocolatey, msi)
- Automated setup
"""

import os
import platform
from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class InstallType(Enum):
    """Installation types."""
    PIP = "pip"
    DOCKER = "docker"
    BINARY = "binary"
    SOURCE = "source"


@dataclass
class InstallConfig:
    """Installation configuration."""
    install_type: InstallType = InstallType.PIP
    version: str = "latest"
    prefix: str = "/usr/local"
    user_install: bool = False
    create_user: bool = True
    user_name: str = "agent-cluster"
    create_directories: bool = True
    data_dir: str = "/var/lib/agent-cluster"
    config_dir: str = "/etc/agent-cluster"
    log_dir: str = "/var/log/agent-cluster"
    enable_service: bool = True
    start_service: bool = True


class Installer:
    """
    Generates installation scripts.
    
    Usage:
        installer = Installer()
        
        # Generate Linux install script
        script = installer.generate_linux_script()
        
        # Generate Docker install
        script = installer.generate_docker_compose()
    """
    
    def __init__(self, config: InstallConfig = None):
        self.config = config or InstallConfig()
        self.system = platform.system().lower()
    
    def detect_package_manager(self) -> str:
        """Detect system package manager."""
        if os.path.exists("/usr/bin/apt"):
            return "apt"
        elif os.path.exists("/usr/bin/yum"):
            return "yum"
        elif os.path.exists("/usr/bin/dnf"):
            return "dnf"
        elif os.path.exists("/usr/bin/pacman"):
            return "pacman"
        elif os.path.exists("/usr/local/bin/brew"):
            return "brew"
        return "unknown"
    
    def generate_linux_script(self) -> str:
        """Generate Linux installation script."""
        pkg_manager = self.detect_package_manager()
        
        install_cmds = {
            "apt": "apt-get update && apt-get install -y python3 python3-pip",
            "yum": "yum install -y python3 python3-pip",
            "dnf": "dnf install -y python3 python3-pip",
            "pacman": "pacman -Sy python python-pip",
        }
        
        pkg_install = install_cmds.get(pkg_manager, "echo 'Package manager not detected'")
        
        return f'''#!/bin/bash
# Agent Cluster Installation Script
# Generated for {pkg_manager}-based systems

set -e

# Colors
RED='\\033[0;31m'
GREEN='\\033[0;32m'
YELLOW='\\033[1;33m'
NC='\\033[0m'

log_info() {{ echo -e "${{GREEN}}[INFO]${{NC}} $1"; }}
log_warn() {{ echo -e "${{YELLOW}}[WARN]${{NC}} $1"; }}
log_error() {{ echo -e "${{RED}}[ERROR]${{NC}} $1"; }}

# Check root
if [ "$EUID" -ne 0 ]; then 
    log_error "Please run as root"
    exit 1
fi

log_info "Installing Agent Cluster..."

# Install dependencies
log_info "Installing Python dependencies..."
{pkg_install}

# Install Python packages
log_info "Installing Python packages..."
pip3 install --upgrade pip
pip3 install agent-cluster

# Create user
if [ "{str(self.config.create_user).lower()}" = "true" ]; then
    log_info "Creating {self.config.user_name} user..."
    if ! id -u {self.config.user_name} &>/dev/null; then
        useradd -r -s /bin/false {self.config.user_name}
    fi
fi

# Create directories
if [ "{str(self.config.create_directories).lower()}" = "true" ]; then
    log_info "Creating directories..."
    mkdir -p {self.config.data_dir}
    mkdir -p {self.config.config_dir}
    mkdir -p {self.config.log_dir}
    chown -R {self.config.user_name}:{self.config.user_name} {self.config.data_dir}
    chown -R {self.config.user_name}:{self.config.user_name} {self.config.log_dir}
fi

# Install systemd service
if [ "{str(self.config.enable_service).lower()}" = "true" ]; then
    log_info "Installing systemd service..."
    cat > /etc/systemd/system/agent-cluster.service << 'EOF'
[Unit]
Description=Agent Cluster Service
After=network.target

[Service]
Type=simple
User={self.config.user_name}
Group={self.config.user_name}
WorkingDirectory={self.config.data_dir}
ExecStart=/usr/local/bin/agent-cluster
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
    
    systemctl daemon-reload
    systemctl enable agent-cluster
    
    if [ "{str(self.config.start_service).lower()}" = "true" ]; then
        systemctl start agent-cluster
    fi
fi

log_info "Installation complete!"
log_info "Configuration: {self.config.config_dir}/config.yaml"
log_info "Logs: {self.config.log_dir}/"
log_info "Status: systemctl status agent-cluster"
'''
    
    def generate_docker_compose(self) -> str:
        """Generate docker-compose.yaml."""
        return '''version: '3.8'

services:
  agent-cluster:
    image: agent-cluster:latest
    container_name: agent-cluster
    restart: unless-stopped
    ports:
      - "8080:8080"
      - "8766:8766"
    volumes:
      - ./data:/var/lib/agent-cluster
      - ./config:/etc/agent-cluster
      - ./logs:/var/log/agent-cluster
    environment:
      - CLUSTER_LOG_LEVEL=INFO
      - CLUSTER_ENABLE_AUTH=true
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s

  dashboard:
    image: agent-cluster-dashboard:latest
    container_name: agent-cluster-dashboard
    restart: unless-stopped
    ports:
      - "3000:80"
    depends_on:
      - agent-cluster

networks:
  default:
    name: agent-cluster-network
'''
    
    def generate_uninstall_script(self) -> str:
        """Generate uninstallation script."""
        return f'''#!/bin/bash
# Agent Cluster Uninstallation Script

set -e

log_info() {{ echo "[INFO] $1"; }}

# Stop service
log_info "Stopping service..."
systemctl stop agent-cluster || true
systemctl disable agent-cluster || true

# Remove service file
rm -f /etc/systemd/system/agent-cluster.service
systemctl daemon-reload

# Uninstall package
log_info "Uninstalling package..."
pip3 uninstall -y agent-cluster || true

# Remove user (optional)
read -p "Remove {self.config.user_name} user? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    userdel {self.config.user_name} || true
fi

# Remove directories (optional)
read -p "Remove data directories? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -rf {self.config.data_dir}
    rm -rf {self.config.config_dir}
    rm -rf {self.config.log_dir}
fi

log_info "Uninstallation complete."
'''
    
    def generate_health_check_script(self) -> str:
        """Generate health check script."""
        return '''#!/bin/bash
# Agent Cluster Health Check

URL="${1:-http://localhost:8080/health}"
TIMEOUT="${2:-10}"

check_health() {
    if curl -sf --max-time "$TIMEOUT" "$URL" > /dev/null; then
        echo "healthy"
        exit 0
    else
        echo "unhealthy"
        exit 1
    fi
}

check_health
'''
    
    def generate_backup_script(self) -> str:
        """Generate backup script."""
        return f'''#!/bin/bash
# Agent Cluster Backup Script

BACKUP_DIR="${{1:-/backup/agent-cluster}}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_PATH="$BACKUP_DIR/agent-cluster-$TIMESTAMP"

mkdir -p "$BACKUP_PATH"

# Backup configuration
echo "Backing up configuration..."
cp -r {self.config.config_dir} "$BACKUP_PATH/config"

# Backup data
echo "Backing up data..."
cp -r {self.config.data_dir} "$BACKUP_PATH/data"

# Compress
echo "Compressing backup..."
tar -czf "$BACKUP_PATH.tar.gz" -C "$BACKUP_DIR" "agent-cluster-$TIMESTAMP"
rm -rf "$BACKUP_PATH"

echo "Backup created: $BACKUP_PATH.tar.gz"
'''
    
    def write_scripts(self, output_dir: str = "scripts"):
        """Write all scripts to directory."""
        os.makedirs(output_dir, exist_ok=True)
        
        scripts = {
            "install.sh": self.generate_linux_script(),
            "docker-compose.yaml": self.generate_docker_compose(),
            "uninstall.sh": self.generate_uninstall_script(),
            "health-check.sh": self.generate_health_check_script(),
            "backup.sh": self.generate_backup_script(),
        }
        
        for filename, content in scripts.items():
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w') as f:
                f.write(content)
            os.chmod(filepath, 0o755)
        
        print(f"Scripts written to {output_dir}/")
