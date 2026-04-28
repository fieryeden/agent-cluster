"""
Deployment Configuration Management

Handles configuration for deployment:
- Environment-specific configs
- Configuration validation
- Secret injection
- Configuration encryption
"""

import os
import json
import yaml
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, List
from pathlib import Path
from enum import Enum


class Environment(Enum):
    """Deployment environments."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class DeploymentConfig:
    """Main deployment configuration."""
    # Environment
    environment: Environment = Environment.DEVELOPMENT
    
    # Application
    app_name: str = "agent-cluster"
    version: str = "1.0.0"
    log_level: str = "INFO"
    
    # Network
    host: str = "0.0.0.0"
    port: int = 8080
    websocket_port: int = 8766
    
    # Security
    enable_auth: bool = True
    require_tls: bool = False
    jwt_expiry_seconds: int = 3600
    
    # Reliability
    circuit_breaker_enabled: bool = True
    retry_max_attempts: int = 3
    timeout_seconds: float = 30.0
    rate_limit_per_second: float = 100.0
    
    # Storage
    data_dir: str = "/var/lib/agent-cluster"
    dlq_dir: str = "/var/lib/agent-cluster/dlq"
    audit_log: str = "/var/log/agent-cluster/audit.log"
    
    # Agents
    max_agents: int = 100
    heartbeat_interval: float = 300.0
    agent_timeout: float = 300.0
    
    # Tasks
    max_queue_size: int = 10000
    max_retries: int = 3
    task_timeout: float = 300.0
    
    # Resources
    max_memory_mb: int = 1024
    max_cpu_percent: float = 80.0
    max_file_size_mb: int = 100
    
    # Feature flags
    enable_dashboard: bool = True
    enable_metrics: bool = True
    enable_audit: bool = True
    enable_health_check: bool = True


# Environment-specific defaults
ENVIRONMENT_CONFIGS = {
    Environment.DEVELOPMENT: DeploymentConfig(
        environment=Environment.DEVELOPMENT,
        log_level="DEBUG",
        enable_auth=False,
        require_tls=False,
    ),
    Environment.STAGING: DeploymentConfig(
        environment=Environment.STAGING,
        log_level="INFO",
        enable_auth=True,
        require_tls=False,
    ),
    Environment.PRODUCTION: DeploymentConfig(
        environment=Environment.PRODUCTION,
        log_level="WARNING",
        enable_auth=True,
        require_tls=True,
    ),
}


class ConfigManager:
    """
    Manages deployment configuration.
    
    Usage:
        manager = ConfigManager()
        
        # Load from file
        config = manager.load("config.yaml")
        
        # Load with environment
        config = manager.load_for_environment(Environment.PRODUCTION)
        
        # Validate and save
        manager.validate(config)
        manager.save(config, "config.yaml")
    """
    
    def __init__(self, config_dir: str = None):
        self.config_dir = Path(config_dir or "/etc/agent-cluster")
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        self._config: Optional[DeploymentConfig] = None
        self._overrides: Dict[str, Any] = {}
    
    def load(self, filepath: str = None) -> DeploymentConfig:
        """Load configuration from file."""
        if filepath is None:
            filepath = self.config_dir / "config.yaml"
        else:
            filepath = Path(filepath)
        
        if not filepath.exists():
            return self.get_defaults()
        
        # Read file
        with open(filepath) as f:
            if filepath.suffix in ['.yaml', '.yml']:
                data = yaml.safe_load(f)
            else:
                data = json.load(f)
        
        # Parse environment
        if 'environment' in data:
            data['environment'] = Environment(data['environment'])
        
        # Create config
        self._config = DeploymentConfig(**{
            k: v for k, v in data.items()
            if k in DeploymentConfig.__dataclass_fields__
        })
        
        return self._config
    
    def load_for_environment(
        self,
        environment: Environment,
        filepath: str = None,
    ) -> DeploymentConfig:
        """Load configuration for specific environment."""
        # Start with environment defaults
        base_config = ENVIRONMENT_CONFIGS.get(environment, DeploymentConfig())
        
        # Load file if provided
        if filepath and Path(filepath).exists():
            file_config = self.load(filepath)
            # Merge file config over base
            base_config = DeploymentConfig(**{
                **asdict(base_config),
                **{k: v for k, v in asdict(file_config).items() if v is not None},
            })
        
        self._config = base_config
        return self._config
    
    def get_defaults(self) -> DeploymentConfig:
        """Get default configuration."""
        return DeploymentConfig()
    
    def save(self, config: DeploymentConfig, filepath: str = None):
        """Save configuration to file."""
        filepath = Path(filepath or self.config_dir / "config.yaml")
        
        data = asdict(config)
        data['environment'] = data['environment'].value
        
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w') as f:
            if filepath.suffix in ['.yaml', '.yml']:
                yaml.dump(data, f, default_flow_style=False)
            else:
                json.dump(data, f, indent=2)
    
    def validate(self, config: DeploymentConfig) -> List[str]:
        """Validate configuration, return list of errors."""
        errors = []
        
        # Validate port
        if not (1 <= config.port <= 65535):
            errors.append(f"Invalid port: {config.port}")
        
        # Validate timeout
        if config.timeout_seconds <= 0:
            errors.append(f"Invalid timeout: {config.timeout_seconds}")
        
        # Validate paths
        for path_attr in ['data_dir', 'dlq_dir', 'audit_log']:
            path = getattr(config, path_attr)
            if path and not os.path.isabs(path):
                errors.append(f"{path_attr} should be absolute: {path}")
        
        # Validate resources
        if config.max_memory_mb <= 0:
            errors.append(f"Invalid max_memory_mb: {config.max_memory_mb}")
        
        # Production-specific validation
        if config.environment == Environment.PRODUCTION:
            if not config.enable_auth:
                errors.append("Production requires enable_auth=True")
            if not config.require_tls:
                errors.append("Production requires require_tls=True")
        
        return errors
    
    def apply_environment_overrides(self, config: DeploymentConfig) -> DeploymentConfig:
        """Apply environment variable overrides."""
        overrides = {}
        
        # Map of env var to config key
        env_mapping = {
            'CLUSTER_HOST': 'host',
            'CLUSTER_PORT': ('port', int),
            'CLUSTER_WS_PORT': ('websocket_port', int),
            'CLUSTER_LOG_LEVEL': 'log_level',
            'CLUSTER_ENABLE_AUTH': ('enable_auth', lambda x: x.lower() == 'true'),
            'CLUSTER_DATA_DIR': 'data_dir',
        }
        
        for env_var, config_key in env_mapping.items():
            value = os.environ.get(env_var)
            if value is not None:
                if isinstance(config_key, tuple):
                    key, converter = config_key
                    overrides[key] = converter(value)
                else:
                    overrides[config_key] = value
        
        if overrides:
            current = asdict(config)
            current.update(overrides)
            return DeploymentConfig(**current)
        
        return config
    
    def get_config(self) -> DeploymentConfig:
        """Get current configuration."""
        if self._config is None:
            self._config = self.load()
        return self._config
    
    def get_secrets_config(self) -> Dict[str, str]:
        """Get secrets configuration (paths to secret files)."""
        return {
            'jwt_secret': str(self.config_dir / "secrets" / "jwt_secret"),
            'api_keys': str(self.config_dir / "secrets" / "api_keys.json"),
            'tls_cert': str(self.config_dir / "tls" / "server.crt"),
            'tls_key': str(self.config_dir / "tls" / "server.key"),
        }
    
    def export_env_file(self, config: DeploymentConfig, filepath: str = None):
        """Export configuration as environment file."""
        filepath = Path(filepath or self.config_dir / "agent-cluster.env")
        
        data = asdict(config)
        data['environment'] = data['environment'].value
        
        with open(filepath, 'w') as f:
            for key, value in data.items():
                env_key = f"CLUSTER_{key.upper()}"
                f.write(f"{env_key}={value}\n")
    
    def export_systemd_unit(self, config: DeploymentConfig) -> str:
        """Generate systemd unit file."""
        return f'''[Unit]
Description=Agent Cluster Service
After=network.target

[Service]
Type=simple
User=agent-cluster
Group=agent-cluster
WorkingDirectory={config.data_dir}
ExecStart=/usr/bin/python -m agent_cluster.main --config {self.config_dir}/config.yaml
Restart=always
RestartSec=10

Environment=CLUSTER_ENV={config.environment.value}
Environment=CLUSTER_PORT={config.port}

[Install]
WantedBy=multi-user.target
'''
