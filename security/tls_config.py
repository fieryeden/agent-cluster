"""
TLS Configuration for Secure Connections

Provides TLS/SSL setup for:
- Server certificates
- Client certificates (mTLS)
- Certificate generation
- Certificate validation
"""

import os
import ssl
import socket
import tempfile
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime, timedelta


@dataclass
class TLSCertificate:
    """TLS certificate configuration."""
    cert_path: str
    key_path: str
    ca_path: Optional[str] = None
    
    # Certificate details
    common_name: str = "agent-cluster"
    organization: str = "Agent Cluster"
    country: str = "US"
    
    # Validity
    days_valid: int = 365
    
    # Extensions
    san_dns: List[str] = field(default_factory=list)  # Subject Alternative Names
    san_ip: List[str] = field(default_factory=list)
    
    def exists(self) -> bool:
        """Check if certificate files exist."""
        return (
            os.path.exists(self.cert_path) and
            os.path.exists(self.key_path)
        )


@dataclass
class TLSConfig:
    """TLS configuration for server/client."""
    enabled: bool = True
    
    # Server config
    cert_file: Optional[str] = None
    key_file: Optional[str] = None
    ca_file: Optional[str] = None
    
    # Client config (for mTLS)
    client_cert_required: bool = False
    
    # Protocol settings
    min_version: str = "TLSv1.2"
    verify_mode: str = "REQUIRED"  # NONE, OPTIONAL, REQUIRED
    
    # Cipher settings
    cipher_string: str = "HIGH:!aNULL:!MD5:!3DES"
    
    # Performance
    session_timeout: int = 300
    session_cache_size: int = 512
    
    def to_ssl_context(self, server_side: bool = True) -> ssl.SSLContext:
        """Create SSL context from configuration."""
        protocol = {
            "TLSv1.0": ssl.PROTOCOL_TLS,
            "TLSv1.1": ssl.PROTOCOL_TLS,
            "TLSv1.2": ssl.PROTOCOL_TLS,
            "TLSv1.3": ssl.PROTOCOL_TLS,
        }.get(self.min_version, ssl.PROTOCOL_TLS)
        
        context = ssl.SSLContext(protocol)
        
        # Set minimum version
        if self.min_version == "TLSv1.2":
            context.minimum_version = ssl.TLSVersion.TLSv1_2
        elif self.min_version == "TLSv1.3":
            context.minimum_version = ssl.TLSVersion.TLSv1_3
        
        # Load certificates
        if self.cert_file and self.key_file:
            context.load_cert_chain(self.cert_file, self.key_file)
        
        # CA verification
        if self.ca_file:
            context.load_verify_locations(self.ca_file)
        
        # Verify mode
        if server_side:
            if self.client_cert_required:
                context.verify_mode = ssl.CERT_REQUIRED
            else:
                context.verify_mode = ssl.CERT_OPTIONAL
        else:
            if self.verify_mode == "REQUIRED":
                context.verify_mode = ssl.CERT_REQUIRED
            elif self.verify_mode == "OPTIONAL":
                context.verify_mode = ssl.CERT_OPTIONAL
            else:
                context.verify_mode = ssl.CERT_NONE
        
        # Set ciphers
        context.set_ciphers(self.cipher_string)
        
        # Session settings
        context.session_timeout = self.session_timeout
        
        return context


class TLSManager:
    """
    Manages TLS certificates and configuration.
    
    Usage:
        manager = TLSManager()
        
        # Generate self-signed certificate
        cert = manager.generate_self_signed(
            common_name="coordinator.local",
            san_dns=["localhost", "*.local"],
        )
        
        # Create SSL context for server
        context = manager.create_server_context(cert)
        
        # Use in socket
        with socket.socket() as sock:
            ssl_sock = context.wrap_socket(sock, server_side=True)
    """
    
    def __init__(self, certs_dir: str = "/tmp/agent_cluster/certs"):
        self.certs_dir = Path(certs_dir)
        self.certs_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_self_signed(
        self,
        common_name: str = "agent-cluster",
        organization: str = "Agent Cluster",
        country: str = "US",
        days_valid: int = 365,
        san_dns: List[str] = None,
        san_ip: List[str] = None,
        output_dir: str = None,
    ) -> TLSCertificate:
        """
        Generate self-signed certificate.
        
        Args:
            common_name: Certificate CN
            organization: Organization name
            country: Country code
            days_valid: Days certificate is valid
            san_dns: Subject Alternative Names (DNS)
            san_ip: Subject Alternative Names (IP)
            output_dir: Directory to save certificate
            
        Returns:
            TLSCertificate configuration
        """
        import subprocess
        
        output_dir = Path(output_dir or self.certs_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        cert_path = output_dir / f"{common_name}.crt"
        key_path = output_dir / f"{common_name}.key"
        
        # Build subject
        subject = f"/C={country}/O={organization}/CN={common_name}"
        
        # Build SAN extension
        san_ext = []
        if san_dns:
            san_ext.extend(f"DNS:{name}" for name in san_dns)
        if san_ip:
            san_ext.extend(f"IP:{ip}" for ip in san_ip)
        san_ext.append("DNS:localhost")
        san_ext.append("IP:127.0.0.1")
        
        san_string = ",".join(san_ext)
        
        # Generate key and certificate
        cmd = [
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", str(key_path),
            "-out", str(cert_path),
            "-days", str(days_valid),
            "-nodes",
            "-subj", subject,
            "-addext", f"subjectAltName={san_string}",
        ]
        
        try:
            subprocess.run(cmd, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            # Fallback: create simple cert without SAN if openssl fails
            # This handles cases where openssl version doesn't support -addext
            cmd = [
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", str(key_path),
                "-out", str(cert_path),
                "-days", str(days_valid),
                "-nodes",
                "-subj", subject,
            ]
            subprocess.run(cmd, check=True, capture_output=True)
        
        # Set permissions
        os.chmod(key_path, 0o600)
        os.chmod(cert_path, 0o644)
        
        return TLSCertificate(
            cert_path=str(cert_path),
            key_path=str(key_path),
            common_name=common_name,
            organization=organization,
            country=country,
            days_valid=days_valid,
            san_dns=san_dns or [],
            san_ip=san_ip or [],
        )
    
    def create_ca(
        self,
        common_name: str = "agent-cluster-ca",
        organization: str = "Agent Cluster CA",
        days_valid: int = 3650,
    ) -> TLSCertificate:
        """Create Certificate Authority."""
        import subprocess
        
        ca_cert = self.certs_dir / f"{common_name}.crt"
        ca_key = self.certs_dir / f"{common_name}.key"
        
        # Generate CA key and certificate
        cmd = [
            "openssl", "req", "-x509", "-newkey", "rsa:4096",
            "-keyout", str(ca_key),
            "-out", str(ca_cert),
            "-days", str(days_valid),
            "-nodes",
            "-subj", f"/C=US/O={organization}/CN={common_name}",
        ]
        
        subprocess.run(cmd, check=True, capture_output=True)
        
        os.chmod(ca_key, 0o600)
        
        return TLSCertificate(
            cert_path=str(ca_cert),
            key_path=str(ca_key),
            common_name=common_name,
            organization=organization,
        )
    
    def sign_certificate_request(
        self,
        csr_path: str,
        ca_cert: TLSCertificate,
        days_valid: int = 365,
    ) -> str:
        """Sign a certificate request with CA."""
        import subprocess
        
        cert_path = self.certs_dir / f"signed_{os.path.basename(csr_path).replace('.csr', '.crt')}"
        
        cmd = [
            "openssl", "x509", "-req",
            "-in", csr_path,
            "-CA", ca_cert.cert_path,
            "-CAkey", ca_cert.key_path,
            "-CAcreateserial",
            "-out", str(cert_path),
            "-days", str(days_valid),
        ]
        
        subprocess.run(cmd, check=True, capture_output=True)
        
        return str(cert_path)
    
    def create_server_context(
        self,
        cert: TLSCertificate,
        require_client_cert: bool = False,
        ca_cert: TLSCertificate = None,
    ) -> ssl.SSLContext:
        """Create SSL context for server."""
        config = TLSConfig(
            enabled=True,
            cert_file=cert.cert_path,
            key_file=cert.key_path,
            ca_file=ca_cert.cert_path if ca_cert else None,
            client_cert_required=require_client_cert,
        )
        return config.to_ssl_context(server_side=True)
    
    def create_client_context(
        self,
        cert: TLSCertificate = None,
        ca_cert: TLSCertificate = None,
        verify_server: bool = True,
    ) -> ssl.SSLContext:
        """Create SSL context for client."""
        config = TLSConfig(
            enabled=True,
            cert_file=cert.cert_path if cert else None,
            key_file=cert.key_path if cert else None,
            ca_file=ca_cert.cert_path if ca_cert else None,
            verify_mode="REQUIRED" if verify_server else "NONE",
        )
        return config.to_ssl_context(server_side=False)
    
    def verify_certificate(
        self,
        cert_path: str,
        ca_path: str,
    ) -> bool:
        """Verify certificate against CA."""
        import subprocess
        
        try:
            result = subprocess.run(
                ["openssl", "verify", "-CAfile", ca_path, cert_path],
                capture_output=True,
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def get_certificate_info(self, cert_path: str) -> Dict[str, Any]:
        """Get certificate information."""
        import subprocess
        
        try:
            result = subprocess.run(
                ["openssl", "x509", "-in", cert_path, "-text", "-noout"],
                capture_output=True,
                text=True,
            )
            
            if result.returncode != 0:
                return {}
            
            # Parse basic info
            output = result.stdout
            info = {}
            
            for line in output.split('\n'):
                if 'Subject:' in line:
                    info['subject'] = line.split('Subject:')[1].strip()
                elif 'Issuer:' in line:
                    info['issuer'] = line.split('Issuer:')[1].strip()
                elif 'Not Before:' in line:
                    info['not_before'] = line.split('Not Before:')[1].strip()
                elif 'Not After :' in line:
                    info['not_after'] = line.split('Not After :')[1].strip()
            
            return info
            
        except Exception:
            return {}
