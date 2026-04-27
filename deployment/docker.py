"""
Docker Image Builder

Creates optimized Docker images for Agent Cluster:
- Minimal base images (python:3.8-slim)
- Multi-stage builds
- Layer caching
- Size optimization (<100MB target)
"""

import os
import subprocess
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from pathlib import Path


@dataclass
class DockerConfig:
    """Docker image configuration."""
    image_name: str = "agent-cluster"
    image_tag: str = "latest"
    registry: Optional[str] = None
    
    base_image: str = "python:3.8-slim"
    maintainer: str = "Agent Cluster Team"
    multi_stage: bool = True
    non_root_user: bool = True
    user_name: str = "agent"
    python_version: str = "3.8"
    requirements_file: str = "requirements.txt"
    context_dir: str = "."
    build_args: Dict[str, str] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=dict)
    max_image_size_mb: int = 100


DOCKERFILE_TEMPLATE = '''# Agent Cluster - Optimized Docker Image
FROM python:{python_version}-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc-dev && rm -rf /var/lib/apt/lists/*
COPY {requirements_file} .
RUN pip install --no-cache-dir --target=/build/deps -r {requirements_file}

FROM python:{python_version}-slim
{labels}
WORKDIR /app
{user_creation}
COPY --from=builder /build/deps /usr/local/lib/python{python_version}/site-packages/
COPY . /app/
RUN chown -R {user_name}:{user_name} /app
USER {user_name}
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=10s CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" || exit 1
ENTRYPOINT ["python", "-m", "agent_cluster.main"]
'''


class DockerBuilder:
    """Builds optimized Docker images."""
    
    def __init__(self, config: DockerConfig = None):
        self.config = config or DockerConfig()
    
    def generate_dockerfile(self) -> str:
        """Generate Dockerfile from template."""
        labels = "\n".join(f'LABEL {k}="{v}"' for k, v in {"maintainer": self.config.maintainer, **self.config.labels}.items())
        user_creation = f'RUN groupadd -r {self.config.user_name} && useradd -r -g {self.config.user_name} {self.config.user_name}' if self.config.non_root_user else "# Running as root"
        
        return DOCKERFILE_TEMPLATE.format(
            python_version=self.config.python_version,
            requirements_file=self.config.requirements_file,
            labels=labels,
            user_creation=user_creation,
            user_name=self.config.user_name,
        )
    
    def write_dockerfile(self, output_path: str = "Dockerfile"):
        """Write Dockerfile to disk."""
        with open(output_path, 'w') as f:
            f.write(self.generate_dockerfile())
    
    def build(self, tag: str = None, build_args: Dict[str, str] = None) -> bool:
        """Build Docker image."""
        tag = tag or f"{self.config.image_name}:{self.config.image_tag}"
        cmd = ["docker", "build", "-t", tag]
        
        for k, v in {**self.config.build_args, **(build_args or {})}.items():
            cmd.extend(["--build-arg", f"{k}={v}"])
        
        cmd.append(self.config.context_dir)
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except FileNotFoundError:
            print("Docker not found")
            return False
    
    def get_image_size(self, tag: str = None) -> int:
        """Get image size in MB."""
        tag = tag or f"{self.config.image_name}:{self.config.image_tag}"
        try:
            result = subprocess.run(["docker", "image", "inspect", tag, "--format", "{{.Size}}"], capture_output=True, text=True)
            return int(result.stdout.strip()) // (1024 * 1024) if result.returncode == 0 else -1
        except:
            return -1
    
    def check_size(self, tag: str = None) -> bool:
        """Check if image meets size requirement."""
        size = self.get_image_size(tag)
        print(f"Image size: {size}MB (max: {self.config.max_image_size_mb}MB)")
        return 0 < size <= self.config.max_image_size_mb
    
    def run(self, tag: str = None, port: int = 8080, detach: bool = True) -> Optional[str]:
        """Run Docker container."""
        tag = tag or f"{self.config.image_name}:{self.config.image_tag}"
        cmd = ["docker", "run", "-d" if detach else "-it", "-p", f"{port}:8080", tag]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else None


class PipPackage:
    """Creates pip-installable package."""
    
    def __init__(self, config: "PackageConfig" = None):
        self.config = config or PackageConfig()
    
    def create_setup(self) -> str:
        """Generate setup.py content."""
        return f'''"""Setup for {self.config.name}"""
from setuptools import setup, find_packages

setup(
    name="{self.config.name}",
    version="{self.config.version}",
    author="{self.config.author}",
    author_email="{self.config.email}",
    description="{self.config.description}",
    packages=find_packages(exclude=["tests*"]),
    python_requires="{self.config.python_requires}",
    install_requires={self.config.install_requires},
    entry_points={{
        "console_scripts": [
            "agent-cluster=agent_cluster.main:main",
        ]
    }},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
'''
    
    def create_manifest(self) -> str:
        """Generate MANIFEST.in content."""
        return f'''include README.md
include LICENSE
include requirements.txt
recursive-include agent_cluster *.py
'''
    
    def build(self, dist_dir: str = "dist"):
        """Build package."""
        os.makedirs(dist_dir, exist_ok=True)
        
        # Create setup files
        with open("setup.py", 'w') as f:
            f.write(self.create_setup())
        
        with open("MANIFEST.in", 'w') as f:
            f.write(self.create_manifest())
        
        # Run build
        subprocess.run(["python", "-m", "build", "--outdir", dist_dir], capture_output=True)
    
    def install(self):
        """Install package locally."""
        subprocess.run(["pip", "install", "-e", "."])
    
    def upload(self, repository: str = "pypi"):
        """Upload to PyPI."""
        subprocess.run(["twine", "upload", "--repository", repository, "dist/*"])


@dataclass
class PackageConfig:
    """pip package configuration."""
    name: str = "agent-cluster"
    version: str = "1.0.0"
    author: str = "Agent Cluster Team"
    email: str = "team@agent-cluster.io"
    description: str = "Distributed agent cluster framework"
    python_requires: str = ">=3.6"
    install_requires: List[str] = field(default_factory=lambda: [])


class BinaryBuilder:
    """Creates standalone binary executables."""
    
    def __init__(self, config: "BinaryConfig" = None):
        self.config = config or BinaryConfig()
    
    def build(self, output_dir: str = "dist"):
        """Build binary using PyInstaller."""
        os.makedirs(output_dir, exist_ok=True)
        
        cmd = [
            "pyinstaller",
            "--onefile",
            "--name", self.config.name,
            "--distpath", output_dir,
            "--workpath", "build",
            "--specpath", ".",
        ]
        
        if self.config.console:
            cmd.append("--console")
        else:
            cmd.append("--noconsole")
        
        for hidden in self.config.hidden_imports:
            cmd.extend(["--hidden-import", hidden])
        
        for data in self.config.datas:
            cmd.extend(["--add-data", data])
        
        cmd.append(self.config.entry_point)
        
        subprocess.run(cmd)
    
    def get_size(self) -> int:
        """Get binary size in MB."""
        path = Path("dist") / self.config.name
        return path.stat().st_size // (1024 * 1024) if path.exists() else -1


@dataclass
class BinaryConfig:
    """Binary build configuration."""
    name: str = "agent-cluster"
    entry_point: str = "agent_cluster/main.py"
    console: bool = True
    hidden_imports: List[str] = field(default_factory=list)
    datas: List[str] = field(default_factory=list)


class AndroidPackager:
    """Packages Python app for Android (using Chaquopy/Python-for-Android)."""
    
    def __init__(self, config: "AndroidConfig" = None):
        self.config = config or AndroidConfig()
    
    def create_buildozer_spec(self) -> str:
        """Generate buildozer.spec for Python-for-Android."""
        return f'''[app]
title = {self.config.app_name}
package.name = {self.config.package_name}
package.domain = {self.config.package_domain}
source.dir = {self.config.source_dir}
source.include_exts = py,png,jpg,kv,atlas,json
version = {self.config.version}
requirements = python3,kivy,{','.join(self.config.requirements)}
orientation = {self.config.orientation}
fullscreen = 0
android.permissions = {','.join(self.config.permissions)}
android.api = {self.config.android_api}
android.minapi = {self.config.min_api}
android.ndk = {self.config.ndk_version}
android.sdk = {self.config.sdk_version}
'''
    
    def build(self):
        """Build APK using buildozer."""
        with open("buildozer.spec", 'w') as f:
            f.write(self.create_buildozer_spec())
        
        subprocess.run(["buildozer", "android", "debug"])
    
    def get_apk_size(self) -> int:
        """Get APK size in MB."""
        apk_path = Path("bin") / f"{self.config.app_name}-{self.config.version}-debug.apk"
        return apk_path.stat().st_size // (1024 * 1024) if apk_path.exists() else -1


@dataclass
class AndroidConfig:
    """Android APK configuration."""
    app_name: str = "AgentCluster"
    package_name: str = "agentcluster"
    package_domain: str = "io.agentcluster"
    source_dir: str = "."
    version: str = "1.0.0"
    requirements: List[str] = field(default_factory=lambda: ["requests"])
    orientation: str = "portrait"
    permissions: List[str] = field(default_factory=lambda: ["INTERNET", "ACCESS_NETWORK_STATE"])
    android_api: int = 33
    min_api: int = 21
    ndk_version: str = "25b"
    sdk_version: str = "33"
