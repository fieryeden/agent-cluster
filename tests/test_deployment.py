"""
Tests for Deployment Module

Tests for Docker configuration, pip packaging, and installation.
"""

import sys
import os
import unittest
import tempfile
import shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from deployment import (
    DockerConfig,
    PipPackage,
    Installer,
)


class TestDockerConfig(unittest.TestCase):
    """Test Docker configuration generation."""

    def test_create_config(self):
        """Should create Docker config."""
        config = DockerConfig()
        self.assertIsNotNone(config)

    def test_config_attributes(self):
        """Should have expected attributes."""
        config = DockerConfig()
        self.assertTrue(hasattr(config, 'base_image'))


class TestPipPackage(unittest.TestCase):
    """Test pip package configuration."""

    def test_create_package(self):
        """Should create package config."""
        pkg = PipPackage()
        self.assertIsNotNone(pkg)

    def test_create_setup(self):
        """Should generate setup.py content."""
        pkg = PipPackage()
        setup_content = pkg.create_setup()
        self.assertIn("setup(", setup_content)


class TestInstaller(unittest.TestCase):
    """Test installer generation."""

    def test_create_installer(self):
        """Should create installer."""
        installer = Installer()
        self.assertIsNotNone(installer)

    def test_generate_linux_script(self):
        """Should generate install script."""
        installer = Installer()
        script = installer.generate_linux_script()
        self.assertIn("#!/bin/bash", script)

    def test_generate_uninstall_script(self):
        """Should generate uninstall script."""
        installer = Installer()
        script = installer.generate_uninstall_script()
        self.assertIn("#!/bin/bash", script)


if __name__ == '__main__':
    unittest.main()
