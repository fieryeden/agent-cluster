"""
System operation handlers.

Handlers for command execution, process management, system info.
"""

import os
import sys
import time
import subprocess
import shutil
import platform
import signal
from typing import Dict, Any, List, Optional
from pathlib import Path

from handlers.base import (
    TaskHandler,
    HandlerResult,
    HandlerConfidence,
)


class SystemExecHandler(TaskHandler):
    """Execute shell command with timeout."""
    
    name = "system_exec"
    category = "system"
    description = "Execute shell command with timeout and output capture"
    dangerous = True
    
    input_schema = {
        'command': {'type': 'string', 'required': True, 'description': 'Command to execute'},
        'shell': {'type': 'boolean', 'default': True, 'description': 'Run through shell'},
        'timeout': {'type': 'integer', 'default': 60, 'description': 'Timeout in seconds'},
        'cwd': {'type': 'string', 'description': 'Working directory'},
        'env': {'type': 'object', 'description': 'Environment variables'},
        'input': {'type': 'string', 'description': 'Stdin input'},
        'check': {'type': 'boolean', 'default': False, 'description': 'Raise on non-zero exit'},
    }
    
    output_schema = {
        'returncode': {'type': 'integer', 'description': 'Exit code'},
        'stdout': {'type': 'string', 'description': 'Standard output'},
        'stderr': {'type': 'string', 'description': 'Standard error'},
        'elapsed_seconds': {'type': 'float', 'description': 'Execution time'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['system_exec', 'exec', 'run', 'shell', 'cmd', 'command']:
            return HandlerConfidence.PERFECT.value
        if 'command' in params:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        command = params['command']
        use_shell = params.get('shell', True)
        timeout = params.get('timeout', 60)
        cwd = params.get('cwd')
        env = params.get('env')
        stdin_data = params.get('input')
        check = params.get('check', False)
        
        try:
            # Prepare environment
            run_env = os.environ.copy()
            if env:
                run_env.update(env)
            
            # Execute command
            result = subprocess.run(
                command if use_shell else command.split(),
                shell=use_shell,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=cwd,
                env=run_env,
                input=stdin_data,
            )
            
            elapsed = time.time() - start_time
            
            if check and result.returncode != 0:
                return HandlerResult(
                    success=False,
                    error=f"Command failed with exit code {result.returncode}",
                    data={
                        'returncode': result.returncode,
                        'stdout': result.stdout,
                        'stderr': result.stderr,
                        'elapsed_seconds': elapsed,
                    },
                    duration_ms=elapsed * 1000,
                )
            
            return HandlerResult(
                success=True,
                data={
                    'returncode': result.returncode,
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'elapsed_seconds': elapsed,
                },
                duration_ms=elapsed * 1000,
            )
            
        except subprocess.TimeoutExpired:
            return HandlerResult(
                success=False,
                error=f"Command timed out after {timeout} seconds",
                duration_ms=(time.time() - start_time) * 1000,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class SystemInfoHandler(TaskHandler):
    """Get system information."""
    
    name = "system_info"
    category = "system"
    description = "Get system information (OS, CPU, memory, disk)"
    
    input_schema = {
        'info_type': {'type': 'string', 'default': 'all', 'enum': ['all', 'os', 'cpu', 'memory', 'disk', 'network']},
    }
    
    output_schema = {
        'os': {'type': 'object', 'description': 'OS information'},
        'cpu': {'type': 'object', 'description': 'CPU information'},
        'memory': {'type': 'object', 'description': 'Memory information'},
        'disk': {'type': 'object', 'description': 'Disk information'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['system_info', 'sysinfo', 'system', 'info', 'uname']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        info_type = params.get('info_type', 'all')
        
        result = {}
        
        try:
            if info_type in ['all', 'os']:
                result['os'] = {
                    'system': platform.system(),
                    'node': platform.node(),
                    'release': platform.release(),
                    'version': platform.version(),
                    'machine': platform.machine(),
                    'processor': platform.processor(),
                    'python_version': platform.python_version(),
                }
            
            if info_type in ['all', 'cpu']:
                try:
                    import psutil
                    result['cpu'] = {
                        'count': psutil.cpu_count(),
                        'count_logical': psutil.cpu_count(logical=True),
                        'percent': psutil.cpu_percent(interval=1),
                        'freq': psutil.cpu_freq()._asdict() if psutil.cpu_freq() else None,
                    }
                except ImportError:
                    result['cpu'] = {
                        'count': os.cpu_count(),
                        'architecture': platform.machine(),
                    }
            
            if info_type in ['all', 'memory']:
                try:
                    import psutil
                    mem = psutil.virtual_memory()
                    result['memory'] = {
                        'total': mem.total,
                        'available': mem.available,
                        'used': mem.used,
                        'percent': mem.percent,
                    }
                except ImportError:
                    result['memory'] = {'error': 'psutil not available'}
            
            if info_type in ['all', 'disk']:
                try:
                    import psutil
                    disk_usage = psutil.disk_usage('/')
                    result['disk'] = {
                        'total': disk_usage.total,
                        'used': disk_usage.used,
                        'free': disk_usage.free,
                        'percent': disk_usage.percent,
                    }
                except ImportError:
                    result['disk'] = {
                        'total': shutil.disk_usage('/').total if hasattr(shutil, 'disk_usage') else None,
                    }
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data=result,
                duration_ms=duration_ms,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class SystemEnvHandler(TaskHandler):
    """Get or set environment variables."""
    
    name = "system_env"
    category = "system"
    description = "Get or set environment variables"
    
    input_schema = {
        'action': {'type': 'string', 'default': 'get', 'enum': ['get', 'set', 'unset', 'list']},
        'name': {'type': 'string', 'description': 'Variable name'},
        'value': {'type': 'string', 'description': 'Variable value (for set)'},
    }
    
    output_schema = {
        'name': {'type': 'string'},
        'value': {'type': 'string'},
        'variables': {'type': 'object', 'description': 'For list action'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['system_env', 'env', 'environment', 'getenv', 'setenv']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        action = params.get('action', 'get')
        name = params.get('name')
        value = params.get('value')
        
        try:
            if action == 'get':
                if not name:
                    return HandlerResult(success=False, error="Name required for get action")
                return HandlerResult(
                    success=True,
                    data={'name': name, 'value': os.environ.get(name)},
                )
            
            elif action == 'set':
                if not name or value is None:
                    return HandlerResult(success=False, error="Name and value required for set action")
                os.environ[name] = value
                return HandlerResult(
                    success=True,
                    data={'name': name, 'value': value},
                )
            
            elif action == 'unset':
                if not name:
                    return HandlerResult(success=False, error="Name required for unset action")
                if name in os.environ:
                    del os.environ[name]
                return HandlerResult(
                    success=True,
                    data={'name': name, 'value': None},
                )
            
            elif action == 'list':
                return HandlerResult(
                    success=True,
                    data={'variables': dict(os.environ)},
                )
            
            duration_ms = (time.time() - start_time) * 1000
            return HandlerResult(success=False, error=f"Unknown action: {action}")
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class SystemProcessHandler(TaskHandler):
    """Manage processes (list, kill, etc.)."""
    
    name = "system_process"
    category = "system"
    description = "Manage processes (list, kill, find)"
    dangerous = True
    
    input_schema = {
        'action': {'type': 'string', 'default': 'list', 'enum': ['list', 'find', 'kill', 'info']},
        'pid': {'type': 'integer', 'description': 'Process ID'},
        'name': {'type': 'string', 'description': 'Process name (for find/kill)'},
        'signal': {'type': 'string', 'default': 'TERM', 'description': 'Signal for kill (TERM, KILL, INT)'},
    }
    
    output_schema = {
        'processes': {'type': 'array', 'description': 'Process list'},
        'pid': {'type': 'integer', 'description': 'Affected PID'},
        'killed': {'type': 'boolean'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['system_process', 'process', 'ps', 'kill', 'pkill']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        action = params.get('action', 'list')
        pid = params.get('pid')
        name = params.get('name')
        sig_name = params.get('signal', 'TERM')
        
        try:
            # Try psutil first
            try:
                import psutil
                
                if action == 'list':
                    processes = []
                    for p in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent', 'status']):
                        processes.append(p.info)
                    return HandlerResult(
                        success=True,
                        data={'processes': processes},
                    )
                
                elif action == 'find':
                    if not name:
                        return HandlerResult(success=False, error="Name required for find action")
                    found = []
                    for p in psutil.process_iter(['pid', 'name']):
                        if name.lower() in p.info['name'].lower():
                            found.append(p.info)
                    return HandlerResult(
                        success=True,
                        data={'processes': found},
                    )
                
                elif action == 'kill':
                    signal_map = {'TERM': signal.SIGTERM, 'KILL': signal.SIGKILL, 'INT': signal.SIGINT}
                    sig = signal_map.get(sig_name, signal.SIGTERM)
                    
                    if pid:
                        p = psutil.Process(pid)
                        p.send_signal(sig)
                        return HandlerResult(
                            success=True,
                            data={'pid': pid, 'killed': True, 'signal': sig_name},
                        )
                    elif name:
                        killed = []
                        for p in psutil.process_iter(['pid', 'name']):
                            if name.lower() in p.info['name'].lower():
                                p.send_signal(sig)
                                killed.append(p.info['pid'])
                        return HandlerResult(
                            success=True,
                            data={'pids': killed, 'killed': True, 'signal': sig_name},
                        )
                    else:
                        return HandlerResult(success=False, error="PID or name required for kill action")
                
                elif action == 'info':
                    if not pid:
                        return HandlerResult(success=False, error="PID required for info action")
                    p = psutil.Process(pid)
                    return HandlerResult(
                        success=True,
                        data=p.as_dict(['pid', 'name', 'status', 'cpu_percent', 'memory_percent', 'cmdline', 'exe']),
                    )
                
            except ImportError:
                # Fallback: use subprocess
                if action == 'list':
                    result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
                    return HandlerResult(
                        success=True,
                        data={'output': result.stdout, 'raw': True},
                    )
                elif action == 'kill':
                    if pid:
                        os.kill(pid, signal.SIGTERM)
                        return HandlerResult(success=True, data={'pid': pid, 'killed': True})
                    return HandlerResult(success=False, error="psutil required for name-based kill")
                else:
                    return HandlerResult(success=False, error="psutil required for this action")
            
            duration_ms = (time.time() - start_time) * 1000
            return HandlerResult(success=False, error=f"Unknown action: {action}")
            
        except psutil.NoSuchProcess:
            return HandlerResult(
                success=False,
                error=f"Process not found: {pid or name}",
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class SystemMonitorHandler(TaskHandler):
    """Monitor system resources over time."""
    
    name = "system_monitor"
    category = "system"
    description = "Monitor system resources over time"
    
    input_schema = {
        'duration_seconds': {'type': 'integer', 'default': 10, 'description': 'Monitor duration'},
        'interval_seconds': {'type': 'integer', 'default': 1, 'description': 'Sample interval'},
        'metrics': {'type': 'array', 'default': ['cpu', 'memory', 'disk'], 'description': 'Metrics to collect'},
    }
    
    output_schema = {
        'samples': {'type': 'array', 'description': 'Time-series samples'},
        'summary': {'type': 'object', 'description': 'Aggregated statistics'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['system_monitor', 'monitor', 'top', 'htop']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        duration = params.get('duration_seconds', 10)
        interval = params.get('interval_seconds', 1)
        metrics = params.get('metrics', ['cpu', 'memory', 'disk'])
        
        try:
            import psutil
            
            samples = []
            elapsed = 0
            
            while elapsed < duration:
                sample = {'timestamp': time.time()}
                
                if 'cpu' in metrics:
                    sample['cpu_percent'] = psutil.cpu_percent(interval=min(interval, 0.1))
                
                if 'memory' in metrics:
                    mem = psutil.virtual_memory()
                    sample['memory_percent'] = mem.percent
                    sample['memory_available'] = mem.available
                
                if 'disk' in metrics:
                    disk = psutil.disk_usage('/')
                    sample['disk_percent'] = disk.percent
                
                samples.append(sample)
                
                time.sleep(max(0, interval - (time.time() - start_time) % interval))
                elapsed = time.time() - start_time
            
            # Calculate summary
            summary = {}
            if samples:
                for metric in ['cpu_percent', 'memory_percent', 'disk_percent']:
                    values = [s[metric] for s in samples if metric in s]
                    if values:
                        summary[metric] = {
                            'min': min(values),
                            'max': max(values),
                            'avg': sum(values) / len(values),
                        }
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'samples': samples,
                    'summary': summary,
                    'duration_seconds': elapsed,
                },
                duration_ms=duration_ms,
            )
            
        except ImportError:
            return HandlerResult(
                success=False,
                error="psutil required for system monitoring",
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )
