"""
Input Validation and Sanitization Module

Provides:
- Schema validation
- Input sanitization
- Command whitelisting
- Path validation
- Injection prevention
"""

import re
import os
import shlex
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable, Tuple, Union
from enum import Enum
from pathlib import Path


class ValidationError(Exception):
    """Raised when validation fails."""
    
    def __init__(self, field: str, message: str):
        self.field = field
        self.message = message
        super().__init__(f"{field}: {message}")


class ValidationLevel(Enum):
    """Validation strictness levels."""
    STRICT = "strict"
    NORMAL = "normal"
    PERMISSIVE = "permissive"


@dataclass
class ValidationRule:
    """Single validation rule."""
    field: str
    required: bool = True
    type: type = str
    min_value: Any = None
    max_value: Any = None
    pattern: Optional[str] = None
    enum: Optional[List[Any]] = None
    custom: Optional[Callable[[Any], bool]] = None
    custom_message: Optional[str] = None
    sanitize: bool = True
    
    def validate(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate a value against this rule."""
        # Check required
        if value is None:
            if self.required:
                return False, "Field is required"
            return True, None
        
        # Check type
        if not isinstance(value, self.type):
            # Allow int when expecting float
            if self.type == float and isinstance(value, int):
                value = float(value)
            else:
                return False, f"Expected {self.type.__name__}, got {type(value).__name__}"
        
        # Check min/max
        if self.min_value is not None and value < self.min_value:
            return False, f"Value must be >= {self.min_value}"
        
        if self.max_value is not None and value > self.max_value:
            return False, f"Value must be <= {self.max_value}"
        
        # Check pattern
        if self.pattern and isinstance(value, str):
            if not re.match(self.pattern, value):
                return False, f"Value does not match pattern {self.pattern}"
        
        # Check enum
        if self.enum and value not in self.enum:
            return False, f"Value must be one of: {self.enum}"
        
        # Custom validation
        if self.custom:
            try:
                if not self.custom(value):
                    return False, self.custom_message or "Custom validation failed"
            except Exception as e:
                return False, str(e)
        
        return True, None


class InputValidator:
    """
    Validates input against defined schema.
    
    Usage:
        validator = InputValidator()
        validator.add_rule('name', type=str, min_value=1, max_value=100)
        validator.add_rule('age', type=int, min_value=0, max_value=150)
        
        try:
            validated = validator.validate({'name': 'Alice', 'age': 30})
        except ValidationError as e:
            print(e)
    """
    
    def __init__(
        self,
        level: ValidationLevel = ValidationLevel.NORMAL,
        on_error: Callable[[str, str], None] = None,
    ):
        self.level = level
        self.on_error = on_error
        self._rules: Dict[str, List[ValidationRule]] = {}
        self._pre_hooks: List[Callable[[Dict], Dict]] = []
        self._post_hooks: List[Callable[[Dict], Dict]] = []
    
    def add_rule(
        self,
        field: str,
        required: bool = True,
        type: type = str,
        min_value: Any = None,
        max_value: Any = None,
        pattern: str = None,
        enum: List[Any] = None,
        custom: Callable[[Any], bool] = None,
        custom_message: str = None,
        sanitize: bool = True,
    ):
        """Add validation rule for a field."""
        rule = ValidationRule(
            field=field,
            required=required,
            type=type,
            min_value=min_value,
            max_value=max_value,
            pattern=pattern,
            enum=enum,
            custom=custom,
            custom_message=custom_message,
            sanitize=sanitize,
        )
        
        if field not in self._rules:
            self._rules[field] = []
        self._rules[field].append(rule)
    
    def add_schema(self, schema: Dict[str, Dict]):
        """Add multiple validation rules from schema dict."""
        for field, config in schema.items():
            self.add_rule(field, **config)
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate data against rules.
        
        Args:
            data: Input data to validate
            
        Returns:
            Validated and sanitized data
            
        Raises:
            ValidationError: If validation fails
        """
        # Run pre-hooks
        for hook in self._pre_hooks:
            data = hook(data)
        
        result = {}
        
        # Check all rules
        for field, rules in self._rules.items():
            value = data.get(field)
            
            for rule in rules:
                valid, error = rule.validate(value)
                
                if not valid:
                    if self.on_error:
                        self.on_error(field, error)
                    raise ValidationError(field, error)
                
                # Sanitize if needed
                if value is not None and rule.sanitize:
                    value = self._sanitize(value, rule.type)
                
                result[field] = value
        
        # Check for unknown fields (strict mode)
        if self.level == ValidationLevel.STRICT:
            unknown = set(data.keys()) - set(self._rules.keys())
            if unknown:
                raise ValidationError(
                    'unknown_fields',
                    f"Unknown fields: {unknown}"
                )
        
        # Run post-hooks
        for hook in self._post_hooks:
            result = hook(result)
        
        return result
    
    def _sanitize(self, value: Any, expected_type: type) -> Any:
        """Sanitize value based on type."""
        if expected_type == str:
            return self._sanitize_string(value)
        elif expected_type == int or expected_type == float:
            return expected_type(value)
        elif expected_type == list:
            return list(value) if isinstance(value, (list, tuple)) else [value]
        elif expected_type == dict:
            return dict(value) if isinstance(value, dict) else {'value': value}
        
        return value
    
    def _sanitize_string(self, value: str) -> str:
        """Sanitize string value."""
        # Remove null bytes
        value = value.replace('\x00', '')
        
        # Normalize unicode
        value = value.encode('utf-8', errors='replace').decode('utf-8')
        
        # Trim whitespace
        value = value.strip()
        
        return value
    
    def add_pre_hook(self, hook: Callable[[Dict], Dict]):
        """Add pre-validation hook."""
        self._pre_hooks.append(hook)
    
    def add_post_hook(self, hook: Callable[[Dict], Dict]):
        """Add post-validation hook."""
        self._post_hooks.append(hook)
    
    def get_schema(self) -> Dict[str, Any]:
        """Get validation schema as dict."""
        return {
            field: [{'type': r.type.__name__, 'required': r.required} for r in rules]
            for field, rules in self._rules.items()
        }


class Sanitizer:
    """
    Input sanitizer for various contexts.
    
    Usage:
        sanitizer = Sanitizer()
        
        # Sanitize for HTML
        safe_html = sanitizer.html(user_input)
        
        # Sanitize for SQL (basic)
        safe_sql = sanitizer.sql_identifier(user_input)
        
        # Sanitize for shell
        safe_shell = sanitizer.shell(user_input)
    """
    
    # Characters that are dangerous in different contexts
    HTML_DANGEROUS = ['<', '>', '&', '"', "'", '/', '\x00']
    SQL_DANGEROUS = ["'", '"', ';', '--', '/*', '*/', 'DROP', 'DELETE', 'INSERT', 'UPDATE']
    SHELL_DANGEROUS = ['`', '$', '(', ')', '|', ';', '&', '<', '>', '\n', '\r']
    
    def __init__(self):
        self._html_entity_map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#x27;',
            '/': '&#x2F;',
        }
    
    def html(self, value: str) -> str:
        """Sanitize string for HTML context."""
        result = value
        for char, entity in self._html_entity_map.items():
            result = result.replace(char, entity)
        return result
    
    def html_attribute(self, value: str) -> str:
        """Sanitize for HTML attribute."""
        # Escape all special characters
        return value.replace('"', '&quot;').replace("'", '&#x27;')
    
    def javascript(self, value: str) -> str:
        """Sanitize for JavaScript string."""
        return (
            value
            .replace('\\', '\\\\')
            .replace('"', '\\"')
            .replace("'", "\\'")
            .replace('\n', '\\n')
            .replace('\r', '\\r')
            .replace('<', '\\x3C')
            .replace('>', '\\x3E')
        )
    
    def url(self, value: str) -> str:
        """Sanitize for URL parameter."""
        import urllib.parse
        return urllib.parse.quote(value, safe='')
    
    def sql_identifier(self, value: str) -> str:
        """Sanitize SQL identifier (table/column name)."""
        # Only allow alphanumeric and underscore
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', value):
            raise ValidationError('sql_identifier', 'Invalid SQL identifier')
        return value
    
    def filename(self, value: str) -> str:
        """Sanitize filename."""
        # Remove path separators and null bytes
        value = value.replace('/', '_').replace('\\', '_').replace('\x00', '')
        # Remove leading dots (hidden files)
        value = value.lstrip('.')
        # Limit length
        max_len = 255
        if len(value) > max_len:
            name, ext = os.path.splitext(value)
            value = name[:max_len - len(ext)] + ext
        return value
    
    def path(self, value: str, base_dir: str = None) -> str:
        """Sanitize and validate file path."""
        # Normalize path
        path = os.path.normpath(value)
        
        # Check for path traversal
        if '..' in path.split(os.sep):
            raise ValidationError('path', 'Path traversal detected')
        
        # If base_dir specified, ensure path is within it
        if base_dir:
            base = os.path.abspath(base_dir)
            full_path = os.path.abspath(os.path.join(base_dir, path))
            if not full_path.startswith(base):
                raise ValidationError('path', 'Path escapes base directory')
            return full_path
        
        return path
    
    def shell(self, value: str) -> str:
        """Sanitize for shell argument."""
        # Use shlex.quote for shell escaping
        return shlex.quote(value)
    
    def email(self, value: str) -> str:
        """Validate and sanitize email."""
        value = value.strip().lower()
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, value):
            raise ValidationError('email', 'Invalid email format')
        return value
    
    def phone(self, value: str) -> str:
        """Validate and sanitize phone number."""
        # Remove all non-digits
        digits = re.sub(r'\D', '', value)
        if len(digits) < 10 or len(digits) > 15:
            raise ValidationError('phone', 'Invalid phone number length')
        return '+' + digits


class CommandWhitelist:
    """
    Whitelist for allowed shell commands.
    
    Usage:
        whitelist = CommandWhitelist()
        whitelist.allow('ls')
        whitelist.allow('cat', args=['filename'])
        whitelist.allow('curl', args=['url'], dangerous=True)
        
        # Check if allowed
        if whitelist.is_allowed('ls', ['-la']):
            # Execute
    """
    
    def __init__(self, strict: bool = True):
        self.strict = strict
        self._allowed: Dict[str, Dict[str, Any]] = {}
        self._forbidden_args: Dict[str, List[str]] = {}
        self._dangerous_commands: set = set()
    
    def allow(
        self,
        command: str,
        args: List[str] = None,
        dangerous: bool = False,
        validator: Callable[[List[str]], bool] = None,
    ):
        """
        Allow a command.
        
        Args:
            command: Command name
            args: Allowed argument names (None = any)
            dangerous: Mark as dangerous (requires extra validation)
            validator: Custom argument validator
        """
        self._allowed[command] = {
            'args': args,
            'validator': validator,
        }
        
        if dangerous:
            self._dangerous_commands.add(command)
    
    def forbid_arg(self, command: str, arg: str):
        """Forbid specific argument for a command."""
        if command not in self._forbidden_args:
            self._forbidden_args[command] = []
        self._forbidden_args[command].append(arg)
    
    def is_allowed(
        self,
        command: str,
        args: List[str] = None,
    ) -> Tuple[bool, str]:
        """
        Check if command is allowed.
        
        Args:
            command: Command name
            args: Command arguments
            
        Returns:
            (allowed, reason)
        """
        # Extract base command
        base_cmd = os.path.basename(command)
        
        # Check if command is allowed
        if base_cmd not in self._allowed:
            if self.strict:
                return False, f"Command '{base_cmd}' not in whitelist"
            return True, "Allowed (non-strict mode)"
        
        cmd_config = self._allowed[base_cmd]
        
        # Check forbidden args
        if args:
            forbidden = self._forbidden_args.get(base_cmd, [])
            for arg in args:
                for fb in forbidden:
                    if arg.startswith(fb):
                        return False, f"Argument '{arg}' is forbidden"
        
        # Check allowed args
        if cmd_config['args'] is not None and args:
            for arg in args:
                arg_name = arg.split('=')[0] if '=' in arg else arg
                if arg_name not in cmd_config['args']:
                    return False, f"Argument '{arg_name}' not allowed"
        
        # Custom validator
        if cmd_config['validator'] and args:
            try:
                if not cmd_config['validator'](args):
                    return False, "Custom validation failed"
            except Exception as e:
                return False, str(e)
        
        return True, "Allowed"
    
    def is_dangerous(self, command: str) -> bool:
        """Check if command is marked as dangerous."""
        return os.path.basename(command) in self._dangerous_commands
    
    def get_allowed(self) -> List[str]:
        """Get list of allowed commands."""
        return list(self._allowed.keys())
    
    def get_dangerous(self) -> List[str]:
        """Get list of dangerous commands."""
        return list(self._dangerous_commands)


# Default safe command whitelist
DEFAULT_SAFE_COMMANDS = [
    'ls', 'cat', 'head', 'tail', 'wc', 'grep', 'find',
    'echo', 'pwd', 'date', 'whoami', 'id',
    'curl', 'wget', 'ping', 'nslookup',
]


def create_default_whitelist() -> CommandWhitelist:
    """Create default command whitelist."""
    whitelist = CommandWhitelist(strict=True)
    
    for cmd in DEFAULT_SAFE_COMMANDS:
        whitelist.allow(cmd)
    
    # Explicitly forbid dangerous commands
    whitelist.allow('rm', dangerous=True, validator=lambda args: '-rf' not in ' '.join(args))
    whitelist.allow('sudo', dangerous=True)
    whitelist.allow('chmod', dangerous=True)
    
    return whitelist
