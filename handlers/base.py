"""
Base classes for task handlers.

All handlers inherit from TaskHandler and implement:
- can_handle(params) -> HandlerConfidence
- execute(params) -> HandlerResult
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List
from enum import Enum


class HandlerConfidence(Enum):
    """Confidence levels for handler capability matching."""
    NONE = 0.0      # Cannot handle this task
    LOW = 0.3      # Can handle but not ideal
    MEDIUM = 0.6   # Capable with some limitations
    HIGH = 0.8     # Well-suited for this task
    PERFECT = 1.0  # Exact match, optimal handler


@dataclass
class HandlerResult:
    """Result of task execution."""
    success: bool
    data: Any = None
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    
    # Timing
    duration_ms: float = 0.0
    
    # Resource usage
    bytes_read: int = 0
    bytes_written: int = 0
    memory_peak_mb: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'success': self.success,
            'data': self.data,
            'error': self.error,
            'warnings': self.warnings,
            'metrics': self.metrics,
            'duration_ms': self.duration_ms,
            'bytes_read': self.bytes_read,
            'bytes_written': self.bytes_written,
            'memory_peak_mb': self.memory_peak_mb,
        }


class TaskHandler:
    """
    Base class for all task handlers.
    
    Subclasses must implement:
    - name: Handler identifier
    - category: Handler category
    - description: What this handler does
    - input_schema: Expected input parameters
    - output_schema: Expected output structure
    - can_handle(params) -> float: Confidence score (0.0-1.0)
    - execute(params) -> HandlerResult: Execute the task
    """
    
    name: str = "base"
    category: str = "general"
    description: str = "Base handler class"
    input_schema: Dict[str, Any] = {}
    output_schema: Dict[str, Any] = {}
    
    # Handler capabilities
    requires_network: bool = False
    requires_filesystem: bool = False
    requires_external_api: bool = False
    dangerous: bool = False  # Can cause side effects
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        """
        Return confidence score (0.0-1.0) for handling this task.
        
        Override this method to implement custom matching logic.
        
        Args:
            params: Task parameters
            
        Returns:
            Confidence score where 0.0 = cannot handle, 1.0 = perfect match
        """
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        """
        Execute the task with given parameters.
        
        Override this method to implement task logic.
        
        Args:
            params: Task parameters matching input_schema
            
        Returns:
            HandlerResult with success status and data/error
        """
        raise NotImplementedError("Subclasses must implement execute()")
    
    def validate_params(self, params: Dict[str, Any]) -> List[str]:
        """
        Validate input parameters against schema.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check required parameters
        for key, spec in self.input_schema.items():
            if spec.get('required', False) and key not in params:
                errors.append(f"Missing required parameter: {key}")
        
        return errors
    
    def get_info(self) -> Dict[str, Any]:
        """Return handler information for registry."""
        return {
            'name': self.name,
            'category': self.category,
            'description': self.description,
            'input_schema': self.input_schema,
            'output_schema': self.output_schema,
            'requires_network': self.requires_network,
            'requires_filesystem': self.requires_filesystem,
            'requires_external_api': self.requires_external_api,
            'dangerous': self.dangerous,
        }
