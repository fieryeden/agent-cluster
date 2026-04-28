"""
Handler Registry for task routing.

Maintains registry of all available handlers and routes tasks to the
best matching handler based on capability confidence.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Type
from enum import Enum
import importlib


class HandlerCategory(Enum):
    """Categories for organizing handlers."""
    FILE = "file"
    DATA = "data"
    WEB = "web"
    SYSTEM = "system"
    COMMUNICATION = "communication"
    AI = "ai"
    DATABASE = "database"
    CLOUD = "cloud"
    INTEGRATION = "integration"


@dataclass
class HandlerInfo:
    """Information about a registered handler."""
    name: str
    category: HandlerCategory
    description: str
    handler_class: Type
    input_schema: Dict[str, Any] = field(default_factory=dict)
    output_schema: Dict[str, Any] = field(default_factory=dict)
    requires_network: bool = False
    requires_filesystem: bool = False
    requires_external_api: bool = False
    dangerous: bool = False


class HandlerRegistry:
    """
    Central registry for task handlers.
    
    Usage:
        registry = HandlerRegistry()
        registry.register_all()
        
        # Find best handler
        handler = registry.find_best_handler({'task': 'file_read', 'path': '/tmp/test.txt'})
        
        # Execute
        result = handler.execute({'path': '/tmp/test.txt'})
    """
    
    def __init__(self):
        self.handlers: Dict[str, HandlerInfo] = {}
        self._instances: Dict[str, Any] = {}
    
    def register(self, handler_class: Type, category: HandlerCategory = None):
        """
        Register a handler class.
        
        Args:
            handler_class: TaskHandler subclass
            category: Optional category override
        """
        # Create instance to get metadata
        instance = handler_class()
        name = instance.name
        
        # Determine category
        cat = category or HandlerCategory(instance.category)
        
        info = HandlerInfo(
            name=name,
            category=cat,
            description=instance.description,
            handler_class=handler_class,
            input_schema=instance.input_schema,
            output_schema=instance.output_schema,
            requires_network=getattr(instance, 'requires_network', False),
            requires_filesystem=getattr(instance, 'requires_filesystem', False),
            requires_external_api=getattr(instance, 'requires_external_api', False),
            dangerous=getattr(instance, 'dangerous', False),
        )
        
        self.handlers[name] = info
        self._instances[name] = instance
    
    def register_all(self):
        """Register all built-in handlers."""
        # File handlers
        from handlers.file import (
            FileReadHandler,
            FileWriteHandler,
            FileCopyHandler,
            FileMoveHandler,
            FileDeleteHandler,
            FileCompressHandler,
            FileExtractHandler,
            FileWatchHandler,
            FileListHandler,
            FileChecksumHandler,
        )
        for h in [FileReadHandler, FileWriteHandler, FileCopyHandler, FileMoveHandler,
                  FileDeleteHandler, FileCompressHandler, FileExtractHandler,
                  FileWatchHandler, FileListHandler, FileChecksumHandler]:
            self.register(h)
        
        # Data handlers
        from handlers.data import (
            CSVParseHandler,
            CSVTransformHandler,
            JSONTransformHandler,
            XMLParseHandler,
            ExcelReadHandler,
            PDFExtractHandler,
            DataMergeHandler,
            DataFilterHandler,
            DataValidateHandler,
            DataAggregateHandler,
        )
        for h in [CSVParseHandler, CSVTransformHandler, JSONTransformHandler,
                  XMLParseHandler, ExcelReadHandler, PDFExtractHandler,
                  DataMergeHandler, DataFilterHandler, DataValidateHandler,
                  DataAggregateHandler]:
            self.register(h)
        
        # Web handlers
        from handlers.web import (
            WebFetchHandler,
            WebScrapeHandler,
            WebAPIHandler,
            WebDownloadHandler,
            WebSubmitHandler,
            WebHealthCheckHandler,
            WebProxyHandler,
            WebGraphQLHandler,
        )
        for h in [WebFetchHandler, WebScrapeHandler, WebAPIHandler,
                  WebDownloadHandler, WebSubmitHandler, WebHealthCheckHandler,
                  WebProxyHandler, WebGraphQLHandler]:
            self.register(h)
        
        # System handlers
        from handlers.system import (
            SystemExecHandler,
            SystemInfoHandler,
            SystemEnvHandler,
            SystemProcessHandler,
            SystemMonitorHandler,
        )
        for h in [SystemExecHandler, SystemInfoHandler, SystemEnvHandler,
                  SystemProcessHandler, SystemMonitorHandler]:
            self.register(h)
        
        # Communication handlers
        from handlers.communication import (
            EmailSendHandler,
            SlackSendHandler,
            DiscordSendHandler,
            NotificationHandler,
            WebhookHandler,
        )
        for h in [EmailSendHandler, SlackSendHandler, DiscordSendHandler,
                  NotificationHandler, WebhookHandler]:
            self.register(h)
        
        # AI handlers
        from handlers.ai import (
            LLMChatHandler,
            LLMCompleteHandler,
            EmbeddingHandler,
            TextSummarizeHandler,
            TextClassifyHandler,
            TextExtractHandler,
        )
        for h in [LLMChatHandler, LLMCompleteHandler, EmbeddingHandler,
                  TextSummarizeHandler, TextClassifyHandler, TextExtractHandler]:
            self.register(h)
        
        # Database handlers
        from handlers.database import (
            SQLQueryHandler,
            RedisHandler,
            MongoDBHandler,
            SQLiteHandler,
        )
        for h in [SQLQueryHandler, RedisHandler, MongoDBHandler, SQLiteHandler]:
            self.register(h)
        
        # Cloud handlers
        from handlers.cloud import (
            S3Handler,
            LambdaHandler,
            CloudStorageHandler,
            AzureBlobHandler,
        )
        for h in [S3Handler, LambdaHandler, CloudStorageHandler, AzureBlobHandler]:
            self.register(h)
        
        # Integration handlers
        from handlers.integration import (
            GitHubHandler,
            StripeHandler,
            TwilioHandler,
            SlackAPIHandler,
        )
        for h in [GitHubHandler, StripeHandler, TwilioHandler, SlackAPIHandler]:
            self.register(h)
    
    def get_handler(self, name: str) -> Optional[Any]:
        """Get handler instance by name."""
        if name in self._instances:
            return self._instances[name]
        return None
    
    def find_best_handler(self, params: Dict[str, Any]) -> Optional[Any]:
        """
        Find the best handler for given parameters.
        
        Queries all handlers for confidence scores and returns
        the handler with highest confidence.
        
        Args:
            params: Task parameters
            
        Returns:
            Handler instance with highest confidence, or None
        """
        best_handler = None
        best_confidence = 0.0
        
        for name, instance in self._instances.items():
            confidence = instance.can_handle(params)
            if confidence > best_confidence:
                best_confidence = confidence
                best_handler = instance
        
        return best_handler if best_confidence > 0 else None
    
    def list_handlers(self, category: HandlerCategory = None) -> List[Dict[str, Any]]:
        """
        List all registered handlers.
        
        Args:
            category: Optional category filter
            
        Returns:
            List of handler info dictionaries
        """
        result = []
        for name, info in self.handlers.items():
            if category is None or info.category == category:
                result.append({
                    'name': info.name,
                    'category': info.category.value,
                    'description': info.description,
                    'requires_network': info.requires_network,
                    'requires_filesystem': info.requires_filesystem,
                    'requires_external_api': info.requires_external_api,
                    'dangerous': info.dangerous,
                })
        return result
    
    def get_handlers_by_category(self, category: HandlerCategory) -> List[Any]:
        """Get all handlers in a category."""
        return [
            self._instances[name]
            for name, info in self.handlers.items()
            if info.category == category
        ]
    
    def count_handlers(self) -> Dict[str, int]:
        """Count handlers by category."""
        counts = {}
        for name, info in self.handlers.items():
            cat = info.category.value
            counts[cat] = counts.get(cat, 0) + 1
        return counts
