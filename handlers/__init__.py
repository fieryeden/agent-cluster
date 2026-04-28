"""
Production Task Handlers for Agent Cluster

This module provides 50+ production-ready task handlers organized by category:
- file: File operations (read, write, copy, move, delete, compress, extract)
- data: Data processing (csv, json, xml, excel, pdf)
- web: Web operations (fetch, scrape, api, download)
- system: System operations (exec, info, env, process, monitor)
- communication: Messaging (email, slack, discord, notification, webhook)
- ai: AI/ML tasks (llm, embedding, summarize, classify, extract)
- database: Database operations (sql, redis, mongodb, sqlite)
- cloud: Cloud services (s3, lambda, gcs, azure)
- integration: Third-party APIs (github, stripe, twilio, slack)

Each handler:
- Returns confidence score (0.0-1.0)
- Has clear input/output schema
- Handles errors gracefully
- Is independently testable
"""

from handlers.registry import (
    HandlerRegistry,
    HandlerInfo,
    HandlerCategory,
)

from handlers.base import (
    TaskHandler,
    HandlerResult,
    HandlerConfidence,
)

# Import file handlers
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

# Import data handlers
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

# Import web handlers
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

# Import system handlers
from handlers.system import (
    SystemExecHandler,
    SystemInfoHandler,
    SystemEnvHandler,
    SystemProcessHandler,
    SystemMonitorHandler,
)

# Import communication handlers
from handlers.communication import (
    EmailSendHandler,
    SlackSendHandler,
    DiscordSendHandler,
    NotificationHandler,
    WebhookHandler,
)

# Import AI handlers
from handlers.ai import (
    LLMChatHandler,
    LLMCompleteHandler,
    EmbeddingHandler,
    TextSummarizeHandler,
    TextClassifyHandler,
    TextExtractHandler,
)

# Import database handlers
from handlers.database import (
    SQLQueryHandler,
    RedisHandler,
    MongoDBHandler,
    SQLiteHandler,
)

# Import cloud handlers
from handlers.cloud import (
    S3Handler,
    LambdaHandler,
    CloudStorageHandler,
    AzureBlobHandler,
)

# Import integration handlers
from handlers.integration import (
    GitHubHandler,
    StripeHandler,
    TwilioHandler,
    SlackAPIHandler,
)


def get_all_handlers():
    """Return all available handlers with their registry info."""
    registry = HandlerRegistry()
    registry.register_all()
    return registry.list_handlers()


def get_handler(name: str):
    """Get a specific handler by name."""
    registry = HandlerRegistry()
    registry.register_all()
    return registry.get_handler(name)


__all__ = [
    # Registry
    'HandlerRegistry',
    'HandlerInfo',
    'HandlerCategory',
    
    # Base
    'TaskHandler',
    'HandlerResult',
    'HandlerConfidence',
    
    # File handlers
    'FileReadHandler',
    'FileWriteHandler',
    'FileCopyHandler',
    'FileMoveHandler',
    'FileDeleteHandler',
    'FileCompressHandler',
    'FileExtractHandler',
    'FileWatchHandler',
    'FileListHandler',
    'FileChecksumHandler',
    
    # Data handlers
    'CSVParseHandler',
    'CSVTransformHandler',
    'JSONTransformHandler',
    'XMLParseHandler',
    'ExcelReadHandler',
    'PDFExtractHandler',
    'DataMergeHandler',
    'DataFilterHandler',
    'DataValidateHandler',
    'DataAggregateHandler',
    
    # Web handlers
    'WebFetchHandler',
    'WebScrapeHandler',
    'WebAPIHandler',
    'WebDownloadHandler',
    'WebSubmitHandler',
    'WebHealthCheckHandler',
    'WebProxyHandler',
    'WebGraphQLHandler',
    
    # System handlers
    'SystemExecHandler',
    'SystemInfoHandler',
    'SystemEnvHandler',
    'SystemProcessHandler',
    'SystemMonitorHandler',
    
    # Communication handlers
    'EmailSendHandler',
    'SlackSendHandler',
    'DiscordSendHandler',
    'NotificationHandler',
    'WebhookHandler',
    
    # AI handlers
    'LLMChatHandler',
    'LLMCompleteHandler',
    'EmbeddingHandler',
    'TextSummarizeHandler',
    'TextClassifyHandler',
    'TextExtractHandler',
    
    # Database handlers
    'SQLQueryHandler',
    'RedisHandler',
    'MongoDBHandler',
    'SQLiteHandler',
    
    # Cloud handlers
    'S3Handler',
    'LambdaHandler',
    'CloudStorageHandler',
    'AzureBlobHandler',
    
    # Integration handlers
    'GitHubHandler',
    'StripeHandler',
    'TwilioHandler',
    'SlackAPIHandler',
    
    # Utility functions
    'get_all_handlers',
    'get_handler',
]
