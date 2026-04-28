"""
File operation handlers.

Handlers for reading, writing, copying, moving, deleting,
compressing, and extracting files.
"""

import os
import shutil
import hashlib
import gzip
import zipfile
import tarfile
import time
import mimetypes
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable

from handlers.base import (
    TaskHandler,
    HandlerResult,
    HandlerConfidence,
)


class FileReadHandler(TaskHandler):
    """Read file contents with encoding detection."""
    
    name = "file_read"
    category = "file"
    description = "Read file contents with automatic encoding detection"
    requires_filesystem = True
    
    input_schema = {
        'path': {'type': 'string', 'required': True, 'description': 'File path to read'},
        'encoding': {'type': 'string', 'default': 'utf-8', 'description': 'File encoding'},
        'binary': {'type': 'boolean', 'default': False, 'description': 'Read as binary'},
        'start_line': {'type': 'integer', 'description': 'Start line (for text files)'},
        'end_line': {'type': 'integer', 'description': 'End line (for text files)'},
        'max_bytes': {'type': 'integer', 'default': 10485760, 'description': 'Max bytes to read (default 10MB)'},
    }
    
    output_schema = {
        'content': {'type': 'string', 'description': 'File content'},
        'size': {'type': 'integer', 'description': 'File size in bytes'},
        'mime_type': {'type': 'string', 'description': 'Detected MIME type'},
        'encoding': {'type': 'string', 'description': 'Detected encoding'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['file_read', 'read_file', 'read']:
            return HandlerConfidence.PERFECT.value
        if params.get('task') in ['cat', 'head', 'tail']:
            return HandlerConfidence.HIGH.value
        if 'path' in params and 'content' not in params:
            return HandlerConfidence.MEDIUM.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        path = params['path']
        encoding = params.get('encoding', 'utf-8')
        binary = params.get('binary', False)
        max_bytes = params.get('max_bytes', 10485760)
        
        try:
            path_obj = Path(path)
            
            if not path_obj.exists():
                return HandlerResult(
                    success=False,
                    error=f"File not found: {path}",
                )
            
            if not path_obj.is_file():
                return HandlerResult(
                    success=False,
                    error=f"Path is not a file: {path}",
                )
            
            file_size = path_obj.stat().st_size
            
            if file_size > max_bytes:
                return HandlerResult(
                    success=False,
                    error=f"File too large: {file_size} bytes (max: {max_bytes})",
                )
            
            # Detect MIME type
            mime_type, _ = mimetypes.guess_type(path)
            
            if binary:
                with open(path, 'rb') as f:
                    content = f.read()
                detected_encoding = None
            else:
                # Try common encodings
                encodings_to_try = [encoding, 'utf-8', 'latin-1', 'cp1252']
                content = None
                detected_encoding = encoding
                
                for enc in encodings_to_try:
                    try:
                        with open(path, 'r', encoding=enc) as f:
                            lines = f.readlines()
                            
                            # Handle line range
                            start_line = params.get('start_line', 1) - 1
                            end_line = params.get('end_line', len(lines))
                            
                            content = ''.join(lines[start_line:end_line])
                            detected_encoding = enc
                            break
                    except UnicodeDecodeError:
                        continue
                
                if content is None:
                    return HandlerResult(
                        success=False,
                        error="Could not decode file with any common encoding",
                    )
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'content': content,
                    'size': file_size,
                    'mime_type': mime_type,
                    'encoding': detected_encoding,
                    'path': str(path_obj.absolute()),
                },
                duration_ms=duration_ms,
                bytes_read=file_size,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class FileWriteHandler(TaskHandler):
    """Write content to file with atomic writes."""
    
    name = "file_write"
    category = "file"
    description = "Write content to file with atomic write support"
    requires_filesystem = True
    dangerous = True
    
    input_schema = {
        'path': {'type': 'string', 'required': True, 'description': 'File path to write'},
        'content': {'type': 'string', 'required': True, 'description': 'Content to write'},
        'encoding': {'type': 'string', 'default': 'utf-8', 'description': 'File encoding'},
        'binary': {'type': 'boolean', 'default': False, 'description': 'Write as binary'},
        'append': {'type': 'boolean', 'default': False, 'description': 'Append to file'},
        'atomic': {'type': 'boolean', 'default': True, 'description': 'Use atomic write'},
        'mode': {'type': 'string', 'default': '644', 'description': 'File permissions (Unix)'},
    }
    
    output_schema = {
        'path': {'type': 'string', 'description': 'Written file path'},
        'bytes_written': {'type': 'integer', 'description': 'Bytes written'},
        'created': {'type': 'boolean', 'description': 'File was created'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['file_write', 'write_file', 'write']:
            return HandlerConfidence.PERFECT.value
        if 'path' in params and 'content' in params:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        path = params['path']
        content = params['content']
        encoding = params.get('encoding', 'utf-8')
        binary = params.get('binary', False)
        append = params.get('append', False)
        atomic = params.get('atomic', True)
        
        try:
            path_obj = Path(path)
            created = not path_obj.exists()
            
            # Create parent directories
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            mode = 'ab' if binary else ('a' if append else 'w')
            if not binary:
                mode += 'b' if atomic else ''
            
            if atomic and not append:
                # Write to temp file, then rename
                temp_path = str(path) + '.tmp'
                if binary:
                    with open(temp_path, 'wb') as f:
                        bytes_written = f.write(content)
                else:
                    with open(temp_path, 'w', encoding=encoding) as f:
                        bytes_written = f.write(content)
                        f.flush()
                        os.fsync(f.fileno())
                
                os.replace(temp_path, path)
            else:
                if binary:
                    with open(path, mode) as f:
                        bytes_written = f.write(content)
                else:
                    with open(path, mode, encoding=encoding) as f:
                        bytes_written = f.write(content)
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'path': str(path_obj.absolute()),
                    'bytes_written': bytes_written,
                    'created': created,
                },
                duration_ms=duration_ms,
                bytes_written=bytes_written,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class FileCopyHandler(TaskHandler):
    """Copy file or directory."""
    
    name = "file_copy"
    category = "file"
    description = "Copy file or directory with metadata preservation"
    requires_filesystem = True
    
    input_schema = {
        'source': {'type': 'string', 'required': True, 'description': 'Source path'},
        'destination': {'type': 'string', 'required': True, 'description': 'Destination path'},
        'preserve_metadata': {'type': 'boolean', 'default': True, 'description': 'Preserve timestamps and mode'},
        'overwrite': {'type': 'boolean', 'default': False, 'description': 'Overwrite existing'},
    }
    
    output_schema = {
        'source': {'type': 'string', 'description': 'Source path'},
        'destination': {'type': 'string', 'description': 'Destination path'},
        'bytes_copied': {'type': 'integer', 'description': 'Bytes copied'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['file_copy', 'copy_file', 'copy', 'cp']:
            return HandlerConfidence.PERFECT.value
        if 'source' in params and 'destination' in params:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        source = params['source']
        destination = params['destination']
        preserve = params.get('preserve_metadata', True)
        overwrite = params.get('overwrite', False)
        
        try:
            src_path = Path(source)
            dst_path = Path(destination)
            
            if not src_path.exists():
                return HandlerResult(
                    success=False,
                    error=f"Source not found: {source}",
                )
            
            if dst_path.exists() and not overwrite:
                return HandlerResult(
                    success=False,
                    error=f"Destination exists: {destination}",
                )
            
            # Create parent directories
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            
            if src_path.is_file():
                if preserve:
                    shutil.copy2(src_path, dst_path)
                else:
                    shutil.copy(src_path, dst_path)
                bytes_copied = src_path.stat().st_size
            else:
                if dst_path.exists():
                    shutil.rmtree(dst_path)
                if preserve:
                    shutil.copytree(src_path, dst_path)
                else:
                    shutil.copytree(src_path, dst_path, copy_function=shutil.copy)
                bytes_copied = sum(f.stat().st_size for f in dst_path.rglob('*') if f.is_file())
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'source': str(src_path.absolute()),
                    'destination': str(dst_path.absolute()),
                    'bytes_copied': bytes_copied,
                },
                duration_ms=duration_ms,
                bytes_read=bytes_copied,
                bytes_written=bytes_copied,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class FileMoveHandler(TaskHandler):
    """Move file or directory."""
    
    name = "file_move"
    category = "file"
    description = "Move file or directory"
    requires_filesystem = True
    dangerous = True
    
    input_schema = {
        'source': {'type': 'string', 'required': True, 'description': 'Source path'},
        'destination': {'type': 'string', 'required': True, 'description': 'Destination path'},
        'overwrite': {'type': 'boolean', 'default': False, 'description': 'Overwrite existing'},
    }
    
    output_schema = {
        'source': {'type': 'string', 'description': 'Original path'},
        'destination': {'type': 'string', 'description': 'New path'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['file_move', 'move_file', 'move', 'mv']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        source = params['source']
        destination = params['destination']
        overwrite = params.get('overwrite', False)
        
        try:
            src_path = Path(source)
            dst_path = Path(destination)
            
            if not src_path.exists():
                return HandlerResult(
                    success=False,
                    error=f"Source not found: {source}",
                )
            
            if dst_path.exists():
                if not overwrite:
                    return HandlerResult(
                        success=False,
                        error=f"Destination exists: {destination}",
                    )
                if dst_path.is_dir():
                    shutil.rmtree(dst_path)
                else:
                    dst_path.unlink()
            
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src_path), str(dst_path))
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'source': str(src_path.absolute()),
                    'destination': str(dst_path.absolute()),
                },
                duration_ms=duration_ms,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class FileDeleteHandler(TaskHandler):
    """Delete file or directory."""
    
    name = "file_delete"
    category = "file"
    description = "Delete file or directory (moves to trash if available)"
    requires_filesystem = True
    dangerous = True
    
    input_schema = {
        'path': {'type': 'string', 'required': True, 'description': 'Path to delete'},
        'recursive': {'type': 'boolean', 'default': False, 'description': 'Delete directories recursively'},
        'trash': {'type': 'boolean', 'default': True, 'description': 'Move to trash instead of permanent delete'},
        'force': {'type': 'boolean', 'default': False, 'description': 'Force deletion without confirmation'},
    }
    
    output_schema = {
        'path': {'type': 'string', 'description': 'Deleted path'},
        'type': {'type': 'string', 'description': 'file or directory'},
        'bytes_freed': {'type': 'integer', 'description': 'Bytes freed'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['file_delete', 'delete_file', 'delete', 'rm', 'remove']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        path = params['path']
        recursive = params.get('recursive', False)
        use_trash = params.get('trash', True)
        
        try:
            path_obj = Path(path)
            
            if not path_obj.exists():
                return HandlerResult(
                    success=False,
                    error=f"Path not found: {path}",
                )
            
            is_dir = path_obj.is_dir()
            
            # Calculate size before deletion
            if is_dir:
                bytes_freed = sum(f.stat().st_size for f in path_obj.rglob('*') if f.is_file())
            else:
                bytes_freed = path_obj.stat().st_size
            
            # Try to use trash if requested
            if use_trash:
                try:
                    from send2trash import send2trash
                    send2trash(str(path_obj))
                    trashed = True
                except ImportError:
                    trashed = False
            else:
                trashed = False
            
            if not trashed:
                if is_dir:
                    if not recursive:
                        return HandlerResult(
                            success=False,
                            error=f"Is a directory, use recursive=True: {path}",
                        )
                    shutil.rmtree(path_obj)
                else:
                    path_obj.unlink()
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'path': str(path_obj.absolute()),
                    'type': 'directory' if is_dir else 'file',
                    'bytes_freed': bytes_freed,
                    'trashed': trashed,
                },
                duration_ms=duration_ms,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class FileCompressHandler(TaskHandler):
    """Compress files or directories."""
    
    name = "file_compress"
    category = "file"
    description = "Compress files or directories to zip/tar.gz"
    requires_filesystem = True
    
    input_schema = {
        'source': {'type': 'string', 'required': True, 'description': 'Source path(s)'},
        'destination': {'type': 'string', 'required': True, 'description': 'Archive path'},
        'format': {'type': 'string', 'default': 'zip', 'enum': ['zip', 'tar.gz', 'tar.bz2', 'gz']},
        'compression': {'type': 'integer', 'default': 6, 'description': 'Compression level (1-9)'},
    }
    
    output_schema = {
        'source': {'type': 'string', 'description': 'Source path(s)'},
        'destination': {'type': 'string', 'description': 'Archive path'},
        'original_size': {'type': 'integer', 'description': 'Original size in bytes'},
        'compressed_size': {'type': 'integer', 'description': 'Compressed size in bytes'},
        'compression_ratio': {'type': 'float', 'description': 'Compression ratio'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['file_compress', 'compress', 'zip', 'archive']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        source = params['source']
        destination = params['destination']
        fmt = params.get('format', 'zip')
        compression = params.get('compression', 6)
        
        try:
            sources = [source] if isinstance(source, str) else source
            src_paths = [Path(s) for s in sources]
            dst_path = Path(destination)
            
            # Validate sources
            for p in src_paths:
                if not p.exists():
                    return HandlerResult(
                        success=False,
                        error=f"Source not found: {p}",
                    )
            
            # Calculate original size
            original_size = 0
            for p in src_paths:
                if p.is_file():
                    original_size += p.stat().st_size
                else:
                    original_size += sum(f.stat().st_size for f in p.rglob('*') if f.is_file())
            
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            
            if fmt == 'zip':
                with zipfile.ZipFile(dst_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=compression) as zf:
                    for src in src_paths:
                        if src.is_file():
                            zf.write(src, src.name)
                        else:
                            for file_path in src.rglob('*'):
                                if file_path.is_file():
                                    arcname = file_path.relative_to(src.parent)
                                    zf.write(file_path, arcname)
            
            elif fmt == 'gz':
                if len(src_paths) != 1 or not src_paths[0].is_file():
                    return HandlerResult(
                        success=False,
                        error="gz format only supports single file compression",
                    )
                with open(src_paths[0], 'rb') as f_in:
                    with gzip.open(dst_path, 'wb', compresslevel=compression) as f_out:
                        shutil.copyfileobj(f_in, f_out)
            
            elif fmt in ['tar.gz', 'tar.bz2']:
                mode = 'w:gz' if fmt == 'tar.gz' else 'w:bz2'
                with tarfile.open(dst_path, mode) as tf:
                    for src in src_paths:
                        tf.add(src, arcname=src.name)
            
            compressed_size = dst_path.stat().st_size
            ratio = original_size / compressed_size if compressed_size > 0 else 0
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'source': sources,
                    'destination': str(dst_path.absolute()),
                    'original_size': original_size,
                    'compressed_size': compressed_size,
                    'compression_ratio': round(ratio, 2),
                },
                duration_ms=duration_ms,
                bytes_read=original_size,
                bytes_written=compressed_size,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class FileExtractHandler(TaskHandler):
    """Extract archive files."""
    
    name = "file_extract"
    category = "file"
    description = "Extract zip/tar.gz archives"
    requires_filesystem = True
    
    input_schema = {
        'source': {'type': 'string', 'required': True, 'description': 'Archive path'},
        'destination': {'type': 'string', 'required': True, 'description': 'Extraction directory'},
        'strip_components': {'type': 'integer', 'default': 0, 'description': 'Strip leading path components'},
    }
    
    output_schema = {
        'source': {'type': 'string', 'description': 'Archive path'},
        'destination': {'type': 'string', 'description': 'Extraction directory'},
        'files': {'type': 'array', 'description': 'Extracted file paths'},
        'bytes_extracted': {'type': 'integer', 'description': 'Total bytes extracted'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['file_extract', 'extract', 'unzip', 'untar']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        source = params['source']
        destination = params['destination']
        strip = params.get('strip_components', 0)
        
        try:
            src_path = Path(source)
            dst_path = Path(destination)
            
            if not src_path.exists():
                return HandlerResult(
                    success=False,
                    error=f"Archive not found: {source}",
                )
            
            dst_path.mkdir(parents=True, exist_ok=True)
            
            # Detect archive type
            suffix = ''.join(src_path.suffixes).lower()
            extracted_files = []
            
            if suffix == '.zip':
                with zipfile.ZipFile(src_path, 'r') as zf:
                    for member in zf.namelist():
                        # Strip components if requested
                        if strip > 0:
                            parts = member.split('/')
                            if len(parts) > strip:
                                member = '/'.join(parts[strip:])
                            else:
                                continue
                        
                        zf.extract(member, dst_path)
                        extracted_files.append(str(dst_path / member))
            
            elif suffix in ['.tar.gz', '.tgz']:
                with tarfile.open(src_path, 'r:gz') as tf:
                    for member in tf.getmembers():
                        if strip > 0:
                            parts = member.name.split('/')
                            if len(parts) > strip:
                                member.name = '/'.join(parts[strip:])
                            else:
                                continue
                        tf.extract(member, dst_path)
                        extracted_files.append(str(dst_path / member.name))
            
            elif suffix == '.gz':
                # Single file gzip
                output_name = src_path.stem
                output_path = dst_path / output_name
                with gzip.open(src_path, 'rb') as f_in:
                    with open(output_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                extracted_files.append(str(output_path))
            
            else:
                return HandlerResult(
                    success=False,
                    error=f"Unsupported archive format: {suffix}",
                )
            
            # Calculate extracted size
            bytes_extracted = 0
            for f in extracted_files:
                p = Path(f)
                if p.exists():
                    bytes_extracted += p.stat().st_size
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'source': str(src_path.absolute()),
                    'destination': str(dst_path.absolute()),
                    'files': extracted_files,
                    'bytes_extracted': bytes_extracted,
                },
                duration_ms=duration_ms,
                bytes_read=src_path.stat().st_size,
                bytes_written=bytes_extracted,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class FileListHandler(TaskHandler):
    """List directory contents."""
    
    name = "file_list"
    category = "file"
    description = "List directory contents with filtering"
    requires_filesystem = True
    
    input_schema = {
        'path': {'type': 'string', 'required': True, 'description': 'Directory path'},
        'pattern': {'type': 'string', 'default': '*', 'description': 'Glob pattern'},
        'recursive': {'type': 'boolean', 'default': False, 'description': 'List recursively'},
        'include_hidden': {'type': 'boolean', 'default': False, 'description': 'Include hidden files'},
        'sort_by': {'type': 'string', 'default': 'name', 'enum': ['name', 'size', 'modified']},
    }
    
    output_schema = {
        'path': {'type': 'string', 'description': 'Directory path'},
        'files': {'type': 'array', 'description': 'List of files with metadata'},
        'total_size': {'type': 'integer', 'description': 'Total size in bytes'},
        'count': {'type': 'integer', 'description': 'Number of items'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['file_list', 'list', 'ls', 'dir']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        path = params['path']
        pattern = params.get('pattern', '*')
        recursive = params.get('recursive', False)
        include_hidden = params.get('include_hidden', False)
        sort_by = params.get('sort_by', 'name')
        
        try:
            path_obj = Path(path)
            
            if not path_obj.exists():
                return HandlerResult(
                    success=False,
                    error=f"Path not found: {path}",
                )
            
            if not path_obj.is_dir():
                return HandlerResult(
                    success=False,
                    error=f"Path is not a directory: {path}",
                )
            
            files = []
            total_size = 0
            
            if recursive:
                items = path_obj.rglob(pattern)
            else:
                items = path_obj.glob(pattern)
            
            for item in items:
                # Skip hidden files if not requested
                if not include_hidden and item.name.startswith('.'):
                    continue
                
                stat = item.stat()
                is_dir = item.is_dir()
                size = 0 if is_dir else stat.st_size
                total_size += size
                
                files.append({
                    'name': item.name,
                    'path': str(item.relative_to(path_obj)),
                    'absolute_path': str(item.absolute()),
                    'type': 'directory' if is_dir else 'file',
                    'size': size,
                    'modified': stat.st_mtime,
                })
            
            # Sort
            if sort_by == 'size':
                files.sort(key=lambda x: x['size'])
            elif sort_by == 'modified':
                files.sort(key=lambda x: x['modified'], reverse=True)
            else:
                files.sort(key=lambda x: x['name'])
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'path': str(path_obj.absolute()),
                    'files': files,
                    'total_size': total_size,
                    'count': len(files),
                },
                duration_ms=duration_ms,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class FileChecksumHandler(TaskHandler):
    """Calculate file checksum/hash."""
    
    name = "file_checksum"
    category = "file"
    description = "Calculate file checksum (MD5, SHA1, SHA256, SHA512)"
    requires_filesystem = True
    
    input_schema = {
        'path': {'type': 'string', 'required': True, 'description': 'File path'},
        'algorithm': {'type': 'string', 'default': 'sha256', 'enum': ['md5', 'sha1', 'sha256', 'sha512']},
    }
    
    output_schema = {
        'path': {'type': 'string', 'description': 'File path'},
        'algorithm': {'type': 'string', 'description': 'Algorithm used'},
        'checksum': {'type': 'string', 'description': 'Hex checksum'},
        'size': {'type': 'integer', 'description': 'File size'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['file_checksum', 'checksum', 'hash', 'md5', 'sha256']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        path = params['path']
        algorithm = params.get('algorithm', 'sha256')
        
        try:
            path_obj = Path(path)
            
            if not path_obj.exists():
                return HandlerResult(
                    success=False,
                    error=f"File not found: {path}",
                )
            
            if not path_obj.is_file():
                return HandlerResult(
                    success=False,
                    error=f"Path is not a file: {path}",
                )
            
            # Calculate hash
            hash_func = getattr(hashlib, algorithm)()
            
            with open(path_obj, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    hash_func.update(chunk)
            
            checksum = hash_func.hexdigest()
            file_size = path_obj.stat().st_size
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'path': str(path_obj.absolute()),
                    'algorithm': algorithm,
                    'checksum': checksum,
                    'size': file_size,
                },
                duration_ms=duration_ms,
                bytes_read=file_size,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class FileWatchHandler(TaskHandler):
    """Watch file/directory for changes."""
    
    name = "file_watch"
    category = "file"
    description = "Watch file or directory for changes"
    requires_filesystem = True
    
    input_schema = {
        'path': {'type': 'string', 'required': True, 'description': 'Path to watch'},
        'events': {'type': 'array', 'default': ['created', 'modified', 'deleted'], 'description': 'Events to watch'},
        'timeout_seconds': {'type': 'integer', 'default': 60, 'description': 'Watch timeout'},
        'recursive': {'type': 'boolean', 'default': True, 'description': 'Watch recursively'},
    }
    
    output_schema = {
        'path': {'type': 'string', 'description': 'Watched path'},
        'events': {'type': 'array', 'description': 'Detected events'},
        'timed_out': {'type': 'boolean', 'description': 'Whether watch timed out'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['file_watch', 'watch', 'monitor']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        path = params['path']
        watch_events = params.get('events', ['created', 'modified', 'deleted'])
        timeout = params.get('timeout_seconds', 60)
        recursive = params.get('recursive', True)
        
        try:
            path_obj = Path(path)
            
            if not path_obj.exists():
                return HandlerResult(
                    success=False,
                    error=f"Path not found: {path}",
                )
            
            # Try to use watchdog if available
            try:
                from watchdog.observers import Observer
                from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent, FileDeletedEvent
                
                detected_events = []
                
                class EventHandler(FileSystemEventHandler):
                    def on_any_event(self, event):
                        event_type = None
                        if isinstance(event, FileCreatedEvent):
                            event_type = 'created'
                        elif isinstance(event, FileModifiedEvent):
                            event_type = 'modified'
                        elif isinstance(event, FileDeletedEvent):
                            event_type = 'deleted'
                        
                        if event_type in watch_events:
                            detected_events.append({
                                'type': event_type,
                                'path': event.src_path,
                                'timestamp': time.time(),
                            })
                
                observer = Observer()
                handler = EventHandler()
                observer.schedule(handler, str(path_obj), recursive=recursive)
                observer.start()
                
                # Wait for events or timeout
                time.sleep(timeout)
                observer.stop()
                observer.join()
                
                duration_ms = (time.time() - start_time) * 1000
                
                return HandlerResult(
                    success=True,
                    data={
                        'path': str(path_obj.absolute()),
                        'events': detected_events,
                        'timed_out': len(detected_events) == 0,
                    },
                    duration_ms=duration_ms,
                )
                
            except ImportError:
                # Fallback: Poll for changes
                initial_mtime = path_obj.stat().st_mtime
                detected_events = []
                
                poll_interval = 0.5
                elapsed = 0
                
                while elapsed < timeout:
                    time.sleep(poll_interval)
                    elapsed += poll_interval
                    
                    try:
                        current_mtime = path_obj.stat().st_mtime
                        if current_mtime != initial_mtime:
                            detected_events.append({
                                'type': 'modified',
                                'path': str(path_obj),
                                'timestamp': time.time(),
                            })
                            initial_mtime = current_mtime
                    except FileNotFoundError:
                        detected_events.append({
                            'type': 'deleted',
                            'path': str(path_obj),
                            'timestamp': time.time(),
                        })
                        break
                
                duration_ms = (time.time() - start_time) * 1000
                
                return HandlerResult(
                    success=True,
                    data={
                        'path': str(path_obj.absolute()),
                        'events': detected_events,
                        'timed_out': len(detected_events) == 0,
                    },
                    duration_ms=duration_ms,
                    warnings=['watchdog not installed, using polling fallback'],
                )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )
