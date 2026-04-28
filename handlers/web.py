"""
Web operation handlers.

Handlers for HTTP requests, web scraping, API calls.
"""

import time
import json
import urllib.request
import urllib.error
import urllib.parse
from typing import Dict, Any, Optional, List
from http.client import HTTPResponse
import ssl

from handlers.base import (
    TaskHandler,
    HandlerResult,
    HandlerConfidence,
)


class WebFetchHandler(TaskHandler):
    """Fetch URL content with retries and timeout."""
    
    name = "web_fetch"
    category = "web"
    description = "Fetch URL content with configurable retries and timeout"
    
    input_schema = {
        'url': {'type': 'string', 'required': True, 'description': 'URL to fetch'},
        'method': {'type': 'string', 'default': 'GET', 'enum': ['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS']},
        'headers': {'type': 'object', 'description': 'Request headers'},
        'body': {'type': 'any', 'description': 'Request body'},
        'timeout': {'type': 'integer', 'default': 30, 'description': 'Timeout in seconds'},
        'retries': {'type': 'integer', 'default': 3, 'description': 'Number of retries'},
        'follow_redirects': {'type': 'boolean', 'default': True, 'description': 'Follow redirects'},
        'verify_ssl': {'type': 'boolean', 'default': True, 'description': 'Verify SSL certificates'},
    }
    
    output_schema = {
        'status_code': {'type': 'integer', 'description': 'HTTP status code'},
        'headers': {'type': 'object', 'description': 'Response headers'},
        'body': {'type': 'string', 'description': 'Response body'},
        'url': {'type': 'string', 'description': 'Final URL (after redirects)'},
        'elapsed_seconds': {'type': 'float', 'description': 'Request time'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        task = params.get('task', '')
        if task in ['web_fetch', 'fetch', 'http_get', 'http', 'get', 'request']:
            return HandlerConfidence.PERFECT.value
        if 'url' in params and params.get('method', 'GET') in ['GET', 'POST', 'PUT', 'DELETE']:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        url = params['url']
        method = params.get('method', 'GET').upper()
        headers = params.get('headers', {})
        body = params.get('body')
        timeout = params.get('timeout', 30)
        retries = params.get('retries', 3)
        follow_redirects = params.get('follow_redirects', True)
        verify_ssl = params.get('verify_ssl', True)
        
        last_error = None
        
        for attempt in range(retries):
            try:
                # Prepare request
                if isinstance(body, (dict, list)):
                    body = json.dumps(body).encode('utf-8')
                    headers.setdefault('Content-Type', 'application/json')
                elif isinstance(body, str):
                    body = body.encode('utf-8')
                
                req = urllib.request.Request(url, data=body, headers=headers, method=method)
                
                # SSL context
                ctx = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()
                
                # Make request
                response = urllib.request.urlopen(req, timeout=timeout, context=ctx)
                
                # Read response
                response_body = response.read().decode('utf-8', errors='replace')
                
                # Build result
                result_headers = dict(response.headers)
                final_url = response.geturl()
                status_code = response.status
                
                elapsed = time.time() - start_time
                
                # Parse JSON if content-type indicates
                if 'application/json' in result_headers.get('Content-Type', ''):
                    try:
                        response_body = json.loads(response_body)
                    except json.JSONDecodeError:
                        pass
                
                return HandlerResult(
                    success=True,
                    data={
                        'status_code': status_code,
                        'headers': result_headers,
                        'body': response_body,
                        'url': final_url,
                        'elapsed_seconds': elapsed,
                    },
                    duration_ms=elapsed * 1000,
                )
                
            except urllib.error.HTTPError as e:
                # HTTP error - return response with error status
                elapsed = time.time() - start_time
                return HandlerResult(
                    success=False,
                    error=f"HTTP {e.code}: {e.reason}",
                    data={
                        'status_code': e.code,
                        'headers': dict(e.headers) if e.headers else {},
                        'body': e.read().decode('utf-8', errors='replace') if e.fp else '',
                        'url': url,
                    },
                    duration_ms=elapsed * 1000,
                )
                
            except urllib.error.URLError as e:
                last_error = str(e.reason)
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                continue
                
            except Exception as e:
                last_error = str(e)
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                continue
        
        return HandlerResult(
            success=False,
            error=f"Failed after {retries} attempts: {last_error}",
            duration_ms=(time.time() - start_time) * 1000,
        )


class WebScrapeHandler(TaskHandler):
    """Scrape web page and extract content."""
    
    name = "web_scrape"
    category = "web"
    description = "Scrape web page and extract structured content"
    
    input_schema = {
        'url': {'type': 'string', 'required': True, 'description': 'URL to scrape'},
        'selector': {'type': 'string', 'description': 'CSS selector to extract'},
        'extract': {'type': 'array', 'default': ['text'], 'description': 'What to extract: text, links, images'},
        'follow_links': {'type': 'boolean', 'default': False, 'description': 'Follow links on page'},
        'max_depth': {'type': 'integer', 'default': 1, 'description': 'Max link depth to follow'},
    }
    
    output_schema = {
        'url': {'type': 'string', 'description': 'Scraped URL'},
        'title': {'type': 'string', 'description': 'Page title'},
        'text': {'type': 'string', 'description': 'Extracted text'},
        'links': {'type': 'array', 'description': 'Extracted links'},
        'images': {'type': 'array', 'description': 'Extracted images'},
        'elements': {'type': 'array', 'description': 'Elements matching selector'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['web_scrape', 'scrape', 'scrape_web', 'crawl']:
            return HandlerConfidence.PERFECT.value
        if 'url' in params and 'selector' in params:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        url = params['url']
        selector = params.get('selector')
        extract = params.get('extract', ['text'])
        
        try:
            # Fetch the page
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            ctx = ssl.create_default_context()
            response = urllib.request.urlopen(req, timeout=30, context=ctx)
            html = response.read().decode('utf-8', errors='replace')
            
            # Try BeautifulSoup if available
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, 'html.parser')
                
                result = {
                    'url': url,
                    'title': soup.title.string if soup.title else '',
                }
                
                # Extract text
                if 'text' in extract:
                    # Remove script and style elements
                    for element in soup(['script', 'style', 'nav', 'footer']):
                        element.decompose()
                    result['text'] = soup.get_text(separator='\n', strip=True)
                
                # Extract links
                if 'links' in extract:
                    result['links'] = [
                        {'text': a.get_text(strip=True), 'href': a.get('href', '')}
                        for a in soup.find_all('a', href=True)
                    ]
                
                # Extract images
                if 'images' in extract:
                    result['images'] = [
                        {'alt': img.get('alt', ''), 'src': img.get('src', '')}
                        for img in soup.find_all('img', src=True)
                    ]
                
                # Extract by selector
                if selector:
                    elements = soup.select(selector)
                    result['elements'] = [e.get_text(strip=True) for e in elements]
                
            except ImportError:
                # Fallback: simple regex extraction
                import re
                
                result = {'url': url}
                
                # Title
                title_match = re.search(r'<title[^>]*>([^<]+)</title>', html, re.IGNORECASE)
                result['title'] = title_match.group(1) if title_match else ''
                
                # Text (crude: strip tags)
                if 'text' in extract:
                    text = re.sub(r'<[^>]+>', ' ', html)
                    text = re.sub(r'\s+', ' ', text).strip()
                    result['text'] = text
                
                # Links
                if 'links' in extract:
                    result['links'] = [
                        {'text': m.group(2), 'href': m.group(1)}
                        for m in re.finditer(r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>([^<]+)</a>', html, re.IGNORECASE)
                    ]
                
                # Images
                if 'images' in extract:
                    result['images'] = [
                        {'alt': m.group(2) or '', 'src': m.group(1)}
                        for m in re.finditer(r'<img[^>]*src=["\']([^"\']+)["\'][^>]*alt=["\']([^"\']*)["\'][^>]*/>', html, re.IGNORECASE)
                    ]
                
                if selector:
                    result['elements'] = []
                    result['warnings'] = ['BeautifulSoup not available, selector extraction skipped']
            
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


class WebAPIHandler(TaskHandler):
    """Call REST API with JSON handling."""
    
    name = "web_api"
    category = "web"
    description = "Call REST API with automatic JSON handling"
    
    input_schema = {
        'url': {'type': 'string', 'required': True, 'description': 'API endpoint URL'},
        'method': {'type': 'string', 'default': 'GET'},
        'headers': {'type': 'object', 'description': 'Request headers'},
        'params': {'type': 'object', 'description': 'Query parameters'},
        'body': {'type': 'object', 'description': 'Request body (for POST/PUT/PATCH)'},
        'auth_token': {'type': 'string', 'description': 'Bearer token for Authorization header'},
        'api_key': {'type': 'string', 'description': 'API key for X-API-Key header'},
    }
    
    output_schema = {
        'status_code': {'type': 'integer'},
        'data': {'type': 'any', 'description': 'Parsed JSON response'},
        'headers': {'type': 'object'},
        'elapsed_seconds': {'type': 'float'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['web_api', 'api_call', 'api', 'rest']:
            return HandlerConfidence.PERFECT.value
        if params.get('url') and ('api' in params.get('url', '').lower() or params.get('body')):
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        url = params['url']
        method = params.get('method', 'GET').upper()
        headers = dict(params.get('headers', {}))
        query_params = params.get('params', {})
        body = params.get('body')
        
        # Add auth headers
        if params.get('auth_token'):
            headers['Authorization'] = f"Bearer {params['auth_token']}"
        if params.get('api_key'):
            headers['X-API-Key'] = params['api_key']
        
        headers.setdefault('Content-Type', 'application/json')
        headers.setdefault('Accept', 'application/json')
        
        try:
            # Add query params
            if query_params:
                parsed = urllib.parse.urlparse(url)
                existing = urllib.parse.parse_qs(parsed.query)
                existing.update({k: [str(v)] for k, v in query_params.items()})
                new_query = urllib.parse.urlencode(existing, doseq=True)
                url = urllib.parse.urlunparse(parsed._replace(query=new_query))
            
            # Prepare body
            data = None
            if body and method in ['POST', 'PUT', 'PATCH']:
                data = json.dumps(body).encode('utf-8')
            
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            ctx = ssl.create_default_context()
            
            response = urllib.request.urlopen(req, timeout=60, context=ctx)
            response_body = response.read().decode('utf-8')
            
            # Parse JSON
            try:
                response_data = json.loads(response_body)
            except json.JSONDecodeError:
                response_data = response_body
            
            elapsed = time.time() - start_time
            
            return HandlerResult(
                success=True,
                data={
                    'status_code': response.status,
                    'data': response_data,
                    'headers': dict(response.headers),
                    'elapsed_seconds': elapsed,
                },
                duration_ms=elapsed * 1000,
            )
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else ''
            try:
                error_data = json.loads(error_body)
            except:
                error_data = error_body
            
            return HandlerResult(
                success=False,
                error=f"HTTP {e.code}: {e.reason}",
                data={
                    'status_code': e.code,
                    'data': error_data,
                    'headers': dict(e.headers) if e.headers else {},
                },
                duration_ms=(time.time() - start_time) * 1000,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class WebDownloadHandler(TaskHandler):
    """Download file from URL."""
    
    name = "web_download"
    category = "web"
    description = "Download file from URL with progress tracking"
    requires_filesystem = True
    
    input_schema = {
        'url': {'type': 'string', 'required': True, 'description': 'File URL'},
        'path': {'type': 'string', 'required': True, 'description': 'Local save path'},
        'chunk_size': {'type': 'integer', 'default': 8192, 'description': 'Download chunk size'},
        'resume': {'type': 'boolean', 'default': False, 'description': 'Resume partial download'},
    }
    
    output_schema = {
        'url': {'type': 'string'},
        'path': {'type': 'string'},
        'bytes_downloaded': {'type': 'integer'},
        'elapsed_seconds': {'type': 'float'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['web_download', 'download', 'download_file']:
            return HandlerConfidence.PERFECT.value
        if 'url' in params and 'path' in params:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        import os
        start_time = time.time()
        url = params['url']
        path = params['path']
        chunk_size = params.get('chunk_size', 8192)
        resume = params.get('resume', False)
        
        try:
            # Create directory
            os.makedirs(os.path.dirname(path) if os.path.dirname(path) else '.', exist_ok=True)
            
            # Resume support
            existing_size = 0
            mode = 'wb'
            if resume and os.path.exists(path):
                existing_size = os.path.getsize(path)
                mode = 'ab'
            
            headers = {}
            if existing_size > 0:
                headers['Range'] = f'bytes={existing_size}-'
            
            req = urllib.request.Request(url, headers=headers)
            ctx = ssl.create_default_context()
            response = urllib.request.urlopen(req, timeout=300, context=ctx)
            
            # Check if server supports resume
            if existing_size > 0 and response.status not in [206, 200]:
                # Server doesn't support range, start fresh
                mode = 'wb'
                existing_size = 0
            
            bytes_downloaded = existing_size
            
            with open(path, mode) as f:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
            
            elapsed = time.time() - start_time
            
            return HandlerResult(
                success=True,
                data={
                    'url': url,
                    'path': os.path.abspath(path),
                    'bytes_downloaded': bytes_downloaded,
                    'elapsed_seconds': elapsed,
                },
                duration_ms=elapsed * 1000,
                bytes_written=bytes_downloaded - existing_size,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class WebSubmitHandler(TaskHandler):
    """Submit HTML form."""
    
    name = "web_submit"
    category = "web"
    description = "Submit HTML form data"
    
    input_schema = {
        'url': {'type': 'string', 'required': True, 'description': 'Form action URL'},
        'fields': {'type': 'object', 'required': True, 'description': 'Form fields'},
        'method': {'type': 'string', 'default': 'POST'},
        'multipart': {'type': 'boolean', 'default': False, 'description': 'Use multipart/form-data'},
        'files': {'type': 'object', 'description': 'Files to upload'},
    }
    
    output_schema = {
        'status_code': {'type': 'integer'},
        'body': {'type': 'string'},
        'redirect_url': {'type': 'string'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['web_submit', 'submit_form', 'form_submit']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        url = params['url']
        fields = params['fields']
        method = params.get('method', 'POST').upper()
        multipart = params.get('multipart', False)
        files = params.get('files', {})
        
        try:
            # Encode form data
            if multipart or files:
                # Multipart encoding
                boundary = '----WebKitFormBoundary' + str(time.time()).replace('.', '')
                body_parts = []
                
                for key, value in fields.items():
                    body_parts.append(f'--{boundary}\r\n')
                    body_parts.append(f'Content-Disposition: form-data; name="{key}"\r\n\r\n')
                    body_parts.append(f'{value}\r\n')
                
                for key, file_info in files.items():
                    filepath = file_info.get('path')
                    filename = file_info.get('filename', os.path.basename(filepath))
                    with open(filepath, 'rb') as f:
                        file_content = f.read()
                    body_parts.append(f'--{boundary}\r\n')
                    body_parts.append(f'Content-Disposition: form-data; name="{key}"; filename="{filename}"\r\n')
                    body_parts.append(f'Content-Type: {file_info.get("content_type", "application/octet-stream")}\r\n\r\n')
                    body_parts.append(file_content)
                    body_parts.append(b'\r\n')
                
                body_parts.append(f'--{boundary}--\r\n')
                
                import os
                body = b''
                for part in body_parts:
                    if isinstance(part, str):
                        body += part.encode('utf-8')
                    else:
                        body += part
                
                content_type = f'multipart/form-data; boundary={boundary}'
            else:
                # URL-encoded
                body = urllib.parse.urlencode(fields).encode('utf-8')
                content_type = 'application/x-www-form-urlencoded'
            
            headers = {'Content-Type': content_type}
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            ctx = ssl.create_default_context()
            
            response = urllib.request.urlopen(req, timeout=60, context=ctx)
            response_body = response.read().decode('utf-8', errors='replace')
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'status_code': response.status,
                    'body': response_body,
                    'redirect_url': response.geturl() if response.geturl() != url else None,
                    'headers': dict(response.headers),
                },
                duration_ms=duration_ms,
            )
            
        except urllib.error.HTTPError as e:
            return HandlerResult(
                success=False,
                error=f"HTTP {e.code}: {e.reason}",
                data={'status_code': e.code, 'body': e.read().decode('utf-8', errors='replace')},
                duration_ms=(time.time() - start_time) * 1000,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class WebHealthCheckHandler(TaskHandler):
    """Check website/endpoint health."""
    
    name = "web_health"
    category = "web"
    description = "Check website/endpoint health and response time"
    
    input_schema = {
        'url': {'type': 'string', 'required': True, 'description': 'URL to check'},
        'expected_status': {'type': 'integer', 'default': 200, 'description': 'Expected HTTP status'},
        'expected_content': {'type': 'string', 'description': 'Expected content in response'},
        'timeout': {'type': 'integer', 'default': 10, 'description': 'Timeout in seconds'},
    }
    
    output_schema = {
        'healthy': {'type': 'boolean'},
        'status_code': {'type': 'integer'},
        'response_time_ms': {'type': 'integer'},
        'checks': {'type': 'array', 'description': 'Individual check results'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['web_health', 'health_check', 'healthcheck', 'ping']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        url = params['url']
        expected_status = params.get('expected_status', 200)
        expected_content = params.get('expected_content')
        timeout = params.get('timeout', 10)
        
        checks = []
        healthy = True
        
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'HealthChecker/1.0'})
            ctx = ssl.create_default_context()
            
            response_start = time.time()
            response = urllib.request.urlopen(req, timeout=timeout, context=ctx)
            response_time_ms = int((time.time() - response_start) * 1000)
            
            # Status check
            status_ok = response.status == expected_status
            checks.append({
                'name': 'status_code',
                'expected': expected_status,
                'actual': response.status,
                'passed': status_ok,
            })
            if not status_ok:
                healthy = False
            
            # Response time check
            response_time_ok = response_time_ms < timeout * 1000
            checks.append({
                'name': 'response_time',
                'expected': f'<{timeout*1000}ms',
                'actual': f'{response_time_ms}ms',
                'passed': response_time_ok,
            })
            
            # Content check
            if expected_content:
                body = response.read().decode('utf-8', errors='replace')
                content_ok = expected_content in body
                checks.append({
                    'name': 'content',
                    'expected': f'contains "{expected_content}"',
                    'actual': 'found' if content_ok else 'not found',
                    'passed': content_ok,
                })
                if not content_ok:
                    healthy = False
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'healthy': healthy,
                    'status_code': response.status,
                    'response_time_ms': response_time_ms,
                    'checks': checks,
                },
                duration_ms=duration_ms,
            )
            
        except urllib.error.HTTPError as e:
            checks.append({
                'name': 'status_code',
                'expected': expected_status,
                'actual': e.code,
                'passed': e.code == expected_status,
            })
            
            return HandlerResult(
                success=True,
                data={
                    'healthy': e.code == expected_status,
                    'status_code': e.code,
                    'response_time_ms': int((time.time() - start_time) * 1000),
                    'checks': checks,
                    'error': str(e),
                },
                duration_ms=(time.time() - start_time) * 1000,
            )
            
        except Exception as e:
            checks.append({
                'name': 'connection',
                'expected': 'success',
                'actual': str(e),
                'passed': False,
            })
            
            return HandlerResult(
                success=True,
                data={
                    'healthy': False,
                    'status_code': None,
                    'response_time_ms': int((time.time() - start_time) * 1000),
                    'checks': checks,
                    'error': str(e),
                },
                duration_ms=(time.time() - start_time) * 1000,
            )


class WebProxyHandler(TaskHandler):
    """Make request through proxy."""
    
    name = "web_proxy"
    category = "web"
    description = 'Make HTTP request through proxy server'
    
    input_schema = {
        'url': {'type': 'string', 'required': True},
        'proxy': {'type': 'string', 'required': True, 'description': 'Proxy URL (http://host:port)'},
        'method': {'type': 'string', 'default': 'GET'},
        'headers': {'type': 'object'},
        'timeout': {'type': 'integer', 'default': 30},
    }
    
    output_schema = {
        'status_code': {'type': 'integer'},
        'body': {'type': 'string'},
        'proxy_used': {'type': 'string'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['web_proxy', 'proxy_request']:
            return HandlerConfidence.PERFECT.value
        if 'url' in params and 'proxy' in params:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        url = params['url']
        proxy = params['proxy']
        method = params.get('method', 'GET')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)
        
        try:
            # Parse proxy
            proxy_parsed = urllib.parse.urlparse(proxy)
            
            # Set up proxy handler
            proxy_handler = urllib.request.ProxyHandler({
                'http': proxy,
                'https': proxy,
            })
            opener = urllib.request.build_opener(proxy_handler)
            
            req = urllib.request.Request(url, headers=headers, method=method)
            response = opener.open(req, timeout=timeout)
            
            body = response.read().decode('utf-8', errors='replace')
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'status_code': response.status,
                    'body': body,
                    'proxy_used': proxy,
                    'headers': dict(response.headers),
                },
                duration_ms=duration_ms,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class WebGraphQLHandler(TaskHandler):
    """Execute GraphQL query."""
    
    name = "web_graphql"
    category = "web"
    description = 'Execute GraphQL query against endpoint'
    
    input_schema = {
        'url': {'type': 'string', 'required': True, 'description': 'GraphQL endpoint URL'},
        'query': {'type': 'string', 'required': True, 'description': 'GraphQL query'},
        'variables': {'type': 'object', 'description': 'Query variables'},
        'operation_name': {'type': 'string', 'description': 'Operation name'},
        'headers': {'type': 'object', 'description': 'Additional headers'},
    }
    
    output_schema = {
        'data': {'type': 'any', 'description': 'Query result'},
        'errors': {'type': 'array', 'description': 'GraphQL errors'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['web_graphql', 'graphql', 'gql']:
            return HandlerConfidence.PERFECT.value
        if 'query' in params and params.get('url', '').endswith('/graphql'):
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        url = params['url']
        query = params['query']
        variables = params.get('variables', {})
        operation_name = params.get('operation_name')
        headers = params.get('headers', {})
        
        try:
            # Build request body
            body = {'query': query}
            if variables:
                body['variables'] = variables
            if operation_name:
                body['operationName'] = operation_name
            
            headers.setdefault('Content-Type', 'application/json')
            
            req = urllib.request.Request(
                url,
                data=json.dumps(body).encode('utf-8'),
                headers=headers,
            )
            ctx = ssl.create_default_context()
            response = urllib.request.urlopen(req, timeout=60, context=ctx)
            
            result = json.loads(response.read().decode('utf-8'))
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success='errors' not in result,
                data=result,
                duration_ms=duration_ms,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )
