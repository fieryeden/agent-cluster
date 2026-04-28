"""
Integration handlers.

Handlers for third-party APIs and services.
"""

import time
import json
import urllib.request
import urllib.error
import ssl
import base64
import hashlib
import hmac
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from handlers.base import (
    TaskHandler,
    HandlerResult,
    HandlerConfidence,
)


class GitHubHandler(TaskHandler):
    """GitHub API operations."""
    
    name = "github"
    category = "integration"
    description = "GitHub API operations"
    
    input_schema = {
        'operation': {'type': 'string', 'required': True, 'enum': ['create_issue', 'get_issue', 'list_issues', 'create_pr', 'get_repo', 'list_repos']},
        'owner': {'type': 'string', 'description': 'Repository owner'},
        'repo': {'type': 'string', 'description': 'Repository name'},
        'title': {'type': 'string', 'description': 'Issue/PR title'},
        'body': {'type': 'string', 'description': 'Issue/PR body'},
        'labels': {'type': 'array', 'description': 'Issue labels'},
        'state': {'type': 'string', 'default': 'open', 'enum': ['open', 'closed', 'all']},
        'token': {'type': 'string', 'description': 'GitHub personal access token'},
    }
    
    output_schema = {
        'number': {'type': 'integer', 'description': 'Issue/PR number'},
        'html_url': {'type': 'string'},
        'items': {'type': 'array'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['github', 'gh', 'create_issue', 'github_issue']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        operation = params['operation']
        owner = params.get('owner')
        repo = params.get('repo')
        token = params.get('token') or os.environ.get('GITHUB_TOKEN')
        
        try:
            base_url = 'https://api.github.com'
            headers = {'Accept': 'application/vnd.github.v3+json'}
            if token:
                headers['Authorization'] = f'token {token}'
            
            if operation == 'create_issue':
                if not all([owner, repo, params.get('title')]):
                    return HandlerResult(success=False, error="owner, repo, and title required")
                
                data = {
                    'title': params['title'],
                    'body': params.get('body', ''),
                    'labels': params.get('labels', []),
                }
                
                req = urllib.request.Request(
                    f'{base_url}/repos/{owner}/{repo}/issues',
                    data=json.dumps(data).encode('utf-8'),
                    headers=headers,
                    method='POST',
                )
                
            elif operation == 'get_issue':
                if not all([owner, repo, params.get('number')]):
                    return HandlerResult(success=False, error="owner, repo, and number required")
                
                req = urllib.request.Request(
                    f'{base_url}/repos/{owner}/{repo}/issues/{params["number"]}',
                    headers=headers,
                )
            
            elif operation == 'list_issues':
                if not all([owner, repo]):
                    return HandlerResult(success=False, error="owner and repo required")
                
                query = f'state={params.get("state", "open")}'
                req = urllib.request.Request(
                    f'{base_url}/repos/{owner}/{repo}/issues?{query}',
                    headers=headers,
                )
            
            elif operation == 'create_pr':
                if not all([owner, repo, params.get('title'), params.get('head'), params.get('base')]):
                    return HandlerResult(success=False, error="owner, repo, title, head, and base required")
                
                data = {
                    'title': params['title'],
                    'body': params.get('body', ''),
                    'head': params['head'],
                    'base': params['base'],
                }
                
                req = urllib.request.Request(
                    f'{base_url}/repos/{owner}/{repo}/pulls',
                    data=json.dumps(data).encode('utf-8'),
                    headers=headers,
                    method='POST',
                )
            
            elif operation == 'get_repo':
                if not all([owner, repo]):
                    return HandlerResult(success=False, error="owner and repo required")
                
                req = urllib.request.Request(
                    f'{base_url}/repos/{owner}/{repo}',
                    headers=headers,
                )
            
            elif operation == 'list_repos':
                if not owner:
                    return HandlerResult(success=False, error="owner required")
                
                req = urllib.request.Request(
                    f'{base_url}/users/{owner}/repos',
                    headers=headers,
                )
            
            else:
                return HandlerResult(success=False, error=f"Unknown operation: {operation}")
            
            ctx = ssl.create_default_context()
            response = urllib.request.urlopen(req, timeout=30, context=ctx)
            result = json.loads(response.read().decode('utf-8'))
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data=result if isinstance(result, dict) else {'items': result},
                duration_ms=duration_ms,
            )
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            return HandlerResult(
                success=False,
                error=f"HTTP {e.code}: {error_body}",
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


import os

class StripeHandler(TaskHandler):
    """Stripe API operations."""
    
    name = "stripe"
    category = "integration"
    description = "Stripe payment API operations"
    dangerous = True
    
    input_schema = {
        'operation': {'type': 'string', 'required': True, 'enum': ['create_charge', 'create_customer', 'get_customer', 'list_charges', 'create_refund']},
        'amount': {'type': 'integer', 'description': 'Amount in cents'},
        'currency': {'type': 'string', 'default': 'usd', 'description': 'Currency code'},
        'customer': {'type': 'string', 'description': 'Customer ID'},
        'source': {'type': 'string', 'description': 'Payment source token'},
        'email': {'type': 'string', 'description': 'Customer email'},
        'api_key': {'type': 'string', 'description': 'Stripe API key'},
    }
    
    output_schema = {
        'id': {'type': 'string', 'description': 'Stripe object ID'},
        'status': {'type': 'string'},
        'amount': {'type': 'integer'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['stripe', 'payment', 'charge', 'stripe_charge']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        operation = params['operation']
        api_key = params.get('api_key') or os.environ.get('STRIPE_API_KEY')
        
        try:
            if not api_key:
                return HandlerResult(success=False, error="Stripe API key required")
            
            base_url = 'https://api.stripe.com/v1'
            auth_header = base64.b64encode(f'{api_key}:'.encode()).decode()
            headers = {
                'Authorization': f'Basic {auth_header}',
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            
            if operation == 'create_charge':
                data = urllib.parse.urlencode({
                    'amount': params['amount'],
                    'currency': params.get('currency', 'usd'),
                    'source': params.get('source'),
                    'customer': params.get('customer'),
                    'description': params.get('description', 'Charge via agent'),
                }).encode()
                
                req = urllib.request.Request(
                    f'{base_url}/charges',
                    data=data,
                    headers=headers,
                    method='POST',
                )
            
            elif operation == 'create_customer':
                customer_data = {}
                if params.get('email'):
                    customer_data['email'] = params['email']
                if params.get('source'):
                    customer_data['source'] = params['source']
                
                data = urllib.parse.urlencode(customer_data).encode()
                req = urllib.request.Request(
                    f'{base_url}/customers',
                    data=data,
                    headers=headers,
                    method='POST',
                )
            
            elif operation == 'get_customer':
                if not params.get('customer'):
                    return HandlerResult(success=False, error="customer ID required")
                
                req = urllib.request.Request(
                    f'{base_url}/customers/{params["customer"]}',
                    headers=headers,
                )
            
            elif operation == 'list_charges':
                req = urllib.request.Request(
                    f'{base_url}/charges',
                    headers=headers,
                )
            
            elif operation == 'create_refund':
                if not params.get('charge'):
                    return HandlerResult(success=False, error="charge ID required")
                
                data = urllib.parse.urlencode({
                    'charge': params['charge'],
                    'amount': params.get('amount'),
                }).encode()
                
                req = urllib.request.Request(
                    f'{base_url}/refunds',
                    data=data,
                    headers=headers,
                    method='POST',
                )
            
            else:
                return HandlerResult(success=False, error=f"Unknown operation: {operation}")
            
            ctx = ssl.create_default_context()
            response = urllib.request.urlopen(req, timeout=30, context=ctx)
            result = json.loads(response.read().decode('utf-8'))
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data=result,
                duration_ms=duration_ms,
            )
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            return HandlerResult(
                success=False,
                error=f"HTTP {e.code}: {error_body}",
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class TwilioHandler(TaskHandler):
    """Twilio SMS/Voice operations."""
    
    name = "twilio"
    category = "integration"
    description = "Twilio SMS and Voice operations"
    dangerous = True
    
    input_schema = {
        'operation': {'type': 'string', 'required': True, 'enum': ['send_sms', 'get_message', 'list_messages', 'make_call']},
        'to': {'type': 'string', 'description': 'Destination phone number'},
        'from': {'type': 'string', 'description': 'From phone number (Twilio number)'},
        'body': {'type': 'string', 'description': 'Message body'},
        'url': {'type': 'string', 'description': 'Voice URL for calls'},
        'account_sid': {'type': 'string', 'description': 'Twilio Account SID'},
        'auth_token': {'type': 'string', 'description': 'Twilio Auth Token'},
    }
    
    output_schema = {
        'sid': {'type': 'string', 'description': 'Message SID'},
        'status': {'type': 'string'},
        'to': {'type': 'string'},
        'from': {'type': 'string'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['twilio', 'sms', 'send_sms', 'voice']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        import urllib.parse
        
        start_time = time.time()
        operation = params['operation']
        account_sid = params.get('account_sid') or os.environ.get('TWILIO_ACCOUNT_SID')
        auth_token = params.get('auth_token') or os.environ.get('TWILIO_AUTH_TOKEN')
        
        try:
            if not account_sid or not auth_token:
                return HandlerResult(success=False, error="Twilio Account SID and Auth Token required")
            
            base_url = f'https://api.twilio.com/2010-04-01/Accounts/{account_sid}'
            auth_header = base64.b64encode(f'{account_sid}:{auth_token}'.encode()).decode()
            headers = {
                'Authorization': f'Basic {auth_header}',
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            
            if operation == 'send_sms':
                if not all([params.get('to'), params.get('from'), params.get('body')]):
                    return HandlerResult(success=False, error="to, from, and body required")
                
                data = urllib.parse.urlencode({
                    'To': params['to'],
                    'From': params['from'],
                    'Body': params['body'],
                }).encode()
                
                req = urllib.request.Request(
                    f'{base_url}/Messages.json',
                    data=data,
                    headers=headers,
                    method='POST',
                )
            
            elif operation == 'get_message':
                if not params.get('sid'):
                    return HandlerResult(success=False, error="Message SID required")
                
                req = urllib.request.Request(
                    f'{base_url}/Messages/{params["sid"]}.json',
                    headers=headers,
                )
            
            elif operation == 'list_messages':
                req = urllib.request.Request(
                    f'{base_url}/Messages.json',
                    headers=headers,
                )
            
            elif operation == 'make_call':
                if not all([params.get('to'), params.get('from'), params.get('url')]):
                    return HandlerResult(success=False, error="to, from, and url required")
                
                data = urllib.parse.urlencode({
                    'To': params['to'],
                    'From': params['from'],
                    'Url': params['url'],
                }).encode()
                
                req = urllib.request.Request(
                    f'{base_url}/Calls.json',
                    data=data,
                    headers=headers,
                    method='POST',
                )
            
            else:
                return HandlerResult(success=False, error=f"Unknown operation: {operation}")
            
            ctx = ssl.create_default_context()
            response = urllib.request.urlopen(req, timeout=30, context=ctx)
            result = json.loads(response.read().decode('utf-8'))
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data=result,
                duration_ms=duration_ms,
            )
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            return HandlerResult(
                success=False,
                error=f"HTTP {e.code}: {error_body}",
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class SlackAPIHandler(TaskHandler):
    """Slack Web API operations (more than webhooks)."""
    
    name = "slack_api"
    category = "integration"
    description = "Slack Web API operations"
    dangerous = True
    
    input_schema = {
        'operation': {'type': 'string', 'required': True, 'enum': ['post_message', 'get_channels', 'get_users', 'upload_file', 'search']},
        'channel': {'type': 'string', 'description': 'Channel ID'},
        'text': {'type': 'string', 'description': 'Message text'},
        'file': {'type': 'string', 'description': 'File path for upload'},
        'query': {'type': 'string', 'description': 'Search query'},
        'token': {'type': 'string', 'description': 'Slack API token'},
    }
    
    output_schema = {
        'ok': {'type': 'boolean'},
        'messages': {'type': 'array'},
        'channels': {'type': 'array'},
        'users': {'type': 'array'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['slack_api', 'slack_search', 'slack_upload']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        import urllib.parse
        
        start_time = time.time()
        operation = params['operation']
        token = params.get('token') or os.environ.get('SLACK_TOKEN')
        
        try:
            if not token:
                return HandlerResult(success=False, error="Slack API token required")
            
            base_url = 'https://slack.com/api'
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
            }
            
            if operation == 'post_message':
                if not all([params.get('channel'), params.get('text')]):
                    return HandlerResult(success=False, error="channel and text required")
                
                data = json.dumps({
                    'channel': params['channel'],
                    'text': params['text'],
                }).encode()
                
                req = urllib.request.Request(
                    f'{base_url}/chat.postMessage',
                    data=data,
                    headers=headers,
                    method='POST',
                )
            
            elif operation == 'get_channels':
                req = urllib.request.Request(
                    f'{base_url}/conversations.list',
                    headers=headers,
                )
            
            elif operation == 'get_users':
                req = urllib.request.Request(
                    f'{base_url}/users.list',
                    headers=headers,
                )
            
            elif operation == 'upload_file':
                if not all([params.get('channel'), params.get('file')]):
                    return HandlerResult(success=False, error="channel and file required")
                
                filepath = Path(params['file'])
                if not filepath.exists():
                    return HandlerResult(success=False, error=f"File not found: {params['file']}")
                
                # Use form upload
                boundary = '----SlackUploadBoundary' + str(time.time()).replace('.', '')
                with open(filepath, 'rb') as f:
                    file_content = f.read()
                
                body = f'--{boundary}\r\n'
                body += 'Content-Disposition: form-data; name="file"; filename="' + filepath.name + '"\r\n'
                body += 'Content-Type: application/octet-stream\r\n\r\n'
                body_bytes = body.encode('utf-8') + file_content
                body_bytes += f'\r\n--{boundary}\r\n'.encode('utf-8')
                body_bytes += f'Content-Disposition: form-data; name="channels"\r\n\r\n{params["channel"]}\r\n'.encode('utf-8')
                body_bytes += f'--{boundary}--\r\n'.encode('utf-8')
                
                headers = {
                    'Authorization': f'Bearer {token}',
                    'Content-Type': f'multipart/form-data; boundary={boundary}',
                }
                
                req = urllib.request.Request(
                    f'{base_url}/files.upload',
                    data=body_bytes,
                    headers=headers,
                    method='POST',
                )
            
            elif operation == 'search':
                if not params.get('query'):
                    return HandlerResult(success=False, error="query required")
                
                req = urllib.request.Request(
                    f'{base_url}/search.messages?query={urllib.parse.quote(params["query"])}',
                    headers=headers,
                )
            
            else:
                return HandlerResult(success=False, error=f"Unknown operation: {operation}")
            
            ctx = ssl.create_default_context()
            response = urllib.request.urlopen(req, timeout=30, context=ctx)
            result = json.loads(response.read().decode('utf-8'))
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=result.get('ok', False),
                data=result,
                duration_ms=duration_ms,
            )
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            return HandlerResult(
                success=False,
                error=f"HTTP {e.code}: {error_body}",
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )
