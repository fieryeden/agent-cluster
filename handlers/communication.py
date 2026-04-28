"""
Communication handlers.

Handlers for email, messaging, notifications.
"""

import time
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Dict, Any, List, Optional
from pathlib import Path
import json

from handlers.base import (
    TaskHandler,
    HandlerResult,
    HandlerConfidence,
)


class EmailSendHandler(TaskHandler):
    """Send email via SMTP."""
    
    name = "email_send"
    category = "communication"
    description = "Send email via SMTP"
    dangerous = True
    
    input_schema = {
        'to': {'type': 'array', 'required': True, 'description': 'Recipient addresses'},
        'subject': {'type': 'string', 'required': True, 'description': 'Email subject'},
        'body': {'type': 'string', 'required': True, 'description': 'Email body'},
        'from': {'type': 'string', 'description': 'Sender address'},
        'cc': {'type': 'array', 'description': 'CC addresses'},
        'bcc': {'type': 'array', 'description': 'BCC addresses'},
        'html': {'type': 'boolean', 'default': False, 'description': 'Body is HTML'},
        'attachments': {'type': 'array', 'description': 'File paths to attach'},
        'smtp_host': {'type': 'string', 'description': 'SMTP server host'},
        'smtp_port': {'type': 'integer', 'default': 587, 'description': 'SMTP port'},
        'smtp_user': {'type': 'string', 'description': 'SMTP username'},
        'smtp_password': {'type': 'string', 'description': 'SMTP password'},
        'use_tls': {'type': 'boolean', 'default': True, 'description': 'Use TLS'},
    }
    
    output_schema = {
        'message_id': {'type': 'string', 'description': 'Message ID'},
        'recipients': {'type': 'array', 'description': 'All recipients'},
        'timestamp': {'type': 'string', 'description': 'Send time'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['email_send', 'send_email', 'email', 'mail']:
            return HandlerConfidence.PERFECT.value
        if 'to' in params and 'subject' in params and 'body' in params:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        to_addrs = params['to']
        subject = params['subject']
        body = params['body']
        from_addr = params.get('from', params.get('smtp_user'))
        cc = params.get('cc', [])
        bcc = params.get('bcc', [])
        is_html = params.get('html', False)
        attachments = params.get('attachments', [])
        
        smtp_host = params.get('smtp_host')
        smtp_port = params.get('smtp_port', 587)
        smtp_user = params.get('smtp_user')
        smtp_password = params.get('smtp_password')
        use_tls = params.get('use_tls', True)
        
        try:
            if not smtp_host:
                return HandlerResult(success=False, error="SMTP host required")
            
            # Create message
            msg = MIMEMultipart() if attachments else (MIMEText(body, 'html' if is_html else 'plain'))
            
            if attachments:
                msg.attach(MIMEText(body, 'html' if is_html else 'plain'))
            
            msg['From'] = from_addr
            msg['To'] = ', '.join(to_addrs)
            if cc:
                msg['Cc'] = ', '.join(cc)
            msg['Subject'] = subject
            
            # Add attachments
            for filepath in attachments:
                path = Path(filepath)
                if not path.exists():
                    return HandlerResult(success=False, error=f"Attachment not found: {filepath}")
                
                with open(path, 'rb') as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename="{path.name}"')
                    msg.attach(part)
            
            # Send
            all_recipients = to_addrs + cc + bcc
            
            if use_tls:
                context = ssl.create_default_context()
                with smtplib.SMTP(smtp_host, smtp_port) as server:
                    server.starttls(context=context)
                    if smtp_user and smtp_password:
                        server.login(smtp_user, smtp_password)
                    server.sendmail(from_addr, all_recipients, msg.as_string())
                    message_id = server.last_response
            else:
                with smtplib.SMTP(smtp_host, smtp_port) as server:
                    if smtp_user and smtp_password:
                        server.login(smtp_user, smtp_password)
                    server.sendmail(from_addr, all_recipients, msg.as_string())
                    message_id = server.last_response
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'message_id': message_id,
                    'recipients': all_recipients,
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                },
                duration_ms=duration_ms,
            )
            
        except smtplib.SMTPException as e:
            return HandlerResult(
                success=False,
                error=f"SMTP error: {e}",
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class SlackSendHandler(TaskHandler):
    """Send Slack message via webhook or API."""
    
    name = "slack_send"
    category = "communication"
    description = "Send message to Slack channel"
    dangerous = True
    
    input_schema = {
        'channel': {'type': 'string', 'description': 'Channel ID (for API)'},
        'text': {'type': 'string', 'required': True, 'description': 'Message text'},
        'blocks': {'type': 'array', 'description': 'Slack blocks for rich formatting'},
        'attachments': {'type': 'array', 'description': 'Slack attachments'},
        'webhook_url': {'type': 'string', 'description': 'Webhook URL'},
        'api_token': {'type': 'string', 'description': 'Slack API token'},
        'username': {'type': 'string', 'description': 'Override username'},
        'icon_emoji': {'type': 'string', 'description': 'Icon emoji'},
    }
    
    output_schema = {
        'ok': {'type': 'boolean'},
        'ts': {'type': 'string', 'description': 'Message timestamp'},
        'channel': {'type': 'string'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['slack_send', 'slack', 'slack_message']:
            return HandlerConfidence.PERFECT.value
        if params.get('webhook_url') or params.get('api_token'):
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        import urllib.request
        import urllib.error
        
        start_time = time.time()
        text = params['text']
        webhook_url = params.get('webhook_url')
        api_token = params.get('api_token')
        channel = params.get('channel')
        
        try:
            if webhook_url:
                # Use webhook
                payload = {'text': text}
                if params.get('username'):
                    payload['username'] = params['username']
                if params.get('icon_emoji'):
                    payload['icon_emoji'] = params['icon_emoji']
                if params.get('blocks'):
                    payload['blocks'] = params['blocks']
                if params.get('attachments'):
                    payload['attachments'] = params['attachments']
                
                data = json.dumps(payload).encode('utf-8')
                req = urllib.request.Request(
                    webhook_url,
                    data=data,
                    headers={'Content-Type': 'application/json'},
                )
                response = urllib.request.urlopen(req, timeout=30)
                result = response.read().decode('utf-8')
                
                return HandlerResult(
                    success=result == 'ok',
                    data={'ok': result == 'ok'},
                    duration_ms=(time.time() - start_time) * 1000,
                )
            
            elif api_token and channel:
                # Use API
                payload = {
                    'channel': channel,
                    'text': text,
                }
                if params.get('blocks'):
                    payload['blocks'] = params['blocks']
                
                data = json.dumps(payload).encode('utf-8')
                req = urllib.request.Request(
                    'https://slack.com/api/chat.postMessage',
                    data=data,
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {api_token}',
                    },
                )
                response = urllib.request.urlopen(req, timeout=30)
                result = json.loads(response.read().decode('utf-8'))
                
                duration_ms = (time.time() - start_time) * 1000
                
                return HandlerResult(
                    success=result.get('ok', False),
                    data=result,
                    duration_ms=duration_ms,
                )
            
            else:
                return HandlerResult(
                    success=False,
                    error="webhook_url or (api_token + channel) required",
                )
            
        except urllib.error.HTTPError as e:
            return HandlerResult(
                success=False,
                error=f"HTTP {e.code}: {e.read().decode('utf-8')}",
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class DiscordSendHandler(TaskHandler):
    """Send Discord message via webhook."""
    
    name = "discord_send"
    category = "communication"
    description = "Send message to Discord channel via webhook"
    dangerous = True
    
    input_schema = {
        'webhook_url': {'type': 'string', 'required': True, 'description': 'Discord webhook URL'},
        'content': {'type': 'string', 'description': 'Message content'},
        'embeds': {'type': 'array', 'description': 'Embed objects'},
        'username': {'type': 'string', 'description': 'Override username'},
        'avatar_url': {'type': 'string', 'description': 'Override avatar'},
        'file': {'type': 'string', 'description': 'File to attach'},
    }
    
    output_schema = {
        'ok': {'type': 'boolean'},
        'message_id': {'type': 'string'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['discord_send', 'discord', 'discord_message']:
            return HandlerConfidence.PERFECT.value
        if params.get('webhook_url', '').startswith('https://discord.com/api/webhooks/'):
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        import urllib.request
        import urllib.error
        
        start_time = time.time()
        webhook_url = params['webhook_url']
        
        try:
            payload = {}
            if params.get('content'):
                payload['content'] = params['content']
            if params.get('username'):
                payload['username'] = params['username']
            if params.get('avatar_url'):
                payload['avatar_url'] = params['avatar_url']
            if params.get('embeds'):
                payload['embeds'] = params['embeds']
            
            if params.get('file'):
                # Multipart upload
                import os
                filepath = Path(params['file'])
                if not filepath.exists():
                    return HandlerResult(success=False, error=f"File not found: {params['file']}")
                
                boundary = '----DiscordWebhookBoundary' + str(time.time()).replace('.', '')
                with open(filepath, 'rb') as f:
                    file_content = f.read()
                
                body = f'--{boundary}\r\n'
                body += f'Content-Disposition: form-data; name="file"; filename="{filepath.name}"\r\n'
                body += 'Content-Type: application/octet-stream\r\n\r\n'
                body_bytes = body.encode('utf-8') + file_content + f'\r\n--{boundary}--\r\n'.encode('utf-8')
                
                req = urllib.request.Request(
                    webhook_url,
                    data=body_bytes,
                    headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
                )
            else:
                data = json.dumps(payload).encode('utf-8')
                req = urllib.request.Request(
                    webhook_url,
                    data=data,
                    headers={'Content-Type': 'application/json'},
                )
            
            response = urllib.request.urlopen(req, timeout=30)
            result = response.read().decode('utf-8')
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={'ok': True},
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


class NotificationHandler(TaskHandler):
    """Send system notification."""
    
    name = "notification"
    category = "communication"
    description = "Send desktop/mobile notification"
    
    input_schema = {
        'title': {'type': 'string', 'required': True, 'description': 'Notification title'},
        'body': {'type': 'string', 'required': True, 'description': 'Notification body'},
        'priority': {'type': 'string', 'default': 'normal', 'enum': ['low', 'normal', 'high', 'urgent']},
        'sound': {'type': 'boolean', 'default': True, 'description': 'Play sound'},
        'actions': {'type': 'array', 'description': 'Action buttons'},
    }
    
    output_schema = {
        'sent': {'type': 'boolean'},
        'platform': {'type': 'string', 'description': 'Notification platform used'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['notification', 'notify', 'alert', 'push']:
            return HandlerConfidence.PERFECT.value
        if 'title' in params and 'body' in params:
            return HandlerConfidence.MEDIUM.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        import subprocess
        import platform as plat
        
        start_time = time.time()
        title = params['title']
        body = params['body']
        priority = params.get('priority', 'normal')
        sound = params.get('sound', True)
        
        try:
            system = plat.system()
            
            if system == 'Darwin':  # macOS
                cmd = ['osascript', '-e', f'display notification "{body}" with title "{title}"']
                if sound:
                    cmd[-1] += ' sound name "Glass"'
                subprocess.run(cmd, check=True)
                platform_used = 'macos'
            
            elif system == 'Linux':
                # Try notify-send
                try:
                    urgency = {'low': 'low', 'normal': 'normal', 'high': 'critical', 'urgent': 'critical'}
                    cmd = ['notify-send', '-u', urgency.get(priority, 'normal'), title, body]
                    subprocess.run(cmd, check=True)
                    platform_used = 'linux-notify-send'
                except FileNotFoundError:
                    # Try zenity
                    subprocess.run(['zenity', '--notification', f'--text={title}: {body}'], check=True)
                    platform_used = 'linux-zenity'
            
            elif system == 'Windows':
                # Try Windows 10 toast
                try:
                    from win10toast import ToastNotifier
                    toaster = ToastNotifier()
                    toaster.show_toast(title, body, duration=5)
                    platform_used = 'windows-toast'
                except ImportError:
                    # Fallback to PowerShell
                    ps_script = f'''
                    [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
                    [Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] | Out-Null
                    $template = @"
                    <toast>
                        <visual>
                            <binding template="ToastText02">
                                <text id="1">{title}</text>
                                <text id="2">{body}</text>
                            </binding>
                        </visual>
                    </toast>
                    "@
                    $xml = New-Object Windows.Data.Xml.Dom.XmlDocument
                    $xml.LoadXml($template)
                    $toast = [Windows.UI.Notifications.ToastNotification]::new($xml)
                    [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("Agent Cluster").Show($toast)
                    '''
                    subprocess.run(['powershell', '-Command', ps_script], check=True)
                    platform_used = 'windows-powershell'
            else:
                return HandlerResult(
                    success=False,
                    error=f"Unsupported platform: {system}",
                )
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={'sent': True, 'platform': platform_used},
                duration_ms=duration_ms,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class WebhookHandler(TaskHandler):
    """Send webhook to external service."""
    
    name = "webhook_send"
    category = "communication"
    description = "Send webhook to external service"
    dangerous = True
    
    input_schema = {
        'url': {'type': 'string', 'required': True, 'description': 'Webhook URL'},
        'method': {'type': 'string', 'default': 'POST', 'enum': ['GET', 'POST', 'PUT', 'DELETE']},
        'headers': {'type': 'object', 'description': 'Request headers'},
        'payload': {'type': 'object', 'description': 'Request payload'},
        'secret': {'type': 'string', 'description': 'Secret for HMAC signature'},
        'timeout': {'type': 'integer', 'default': 30, 'description': 'Timeout in seconds'},
    }
    
    output_schema = {
        'status_code': {'type': 'integer'},
        'response': {'type': 'any'},
        'elapsed_seconds': {'type': 'float'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['webhook_send', 'webhook', 'trigger']:
            return HandlerConfidence.PERFECT.value
        if params.get('url') and params.get('payload'):
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        import urllib.request
        import urllib.error
        import hashlib
        import hmac
        import ssl
        
        start_time = time.time()
        url = params['url']
        method = params.get('method', 'POST')
        headers = dict(params.get('headers', {}))
        payload = params.get('payload', {})
        secret = params.get('secret')
        timeout = params.get('timeout', 30)
        
        try:
            data = json.dumps(payload).encode('utf-8')
            
            # Add HMAC signature if secret provided
            if secret:
                signature = hmac.new(
                    secret.encode('utf-8'),
                    data,
                    hashlib.sha256,
                ).hexdigest()
                headers['X-Signature'] = f'sha256={signature}'
            
            headers.setdefault('Content-Type', 'application/json')
            
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            ctx = ssl.create_default_context()
            
            response = urllib.request.urlopen(req, timeout=timeout, context=ctx)
            response_body = response.read().decode('utf-8')
            
            try:
                response_data = json.loads(response_body)
            except json.JSONDecodeError:
                response_data = response_body
            
            elapsed = time.time() - start_time
            
            return HandlerResult(
                success=True,
                data={
                    'status_code': response.status,
                    'response': response_data,
                    'elapsed_seconds': elapsed,
                },
                duration_ms=elapsed * 1000,
            )
            
        except urllib.error.HTTPError as e:
            return HandlerResult(
                success=False,
                error=f"HTTP {e.code}: {e.reason}",
                data={'status_code': e.code, 'response': e.read().decode('utf-8')},
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )
