"""
AI operation handlers.

Handlers for LLM calls, embeddings, text processing.
"""

import time
import json
import urllib.request
import urllib.error
import ssl
from typing import Dict, Any, List, Optional
import hashlib
import os

from handlers.base import (
    TaskHandler,
    HandlerResult,
    HandlerConfidence,
)


class LLMChatHandler(TaskHandler):
    """Call LLM API for chat completion."""
    
    name = "llm_chat"
    category = "ai"
    description = "Call LLM API for chat completion"
    
    input_schema = {
        'messages': {'type': 'array', 'required': True, 'description': 'Chat messages'},
        'model': {'type': 'string', 'default': 'gpt-3.5-turbo', 'description': 'Model to use'},
        'temperature': {'type': 'number', 'default': 0.7, 'description': 'Sampling temperature'},
        'max_tokens': {'type': 'integer', 'default': 1000, 'description': 'Max tokens to generate'},
        'api_key': {'type': 'string', 'description': 'API key'},
        'api_base': {'type': 'string', 'default': 'https://api.openai.com/v1', 'description': 'API base URL'},
        'system': {'type': 'string', 'description': 'System message'},
    }
    
    output_schema = {
        'response': {'type': 'string', 'description': 'Generated response'},
        'tokens_used': {'type': 'integer', 'description': 'Tokens used'},
        'model': {'type': 'string', 'description': 'Model used'},
        'finish_reason': {'type': 'string', 'description': 'Why generation stopped'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['llm_chat', 'chat', 'complete', 'gpt', 'ai']:
            return HandlerConfidence.PERFECT.value
        if 'messages' in params:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        messages = params['messages']
        model = params.get('model', 'gpt-3.5-turbo')
        temperature = params.get('temperature', 0.7)
        max_tokens = params.get('max_tokens', 1000)
        api_key = params.get('api_key') or os.environ.get('OPENAI_API_KEY')
        api_base = params.get('api_base', 'https://api.openai.com/v1')
        system = params.get('system')
        
        try:
            if not api_key:
                return HandlerResult(success=False, error="API key required")
            
            # Build messages
            full_messages = []
            if system:
                full_messages.append({'role': 'system', 'content': system})
            full_messages.extend(messages)
            
            # Make request
            payload = {
                'model': model,
                'messages': full_messages,
                'temperature': temperature,
                'max_tokens': max_tokens,
            }
            
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                f'{api_base}/chat/completions',
                data=data,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {api_key}',
                },
            )
            ctx = ssl.create_default_context()
            
            response = urllib.request.urlopen(req, timeout=120, context=ctx)
            result = json.loads(response.read().decode('utf-8'))
            
            choice = result['choices'][0]
            usage = result.get('usage', {})
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'response': choice['message']['content'],
                    'tokens_used': usage.get('total_tokens'),
                    'model': result.get('model'),
                    'finish_reason': choice.get('finish_reason'),
                    'prompt_tokens': usage.get('prompt_tokens'),
                    'completion_tokens': usage.get('completion_tokens'),
                },
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


class LLMCompleteHandler(TaskHandler):
    """Single-turn text completion."""
    
    name = "llm_complete"
    category = "ai"
    description = "Single-turn text completion"
    
    input_schema = {
        'prompt': {'type': 'string', 'required': True, 'description': 'Prompt text'},
        'model': {'type': 'string', 'default': 'gpt-3.5-turbo-instruct'},
        'temperature': {'type': 'number', 'default': 0.7},
        'max_tokens': {'type': 'integer', 'default': 1000},
        'api_key': {'type': 'string'},
        'api_base': {'type': 'string'},
    }
    
    output_schema = {
        'text': {'type': 'string'},
        'tokens_used': {'type': 'integer'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['llm_complete', 'complete', 'completion']:
            return HandlerConfidence.PERFECT.value
        if 'prompt' in params and not params.get('messages'):
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        prompt = params['prompt']
        model = params.get('model', 'gpt-3.5-turbo-instruct')
        temperature = params.get('temperature', 0.7)
        max_tokens = params.get('max_tokens', 1000)
        api_key = params.get('api_key') or os.environ.get('OPENAI_API_KEY')
        api_base = params.get('api_base', 'https://api.openai.com/v1')
        
        try:
            if not api_key:
                return HandlerResult(success=False, error="API key required")
            
            payload = {
                'model': model,
                'prompt': prompt,
                'temperature': temperature,
                'max_tokens': max_tokens,
            }
            
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                f'{api_base}/completions',
                data=data,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {api_key}',
                },
            )
            ctx = ssl.create_default_context()
            
            response = urllib.request.urlopen(req, timeout=120, context=ctx)
            result = json.loads(response.read().decode('utf-8'))
            
            choice = result['choices'][0]
            usage = result.get('usage', {})
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'text': choice['text'],
                    'tokens_used': usage.get('total_tokens'),
                    'model': result.get('model'),
                },
                duration_ms=duration_ms,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class EmbeddingHandler(TaskHandler):
    """Generate text embeddings."""
    
    name = "embedding"
    category = "ai"
    description = "Generate text embeddings for semantic similarity"
    
    input_schema = {
        'text': {'type': 'any', 'required': True, 'description': 'Text or list of texts'},
        'model': {'type': 'string', 'default': 'text-embedding-ada-002'},
        'api_key': {'type': 'string'},
        'api_base': {'type': 'string'},
    }
    
    output_schema = {
        'embeddings': {'type': 'array', 'description': 'Embedding vectors'},
        'dimensions': {'type': 'integer'},
        'tokens_used': {'type': 'integer'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['embedding', 'embed', 'embeddings']:
            return HandlerConfidence.PERFECT.value
        if 'text' in params and 'embedding' in params.get('model', ''):
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        text = params['text']
        model = params.get('model', 'text-embedding-ada-002')
        api_key = params.get('api_key') or os.environ.get('OPENAI_API_KEY')
        api_base = params.get('api_base', 'https://api.openai.com/v1')
        
        try:
            if not api_key:
                return HandlerResult(success=False, error="API key required")
            
            # Normalize input
            texts = [text] if isinstance(text, str) else text
            
            payload = {
                'model': model,
                'input': texts,
            }
            
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                f'{api_base}/embeddings',
                data=data,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {api_key}',
                },
            )
            ctx = ssl.create_default_context()
            
            response = urllib.request.urlopen(req, timeout=60, context=ctx)
            result = json.loads(response.read().decode('utf-8'))
            
            embeddings = [d['embedding'] for d in result['data']]
            usage = result.get('usage', {})
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'embeddings': embeddings,
                    'dimensions': len(embeddings[0]) if embeddings else 0,
                    'tokens_used': usage.get('total_tokens'),
                    'model': result.get('model'),
                },
                duration_ms=duration_ms,
            )
            
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class TextSummarizeHandler(TaskHandler):
    """Summarize text using LLM."""
    
    name = "text_summarize"
    category = "ai"
    description = "Summarize text using LLM"
    
    input_schema = {
        'text': {'type': 'string', 'required': True, 'description': 'Text to summarize'},
        'max_length': {'type': 'integer', 'default': 200, 'description': 'Max summary length in words'},
        'style': {'type': 'string', 'default': 'concise', 'enum': ['concise', 'detailed', 'bullet', 'tldr']},
        'model': {'type': 'string'},
        'api_key': {'type': 'string'},
    }
    
    output_schema = {
        'summary': {'type': 'string'},
        'original_length': {'type': 'integer'},
        'summary_length': {'type': 'integer'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['text_summarize', 'summarize', 'summary', 'tldr']:
            return HandlerConfidence.PERFECT.value
        if 'text' in params and 'summarize' in str(params.get('task', '')).lower():
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        text = params['text']
        max_length = params.get('max_length', 200)
        style = params.get('style', 'concise')
        
        # Build prompt based on style
        style_prompts = {
            'concise': f'Summarize the following text in at most {max_length} words. Be concise and capture the key points:',
            'detailed': f'Summarize the following text in {max_length} words. Include all important details:',
            'bullet': f'Summarize the following text as bullet points. Create at most {max_length // 20} bullets:',
            'tldr': 'Provide a TL;DR (one sentence summary) of the following text:',
        }
        
        prompt = f"{style_prompts.get(style, style_prompts['concise'])}\n\n{text}"
        
        # Use LLM chat
        llm_params = {
            'messages': [{'role': 'user', 'content': prompt}],
            'model': params.get('model', 'gpt-3.5-turbo'),
            'max_tokens': max_length * 2,  # Rough token estimate
            'api_key': params.get('api_key'),
        }
        
        handler = LLMChatHandler()
        result = handler.execute(llm_params)
        
        if result.success:
            summary = result.data['response']
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data={
                    'summary': summary,
                    'original_length': len(text.split()),
                    'summary_length': len(summary.split()),
                    'style': style,
                },
                duration_ms=duration_ms,
            )
        else:
            return result


class TextClassifyHandler(TaskHandler):
    """Classify text into categories."""
    
    name = "text_classify"
    category = "ai"
    description = "Classify text into categories using LLM"
    
    input_schema = {
        'text': {'type': 'string', 'required': True, 'description': 'Text to classify'},
        'categories': {'type': 'array', 'required': True, 'description': 'Possible categories'},
        'model': {'type': 'string'},
        'api_key': {'type': 'string'},
    }
    
    output_schema = {
        'category': {'type': 'string', 'description': 'Predicted category'},
        'confidence': {'type': 'number', 'description': 'Confidence score'},
        'probabilities': {'type': 'object', 'description': 'Probabilities per category'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['text_classify', 'classify', 'categorize', 'category']:
            return HandlerConfidence.PERFECT.value
        if 'text' in params and 'categories' in params:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        text = params['text']
        categories = params['categories']
        
        prompt = f"""Classify the following text into exactly one of these categories: {', '.join(categories)}

Text: {text}

Respond with only the category name, nothing else."""
        
        llm_params = {
            'messages': [{'role': 'user', 'content': prompt}],
            'model': params.get('model', 'gpt-3.5-turbo'),
            'max_tokens': 20,
            'temperature': 0.0,
            'api_key': params.get('api_key'),
        }
        
        handler = LLMChatHandler()
        result = handler.execute(llm_params)
        
        if result.success:
            predicted = result.data['response'].strip()
            
            # Validate category
            if predicted in categories:
                duration_ms = (time.time() - start_time) * 1000
                return HandlerResult(
                    success=True,
                    data={
                        'category': predicted,
                        'confidence': 1.0,  # LLM doesn't provide probabilities
                        'all_categories': categories,
                    },
                    duration_ms=duration_ms,
                )
            else:
                # Try fuzzy match
                for cat in categories:
                    if cat.lower() in predicted.lower():
                        duration_ms = (time.time() - start_time) * 1000
                        return HandlerResult(
                            success=True,
                            data={
                                'category': cat,
                                'confidence': 0.8,
                                'raw_response': predicted,
                            },
                            duration_ms=duration_ms,
                        )
                
                return HandlerResult(
                    success=False,
                    error=f"Invalid category returned: {predicted}",
                )
        else:
            return result


class TextExtractHandler(TaskHandler):
    """Extract structured data from text."""
    
    name = "text_extract"
    category = "ai"
    description = "Extract structured data from unstructured text"
    
    input_schema = {
        'text': {'type': 'string', 'required': True, 'description': 'Text to extract from'},
        'fields': {'type': 'array', 'required': True, 'description': 'Fields to extract'},
        'model': {'type': 'string'},
        'api_key': {'type': 'string'},
    }
    
    output_schema = {
        'extracted': {'type': 'object', 'description': 'Extracted fields'},
        'raw_response': {'type': 'string'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['text_extract', 'extract', 'extract_data']:
            return HandlerConfidence.PERFECT.value
        if 'text' in params and 'fields' in params:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        text = params['text']
        fields = params['fields']
        
        prompt = f"""Extract the following information from the text below. Return a JSON object with these keys: {', '.join(fields)}

If a field is not found, use null as the value.

Text: {text}

Return only valid JSON:"""
        
        llm_params = {
            'messages': [{'role': 'user', 'content': prompt}],
            'model': params.get('model', 'gpt-3.5-turbo'),
            'max_tokens': 500,
            'temperature': 0.0,
            'api_key': params.get('api_key'),
        }
        
        handler = LLMChatHandler()
        result = handler.execute(llm_params)
        
        if result.success:
            response = result.data['response'].strip()
            
            # Parse JSON
            try:
                # Remove markdown code blocks if present
                if response.startswith('```'):
                    lines = response.split('\n')
                    response = '\n'.join(lines[1:-1] if lines[-1] == '```' else lines[1:])
                
                extracted = json.loads(response)
                duration_ms = (time.time() - start_time) * 1000
                
                return HandlerResult(
                    success=True,
                    data={
                        'extracted': extracted,
                        'fields': fields,
                    },
                    duration_ms=duration_ms,
                )
            except json.JSONDecodeError as e:
                return HandlerResult(
                    success=False,
                    error=f"Failed to parse JSON: {e}",
                    data={'raw_response': response},
                )
        else:
            return result
