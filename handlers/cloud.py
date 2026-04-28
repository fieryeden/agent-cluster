"""
Cloud service handlers.

Handlers for AWS, GCP, Azure operations.
"""

import time
import json
import os
import urllib.request
import urllib.error
import ssl
import hashlib
import base64
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from handlers.base import (
    TaskHandler,
    HandlerResult,
    HandlerConfidence,
)


class S3Handler(TaskHandler):
    """AWS S3 operations."""
    
    name = "s3"
    category = "cloud"
    description = "AWS S3 bucket operations"
    
    input_schema = {
        'operation': {'type': 'string', 'required': True, 'enum': ['upload', 'download', 'list', 'delete', 'presign']},
        'bucket': {'type': 'string', 'required': True, 'description': 'Bucket name'},
        'key': {'type': 'string', 'description': 'Object key'},
        'file': {'type': 'string', 'description': 'Local file path'},
        'expires': {'type': 'integer', 'default': 3600, 'description': 'Presign URL expiry (seconds)'},
        'prefix': {'type': 'string', 'description': 'Key prefix for list'},
    }
    
    output_schema = {
        'url': {'type': 'string'},
        'objects': {'type': 'array'},
        'deleted': {'type': 'integer'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['s3', 'aws_s3', 's3_upload', 's3_download']:
            return HandlerConfidence.PERFECT.value
        if params.get('bucket') and params.get('operation') in ['upload', 'download', 'list']:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        operation = params['operation']
        bucket = params['bucket']
        key = params.get('key')
        filepath = params.get('file')
        prefix = params.get('prefix', '')
        expires = params.get('expires', 3600)
        
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError
            
            s3 = boto3.client('s3')
            
            if operation == 'upload':
                if not key or not filepath:
                    return HandlerResult(success=False, error="Key and file required for upload")
                
                path = Path(filepath)
                if not path.exists():
                    return HandlerResult(success=False, error=f"File not found: {filepath}")
                
                s3.upload_file(str(path), bucket, key)
                result_data = {'uploaded': True, 'bucket': bucket, 'key': key}
            
            elif operation == 'download':
                if not key or not filepath:
                    return HandlerResult(success=False, error="Key and file required for download")
                
                s3.download_file(bucket, key, filepath)
                result_data = {'downloaded': True, 'bucket': bucket, 'key': key, 'file': filepath}
            
            elif operation == 'list':
                kwargs = {'Bucket': bucket}
                if prefix:
                    kwargs['Prefix'] = prefix
                
                response = s3.list_objects_v2(**kwargs)
                objects = []
                for obj in response.get('Contents', []):
                    objects.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat(),
                        'etag': obj['ETag'].strip('"'),
                    })
                result_data = {'objects': objects, 'count': len(objects)}
            
            elif operation == 'delete':
                if not key:
                    return HandlerResult(success=False, error="Key required for delete")
                
                response = s3.delete_object(Bucket=bucket, Key=key)
                result_data = {'deleted': True, 'bucket': bucket, 'key': key}
            
            elif operation == 'presign':
                if not key:
                    return HandlerResult(success=False, error="Key required for presign")
                
                url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket, 'Key': key},
                    ExpiresIn=expires,
                )
                result_data = {'url': url, 'expires_in': expires}
            
            else:
                return HandlerResult(success=False, error=f"Unknown operation: {operation}")
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data=result_data,
                duration_ms=duration_ms,
            )
            
        except ImportError:
            return HandlerResult(
                success=False,
                error="S3 requires boto3. Use pip install boto3.",
            )
        except NoCredentialsError:
            return HandlerResult(
                success=False,
                error="AWS credentials not configured. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.",
            )
        except ClientError as e:
            return HandlerResult(
                success=False,
                error=f"AWS error: {e.response['Error']['Message']}",
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class LambdaHandler(TaskHandler):
    """AWS Lambda operations."""
    
    name = "lambda"
    category = "cloud"
    description = "AWS Lambda function operations"
    
    input_schema = {
        'operation': {'type': 'string', 'required': True, 'enum': ['invoke', 'list', 'get']},
        'function': {'type': 'string', 'description': 'Function name or ARN'},
        'payload': {'type': 'object', 'description': 'Invocation payload'},
        'invocation_type': {'type': 'string', 'default': 'RequestResponse', 'enum': ['RequestResponse', 'Event', 'DryRun']},
        'qualifier': {'type': 'string', 'description': 'Version or alias'},
    }
    
    output_schema = {
        'status_code': {'type': 'integer'},
        'result': {'type': 'any'},
        'log_result': {'type': 'string'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['lambda', 'aws_lambda', 'invoke_lambda']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        operation = params['operation']
        function_name = params.get('function')
        payload = params.get('payload', {})
        invocation_type = params.get('invocation_type', 'RequestResponse')
        qualifier = params.get('qualifier')
        
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError
            
            lambda_client = boto3.client('lambda')
            
            if operation == 'invoke':
                if not function_name:
                    return HandlerResult(success=False, error="Function name required")
                
                kwargs = {
                    'FunctionName': function_name,
                    'InvocationType': invocation_type,
                    'Payload': json.dumps(payload),
                }
                if qualifier:
                    kwargs['Qualifier'] = qualifier
                
                response = lambda_client.invoke(**kwargs)
                
                result_data = {
                    'status_code': response['StatusCode'],
                    'executed_version': response.get('ExecutedVersion'),
                }
                
                if 'FunctionError' in response:
                    result_data['error'] = response['FunctionError']
                
                if response.get('LogResult'):
                    result_data['log_result'] = base64.b64decode(response['LogResult']).decode('utf-8')
                
                # Read payload for sync invocations
                if invocation_type == 'RequestResponse':
                    result_payload = response['Payload'].read()
                    try:
                        result_data['result'] = json.loads(result_payload)
                    except json.JSONDecodeError:
                        result_data['result'] = result_payload.decode('utf-8')
            
            elif operation == 'list':
                response = lambda_client.list_functions()
                functions = []
                for fn in response.get('Functions', []):
                    functions.append({
                        'name': fn['FunctionName'],
                        'arn': fn['FunctionArn'],
                        'runtime': fn.get('Runtime'),
                        'handler': fn.get('Handler'),
                        'last_modified': fn['LastModified'].isoformat(),
                    })
                result_data = {'functions': functions, 'count': len(functions)}
            
            elif operation == 'get':
                if not function_name:
                    return HandlerResult(success=False, error="Function name required")
                
                response = lambda_client.get_function(FunctionName=function_name)
                conf = response['Configuration']
                result_data = {
                    'name': conf['FunctionName'],
                    'arn': conf['FunctionArn'],
                    'runtime': conf.get('Runtime'),
                    'handler': conf.get('Handler'),
                    'timeout': conf.get('Timeout'),
                    'memory_size': conf.get('MemorySize'),
                    'last_modified': conf['LastModified'].isoformat(),
                }
            
            else:
                return HandlerResult(success=False, error=f"Unknown operation: {operation}")
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data=result_data,
                duration_ms=duration_ms,
            )
            
        except ImportError:
            return HandlerResult(
                success=False,
                error="Lambda requires boto3. Use pip install boto3.",
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class CloudStorageHandler(TaskHandler):
    """Google Cloud Storage operations."""
    
    name = "gcs"
    category = "cloud"
    description = "Google Cloud Storage operations"
    
    input_schema = {
        'operation': {'type': 'string', 'required': True, 'enum': ['upload', 'download', 'list', 'delete']},
        'bucket': {'type': 'string', 'required': True},
        'blob': {'type': 'string', 'description': 'Blob name'},
        'file': {'type': 'string', 'description': 'Local file path'},
        'prefix': {'type': 'string', 'description': 'Prefix for list'},
    }
    
    output_schema = {
        'uploaded': {'type': 'boolean'},
        'objects': {'type': 'array'},
        'deleted': {'type': 'boolean'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['gcs', 'google_cloud_storage', 'gcs_upload']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        operation = params['operation']
        bucket_name = params['bucket']
        blob_name = params.get('blob')
        filepath = params.get('file')
        prefix = params.get('prefix', '')
        
        try:
            from google.cloud import storage
            from google.cloud.exceptions import NotFound
            
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            
            if operation == 'upload':
                if not blob_name or not filepath:
                    return HandlerResult(success=False, error="Blob and file required for upload")
                
                blob = bucket.blob(blob_name)
                blob.upload_from_filename(filepath)
                result_data = {'uploaded': True, 'bucket': bucket_name, 'blob': blob_name}
            
            elif operation == 'download':
                if not blob_name or not filepath:
                    return HandlerResult(success=False, error="Blob and file required for download")
                
                blob = bucket.blob(blob_name)
                blob.download_to_filename(filepath)
                result_data = {'downloaded': True, 'bucket': bucket_name, 'blob': blob_name}
            
            elif operation == 'list':
                blobs = client.list_blobs(bucket_name, prefix=prefix)
                objects = []
                for blob in blobs:
                    objects.append({
                        'name': blob.name,
                        'size': blob.size,
                        'updated': blob.updated.isoformat(),
                        'content_type': blob.content_type,
                    })
                result_data = {'objects': objects, 'count': len(objects)}
            
            elif operation == 'delete':
                if not blob_name:
                    return HandlerResult(success=False, error="Blob name required for delete")
                
                blob = bucket.blob(blob_name)
                blob.delete()
                result_data = {'deleted': True, 'bucket': bucket_name, 'blob': blob_name}
            
            else:
                return HandlerResult(success=False, error=f"Unknown operation: {operation}")
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data=result_data,
                duration_ms=duration_ms,
            )
            
        except ImportError:
            return HandlerResult(
                success=False,
                error="GCS requires google-cloud-storage. Use pip install google-cloud-storage.",
            )
        except NotFound as e:
            return HandlerResult(
                success=False,
                error=f"Not found: {e}",
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class AzureBlobHandler(TaskHandler):
    """Azure Blob Storage operations."""
    
    name = "azure_blob"
    category = "cloud"
    description = "Azure Blob Storage operations"
    
    input_schema = {
        'operation': {'type': 'string', 'required': True, 'enum': ['upload', 'download', 'list', 'delete']},
        'container': {'type': 'string', 'required': True},
        'blob': {'type': 'string', 'description': 'Blob name'},
        'file': {'type': 'string', 'description': 'Local file path'},
        'connection_string': {'type': 'string', 'description': 'Azure storage connection string'},
    }
    
    output_schema = {
        'uploaded': {'type': 'boolean'},
        'objects': {'type': 'array'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['azure_blob', 'azure_storage', 'blob_storage']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        operation = params['operation']
        container_name = params['container']
        blob_name = params.get('blob')
        filepath = params.get('file')
        conn_str = params.get('connection_string') or os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        
        try:
            from azure.storage.blob import BlobServiceClient
            from azure.core.exceptions import ResourceNotFoundError
            
            if not conn_str:
                return HandlerResult(success=False, error="Azure storage connection string required")
            
            blob_service = BlobServiceClient.from_connection_string(conn_str)
            container_client = blob_service.get_container_client(container_name)
            
            if operation == 'upload':
                if not blob_name or not filepath:
                    return HandlerResult(success=False, error="Blob and file required for upload")
                
                blob_client = container_client.get_blob_client(blob_name)
                with open(filepath, 'rb') as data:
                    blob_client.upload_blob(data, overwrite=True)
                result_data = {'uploaded': True, 'container': container_name, 'blob': blob_name}
            
            elif operation == 'download':
                if not blob_name or not filepath:
                    return HandlerResult(success=False, error="Blob and file required for download")
                
                blob_client = container_client.get_blob_client(blob_name)
                with open(filepath, 'wb') as download_file:
                    download_file.write(blob_client.download_blob().readall())
                result_data = {'downloaded': True, 'container': container_name, 'blob': blob_name}
            
            elif operation == 'list':
                blobs = container_client.list_blobs()
                objects = []
                for blob in blobs:
                    objects.append({
                        'name': blob.name,
                        'size': blob.size,
                        'last_modified': blob.last_modified.isoformat(),
                        'content_type': blob.content_settings.content_type,
                    })
                result_data = {'objects': objects, 'count': len(objects)}
            
            elif operation == 'delete':
                if not blob_name:
                    return HandlerResult(success=False, error="Blob name required for delete")
                
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.delete_blob()
                result_data = {'deleted': True, 'container': container_name, 'blob': blob_name}
            
            else:
                return HandlerResult(success=False, error=f"Unknown operation: {operation}")
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data=result_data,
                duration_ms=duration_ms,
            )
            
        except ImportError:
            return HandlerResult(
                success=False,
                error="Azure Blob requires azure-storage-blob. Use pip install azure-storage-blob.",
            )
        except ResourceNotFoundError as e:
            return HandlerResult(
                success=False,
                error=f"Resource not found: {e}",
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )
