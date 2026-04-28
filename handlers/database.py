"""
Database handlers.

Handlers for SQL, NoSQL, key-value stores.
"""

import time
import json
import sqlite3
import os
from typing import Dict, Any, List, Optional, Union
from pathlib import Path

from handlers.base import (
    TaskHandler,
    HandlerResult,
    HandlerConfidence,
)


class SQLQueryHandler(TaskHandler):
    """Execute SQL queries."""
    
    name = "sql_query"
    category = "database"
    description = "Execute SQL query on database"
    dangerous = True
    
    input_schema = {
        'query': {'type': 'string', 'required': True, 'description': 'SQL query'},
        'database': {'type': 'string', 'required': True, 'description': 'Database path or connection string'},
        'params': {'type': 'array', 'description': 'Query parameters'},
        'fetch': {'type': 'string', 'default': 'all', 'enum': ['all', 'one', 'none'], 'description': 'How to fetch results'},
    }
    
    output_schema = {
        'rows': {'type': 'array', 'description': 'Result rows'},
        'rowcount': {'type': 'integer', 'description': 'Rows affected'},
        'lastrowid': {'type': 'integer', 'description': 'Last inserted row ID'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['sql_query', 'sql', 'query', 'db_query']:
            return HandlerConfidence.PERFECT.value
        if 'query' in params and params.get('query', '').strip().upper().startswith(('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE')):
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        query = params['query']
        database = params['database']
        query_params = params.get('params', [])
        fetch_mode = params.get('fetch', 'all')
        
        conn = None
        try:
            # Determine database type
            if database.startswith(('postgresql://', 'postgres://')):
                return HandlerResult(
                    success=False,
                    error="PostgreSQL requires psycopg2. Use pip install psycopg2-binary.",
                    duration_ms=(time.time() - start_time) * 1000,
                )
            elif database.startswith('mysql://'):
                return HandlerResult(
                    success=False,
                    error="MySQL requires mysql-connector-python or pymysql.",
                    duration_ms=(time.time() - start_time) * 1000,
                )
            else:
                # SQLite
                conn = sqlite3.connect(database)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                if query_params:
                    cursor.execute(query, query_params)
                else:
                    cursor.execute(query)
                
                result_data = {
                    'rowcount': cursor.rowcount,
                    'lastrowid': cursor.lastrowid,
                }
                
                # Fetch results
                if query.strip().upper().startswith('SELECT'):
                    if fetch_mode == 'all':
                        rows = [dict(row) for row in cursor.fetchall()]
                    elif fetch_mode == 'one':
                        row = cursor.fetchone()
                        rows = [dict(row)] if row else []
                    else:
                        rows = []
                    
                    result_data['rows'] = rows
                    result_data['rowcount'] = len(rows)
                else:
                    conn.commit()
                    result_data['rows'] = []
                
                duration_ms = (time.time() - start_time) * 1000
                
                return HandlerResult(
                    success=True,
                    data=result_data,
                    duration_ms=duration_ms,
                )
                
        except sqlite3.Error as e:
            return HandlerResult(
                success=False,
                error=f"SQLite error: {e}",
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )
        finally:
            if conn:
                conn.close()


class RedisHandler(TaskHandler):
    """Redis key-value operations."""
    
    name = "redis"
    category = "database"
    description = "Redis key-value store operations"
    
    input_schema = {
        'operation': {'type': 'string', 'required': True, 'enum': ['get', 'set', 'delete', 'exists', 'expire', 'keys']},
        'key': {'type': 'string', 'required': True, 'description': 'Redis key'},
        'value': {'type': 'any', 'description': 'Value to set'},
        'ttl': {'type': 'integer', 'description': 'TTL in seconds'},
        'pattern': {'type': 'string', 'description': 'Key pattern for keys operation'},
        'host': {'type': 'string', 'default': 'localhost'},
        'port': {'type': 'integer', 'default': 6379},
        'db': {'type': 'integer', 'default': 0},
    }
    
    output_schema = {
        'value': {'type': 'any'},
        'exists': {'type': 'boolean'},
        'keys': {'type': 'array'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['redis', 'redis_get', 'redis_set', 'kv']:
            return HandlerConfidence.PERFECT.value
        if params.get('key') and params.get('operation') in ['get', 'set', 'delete']:
            return HandlerConfidence.HIGH.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        operation = params['operation']
        key = params['key']
        value = params.get('value')
        ttl = params.get('ttl')
        pattern = params.get('pattern', '*')
        host = params.get('host', 'localhost')
        port = params.get('port', 6379)
        db = params.get('db', 0)
        
        try:
            import redis
            
            r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
            
            if operation == 'get':
                val = r.get(key)
                # Try to parse JSON
                if val:
                    try:
                        val = json.loads(val)
                    except json.JSONDecodeError:
                        pass
                result_data = {'value': val, 'exists': val is not None}
            
            elif operation == 'set':
                if value is None:
                    return HandlerResult(success=False, error="Value required for set operation")
                
                # Serialize non-strings
                if not isinstance(value, str):
                    value = json.dumps(value)
                
                if ttl:
                    r.setex(key, ttl, value)
                else:
                    r.set(key, value)
                result_data = {'value': value, 'set': True}
            
            elif operation == 'delete':
                deleted = r.delete(key)
                result_data = {'deleted': deleted > 0}
            
            elif operation == 'exists':
                exists = r.exists(key)
                result_data = {'exists': exists > 0}
            
            elif operation == 'expire':
                if ttl is None:
                    return HandlerResult(success=False, error="TTL required for expire operation")
                r.expire(key, ttl)
                result_data = {'expired': True}
            
            elif operation == 'keys':
                keys = r.keys(pattern)
                result_data = {'keys': keys}
            
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
                error="Redis requires redis package. Use pip install redis.",
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class MongoDBHandler(TaskHandler):
    """MongoDB document operations."""
    
    name = "mongodb"
    category = "database"
    description = "MongoDB document database operations"
    
    input_schema = {
        'operation': {'type': 'string', 'required': True, 'enum': ['find', 'insert', 'update', 'delete', 'count']},
        'collection': {'type': 'string', 'required': True, 'description': 'Collection name'},
        'document': {'type': 'object', 'description': 'Document to insert'},
        'filter': {'type': 'object', 'description': 'Query filter'},
        'update': {'type': 'object', 'description': 'Update operations'},
        'limit': {'type': 'integer', 'default': 100},
        'skip': {'type': 'integer'},
        'uri': {'type': 'string', 'default': 'mongodb://localhost:27017'},
        'database': {'type': 'string', 'default': 'test'},
    }
    
    output_schema = {
        'documents': {'type': 'array'},
        'inserted_id': {'type': 'string'},
        'modified_count': {'type': 'integer'},
        'deleted_count': {'type': 'integer'},
        'count': {'type': 'integer'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['mongodb', 'mongo', 'db_find', 'db_insert']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        operation = params['operation']
        collection_name = params['collection']
        database_name = params.get('database', 'test')
        uri = params.get('uri', 'mongodb://localhost:27017')
        
        try:
            from pymongo import MongoClient
            from bson.objectid import ObjectId
            
            client = MongoClient(uri)
            db = client[database_name]
            collection = db[collection_name]
            
            if operation == 'find':
                query_filter = params.get('filter', {})
                cursor = collection.find(query_filter)
                
                if params.get('skip'):
                    cursor = cursor.skip(params['skip'])
                if params.get('limit'):
                    cursor = cursor.limit(params['limit'])
                
                # Convert ObjectIds to strings
                documents = []
                for doc in cursor:
                    if '_id' in doc:
                        doc['_id'] = str(doc['_id'])
                    documents.append(doc)
                
                result_data = {'documents': documents, 'count': len(documents)}
            
            elif operation == 'insert':
                document = params.get('document')
                if not document:
                    return HandlerResult(success=False, error="Document required for insert operation")
                
                result = collection.insert_one(document)
                result_data = {'inserted_id': str(result.inserted_id)}
            
            elif operation == 'update':
                query_filter = params.get('filter', {})
                update_ops = params.get('update', {})
                
                result = collection.update_many(query_filter, update_ops)
                result_data = {'modified_count': result.modified_count}
            
            elif operation == 'delete':
                query_filter = params.get('filter', {})
                result = collection.delete_many(query_filter)
                result_data = {'deleted_count': result.deleted_count}
            
            elif operation == 'count':
                query_filter = params.get('filter', {})
                count = collection.count_documents(query_filter)
                result_data = {'count': count}
            
            else:
                return HandlerResult(success=False, error=f"Unknown operation: {operation}")
            
            client.close()
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data=result_data,
                duration_ms=duration_ms,
            )
            
        except ImportError:
            return HandlerResult(
                success=False,
                error="MongoDB requires pymongo. Use pip install pymongo.",
            )
        except Exception as e:
            return HandlerResult(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )


class SQLiteHandler(TaskHandler):
    """SQLite database operations (simpler interface)."""
    
    name = "sqlite"
    category = "database"
    description = "SQLite database operations with auto-schema"
    
    input_schema = {
        'operation': {'type': 'string', 'required': True, 'enum': ['create', 'insert', 'select', 'update', 'delete', 'schema']},
        'table': {'type': 'string', 'description': 'Table name'},
        'data': {'type': 'object', 'description': 'Data for insert/update'},
        'where': {'type': 'object', 'description': 'Where conditions'},
        'path': {'type': 'string', 'default': ':memory:', 'description': 'Database file path'},
    }
    
    output_schema = {
        'rows': {'type': 'array'},
        'lastrowid': {'type': 'integer'},
        'changes': {'type': 'integer'},
        'schema': {'type': 'object'},
    }
    
    def can_handle(self, params: Dict[str, Any]) -> float:
        if params.get('task') in ['sqlite', 'sqlite3']:
            return HandlerConfidence.PERFECT.value
        return 0.0
    
    def execute(self, params: Dict[str, Any]) -> HandlerResult:
        start_time = time.time()
        operation = params['operation']
        table = params.get('table')
        data = params.get('data', {})
        where = params.get('where', {})
        path = params.get('path', ':memory:')
        
        conn = None
        try:
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if operation == 'create':
                if not table:
                    return HandlerResult(success=False, error="Table name required")
                
                columns = params.get('columns')
                if not columns:
                    # Infer from data
                    columns = {k: 'TEXT' for k in data.keys()}
                
                cols_sql = ', '.join([f'{k} {v}' for k, v in columns.items()])
                cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({cols_sql})')
                conn.commit()
                
                result_data = {'created': True, 'table': table}
            
            elif operation == 'insert':
                if not table or not data:
                    return HandlerResult(success=False, error="Table and data required")
                
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['?' for _ in data])
                values = list(data.values())
                
                cursor.execute(f'INSERT INTO {table} ({columns}) VALUES ({placeholders})', values)
                conn.commit()
                
                result_data = {'lastrowid': cursor.lastrowid, 'changes': cursor.rowcount}
            
            elif operation == 'select':
                if not table:
                    return HandlerResult(success=False, error="Table name required")
                
                query = f'SELECT * FROM {table}'
                values = []
                
                if where:
                    where_clause = ' AND '.join([f'{k} = ?' for k in where.keys()])
                    query += f' WHERE {where_clause}'
                    values = list(where.values())
                
                cursor.execute(query, values)
                rows = [dict(row) for row in cursor.fetchall()]
                
                result_data = {'rows': rows, 'count': len(rows)}
            
            elif operation == 'update':
                if not table or not data or not where:
                    return HandlerResult(success=False, error="Table, data, and where required")
                
                set_clause = ', '.join([f'{k} = ?' for k in data.keys()])
                where_clause = ' AND '.join([f'{k} = ?' for k in where.keys()])
                values = list(data.values()) + list(where.values())
                
                cursor.execute(f'UPDATE {table} SET {set_clause} WHERE {where_clause}', values)
                conn.commit()
                
                result_data = {'changes': cursor.rowcount}
            
            elif operation == 'delete':
                if not table or not where:
                    return HandlerResult(success=False, error="Table and where required")
                
                where_clause = ' AND '.join([f'{k} = ?' for k in where.keys()])
                values = list(where.values())
                
                cursor.execute(f'DELETE FROM {table} WHERE {where_clause}', values)
                conn.commit()
                
                result_data = {'changes': cursor.rowcount}
            
            elif operation == 'schema':
                if not table:
                    # List all tables
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]
                    result_data = {'tables': tables}
                else:
                    cursor.execute(f'PRAGMA table_info({table})')
                    columns = [{'name': row[1], 'type': row[2]} for row in cursor.fetchall()]
                    result_data = {'table': table, 'columns': columns}
            
            else:
                return HandlerResult(success=False, error=f"Unknown operation: {operation}")
            
            duration_ms = (time.time() - start_time) * 1000
            
            return HandlerResult(
                success=True,
                data=result_data,
                duration_ms=duration_ms,
            )
            
        except sqlite3.Error as e:
            return HandlerResult(
                success=False,
                error=f"SQLite error: {e}",
                duration_ms=(time.time() - start_time) * 1000,
            )
        finally:
            if conn:
                conn.close()
