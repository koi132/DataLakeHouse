import trino
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import os
from datetime import datetime, date
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

def get_trino_config():
    return {
        "host": os.getenv("TRINO_HOST", "localhost"),
        "port": int(os.getenv("TRINO_PORT", "8080")),
        "user": os.getenv("TRINO_USER", "admin"),
        "catalog": os.getenv("TRINO_CATALOG", "delta"),
        "schema": os.getenv("TRINO_SCHEMA", "gold"),
        "http_scheme": os.getenv("TRINO_HTTP_SCHEME", "http"),
        "auth": None
    }

@contextmanager
def get_trino_connection():
    conn = None
    try:
        config = get_trino_config()
        conn = trino.dbapi.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            catalog=config["catalog"],
            schema=config["schema"]
        )
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def execute_query(query: str) -> Dict[str, Any]:
    with get_trino_connection() as conn:
        cursor = conn.cursor()
        
        try:
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            rows = cursor.fetchall()
            
            # Format data
            data = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    if isinstance(value, (date, datetime)):
                        row_dict[columns[i]] = value.isoformat()
                    else:
                        row_dict[columns[i]] = value
                data.append(row_dict)
            
            return {
                "count": len(data),
                "query_executed": query.strip(),
                "data": data,
                "columns": columns
            }
            
        finally:
            cursor.close()