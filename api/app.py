import logging
import os
from datetime import datetime
import time 
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from core import execute_sql_with_filters, get_sql_schema, get_trino_connection, execute_raw_query
from core.chatbot import get_chatbot
from config import get_available_sql_files, validate_sql_file_exists

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Trino SQL API",
    description="API for SQL queries on Trino with MiMo AI Chatbot",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def root():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))

class ChatRequest(BaseModel):
    message: str

class ChatResponse(BaseModel):
    response: str
    timestamp: str

# SQL Editor endpoints
class SqlExecuteRequest(BaseModel):
    sql: str
    limit: int = 100

@app.post("/sql/execute")
async def execute_sql_direct(request: SqlExecuteRequest):
    """Execute raw SQL on Trino (SELECT only)."""
    try:
        start = time.time()
        result = execute_raw_query(request.sql, request.limit)
        result["execution_time_ms"] = round((time.time() - start) * 1000)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


@app.get("/sql/schemas")
async def get_schema_tree():
    """Return catalog -> schema -> table -> columns tree from Trino."""
    try:
        with get_trino_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW CATALOGS")
            catalogs = [row[0] for row in cursor.fetchall()]

            tree = []
            for cat in catalogs:
                cat_node = {"name": cat, "type": "catalog", "children": []}
                try:
                    cursor.execute(f"SHOW SCHEMAS FROM {cat}")
                    schemas = [r[0] for r in cursor.fetchall() if r[0] != "information_schema"]
                    for sch in schemas:
                        sch_node = {"name": sch, "type": "schema", "children": []}
                        try:
                            cursor.execute(
                                f"SELECT table_name, column_name, data_type "
                                f"FROM {cat}.information_schema.columns "
                                f"WHERE table_schema = '{sch}' "
                                f"ORDER BY table_name, ordinal_position"
                            )
                            tables = {}
                            for tname, cname, dtype in cursor.fetchall():
                                if tname not in tables:
                                    tables[tname] = []
                                tables[tname].append({"name": cname, "type": dtype})
                            for tname, cols in tables.items():
                                sch_node["children"].append({"name": tname, "type": "table", "columns": cols})
                        except Exception:
                            try:
                                cursor.execute(f"SHOW TABLES FROM {cat}.{sch}")
                                for r in cursor.fetchall():
                                    sch_node["children"].append({"name": r[0], "type": "table", "columns": []})
                            except Exception:
                                pass
                        cat_node["children"].append(sch_node)
                except Exception:
                    pass
                tree.append(cat_node)

            cursor.close()
        return {"tree": tree}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema error: {str(e)}")
    
# Dynamic endpoint
@app.get("/api/v1/{sql_file_name}")
async def get_data_universal(
    sql_file_name: str,
    request: Request,
    limit: int = Query(50, le=10000, description="Limit results")
):

    if not validate_sql_file_exists(sql_file_name):
        raise HTTPException(status_code=404, detail=f"SQL file {sql_file_name} not found")

    # Get all query parameters as filters
    query_params = dict(request.query_params)
    query_params.pop('limit', None)

    data = execute_sql_with_filters(sql_file_name, query_params, limit)

    return {
        "count": len(data),
        "data": data,
        "sql_file": sql_file_name,
        "filters": query_params,
        "timestamp": datetime.now().isoformat()
    }

# Get schema information
@app.get("/api/v1/{sql_file_name}/schema")
async def get_schema_endpoint(sql_file_name: str):

    if not validate_sql_file_exists(sql_file_name):
        raise HTTPException(status_code=404, detail=f"SQL file {sql_file_name} not found")

    return get_sql_schema(sql_file_name)

@app.get("/apis")
async def list_apis():
    return {
        "endpoint": "/api/v1/{sql_file_name}",
        "sql_files": get_available_sql_files(),

        "filter_syntax": {
            "exact_match": "?column_name=value",
            "greater_than_equal": "?column_name_gte=value or ?column_name_from=value",
            "less_than_equal": "?column_name_lte=value or ?column_name_to=value",
            "greater_than": "?column_name_gt=value",
            "less_than": "?column_name_lt=value",
            "like_search": "?column_name_like=search_term",
            "in_list": "?column_name_in=value1,value2,value3"
        },
        "schema_endpoint": "/api/v1/{sql_file_name}/schema"
    }

# Chatbot endpoints
@app.post("/chat", response_model=ChatResponse)
async def chat_with_bot(request: ChatRequest):
    try:
        logger.info(f"Chat request received: {request.message[:50]}...")
        chatbot = get_chatbot()
        response = chatbot.chat(request.message)
        logger.info("Chat response generated successfully")
        return ChatResponse(
            response=response,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        # Safety net: if even the chatbot fails to initialize, still return
        # a polite message rather than an HTTP 500 with a raw traceback.
        logger.error(f"Chatbot error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return ChatResponse(
            response=(
                "I'm sorry, I'm temporarily unavailable right now. 😔 "
                "Please try again in a moment — the system may still be starting up."
            ),
            timestamp=datetime.now().isoformat()
        )


@app.post("/chat/reset")
async def reset_chat():
    chatbot = get_chatbot()
    chatbot.reset()
    return {"message": "Chat history cleared", "timestamp": datetime.now().isoformat()}


@app.get("/chat/history")
async def get_chat_history():
    chatbot = get_chatbot()
    return {
        "history": chatbot.get_history(),
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
