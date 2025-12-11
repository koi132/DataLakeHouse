from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import datetime
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from core import execute_sql_with_filters, get_sql_schema
from core.chatbot import get_chatbot
from config import get_available_sql_files, validate_sql_file_exists

app = FastAPI(
    title="Trino SQL API",
    description="API for SQL queries on Trino with Gemini Chatbot",
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
        logger.info(f"Chat response generated successfully")
        return ChatResponse(
            response=response,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        logger.error(f"Chatbot error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Chatbot error: {str(e)}")


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