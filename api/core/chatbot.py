import os
from typing import Dict, Any, List
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

GEMINI_API_KEY = "gemini api key here"
os.environ["GOOGLE_API_KEY"] = GEMINI_API_KEY

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

from config import get_available_sql_files
from core.sql_query import execute_sql_with_filters, get_sql_schema

# Define Tools using LangChain @tool decorator
@tool
def get_data(api_name: str, limit: int = 50) -> Dict[str, Any]:
    """Fetch data from SQL API.
    
    Args:
        api_name: Name of the API (cus_cnt for customer count, pro_cnt for product count)
        limit: Maximum records to return (default 50)
    """
    available = get_available_sql_files()
    if api_name not in available:
        return {"error": f"API '{api_name}' not found. Available: {available}"}
    
    data = execute_sql_with_filters(api_name, {}, limit)
    return {"api": api_name, "count": len(data), "data": data}

@tool
def list_available_apis() -> Dict[str, Any]:
    """Get list of all available data APIs with descriptions."""
    apis = get_available_sql_files()
    return {
        "available_apis": apis,
        "descriptions": {
            "cus_cnt": "Get customer count from dim_customer table",
            "prd_cnt": "Get distinct product count from dim_product table"
        }
    }

@tool
def get_api_schema(api_name: str) -> Dict[str, Any]:
    """Get schema information for a specific API.
    
    Args:
        api_name: Name of the API to get schema for
    """
    available = get_available_sql_files()
    if api_name not in available:
        return {"error": f"API '{api_name}' not found. Available: {available}"}
    return get_sql_schema(api_name)


SYSTEM_PROMPT = """You are a data analyst assistant for a Data Lakehouse system.
You help users query and understand data from the available APIs.

Available APIs:
- cus_cnt: Returns customer count from the gold layer dim_customer table
- prd_cnt: Returns distinct product count from the gold layer dim_product table

When users ask about data:
1. Use get_data tool to fetch the actual data
2. Present the results clearly
3. Provide insights when appropriate

Always be concise and helpful. Answer in the same language as the user's question."""


class DataChatbot:
    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY is not set. Please configure it in .env file or environment variables.")
        
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            google_api_key=GEMINI_API_KEY,
            temperature=0.3
        )
        
        self.tools = [get_data, list_available_apis, get_api_schema]
        
        self.agent = create_react_agent(
            self.llm,
            self.tools
        )
        
        self.chat_history: List = []
    
    def chat(self, user_message: str) -> str:
        
        messages = [SystemMessage(content=SYSTEM_PROMPT)]
        messages.extend(self.chat_history)
        messages.append(HumanMessage(content=user_message))
        
        response = self.agent.invoke({"messages": messages})
        
        # Get the last AI message
        ai_messages = [m for m in response["messages"] if isinstance(m, AIMessage)]
        if ai_messages:
            last_response = ai_messages[-1].content
            
            # Update history
            self.chat_history.append(HumanMessage(content=user_message))
            self.chat_history.append(AIMessage(content=last_response))
            
            return last_response
        
        return "No response generated"
    
    def reset(self):
        self.chat_history = []
    
    def get_history(self) -> List[Dict]:
        history = []
        for msg in self.chat_history:
            if isinstance(msg, HumanMessage):
                history.append({"role": "user", "content": msg.content})
            elif isinstance(msg, AIMessage) and msg.content:
                history.append({"role": "assistant", "content": msg.content})
        return history


# Singleton instance
_chatbot_instance = None

def get_chatbot() -> DataChatbot:
    global _chatbot_instance
    if _chatbot_instance is None:
        _chatbot_instance = DataChatbot()
    return _chatbot_instance
