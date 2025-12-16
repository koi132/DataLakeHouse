import os
from pathlib import Path
from typing import Dict, Any, List
from dotenv import load_dotenv
import logging
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
os.environ["GOOGLE_API_KEY"] = GEMINI_API_KEY or ""

logger.info(f"GEMINI_API_KEY loaded: {'Yes' if GEMINI_API_KEY else 'No'}")

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
            "prd_cnt": "Get distinct product count from dim_product table",
            "revenue_by_region": "Get revenue analytics by geographic region and state",
            "top_products_by_category": "Get top 10 selling product categories with sales metrics",
            "review_analysis": "Get top 10 product categories review sentiment analysis with satisfaction rates"
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
- revenue_by_region: Returns revenue analytics grouped by geographic region and state (supports date filters: full_date_from, full_date_to)
- top_products_by_category: Returns top 20 selling product categories with items sold, order count, total sales and average price
- review_analysis: Returns top 20 product categories review sentiment analysis including total reviews, average score, positive/negative/neutral counts and satisfaction rate percentage

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
            model="gemini-2.5-flash", 
            google_api_key=GEMINI_API_KEY,
            temperature=0.3,
            timeout=60,
            max_retries=2
        )
        
        self.tools = [get_data, list_available_apis, get_api_schema]
        
        self.agent = create_react_agent(
            self.llm,
            self.tools
        )
        
        self.chat_history: List = []
        logger.info("DataChatbot initialized successfully")
    
    def chat(self, user_message: str) -> str:
        try:
            logger.info(f"Received message: {user_message[:100]}...")
            
            # Simple test first - direct LLM call without agent
            if user_message.lower() in ["hello", "hi", "xin chào", "chào"]:
                logger.info("Simple greeting - responding directly")
                return "Xin chào! Tôi là Data Lakehouse Assistant. Tôi có thể giúp bạn truy vấn dữ liệu về khách hàng, sản phẩm, doanh thu và đánh giá. Bạn cần hỗ trợ gì?"
            
            messages = [SystemMessage(content=SYSTEM_PROMPT)]
            messages.extend(self.chat_history)
            messages.append(HumanMessage(content=user_message))
            
            logger.info("Invoking agent...")
            response = self.agent.invoke(
                {"messages": messages},
                {"recursion_limit": 10}  # Limit recursion to prevent infinite loops
            )
            logger.info("Agent response received")
            
            # Get the last AI message
            ai_messages = [m for m in response["messages"] if isinstance(m, AIMessage)]
            if ai_messages:
                last_response = ai_messages[-1].content
                
                # Update history
                self.chat_history.append(HumanMessage(content=user_message))
                self.chat_history.append(AIMessage(content=last_response))
                
                logger.info(f"Response generated: {str(last_response)[:100]}...")
                return last_response
            
            logger.warning("No AI message in response")
            return "No response generated"
            
        except Exception as e:
            logger.error(f"Error in chat: {str(e)}")
            logger.error(traceback.format_exc())
            raise e
    
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
    # Always create new instance to pick up config changes
    _chatbot_instance = DataChatbot()
    return _chatbot_instance
