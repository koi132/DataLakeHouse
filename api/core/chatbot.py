import logging
import os
import traceback
from pathlib import Path
from typing import Dict, Any, List

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

from config import get_available_sql_files
from core.sql_query import execute_sql_with_filters, get_sql_schema
from core.database import execute_query, execute_raw_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

MIMO_API_KEY = os.getenv("MIMO_API_KEY")
MIMO_BASE_URL = os.getenv("MIMO_BASE_URL", "https://api.xiaomimimo.com/v1")
MIMO_MODEL = os.getenv("MIMO_MODEL", "MiMo-V2.5-Pro")

logger.info(f"MIMO_API_KEY loaded: {'Yes' if MIMO_API_KEY else 'No'}")

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@tool
def list_tables() -> Dict[str, Any]:
    """List all tables available in the gold layer of the Data Lakehouse (delta.gold schema)."""
    try:
        result = execute_query("SHOW TABLES FROM delta.gold")
        tables = [row[list(row.keys())[0]] for row in result["data"]]
        return {"tables": tables, "schema": "delta.gold"}
    except Exception as e:
        return {"error": str(e)}


@tool
def describe_table(table_name: str) -> Dict[str, Any]:
    """Get column names and data types for a table in delta.gold.

    Args:
        table_name: Table name without schema prefix, e.g. 'fact_orders' or 'dim_customer'
    """
    try:
        result = execute_query(f"DESCRIBE delta.gold.{table_name}")
        columns = [
            {"column": row.get("Column", row.get("column", "")),
             "type": row.get("Type", row.get("type", ""))}
            for row in result["data"]
        ]
        return {"table": f"delta.gold.{table_name}", "columns": columns}
    except Exception as e:
        return {"error": str(e)}


@tool
def run_select_query(sql: str, limit: int = 50) -> Dict[str, Any]:
    """Execute a custom SELECT query against the Trino Data Lakehouse.

    Only SELECT statements are allowed. INSERT, UPDATE, DELETE, DROP, CREATE,
    ALTER, TRUNCATE, MERGE, REPLACE, GRANT, REVOKE are all blocked.
    A LIMIT clause will be added automatically if not present.
    Always use fully-qualified table names: delta.gold.<table_name>.

    Args:
        sql: A valid Trino SELECT query.
        limit: Max rows to return (default 50, max 1000).
    """
    try:
        limit = min(limit, 1000)
        result = execute_raw_query(sql, limit=limit)
        return {
            "query_executed": result["query_executed"],
            "count": result["count"],
            "columns": result["columns"],
            "data": result["data"],
        }
    except ValueError as e:
        # Safety guard triggered
        return {"error": str(e), "blocked": True}
    except Exception as e:
        return {"error": str(e)}


@tool
def get_data(api_name: str, limit: int = 50) -> Dict[str, Any]:
    """Fetch data using a pre-built SQL template by name.

    Available templates: cus_cnt, prd_cnt, revenue_by_region,
    top_products_by_category, review_analysis.

    Args:
        api_name: Template name (without .sql extension).
        limit: Maximum records to return (default 50).
    """
    available = get_available_sql_files()
    if api_name not in available:
        return {"error": f"Template '{api_name}' not found. Available: {available}"}
    
    from core.sql_query import load_sql_file
    try:
        base_sql = load_sql_file(api_name)
        if 'LIMIT' not in base_sql.upper():
            base_sql += f" LIMIT {limit}"
        query_executed = base_sql
    except Exception:
        query_executed = f"SELECT * FROM delta.gold.{api_name} LIMIT {limit}"

    data = execute_sql_with_filters(api_name, {}, limit)
    return {"api": api_name, "query_executed": query_executed, "count": len(data), "data": data}


# ---------------------------------------------------------------------------
# System prompt — full star schema knowledge baked in
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an expert Data Analyst assistant for a Data Lakehouse built on Apache Trino + Delta Lake.
You can answer ANY question about data in the lakehouse by generating and running Trino SQL.

════════════════════════════════════════════
STAR SCHEMA — delta.gold layer
════════════════════════════════════════════

FACT TABLES
───────────
• fact_orders   — one row per order item
    order_id, customer_sk, product_sk, date_sk,
    total_item_value, freight_value, payment_value,
    order_status, seller_sk

• fact_reviews   — product reviews linked to orders
    review_id, order_id, product_sk, date_sk,
    review_score, review_comment_title, review_comment_message

DIMENSION TABLES
────────────────
• dim_customer  — customer master
    customer_sk (PK), customer_id,
    customer_city, customer_state, customer_region

• dim_product   — product master
    product_sk (PK), product_id,
    product_category_name, product_category_name_english,
    product_weight_g, product_length_cm

• dim_date      — calendar dimension
    date_sk (PK), full_date, year, month, day,
    quarter, day_of_week, week_of_year

JOIN KEYS
─────────
  fact_orders.customer_sk  → dim_customer.customer_sk
  fact_orders.product_sk   → dim_product.product_sk
  fact_orders.date_sk      → dim_date.date_sk
  fact_reviews.product_sk  → dim_product.product_sk
  fact_reviews.date_sk     → dim_date.date_sk

════════════════════════════════════════════
PRE-BUILT TEMPLATES (shortcuts)
════════════════════════════════════════════
cus_cnt                 — total customer count
prd_cnt                 — distinct product count
revenue_by_region       — revenue grouped by region/state
top_products_by_category — top categories by sales
review_analysis         — sentiment breakdown by category

════════════════════════════════════════════
RULES YOU MUST FOLLOW (CRITICAL & STRICT)
════════════════════════════════════════════
1. ONLY generate SELECT queries. Never generate INSERT, UPDATE, DELETE,
   DROP, CREATE, ALTER, TRUNCATE, MERGE, REPLACE, GRANT, or REVOKE.
   If the user asks for a write operation, politely refuse and explain why.

2. Always use fully-qualified table names: delta.gold.<table_name>

3. When unsure about columns, call describe_table first.

4. Default LIMIT is 50. Apply LIMIT unless the user explicitly asks for all rows.

5. MANDATORY QUERY INSPECTOR REQUIREMENT:
   Whenever you run a query using `run_select_query` or load a pre-built template using `get_data`, you MUST include the exact executed SQL query in a ```sql markdown code block in your response. 
   Do NOT omit it! The frontend UI detects this ```sql block to render an interactive clickable accordion dropdown (query inspector).
   Example format:
   ---
   ### SQL Query Executed
   ```sql
   SELECT ...
   ```
   ### Results
   ...
   ---

6. Provide brief, meaningful insights after returning the data.

7. Language Preference: Always respond in the language the user asked in. If the user asks in Vietnamese ("năm có doanh thu cao nhất"), reply in Vietnamese! If the user asks in English, reply in English! Keep technical SQL code blocks in SQL.
"""


# ---------------------------------------------------------------------------
# Chatbot class
# ---------------------------------------------------------------------------

class DataChatbot:
    def __init__(self):
        if not MIMO_API_KEY:
            raise ValueError("MIMO_API_KEY is not set. Please configure it in .env file.")

        self.llm = ChatOpenAI(
            model=MIMO_MODEL,
            api_key=MIMO_API_KEY,
            base_url=MIMO_BASE_URL,
            temperature=0.3,
            timeout=60,
            max_retries=2,
            extra_body={"thinking": {"type": "disabled"}},
        )

        self.tools = [list_tables, describe_table, run_select_query, get_data]

        self.agent = create_react_agent(self.llm, self.tools)

        self.chat_history: List = []
        logger.info("DataChatbot initialized successfully")

    def chat(self, user_message: str) -> str:
        try:
            logger.info(f"Received message: {user_message[:100]}...")

            messages = [SystemMessage(content=SYSTEM_PROMPT)]
            messages.extend(self.chat_history)
            messages.append(HumanMessage(content=user_message))

            logger.info("Invoking agent...")
            response = self.agent.invoke(
                {"messages": messages},
                {"recursion_limit": 15}
            )
            logger.info("Agent response received")

            ai_messages = [m for m in response["messages"] if isinstance(m, AIMessage)]
            if ai_messages:
                last_response = ai_messages[-1].content

                self.chat_history.append(HumanMessage(content=user_message))
                self.chat_history.append(AIMessage(content=last_response))

                logger.info(f"Response: {str(last_response)[:100]}...")
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


# Singleton
_chatbot_instance = None

def get_chatbot() -> DataChatbot:
    global _chatbot_instance
    _chatbot_instance = DataChatbot()
    return _chatbot_instance
