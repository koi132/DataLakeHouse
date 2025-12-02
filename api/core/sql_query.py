import os
from typing import Dict, List, Any
from fastapi import HTTPException
from config import get_sql_file_path, validate_sql_file_exists
from core import execute_query

def load_sql_file(sql_file_name: str) -> str:
    if not validate_sql_file_exists(sql_file_name):
        raise HTTPException(status_code=404, detail=f"SQL file {sql_file_name} not found")
    
    sql_path = get_sql_file_path(sql_file_name)
    
    if not os.path.exists(sql_path):
        raise HTTPException(status_code=404, detail=f"SQL file {sql_path} not found")
    
    with open(sql_path, 'r', encoding='utf-8') as f:
        return f.read().strip()


def build_dynamic_filters(base_sql: str, filters: Dict[str, Any]) -> str:
    if not filters:
        return base_sql
    
    where_conditions = []
    
    for key, value in filters.items():
        if value is not None and value != '':
            
            if key.endswith('_from') or key.endswith('_gte'):
                # Greater than or equal
                column = key.replace('_from', '').replace('_gte', '')
                where_conditions.append(f"{column} >=  DATE'{value}'")
            
            elif key.endswith('_to') or key.endswith('_lte'):
                # Less than or equal
                column = key.replace('_to', '').replace('_lte', '')
                where_conditions.append(f"{column} <= DATE'{value}'")
            
            elif key.endswith('_gt'):
                # Greater than
                column = key.replace('_gt', '')
                where_conditions.append(f"{column} >  DATE'{value}'")

            elif key.endswith('_lt'):
                # Less than
                column = key.replace('_lt', '')
                where_conditions.append(f"{column} <  DATE'{value}'")
            
            elif key.endswith('_like'):
                # LIKE operator
                column = key.replace('_like', '')
                where_conditions.append(f"{column} LIKE '%{value}%'")
            
            elif key.endswith('_in'):
                # IN operator
                column = key.replace('_in', '')
                values = value.split(',')
                values_str = "','".join(values)
                where_conditions.append(f"{column} IN ('{values_str}')")
            
            else:
                # Exact match
                where_conditions.append(f"{key} = '{value}'")
    
    if where_conditions:
        if 'WHERE' in base_sql.upper():
            base_sql += ' AND ' + ' AND '.join(where_conditions)
        else:
            base_sql += f" WHERE {' AND '.join(where_conditions)}"
    
    return base_sql


def execute_sql_with_filters(sql_file_name: str, filters: Dict[str, Any] = None, limit: int = 1000) -> List[Dict]:
    try:

        base_sql = load_sql_file(sql_file_name)
        
        filtered_sql = build_dynamic_filters(base_sql, filters or {})
        
        # Add limit if not already present
        if 'LIMIT' not in filtered_sql.upper():
            filtered_sql += f" LIMIT {limit}"
        
        result = execute_query(filtered_sql)
        
        return result["data"]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


def get_sql_schema(sql_file_name: str) -> Dict:
    try:

        base_sql = load_sql_file(sql_file_name)
        
        schema_sql = f"SELECT * FROM ({base_sql}) LIMIT 1"
        
        result = execute_query(schema_sql)
        
        # Extract column information
        columns = []
        if result["data"]:
            sample_row = result["data"][0]
            for column_name, value in sample_row.items():
                columns.append({
                    "name": column_name,
                    "example": value
                })
        
        return {
            "sql_file": sql_file_name,
            "columns": columns,
            "filter_examples": {
                "exact_match": "?column_name=value",
                "greater_than": "?column_name_gte=value",
                "less_than": "?column_name_lte=value", 
                "date_range": "?date_from=2024-01-01&date_to=2024-12-31",
                "like_search": "?column_name_like=search_term",
                "in_list": "?column_name_in=value1,value2,value3"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema error: {str(e)}")
