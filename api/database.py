import os
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from typing import Optional

# Using dotenv to load the same .env file we used for the ingestion and dbt setup
from dotenv import load_dotenv

load_dotenv()

def get_config(key: str, default: str = None) -> str:
    """Helper to get config from st.secrets (Cloud) or os.getenv (Local)"""
    try:
        import streamlit as st
        if hasattr(st, "secrets") and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)

def get_snowflake_connection() -> SnowflakeConnection:
    """
    Establish a connection to the Snowflake Data Warehouse.
    Uses the Analyst role to ensure least-privilege security.
    """
    account = get_config("SNOWFLAKE_ACCOUNT")
    user = get_config("SNOWFLAKE_USER")
    password = get_config("SNOWFLAKE_PASSWORD")

    if not all([account, user, password]):
        raise ValueError("Missing Snowflake credentials. Add them to .env (local) or Streamlit Secrets (cloud).")

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=get_config("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=get_config("SNOWFLAKE_WAREHOUSE", "TRANSFORM_WH"),
        database=get_config("SNOWFLAKE_DATABASE", "UK_REAL_ESTATE"),
        schema=get_config("SNOWFLAKE_SCHEMA", "STG_MARTS")
    )
    
    return conn

def execute_query(query: str, params: Optional[tuple] = None) -> list[dict]:
    """Helper to execute a query and return results as a list of dictionaries."""
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            
            # Fetch column names from the cursor description
            columns = [col[0].lower() for col in cur.description]
            
            # Zip column names with row values for dict mapping
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()
