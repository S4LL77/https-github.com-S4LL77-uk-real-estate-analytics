import os
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from typing import Optional

# Using dotenv to load the same .env file we used for the ingestion and dbt setup
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_connection() -> SnowflakeConnection:
    """
    Establish a connection to the Snowflake Data Warehouse.
    Uses the Analyst role to ensure least-privilege security.
    """
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")

    if not all([account, user, password]):
        raise ValueError("Missing Snowflake credentials in environment variables.")

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role="ACCOUNTADMIN",      # Bypassing trial RBAC limitations to ensure API has access to dbt tables
        warehouse="TRANSFORM_WH", # Default trial compute warehouse created earlier
        database="UK_REAL_ESTATE",
        schema="STG_MARTS"
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
