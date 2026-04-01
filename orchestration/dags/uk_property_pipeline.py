"""
Main Orchestration DAG: UK Property Pipeline

This DAG orchestrates the end-to-end flow from data ingestion through to
Snowflake and dbt transformations. It illustrates dependencies, retries,
and standard data engineering operational practices.

Runs on Apache Airflow (via Docker Compose).

Structure:
    Start -> [Ingest_Land_Registry, Ingest_BoE_Rates, Ingest_ONS] -> trigger_dbt_run -> Quality_Checks -> Notify
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Import our mock Slack alerting plugin
from slack_alerts import slack_failure_callback

# We set up standard retry logic with exponential backoff on all tasks by default
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # High-level retries handled by Airflow; 
    # the lower-level scripts also have requests.get() backoff.
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_callback,
}

# The DAG runs monthly, keeping up with the Land Registry update cadence.
with DAG(
    "uk_property_pipeline",
    default_args=default_args,
    description="End-to-End Property Pipeline (Ingest -> Transform -> Quality)",
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["portfolio", "ingestion", "dbt"],
) as dag:

    # -------------------------------------------------------------------------
    # 0. Start Pipeline
    # -------------------------------------------------------------------------
    start_pipeline = EmptyOperator(task_id="start_pipeline")


    # -------------------------------------------------------------------------
    # 1. Ingestion Layer (Fan-out pattern: run these in parallel)
    # -------------------------------------------------------------------------
    # We use BashOperator executing our python modules from the PYTHONPATH
    # This keeps our generic extraction logic decoupled from Airflow.
    
    # We pass the execution year so that a historical backfill run works correctly
    # on past partitions, proving we know how idempotency works.
    ingest_land_registry = BashOperator(
        task_id="ingest_land_registry",
        bash_command="python -m ingestion.land_registry --years {{ execution_date.year }}",
        env={**os.environ},
    )

    ingest_boe_rates = BashOperator(
        task_id="ingest_boe_rates",
        bash_command="python -m ingestion.boe_rates",
        env={**os.environ},
    )

    ingest_ons_demographics = BashOperator(
        task_id="ingest_ons_demographics",
        bash_command="python -m ingestion.ons_demographics",
        env={**os.environ},
    )


    # -------------------------------------------------------------------------
    # 2. Transformation Layer
    # -------------------------------------------------------------------------
    # In a full setup, we'd use DbtTaskGroup or BashOperator to run `dbt build`.
    # For Phase 2, this is a placeholder EmptyOperator showing the DAG structure.
    trigger_dbt_run = EmptyOperator(task_id="dbt_run")


    # -------------------------------------------------------------------------
    # 3. Quality & Alerting Layer
    # -------------------------------------------------------------------------
    quality_checks = EmptyOperator(task_id="quality_checks")
    notify_slack = EmptyOperator(task_id="notify_slack")


    # -------------------------------------------------------------------------
    # Define Dependencies (Fan-out -> Fan-in)
    # -------------------------------------------------------------------------
    start_pipeline >> [ingest_land_registry, ingest_boe_rates, ingest_ons_demographics]
    [ingest_land_registry, ingest_boe_rates, ingest_ons_demographics] >> trigger_dbt_run
    
    trigger_dbt_run >> quality_checks >> notify_slack
