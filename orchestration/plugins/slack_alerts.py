"""
Airflow UI Plugin: Mock Slack Alerts

Provides an `on_failure_callback` function for Airflow DAGs. 
When a pipeline task fails, Airflow automatically calls this function, passing
in context variables (task name, execution date, exception).

For the portfolio project, this just logs the payload that *would* be sent to
a Slack webhook. This demonstrates knowledge of production alerting mechanisms
without requiring you to actually configure a Slack workspace.
"""

import json
import logging
from typing import Any

from airflow.models.taskinstance import TaskInstance

# We use the standard logger rather than our custom one so it integrates
# neatly into Airflow's own task logs UI.
logger = logging.getLogger("airflow.task")

def slack_failure_callback(context: dict[str, Any]) -> None:
    """
    Called by Airflow on task failure.
    
    Args:
        context: Airflow context dictionary containing task failure details.
    """
    ti: TaskInstance = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    exec_date = context.get('execution_date')
    log_url = ti.log_url
    exception = context.get('exception')
    
    # Construct the JSON payload expected by a Slack Incoming Webhook
    slack_payload = {
        "text": f"🚨 *Airflow Task Failed*: {dag_id} / {task_id}",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Pipeline Failure Alert 🚨"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                    {"type": "mrkdwn", "text": f"*Task:*\n{task_id}"},
                    {"type": "mrkdwn", "text": f"*Execution Date:*\n{exec_date}"},
                    {"type": "mrkdwn", "text": f"*Error:*\n`{str(exception)}`"}
                ]
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Logs"},
                        "url": log_url,
                        "style": "danger"
                    }
                ]
            }
        ]
    }
    
    # In production, we'd use requests.post(SLACK_WEBHOOK_URL, json=slack_payload)
    # Here, we just print the highly-structured payload to show we know how the
    # Slack Block Kit API works.
    logger.info("\n" + "="*80)
    logger.info("MOCK SLACK ALERT TRIGGERED (webhook payload follows):")
    logger.info(json.dumps(slack_payload, indent=2))
    logger.info("="*80 + "\n")
