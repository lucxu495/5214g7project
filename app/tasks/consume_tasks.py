"""
Project: QF5214 Polymarket Data Pipeline
File: consume_tasks.py
Author: Xu
Created: 2026-03-14

Description:
Kafka consumer task definitions
"""
from app.celery.celery_app import celery_app
from app.services.consume_service import consume_service
from app.utils.wrapper_utils import log_task_run


@celery_app.task(
    name="app.tasks.consume_tasks.consume_polymarket_markets_raw_to_clickhouse",
    time_limit=900,
)
@log_task_run(
    task_name="consume_polymarket_markets_raw_to_clickhouse",
    task_type="consume",
)
def consume_polymarket_markets_raw_to_clickhouse(
    max_messages_per_partition: int = 10000,
    max_workers: int = 4,
):
    """
    Consume polymarket.markets.raw from Kafka, parse snapshots, and insert into
    ClickHouse dim/snapshot tables while preserving captured_at.
    """
    return consume_service.consume_polymarket_markets_raw_to_clickhouse(
        max_messages_per_partition=max_messages_per_partition,
        max_workers=max_workers,
    )
