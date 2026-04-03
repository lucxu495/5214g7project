"""
Project: QF5214 Polymarket Data Pipeline
File: consume_service.py
Author: Xu
Created: 2026-03-14

Description:
Kafka consumer service utilities
"""
import json
from datetime import datetime, timezone
from typing import Any

from app.clients.clickhouse_client import ClickHouseClient
from app.clients.kafka_client import KafkaConsumerClient
from app.config.settings import KAFKA_TOPIC_POLYMARKET_MARKETS_RAW
from app.logging.logger import setup_logger
from app.schemas.clickhouse_tables import CLICKHOUSE_TABLE_COLS_ENUM


logger = setup_logger(__name__)


class ConsumeService:
    @staticmethod
    def _parse_dt_utc(value: str | None) -> datetime:
        if not value:
            return datetime.now(timezone.utc).replace(tzinfo=None)
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _as_float(value: Any, default: float = 0.0) -> float:
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _first_present(
        data: dict[str, Any],
        keys: tuple[str, ...],
        default: Any = None,
    ) -> Any:
        for key in keys:
            if key in data and data[key] is not None:
                return data[key]
        return default

    @staticmethod
    def _parse_outcomes(payload: dict[str, Any]) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        outcomes = payload.get("outcomes") or []
        if isinstance(outcomes, list):
            merged.extend(outcome for outcome in outcomes if isinstance(outcome, dict))

        for side_key in ("yes", "no", "up", "down"):
            side_outcome = payload.get(side_key)
            if isinstance(side_outcome, dict):
                merged.append(side_outcome)

        dedup: dict[str, dict[str, Any]] = {}
        for outcome in merged:
            outcome_id = outcome.get("outcome_id") or outcome.get("outcomeId")
            if outcome_id is None:
                continue
            dedup[str(outcome_id)] = outcome
        return list(dedup.values())

    @classmethod
    def consume_polymarket_markets_raw_to_clickhouse(
        cls,
        *,
        topic: str | None = None,
        group_id: str = "polymarket.markets.raw.to.clickhouse.v1",
        max_messages_per_partition: int = 10000,
        max_workers: int = 4,
    ) -> int:
        topic_name = topic or KAFKA_TOPIC_POLYMARKET_MARKETS_RAW
        logger.info(
            "Consuming Kafka topic '%s' into ClickHouse (group_id=%s)",
            topic_name,
            group_id,
        )

        with KafkaConsumerClient(
            topic=topic_name,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=10000,
        ) as consumer:
            messages_by_partition = consumer.consume(
                max_messages_per_partition=max_messages_per_partition,
                max_workers=max_workers,
            )

        messages = [
            message
            for partition_messages in messages_by_partition.values()
            for message in partition_messages
            if isinstance(message, dict)
        ]

        if not messages:
            logger.info("No Kafka messages consumed from topic '%s'", topic_name)
            return 0

        now = datetime.utcnow()
        market_dim_rows: dict[str, dict[str, Any]] = {}
        market_snapshot_rows: list[dict[str, Any]] = []
        outcome_snapshot_rows: list[dict[str, Any]] = []

        for message in messages:
            payload = message.get("payload") or {}
            if not isinstance(payload, dict):
                logger.warning("Skip invalid message payload: %s", type(payload))
                continue

            market_id = str(message.get("market_id") or payload.get("market_id") or "")
            if not market_id:
                logger.warning("Skip message missing market_id")
                continue

            captured_at = cls._parse_dt_utc(message.get("captured_at"))
            source = str(message.get("source") or "pmxt")
            exchange = str(message.get("exchange") or "polymarket")

            tags = payload.get("tags") or []
            if not isinstance(tags, list):
                tags = []

            # dim 只存描述信息，不用随时间更新
            market_dim_rows[market_id] = {
                "market_id": market_id,
                "event_id": str(payload.get("event_id") or ""),
                "title": str(payload.get("title") or ""),
                "description": str(payload.get("description") or ""),
                "url": str(payload.get("url") or ""),
                "image": str(payload.get("image") or ""),
                "category": str(payload.get("category") or ""),
                "tags_json": json.dumps(tags, ensure_ascii=False, default=str),
                "updated_at": captured_at,
                "ck_insert_time": now,
            }

            market_snapshot_rows.append(
                {
                    "source": source,
                    "exchange": exchange,
                    "market_id": market_id,
                    "captured_at": captured_at,
                    "resolution_date": str(payload.get("resolution_date") or ""),
                    "volume24h": cls._as_float(
                        cls._first_present(payload, ("volume24h", "volume_24h"), 0.0)
                    ),
                    "volume": cls._as_float(payload.get("volume")),
                    "liquidity": cls._as_float(payload.get("liquidity")),
                    "open_interest": cls._as_float(payload.get("open_interest")),
                    "fetch_params_json": json.dumps(
                        message.get("fetch_params") or {},
                        ensure_ascii=False,
                        default=str,
                    ),
                    "ck_insert_time": now,
                }
            )

            for outcome in cls._parse_outcomes(payload):
                outcome_id = outcome.get("outcome_id") or outcome.get("outcomeId")
                if outcome_id is None:
                    continue
                outcome_snapshot_rows.append(
                    {
                        "source": source,
                        "exchange": exchange,
                        "market_id": market_id,
                        "outcome_id": str(outcome_id),
                        "label": str(outcome.get("label") or ""),
                        "captured_at": captured_at,
                        "price": cls._as_float(outcome.get("price")),
                        "price_change24h": cls._as_float(
                            cls._first_present(
                                outcome,
                                ("price_change24h", "price_change_24h"),
                                0.0,
                            )
                        ),
                        "metadata_json": json.dumps(
                            outcome.get("metadata") or {},
                            ensure_ascii=False,
                            default=str,
                        ),
                        "ck_insert_time": now,
                    }
                )

        if not market_snapshot_rows:
            logger.info("No valid rows parsed from Kafka topic '%s'", topic_name)
            return 0

        clickhouse = ClickHouseClient()
        clickhouse.insert_rows(
            table="polymarket_market_dim",
            rows=list(market_dim_rows.values()),
            column_names=CLICKHOUSE_TABLE_COLS_ENUM.POLYMARKET_MARKET_DIM.value,
        )
        clickhouse.insert_rows(
            table="polymarket_market_snapshot",
            rows=market_snapshot_rows,
            column_names=CLICKHOUSE_TABLE_COLS_ENUM.POLYMARKET_MARKET_SNAPSHOT.value,
        )
        clickhouse.insert_rows(
            table="polymarket_outcome_snapshot",
            rows=outcome_snapshot_rows,
            column_names=CLICKHOUSE_TABLE_COLS_ENUM.POLYMARKET_OUTCOME_SNAPSHOT.value,
        )

        logger.info(
            "Consumed and inserted Kafka messages: markets=%s, snapshots=%s, outcomes=%s",
            len(market_dim_rows),
            len(market_snapshot_rows),
            len(outcome_snapshot_rows),
        )

        return len(market_snapshot_rows)


consume_service = ConsumeService()
