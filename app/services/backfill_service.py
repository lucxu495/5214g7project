"""
Project: QF5214 Polymarket Data Pipeline
File: backfill_service.py
Author: Xu
Created: 2026-03-21

Description:
Backfill service for ingesting Polymarket orderbook parquet data into ClickHouse.
"""

from __future__ import annotations
import re
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from app.clients.backfill_client import BackfillClient
from app.clients.clickhouse_client import ClickHouseClient
from app.clients.kafka_client import KafkaProducerClient, KafkaConsumerClient
from app.logging.logger import setup_logger
from app.schemas.clickhouse_tables import CLICKHOUSE_TABLE_COLS_ENUM
from app.utils.common_utils import get_task_config, update_task_config, get_target_outcome_ids
from app.config.settings import KAFKA_TOPIC_1, KAFKA_TOPIC_2
from time import perf_counter
from app.utils.time_utils import get_utc_now


logger = setup_logger(__name__)


class BackfillService:
    """
    Service for backfilling Polymarket orderbook parquet data into ClickHouse.
    """

    TABLE_NAME = "polymarket_orderbook_raw"
    DEFAULT_SOURCE = "polymarket"
    DEFAULT_EXCHANGE = "polymarket"

    @classmethod
    def _extract_file_hour_from_url(cls, url: str) -> datetime:
        """
        Extract parquet file hour from URL as UTC timezone-aware datetime.

        Example:
            https://r2.pmxt.dev/polymarket_orderbook_2026-03-19T23.parquet
            -> 2026-03-19 23:00:00+00:00
        """
        match = re.search(
            r"polymarket_orderbook_(\d{4}-\d{2}-\d{2}T\d{2})\.parquet",
            url,
        )
        if not match:
            raise ValueError(f"Cannot extract file hour from url: {url}")

        return datetime.strptime(
            match.group(1),
            "%Y-%m-%dT%H",
        ).replace(tzinfo=timezone.utc)

    @classmethod
    def ingest_backfill_price_change_data(
        cls, start_time: datetime = None, end_time: datetime = None,
        max_urls_per_run: int = None, is_manual: bool = False
    ) -> int:
        """
        Ingest configured backfill parquet data into Kafka.

        Returns:
            Total output row count.
        """
        logger.info("Starting backfill ingestion")
        if not start_time:
            start_time = datetime(2026, 3, 21, 4, 0, 0, tzinfo=ZoneInfo("Asia/Singapore"))

        if not end_time:
            end_time = datetime(2026, 3, 21, 9, 0, 0, tzinfo=ZoneInfo("Asia/Singapore"))

        if not max_urls_per_run:
            max_urls_per_run = 5

        if is_manual:
            update_task_config(
                "backfill_orderbook",
                {
                    "is_open": True,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "chunk_minutes": 30,
                    "max_urls_per_run": max_urls_per_run,
                },
            )

        task_config = get_task_config(
            "backfill_orderbook",
            {
                "is_open": True,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "chunk_minutes": 30,
                "max_urls_per_run": max_urls_per_run,
            },
        )
        logger.info("Backfill task config: %s", task_config)

        if not task_config["is_open"]:
            logger.info("Backfill task is not open, skipping")
            return 0

        target_outcome_ids = get_target_outcome_ids()
        logger.info("Loaded target outcome IDs count=%s", len(target_outcome_ids))

        target_outcome_ids = sorted(set(target_outcome_ids))
        logger.info("Deduplicated target outcome IDs count=%s", len(target_outcome_ids))

        if not target_outcome_ids:
            extra_where_sql = "1 = 0"
            logger.info("No target outcome IDs found, extra_where_sql=1=0")
        else:
            ids_str = ",".join([f"'{x}'" for x in target_outcome_ids])
            extra_where_sql = f"""
            update_type = 'price_change'
            AND json_extract_string(data, '$.token_id') IN ({ids_str})
            """
            logger.info(
                "Prepared extra_where_sql for target outcome IDs count=%s",
                len(target_outcome_ids),
            )

        task_started_at = perf_counter()

        start_time = datetime.fromisoformat(task_config["start_time"])
        end_time = datetime.fromisoformat(task_config["end_time"])
        chunk_minutes = int(task_config.get("chunk_minutes", 5))
        max_urls_per_run = int(task_config.get("max_urls_per_run", 5))

        if start_time.tzinfo is None or end_time.tzinfo is None:
            raise ValueError("start_time and end_time must be timezone-aware")

        if end_time <= start_time:
            logger.warning(
                "Invalid backfill window start_time=%s end_time=%s",
                start_time,
                end_time,
            )
            return 0

        backfill_client = BackfillClient()

        urls = backfill_client.build_orderbook_urls(
            start_time=start_time,
            end_time=end_time,
        )

        if max_urls_per_run > 0:
            urls = urls[:max_urls_per_run]

        logger.info("Backfill URLs to process count=%s", len(urls))

        total_output_rows = 0
        total_chunk_count = 0

        start_time_utc = start_time.astimezone(timezone.utc)
        end_time_utc = end_time.astimezone(timezone.utc)

        with KafkaProducerClient() as kafka_client:
            for url_index, url in enumerate(urls):
                url_started_at = perf_counter()
                url_output_rows = 0
                url_chunk_count = 0

                file_hour_utc = cls._extract_file_hour_from_url(url)
                file_start_time_utc = file_hour_utc
                file_end_time_utc = file_hour_utc + timedelta(hours=1)

                window_start_time = max(start_time_utc, file_start_time_utc)
                window_end_time = min(end_time_utc, file_end_time_utc)

                logger.info(
                    "Processing backfill url_index=%s url=%s "
                    "file_start_time_utc=%s file_end_time_utc=%s "
                    "window_start_time_utc=%s window_end_time_utc=%s",
                    url_index,
                    url,
                    file_start_time_utc,
                    file_end_time_utc,
                    window_start_time,
                    window_end_time,
                )

                if window_end_time <= window_start_time:
                    logger.info("Skipping empty backfill window for url=%s", url)
                    continue

                for chunk_index, df in enumerate(
                    backfill_client.read_orderbook_parquet_by_time_chunks(
                        url=url,
                        start_time=window_start_time,
                        end_time=window_end_time,
                        chunk_minutes=chunk_minutes,
                        select_sqls=[
                            "json_extract_string(data, '$.update_type') AS update_type",
                            "json_extract_string(data, '$.market_id') AS market_id",
                            "json_extract_string(data, '$.token_id') AS token_id",
                            "json_extract_string(data, '$.side') AS side",
                            "json_extract_string(data, '$.best_bid') AS best_bid",
                            "json_extract_string(data, '$.best_ask') AS best_ask",
                            "CAST(json_extract(data, '$.timestamp') AS DOUBLE) AS timestamp",
                            "json_extract_string(data, '$.change_price') AS change_price",
                            "json_extract_string(data, '$.change_size') AS change_size",
                            "json_extract_string(data, '$.change_side') AS change_side",
                        ],
                        extra_where_sql=extra_where_sql,
                        time_column="timestamp_received",
                    )
                ):
                    if df.empty:
                        logger.warning("Backfill chunk empty url=%s chunk_index=%s", url, chunk_index)
                        continue

                    process_started_at = perf_counter()
                    raw_row_count = len(df)

                    df = cls._clean_df(df)
                    cleaned_row_count = len(df)

                    df = cls._filter_df(df)
                    filtered_row_count = len(df)

                    if df.empty:
                        process_duration_minutes = (perf_counter() - process_started_at) / 60

                        logger.info(
                            "Backfill chunk done url=%s chunk_index=%s "
                            "raw=%s cleaned=%s filtered=%s output=0 "
                            "clean_rate=%.4f filter_rate=%.4f process_min=%.4f",
                            url,
                            chunk_index,
                            raw_row_count,
                            cleaned_row_count,
                            filtered_row_count,
                            (cleaned_row_count / raw_row_count) if raw_row_count > 0 else 0.0,
                            (filtered_row_count / cleaned_row_count) if cleaned_row_count > 0 else 0.0,
                            process_duration_minutes,
                        )
                        continue

                    kafka_values = df.to_dict(orient="records")
                    output_row_count = len(kafka_values)

                    if not kafka_values:
                        process_duration_minutes = (perf_counter() - process_started_at) / 60

                        logger.info(
                            "Backfill chunk done url=%s chunk_index=%s "
                            "raw=%s cleaned=%s filtered=%s output=0 "
                            "clean_rate=%.4f filter_rate=%.4f process_min=%.4f",
                            url,
                            chunk_index,
                            raw_row_count,
                            cleaned_row_count,
                            filtered_row_count,
                            (cleaned_row_count / raw_row_count) if raw_row_count > 0 else 0.0,
                            (filtered_row_count / cleaned_row_count) if cleaned_row_count > 0 else 0.0,
                            process_duration_minutes,
                        )
                        continue

                    send_started_at = perf_counter()

                    kafka_client.send_batch(
                        topic=KAFKA_TOPIC_1,
                        values=kafka_values,
                        key_field="market_id",
                    )
                    kafka_client.flush()

                    send_duration_minutes = (perf_counter() - send_started_at) / 60
                    process_duration_minutes = (perf_counter() - process_started_at) / 60

                    total_output_rows += output_row_count
                    total_chunk_count += 1
                    url_output_rows += output_row_count
                    url_chunk_count += 1

                    logger.info(
                        "Backfill chunk done url=%s chunk_index=%s "
                        "raw=%s cleaned=%s filtered=%s output=%s "
                        "clean_rate=%.4f filter_rate=%.4f output_rate=%.4f "
                        "process_min=%.4f send_min=%.4f output_rpm=%.2f total_output=%s",
                        url,
                        chunk_index,
                        raw_row_count,
                        cleaned_row_count,
                        filtered_row_count,
                        output_row_count,
                        (cleaned_row_count / raw_row_count) if raw_row_count > 0 else 0.0,
                        (filtered_row_count / cleaned_row_count) if cleaned_row_count > 0 else 0.0,
                        (output_row_count / raw_row_count) if raw_row_count > 0 else 0.0,
                        process_duration_minutes,
                        send_duration_minutes,
                        (output_row_count / process_duration_minutes) if process_duration_minutes > 0 else 0.0,
                        total_output_rows,
                    )

                url_duration_minutes = (perf_counter() - url_started_at) / 60

                logger.info(
                    "Backfill url done url_index=%s url=%s chunk_count=%s output=%s "
                    "duration_min=%.4f avg_output_per_chunk=%.2f",
                    url_index,
                    url,
                    url_chunk_count,
                    url_output_rows,
                    url_duration_minutes,
                    (url_output_rows / url_chunk_count) if url_chunk_count > 0 else 0.0,
                )

        task_duration_minutes = (perf_counter() - task_started_at) / 60

        logger.info(
            "Backfill ingestion finished total_output=%s total_chunks=%s "
            "duration_min=%.4f avg_output_per_chunk=%.2f",
            total_output_rows,
            total_chunk_count,
            task_duration_minutes,
            (total_output_rows / total_chunk_count) if total_chunk_count > 0 else 0.0,
        )

        if not is_manual:
            new_end_time = start_time
            new_start_time = new_end_time - timedelta(hours=5)
            update_task_config(
                "backfill_orderbook",
                {
                    "is_open": True,
                    "start_time": new_start_time.isoformat(),
                    "end_time": new_end_time.isoformat(),
                    "chunk_minutes": 30,
                    "max_urls_per_run": 5,
                },
            )

        return total_output_rows

    @staticmethod
    def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Keep 1 row per second per market_id based on data.timestamp.
        """

        if df.empty:
            return df

        # 1. 转 timestamp（float → datetime）
        df["event_time"] = pd.to_datetime(
            df["timestamp"],
            unit="s",
            utc=True,
        )

        # 2. 秒级 bucket
        df["ts_sec"] = df["event_time"].dt.floor("s")

        # 3. 排序（保留最新）
        df = df.sort_values(
            by="event_time",
            ascending=False,
        )

        # 4. 每个 token_id 每秒保留一条
        df = df.drop_duplicates(
            subset=["token_id", "ts_sec"],
            keep="first",
        )

        # 5. 清理
        df = df.drop(columns=["ts_sec"])

        return df

    @staticmethod
    def _filter_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter invalid / unwanted rows before sending to Kafka.

        Rules:
        - drop empty market_id
        - drop missing timestamp
        - keep only selected update_type
        - optional: filter invalid prices
        """

        if df.empty:
            return df

        original_count = len(df)

        # 1. 必须字段
        df = df[
            df["market_id"].notna() &
            df["timestamp"].notna()
        ]

        # 2. update_type 过滤（核心）
        df = df[
            df["update_type"].isin([
                "price_change",
                "book_snapshot",
            ])
        ]

        # 3. optional：价格过滤（建议打开）
        if "best_bid" in df.columns:
            df = df[df["best_bid"].notna()]

        if "best_ask" in df.columns:
            df = df[df["best_ask"].notna()]

        # 转 float（避免字符串比较问题）
        for col in ["best_bid", "best_ask"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 去掉无效价格
        if "best_bid" in df.columns:
            df = df[df["best_bid"] > 0]

        if "best_ask" in df.columns:
            df = df[df["best_ask"] > 0]

        logger.info(
            "Filter df rows: before=%s after=%s dropped=%s",
            original_count,
            len(df),
            original_count - len(df),
        )

        df = df[[
            "update_type",
            "token_id",
            "side",
            "best_bid",
            "best_ask",
            "timestamp",
            "change_price",
            "change_size",
            "change_side",
        ]]

        return df

    @staticmethod
    def _split_df_by_update_type(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """
        Split dataframe by update_type.
        """
        if df.empty or "update_type" not in df.columns:
            return {}

        result: dict[str, pd.DataFrame] = {}

        for update_type, group_df in df.groupby("update_type"):
            if pd.isna(update_type):
                continue
            result[str(update_type)] = group_df.reset_index(drop=True)

        return result

    @staticmethod
    def _transform_price_change_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform price_change messages from data JSON into structured dataframe.
        """
        if df.empty:
            return df

        parsed = df["data"].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )
        out = pd.json_normalize(parsed)

        expected_cols = [
            "update_type",
            "market_id",
            "token_id",
            "side",
            "best_bid",
            "best_ask",
            "timestamp",
            "change_price",
            "change_size",
            "change_side",
        ]

        for col in expected_cols:
            if col not in out.columns:
                out[col] = None

        return out[expected_cols]

    @staticmethod
    def _transform_book_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform book_snapshot messages from data JSON into structured dataframe.
        """
        if df.empty:
            return df

        parsed = df["data"].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )
        out = pd.json_normalize(parsed)

        return out

    @classmethod
    def consume_backfill_price_change_data(cls) -> int:
        """
        Consume backfill price change data from Kafka and insert into ClickHouse.

        Returns:
            Inserted row count.
        """
        logger.info("Starting backfill price change consumption")

        task_config = get_task_config(
            "consume_backfill_price_change_data",
            {
                "is_open": True,
                "max_messages_per_partition": 5000,
                "max_workers": 4,
            },
        )
        logger.info("Backfill consume task config: %s", task_config)

        if not task_config["is_open"]:
            logger.info("Backfill consume task is not open, skipping")
            return 0

        max_messages_per_partition = int(
            task_config.get("max_messages_per_partition", 5000)
        )
        max_workers = int(task_config.get("max_workers", 4))

        task_started_at = perf_counter()
        total_inserted_rows = 0
        total_partitions = 0

        clickhouse_client = ClickHouseClient()

        with KafkaConsumerClient(
            topic=KAFKA_TOPIC_1,
            group_id="polymarket_backfill_price_change_consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
        ) as kafka_consumer:
            partition_messages = kafka_consumer.consume(
                max_messages_per_partition=max_messages_per_partition,
                max_workers=max_workers,
            )

        if not partition_messages:
            logger.info("No backfill Kafka messages consumed")
            return 0

        for partition_id, messages in partition_messages.items():
            partition_started_at = perf_counter()
            total_partitions += 1

            raw_message_count = len(messages)

            logger.info(
                "Processing consumed partition partition_id=%s raw_message_count=%s",
                partition_id,
                raw_message_count,
            )

            if not messages:
                logger.info(
                    "Skipping empty partition partition_id=%s",
                    partition_id,
                )
                continue

            rows = []

            for message_index, value in enumerate(messages):
                try:
                    rows.append(
                        {
                            "update_type": str(value.get("update_type", "")),
                            "market_id": str(value.get("market_id", "")),
                            "token_id": str(value.get("token_id", "")),
                            "side": str(value.get("side", "")),
                            "best_bid": float(value.get("best_bid") or 0),
                            "best_ask": float(value.get("best_ask") or 0),
                            "event_timestamp": datetime.fromtimestamp(
                                float(value.get("timestamp") or 0),
                                tz=timezone.utc,
                            ),
                            "change_price": float(value.get("change_price") or 0),
                            "change_size": float(value.get("change_size") or 0),
                            "change_side": str(value.get("change_side", "")),
                            "ck_insert_time": get_utc_now(),
                        }
                    )
                except Exception as e:
                    logger.error(
                        "Failed to parse backfill Kafka message partition_id=%s message_index=%s error=%s value=%s",
                        partition_id,
                        message_index,
                        str(e),
                        value,
                    )

            parsed_row_count = len(rows)

            if not rows:
                partition_duration_minutes = (perf_counter() - partition_started_at) / 60

                logger.info(
                    "Finished partition partition_id=%s raw_message_count=%s parsed_row_count=0 inserted_row_count=0 duration_min=%.4f",
                    partition_id,
                    raw_message_count,
                    partition_duration_minutes,
                )
                continue
            insert_started_at = perf_counter()

            clickhouse_client.insert_rows(
                table="polymarket_backfill_price_change",
                rows=rows,
                column_names=[
                    "update_type",
                    "market_id",
                    "token_id",
                    "side",
                    "best_bid",
                    "best_ask",
                    "event_timestamp",
                    "change_price",
                    "change_size",
                    "change_side",
                    "ck_insert_time",
                ],
            )

            insert_duration_minutes = (perf_counter() - insert_started_at) / 60
            partition_duration_minutes = (perf_counter() - partition_started_at) / 60

            inserted_row_count = len(rows)
            total_inserted_rows += inserted_row_count

            logger.info(
                "Finished partition partition_id=%s raw_message_count=%s parsed_row_count=%s inserted_row_count=%s "
                "parse_success_rate=%.4f insert_min=%.4f duration_min=%.4f inserted_rpm=%.2f total_inserted_rows=%s",
                partition_id,
                raw_message_count,
                parsed_row_count,
                inserted_row_count,
                (parsed_row_count / raw_message_count) if raw_message_count > 0 else 0.0,
                insert_duration_minutes,
                partition_duration_minutes,
                (inserted_row_count / partition_duration_minutes)
                if partition_duration_minutes > 0 else 0.0,
                total_inserted_rows,
            )

        task_duration_minutes = (perf_counter() - task_started_at) / 60

        logger.info(
            "Finished backfill price change consumption total_partitions=%s total_inserted_rows=%s "
            "duration_min=%.4f avg_rows_per_partition=%.2f",
            total_partitions,
            total_inserted_rows,
            task_duration_minutes,
            (total_inserted_rows / total_partitions) if total_partitions > 0 else 0.0,
        )

        return total_inserted_rows

    @classmethod
    def ingest_backfill_book_snapshot_data(
        cls,
        start_time: datetime = None,
        end_time: datetime = None,
        max_urls_per_run: int = None,
        is_manual: bool = False,
    ) -> int:
        """
        Ingest configured backfill book snapshot parquet data into Kafka.

        Returns:
            Total output row count.
        """
        logger.info("Starting backfill book_snapshot ingestion")

        if not start_time:
            start_time = datetime(
                2026,
                3,
                21,
                4,
                0,
                0,
                tzinfo=ZoneInfo("Asia/Singapore"),
            )

        if not end_time:
            end_time = datetime(
                2026,
                3,
                21,
                9,
                0,
                0,
                tzinfo=ZoneInfo("Asia/Singapore"),
            )

        if not max_urls_per_run:
            max_urls_per_run = 5

        if is_manual:
            update_task_config(
                "backfill_orderbook_book_snapshot",
                {
                    "is_open": True,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "chunk_minutes": 30,
                    "max_urls_per_run": max_urls_per_run,
                },
            )

        task_config = get_task_config(
            "backfill_orderbook_book_snapshot",
            {
                "is_open": True,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "chunk_minutes": 30,
                "max_urls_per_run": max_urls_per_run,
            },
        )
        logger.info("Backfill book_snapshot task config: %s", task_config)

        if not task_config["is_open"]:
            logger.info("Backfill book_snapshot task is not open, skipping")
            return 0

        target_outcome_ids = get_target_outcome_ids()
        logger.info("Loaded target outcome IDs count=%s", len(target_outcome_ids))

        target_outcome_ids = sorted(set(target_outcome_ids))
        logger.info("Deduplicated target outcome IDs count=%s", len(target_outcome_ids))

        if not target_outcome_ids:
            extra_where_sql = "1 = 0"
            logger.info("No target outcome IDs found, extra_where_sql=1=0")
        else:
            ids_str = ",".join([f"'{x}'" for x in target_outcome_ids])
            extra_where_sql = f"""
            update_type = 'book_snapshot'
            AND json_extract_string(data, '$.token_id') IN ({ids_str})
            """
            logger.info(
                "Prepared extra_where_sql for target outcome IDs count=%s",
                len(target_outcome_ids),
            )

        task_started_at = perf_counter()

        start_time = datetime.fromisoformat(task_config["start_time"])
        end_time = datetime.fromisoformat(task_config["end_time"])
        chunk_minutes = int(task_config.get("chunk_minutes", 5))
        max_urls_per_run = int(task_config.get("max_urls_per_run", 5))

        if start_time.tzinfo is None or end_time.tzinfo is None:
            raise ValueError("start_time and end_time must be timezone-aware")

        if end_time <= start_time:
            logger.warning(
                "Invalid backfill window start_time=%s end_time=%s",
                start_time,
                end_time,
            )
            return 0

        backfill_client = BackfillClient()

        urls = backfill_client.build_orderbook_urls(
            start_time=start_time,
            end_time=end_time,
        )

        if max_urls_per_run > 0:
            urls = urls[:max_urls_per_run]

        logger.info("Backfill book_snapshot URLs to process count=%s", len(urls))

        total_output_rows = 0
        total_chunk_count = 0

        start_time_utc = start_time.astimezone(timezone.utc)
        end_time_utc = end_time.astimezone(timezone.utc)

        with KafkaProducerClient() as kafka_client:
            for url_index, url in enumerate(urls):
                url_started_at = perf_counter()
                url_output_rows = 0
                url_chunk_count = 0

                file_hour_utc = cls._extract_file_hour_from_url(url)
                file_start_time_utc = file_hour_utc
                file_end_time_utc = file_hour_utc + timedelta(hours=1)

                window_start_time = max(start_time_utc, file_start_time_utc)
                window_end_time = min(end_time_utc, file_end_time_utc)

                logger.info(
                    "Processing backfill book_snapshot url_index=%s url=%s "
                    "file_start_time_utc=%s file_end_time_utc=%s "
                    "window_start_time_utc=%s window_end_time_utc=%s",
                    url_index,
                    url,
                    file_start_time_utc,
                    file_end_time_utc,
                    window_start_time,
                    window_end_time,
                )

                if window_end_time <= window_start_time:
                    logger.info("Skipping empty backfill window for url=%s", url)
                    continue

                for chunk_index, df in enumerate(
                    backfill_client.read_orderbook_parquet_by_time_chunks(
                        url=url,
                        start_time=window_start_time,
                        end_time=window_end_time,
                        chunk_minutes=chunk_minutes,
                        select_sqls=[
                            "json_extract_string(data, '$.update_type') AS update_type",
                            "json_extract_string(data, '$.market_id') AS market_id",
                            "json_extract_string(data, '$.token_id') AS token_id",
                            "json_extract_string(data, '$.side') AS side",
                            "CAST(json_extract_string(data, '$.best_bid') AS DOUBLE) AS best_bid",
                            "CAST(json_extract_string(data, '$.best_ask') AS DOUBLE) AS best_ask",
                            "CAST(json_extract(data, '$.timestamp') AS DOUBLE) AS timestamp",
                            "CAST(json_extract(data, '$.bids') AS VARCHAR) AS bids",
                            "CAST(json_extract(data, '$.asks') AS VARCHAR) AS asks",
                        ],
                        extra_where_sql=extra_where_sql,
                        time_column="timestamp_received",
                    )
                ):
                    process_started_at = perf_counter()
                    raw_row_count = len(df)

                    df = cls._clean_book_snapshot_df(df)
                    cleaned_row_count = len(df)

                    df = cls._filter_book_snapshot_df(df)
                    filtered_row_count = len(df)

                    if df.empty:
                        process_duration_minutes = (perf_counter() - process_started_at) / 60

                        logger.info(
                            "Backfill book_snapshot chunk done url=%s chunk_index=%s "
                            "raw=%s cleaned=%s filtered=%s output=0 "
                            "clean_rate=%.4f filter_rate=%.4f process_min=%.4f",
                            url,
                            chunk_index,
                            raw_row_count,
                            cleaned_row_count,
                            filtered_row_count,
                            (cleaned_row_count / raw_row_count) if raw_row_count > 0 else 0.0,
                            (filtered_row_count / cleaned_row_count) if cleaned_row_count > 0 else 0.0,
                            process_duration_minutes,
                        )
                        continue

                    kafka_values = df.to_dict(orient="records")
                    output_row_count = len(kafka_values)

                    if not kafka_values:
                        process_duration_minutes = (perf_counter() - process_started_at) / 60

                        logger.info(
                            "Backfill book_snapshot chunk done url=%s chunk_index=%s "
                            "raw=%s cleaned=%s filtered=%s output=0 "
                            "clean_rate=%.4f filter_rate=%.4f process_min=%.4f",
                            url,
                            chunk_index,
                            raw_row_count,
                            cleaned_row_count,
                            filtered_row_count,
                            (cleaned_row_count / raw_row_count) if raw_row_count > 0 else 0.0,
                            (filtered_row_count / cleaned_row_count) if cleaned_row_count > 0 else 0.0,
                            process_duration_minutes,
                        )
                        continue

                    send_started_at = perf_counter()

                    kafka_client.send_batch(
                        topic=KAFKA_TOPIC_2,
                        values=kafka_values,
                        key_field="market_id",
                    )
                    kafka_client.flush()

                    send_duration_minutes = (perf_counter() - send_started_at) / 60
                    process_duration_minutes = (perf_counter() - process_started_at) / 60

                    total_output_rows += output_row_count
                    total_chunk_count += 1
                    url_output_rows += output_row_count
                    url_chunk_count += 1

                    logger.info(
                        "Backfill book_snapshot chunk done url=%s chunk_index=%s "
                        "raw=%s cleaned=%s filtered=%s output=%s "
                        "clean_rate=%.4f filter_rate=%.4f output_rate=%.4f "
                        "process_min=%.4f send_min=%.4f output_rpm=%.2f total_output=%s",
                        url,
                        chunk_index,
                        raw_row_count,
                        cleaned_row_count,
                        filtered_row_count,
                        output_row_count,
                        (cleaned_row_count / raw_row_count) if raw_row_count > 0 else 0.0,
                        (filtered_row_count / cleaned_row_count) if cleaned_row_count > 0 else 0.0,
                        (output_row_count / raw_row_count) if raw_row_count > 0 else 0.0,
                        process_duration_minutes,
                        send_duration_minutes,
                        (output_row_count / process_duration_minutes) if process_duration_minutes > 0 else 0.0,
                        total_output_rows,
                    )

                url_duration_minutes = (perf_counter() - url_started_at) / 60

                logger.info(
                    "Backfill book_snapshot url done url_index=%s url=%s chunk_count=%s output=%s "
                    "duration_min=%.4f avg_output_per_chunk=%.2f",
                    url_index,
                    url,
                    url_chunk_count,
                    url_output_rows,
                    url_duration_minutes,
                    (url_output_rows / url_chunk_count) if url_chunk_count > 0 else 0.0,
                )

        task_duration_minutes = (perf_counter() - task_started_at) / 60

        logger.info(
            "Backfill book_snapshot ingestion finished total_output=%s total_chunks=%s "
            "duration_min=%.4f avg_output_per_chunk=%.2f",
            total_output_rows,
            total_chunk_count,
            task_duration_minutes,
            (total_output_rows / total_chunk_count) if total_chunk_count > 0 else 0.0,
        )

        if not is_manual:
            new_end_time = start_time
            new_start_time = new_end_time - timedelta(hours=5)
            update_task_config(
                "backfill_orderbook_book_snapshot",
                {
                    "is_open": True,
                    "start_time": new_start_time.isoformat(),
                    "end_time": new_end_time.isoformat(),
                    "chunk_minutes": 30,
                    "max_urls_per_run": 5,
                },
            )

        return total_output_rows

    @classmethod
    def consume_backfill_book_snapshot_data(cls) -> int:
        """
        Consume backfill book snapshot data from Kafka and insert into ClickHouse.

        Returns:
            Inserted row count.
        """
        logger.info("Starting backfill book snapshot consumption")

        task_config = get_task_config(
            "consume_backfill_book_snapshot_data",
            {
                "is_open": True,
                "max_messages_per_partition": 5000,
                "max_workers": 4,
            },
        )
        logger.info("Backfill book snapshot consume task config: %s", task_config)

        if not task_config["is_open"]:
            logger.info("Backfill book snapshot consume task is not open, skipping")
            return 0

        max_messages_per_partition = int(
            task_config.get("max_messages_per_partition", 5000)
        )
        max_workers = int(task_config.get("max_workers", 4))

        task_started_at = perf_counter()
        total_inserted_rows = 0
        total_partitions = 0

        clickhouse_client = ClickHouseClient()

        with KafkaConsumerClient(
            topic=KAFKA_TOPIC_2,
            group_id="polymarket_backfill_book_snapshot_consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
        ) as kafka_consumer:
            partition_messages = kafka_consumer.consume(
                max_messages_per_partition=max_messages_per_partition,
                max_workers=max_workers,
            )

        if not partition_messages:
            logger.info("No backfill book snapshot Kafka messages consumed")
            return 0

        for partition_id, messages in partition_messages.items():
            partition_started_at = perf_counter()
            total_partitions += 1

            raw_message_count = len(messages)

            logger.info(
                "Processing consumed book snapshot partition partition_id=%s raw_message_count=%s",
                partition_id,
                raw_message_count,
            )

            if not messages:
                logger.info(
                    "Skipping empty partition partition_id=%s",
                    partition_id,
                )
                continue

            rows = []

            for message_index, value in enumerate(messages):
                try:
                    rows.append(
                        {
                            "update_type": str(value.get("update_type", "")),
                            "market_id": str(value.get("market_id", "")),
                            "token_id": str(value.get("token_id", "")),
                            "side": str(value.get("side", "")),
                            "best_bid": float(value.get("best_bid") or 0),
                            "best_ask": float(value.get("best_ask") or 0),
                            "event_timestamp": datetime.fromtimestamp(
                                float(value.get("timestamp") or 0),
                                tz=timezone.utc,
                            ),
                            "bids": str(value.get("bids") or "[]"),
                            "asks": str(value.get("asks") or "[]"),
                            "ck_insert_time": get_utc_now(),
                        }
                    )
                except Exception as e:
                    logger.error(
                        "Failed to parse backfill book snapshot Kafka message partition_id=%s message_index=%s error=%s value=%s",
                        partition_id,
                        message_index,
                        str(e),
                        value,
                    )

            parsed_row_count = len(rows)

            if not rows:
                partition_duration_minutes = (perf_counter() - partition_started_at) / 60

                logger.info(
                    "Finished book snapshot partition partition_id=%s raw_message_count=%s parsed_row_count=0 inserted_row_count=0 duration_min=%.4f",
                    partition_id,
                    raw_message_count,
                    partition_duration_minutes,
                )
                continue

            insert_started_at = perf_counter()

            clickhouse_client.insert_rows(
                table="polymarket_backfill_book_snapshot",
                rows=rows,
                column_names=[
                    "update_type",
                    "market_id",
                    "token_id",
                    "side",
                    "best_bid",
                    "best_ask",
                    "event_timestamp",
                    "bids",
                    "asks",
                    "ck_insert_time",
                ],
            )

            insert_duration_minutes = (perf_counter() - insert_started_at) / 60
            partition_duration_minutes = (perf_counter() - partition_started_at) / 60

            inserted_row_count = len(rows)
            total_inserted_rows += inserted_row_count

            logger.info(
                "Finished book snapshot partition partition_id=%s raw_message_count=%s parsed_row_count=%s inserted_row_count=%s "
                "parse_success_rate=%.4f insert_min=%.4f duration_min=%.4f inserted_rpm=%.2f total_inserted_rows=%s",
                partition_id,
                raw_message_count,
                parsed_row_count,
                inserted_row_count,
                (parsed_row_count / raw_message_count) if raw_message_count > 0 else 0.0,
                insert_duration_minutes,
                partition_duration_minutes,
                (inserted_row_count / partition_duration_minutes)
                if partition_duration_minutes > 0 else 0.0,
                total_inserted_rows,
            )

        task_duration_minutes = (perf_counter() - task_started_at) / 60

        logger.info(
            "Finished backfill book snapshot consumption total_partitions=%s total_inserted_rows=%s "
            "duration_min=%.4f avg_rows_per_partition=%.2f",
            total_partitions,
            total_inserted_rows,
            task_duration_minutes,
            (total_inserted_rows / total_partitions) if total_partitions > 0 else 0.0,
        )

        return total_inserted_rows

    @staticmethod
    def _clean_book_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Keep 1 row per second per token_id based on data.timestamp.
        """

        if df.empty:
            return df

        df = df.copy()

        # 1. timestamp -> datetime
        df["event_time"] = pd.to_datetime(
            df["timestamp"],
            unit="s",
            utc=True,
            errors="coerce",
        )

        # 2. 秒级 bucket
        df["ts_sec"] = df["event_time"].dt.floor("s")

        # 3. 排序（保留最新）
        df = df.sort_values(
            by="event_time",
            ascending=False,
        )

        # 4. 每个 token_id 每秒保留一条
        df = df.drop_duplicates(
            subset=["token_id", "ts_sec"],
            keep="first",
        )

        # 5. 清理辅助列
        df = df.drop(columns=["ts_sec"], errors="ignore")

        return df

    @staticmethod
    def _filter_book_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter invalid / unwanted book snapshot rows before sending to Kafka.

        Rules:
        - drop empty market_id / token_id
        - drop missing timestamp
        - keep only book_snapshot
        - filter invalid best_bid / best_ask
        """

        if df.empty:
            return df

        df = df.copy()
        original_count = len(df)

        # 1. 必须字段
        df = df[
            df["market_id"].notna() &
            df["token_id"].notna() &
            df["timestamp"].notna()
        ]

        # 2. update_type 过滤
        df = df[df["update_type"] == "book_snapshot"]

        # 3. 转数值，避免字符串比较问题
        for col in ["best_bid", "best_ask", "timestamp"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 4. 过滤无效价格
        if "best_bid" in df.columns:
            df = df[df["best_bid"].notna()]
            df = df[df["best_bid"] > 0]

        if "best_ask" in df.columns:
            df = df[df["best_ask"].notna()]
            df = df[df["best_ask"] > 0]

        # 5. bids / asks 缺失时补空数组字符串
        if "bids" in df.columns:
            df["bids"] = df["bids"].fillna("[]")

        if "asks" in df.columns:
            df["asks"] = df["asks"].fillna("[]")

        logger.info(
            "Filter book snapshot df rows: before=%s after=%s dropped=%s",
            original_count,
            len(df),
            original_count - len(df),
        )

        df = df[[
            "update_type",
            "market_id",
            "token_id",
            "side",
            "best_bid",
            "best_ask",
            "timestamp",
            "bids",
            "asks",
        ]]

        return df
