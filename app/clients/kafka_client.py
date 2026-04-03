"""
Project: QF5214 Polymarket Data Pipeline
File: kafka_client.py
Author: Xu
Created: 2026-03-14

Description:
Kafka producer and consumer clients using context manager.
"""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from app.config.settings import KAFKA_BOOTSTRAP_SERVERS
from app.logging.logger import setup_logger


logger = setup_logger(__name__)


def _parse_bootstrap_servers(bootstrap_servers: str) -> list[str]:
    """
    Parse bootstrap servers string into a list.

    Args:
        bootstrap_servers: Comma-separated bootstrap servers string.

    Returns:
        List of bootstrap server addresses.
    """
    return [
        server.strip()
        for server in bootstrap_servers.split(",")
        if server.strip()
    ]


class KafkaProducerClient:
    """
    Context-managed Kafka producer client.
    """

    def __init__(self) -> None:
        """
        Initialize Kafka producer client configuration.
        """
        self.bootstrap_servers = _parse_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        self.producer: KafkaProducer | None = None

        logger.info(
            "Initializing KafkaProducerClient (bootstrap_servers=%s)",
            self.bootstrap_servers,
        )

    def __enter__(self) -> "KafkaProducerClient":
        """
        Enter runtime context and create Kafka producer.

        Returns:
            KafkaProducerClient instance.
        """
        logger.info("Opening Kafka producer connection")

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(
                value,
                ensure_ascii=False,
                default=str,
            ).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8") if key else None,
            linger_ms=5,
            batch_size=16384,
        )

        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """
        Exit runtime context and close Kafka producer.

        Args:
            exc_type: Exception type if an exception occurred.
            exc: Exception instance if an exception occurred.
            tb: Traceback if an exception occurred.
        """
        if self.producer:
            logger.info("Closing Kafka producer connection")
            self.producer.flush()
            self.producer.close()

    def send(
        self,
        topic: str,
        value: dict[str, Any],
        key: str | None = None,
    ) -> None:
        """
        Send a single message to Kafka.

        Args:
            topic: Kafka topic name.
            value: Message payload.
            key: Optional Kafka message key.
        """
        if not self.producer:
            raise RuntimeError("Producer not initialized. Use context manager.")

        logger.debug("Sending message to topic=%s key=%s", topic, key)

        self.producer.send(
            topic=topic,
            value=value,
            key=key,
        )

    def send_batch(
        self,
        topic: str,
        values: list[dict[str, Any]],
        key_field: str | None = None,
    ) -> None:
        """
        Send a batch of messages to Kafka.

        Args:
            topic: Kafka topic name.
            values: List of message payloads.
            key_field: Optional field name from each payload used as Kafka key.
        """
        if not self.producer:
            raise RuntimeError("Producer not initialized. Use context manager.")

        logger.info(
            "Sending batch of %s messages to topic '%s'",
            len(values),
            topic,
        )

        for value in values:
            key = None
            if key_field and key_field in value and value[key_field] is not None:
                key = str(value[key_field])

            self.producer.send(
                topic=topic,
                value=value,
                key=key,
            )

        logger.info(
            "Batch send completed (%s messages -> topic '%s')",
            len(values),
            topic,
        )

    def flush(self) -> None:
        """
        Flush Kafka producer buffer.
        """
        if not self.producer:
            raise RuntimeError("Producer not initialized. Use context manager.")

        logger.info("Flushing Kafka producer")
        self.producer.flush()


class KafkaConsumerClient:
    """
    Kafka consumer client for scheduled batch pulling.

    Features:
        1. No subscribe mode
        2. Partition-based multithread consumption
        3. Offset-aware bounded consumption
    """

    def __init__(
        self,
        topic: str,
        group_id: str,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
        consumer_timeout_ms: int = 3000,
    ) -> None:
        """
        Initialize Kafka consumer client.

        Args:
            topic: Kafka topic name.
            group_id: Consumer group ID.
            auto_offset_reset: Offset reset strategy when no committed offset exists.
            enable_auto_commit: Whether Kafka should auto-commit offsets.
            consumer_timeout_ms: Consumer timeout in milliseconds.
        """
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = _parse_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.consumer_timeout_ms = consumer_timeout_ms
        self.consumer: KafkaConsumer | None = None

        logger.info(
            "Initializing KafkaConsumerClient (topic=%s, group_id=%s)",
            self.topic,
            self.group_id,
        )

    def __enter__(self) -> "KafkaConsumerClient":
        """
        Enter runtime context and create Kafka consumer.

        Returns:
            KafkaConsumerClient instance.
        """
        logger.info(
            "Opening Kafka consumer connection (topic=%s, group_id=%s)",
            self.topic,
            self.group_id,
        )

        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=self.enable_auto_commit,
            consumer_timeout_ms=self.consumer_timeout_ms,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )

        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """
        Exit runtime context and close Kafka consumer.

        Args:
            exc_type: Exception type if an exception occurred.
            exc: Exception instance if an exception occurred.
            tb: Traceback if an exception occurred.
        """
        if self.consumer:
            logger.info(
                "Closing Kafka consumer (topic=%s, group_id=%s)",
                self.topic,
                self.group_id,
            )
            self.consumer.close()

    def consume(
        self,
        max_messages_per_partition: int = 100,
        max_workers: int | None = None,
    ) -> dict[int, list[dict[str, Any]]]:
        """
        Consume messages with:
            - explicit partition assignment
            - multithread processing by partition
            - bounded offset range per partition

        Args:
            max_messages_per_partition: Maximum number of messages to consume per partition.
            max_workers: Maximum number of worker threads. Defaults to partition count.

        Returns:
            Dict keyed by partition ID with consumed messages.
        """
        if not self.consumer:
            raise RuntimeError("Consumer not initialized. Use context manager.")

        partitions = self.consumer.partitions_for_topic(self.topic)

        if not partitions:
            logger.warning("No partitions found for topic '%s'", self.topic)
            return {}

        topic_partitions = [
            TopicPartition(self.topic, partition_id)
            for partition_id in sorted(list(partitions))
        ]

        beginning_offsets = self.consumer.beginning_offsets(topic_partitions)
        end_offsets = self.consumer.end_offsets(topic_partitions)
        committed_offsets = {
            topic_partition: self.consumer.committed(topic_partition)
            for topic_partition in topic_partitions
        }

        worker_count = max_workers or len(topic_partitions)

        logger.info(
            "Starting scheduled bounded consumption "
            "(topic=%s, partitions=%s, max_per_partition=%s)",
            self.topic,
            [topic_partition.partition for topic_partition in topic_partitions],
            max_messages_per_partition,
        )

        results: dict[int, list[dict[str, Any]]] = {}

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_partition = {
                executor.submit(
                    self._consume_partition_range,
                    topic_partition,
                    beginning_offsets[topic_partition],
                    end_offsets[topic_partition],
                    committed_offsets[topic_partition],
                    max_messages_per_partition,
                ): topic_partition.partition
                for topic_partition in topic_partitions
            }

            for future in as_completed(future_to_partition):
                partition_id = future_to_partition[future]
                results[partition_id] = future.result()

        total_messages = sum(len(messages) for messages in results.values())

        logger.info(
            "Scheduled bounded consumption completed "
            "(topic=%s, total_partitions=%s, total_messages=%s)",
            self.topic,
            len(results),
            total_messages,
        )

        return results

    def _build_partition_consumer(self) -> KafkaConsumer:
        """
        Build an independent Kafka consumer instance for one partition worker.

        Returns:
            KafkaConsumer instance.
        """
        return KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=self.enable_auto_commit,
            consumer_timeout_ms=self.consumer_timeout_ms,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )

    def _consume_partition_range(
        self,
        topic_partition: TopicPartition,
        beginning_offset: int,
        end_offset: int,
        committed_offset: int | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """
        Consume a bounded offset range for one partition.

        Args:
            topic_partition: TopicPartition object.
            beginning_offset: Earliest available offset for the partition.
            end_offset: Latest end offset for the partition.
            committed_offset: Current committed offset for the consumer group.
            limit: Maximum number of messages to consume from the partition.

        Returns:
            List of consumed message payloads.
        """
        consumer = self._build_partition_consumer()
        consumer.assign([topic_partition])

        start_offset = committed_offset if committed_offset is not None else beginning_offset
        stop_offset = min(start_offset + limit, end_offset)

        logger.info(
            "Partition %s bounded consume (start=%s, stop=%s, end=%s)",
            topic_partition.partition,
            start_offset,
            stop_offset,
            end_offset,
        )

        consumer.seek(topic_partition, start_offset)

        messages: list[dict[str, Any]] = []

        try:
            while True:
                message_pack = consumer.poll(timeout_ms=5000)

                if not message_pack:
                    break

                should_stop = False

                for _, records in message_pack.items():
                    for record in records:
                        if record.offset >= stop_offset:
                            should_stop = True
                            break

                        messages.append(record.value)

                    if should_stop:
                        break

                if should_stop:
                    break

            if not self.enable_auto_commit:
                consumer.commit()

            logger.info(
                "Partition %s consume completed (%s messages)",
                topic_partition.partition,
                len(messages),
            )

            return messages

        finally:
            consumer.close()
