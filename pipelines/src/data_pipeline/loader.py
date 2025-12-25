"""Kafka consumer and batch loader for raw Zeek logs"""

import datetime
import json
import logging
import time
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
from confluent_kafka import Consumer, KafkaError

from .const import (
    CONN_ARROW_SCHEMA,
    DNS_ARROW_SCHEMA,
    FTP_ARROW_SCHEMA,
    HTTP_ARROW_SCHEMA,
    PKT_STATS_ARROW_SCHEMA,
    SSL_ARROW_SCHEMA,
)

logger = logging.getLogger(__name__)


class LogBatch:
    __slots__ = (
        "table_name",
        "schema",
        "batch_size",
        "max_age_sec",
        "first_insert_time",
        "data",
        "rows",
    )

    def __init__(self, table_name, schema, batch_size, max_age_sec):
        self.table_name = table_name
        self.schema = schema
        self.batch_size = batch_size
        self.max_age_sec = max_age_sec

        self.first_insert_time = None
        self.data: dict[str, list[Any]] = {field.name: [] for field in schema}
        self.rows = 0

    def __len__(self) -> int:
        return self.rows

    def append(self, row: dict[str, Any]):
        if self.first_insert_time is None:
            self.first_insert_time = time.time()

        ingestion_time = datetime.datetime.now(datetime.timezone.utc)

        for field in self.schema:
            col_name = field.name

            # Special handling for ingested_at - set to current timestamp
            if col_name == "ingested_at":
                value = ingestion_time
            else:
                value = row.get(col_name)

            if pa.types.is_list(field.type):
                if value is None:
                    # Missing field or explicit None
                    self.data[col_name].append(None)
                else:
                    # Already a list/tuple
                    self.data[col_name].append(list(value))  # ty: ignore
            else:
                # Single value - append as-is (None if missing)
                self.data[col_name].append(value)

        self.rows += 1

    def age(self) -> float:
        if self.first_insert_time is None:
            return 0.0
        return time.time() - self.first_insert_time

    def should_flush(self) -> bool:
        return len(self) > 0 and (
            len(self) >= self.batch_size or self.age() >= self.max_age_sec
        )

    def to_record_batch(self) -> pa.RecordBatch:
        """Convert batch data to PyArrow RecordBatch with proper schema"""
        arrays = []
        for field in self.schema:
            col_data = self.data[field.name]
            arrays.append(pa.array(col_data, type=field.type))
        return pa.RecordBatch.from_arrays(arrays, schema=self.schema)

    def clear(self):
        self.data = {field.name: [] for field in self.schema}
        self.rows = 0
        self.first_insert_time = None


class ZeekLoader:
    """Loads raw Zeek logs from Kafka into DuckDB staging tables"""

    def __init__(
        self,
        db_path: str,
        sql_dir: Path | str,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "zeek-consumer",
        batch_size: int = 122880,  # https://arrow.apache.org/blog/2025/03/10/fast-streaming-inserts-in-duckdb-with-adbc/
        max_age_sec: float = 5,
    ):
        self.db_path = db_path
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.max_age_sec = max_age_sec

    def consume_and_insert(
        self, global_conn: duckdb.DuckDBPyConnection, duration_seconds: int | None
    ):
        """Kafka consumer thread that inserts raw messages into staging tables"""

        conn = global_conn.cursor()

        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        consumer.subscribe([self.topic])
        logger.info(f"Consumer started, subscribing to topic: {self.topic}")

        start_time = time.time()
        messages_processed = 0

        log_batches: dict[str, LogBatch] = {
            "conn": LogBatch(
                "raw_conn",
                CONN_ARROW_SCHEMA,
                self.batch_size,
                self.max_age_sec,
            ),
            "pkt_stats": LogBatch(
                "raw_pkt_stats",
                PKT_STATS_ARROW_SCHEMA,
                self.batch_size,
                self.max_age_sec,
            ),
            "dns": LogBatch(
                "raw_dns",
                DNS_ARROW_SCHEMA,
                self.batch_size,
                self.max_age_sec,
            ),
            "http": LogBatch(
                "raw_http",
                HTTP_ARROW_SCHEMA,
                self.batch_size,
                self.max_age_sec,
            ),
            "ssl": LogBatch(
                "raw_ssl",
                SSL_ARROW_SCHEMA,
                self.batch_size,
                self.max_age_sec,
            ),
            "ftp": LogBatch(
                "raw_ftp",
                FTP_ARROW_SCHEMA,
                self.batch_size,
                self.max_age_sec,
            ),
        }

        try:
            while (
                duration_seconds is None or time.time() - start_time < duration_seconds
            ):
                # Poll for messages with 1 second timeout
                msg = consumer.poll(1.0)

                if msg is None:
                    # Check if any batches aged out during quiet period
                    flushables = [
                        batch
                        for _, batch in log_batches.items()
                        if batch.should_flush()
                    ]

                    if flushables:
                        flushed_rows = self._flush_batches(conn, consumer, flushables)
                        logger.warning("Quiet period - flushing pending batches")
                        for tbl, nrows in flushed_rows.items():
                            logger.info(f"Flushed {nrows} to {tbl}")

                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Parse json message
                    event = json.loads(msg.value().decode("utf-8"))
                    message_has_data = False

                    # Extract conn log if present
                    if "conn" in event:
                        c = event["conn"]
                        if c.get("ts") and c.get("uid"):
                            message_has_data = True
                            log_batches["conn"].append(c)
                        else:
                            logger.warning(
                                "Missing required fields (ts/uid). Skip message"
                            )

                    # Extract pkt-stats log if present (note: hyphen in key!)
                    elif "pkt-stats" in event:
                        p = event["pkt-stats"]
                        if p.get("ts") and p.get("uid"):
                            message_has_data = True
                            log_batches["pkt_stats"].append(p)
                        else:
                            logger.warning(
                                "Missing required fields (ts/uid). Skip message"
                            )
                    # Extract dns log if present
                    elif "dns" in event:
                        d = event["dns"]
                        if d.get("ts") and d.get("uid"):
                            message_has_data = True
                            log_batches["dns"].append(d)
                        else:
                            logger.warning(
                                "Missing required fields (ts/uid). Skip message"
                            )
                    # Extract http log if present
                    elif "http" in event:
                        h = event["http"]
                        if h.get("ts") and h.get("uid"):
                            message_has_data = True
                            log_batches["http"].append(h)
                        else:
                            logger.warning(
                                "Missing required fields (ts/uid). Skip message"
                            )
                    elif "ssl" in event:
                        s = event["ssl"]
                        if s.get("ts") and s.get("uid"):
                            message_has_data = True
                            log_batches["ssl"].append(s)
                        else:
                            logger.warning(
                                "Missing required fields (ts/uid). Skip message"
                            )
                    elif "ftp" in event:
                        f = event["ftp"]
                        if f.get("ts") and f.get("uid"):
                            message_has_data = True
                            log_batches["ftp"].append(f)
                        else:
                            logger.warning(
                                "Missing required fields (ts/uid). Skip message"
                            )

                    # Only count messages that had valid data
                    if message_has_data:
                        messages_processed += 1

                    flusables = [
                        batch
                        for _, batch in log_batches.items()
                        if batch.should_flush()
                    ]
                    if len(flusables) != 0:
                        try:
                            flushed_rows = self._flush_batches(
                                conn, consumer, flusables
                            )

                            logger.info(f"Processed {messages_processed} messages")
                            for tbl, nrows in flushed_rows.items():
                                logger.info(f"Flushed {nrows} to {tbl}")

                        except Exception as e:
                            logger.error(f"Batch flush failed: {e}", exc_info=True)
                            raise  # Stop consumer

                except (json.JSONDecodeError, Exception) as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    try:
                        consumer.commit()
                    except Exception as commit_e:
                        logger.error(f"Failed to commit bad message offset: {commit_e}")

        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        finally:
            # Check for remain in batches
            flusables = [batch for _, batch in log_batches.items() if len(batch) > 0]
            if len(flusables) != 0:
                try:
                    flushed_rows = self._flush_batches(conn, consumer, flusables)

                    logger.info(f"Processed {messages_processed} messages")
                    for tbl, nrows in flushed_rows.items():
                        logger.info(f"Flushed {nrows} to {tbl}")

                except Exception as e:
                    logger.error(f"Final flush failed: {e}", exc_info=True)

            consumer.close()
            conn.close()

            logger.info(f"Consumer stopped. Total: {messages_processed} messages")

    def _flush_batches(
        self,
        conn: duckdb.DuckDBPyConnection,
        consumer: Consumer,
        flusables: list[LogBatch],
    ):
        """
        Flush log batches to duckdb using persistent connection with Arrow integration
        """
        # Use persistent connection with DuckDB's from_arrow() method
        for batch in flusables:
            record_batch = batch.to_record_batch()
            logger.debug(f"Flushing {len(batch)} rows to {batch.table_name}")
            logger.debug(f"Arrow schema: {record_batch.schema}")
            try:
                # Convert Arrow RecordBatch to PyArrow Table and insert directly
                arrow_table = pa.Table.from_batches([record_batch])
                # Use DuckDB's from_arrow to create a relation, then insert
                conn.from_arrow(arrow_table).insert_into(batch.table_name)
            except Exception as e:
                logger.error(f"Failed to ingest into {batch.table_name}: {e}")
                logger.error(
                    f"Arrow schema fields: {[f.name for f in record_batch.schema]}"
                )
                raise

        consumer.commit()

        flushed_rows = {}
        for batch in flusables:
            flushed_rows[batch.table_name] = len(batch)
            batch.clear()

        return flushed_rows
