"""Kafka consumer and batch loader for raw Zeek logs"""

import json
import logging
import time
from pathlib import Path

import duckdb
from confluent_kafka import Consumer, KafkaError

from sqlutils import SQL

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


class ZeekLoader:
    """Loads raw Zeek logs from Kafka into DuckDB staging tables"""

    def __init__(
        self,
        db_path: str,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "zeek-consumer",
        batch_size: int = 100,
    ):
        self.db_path = db_path
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size

        self.insert_conn_sql = SQL.from_file(
            SQL_DIR / "inserts" / "insert_raw_conn.sql"
        )
        self.insert_unsw_sql = SQL.from_file(
            SQL_DIR / "inserts" / "insert_raw_unsw.sql"
        )

    def consume_and_insert(self, duration_seconds: int | None):
        """Kafka consumer thread that inserts raw messages into staging tables"""
        conn = duckdb.connect(self.db_path)
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
        )

        consumer.subscribe([self.topic])
        logger.info(f"Consumer started, subscribing to topic: {self.topic}")

        start_time = time.time()
        messages_processed = 0
        conn_batch, unsw_batch = [], []

        try:
            while (
                duration_seconds is None or time.time() - start_time < duration_seconds
            ):
                # Poll for messages with 1 second timeout
                msg = consumer.poll(1.0)

                if msg is None:
                    # No new message - flush any pending batch
                    if conn_batch or unsw_batch:
                        self._flush_batch(conn, conn_batch, unsw_batch)
                        conn_batch, unsw_batch = [], []
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Parse json message
                    event = json.loads(msg.value().decode("utf-8"))

                    # Extract conn log if present
                    if "conn" in event:
                        c = event["conn"]
                        conn_batch.append(
                            (
                                c.get("ts"),
                                c.get("uid"),
                                c.get("id.orig_h"),
                                c.get("id.orig_p"),
                                c.get("id.resp_h"),
                                c.get("id.resp_p"),
                                c.get("proto"),
                                c.get("service"),
                                c.get("duration"),
                                c.get("orig_bytes"),
                                c.get("resp_bytes"),
                                c.get("conn_state"),
                                c.get("local_orig"),
                                c.get("local_resp"),
                                c.get("missed_bytes"),
                                c.get("history"),
                                c.get("orig_pkts"),
                                c.get("orig_ip_bytes"),
                                c.get("resp_pkts"),
                                c.get("resp_ip_bytes"),
                                c.get("tunnel_parents"),
                            )
                        )

                    # Extract unsw-extra log if present
                    if "unsw-extra" in event:
                        u = event["unsw-extra"]
                        unsw_batch.append(
                            (
                                u.get("ts"),
                                u.get("uid"),
                                u.get("id.orig_h"),
                                u.get("id.orig_p"),
                                u.get("id.resp_h"),
                                u.get("id.resp_p"),
                                u.get("tcp_rtt"),
                                u.get("src_pkt_times", []),
                                u.get("dst_pkt_times", []),
                                u.get("src_ttl"),
                                u.get("dst_ttl"),
                                u.get("src_pkt_sizes", []),
                                u.get("dst_pkt_sizes", []),
                            )
                        )

                    messages_processed += 1

                    if (
                        len(conn_batch) >= self.batch_size
                        or len(unsw_batch) >= self.batch_size
                    ):
                        self._flush_batch(conn, conn_batch, unsw_batch)
                        conn_batch, unsw_batch = [], []
                        logger.info(f"Processed {messages_processed} messages")
                except (json.JSONDecodeError, Exception) as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        finally:
            # Flush remaining messages
            if conn_batch or unsw_batch:
                self._flush_batch(conn, conn_batch, unsw_batch)

            consumer.close()
            conn.close()
            logger.info(f"Consumer stopped. Total: {messages_processed} messages")

    def _flush_batch(self, conn, conn_batch, unsw_batch):
        """
        Flush batched messages using bulk operations
        """
        cursor = conn.cursor()

        if conn_batch:
            bulk_conn = self.insert_conn_sql.bind_many(conn_batch)
            cursor.executemany(*bulk_conn.duck_many)

        if unsw_batch:
            bulk_unsw = self.insert_unsw_sql.bind_many(unsw_batch)
            cursor.executemany(*bulk_unsw.duck_many)

        cursor.close()
