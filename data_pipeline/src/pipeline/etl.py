import argparse
import logging
import threading
from pathlib import Path

import duckdb

from sqlutils import SQL

from .archiver import DataArchiver
from .loader import ZeekLoader
from .transformer import FeatureTransformer

# Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


class ZeekETLPipeline:
    """Orchestrates the Zeek ETL pipeline."""

    def __init__(
        self,
        db_path: str,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "zeek-consumer",
        hot_window_hours: int = 24,
        aggregation_interval_sec: int = 5,
        archive_interval_sec: int = 21600,
    ):
        self.db_path = db_path
        self.loader = ZeekLoader(db_path, bootstrap_servers, topic, group_id)
        self.transformer = FeatureTransformer(db_path, aggregation_interval_sec)
        self.archiver = DataArchiver(db_path, hot_window_hours, archive_interval_sec)

    def init_db(self):
        """Initialize DuckDB schema."""
        conn = duckdb.connect(self.db_path)
        cursor = conn.cursor()

        for sql_file in [
            "create_raw_conn.sql",
            "create_raw_unsw_extra.sql",
            "create_og_nb15_features.sql",
            "create_indices.sql",
        ]:
            sql = SQL.from_file(SQL_DIR / "schema" / sql_file)
            cursor.execute(*sql.duck)

        logger.info("Database schema initialized")
        cursor.close()
        conn.close()

    def run(self, duration_seconds: int | None = None):
        """Start all threads."""
        logger.info("Starting Zeek ETL pipeline")

        threads = [
            threading.Thread(
                target=self.loader.consume_and_insert,
                args=(duration_seconds,),
                name="Loader",
                daemon=True,
            ),
            threading.Thread(
                target=self.transformer.aggregate_features,
                args=(duration_seconds,),
                name="Transformer",
                daemon=True,
            ),
            threading.Thread(
                target=self.archiver.archive_old_data,
                args=(duration_seconds,),
                name="Archiver",
                daemon=True,
            ),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        logger.info("Pipeline stopped")


def main():
    parser = argparse.ArgumentParser(
        description="Zeek Kafka to DuckDB streaming ETL pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="zeek_features.db",
        help="Path to DuckDB database file",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="broker:29092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic", type=str, default="zeek.logs", help="Kafka topic to consume from"
    )
    parser.add_argument(
        "--group-id", type=str, default="zeek-consumer", help="Kafka consumer group ID"
    )
    parser.add_argument(
        "--hot-window-hours",
        type=int,
        default=24,
        help="Hours of data to keep in hot storage",
    )
    parser.add_argument(
        "--aggregation-interval",
        type=int,
        default=5,
        help="Feature aggregation interval in seconds",
    )
    parser.add_argument(
        "--archive-interval",
        type=int,
        default=21600,
        help="Archival interval in seconds (default: 6 hours)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Duration to run pipeline in seconds (None = run forever)",
    )

    args = parser.parse_args()

    pipeline = ZeekETLPipeline(
        db_path=args.db_path,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        hot_window_hours=args.hot_window_hours,
        aggregation_interval_sec=args.aggregation_interval,
        archive_interval_sec=args.archive_interval,
    )

    try:
        pipeline.run(duration_seconds=args.duration)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")


if __name__ == "__main__":
    main()
