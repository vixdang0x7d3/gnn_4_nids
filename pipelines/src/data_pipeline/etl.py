import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
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


class ZeekETLPipeline:
    """Orchestrates the Zeek ETL pipeline."""

    def __init__(
        self,
        db_path: str,
        archive_path: str,
        sql_dir: Path | str,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "zeek-consumer",
        aggregation_interval_sec: int = 5,
        archive_age_sec: int = 10,  # 10 seconds (very fast archival)
        retention_sec: int = 60,  # 1 minute (fast cleanup)
        feature_set: str = "og",  # "og" or "nf"
        batch_size_threshold: int = 10000,
    ):
        self.db_path = db_path
        self.archive_path = archive_path
        self.sql_dir = Path(sql_dir)

        self.loader = ZeekLoader(db_path, sql_dir, bootstrap_servers, topic, group_id)

        self.transformer = FeatureTransformer(
            db_path=db_path,
            sql_dir=sql_dir,
            aggregation_interval_sec=aggregation_interval_sec,
            slack_seconds=5.0,
            feature_set=feature_set,
        )

        self.archiver = DataArchiver(
            db_path=db_path,
            archive_path=archive_path,
            sql_dir=sql_dir,
            archive_age_secs=archive_age_sec,
            retention_secs=retention_sec,
            feature_set=feature_set,
            batch_size_threshold=batch_size_threshold,
            min_archive_interval_sec=30,
        )

    def init_db(self):
        """Initialize DuckDB schema."""
        conn = duckdb.connect(self.db_path)
        cursor = conn.cursor()

        logger.info("Initializing database schema...")

        schema_files = [
            # Raw tables
            "create_raw_conn.sql",
            "create_raw_pkt_stats.sql",
            "create_raw_dns.sql",
            "create_raw_http.sql",
            "create_raw_ssl.sql",
            "create_raw_ftp.sql",
            # Intermediate table
            "create_unified_flows.sql",
            # Feature tables
            "create_og_nb15_features.sql",
            "create_nf_nb15_v3_features.sql",
            # Watermarks
            "create_source_watermarks.sql",
            "create_etl_watermarks.sql",
        ]

        for sql_file in schema_files:
            logger.debug(f"Executing {sql_file}")
            sql = SQL.from_file(self.sql_dir / "schema" / sql_file)
            cursor.execute(*sql.duck)

        logger.info("Creating indices...")
        # Create indices - execute one by one
        indices_sql_path = self.sql_dir / "schema" / "create_indices.sql"
        indices_content = indices_sql_path.read_text()

        # Split by semicolon and execute each statement
        for statement in indices_content.split(";"):
            statement = statement.strip()
            if statement:
                cursor.execute(statement)

        logger.info("Initializing watermarks...")
        # Initialize watermarks
        init_files = [
            "init_source_watermarks.sql",
            "init_etl_watermarks.sql",
        ]

        for sql_file in init_files:
            logger.debug(f"Executing {sql_file}")
            sql = SQL.from_file(self.sql_dir / "inserts" / sql_file)
            cursor.execute(*sql.duck)

        logger.info("Database schema initialized successfully")
        cursor.close()
        conn.close()

    def run(self, duration_seconds: int | None = None):
        """Start all threads."""
        logger.info("Starting Zeek ETL pipeline")

        self.init_db()

        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="ETL") as executor:
            futures = {
                executor.submit(
                    self.loader.consume_and_insert, duration_seconds
                ): "Loader",
                executor.submit(
                    self.transformer.aggregate_features, duration_seconds
                ): "Transformer",
                executor.submit(
                    self.archiver.archive_old_data, duration_seconds, 30
                ): "Archiver",  # check_interval_sec=30
            }

            for future in as_completed(futures):
                thread_name = futures[future]
                try:
                    future.result()
                    logger.info(f"{thread_name} completed successfully")
                except Exception as e:
                    logger.critical(f"{thread_name} crashed: {e}", exc_info=True)
                    # Cancel remaining tasks and raise
                    for f in futures:
                        f.cancel()
                    raise

        logger.info("Pipeline stopped")


def main():
    parser = argparse.ArgumentParser(
        description="Zeek Kafka to DuckDB streaming ETL pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="/data/features.db",
        help="Path to DuckDB database file",
    )
    parser.add_argument(
        "--archive-path",
        type=str,
        default="/data/archive",
        help="Path to archive directory",
    )
    parser.add_argument(
        "--sql-dir",
        type=str,
        default="/sql",
        help="Path to SQL statements directory",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="broker:29092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic", type=str, default="zeek.dpi", help="Kafka topic to consume from"
    )
    parser.add_argument(
        "--group-id", type=str, default="zeek-consumer", help="Kafka consumer group ID"
    )
    parser.add_argument(
        "--aggregation-interval",
        type=int,
        default=5,
        help="Aggregation interval in seconds (default: 5 seconds)",
    )
    parser.add_argument(
        "--archive-age-sec",
        type=int,
        default=10,
        help="How old data must be before archiving in seconds (default: 10s for very fast archival)",
    )
    parser.add_argument(
        "--retention-sec",
        type=int,
        default=60,
        help="How long to keep data in hot storage in seconds (default: 60s = 1 minute for fast cleanup)",
    )
    parser.add_argument(
        "--feature-set",
        type=str,
        default="nf",
        choices=["og", "nf"],
        help="Feature set to use: og (Original UNSW-NB15) or nf (NetFlow)",
    )
    parser.add_argument(
        "--batch-size-threshold",
        type=int,
        default=10000,
        help="Batch size threshold for archiving (default: 10000 records)",
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
        archive_path=args.archive_path,
        sql_dir=args.sql_dir,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        aggregation_interval_sec=args.aggregation_interval,
        archive_age_sec=args.archive_age_sec,
        retention_sec=args.retention_sec,
        feature_set=args.feature_set,
        batch_size_threshold=args.batch_size_threshold,
    )

    try:
        pipeline.run(duration_seconds=args.duration)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")


if __name__ == "__main__":
    main()
