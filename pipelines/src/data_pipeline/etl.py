import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb

from sqlutils import SQL

from .archiver import DataArchiver
from .loader import ZeekLogLoader
from .transformer import FeatureTransformer

# Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


class ZeekDataPipeline:
    """Orchestrates the Zeek ETL pipeline."""

    def __init__(
        self,
        db_path: str,
        archive_path: str,
        sql_dir: Path | str,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "zeek-consumer",
        feature_set: str = "nf",  # "og" or "nf"
        loader_batch_size: int = 122880,
        loader_max_age: float = 5,
        aggregation_interval: float = 30,
        slack: float = 5,
        archive_interval: float = 5,
        retention: float = 3600,
        backup_interval: float = 12 * 3600,
        reserved: int = 10000,
    ):
        self.db_path = db_path
        self.archive_path = archive_path
        self.sql_dir = Path(sql_dir)

        self.loader = ZeekLogLoader(
            sql_dir=sql_dir,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            batch_size=loader_batch_size,
            max_age=loader_max_age,
        )

        self.transformer = FeatureTransformer(
            sql_dir=sql_dir,
            feature_set=feature_set,
            aggregation_interval=aggregation_interval,
            slack=slack,
        )

        self.archiver = DataArchiver(
            sql_dir=sql_dir,
            archive_path=archive_path,
            feature_set=feature_set,
            archive_interval=archive_interval,
            retention=retention,
            backup_interval=backup_interval,
            reserved=reserved,
        )

    def init_db(self, global_conn: duckdb.DuckDBPyConnection):
        """Initialize DuckDB schema."""
        conn = global_conn.cursor()

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
            conn.execute(*sql.duck)

        logger.info("Creating indices...")
        # Create indices - execute one by one
        indices_sql_path = self.sql_dir / "schema" / "create_indices.sql"
        indices_content = indices_sql_path.read_text()

        # Split by semicolon and execute each statement
        for statement in indices_content.split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(statement)

        logger.info("Initializing watermarks...")

        init_files = [
            "init_source_watermarks.sql",
            "init_etl_watermarks.sql",
        ]

        for sql_file in init_files:
            logger.debug(f"Executing {sql_file}")
            sql = SQL.from_file(self.sql_dir / "watermarks" / sql_file)
            conn.execute(*sql.duck)

        logger.info("Database schema initialized successfully")
        conn.close()

    def create_s3_secret(self, global_conn: duckdb.DuckDBPyConnection):
        key_id = os.getenv("AWS_ACCESS_KEY_ID")
        secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        endpoint = os.getenv("DUCKDB_S3_ENDPOINT")

        if not key_id or not secret or not endpoint:
            logger.warning(
                "S3 credentials incomplete or not found, skip AWS secret setup"
            )
            return

        conn = global_conn.cursor()
        conn.execute("DROP SECRET IF EXISTS s3_secret")
        conn.execute(
            f"""
            CREATE PERSISTENT SECRET s3_secret (
                -- default
                TYPE s3,
                REGION 'us-east-1',
                USE_SSL false,
                URL_STYLE 'path',

                -- parameters
                KEY_ID   '{key_id}',
                SECRET   '{secret}',
                ENDPOINT '{endpoint}'
            )
            """,
        )

        result = conn.execute("SELECT * FROM duckdb_secrets();").fetchall()
        logger.info(f"Secret created: {result}")
        conn.close()

    def run(
        self,
        duration: float | None = None,
        disable_transform: bool = False,
        disable_archive: bool = False,
    ):
        """Start all threads."""
        logger.info("Starting Zeek ETL pipeline")

        conn = duckdb.connect(self.db_path, config={"autoload_known_extensions": True})

        self.init_db(conn)
        self.create_s3_secret(conn)

        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="ETL") as executor:
            futures = {
                executor.submit(
                    self.loader.consume_and_insert,
                    conn,
                    duration,
                ): "Loader",
            }

            if not disable_transform:
                futures[
                    executor.submit(
                        self.transformer.aggregate_features,
                        conn,
                        duration,
                    )
                ] = "Transformer"

            if not disable_archive:
                futures[
                    executor.submit(
                        self.archiver.archive_old_data,
                        conn,
                        duration,
                    )
                ] = "Archiver"

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
        description="Zeek-Kafka to DuckDB streaming ETL pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="/data/pipeline.db",
        help="Path to DuckDB database file",
    )
    parser.add_argument(
        "--archive-path",
        type=str,
        default="s3://pipeline-data",
        help="Path to archive directory (local path or s3 compatible storage)",
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
        "--topic",
        type=str,
        default="zeek.dpi",
        help="Kafka topic to consume from",
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default="zeek-consumer",
        help="Kafka consumer group ID",
    )
    parser.add_argument(
        "--feature-set",
        type=str,
        default="nf",
        choices=["og", "nf"],
        help="Feature set to use: og (Original UNSW-NB15) or nf (NetFlow)",
    )
    parser.add_argument(
        "--loader-batch-size",
        type=int,
        default=122880,
        help="Batch size for incoming messages per log head before flushing to database (default: 122880)",
    )
    parser.add_argument(
        "--loader-max-age",
        type=float,
        default=5.0,
        help="Maximum age allowed for data to sit in batch before flushing in seconds (default: 5s)",
    )
    parser.add_argument(
        "--aggregation-interval",
        type=float,
        default=30.0,
        help="Feature aggregation interval in seconds (default: 30s)",
    )
    parser.add_argument(
        "--slack",
        type=float,
        default=5.0,
        help="Delay in seconds for performing join on log sources to mitigate incomplete data (default: 5s)",
    )
    parser.add_argument(
        "--archive-interval",
        type=float,
        default=5.0,
        help="Computed feature archival in seconds",
    )
    parser.add_argument(
        "--retention",
        type=float,
        default=3600,
        help="Reserves events within this interval when cleanup",
    )
    parser.add_argument(
        "--backup-interval",
        type=float,
        default=12 * 3600,
        help="Backup interval in seconds (default: (12 * 3600)s)",
    )
    parser.add_argument(
        "--reserved",
        type=int,
        default=10_000,
        help="Minimum records to reserve when cleanup",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Duration to run pipeline in seconds (None = run forever)",
    )

    args = parser.parse_args()

    pipeline = ZeekDataPipeline(
        db_path=args.db_path,
        archive_path=args.archive_path,
        sql_dir=args.sql_dir,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        feature_set=args.feature_set,
        loader_batch_size=args.loader_batch_size,
        loader_max_age=args.loader_max_age,
        aggregation_interval=args.aggregation_interval,
        slack=args.slack,
        archive_interval=args.archive_interval,
        retention=args.retention,
        backup_interval=args.backup_interval,
        reserved=args.reserved,
    )

    try:
        pipeline.run(
            duration=args.duration,
        )
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")


if __name__ == "__main__":
    main()
