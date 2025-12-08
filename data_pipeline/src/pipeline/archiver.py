"""Cold storage archival"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from sqlutils import SQL

logger = logging.getLogger(__name__)


class DataArchiver:
    """Manages hot/cold storage lifecycle."""

    def __init__(
        self,
        db_path: str,
        archive_path: Path | str,
        sql_dir: Path | str,
        archive_interval_sec: int = 21600,
    ):
        self.db_path = db_path
        self.archive_path = Path(archive_path)
        self.archive_interval_sec = archive_interval_sec

        self.export_sql = SQL.from_file(
            Path(sql_dir) / "archival" / "export_features.sql"
        )
        self.delete_conn_sql = SQL.from_file(
            Path(sql_dir) / "archival" / "delete_old_conn.sql"
        )
        self.delete_unsw_sql = SQL.from_file(
            Path(sql_dir) / "archival" / "delete_old_unsw.sql"
        )
        self.delete_features_sql = SQL.from_file(
            Path(sql_dir) / "archival" / "delete_old_features.sql"
        )

    def archive_old_data(self, duration_seconds: int | None = None):
        """Archival thread for cold storage."""
        con = duckdb.connect(self.db_path)
        cursor = con.cursor()

        logger.info(f"Archival started (interval: {self.archive_interval_sec}s)")
        start_time = time.time()

        try:
            while (
                duration_seconds is None or time.time() - start_time < duration_seconds
            ):
                try:
                    cutoff_ts = (
                        datetime.now() - timedelta(seconds=self.archive_interval_sec)
                    ).timestamp()

                    output_path = str(
                        self.archive_path
                        / f"features_{datetime.now().strftime('%Y-%m-%d')}.parquet"
                    )

                    # Export and delete
                    cursor.execute(
                        *self.export_sql(
                            cutoff_ts=cutoff_ts, output_path=output_path
                        ).duck
                    )

                    cursor.execute(*self.delete_conn_sql(cutoff_ts=cutoff_ts).duck)
                    deleted_conn = cursor.rowcount

                    cursor.execute(*self.delete_unsw_sql(cutoff_ts=cutoff_ts).duck)
                    deleted_unsw = cursor.rowcount

                    cursor.execute(*self.delete_features_sql(cutoff_ts=cutoff_ts).duck)
                    deleted_features = cursor.rowcount

                    logger.info(
                        f"Archived: {deleted_conn} conn, {deleted_unsw} unsw, {deleted_features} features"
                    )

                except Exception as e:
                    logger.error(f"Archival error: {e}", exc_info=True)

                time.sleep(self.archive_interval_sec)

        except KeyboardInterrupt:
            logger.info("Archival interrupted")
        finally:
            con.close()
