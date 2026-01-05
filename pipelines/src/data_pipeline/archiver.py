"""Cold storage archival"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from sqlutils import SQL, SQLTemplate

logger = logging.getLogger(__name__)


class DataArchiver:
    """Move data to external storage."""

    def __init__(
        self,
        archive_path: Path | str,
        sql_dir: Path | str,
        feature_set: str,  # 'nf' or 'og'
        archive_interval: float,  # archive new features every n seconds
        retention: float,  # event retention interval in seconds
        backup_interval: float,  # backup every n seconds
        reserved: int,  # keep at least n records during deletion
    ):
        if isinstance(archive_path, str) and archive_path.startswith("s3://"):
            self.archive_path = archive_path
        else:
            archive_path = Path(archive_path)
            (archive_path / "features").mkdir(parents=True, exist_ok=True)
            (archive_path / "backups").mkdir(parents=True, exist_ok=True)
            self.archive_path = str(archive_path)

        self.feature_set = feature_set
        self.archive_interval = archive_interval
        self.retention = retention
        self.backup_inteval = backup_interval
        self.reserved = reserved

        self._last_backup_dt = datetime.now()

        sql_dir = Path(sql_dir)

        self.update_watermark_sql = SQL.from_file(
            sql_dir / "watermarks" / "update_pipeline_watermark.sql"
        )

        self.export_template = SQLTemplate.from_file(
            sql_dir / "archival" / "export.sql"
        )

        self.delete_template = SQLTemplate.from_file(
            sql_dir / "archival" / "delete.sql"
        )

        if self.feature_set not in ("og", "nf"):
            raise ValueError(f"Invalid feature set: {self.feature_set}")

    def _get_last_processed_ts(
        self, conn: duckdb.DuckDBPyConnection, stage: str
    ) -> float:
        """Get last archival timestamp"""

        result = conn.execute(
            """
            SELECT last_processed_ts FROM etl_watermarks
            WHERE pipeline_stage = ?
            """,
            [stage],
        ).fetchone()

        return result[0] if result and result[0] else 0.0

    def _update_pipeline_watermark(
        self, conn: duckdb.DuckDBPyConnection, stage: str, timestamp: float
    ):
        """Update archival watermark to new timestamp"""

        sql, params = self.update_watermark_sql(
            stage=stage,
            last_processed_ts=timestamp,
        ).duck

        conn.execute(sql, params)

    def _get_safe_cutoff_ts(
        self, conn: duckdb.DuckDBPyConnection, source: str, downstream_stage: str
    ) -> float:
        safe_cutoff = conn.execute(
            f"""
            WITH bounds AS (
                SELECT
                    MAX(ts) - $retention AS time_cutoff,
                    (SELECT ts FROM {source} ORDER BY ts DESC LIMIT 1 OFFSET $reserved) AS count_cutoff
                FROM {source}
            ),
            downstream_bound AS (
                SELECT COALESCE(
                    last_processed_ts,
                    (SELECT MAX(ts) FROM {source})
                ) AS downstream_cutoff
                FROM etl_watermarks
                WHERE pipeline_stage = '{downstream_stage}'
            )
            SELECT LEAST (
                time_cutoff,
                count_cutoff,
                downstream_cutoff
            ) AS cutoff
            FROM bounds, downstream_bound
            """,
            {
                "retention": self.retention,
                "reserved": self.reserved,
            },
        ).fetchone()

        return safe_cutoff[0] if safe_cutoff and safe_cutoff[0] else 0.0

    def _export_data(
        self,
        conn: duckdb.DuckDBPyConnection,
    ) -> tuple[int, str]:
        """
        Export newly computed features to Parquet
        """

        now = datetime.now()
        output_filename = (
            f"{self.feature_set}_features_{now.strftime('%Y%m%d%H%M%S')}.parquet"
        )
        output_path = f"{self.archive_path}/features/{output_filename}"

        source = f"{self.feature_set}_features"

        # Get max timestamp from feature table
        result = conn.execute(f"SELECT COALESCE(MAX(ts), 0.0) FROM {source}").fetchone()

        pipeline_stage = f"archive_{self.feature_set}_features"

        max_features_ts = result[0] if result else 0.0
        last_archival_ts = self._get_last_processed_ts(conn, pipeline_stage)

        if max_features_ts <= last_archival_ts:
            logger.info("No new features to export")
            return 0, ""

        logger.info(f"Exporting {source} to {output_path}")

        export_feature_sql = self.export_template.substitute(source=source)

        sql, params = export_feature_sql(
            cutoff_ts=last_archival_ts, output_path=output_path
        ).duck

        conn.execute(sql, params)

        # MORE SQL INJECTION!
        count_result = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{output_path}')",
        ).fetchone()

        record_count = count_result[0] if count_result else 0

        # Update archival watermark
        self._update_pipeline_watermark(conn, pipeline_stage, max_features_ts)

        logger.info(f"Exported {record_count:,} records")
        return record_count, output_filename

    def _backup_and_delete(
        self,
        conn: duckdb.DuckDBPyConnection,
    ) -> tuple[dict[str, int], str]:
        """
        Backup and delete old data.
        """

        now = datetime.now()

        output_dirname = f"backup_{now.strftime('%Y%m%d%H%M%S')}"
        output_path = f"{self.archive_path}/backups/{output_dirname}"

        if now - self._last_backup_dt < timedelta(seconds=self.backup_inteval):
            logger.info("Skipping backup")
            return {}, ""

        logger.info(f"Backing up data to {output_path}")

        # backup existing data
        # SQL INJECTION! Fix this or go kill yourself
        conn.execute(
            f"EXPORT DATABASE '{output_path}' (FORMAT parquet)",
        )
        self._last_backup_dt = now

        counts = {}

        lineage = [
            (f"{self.feature_set}_features", f"archive_{self.feature_set}_features"),
            ("unified_flows", f"compute_{self.feature_set}_features"),
            ("raw_conn", "compute_unified_flows"),
            ("raw_pkt_stats", "compute_unified_flows"),
            ("raw_dns", "compute_unified_flows"),
            ("raw_http", "compute_unified_flows"),
            ("raw_ssl", "compute_unified_flows"),
            ("raw_ftp", "compute_unified_flows"),
        ]

        logger.info("Deleting historical data")

        for source, downstream_stage in lineage:
            cutoff_ts = self._get_safe_cutoff_ts(conn, source, downstream_stage)

            delete_sql = self.delete_template.substitute(
                source=source,
            )

            sql, params = delete_sql(cutoff_ts=cutoff_ts).duck
            result = conn.execute(sql, params)
            deleted_count = len(result.fetchall()) if result else 0
            counts[source] = deleted_count

            if deleted_count > 0:
                logger.info(f"Deleted {deleted_count} old records from {source}")

        return counts, output_dirname

    def run_once(
        self,
        global_conn: duckdb.DuckDBPyConnection,
    ) -> dict[str, Any]:
        """
        Run one iteration of the archival process.
        """
        conn = global_conn.cursor()

        try:
            # Step 1: Exports
            exported_count, exported_filename = self._export_data(conn=conn)

            # Step 2: Backup and delete old data
            deleted_count, backup_dirname = self._backup_and_delete(conn)

            # Step 3: VACUUM (skipped for speed)
            # conn.execute("VACUUM")

            return {
                "exported_count": exported_count,
                "deleted_count": deleted_count,
                "feature_output_file": exported_filename,
                "backup_dir": backup_dirname,
            }

        finally:
            conn.close()

    def archive_old_data(
        self,
        global_conn: duckdb.DuckDBPyConnection,
        duration: int | None = None,
    ):
        """
        Args:
            duration: Run duration in seconds. None = run forever.
        """

        logger.info(
            f"Archival started:\n"
            f"  - Feature set: {self.feature_set}\n"
            f"  - Archive path: {self.archive_path}"
            f"  - Archival interval: {self.archive_interval}s\n"
            f"  - Retention: {self.retention}s\n"
        )

        start_time = time.time()

        try:
            while duration is None or time.time() - start_time < duration:
                try:
                    # Check and potentially run archival
                    stats = self.run_once(global_conn)
                    total_deleted = sum(stats["deleted_count"].values())

                    # Log archival summary
                    if stats["exported_count"] > 0:
                        logger.info(
                            f"Archived {stats['exported_count']} records to {stats['feature_output_file']}"
                        )

                    # Log backup-delete summary
                    if stats["backup_dir"] != "" and total_deleted > 0:
                        logger.info(f"Deleted total {total_deleted:,} records")
                        logger.info(f"Backed up to {stats['backup_dir']}")

                except Exception as e:
                    logger.error(f"Archival error: {e}", exc_info=True)

                time.sleep(self.archive_interval)

        except KeyboardInterrupt:
            logger.info("Archival interrupted")

        logger.info("Archival stopped")
