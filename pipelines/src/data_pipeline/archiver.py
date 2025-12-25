"""Cold storage archival"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from sqlutils import SQLTemplate

logger = logging.getLogger(__name__)


class DataArchiver:
    """Manages hot/cold storage lifecycle."""

    def __init__(
        self,
        db_path: str,
        archive_path: Path | str,
        sql_dir: Path | str,
        feature_set: str = "nf",
        batch_size_threshold: int = 10000,
        archive_age_secs: int = 5,
        min_archive_interval_sec: int = 10,
        retention_secs: int = 30 * 60,
    ):
        self.db_path = db_path
        self.archive_path = Path(archive_path)
        self.feature_set = feature_set
        self.archive_age_secs = archive_age_secs
        self.min_archive_interval_sec = min_archive_interval_sec
        self.batch_size_threshold = batch_size_threshold
        self.retention_secs = retention_secs

        # Track last archive time
        self._last_archive_time = 0.0
        self._last_archived_count = 0

        # Ensure archive directory exists
        self.archive_path.mkdir(parents=True, exist_ok=True)

        sql_dir = Path(sql_dir)

        self.export_template = SQLTemplate.from_file(
            sql_dir / "archival" / "export.sql"
        )

        self.delete_old_raw_template = SQLTemplate.from_file(
            sql_dir / "archival" / "delete_old_raw.sql"
        )

        self.delete_old_computed_template = SQLTemplate.from_file(
            sql_dir / "archival" / "delete_old_computed.sql"
        )

        if self.feature_set not in ("og", "nf"):
            raise ValueError(f"Invalid feature set: {self.feature_set}")

    def _should_archive(self, conn: duckdb.DuckDBPyConnection) -> tuple[bool, str, int]:
        """
        Check if archival should run based on configured striggers.

        Args:
            conn: DuckDB connection

        Returns:
            Tuple of (should_archive, reason, ready_count)
        """
        now = time.time()

        # Check interval (prevent thrashing)
        time_since_last = now - self._last_archive_time
        if time_since_last < self.min_archive_interval_sec:
            return False, f"Too soon (last archive {time_since_last:.0f}s ago)", 0

        # Calculate cutoff
        archive_cutoff_datetime = datetime.now() - timedelta(
            seconds=self.archive_age_secs
        )
        archive_cutoff_ts = archive_cutoff_datetime.timestamp()

        # Check how many records are ready to archive
        table_name = f"{self.feature_set}_features"
        count_result = conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE ts < ?",
            [archive_cutoff_ts],
        ).fetchone()

        ready_count = count_result[0] if count_result else 0
        if ready_count == 0:
            return False, "No data ready", 0

        # Trigger: Batch size threshold reached
        if ready_count >= self.batch_size_threshold:
            return (
                True,
                f"Batch threshold reached ({ready_count} >= {self.batch_size_threshold})",
                ready_count,
            )

        # If we have ANY data and enough time has passed, archive it
        # This prevents data from sitting around waiting for batch to fill
        if ready_count > 0 and time_since_last >= self.min_archive_interval_sec * 2:
            return True, f"Time elapsed with {ready_count} records ready", ready_count

        return (
            False,
            f"Waiting ({ready_count}/{self.batch_size_threshold}, {time_since_last:.0f}s elapsed)",
            ready_count,
        )

    def _export_features(
        self,
        conn: duckdb.DuckDBPyConnection,
        cutoff_ts: float,
        output_path: str,
    ) -> int:
        """
        Export features older than cutoff to Parquet
        """
        feature_tbl = f"{self.feature_set}_features"

        logger.info(f"Exporting {feature_tbl} to {output_path}")

        export_feature_sql = self.export_template.substitute(source=feature_tbl)

        sql_str, params = export_feature_sql(
            cutoff_ts=cutoff_ts, output_path=output_path
        ).duck

        conn.execute(sql_str, params)

        count_result = conn.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [output_path]
        ).fetchone()
        record_count = count_result[0] if count_result else 0

        logger.info(f"Successfully exported {record_count} records")
        return record_count

    def _export_unified_flows(
        self,
        conn: duckdb.DuckDBPyConnection,
        cutoff_ts: float,
        output_path: str,
    ) -> int:
        """
        Export unified flows older than cutoff to Parquet
        """
        export_unified_flows_sql = self.export_template.substitute(
            source="unified_flows"
        )
        bound_sql = export_unified_flows_sql(
            cutoff_ts=cutoff_ts, output_path=output_path
        )
        conn.execute(*bound_sql.duck)  # ty: ignore

        count_result = conn.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [output_path]
        ).fetchone()
        record_count = count_result[0] if count_result else 0

        logger.info(f"Successfully exported {record_count} unified flows")
        return record_count

    def _delete_old_data(
        self, conn: duckdb.DuckDBPyConnection, cuttoff_datetime: datetime
    ) -> dict[str, int]:
        """
        Delete old data from all tables.
        """

        counts = {}

        raw_tables = [
            "raw_conn",
            "raw_dns",
            "raw_http",
            "raw_pkt_stats",
            "raw_ssl",
            "raw_ftp",
        ]

        for tbl in raw_tables:
            try:
                sql = self.delete_old_raw_template.substitute(source=tbl)
                bound_sql = sql(cutoff_ts=cuttoff_datetime)
                result = conn.execute(*bound_sql.duck)  # ty: ignore
                deleted_count = len(result.fetchall()) if result else 0
                counts[tbl] = deleted_count

                if deleted_count > 0:
                    logger.info(f"Deleted {deleted_count} old records from {tbl}")

            except Exception as e:
                logger.error(f"Error deleting old data from {tbl}: {e}")
                counts[tbl] = 0

        computed_tables = ["unified_flows", f"{self.feature_set}_features"]

        for tbl in computed_tables:
            try:
                sql = self.delete_old_computed_template.substitute(source=tbl)
                bound_sql = sql(cutoff_ts=cuttoff_datetime)
                result = conn.execute(*bound_sql.duck)  # ty: ignore
                deleted_count = len(result.fetchall()) if result else 0
                counts[tbl] = deleted_count

                if deleted_count > 0:
                    logger.info(f"Deleted {deleted_count} old records from {tbl}")
            except Exception as e:
                logger.error(f"Error deleting old data from {tbl}: {e}")
                counts[tbl] = 0

        return counts

    def run_once(self, force: bool = False) -> dict[str, Any] | None:
        """
        Run one iteration of the archival process.
        """
        conn = duckdb.connect(self.db_path)

        try:
            # Check if we should archive
            if not force:
                should_archive, reason, ready_count = self._should_archive(conn)
                if not should_archive:
                    logger.debug(f"Skipped archival: {reason}")
                    return None
                logger.info(f"Archival triggered: {reason}")

            now = datetime.now()

            # Cutoff for ARCHIVING
            archive_cutoff_datetime = now - timedelta(seconds=self.archive_age_secs)
            archive_cutoff_ts = archive_cutoff_datetime.timestamp()

            # Cutoff for DELETION
            delete_cutoff_datetime = now - timedelta(seconds=self.retention_secs)

            # Generate feature output filename with timestamp
            feature_output_filename = (
                f"{self.feature_set}_features_{now.strftime('%Y%m%d%H%M%S')}.parquet"
            )
            feature_output_path = str(self.archive_path / feature_output_filename)

            # Generate unified flows output filename with timestamp
            unified_flows_output_filename = f"{self.feature_set}_unified_flows_{now.strftime('%Y%m%d%H%M%S')}.parquet"
            unified_flows_output_path = str(
                self.archive_path / unified_flows_output_filename
            )

            # Step 1: Exports
            start_time = time.time()
            exported_feature_count = self._export_features(
                conn, archive_cutoff_ts, feature_output_path
            )
            feature_export_duration = time.time() - start_time

            exported_unified_flows_count = self._export_unified_flows(
                conn, archive_cutoff_ts, unified_flows_output_path
            )
            unified_flows_export_duration = time.time() - start_time

            # Step 2: Delete old data
            delete_start = time.time()
            deleted_count = self._delete_old_data(conn, delete_cutoff_datetime)
            delete_duration = time.time() - delete_start

            # Step 3: Commit all changes
            conn.commit()

            # Step 4: VACUUM (skipped for speed)
            # conn.execute("VACUUM")

            total_duration = time.time() - start_time

            # Update tracking
            self._last_archive_time = time.time()
            self._last_archived_count = (
                exported_feature_count + exported_unified_flows_count
            )

            logger.info(
                f"Archival completed in {total_duration:.2f}s. "
                f"(export: {feature_export_duration:.2f}s, {unified_flows_export_duration:.2f}s;"
                f"delete: {delete_duration:.2f}s)"
            )

            return {
                "exported_features": exported_feature_count,
                "exported_unified_flows": exported_unified_flows_count,
                "deleted_count": deleted_count,
                "feature_output_file": (
                    feature_output_filename if exported_feature_count > 0 else None
                ),
                "unified_flows_output_file": unified_flows_output_filename
                if exported_unified_flows_count > 0
                else None,
                "duration_sec": total_duration,
            }
        finally:
            conn.close()

    def archive_old_data(
        self, duration_seconds: int | None = None, check_interval_sec: int = 30
    ):
        """
        Args:
            duration_seconds: Run duration in seconds. None = run forever.
            check_interval_secs: How often to check for new data (default: 30s)
        """

        logger.info(
            f"Archival started:\n"
            f"  - Archive age: {self.archive_age_secs}s\n"
            f"  - Retention: {self.retention_secs}s\n"
            f"  - Batch threshold: {self.batch_size_threshold} records\n"
            f"  - Min interval: {self.min_archive_interval_sec}s\n"
            f"  - Check interval: {check_interval_sec}s\n"
            f"  - Feature set: {self.feature_set}\n"
            f"  - Archive path: {self.archive_path}"
        )

        start_time = time.time()

        try:
            while (
                duration_seconds is None or time.time() - start_time < duration_seconds
            ):
                try:
                    # Check and potentially run archival
                    stats = self.run_once(force=False)

                    # Log summary if archival ran
                    if stats:
                        total_deleted = sum(stats["deleted_count"].values())
                        logger.info(
                            f"Archived {stats['exported_features']} records -> {stats['feature_output_file']}\n"
                            f"Archived {stats['exported_unified_flows']} unified flows -> {stats['unified_flows_output_file']}\n"
                            f"in {stats['duration_sec']:.2f}s"
                        )

                        if total_deleted > 0:
                            logger.info(f"Deleted {total_deleted:,} records")

                except Exception as e:
                    logger.error(f"Archival error: {e}", exc_info=True)

                time.sleep(check_interval_sec)
        except KeyboardInterrupt:
            logger.info("Archival interrupted")

        logger.info("Archival stopped")
