import datetime
import logging
import time
from pathlib import Path

import duckdb

from sqlutils import SQL

logger = logging.getLogger(__name__)


class FeatureTransformer:
    """Transforms raw Zeek logs to UNSW-NB15 features using delta processing with safe horizon."""

    def __init__(
        self,
        db_path: str,
        sql_dir: Path | str,
        aggregation_interval_sec: int = 5,
        slack_seconds: float = 5.0,
        feature_set: str = "og",  # "og" or "nf"
    ):
        self.db_path = db_path
        self.aggregation_interval_sec = aggregation_interval_sec
        self.slack_seconds = slack_seconds
        self.feature_set = feature_set

        sql_dir = Path(sql_dir)

        # Load SQL queries
        self.aggregate_unified_flows_sql = SQL.from_file(
            sql_dir / "features" / "aggregate_unified_flows.sql"
        )

        if feature_set == "og":
            self.aggregate_features_sql = SQL.from_file(
                sql_dir / "features" / "aggregate_og_features.sql"
            )
        elif feature_set == "nf":
            self.aggregate_features_sql = SQL.from_file(
                sql_dir / "features" / "aggregate_nf_features.sql"
            )
        else:
            raise ValueError(
                f"Invalid feature_set: {feature_set}. Must be 'og' or 'nf'"
            )

    def _update_source_watermarks(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Update source watermarks based on newly arrived data."""
        sources = [
            "raw_conn",
            "raw_dns",
            "raw_http",
            "raw_pkt_stats",
            "raw_ssl",
            "raw_ftp",
        ]

        for source in sources:
            conn.execute(
                f"""
                INSERT INTO source_watermarks (source, max_event_ts, max_ingestion_ts, last_updated)
                SELECT
                    '{source}',
                    COALESCE(MAX(ts), 0.0),
                    COALESCE(MAX(ingested_at), (SELECT CURRENT_TIMESTAMP)),
                    (SELECT CURRENT_TIMESTAMP)
                FROM {source}
                ON CONFLICT (source) DO UPDATE SET
                    max_event_ts = GREATEST(EXCLUDED.max_event_ts, max_event_ts),
                    max_ingestion_ts = GREATEST(EXCLUDED.max_ingestion_ts, max_ingestion_ts),
                    last_updated = EXCLUDED.last_updated
            """
            )

    def _compute_safe_horizon(self, conn: duckdb.DuckDBPyConnection) -> float:
        """Compute safe processing horizon across all source tables."""
        result = conn.execute(
            """
            SELECT MIN(max_event_ts) - ? AS safe_horizon
            FROM source_watermarks
            WHERE source LIKE 'raw_%' AND max_event_ts > 0
        """,
            [self.slack_seconds],
        ).fetchone()

        return result[0] if result and result[0] is not None else 0.0

    def _get_pipeline_watermark(
        self, conn: duckdb.DuckDBPyConnection, stage: str
    ) -> float:
        """Get last processed timestamp for a pipeline stage."""
        result = conn.execute(
            """
            SELECT last_processed_ts FROM etl_watermarks
            WHERE pipeline_stage = ?
        """,
            [stage],
        ).fetchone()

        return result[0] if result and result[0] is not None else 0.0

    def _update_pipeline_watermark(
        self, conn: duckdb.DuckDBPyConnection, stage: str, timestamp: float
    ) -> None:
        """Update pipeline watermark to new timestamp."""
        now = datetime.datetime.now(datetime.timezone.utc)
        conn.execute(
            """
            INSERT INTO etl_watermarks (pipeline_stage, last_processed_ts, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT (pipeline_stage) DO UPDATE SET
                last_processed_ts = EXCLUDED.last_processed_ts,
                updated_at = EXCLUDED.updated_at
        """,
            [stage, timestamp, now],
        )

    def _aggregate_unified_flows(self, conn: duckdb.DuckDBPyConnection) -> int:
        """
        Stage 1: Raw logs → Unified flows (out-of-order safe).

        Returns:
            Number of rows processed
        """
        # Update source watermarks
        self._update_source_watermarks(conn)

        # Compute safe processing horizon
        safe_horizon = self._compute_safe_horizon(conn)

        # Get last processed timestamp
        last_processed = self._get_pipeline_watermark(conn, "unified_flows")

        # Check if there's safe data to process
        if safe_horizon <= last_processed:
            logger.debug(
                f"Stage 1: Waiting for more data (safe_horizon={safe_horizon:.2f}, "
                f"last_processed={last_processed:.2f})"
            )
            return 0

        logger.info(
            f"Stage 1: Processing unified_flows window [{last_processed:.2f}, {safe_horizon:.2f}]"
        )

        # Run aggregation with bounded window
        bound_sql = self.aggregate_unified_flows_sql(last_raw_log_ts=last_processed)
        query, params = bound_sql.duck
        result = conn.execute(query, params)
        row_count = len(result.fetchall()) if result else 0

        # Update watermark to safe horizon (not MAX(ts)!)
        self._update_pipeline_watermark(conn, "unified_flows", safe_horizon)

        logger.info(f"Stage 1: Processed {row_count} unified flows")
        return row_count

    def _aggregate_features(self, conn: duckdb.DuckDBPyConnection) -> int:
        """
        Stage 2: Unified flows → Features (og_features or nf_features).

        Returns:
            Number of rows processed
        """
        # Get max timestamp from unified_flows
        result = conn.execute(
            "SELECT COALESCE(MAX(ts), 0.0) FROM unified_flows"
        ).fetchone()
        max_unified_ts = result[0] if result else 0.0

        # Get last processed timestamp for feature stage
        stage_name = f"{self.feature_set}_features"
        last_processed = self._get_pipeline_watermark(conn, stage_name)

        # Check if there's new data to process
        if max_unified_ts <= last_processed:
            logger.debug(
                f"Stage 2 ({stage_name}): No new data "
                f"(max_unified={max_unified_ts:.2f}, last_processed={last_processed:.2f})"
            )
            return 0

        logger.info(
            f"Stage 2 ({stage_name}): Processing window [{last_processed:.2f}, {max_unified_ts:.2f}]"
        )

        # Run feature aggregation
        bound_sql = self.aggregate_features_sql(last_unified_flow_ts=last_processed)
        query, params = bound_sql.duck
        result = conn.execute(query, params)
        row_count = len(result.fetchall()) if result else 0

        # Update watermark to max unified_flows timestamp
        self._update_pipeline_watermark(conn, stage_name, max_unified_ts)

        logger.info(f"Stage 2 ({stage_name}): Processed {row_count} feature records")
        return row_count

    def run_once(self) -> dict[str, int]:
        """
        Run one iteration of the two-stage delta processing pipeline.

        Returns:
            Dictionary with row counts for each stage
        """
        conn = duckdb.connect(self.db_path)

        try:
            # Stage 1: Raw logs → Unified flows
            unified_count = self._aggregate_unified_flows(conn)

            # Stage 2: Unified flows → Features
            feature_count = self._aggregate_features(conn)

            # Commit all changes (watermarks + data)
            conn.commit()

            return {
                "unified_flows": unified_count,
                f"{self.feature_set}_features": feature_count,
            }

        finally:
            conn.close()

    def aggregate_features(self, duration_seconds: int | None = None):
        """
        Feature aggregation loop with delta processing.

        Args:
            duration_seconds: Run duration in seconds. None = run forever.
        """
        logger.info(
            f"Feature aggregation started (interval: {self.aggregation_interval_sec}s, "
            f"feature_set: {self.feature_set}, slack: {self.slack_seconds}s)"
        )

        start_time = time.time()

        try:
            while (
                duration_seconds is None or time.time() - start_time < duration_seconds
            ):
                try:
                    # Run one iteration of the pipeline
                    counts = self.run_once()

                    # Log results
                    if (
                        counts["unified_flows"] > 0
                        or counts[f"{self.feature_set}_features"] > 0
                    ):
                        logger.info(
                            f"Pipeline iteration complete: "
                            f"unified_flows={counts['unified_flows']}, "
                            f"{self.feature_set}_features={counts[f'{self.feature_set}_features']}"
                        )

                except Exception as e:
                    logger.error(f"Aggregation error: {e}", exc_info=True)

                # Sleep until next iteration
                time.sleep(self.aggregation_interval_sec)

        except KeyboardInterrupt:
            logger.info("Feature aggregation interrupted")

        logger.info("Feature aggregation stopped")
