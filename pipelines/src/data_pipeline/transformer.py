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
        sql_dir: Path | str,
        feature_set: str,  # "og" or "nf"
        aggregation_interval: float,
        slack: float,
    ):
        self.feature_set = feature_set
        self.aggregation_interval = aggregation_interval
        self.slack = slack

        sql_dir = Path(sql_dir)

        self.required_sources = [
            "raw_conn",
            "raw_pkts",
        ]

        # Load SQL queries
        self.update_watermark_sql = SQL.from_file(
            sql_dir / "watermarks" / "update_pipeline_watermark.sql"
        )

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

    def _compute_safe_horizon(self, conn: duckdb.DuckDBPyConnection) -> float:
        """Compute safe processing horizon across all source tables."""
        result = conn.execute(
            """
            SELECT MIN(max_event_ts) - $slack AS safe_horizon
            FROM source_watermarks
            WHERE source IN $required_sources
            HAVING MIN(max_event_ts) > 0
            """,
            {"slack": self.slack, "required_sources": self.required_sources},
        ).fetchone()

        return result[0] if result and result[0] is not None else 0.0

    def _get_last_processed_ts(
        self, conn: duckdb.DuckDBPyConnection, stage: str
    ) -> float:
        """Get last processed timestamp for a pipeline stage."""
        result = conn.execute(
            """
            SELECT last_processed_ts FROM etl_watermarks
            WHERE pipeline_stage = $stage
            """,
            {"stage": stage},
        ).fetchone()

        return result[0] if result and result[0] is not None else 0.0

    def _update_pipeline_watermark(
        self, conn: duckdb.DuckDBPyConnection, stage: str, timestamp: float
    ) -> None:
        """Update pipeline watermark to new timestamp."""
        sql, params = self.update_watermark_sql(
            stage=stage,
            last_processed_ts=timestamp,
        ).duck

        conn.execute(sql, params)

    def _aggregate_unified_flows(self, conn: duckdb.DuckDBPyConnection) -> int:
        """
        Stage 1: Raw logs → Unified flows (out-of-order safe).

        Returns:
            Number of rows processed
        """

        # Compute safe processing horizon
        safe_horizon = self._compute_safe_horizon(conn)

        # Get last processed timestamp
        last_processed_ts = self._get_last_processed_ts(conn, "compute_unified_flows")

        # Check if there's safe data to process
        if safe_horizon <= last_processed_ts:
            logger.debug(
                f"Stage 1: Waiting for more data (safe_horizon={safe_horizon:.2f}, "
                f"last_processed={last_processed_ts:.2f})"
            )
            return 0

        logger.info(
            f"Stage 1: Processing unified_flows window [{last_processed_ts:.2f}, {safe_horizon:.2f}]"
        )

        # Run aggregation with bounded window
        sql, params = self.aggregate_unified_flows_sql(
            last_raw_log_ts=last_processed_ts,
            safe_horizon=safe_horizon,
        ).duck

        result = conn.execute(sql, params)
        row_count = len(result.fetchall()) if result else 0

        # Update watermark to safe horizon (not MAX(ts)!)
        self._update_pipeline_watermark(conn, "compute_unified_flows", safe_horizon)

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
        stage_name = f"compute_{self.feature_set}_features"
        last_processed = self._get_last_processed_ts(conn, stage_name)

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
        sql, params = self.aggregate_features_sql(
            last_unified_flow_ts=last_processed
        ).duck

        result = conn.execute(sql, params)
        row_count = len(result.fetchall()) if result else 0

        # Update watermark to max unified_flows timestamp
        self._update_pipeline_watermark(conn, stage_name, max_unified_ts)

        logger.info(f"Stage 2 ({stage_name}): Processed {row_count} feature records")
        return row_count

    def run_once(self, global_conn) -> dict[str, int]:
        """
        Run one iteration of the two-stage delta processing pipeline.

        Returns:
            Dictionary with row counts for each stage
        """

        conn = global_conn.cursor()

        try:
            # Stage 1: Raw logs -> Unified flows
            unified_count = self._aggregate_unified_flows(conn)

            # Stage 2: Unified flows -> Features
            feature_count = self._aggregate_features(conn)

            return {
                "unified_flows": unified_count,
                f"{self.feature_set}_features": feature_count,
            }

        finally:
            conn.close()

    def aggregate_features(self, global_conn, duration_seconds: float | None = None):
        """
        Feature aggregation loop with delta processing.

        Args:
            duration_seconds: Run duration in seconds. None = run forever.
        """
        logger.info(
            f"Feature aggregation started (interval: {self.aggregation_interval}s, "
            f"feature_set: {self.feature_set}, slack: {self.slack}s)"
        )

        start_time = time.time()

        try:
            while (
                duration_seconds is None or time.time() - start_time < duration_seconds
            ):
                try:
                    # Run one iteration of the pipeline
                    counts = self.run_once(global_conn)

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
                time.sleep(self.aggregation_interval)

        except KeyboardInterrupt:
            logger.info("Feature aggregation interrupted")

        logger.info("Feature aggregation stopped")
