import logging
import time
from pathlib import Path

import duckdb

from sqlutils import SQL

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent.parent / "sql"


class FeatureTransformer:
    """Transforms raw Zeek logs to UNSW-NB15 features using delta processing."""

    def __init__(self, db_path: str, aggregation_interval_sec: int = 5):
        self.db_path = db_path
        self.aggregation_interval_sec = aggregation_interval_sec

        self.get_last_stime_sql = SQL.from_file(
            SQL_DIR / "features" / "get_last_computed_stime.sql"
        )

        self.aggregation_sql = SQL.from_file(
            SQL_DIR / "features" / "aggregate_og_features.sql"
        )

    def aggregate_features(self, duration_seconds: int | None):
        """Feature aggregation thread with delta processing."""
        conn = duckdb.connect(self.db_path)
        cursor = conn.cursor()

        logger.info(
            f"Feature aggregation started (interval: {self.aggregation_interval_sec}s)"
        )

        start_time = time.time()

        try:
            while (
                duration_seconds is None or time.time() - start_time < duration_seconds
            ):
                try:
                    _result = cursor.execute(*self.get_last_stime_sql.duck).fetchone()
                    last_stime = _result[0] if _result else None

                    logger.info(f"Processing deltas since stime={last_stime}")

                    bound_sql = self.aggregation_sql(last_stime=last_stime)
                    cursor.execute(*bound_sql.duck)

                    _result = cursor.execute("SELECT changes()").fetchone()
                    rows_affected = _result[0] if _result else 0

                    if rows_affected > 0:
                        logger.info(f"Faeture aggregation: {rows_affected} rows")

                except Exception as e:
                    logger.error(f"Aggregation error: {e}", exc_info=True)

                time.sleep(self.aggregation_interval_sec)

        except KeyboardInterrupt:
            logger.info("Feature aggregation interrupted")
        finally:
            conn.close()
