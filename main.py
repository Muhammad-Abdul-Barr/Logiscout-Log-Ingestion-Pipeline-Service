"""
LogiScout Logs Ingestion Pipeline (Without Spark) — Docker Entry Point.

Starts the scheduled pipeline loop that polls ClickHouse via clickhouse-connect
and ingests promoted traces into Qdrant.
"""

import logging
import sys
from dotenv import load_dotenv

# Load .env before config reads os.getenv()
load_dotenv(override=True)

from config import Config
from pipeline import LogProcessingPipeline

# ── Logging Setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("logiscout")


def main():
    logger.info("=" * 60)
    logger.info("  LogiScout Logs Ingestion Pipeline (Without Spark)")
    logger.info("=" * 60)

    config = Config.from_env()

    logger.info("OLAP:     %s:%s/%s", config.olap_host, config.olap_port, config.olap_database)
    logger.info("Table:    %s", config.olap_table)
    logger.info("Qdrant:   %s", config.qdrant_url)
    logger.info("Interval: %d min", config.fetch_interval_minutes)
    logger.info("Batch:    %d rows", config.fetch_batch_size)

    pipeline = LogProcessingPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
