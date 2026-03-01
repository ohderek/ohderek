"""
main.py
───────────────────────────────────────────────────────────────────────────────
Entry point for the CoinMarketCap → Snowflake ETL pipeline.

Usage:
    python main.py                        # top 500 coins (default)
    python main.py --limit 5000           # top 5,000 coins
    python main.py --no-global-metrics    # skip global metrics snapshot

Environment variables required:
    CMC_API_KEY               — CoinMarketCap Pro API key
    SNOWFLAKE_ACCOUNT         — e.g. xy12345.us-east-1
    SNOWFLAKE_USER
    SNOWFLAKE_PRIVATE_KEY_PATH
    SNOWFLAKE_ROLE            (optional, default: TRANSFORMER)
    SNOWFLAKE_WAREHOUSE       (optional, default: CRYPTO_WH)
    SNOWFLAKE_DATABASE        (optional, default: CRYPTO)
    SNOWFLAKE_SCHEMA          (optional, default: RAW)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

from src.coinmarketcap_client import CoinMarketCapClient
from src.transform import listings_to_parquet, global_metrics_to_parquet
from src.snowflake_loader import SnowflakeLoader

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("crypto-etl")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CoinMarketCap → Snowflake ETL")
    p.add_argument("--limit",             type=int, default=500,
                   help="Number of top coins to fetch (max 5000)")
    p.add_argument("--no-global-metrics", action="store_true",
                   help="Skip the global market metrics snapshot")
    return p.parse_args()


def main() -> None:
    args     = parse_args()
    api_key  = os.environ.get("CMC_API_KEY", "")
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if not api_key:
        logger.error("CMC_API_KEY environment variable is not set")
        sys.exit(1)

    logger.info("Starting CoinMarketCap ETL — limit=%d  run_date=%s", args.limit, run_date)

    client = CoinMarketCapClient(api_key=api_key)
    summary: dict[str, dict] = {}

    with SnowflakeLoader() as loader:

        # ------------------------------------------------------------------
        # Coin listings  (paginated — yields one batch per API call)
        # ------------------------------------------------------------------
        total_coins = 0

        for page_num, page in enumerate(client.listings_pages(limit=args.limit), start=1):
            parquet_bytes = listings_to_parquet(page)
            metrics       = loader.load_listings(
                parquet_bytes = parquet_bytes,
                run_date      = f"{run_date}_page{page_num:03d}",
            )
            total_coins  += len(page)
            logger.info("Page %d: %d coins  %s", page_num, len(page), metrics)

        summary["listings"] = {"coins_fetched": total_coins}

        # ------------------------------------------------------------------
        # Global market metrics (single snapshot)
        # ------------------------------------------------------------------
        if not args.no_global_metrics:
            metrics_snapshot  = client.global_metrics()
            parquet_bytes     = global_metrics_to_parquet(metrics_snapshot)
            metrics           = loader.load_global_metrics(
                parquet_bytes = parquet_bytes,
                run_date      = run_date,
            )
            summary["global_metrics"] = metrics
            logger.info(
                "Global metrics: BTC dominance=%.2f%%  total_mcap=$%.2fT",
                metrics_snapshot.btc_dominance or 0,
                (metrics_snapshot.total_market_cap_usd or 0) / 1e12,
            )

    logger.info("ETL complete: %s", summary)


if __name__ == "__main__":
    main()
