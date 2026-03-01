"""
transform.py
───────────────────────────────────────────────────────────────────────────────
Serialises CoinMarketCap API response objects to Parquet bytes via PyArrow.

Using PyArrow + Parquet instead of row-by-row inserts because:
  - Snowflake COPY INTO natively reads Parquet — no CSV quoting edge cases
  - Columnar encoding compresses numeric data (prices, volumes) efficiently
  - Schema is enforced at write time, not at load time
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from coinmarketcap_client import CoinListing, GlobalMetrics


# ---------------------------------------------------------------------------
# PyArrow schemas — explicit types prevent silent precision loss on floats
# ---------------------------------------------------------------------------

LISTINGS_SCHEMA = pa.schema([
    pa.field("id",                       pa.int64()),
    pa.field("name",                     pa.string()),
    pa.field("symbol",                   pa.string()),
    pa.field("slug",                     pa.string()),
    pa.field("cmc_rank",                 pa.int32()),
    pa.field("num_market_pairs",         pa.int32()),
    pa.field("circulating_supply",       pa.float64()),
    pa.field("total_supply",             pa.float64()),
    pa.field("max_supply",               pa.float64()),
    pa.field("infinite_supply",          pa.bool_()),
    pa.field("price_usd",                pa.float64()),
    pa.field("volume_24h_usd",           pa.float64()),
    pa.field("volume_change_24h_pct",    pa.float64()),
    pa.field("market_cap_usd",           pa.float64()),
    pa.field("market_cap_dominance",     pa.float64()),
    pa.field("fully_diluted_market_cap", pa.float64()),
    pa.field("pct_change_1h",            pa.float64()),
    pa.field("pct_change_24h",           pa.float64()),
    pa.field("pct_change_7d",            pa.float64()),
    pa.field("pct_change_30d",           pa.float64()),
    pa.field("last_updated",             pa.string()),
    pa.field("fetched_at",               pa.string()),
])

GLOBAL_METRICS_SCHEMA = pa.schema([
    pa.field("total_market_cap_usd",       pa.float64()),
    pa.field("total_volume_24h_usd",       pa.float64()),
    pa.field("total_volume_24h_reported",  pa.float64()),
    pa.field("btc_dominance",              pa.float64()),
    pa.field("eth_dominance",              pa.float64()),
    pa.field("active_cryptocurrencies",    pa.int32()),
    pa.field("active_exchanges",           pa.int32()),
    pa.field("active_market_pairs",        pa.int32()),
    pa.field("last_updated",               pa.string()),
    pa.field("fetched_at",                 pa.string()),
])


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def listings_to_parquet(listings: Sequence[CoinListing]) -> bytes:
    """Convert a list of CoinListing objects to Parquet bytes."""
    rows  = [asdict(c) for c in listings]
    table = pa.Table.from_pylist(rows, schema=LISTINGS_SCHEMA)
    buf   = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue().to_pybytes()


def global_metrics_to_parquet(metrics: GlobalMetrics) -> bytes:
    """Convert a single GlobalMetrics snapshot to Parquet bytes."""
    rows  = [asdict(metrics)]
    table = pa.Table.from_pylist(rows, schema=GLOBAL_METRICS_SCHEMA)
    buf   = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue().to_pybytes()
