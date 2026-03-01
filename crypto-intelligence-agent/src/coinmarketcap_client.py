"""
coinmarketcap_client.py
───────────────────────────────────────────────────────────────────────────────
CoinMarketCap REST API client.

Wraps the two endpoints used by this pipeline:
  - /v1/cryptocurrency/listings/latest  (top-N coins by market cap)
  - /v1/global-metrics/quotes/latest    (total market stats)

Features:
  - API key injection via header (never in query params or logs)
  - Automatic retry with exponential back-off on 429 / 5xx
  - Pagination for listings endpoint (CMC caps at 5,000 per request)
  - Structured dataclasses for type-safe downstream handling
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Generator

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Response dataclasses
# ---------------------------------------------------------------------------

@dataclass
class CoinListing:
    id:                     int
    name:                   str
    symbol:                 str
    slug:                   str
    cmc_rank:               int
    num_market_pairs:       int
    circulating_supply:     float | None
    total_supply:           float | None
    max_supply:             float | None
    infinite_supply:        bool
    price_usd:              float | None
    volume_24h_usd:         float | None
    volume_change_24h_pct:  float | None
    market_cap_usd:         float | None
    market_cap_dominance:   float | None
    fully_diluted_market_cap: float | None
    pct_change_1h:          float | None
    pct_change_24h:         float | None
    pct_change_7d:          float | None
    pct_change_30d:         float | None
    last_updated:           str
    fetched_at:             str


@dataclass
class GlobalMetrics:
    total_market_cap_usd:       float | None
    total_volume_24h_usd:       float | None
    total_volume_24h_reported:  float | None
    btc_dominance:              float | None
    eth_dominance:              float | None
    active_cryptocurrencies:    int   | None
    active_exchanges:           int   | None
    active_market_pairs:        int   | None
    last_updated:               str
    fetched_at:                 str


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class CoinMarketCapClient:
    """
    Thin httpx-based client for the CoinMarketCap Pro API.

    Usage:
        client = CoinMarketCapClient(api_key="cmc-key-here")
        for page in client.listings_pages(limit=5000):
            process(page)
        metrics = client.global_metrics()
    """

    BASE_URL       = "https://pro-api.coinmarketcap.com"
    MAX_RETRIES    = 4
    BACKOFF_BASE   = 2   # seconds; doubles on each retry

    def __init__(self, api_key: str, timeout: float = 30.0) -> None:
        self._headers = {
            "X-CMC_PRO_API_KEY": api_key,
            "Accept":            "application/json",
        }
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def listings_pages(
        self,
        limit: int        = 5000,
        convert: str      = "USD",
        page_size: int    = 1000,
    ) -> Generator[list[CoinListing], None, None]:
        """
        Yield pages of CoinListing objects for the top `limit` coins by market cap.

        CMC's /listings/latest endpoint caps at 5,000 coins per call; this
        method handles pagination transparently so callers don't need to track
        start offsets.
        """
        fetched = 0
        start   = 1

        while fetched < limit:
            batch_size = min(page_size, limit - fetched)
            raw = self._get(
                "/v1/cryptocurrency/listings/latest",
                params={
                    "start":   start,
                    "limit":   batch_size,
                    "convert": convert,
                    "aux":     "num_market_pairs,cmc_rank,max_supply,circulating_supply,"
                               "total_supply,market_cap_by_total_supply,volume_24h_reported,"
                               "infinite_supply",
                },
            )

            data = raw.get("data", [])
            if not data:
                break

            fetched_at = datetime.now(timezone.utc).isoformat()
            page = [self._parse_listing(coin, fetched_at, convert) for coin in data]
            logger.info("Listings page start=%d fetched=%d coins", start, len(page))
            yield page

            fetched += len(data)
            start   += len(data)

            if len(data) < batch_size:
                break   # CMC returned fewer than requested — we've hit the end

    def global_metrics(self, convert: str = "USD") -> GlobalMetrics:
        """Fetch a single GlobalMetrics snapshot."""
        raw = self._get(
            "/v1/global-metrics/quotes/latest",
            params={"convert": convert},
        )
        return self._parse_global_metrics(raw.get("data", {}), convert)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GET request with retry / back-off logic."""
        url = f"{self.BASE_URL}{path}"

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                with httpx.Client(timeout=self._timeout) as client:
                    resp = client.get(url, headers=self._headers, params=params)

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code == 429:
                    # Rate limited — honour Retry-After header if present
                    retry_after = int(resp.headers.get("Retry-After", self.BACKOFF_BASE ** attempt))
                    logger.warning("Rate limited (429). Waiting %ds before retry %d/%d",
                                   retry_after, attempt, self.MAX_RETRIES)
                    time.sleep(retry_after)
                    continue

                if resp.status_code >= 500:
                    wait = self.BACKOFF_BASE ** attempt
                    logger.warning("Server error %d. Retry %d/%d in %ds",
                                   resp.status_code, attempt, self.MAX_RETRIES, wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()   # 4xx (not 429) — fail immediately

            except httpx.TimeoutException:
                wait = self.BACKOFF_BASE ** attempt
                logger.warning("Request timeout. Retry %d/%d in %ds", attempt, self.MAX_RETRIES, wait)
                time.sleep(wait)

        raise RuntimeError(f"CoinMarketCap API call failed after {self.MAX_RETRIES} retries: {path}")

    @staticmethod
    def _parse_listing(coin: dict, fetched_at: str, convert: str) -> CoinListing:
        quote = coin.get("quote", {}).get(convert, {})
        return CoinListing(
            id                        = coin["id"],
            name                      = coin.get("name", ""),
            symbol                    = coin.get("symbol", ""),
            slug                      = coin.get("slug", ""),
            cmc_rank                  = coin.get("cmc_rank", 0),
            num_market_pairs          = coin.get("num_market_pairs", 0),
            circulating_supply        = coin.get("circulating_supply"),
            total_supply              = coin.get("total_supply"),
            max_supply                = coin.get("max_supply"),
            infinite_supply           = coin.get("infinite_supply", False),
            price_usd                 = quote.get("price"),
            volume_24h_usd            = quote.get("volume_24h"),
            volume_change_24h_pct     = quote.get("volume_change_24h"),
            market_cap_usd            = quote.get("market_cap"),
            market_cap_dominance      = quote.get("market_cap_dominance"),
            fully_diluted_market_cap  = quote.get("fully_diluted_market_cap"),
            pct_change_1h             = quote.get("percent_change_1h"),
            pct_change_24h            = quote.get("percent_change_24h"),
            pct_change_7d             = quote.get("percent_change_7d"),
            pct_change_30d            = quote.get("percent_change_30d"),
            last_updated              = coin.get("last_updated", ""),
            fetched_at                = fetched_at,
        )

    @staticmethod
    def _parse_global_metrics(data: dict, convert: str) -> GlobalMetrics:
        quote      = data.get("quote", {}).get(convert, {})
        fetched_at = datetime.now(timezone.utc).isoformat()
        return GlobalMetrics(
            total_market_cap_usd      = quote.get("total_market_cap"),
            total_volume_24h_usd      = quote.get("total_volume_24h"),
            total_volume_24h_reported = quote.get("total_volume_24h_reported"),
            btc_dominance             = data.get("btc_dominance"),
            eth_dominance             = data.get("eth_dominance"),
            active_cryptocurrencies   = data.get("active_cryptocurrencies"),
            active_exchanges          = data.get("active_exchanges"),
            active_market_pairs       = data.get("active_market_pairs"),
            last_updated              = data.get("last_updated", ""),
            fetched_at                = fetched_at,
        )
