"""
Generic paginated REST API extractor.
Supports cursor-based and offset-based pagination.
"""

import os
import logging
from datetime import datetime
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base_extractor import BaseExtractor, ExtractorConfig

logger = logging.getLogger(__name__)


class APIExtractorConfig(ExtractorConfig):
    base_url: str = ""
    endpoint: str = ""
    auth_type: str = "bearer"          # bearer | api_key | basic
    pagination_type: str = "offset"    # offset | cursor | page
    date_filter_param: str = "updated_after"
    response_data_key: str = "data"    # JSON key containing records
    timeout_seconds: int = 30


class APIExtractor(BaseExtractor):
    """
    Extracts data from a paginated REST API.

    Authentication credentials are read from environment variables —
    never hardcoded or logged.

    Example usage:
        config = APIExtractorConfig(
            source_name="orders_api",
            base_url=os.environ["SOURCE_API_BASE_URL"],
            endpoint="/v1/orders",
            batch_size=500,
            incremental=True,
            watermark_column="updated_at",
        )
        extractor = APIExtractor(config)
        result = extractor.extract(since="2024-01-01T00:00:00Z")
    """

    def __init__(self, config: APIExtractorConfig):
        super().__init__(config)
        self.config: APIExtractorConfig = config
        self._session: Optional[requests.Session] = None

    def _connect(self) -> None:
        self._session = requests.Session()

        # Retry on transient HTTP errors
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

        # Attach auth headers — credentials from environment only
        self._session.headers.update(self._build_auth_headers())
        self._session.headers.update({"Accept": "application/json"})

        logger.info(
            "API session created",
            extra={"source": self.config.source_name, "endpoint": self.config.endpoint},
        )

    def _disconnect(self) -> None:
        if self._session:
            self._session.close()
            self._session = None

    def _build_auth_headers(self) -> dict:
        """
        Build authentication headers from environment variables.
        Credential keys follow the pattern: <SOURCE_NAME>_API_TOKEN
        """
        source_key = self.config.source_name.upper().replace("-", "_")

        if self.config.auth_type == "bearer":
            token = os.environ.get(f"{source_key}_API_TOKEN", "")
            return {"Authorization": f"Bearer {token}"}

        if self.config.auth_type == "api_key":
            api_key = os.environ.get(f"{source_key}_API_KEY", "")
            return {"X-API-Key": api_key}

        return {}

    def _fetch_batch(
        self,
        offset: int,
        limit: int,
        since: Optional[Any] = None,
    ) -> list[dict]:
        if self._session is None:
            raise RuntimeError("Session not initialised — call _connect() first")

        params: dict = {"limit": limit}

        if self.config.pagination_type == "offset":
            params["offset"] = offset
        elif self.config.pagination_type == "page":
            params["page"] = (offset // limit) + 1

        if since is not None:
            params[self.config.date_filter_param] = (
                since.isoformat() if isinstance(since, datetime) else str(since)
            )

        url = f"{self.config.base_url.rstrip('/')}/{self.config.endpoint.lstrip('/')}"
        response = self._session.get(
            url, params=params, timeout=self.config.timeout_seconds
        )
        response.raise_for_status()

        payload = response.json()
        records = payload.get(self.config.response_data_key, payload)

        if not isinstance(records, list):
            raise ValueError(
                f"Expected list under '{self.config.response_data_key}', "
                f"got {type(records).__name__}"
            )

        logger.debug(
            "Batch fetched",
            extra={
                "source": self.config.source_name,
                "offset": offset,
                "records_in_batch": len(records),
            },
        )

        return records
