"""
Base extractor with retry logic, structured logging, and watermark management.
All source-specific extractors inherit from this class.
"""

import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Generator, Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ExtractorConfig:
    source_name: str
    batch_size: int = 10_000
    max_retries: int = 3
    retry_backoff_seconds: float = 2.0
    timeout_seconds: int = 300
    incremental: bool = True
    watermark_column: str = "updated_at"
    extra: dict = field(default_factory=dict)


@dataclass
class ExtractResult:
    records: list[dict]
    source_name: str
    extracted_at: datetime
    row_count: int
    watermark_value: Optional[Any] = None
    errors: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.records)


class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors.

    Implements:
    - Retry with exponential backoff
    - Watermark-based incremental extraction
    - Structured logging for pipeline observability
    - Batch iteration for memory-efficient processing
    """

    def __init__(self, config: ExtractorConfig):
        self.config = config
        self._watermark: Optional[Any] = None

    @abstractmethod
    def _connect(self) -> None:
        """Establish connection to the source system."""
        ...

    @abstractmethod
    def _disconnect(self) -> None:
        """Cleanly close the source connection."""
        ...

    @abstractmethod
    def _fetch_batch(
        self,
        offset: int,
        limit: int,
        since: Optional[Any] = None,
    ) -> list[dict]:
        """Fetch a single batch of records from the source."""
        ...

    def extract(self, since: Optional[Any] = None) -> ExtractResult:
        """
        Execute a full extraction run with retries.

        Args:
            since: Watermark value — only fetch records updated after this.
                   If None and config.incremental is True, uses stored watermark.
        """
        effective_since = since or (self._watermark if self.config.incremental else None)

        logger.info(
            "Starting extraction",
            extra={
                "source": self.config.source_name,
                "incremental": self.config.incremental,
                "since": str(effective_since),
            },
        )

        all_records: list[dict] = []
        errors: list[str] = []
        new_watermark = effective_since

        try:
            self._connect()
            for batch in self._extract_with_retry(effective_since):
                all_records.extend(batch)
                if batch and self.config.watermark_column in batch[-1]:
                    new_watermark = batch[-1][self.config.watermark_column]

        except Exception as exc:
            msg = f"Extraction failed after retries: {exc}"
            logger.error(msg, extra={"source": self.config.source_name})
            errors.append(msg)
        finally:
            self._disconnect()

        result = ExtractResult(
            records=all_records,
            source_name=self.config.source_name,
            extracted_at=datetime.utcnow(),
            row_count=len(all_records),
            watermark_value=new_watermark,
            errors=errors,
        )

        logger.info(
            "Extraction complete",
            extra={
                "source": self.config.source_name,
                "row_count": result.row_count,
                "new_watermark": str(new_watermark),
                "has_errors": result.has_errors,
            },
        )

        # Advance watermark only on clean runs
        if not result.has_errors and new_watermark:
            self._watermark = new_watermark

        return result

    def _extract_with_retry(
        self, since: Optional[Any]
    ) -> Generator[list[dict], None, None]:
        """Yield batches, retrying each batch on transient failures."""
        offset = 0
        while True:
            batch = self._fetch_with_retry(offset, self.config.batch_size, since)
            if not batch:
                break
            yield batch
            if len(batch) < self.config.batch_size:
                break  # Last page
            offset += len(batch)

    def _fetch_with_retry(
        self, offset: int, limit: int, since: Optional[Any]
    ) -> list[dict]:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.config.max_retries + 1):
            try:
                return self._fetch_batch(offset, limit, since)
            except Exception as exc:
                last_exc = exc
                wait = self.config.retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Batch fetch failed, retrying",
                    extra={
                        "source": self.config.source_name,
                        "attempt": attempt,
                        "max_retries": self.config.max_retries,
                        "wait_seconds": wait,
                        "error": str(exc),
                    },
                )
                time.sleep(wait)
        raise RuntimeError(
            f"Batch fetch failed after {self.config.max_retries} attempts"
        ) from last_exc
