"""
Data transformer — schema normalization, type coercion, and field standardisation.
Pure functions where possible; no I/O side effects.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)


DTYPE_MAP = {
    "string":    "object",
    "integer":   "Int64",       # nullable integer
    "float":     "float64",
    "boolean":   "boolean",     # nullable boolean
    "timestamp": "datetime64[ns, UTC]",
    "date":      "object",
}


class SchemaField:
    def __init__(
        self,
        name: str,
        dtype: str,
        nullable: bool = True,
        rename: Optional[str] = None,
        default: Any = None,
    ):
        self.name = name
        self.dtype = dtype
        self.nullable = nullable
        self.rename = rename or name
        self.default = default


class DataTransformer:
    """
    Applies a schema contract to a raw DataFrame.

    Responsibilities:
    - Rename columns to snake_case canonical names
    - Cast to target types, handling nulls gracefully
    - Fill defaults for missing fields
    - Append pipeline metadata columns (_loaded_at, _source)
    - Log schema drift (unexpected columns, type widening)
    """

    def __init__(self, schema: list[SchemaField], source_name: str):
        self.schema = {f.name: f for f in schema}
        self.source_name = source_name

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        df = self._coerce_columns(df)
        df = self._add_metadata(df)
        self._validate_nulls(df)

        logger.info(
            "Transform complete",
            extra={
                "source": self.source_name,
                "rows": len(df),
                "columns": list(df.columns),
            },
        )
        return df

    def _coerce_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        result_frames: dict[str, pd.Series] = {}
        incoming_cols = set(df.columns)
        expected_cols = set(self.schema.keys())

        drift = incoming_cols - expected_cols
        if drift:
            logger.warning(
                "Schema drift detected — unexpected columns",
                extra={"source": self.source_name, "extra_columns": sorted(drift)},
            )

        for field_name, field in self.schema.items():
            if field_name in df.columns:
                series = df[field_name]
            elif field.default is not None:
                series = pd.Series([field.default] * len(df), name=field_name)
            else:
                series = pd.Series([pd.NA] * len(df), name=field_name)

            series = self._cast_series(series, field)
            result_frames[field.rename] = series

        return pd.DataFrame(result_frames)

    def _cast_series(self, series: pd.Series, field: SchemaField) -> pd.Series:
        try:
            if field.dtype == "timestamp":
                return pd.to_datetime(series, utc=True, errors="coerce")
            if field.dtype == "date":
                return pd.to_datetime(series, errors="coerce").dt.date.astype("object")
            if field.dtype in ("integer", "float"):
                return pd.to_numeric(series, errors="coerce").astype(
                    DTYPE_MAP[field.dtype]
                )
            if field.dtype == "boolean":
                return series.map(
                    lambda v: None if pd.isna(v) else bool(v)
                ).astype("boolean")
            return series.astype("object")
        except Exception as exc:
            logger.warning(
                "Type coercion failed",
                extra={
                    "source": self.source_name,
                    "field": field.name,
                    "target_dtype": field.dtype,
                    "error": str(exc),
                },
            )
            return series

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        df["_loaded_at"] = datetime.now(tz=timezone.utc)
        df["_source"] = self.source_name
        return df

    def _validate_nulls(self, df: pd.DataFrame) -> None:
        for field_name, field in self.schema.items():
            target_col = field.rename
            if not field.nullable and target_col in df.columns:
                null_count = df[target_col].isna().sum()
                if null_count > 0:
                    logger.error(
                        "NOT NULL violation",
                        extra={
                            "source": self.source_name,
                            "column": target_col,
                            "null_count": int(null_count),
                        },
                    )


def snake_case(text: str) -> str:
    """Convert camelCase or PascalCase to snake_case."""
    import re
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
    text = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", text)
    return text.replace("-", "_").lower()


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all DataFrame columns to snake_case."""
    return df.rename(columns={col: snake_case(col) for col in df.columns})
