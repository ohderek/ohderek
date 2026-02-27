"""Unit tests for the DataTransformer."""

import pandas as pd
import pytest

from src.transformers.data_transformer import DataTransformer, SchemaField, snake_case, normalize_column_names


ORDERS_SCHEMA = [
    SchemaField("id",          "integer",   nullable=False, rename="order_id"),
    SchemaField("customerId",  "integer",   nullable=False, rename="customer_id"),
    SchemaField("status",      "string",    nullable=False),
    SchemaField("totalAmount", "float",     nullable=True,  rename="total_amount"),
    SchemaField("currency",    "string",    nullable=False, default="USD"),
    SchemaField("createdAt",   "timestamp", nullable=False, rename="created_at"),
]


@pytest.fixture
def transformer():
    return DataTransformer(schema=ORDERS_SCHEMA, source_name="test_source")


@pytest.fixture
def valid_raw_df():
    return pd.DataFrame([
        {
            "id": 1,
            "customerId": 42,
            "status": "completed",
            "totalAmount": 99.99,
            "currency": "USD",
            "createdAt": "2024-06-01T10:00:00Z",
        },
        {
            "id": 2,
            "customerId": 43,
            "status": "pending",
            "totalAmount": 14.50,
            "currency": "EUR",
            "createdAt": "2024-06-01T11:30:00Z",
        },
    ])


class TestTransformColumns:
    def test_renames_columns_correctly(self, transformer, valid_raw_df):
        result = transformer.transform(valid_raw_df)
        assert "order_id" in result.columns
        assert "customer_id" in result.columns
        assert "total_amount" in result.columns
        assert "created_at" in result.columns
        # Original camelCase names should be gone
        assert "id" not in result.columns
        assert "customerId" not in result.columns

    def test_row_count_preserved(self, transformer, valid_raw_df):
        result = transformer.transform(valid_raw_df)
        assert len(result) == len(valid_raw_df)

    def test_metadata_columns_appended(self, transformer, valid_raw_df):
        result = transformer.transform(valid_raw_df)
        assert "_loaded_at" in result.columns
        assert "_source" in result.columns
        assert (result["_source"] == "test_source").all()

    def test_integer_type_coercion(self, transformer, valid_raw_df):
        result = transformer.transform(valid_raw_df)
        assert result["order_id"].dtype.name == "Int64"

    def test_float_type_coercion(self, transformer, valid_raw_df):
        result = transformer.transform(valid_raw_df)
        assert result["total_amount"].dtype == "float64"

    def test_timestamp_coercion_to_utc(self, transformer, valid_raw_df):
        result = transformer.transform(valid_raw_df)
        assert str(result["created_at"].dtype) == "datetime64[ns, UTC]"

    def test_missing_column_uses_default(self, transformer):
        df_no_currency = pd.DataFrame([
            {"id": 3, "customerId": 50, "status": "new",
             "totalAmount": 5.0, "createdAt": "2024-06-01T12:00:00Z"}
        ])
        result = transformer.transform(df_no_currency)
        assert result["currency"].iloc[0] == "USD"

    def test_empty_dataframe_returns_empty(self, transformer):
        result = transformer.transform(pd.DataFrame())
        assert result.empty


class TestNullHandling:
    def test_nullable_float_accepts_none(self, transformer):
        df = pd.DataFrame([
            {"id": 1, "customerId": 1, "status": "new",
             "totalAmount": None, "currency": "USD", "createdAt": "2024-01-01T00:00:00Z"}
        ])
        result = transformer.transform(df)
        assert pd.isna(result["total_amount"].iloc[0])

    def test_non_nullable_field_logs_error(self, transformer, caplog):
        df = pd.DataFrame([
            {"id": None, "customerId": 1, "status": "new",
             "totalAmount": 10.0, "currency": "USD", "createdAt": "2024-01-01T00:00:00Z"}
        ])
        import logging
        with caplog.at_level(logging.ERROR):
            transformer.transform(df)
        assert any("NOT NULL violation" in r.message for r in caplog.records)


class TestSnakeCase:
    @pytest.mark.parametrize("input_str, expected", [
        ("camelCase",      "camel_case"),
        ("PascalCase",     "pascal_case"),
        ("snake_case",     "snake_case"),
        ("XMLParser",      "xml_parser"),
        ("someHTTPSUrl",   "some_https_url"),
        ("already_snake",  "already_snake"),
    ])
    def test_snake_case_conversion(self, input_str, expected):
        assert snake_case(input_str) == expected


class TestNormalizeColumnNames:
    def test_renames_all_columns(self):
        df = pd.DataFrame(columns=["orderId", "CustomerId", "totalAmount"])
        result = normalize_column_names(df)
        assert list(result.columns) == ["order_id", "customer_id", "total_amount"]
