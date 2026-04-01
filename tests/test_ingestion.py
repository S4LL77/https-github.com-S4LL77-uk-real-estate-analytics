"""
Tests for the Land Registry ingestion pipeline.

These tests use mocked HTTP responses to avoid hitting the real
Land Registry servers during CI. The test data mirrors the actual
CSV format (no headers, all fields quoted).

Run: python -m pytest tests/ -v
"""

import io
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.config import LAND_REGISTRY_COLUMNS
from ingestion.land_registry import add_metadata, download_csv, validate_dataframe


# =============================================================================
# Fixtures
# =============================================================================

# Sample CSV matching Land Registry format: no headers, all fields quoted
SAMPLE_CSV = (
    '"{12345-ABCDE}","250000","2024-03-15 00:00","SW1A 1AA","T","N","F",'
    '"10","","DOWNING STREET","","LONDON","CITY OF WESTMINSTER","GREATER LONDON","A","A"\n'
    '"{67890-FGHIJ}","450000","2024-06-20 00:00","EC2V 8AB","F","Y","L",'
    '"","FLAT 3","THREADNEEDLE STREET","","LONDON","CITY OF LONDON","GREATER LONDON","A","A"\n'
    '"{11111-KKKKK}","175000","2024-09-01 00:00","M1 1AA","S","N","F",'
    '"42","","PICCADILLY","","MANCHESTER","MANCHESTER","GREATER MANCHESTER","A","A"\n'
)


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Create a DataFrame from sample CSV, mimicking download_csv output."""
    df = pd.read_csv(
        io.StringIO(SAMPLE_CSV),
        header=None,
        names=LAND_REGISTRY_COLUMNS,
        quoting=1,
    )
    df["price_paid"] = pd.to_numeric(df["price_paid"], errors="coerce")
    return df


@pytest.fixture
def sample_df_with_metadata(sample_df: pd.DataFrame) -> pd.DataFrame:
    """Sample DataFrame with metadata columns added."""
    return add_metadata(sample_df, year=2024, batch_id="test-batch-001")


# =============================================================================
# Tests: Column Schema
# =============================================================================

class TestColumnSchema:
    """Verify the CSV column mapping matches expected Land Registry format."""

    def test_column_count(self):
        """Land Registry CSV has exactly 16 columns."""
        assert len(LAND_REGISTRY_COLUMNS) == 16

    def test_critical_columns_present(self):
        """Key analytical columns must exist."""
        required = ["transaction_id", "price_paid", "date_of_transfer", "postcode", "property_type"]
        for col in required:
            assert col in LAND_REGISTRY_COLUMNS, f"Missing critical column: {col}"

    def test_column_order(self):
        """First column must be transaction_id, second must be price_paid."""
        assert LAND_REGISTRY_COLUMNS[0] == "transaction_id"
        assert LAND_REGISTRY_COLUMNS[1] == "price_paid"
        assert LAND_REGISTRY_COLUMNS[2] == "date_of_transfer"


# =============================================================================
# Tests: CSV Parsing
# =============================================================================

class TestCSVParsing:
    """Verify correct parsing of the headerless, quoted CSV format."""

    def test_row_count(self, sample_df: pd.DataFrame):
        """Sample CSV has exactly 3 rows."""
        assert len(sample_df) == 3

    def test_column_names_applied(self, sample_df: pd.DataFrame):
        """Column names from config are applied correctly."""
        assert list(sample_df.columns) == LAND_REGISTRY_COLUMNS

    def test_price_is_numeric(self, sample_df: pd.DataFrame):
        """Price column is parsed as numeric, not string."""
        assert pd.api.types.is_numeric_dtype(sample_df["price_paid"])

    def test_price_values(self, sample_df: pd.DataFrame):
        """Prices are parsed correctly without quote artifacts."""
        assert sample_df["price_paid"].tolist() == [250_000, 450_000, 175_000]

    def test_transaction_id_format(self, sample_df: pd.DataFrame):
        """Transaction IDs include braces (Land Registry format)."""
        assert sample_df["transaction_id"].iloc[0] == "{12345-ABCDE}"

    def test_property_types(self, sample_df: pd.DataFrame):
        """Property type codes are single characters."""
        valid_types = {"D", "S", "T", "F", "O"}
        actual_types = set(sample_df["property_type"].unique())
        assert actual_types.issubset(valid_types)

    def test_postcode_parsed(self, sample_df: pd.DataFrame):
        """Postcodes are clean strings without extra quotes."""
        postcodes = sample_df["postcode"].tolist()
        assert "SW1A 1AA" in postcodes
        assert all('"' not in str(pc) for pc in postcodes)


# =============================================================================
# Tests: Metadata Columns
# =============================================================================

class TestMetadata:
    """Verify ingestion metadata is added correctly."""

    def test_metadata_columns_added(self, sample_df_with_metadata: pd.DataFrame):
        """Three metadata columns are added."""
        meta_cols = ["_ingested_at", "_source_file", "_batch_id"]
        for col in meta_cols:
            assert col in sample_df_with_metadata.columns, f"Missing metadata column: {col}"

    def test_source_file_format(self, sample_df_with_metadata: pd.DataFrame):
        """Source file follows pp-YYYY.csv pattern."""
        assert sample_df_with_metadata["_source_file"].iloc[0] == "pp-2024.csv"

    def test_batch_id_consistent(self, sample_df_with_metadata: pd.DataFrame):
        """All rows in a batch have the same batch_id."""
        assert sample_df_with_metadata["_batch_id"].nunique() == 1

    def test_ingested_at_is_iso(self, sample_df_with_metadata: pd.DataFrame):
        """_ingested_at is a valid ISO 8601 timestamp."""
        ts = sample_df_with_metadata["_ingested_at"].iloc[0]
        # Should not raise
        datetime.fromisoformat(ts)

    def test_original_data_unchanged(self, sample_df: pd.DataFrame, sample_df_with_metadata: pd.DataFrame):
        """Adding metadata doesn't modify original columns."""
        for col in LAND_REGISTRY_COLUMNS:
            assert col in sample_df_with_metadata.columns
            pd.testing.assert_series_equal(
                sample_df[col].reset_index(drop=True),
                sample_df_with_metadata[col].reset_index(drop=True),
                check_names=False,
            )


# =============================================================================
# Tests: Validation
# =============================================================================

class TestValidation:
    """Verify pre-bronze quality checks catch problems."""

    def test_valid_data_passes(self, sample_df_with_metadata: pd.DataFrame):
        """Clean data should pass all checks."""
        assert validate_dataframe(sample_df_with_metadata, year=2024) is True

    def test_empty_dataframe_fails(self):
        """Empty DataFrame should fail validation."""
        empty_df = pd.DataFrame(columns=LAND_REGISTRY_COLUMNS + ["_ingested_at", "_source_file", "_batch_id"])
        assert validate_dataframe(empty_df, year=2024) is False

    def test_mostly_null_critical_column_fails(self, sample_df_with_metadata: pd.DataFrame):
        """Column with >50% nulls in critical fields should fail."""
        df = sample_df_with_metadata.copy()
        df.loc[:, "transaction_id"] = None  # 100% null
        assert validate_dataframe(df, year=2024) is False


# =============================================================================
# Tests: Download with mocked HTTP
# =============================================================================

class TestDownload:
    """Test download logic with mocked HTTP responses."""

    @patch("ingestion.land_registry.requests.get")
    def test_successful_download(self, mock_get: MagicMock):
        """Successful download returns a DataFrame with correct shape."""
        # Mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [SAMPLE_CSV.encode("utf-8")]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        df = download_csv(2024)

        assert len(df) == 3
        assert list(df.columns) == LAND_REGISTRY_COLUMNS
        mock_get.assert_called_once()

    @patch("ingestion.land_registry.requests.get")
    @patch("ingestion.land_registry.time.sleep")  # Don't actually sleep in tests
    def test_retry_on_failure(self, mock_sleep: MagicMock, mock_get: MagicMock):
        """Failed request should retry with backoff."""
        import requests as req

        # First call fails, second succeeds
        mock_fail = MagicMock()
        mock_fail.raise_for_status.side_effect = req.HTTPError("503 Server Error")

        mock_success = MagicMock()
        mock_success.status_code = 200
        mock_success.iter_content.return_value = [SAMPLE_CSV.encode("utf-8")]
        mock_success.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_fail, mock_success]

        df = download_csv(2024)

        assert len(df) == 3
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once()  # Backoff between retries
