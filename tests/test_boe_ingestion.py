"""
Tests for the Bank of England rates ingestion pipeline.

Run: python -m pytest tests/test_boe_ingestion.py -v
"""

from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from ingestion.boe_rates import download_rates_csv, validate_dataframe, add_metadata

# Sample CSV mimicking BoE format
SAMPLE_BOE_CSV = (
    "Date,IUMABEDR\n"
    "01 Jan 2024,5.25\n"
    "15 Feb 2024,5.25\n"
    "02 Aug 2024,5.00\n"
)

class TestBoEIngestion:
    
    @patch("ingestion.boe_rates.requests.get")
    def test_successful_download(self, mock_get: MagicMock):
        """Successful download parses correctly into DataFrame."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = SAMPLE_BOE_CSV.encode("utf-8")
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        df = download_rates_csv()
        
        # 3 rate changes
        assert len(df) == 3
        # Ensure column renaming worked
        assert list(df.columns) == ["rate_date", "rate_value"]
        
        # Verify types
        assert pd.api.types.is_numeric_dtype(df["rate_value"])
        assert pd.api.types.is_datetime64_any_dtype(df["rate_date"])
        
        # Verify exact parse values
        assert df["rate_value"].iloc[2] == 5.00
        
    def test_add_metadata(self):
        """Check metadata columns logic."""
        df = pd.DataFrame({
            "rate_date": [pd.Timestamp("2024-01-01")], 
            "rate_value": [5.25]
        })
        df_meta = add_metadata(df, "test-batch-001")
        
        assert "_ingested_at" in df_meta.columns
        assert "_source_file" in df_meta.columns
        assert "_batch_id" in df_meta.columns
        assert df_meta["_batch_id"].iloc[0] == "test-batch-001"

    def test_validate_dataframe_passes_on_good_data(self):
        df = pd.DataFrame({
            "rate_date": [pd.Timestamp("2024-01-01")], 
            "rate_value": [5.25]
        })
        assert validate_dataframe(df) is True

    def test_validate_dataframe_fails_on_empty(self):
        df = pd.DataFrame(columns=["rate_date", "rate_value"])
        assert validate_dataframe(df) is False

    def test_validate_dataframe_fails_on_out_of_bounds(self):
        # UK rate is never 25% or -1%
        df = pd.DataFrame({
            "rate_date": [pd.Timestamp("2024-01-01")], 
            "rate_value": [25.0]
        })
        assert validate_dataframe(df) is False
