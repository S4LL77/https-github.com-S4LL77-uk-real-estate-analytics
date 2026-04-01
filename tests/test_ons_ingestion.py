"""
Tests for the ONS demographics ingestion pipeline.

Run: python -m pytest tests/test_ons_ingestion.py -v
"""

from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from ingestion.ons_demographics import download_ons_csv, validate_dataframe, get_latest_csv_url

# Sample CSV mimicking ONS API payload format (tidy format)
SAMPLE_ONS_CSV = (
    "v4_0,time,uk-only,geography,cpih1dim1aggid\n"
    "131.7,2024-01,K02000001,K02000001,cpih01\n"
    "132.8,2024-02,K02000001,K02000001,cpih01\n"
)

# Mocked responses for the JSON API navigation process.
MOCK_METADATA_RESPONSE = {
    "links": {
        "latest_version": {
            "href": "http://mock-api.com/v1/datasets/cpih01/editions/time-series/versions/1"
        }
    }
}

MOCK_VERSION_RESPONSE = {
    "downloads": {
        "csv": {
            "href": "http://mock-api.com/download.csv"
        }
    }
}

class TestONSIngestion:
    
    @patch("ingestion.ons_demographics.requests.get")
    def test_get_latest_csv_url(self, mock_get: MagicMock):
        """Test API navigation logic to find the CSV URL."""
        
        # We need the mock to return metadata the first time, and version data the second time.
        mock_meta_call = MagicMock()
        mock_meta_call.status_code = 200
        mock_meta_call.json.return_value = MOCK_METADATA_RESPONSE
        mock_meta_call.raise_for_status = MagicMock()
        
        mock_version_call = MagicMock()
        mock_version_call.status_code = 200
        mock_version_call.json.return_value = MOCK_VERSION_RESPONSE
        mock_version_call.raise_for_status = MagicMock()
        
        mock_get.side_effect = [mock_meta_call, mock_version_call]
        
        url = get_latest_csv_url("cpih01")
        
        # It should correctly traverse the links to output the CSV href
        assert url == "http://mock-api.com/download.csv"
        assert mock_get.call_count == 2
        

    @patch("ingestion.ons_demographics.requests.get")
    def test_successful_csv_download(self, mock_get: MagicMock):
        """Test actual CSV ingestion."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = SAMPLE_ONS_CSV.encode("utf-8")
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        df = download_ons_csv("http://mock-api.com/download.csv")
        
        # 2 records in sample data
        assert len(df) == 2
        assert "v4_0" in df.columns
        assert df["v4_0"].iloc[0] == 131.7
        

    def test_validate_dataframe_passes_on_standard_ons_columns(self):
        """ONS files change a lot, but usually have v4_0 and time columns."""
        df = pd.DataFrame({
            "v4_0": [131.7], 
            "time": ["2024-01"],
            "uk-only": ["K02000001"]
        })
        assert validate_dataframe(df) is True

    def test_validate_dataframe_fails_on_empty(self):
        df = pd.DataFrame(columns=["v4_0"])
        assert validate_dataframe(df) is False
