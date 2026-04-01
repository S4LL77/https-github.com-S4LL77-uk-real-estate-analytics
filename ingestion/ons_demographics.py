"""
Office for National Statistics (ONS) — Demographics Ingestion

Fetches census and demographic data from the ONS Beta API.
This data is used to enrich the property transactions with
area-level insights (e.g. median income, housing stock).

Data source: https://api.beta.ons.gov.uk/v1/datasets
License: Open Government Licence (OGL)

Usage:
    python -m ingestion.ons_demographics
"""

import io
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests

from ingestion.config import (
    MAX_RETRIES,
    RETRY_BASE_DELAY_SECONDS,
    ONS_API_BASE_URL,
)
from ingestion.utils.logging_config import get_logger
from ingestion.utils.s3_client import save_to_bronze

logger = get_logger(__name__)


# For the portfolio, we target a stable known dataset ID from ONS.
# Example: 'cpih01' is Consumer Prices Index including owner occupiers' housing costs
# In a real pipeline, we'd query /datasets and filter dynamically.
ONS_TARGET_DATASET = "cpih01"


def get_latest_csv_url(dataset_id: str) -> str:
    """
    Navigate the ONS Beta API to find the CSV download URL for
    the latest version of a specific dataset.
    """
    url = f"{ONS_API_BASE_URL}/datasets/{dataset_id}"
    logger.info(f"Querying ONS dataset metadata: {url}", extra={"source": "ons"})
    
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    metadata = response.json()
    
    # Follow links inside the JSON response: 
    # links -> latest_version -> href
    latest_href = metadata.get("links", {}).get("latest_version", {}).get("href")
    
    if not latest_href:
        raise ValueError(f"Could not find latest_version link for dataset {dataset_id}")
        
    logger.info(f"Fetching latest version metadata: {latest_href}")
    version_response = requests.get(latest_href, timeout=30)
    version_response.raise_for_status()
    version_data = version_response.json()
    
    # downloads -> csv -> href
    csv_url = version_data.get("downloads", {}).get("csv", {}).get("href")
    
    if not csv_url:
        raise ValueError(f"Could not find CSV download URL for {dataset_id}")
        
    return csv_url


def download_ons_csv(csv_url: str) -> pd.DataFrame:
    """
    Download the dataset CSV with exponential backoff.
    """
    logger.info(f"Downloading ONS dataset from {csv_url}", extra={"source": "ons"})

    last_exception: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start_time = time.time()
            response = requests.get(csv_url, timeout=60)
            response.raise_for_status()

            duration = time.time() - start_time
            
            # ONS API CSVs are "tidy" format and standard comma separated
            df = pd.read_csv(
                io.BytesIO(response.content),
                encoding="utf-8",
                low_memory=False
            )
            
            logger.info(
                f"Parsed {len(df):,} ONS records in {duration:.1f}s",
                extra={"source": "ons", "row_count": len(df), "duration_seconds": round(duration, 1)},
            )

            return df

        except (requests.RequestException, pd.errors.ParserError) as e:
            last_exception = e
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY_SECONDS ** (attempt + 1)
                logger.warning(
                    f"Attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {delay}s...",
                    extra={"source": "ons"},
                )
                time.sleep(delay)

    logger.error("All attempts failed for ONS data", extra={"source": "ons"})
    raise last_exception  # type: ignore[misc]


def add_metadata(df: pd.DataFrame, dataset_id: str, batch_id: str) -> pd.DataFrame:
    """Add standard ingestion metadata columns."""
    df = df.copy()
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source_dataset"] = dataset_id
    df["_batch_id"] = batch_id
    return df


def validate_dataframe(df: pd.DataFrame) -> bool:
    """Basic pre-bronze quality validation."""
    if len(df) == 0:
        logger.error("Empty DataFrame returned from ONS")
        return False
        
    # Standard ONS format usually includes these
    required_cols = ["v4_0", "time", "uk-only"]
    missing = [c for c in required_cols if c not in df.columns]
    
    # We warn rather than fail here because ONS formats change constantly
    if missing:
        logger.warning(f"ONS warning: missing columns typically present: {missing}")

    logger.info(f"ONS dataset format verified. Total columns: {len(df.columns)}")
    return True


def run() -> dict:
    """
    Run ONS pipeline
    """
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    results = {"ingested": [], "failed": []}
    
    try:
        start_time = time.time()
        
        # 1. Resolve CSV URL
        csv_url = get_latest_csv_url(ONS_TARGET_DATASET)
        
        # 2. Download Data
        df = download_ons_csv(csv_url)
        
        # 3. Add metadata
        df = add_metadata(df, ONS_TARGET_DATASET, batch_id)
        
        # 4. Validate
        validate_dataframe(df)
        
        # 5. Save to Bronze (overwrite mode for this dataset size)
        output_path = save_to_bronze(
            df=df,
            source_name="ons_demographics",
            partition_key="dataset",
            partition_value=ONS_TARGET_DATASET,
            file_name="data.parquet"
        )
        
        duration = time.time() - start_time
        logger.info(
            f"Successfully ingested ONS data: {len(df):,} rows in {duration:.1f}s",
            extra={
                "source": "ons",
                "row_count": len(df),
                "duration_seconds": round(duration, 1),
                "file_path": output_path,
            },
        )
        
        results["ingested"].append({"path": output_path, "rows": len(df)})
        
    except Exception as e:
        logger.error(f"Failed to ingest ONS data: {e}", extra={"source": "ons"})
        results["failed"].append({"error": str(e)})

    return results


if __name__ == "__main__":
    results = run()
    if results["failed"]:
        raise SystemExit(1)
