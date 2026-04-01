"""
Bank of England — Official Bank Rate Ingestion

Fetches the historical base interest rate (Official Bank Rate) from the 
Bank of England's Interactive Statistical Database (IADB).
The rate changes irregularly, so we ingest the full history and 
update incrementally in downstream dbt slowly-changing dimensions.

Data source: https://www.bankofengland.co.uk/boeapps/iadb/
Series code: IUMABEDR
License: Open Government Licence (OGL)

Usage:
    python -m ingestion.boe_rates
"""

import io
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests

from ingestion.config import (
    BOE_RATES_URL,
    MAX_RETRIES,
    RETRY_BASE_DELAY_SECONDS,
)
from ingestion.utils.logging_config import get_logger
from ingestion.utils.s3_client import save_to_bronze

logger = get_logger(__name__)


def download_rates_csv() -> pd.DataFrame:
    """
    Download the Official Bank Rate history as CSV from the BoE API.

    Implements retry logic with exponential backoff.
    
    Returns:
        DataFrame containing 'rate_date' and 'rate_value' columns.
        
    Raises:
        requests.HTTPError: If the download fails after all retries.
    """
    logger.info("Downloading Bank of England interest rates", extra={"source": "boe_rates"})

    last_exception: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start_time = time.time()
            # The BoE block simple scripts, so we pretend to be a standard browser
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            response = requests.get(BOE_RATES_URL, timeout=30, headers=headers)
            response.raise_for_status()

            duration = time.time() - start_time
            
            # The BoE CSV has headers: "Date" and "IUMABEDR"
            df = pd.read_csv(
                io.BytesIO(response.content),
                encoding="utf-8",
                # The returned format has names Date and IUMABEDR, skip 1 assuming standard CSV.
                header=0,
                names=["rate_date", "rate_value"],
                parse_dates=["rate_date"],
            )
            
            # Rate formatting, handle potential parsing errors in poorly formatted BoE rows
            df["rate_value"] = pd.to_numeric(df["rate_value"], errors="coerce")
            
            # Clean up out-of-bounds or completely null data
            df = df.dropna(subset=["rate_value", "rate_date"])

            logger.info(
                f"Parsed {len(df):,} interest rate changes in {duration:.1f}s",
                extra={"source": "boe_rates", "row_count": len(df), "duration_seconds": round(duration, 1)},
            )

            return df

        except (requests.RequestException, pd.errors.ParserError) as e:
            last_exception = e
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY_SECONDS ** (attempt + 1)
                logger.warning(
                    f"Attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {delay}s...",
                    extra={"source": "boe_rates"},
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"All {MAX_RETRIES} attempts failed for BoE rates",
                    extra={"source": "boe_rates"},
                )

    raise last_exception  # type: ignore[misc]


def add_metadata(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """
    Add ingestion metadata columns to the DataFrame.
    """
    df = df.copy()
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source_file"] = "boe_iadb_iumabedr.csv"
    df["_batch_id"] = batch_id
    return df


def validate_dataframe(df: pd.DataFrame) -> bool:
    """
    Basic quality checks on the downloaded data.
    """
    checks_passed = True

    if len(df) == 0:
        logger.error("Empty DataFrame for BoE rates", extra={"source": "boe_rates"})
        checks_passed = False

    if df["rate_value"].isna().any():
         logger.error("Detected null rate values in BoE data", extra={"source": "boe_rates"})
         checks_passed = False

    if df["rate_value"].max() > 20 or df["rate_value"].min() < 0:
         logger.warning("Unusual base rate limit breached (<0 or >20)", extra={"source": "boe_rates"})
         checks_passed = False
         
    if checks_passed:
        logger.info("All pre-bronze quality checks passed for BoE rates", extra={"source": "boe_rates"})

    return checks_passed


def run() -> dict:
    """
    Run the BoE ingestion pipeline.
    Unlike Land Registry, this dataset is extremely small (<100kb),
    so we can just perform a full refresh of the data each time.

    Returns:
        Summary dict with counts and paths for logging/alerting.
    """
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    results = {"ingested": [], "failed": []}
    
    try:
        start_time = time.time()
        
        # 1. Download & Parse
        df = download_rates_csv()
        
        # 2. Add metadata
        df = add_metadata(df, batch_id)
        
        # 3. Validate
        validate_dataframe(df)
        
        # 4. Save to Bronze
        # We don't partition because data is tiny; we just overwrite
        # the single parquet file each time.
        output_path = save_to_bronze(
            df=df,
            source_name="boe_rates",
            partition_key="dataset",
            partition_value="historical",
            file_name="data.parquet"
        )
        
        duration = time.time() - start_time
        logger.info(
            f"Successfully ingested BoE rates: {len(df):,} rows in {duration:.1f}s",
            extra={
                "source": "boe_rates",
                "row_count": len(df),
                "duration_seconds": round(duration, 1),
                "file_path": output_path,
            },
        )
        
        results["ingested"].append({"path": output_path, "rows": len(df)})
        
    except Exception as e:
        logger.error(f"Failed to ingest BoE rates: {e}", extra={"source": "boe_rates"})
        results["failed"].append({"error": str(e)})

    return results


if __name__ == "__main__":
    results = run()
    if results["failed"]:
        raise SystemExit(1)
