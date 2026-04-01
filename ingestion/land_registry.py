"""
HM Land Registry — Price Paid Data Ingestion

Downloads bulk CSV files from the Land Registry's public S3 bucket,
applies column headers (the raw files have none), adds ingestion
metadata, and saves to the bronze layer as Parquet.

Data source: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
License: Open Government Licence (OGL)

Usage:
    # Ingest a single year (default: 2024)
    python -m ingestion.land_registry

    # Ingest specific years
    LAND_REGISTRY_YEARS=2022,2023,2024 python -m ingestion.land_registry

    # Full refresh (re-download even if partition exists)
    python -m ingestion.land_registry --full-refresh
"""

import argparse
import io
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests

from ingestion.config import (
    DOWNLOAD_CHUNK_SIZE,
    LAND_REGISTRY_COLUMNS,
    LAND_REGISTRY_DTYPES,
    LAND_REGISTRY_YEARLY_URL,
    LAND_REGISTRY_YEARS,
    MAX_RETRIES,
    RETRY_BASE_DELAY_SECONDS,
)
from ingestion.utils.logging_config import get_logger
from ingestion.utils.s3_client import list_existing_partitions, save_to_bronze

logger = get_logger(__name__)


def download_csv(year: int) -> pd.DataFrame:
    """
    Download a yearly Price Paid Data CSV from Land Registry.

    Implements retry logic with exponential backoff (2s → 8s → 32s).
    The raw CSV has no headers, so we assign column names from config.

    Args:
        year: The year to download (e.g., 2024).

    Returns:
        DataFrame with proper column names and types.

    Raises:
        requests.HTTPError: If download fails after all retries.
    """
    url = LAND_REGISTRY_YEARLY_URL.format(year=year)
    logger.info(f"Downloading Land Registry data for {year}", extra={"source": "land_registry", "year": year})

    last_exception: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start_time = time.time()

            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()

            # Stream into memory in chunks for progress visibility
            chunks = []
            bytes_downloaded = 0
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                chunks.append(chunk)
                bytes_downloaded += len(chunk)

            content = b"".join(chunks)
            duration = time.time() - start_time

            logger.info(
                f"Downloaded {bytes_downloaded / (1024*1024):.1f} MB in {duration:.1f}s",
                extra={"source": "land_registry", "year": year, "duration_seconds": round(duration, 1)},
            )

            # Parse CSV — no headers in raw file
            df = pd.read_csv(
                io.BytesIO(content),
                header=None,
                names=LAND_REGISTRY_COLUMNS,
                dtype={k: v for k, v in LAND_REGISTRY_DTYPES.items() if v != "int64"},
                quoting=1,  # QUOTE_ALL — Land Registry wraps all fields in quotes
                encoding="utf-8",
                low_memory=False,
            )

            # Convert price_paid to numeric (handles any edge cases)
            df["price_paid"] = pd.to_numeric(df["price_paid"], errors="coerce")

            logger.info(
                f"Parsed {len(df):,} transactions for {year}",
                extra={"source": "land_registry", "year": year, "row_count": len(df)},
            )

            return df

        except (requests.RequestException, pd.errors.ParserError) as e:
            last_exception = e
            if attempt < MAX_RETRIES:
                # Exponential backoff: 2^(attempt*2) seconds → 4s, 16s, 64s
                delay = RETRY_BASE_DELAY_SECONDS ** (attempt + 1)
                logger.warning(
                    f"Attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {delay}s...",
                    extra={"source": "land_registry", "year": year},
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"All {MAX_RETRIES} attempts failed for year {year}",
                    extra={"source": "land_registry", "year": year},
                )

    raise last_exception  # type: ignore[misc]


def add_metadata(df: pd.DataFrame, year: int, batch_id: str) -> pd.DataFrame:
    """
    Add ingestion metadata columns to the DataFrame.

    These columns enable:
    - _ingested_at: Audit trail — when was this data landed?
    - _source_file: Lineage — which file did this row come from?
    - _batch_id: Idempotency — which pipeline run produced this?

    This is a best practice for any data lake: raw data + metadata
    about how it arrived. Interviewers love asking about this.
    """
    df = df.copy()
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source_file"] = f"pp-{year}.csv"
    df["_batch_id"] = batch_id

    return df


def validate_dataframe(df: pd.DataFrame, year: int) -> bool:
    """
    Basic quality checks on the downloaded data.

    These are pre-bronze checks — just enough to catch download
    corruption or schema changes. Full quality checks happen in
    the silver layer with Great Expectations / dbt tests.

    Returns:
        True if all checks pass, False otherwise.
    """
    checks_passed = True

    # Check 1: Non-empty
    if len(df) == 0:
        logger.error(f"Empty DataFrame for year {year}", extra={"year": year})
        checks_passed = False

    # Check 2: Expected column count
    expected_cols = len(LAND_REGISTRY_COLUMNS) + 3  # +3 for metadata columns
    if len(df.columns) != expected_cols:
        logger.error(
            f"Column count mismatch: expected {expected_cols}, got {len(df.columns)}",
            extra={"year": year},
        )
        checks_passed = False

    # Check 3: No fully null critical columns
    critical_columns = ["transaction_id", "price_paid", "date_of_transfer", "postcode"]
    for col in critical_columns:
        null_pct = df[col].isna().mean()
        if null_pct > 0.5:  # >50% nulls = something is wrong
            logger.error(
                f"Column '{col}' is {null_pct:.0%} null — possible parse error",
                extra={"year": year},
            )
            checks_passed = False

    # Check 4: Price sanity check
    if df["price_paid"].notna().any():
        median_price = df["price_paid"].median()
        if median_price < 1000 or median_price > 10_000_000:
            logger.warning(
                f"Unusual median price: £{median_price:,.0f} — verify data",
                extra={"year": year},
            )

    if checks_passed:
        logger.info(f"All pre-bronze quality checks passed for {year}", extra={"year": year})

    return checks_passed


def ingest_year(year: int, batch_id: str) -> str:
    """
    Full ingestion pipeline for a single year of Land Registry data.

    Steps:
        1. Download CSV from Land Registry
        2. Apply column headers
        3. Add metadata columns
        4. Validate basic quality
        5. Save as Parquet to bronze layer

    Args:
        year: Year to ingest.
        batch_id: Unique identifier for this pipeline run.

    Returns:
        Path where the Parquet file was saved.
    """
    start_time = time.time()

    # Download and parse
    df = download_csv(year)

    # Add metadata
    df = add_metadata(df, year, batch_id)

    # Validate
    validate_dataframe(df, year)

    # Save to bronze layer
    output_path = save_to_bronze(
        df=df,
        source_name="land_registry",
        partition_key="year",
        partition_value=str(year),
    )

    duration = time.time() - start_time
    logger.info(
        f"Completed ingestion for {year}: {len(df):,} rows in {duration:.1f}s",
        extra={
            "source": "land_registry",
            "year": year,
            "row_count": len(df),
            "duration_seconds": round(duration, 1),
            "file_path": output_path,
        },
    )

    return output_path


def run(years: list[int] | None = None, full_refresh: bool = False) -> dict:
    """
    Run the Land Registry ingestion pipeline.

    Supports two modes:
    - Incremental (default): Skip years that already exist in bronze.
    - Full refresh: Re-download and overwrite all years.

    This is the entry point called by Airflow or the CLI.

    Args:
        years: Years to ingest. Defaults to config value.
        full_refresh: If True, re-ingest even if partitions exist.

    Returns:
        Summary dict with counts and paths for logging/alerting.
    """
    if years is None:
        years = LAND_REGISTRY_YEARS

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    results = {"ingested": [], "skipped": [], "failed": []}

    # Check existing partitions for incremental mode
    if not full_refresh:
        existing = list_existing_partitions("land_registry", "year")
        logger.info(f"Existing partitions: {existing}")
    else:
        existing = []
        logger.info("Full refresh mode — re-ingesting all years")

    for year in sorted(years):
        if str(year) in existing and not full_refresh:
            logger.info(f"Skipping {year} — partition already exists (use --full-refresh to override)")
            results["skipped"].append(year)
            continue

        try:
            path = ingest_year(year, batch_id)
            results["ingested"].append({"year": year, "path": path})
        except Exception as e:
            logger.error(f"Failed to ingest {year}: {e}", extra={"year": year})
            results["failed"].append({"year": year, "error": str(e)})

    # Summary
    logger.info(
        f"Ingestion complete: {len(results['ingested'])} ingested, "
        f"{len(results['skipped'])} skipped, {len(results['failed'])} failed"
    )

    return results


# =============================================================================
# CLI entry point
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Ingest HM Land Registry Price Paid Data into the bronze layer"
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Comma-separated years to ingest (default: from LAND_REGISTRY_YEARS env var)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Re-download and overwrite existing partitions",
    )
    args = parser.parse_args()

    years = None
    if args.years:
        years = [int(y.strip()) for y in args.years.split(",")]

    results = run(years=years, full_refresh=args.full_refresh)

    # Exit with error code if any ingestion failed
    if results["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
