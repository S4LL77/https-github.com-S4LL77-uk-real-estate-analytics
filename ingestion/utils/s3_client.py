"""
Storage abstraction layer for the medallion architecture.

Provides a unified interface for writing data to either:
  - Local filesystem (default, for development)
  - AWS S3 (production)

The active backend is determined by the STORAGE_MODE env var.
This abstraction means ingestion code doesn't need to know
where data is being stored — it just calls `save_to_bronze()`.

Design decision: We save as Parquet (not CSV) in the bronze layer.
Reasons documented in DECISIONS.md:
  1. Columnar format → 5-10x compression vs CSV
  2. Schema embedded in file → no header ambiguity
  3. Native support in Snowflake COPY INTO
  4. Preserves data types (dates, integers) without casting
"""

from pathlib import Path
from typing import Optional

import pandas as pd

from ingestion.config import (
    BRONZE_DIR,
    S3_BUCKET_NAME,
    S3_BRONZE_PREFIX,
    STORAGE_MODE,
)
from ingestion.utils.logging_config import get_logger

logger = get_logger(__name__)


def save_to_bronze(
    df: pd.DataFrame,
    source_name: str,
    partition_key: str,
    partition_value: str,
    file_name: Optional[str] = None,
) -> str:
    """
    Save a DataFrame to the bronze layer of the data lake.

    Uses Hive-style partitioning: bronze/{source}/partition_key=value/file.parquet
    This is compatible with both Spark and Snowflake external tables.

    Args:
        df: DataFrame to persist.
        source_name: Data source identifier (e.g., "land_registry").
        partition_key: Partition column name (e.g., "year").
        partition_value: Partition value (e.g., "2024").
        file_name: Optional filename. Defaults to "data.parquet".

    Returns:
        The path (local or S3) where the file was saved.

    Example:
        path = save_to_bronze(df, "land_registry", "year", "2024")
    """
    if file_name is None:
        file_name = "data.parquet"

    if STORAGE_MODE == "s3":
        return _save_to_s3(df, source_name, partition_key, partition_value, file_name)
    else:
        return _save_to_local(df, source_name, partition_key, partition_value, file_name)


def _save_to_local(
    df: pd.DataFrame,
    source_name: str,
    partition_key: str,
    partition_value: str,
    file_name: str,
) -> str:
    """Save Parquet file to local filesystem (development mode)."""
    output_dir = BRONZE_DIR / source_name / f"{partition_key}={partition_value}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / file_name
    df.to_parquet(output_path, engine="pyarrow", index=False, compression="snappy")

    logger.info(
        f"Saved {len(df):,} rows to local bronze layer",
        extra={
            "source": source_name,
            "file_path": str(output_path),
            "row_count": len(df),
        },
    )
    return str(output_path)


def _save_to_s3(
    df: pd.DataFrame,
    source_name: str,
    partition_key: str,
    partition_value: str,
    file_name: str,
) -> str:
    """Save Parquet file to S3 bronze layer (production mode)."""
    try:
        import boto3
    except ImportError as e:
        raise ImportError(
            "boto3 is required for S3 storage mode. "
            "Install it with: pip install boto3"
        ) from e

    s3_key = f"{S3_BRONZE_PREFIX}/{source_name}/{partition_key}={partition_value}/{file_name}"

    # Write Parquet to bytes buffer first, then upload
    import io

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
    buffer.seek(0)

    s3_client = boto3.client("s3")
    s3_client.upload_fileobj(buffer, S3_BUCKET_NAME, s3_key)

    s3_path = f"s3://{S3_BUCKET_NAME}/{s3_key}"
    logger.info(
        f"Uploaded {len(df):,} rows to S3 bronze layer",
        extra={
            "source": source_name,
            "file_path": s3_path,
            "row_count": len(df),
        },
    )
    return s3_path


def list_existing_partitions(source_name: str, partition_key: str) -> list[str]:
    """
    List existing partition values in the bronze layer.

    Used for incremental loads — skip partitions that already exist.

    Returns:
        List of partition values (e.g., ["2022", "2023", "2024"]).
    """
    if STORAGE_MODE == "s3":
        return _list_s3_partitions(source_name, partition_key)
    else:
        return _list_local_partitions(source_name, partition_key)


def _list_local_partitions(source_name: str, partition_key: str) -> list[str]:
    """List partition directories in local filesystem."""
    source_dir = BRONZE_DIR / source_name
    if not source_dir.exists():
        return []

    partitions = []
    for path in source_dir.iterdir():
        if path.is_dir() and path.name.startswith(f"{partition_key}="):
            value = path.name.split("=", 1)[1]
            partitions.append(value)

    return sorted(partitions)


def _list_s3_partitions(source_name: str, partition_key: str) -> list[str]:
    """List partition prefixes in S3."""
    try:
        import boto3
    except ImportError:
        return []

    s3_client = boto3.client("s3")
    prefix = f"{S3_BRONZE_PREFIX}/{source_name}/{partition_key}="

    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET_NAME,
        Prefix=prefix,
        Delimiter="/",
    )

    partitions = []
    for common_prefix in response.get("CommonPrefixes", []):
        # Extract value from "bronze/land_registry/year=2024/"
        partition_dir = common_prefix["Prefix"].rstrip("/").split("/")[-1]
        value = partition_dir.split("=", 1)[1]
        partitions.append(value)

    return sorted(partitions)
