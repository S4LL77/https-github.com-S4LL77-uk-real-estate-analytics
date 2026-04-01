"""
Central configuration for the UK Real Estate Analytics pipeline.

All settings are loaded from environment variables (via .env file) with
sensible defaults for local development. This module is the single source
of truth for paths, URLs, column schemas, and feature flags.

Design decision: We use a flat module rather than a Settings class because
this is a data pipeline, not a web app — simplicity over abstraction.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# Project Paths
# =============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# =============================================================================
# Storage Configuration
# =============================================================================
STORAGE_MODE = os.getenv("STORAGE_MODE", "local")  # "local" or "s3"
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "uk-real-estate-analytics")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")

# S3 path prefixes (medallion architecture)
S3_BRONZE_PREFIX = "bronze"
S3_SILVER_PREFIX = "silver"
S3_GOLD_PREFIX = "gold"

# =============================================================================
# Logging
# =============================================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# =============================================================================
# HM Land Registry — Price Paid Data
# =============================================================================
# Official bulk download URL pattern. Files have NO headers.
# Complete file (~4.8GB): pp-complete.csv
# Yearly files (~150MB each): pp-2024.csv, pp-2023.csv, etc.
LAND_REGISTRY_BASE_URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
)
LAND_REGISTRY_YEARLY_URL = f"{LAND_REGISTRY_BASE_URL}/pp-{{year}}.csv"
LAND_REGISTRY_COMPLETE_URL = f"{LAND_REGISTRY_BASE_URL}/pp-complete.csv"

# Years to ingest (comma-separated in env, defaults to 2024)
LAND_REGISTRY_YEARS = [
    int(y.strip())
    for y in os.getenv("LAND_REGISTRY_YEARS", "2024").split(",")
]

# Column names for the headerless CSV (official order from Land Registry docs)
LAND_REGISTRY_COLUMNS = [
    "transaction_id",
    "price_paid",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "paon",       # Primary Addressable Object Name (house number/name)
    "saon",       # Secondary Addressable Object Name (flat/unit)
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",  # A=Standard, B=Additional (repos, BTL)
    "record_status",      # A=Addition, C=Change, D=Delete (monthly only)
]

# Data types for efficient Parquet storage
LAND_REGISTRY_DTYPES = {
    "transaction_id": "string",
    "price_paid": "int64",
    "date_of_transfer": "string",  # Parsed to date downstream
    "postcode": "string",
    "property_type": "category",   # D, S, T, F, O — high cardinality savings
    "old_new": "category",         # Y, N
    "duration": "category",        # F, L
    "paon": "string",
    "saon": "string",
    "street": "string",
    "locality": "string",
    "town_city": "string",
    "district": "string",
    "county": "string",
    "ppd_category_type": "category",  # A, B
    "record_status": "category",      # A, C, D
}

# =============================================================================
# Bank of England — Interest Rates
# =============================================================================
# Official Bank Rate series code: IUMABEDR
# The IADB returns CSV data with Date and Value columns
BOE_SERIES_CODE = "IUMABEDR"
BOE_BASE_URL = "https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"
BOE_RATES_URL = (
    f"{BOE_BASE_URL}?csv.x=yes"
    f"&Datefrom=01/Jan/1975"
    f"&Dateto=now"
    f"&SeriesCodes={BOE_SERIES_CODE}"
    f"&CSVF=TN"
    f"&UsingCodes=Y"
)

# =============================================================================
# ONS — Demographics & Housing
# =============================================================================
ONS_API_BASE_URL = "https://api.beta.ons.gov.uk/v1"

# =============================================================================
# Ingestion Settings
# =============================================================================
# Retry configuration (exponential backoff)
MAX_RETRIES = 3
RETRY_BASE_DELAY_SECONDS = 2  # 2s, 8s, 32s with exponential backoff

# Chunk size for streaming large CSV downloads (bytes)
DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks
