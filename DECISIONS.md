# Architectural Decision Records (ADR)

This document records key architectural decisions for the UK Real Estate Analytics pipeline. Each entry explains the context, decision, and trade-offs — the kind of reasoning that matters in technical interviews.

---

## ADR-001: Parquet over CSV for Bronze Layer

**Status:** Accepted  
**Date:** 2024-12-01

### Context
The Land Registry Price Paid Data is distributed as CSV. We need to decide the storage format for our bronze (raw) layer in the data lake.

### Decision
Store ingested data as **Parquet** (not CSV) in the bronze layer, even though the source is CSV.

### Rationale
| Factor | CSV | Parquet |
|---|---|---|
| Compression | ~150MB/year | ~30MB/year (5x smaller) |
| Schema | No embedded schema | Schema in file metadata |
| Column pruning | Must read all columns | Read only needed columns |
| Snowflake COPY INTO | Supported | Supported (faster) |
| Type safety | Everything is string | Native types preserved |

### Trade-offs
- **Pro:** Massive storage and query cost savings at scale (28M+ rows)
- **Pro:** Schema embedded in file prevents header mismatch errors
- **Pro:** Snappy compression is fast and well-supported
- **Con:** Not human-readable (can't `head file.parquet` in terminal)
- **Con:** Requires pyarrow dependency

### Interview talking point
> "We chose Parquet even at the bronze layer because the storage cost savings at 28M rows are significant, and having the schema embedded in the file prevents a whole class of parsing bugs. The trade-off is lost human readability, but that's what data catalogues are for."

---

## ADR-002: No Rightmove Scraping

**Status:** Accepted  
**Date:** 2024-12-01

### Context
Rightmove is the UK's largest property listing platform. Their data would enrich the pipeline significantly (asking prices, listing descriptions, time on market).

### Decision
**Do not scrape Rightmove.** Use only officially open datasets (Land Registry, ONS, BoE).

### Rationale
1. Rightmove's [Terms of Service](https://www.rightmove.co.uk/this-site/terms-of-use.html) explicitly prohibit automated data collection
2. As a portfolio project, it would signal poor judgement to future employers
3. UK Computer Misuse Act 1990 considerations for unauthorised access patterns
4. Land Registry Price Paid Data provides actual transaction prices (more reliable than asking prices)

### Trade-offs
- **Pro:** Legally and ethically sound — important for UK employer audience
- **Pro:** Land Registry provides *actual* sale prices, not asking prices
- **Con:** Missing listing-level features (descriptions, images, time on market)
- **Con:** No current "for sale" inventory data

### Interview talking point
> "I deliberately excluded Rightmove because their ToS prohibit scraping, and I think it's important to demonstrate that I take data ethics seriously. The Land Registry actually gives us transaction prices, which are more reliable than asking prices for analytics."

---

## ADR-003: Medallion Architecture over Data Vault

**Status:** Accepted  
**Date:** 2024-12-01

### Context
We need a data modelling strategy for the data lake and warehouse. The two main contenders are Medallion (Bronze/Silver/Gold) and Data Vault 2.0 (Hubs/Links/Satellites).

### Decision
Use **Medallion Architecture** for the data lake, with a **Star Schema** in the gold/mart layer.

### Rationale
| Factor | Medallion + Star | Data Vault 2.0 |
|---|---|---|
| Complexity | Lower | Higher (3 entity types) |
| Query performance | Excellent (denormalised) | Requires business vault layer |
| Learning curve | Moderate | Steep |
| Source agility | Moderate | Excellent |
| Audit trail | Good (SCD-2) | Excellent (insert-only) |

### Trade-offs
- **Pro:** Simpler to implement, explain, and query
- **Pro:** Star schema is immediately consumable by BI tools
- **Con:** Less resilient to source schema changes than Data Vault
- **Con:** Updates require merge logic vs. insert-only

### Interview talking point
> "I chose Medallion with Star Schema because it's the right tool for this scope — 3 sources, clear business entities. But I'm aware that for enterprise-scale with 50+ sources and frequent schema changes, Data Vault 2.0's insert-only pattern and hash-based keys would provide better agility. It's about matching architecture to requirements."

---

## ADR-004: dbt Core over dbt Cloud

**Status:** Accepted  
**Date:** 2024-12-01

### Context
dbt is the transformation tool. We can use dbt Core (open source, CLI) or dbt Cloud (managed SaaS).

### Decision
Use **dbt Core** with local CLI execution, orchestrated by Airflow.

### Rationale
1. **Cost:** dbt Core is free; dbt Cloud starts at ~$100/month per seat
2. **CI/CD control:** We run dbt inside GitHub Actions with full control over the pipeline
3. **Portability:** No vendor lock-in; can run anywhere Python runs
4. **Airflow integration:** `BashOperator` or `dbt-airflow` operator for seamless orchestration
5. **Interview relevance:** Shows you can work without managed services

### Trade-offs
- **Pro:** Zero cost, full control, portable
- **Con:** No built-in IDE (we use VS Code + dbt Power User extension)
- **Con:** No managed job scheduling (Airflow handles this)
- **Con:** Must self-manage dbt docs hosting

---

## ADR-005: Star Schema with SCD Type 2

**Status:** Accepted  
**Date:** 2024-12-01

### Context
The property data changes over time (re-sales, re-classifications). We need to track historical states for accurate trend analysis.

### Decision
Use a **star schema** with **SCD Type 2** on the property dimension. Fact table is transaction-grain.

### Rationale
- **SCD Type 2** captures full temporal history: a property sold in 2015 as "terraced" and re-classified in 2020 retains both states
- `valid_from` / `valid_to` / `is_current` columns enable point-in-time queries
- dbt snapshots automate the SCD-2 logic with `check` or `timestamp` strategies
- Star schema enables efficient aggregation queries from Tableau/Looker

### Why not SCD Type 1 or 3?
- **Type 1** (overwrite): Loses history — can't answer "what was the property type when it last sold?"
- **Type 3** (previous value column): Limited to one historical state — insufficient for properties sold 5+ times

### dbt Implementation
```yaml
# dbt snapshot config
snapshots:
  - name: snap_dim_property
    strategy: check
    unique_key: property_nk
    check_cols:
      - property_type
      - old_new
      - duration
```

---

## ADR-006: Partition by Year in Bronze Layer

**Status:** Accepted  
**Date:** 2024-12-01

### Context
Land Registry publishes yearly CSV files (~800K-1.2M rows each). We need a partitioning strategy for the bronze layer.

### Decision
Partition by **year** using Hive-style naming: `bronze/land_registry/year=2024/data.parquet`

### Rationale
1. Matches the source cadence (yearly files)
2. Enables incremental ingestion — skip years already loaded
3. Hive-style partitioning is natively understood by Snowflake external tables and Spark
4. Year-level granularity balances partition count (~30) vs. file size (~30MB each)

### Trade-offs
- **Pro:** Simple incremental logic — just check if partition exists
- **Pro:** Compatible with all major query engines
- **Con:** Within-year updates require full partition rewrite (acceptable for annual data)
- **Con:** Skewed partition sizes (recent years have more transactions)

---

## ADR-007: GDPR Dynamic Masking for Address Fields

**Status:** Accepted  
**Date:** 2024-12-01

### Context
Land Registry data contains address-level information (house name/number, street). Under UK GDPR, this can be considered personal data when combined with other information to identify an individual.

### Decision
Apply Snowflake **Dynamic Data Masking** policies to address fields (PAON, SAON, Street), controlled by RBAC roles.

### Rationale
1. UK GDPR requires data minimisation and purpose limitation
2. Most analytical queries don't need address-level detail (postcode is sufficient)
3. Dynamic masking is applied at query time — no need for separate masked datasets
4. Audit trail via Snowflake `ACCESS_HISTORY` views

### Implementation
- `DATA_ENGINEER` and `DATA_GOVERNANCE` roles see full addresses
- `ANALYST` role sees `***MASKED***`
- All other roles see `***REDACTED***`
- Masking policies are version-controlled and deployed via Terraform/CI

### Interview talking point
> "I applied GDPR masking even though Land Registry data is 'open data' because it demonstrates awareness of UK data protection principles. In a production environment, combining addresses with other datasets could create PII, so masking by default follows the principle of data minimisation."
