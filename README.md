# UK Property Data Pipeline

A two-phase data engineering pipeline that processes UK Land Registry house price data, built to demonstrate core DE skills across local Python/pandas and cloud-based PySpark/Databricks environments.

---

## Project Overview

This pipeline ingests, validates, transforms and summarises UK property transaction data from the [HM Land Registry Price Paid dataset](http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv), which contains every residential property transaction registered in England and Wales.

The project is split into two phases:

- **Phase 1** — Local pipeline built with pandas and pure Python, running in VS Code
- **Phase 2** — Cloud pipeline rebuilt in PySpark on Databricks, following the medallion architecture

---

## Architecture

### Phase 1 — Local Pandas Pipeline

```
Land Registry URL
      ↓
  load_file()        # Ingest CSV, assign column names, log record count
      ↓
   clean()           # Validate, standardise, track rejections
      ↓
  transform()        # Derive new columns (price band, year, quarter etc.)
      ↓
  summarise()        # Aggregate by county and property type
      ↓
 save_outputs()      # Write parquet, CSV and JSON outputs
```

### Phase 2 — Databricks Medallion Architecture

```
Raw Data (uploaded to Databricks)
      ↓
   Bronze            # Raw data as ingested, no modifications
      ↓
   Silver            # Cleaned, validated and transformed records
      ↓
    Gold             # Aggregated summary tables ready for reporting
```

Each layer is persisted as a **Delta table** in Databricks, giving full ACID transaction support and query capability via Spark SQL.

---

## Project Structure

```
uk-property-data-pipeline/
├── src/
│   ├── pipeline.py        # Main pipeline class (HousePricePipeline)
│   ├── clean.py           # Validation and cleaning logic
│   └── transform.py       # Transformation and summarisation logic
├── data/
│   ├── raw/               # Raw input data
│   └── outputs/           # Cleaned parquet, summary CSV, rejected JSON
├── logs/
│   └── pipeline.log       # Pipeline run logs
├── notebooks/
│   └── phase2_pipeline    # Databricks notebook (PySpark)
├── config.json            # Pipeline configuration (not committed)
├── config.example.json    # Example config with placeholder values
├── requirements.txt       # Python dependencies
└── README.md
```

---

## Data

**Source:** HM Land Registry Price Paid Data  
**Format:** CSV, no header row, 16 columns  
**Coverage:** All residential property transactions in England and Wales

| Column | Description |
|---|---|
| transaction_id | Unique transaction reference |
| price | Sale price in GBP |
| transfer_date | Date of transaction |
| postcode | Property postcode |
| property_type | D, S, T, F, O (detached, semi, terraced, flat, other) |
| old_new | Y = new build, N = existing property |
| duration | F = freehold, L = leasehold |
| paon, saon, street, locality, city, district, county | Address fields |
| ppd_category | Price paid data category |
| record_status | Record status indicator |

---

## Validation Rules

Records are rejected and written to a separate output if they fail any of the following checks:

- Null value in `transaction_id`, `price`, `transfer_date` or `postcode`
- `price` is zero or negative
- `property_type` not in valid values: D, S, T, F, O
- `duration` not in valid values: F, L

---

## Derived Columns (Transform Stage)

| Column | Description |
|---|---|
| transfer_year | Year extracted from transfer_date |
| transfer_month | Month extracted from transfer_date |
| transfer_quarter | Quarter e.g. Q1, Q2 |
| price_band | LOW (<£200k), MID (£200k-£500k), HIGH (£500k-£1m), PREMIUM (>£1m) |
| is_new_build | Boolean, True if old_new == Y |
| full_address | Concatenation of paon, street, city, postcode |

---

## Summary Output (Gold Layer)

Grouped by `county` and `property_type`:

| Column | Description |
|---|---|
| total_transactions | Count of transactions |
| total_volume | Sum of all sale prices |
| avg_price | Mean sale price |
| median_price | Median sale price |
| min_price | Minimum sale price |
| max_price | Maximum sale price |
| new_build_pct | Percentage of transactions that are new builds |

---

## Installation & Usage

### Phase 1 — Local Pipeline

**Prerequisites:** Python 3.8+, pip

**Install dependencies:**
```bash
pip install -r requirements.txt
```

**Create config file:**
```bash
cp config.example.json config.json
```

Edit `config.json` with your settings:
```json
{
    "data_url": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv",
    "raw_data_dir": "data/raw",
    "output_dir": "data/outputs",
    "log_filepath": "logs/pipeline.log",
    "rejected_filepath": "data/outputs/rejected.json"
}
```

**Run the pipeline:**
```bash
python src/pipeline.py config.json
```

**Outputs:**
- `data/outputs/outputs_clean.parquet` — cleaned and transformed records
- `data/outputs/outputs_sum.csv` — summary by county and property type
- `data/outputs/rejected.json` — rejected records with rejection reasons
- `logs/pipeline.log` — full pipeline run log

---

### Phase 2 — Databricks Pipeline

**Prerequisites:** Databricks account (Community Edition or above)

1. Upload the raw CSV/parquet file to Databricks via the Catalog
2. Clone this repository into your Databricks workspace via Repos
3. Open `notebooks/phase2_pipeline`
4. Create the Silver and Gold schemas:
```sql
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```
5. Run all cells top to bottom

**Delta table outputs:**
- `silver.house_prices` — cleaned and transformed records
- `silver.house_prices_rejected` — rejected records
- `gold.house_prices_summary` — aggregated summary

---

## Technologies

| Technology | Usage |
|---|---|
| Python 3 | Core language |
| pandas | Data processing (Phase 1) |
| NumPy | Vectorised operations (Phase 1) |
| PySpark | Distributed data processing (Phase 2) |
| Databricks | Cloud platform (Phase 2) |
| Delta Lake | Storage format (Phase 2) |
| pyarrow | Parquet file support |
| logging | Pipeline run logging |
| Git | Version control |

---

## Key Concepts Demonstrated

- End to end pipeline design and implementation
- Data validation and rejection tracking
- Medallion architecture (bronze / silver / gold)
- Object oriented pipeline design using Python classes
- Production logging patterns
- Error handling and graceful failure
- Parquet and Delta table output formats
- Local to cloud pipeline migration
