# UK House Price Data Pipeline

## Overview
A data engineering pipeline that ingests, validates, transforms and summarises
UK house price data from the Land Registry Price Paid dataset.

## Architecture
The pipeline follows a standard extract, clean, transform, summarise pattern:

- **Ingest** — loads monthly CSV data directly from the Land Registry API
- **Clean** — validates and rejects invalid records, tracking rejection reasons
- **Transform** — derives new columns including price bands, new build flags and address fields
- **Summarise** — aggregates statistics by county and property type
- **Save** — outputs parquet, CSV and JSON files

## Project Structure
```
uk_house_prices/
├── src/
│   ├── pipeline.py      # main pipeline and orchestration
│   ├── clean.py         # validation and cleaning logic
│   └── transform.py     # transformation and summarisation
├── data/
│   ├── raw/             # source data
│   └── outputs/         # parquet, CSV and JSON outputs
├── logs/                # pipeline logs
├── config.json          # pipeline configuration
├── requirements.txt     # dependencies
└── README.md
```

## Requirements
- Python 3.9+
- See requirements.txt for dependencies

## Installation
```bash
git clone <repo_url>
cd generated_housing
pip install -r requirements.txt
```

## Configuration
Update config.json with your settings:
```json
{
    "data_url": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv",
    "output_dir": "data/outputs",
    "log_filepath": "logs/pipeline.log"
}
```

## Usage
```bash
python src/pipeline.py config.json
```

## Output Files
- `outputs_clean.parquet` — cleaned and transformed records
- `outputs_sum.csv` — summary statistics by county and property type
- `rejected.json` — rejected records with rejection reasons

## Tech Stack
- Python 3.9+
- pandas
- numpy
- pyarrow

## Author
Jake Caulton
