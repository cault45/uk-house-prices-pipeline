import pandas as pd
import json
import logging
import sys
import requests
import numpy as np

# Benefits of breaking down as class (modest for one of this size)

# State management — instead of passing DataFrames between functions as arguments, they live on self and are accessible everywhere. For a pipeline with many stages this reduces function signatures significantly
# Encapsulation — everything related to the pipeline is bundled together as one object. You can do pipeline = HousePricePipeline(config) and the whole thing is self contained
# Extensibility — if you wanted a DatabricksPipeline that inherits from HousePricePipeline and overrides just the load_file and save_outputs methods, a class makes that very natural
# Instantiation — you could run multiple pipelines with different configs simultaneously: pipeline1 = HousePricePipeline('config1.json'), pipeline2 = HousePricePipeline('config2.json')

# git add .
# git commit -m "descriptive message of what you changed"
# git push


# http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv
# This is a real monthly update file containing every property transaction registered that month in England and Wales.

# Phase 1 — Local Pipeline in VSCode (pandas)
# This is where you start. Build a pipeline that processes the monthly CSV file.
# Step 1 — Ingestion
# Write a load_file(filepath: str) function that:

# Reads the CSV into a DataFrame — note it has no header row so you'll need to assign column names manually
# The columns are: transaction_id, price, transfer_date, postcode, property_type, old_new, duration, paon, saon, street, locality, city, district, county, ppd_category, record_status
# Logs how many records were loaded
# Handles file not found and parse errors gracefully


class HousePricePipeline:

    def __init__(self, config_filepath: str):
        self.config_filepath = config_filepath
        self.config = None
        self.raw_df = None
        self.clean_df = None
        self.rejected_df = None
        self.transformed_df = None
        self.summary_df = None
        self.ws_parquet: bool = None
        self.ws_csv: bool = None
        self.ws_json: bool = None
        self.logger = logging.getLogger(__name__)
        self._setup_logging()


    def _setup_logging(self):

        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        file_handler = logging.FileHandler("logs/pipeline.log", mode='a', encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s",
                                    datefmt="%Y-%m-%d %H:%M:%S")
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)


    def load_config(self) -> None:

        try:
            with open(self.config_filepath, 'r') as c:
                self.config = json.load(c)
        except FileNotFoundError:
            self.logger.error("config.json not found")
            sys.exit(1)
        except json.JSONDecodeError:
            self.logger.error("config.json is malformed")
            sys.exit(1)


    def load_file(self) -> None:

        try:
            data_url = self.config['data_url']
        except KeyError as e:
            self.logger.error("Missing config key: %s", e)
            sys.exit(1)

        columns = ['transaction_id', 'price', 'transfer_date', 'postcode', 'property_type', 'old_new', 'duration',
                    'paon', 'saon', 'street', 'locality', 'city', 'district', 'county', 'ppd_category', 'record_status']

        self.logger.info("Pipeline starting, loading data from %s", data_url)

        try:
            df = pd.read_csv(data_url, header=None, names=columns)
            # df.to_parquet("data/raw/house_prices.parquet", index=False)
        except requests.exceptions.RequestException:
            self.logger.error("Unable to connect to URL")
            return None
        except pd.errors.ParserError:
            self.logger.error("CSV cannot be parsed")
            return None

        self.logger.info("%s records have been loaded", len(df))

        self.raw_df = df

# logger = logging.getLogger(__name__)

# Step 2 — Validation and cleaning
# Write a clean(df) function that:

# Drops rows where transaction_id, price, transfer_date, or postcode are NULL
# Drops rows where price is zero or negative
# Parses transfer_date as a proper datetime
# Standardises property_type — valid values are D (detached), S (semi-detached), T (terraced), F (flat), O (other)
# Standardises duration — valid values are F (freehold) and L (leasehold)
# Tracks rejected records with rejection reasons
# Logs counts of passed and rejected records


    def clean(self) -> None:

        df = self.raw_df.copy()

        df['rejection_reason'] = None

        df['transfer_date'] = pd.to_datetime(df['transfer_date'], errors='coerce', format='%Y-%m-%d %H:%M')

        df['rejection_reason'] = np.where(
            df['transaction_id'].isnull() |
            df['price'].isnull() |
            df['transfer_date'].isnull() |
            df['postcode'].isnull(),
            "NULL value identified in key column",
            None
        )
        df['rejection_reason'] = np.where(
            (df['price'] <= 0) & (df['rejection_reason'].isnull()),
            "Price is less than or equal to zero",
            df['rejection_reason']
        )
        df['property_type'] = df['property_type'].str.upper()
        df['rejection_reason'] = np.where(
            (~df['property_type'].isin(["D", "S", "T", "F", "O"])) & (df['rejection_reason'].isnull()),
            "Invalid property type",
            df['rejection_reason']
        )
        df['duration'] = df['duration'].str.upper()
        df['rejection_reason'] = np.where(
            (~df['duration'].isin(["F", "L"])) & (df['rejection_reason'].isnull()),
            "Invalid duration",
            df['rejection_reason']
        )

        df_rejected = df[df['rejection_reason'].notnull()]
        df = df[df['rejection_reason'].isnull()]

        for _, row in df_rejected.iterrows():
            self.logger.warning("Record with transaction_id %s has been removed from the dataset due to %s", row['transaction_id'], row['rejection_reason'])

        df = df.drop('rejection_reason', axis='columns')
        df = df.reset_index(drop=True)
        df_rejected = df_rejected.reset_index(drop=True)
        self.logger.info("Data validation completed. %s records have passed validation, %s have been removed", len(df), len(df_rejected))

        self.clean_df = df
        self.rejected_df = df_rejected

# Step 3 — Transformation
# Write a transform(df) function that adds the following derived columns:

# transfer_year — year extracted from transfer_date
# transfer_month — month extracted from transfer_date
# transfer_quarter — quarter extracted from transfer_date e.g. Q1, Q2
# price_band — "LOW" under £200k, "MID" £200k-£500k, "HIGH" £500k-£1m, "PREMIUM" over £1m
# is_new_build — boolean True/False based on old_new field which is Y for new build
# full_address — concatenation of paon, street, city, postcode


    def transform(self) -> None:

        df = self.clean_df.copy()

        self.logger.info("Starting transformation")

        df['transfer_year'] = (df['transfer_date']).dt.year
        df['transfer_month'] = ((df['transfer_date']).dt.month)
        df['transfer_quarter'] = "Q" + ((df['transfer_date']).dt.quarter).astype(str)

        bins = [0, 200000, 500000, 1000000, float("inf")]
        labels = ["LOW", "MID", "HIGH", "PREMIUM"]

        try:
            df['price_band'] = pd.cut(df['price'], bins=bins, labels=labels, include_lowest=True, right=True)
            df['price_band'] = df['price_band'].astype(str)
        except ValueError:
            self.logger.error("Value error during transformation")
            sys.exit(1)

        df['is_new_build'] = df['old_new'] == 'Y'

        df['full_address'] = df['paon'].astype(str) + " " + df['street'].astype(str) + " " + df['city'].astype(str) + " " + df['postcode'].astype(str)
        # Alternative and good to know
        # df[['paon', 'street', 'city', 'postcode']].astype(str).agg(' '.join, axis=1)

        self.logger.info("Transformation completed. %s records transformed", len(df))

        self.transformed_df = df


# Step 4 — Summarisation
# Write a summarise(df) function that produces a summary DataFrame with the following stats grouped by county and property_type:

# total_transactions
# total_volume — sum of prices
# avg_price — mean price rounded to 2 decimal places
# median_price
# min_price
# max_price
# new_build_pct — percentage of transactions that are new builds


    def summarise(self) -> None:

        df = self.transformed_df.copy()

        self.logger.info("Starting data summary")

        sum_df = df.groupby(['county', 'property_type']).agg(total_transactions=('transaction_id', 'count'),
                                                            total_volume=('price', lambda x: round(x.sum(),2)),
                                                            avg_price=('price', lambda x: round(x.mean(), 2)),
                                                            median_price=('price', lambda x: round(x.median(), 2)),
                                                            min_price=('price', 'min'),
                                                            max_price=('price', 'max'),
                                                            new_build=('old_new', lambda x: (x == 'Y').sum())
        )

        tt = sum_df['total_transactions']
        new_build = sum_df['new_build']

        sum_df['new_build_pct'] = round((new_build / tt) * 100, 1)

        sum_df = sum_df.drop('new_build', axis='columns')

        sum_df = sum_df.reset_index()

        self.logger.info("Data summary completed. %s county/property type combinations summarised", len(sum_df))

        self.summary_df = sum_df


# Step 5 — Save outputs
# Write a save_outputs(df, summary_df, output_dir) function that:

# Saves the cleaned transformed DataFrame as a parquet file — this is your first time using parquet, it's the standard format in DE
# Saves the summary DataFrame as a CSV for easy viewing
# Saves the rejected records as a JSON file for auditing
# Logs success/failure for each


    def save_outputs(self) -> None:


        df = self.transformed_df.copy()
        summary_df = self.summary_df.copy()
        rejected_df = self.rejected_df.copy()

        try:
            output_dir = self.config["output_dir"]
        except KeyError as e:
            self.logger.error("Missing config key: %s", e)
            sys.exit(1)


        clean_fp = (f"{output_dir}/outputs_clean.parquet")
        sum_fp = (f"{output_dir}/outputs_sum.csv")
        rej_fp = (f"{output_dir}/rejected.json")

        try:
            df.to_parquet(clean_fp, engine='pyarrow', compression='snappy', index=False)
            self.logger.info("%s records successfully written to filepath %s", len(df), clean_fp)
            ws_parquet = True
        except OSError:
            self.logger.error("OSError - failed to write file to filepath %s", clean_fp)
            ws_parquet = False
        except ValueError:
            self.logger.error("ValueError - failed to write file to filepath %s", clean_fp)
            ws_parquet = False

        try:
            summary_df.to_csv(sum_fp)
            self.logger.info("%s records successfully written to filepath %s", len(summary_df), sum_fp)
            ws_csv = True
        except OSError:
            self.logger.error("OSError - failed to write file to filepath %s", sum_fp)
            ws_csv = False
        except ValueError:
            self.logger.error("ValueError - failed to write file to filepath %s", sum_fp)
            ws_csv = False

        try:
            rejected_df.to_json(rej_fp, orient='records', indent=4, date_format="iso")
            self.logger.info("%s records successfully written to filepath %s", len(rejected_df), rej_fp)
            ws_json = True
        except OSError:
            self.logger.error("OSError - failed to write file to filepath %s", rej_fp)
            ws_json = False
        except ValueError:
            self.logger.error("ValueError - failed to write file to filepath %s", rej_fp)
            ws_json = False

        self.ws_parquet = ws_parquet
        self.ws_csv = ws_csv
        self.ws_json = ws_json

    def run(self):

        self.load_config()
        self.load_file()

        if self.raw_df is None:
            self.logger.error("Failed to load data - exiting")
            sys.exit(1)

        self.clean()
        if len(self.clean_df) == 0:
            self.logger.warning("No records passed validation - exiting")
            sys.exit(0)
        self.transform()
        self.summarise()
        self.save_outputs()

        if self.ws_parquet and self.ws_csv and self.ws_json:
            self.logger.info("Pipeline completed with no errors")
            return True
        else:
            self.logger.warning("Pipeline completed with errors")
            return False




if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Missing command line arguments")
        sys.exit(1)
    pipeline = HousePricePipeline(sys.argv[1])
    pipeline.run()
