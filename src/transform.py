import pandas as pd
import logging
import sys
import numpy as np

# logger = logging.getLogger(__name__)

# Step 3 — Transformation
# Write a transform(df) function that adds the following derived columns:

# transfer_year — year extracted from transfer_date
# transfer_month — month extracted from transfer_date
# transfer_quarter — quarter extracted from transfer_date e.g. Q1, Q2
# price_band — "LOW" under £200k, "MID" £200k-£500k, "HIGH" £500k-£1m, "PREMIUM" over £1m
# is_new_build — boolean True/False based on old_new field which is Y for new build
# full_address — concatenation of paon, street, city, postcode


def transform(df: pd.DataFrame) -> pd.DataFrame:

    df = self.clean_df.copy()

    self.logger.info("Starting transformation")

    df['transfer_year'] = (df['transfer_date']).dt.year
    df['transfer_month'] = ((df['transfer_date']).dt.month)
    df['transfer_quarter'] = "Q" + ((df['transfer_date']).dt.quarter).astype(str)

    bins = [0, 200000, 500000, 1000000, float("inf")]
    labels = ["LOW", "MID", "HIGH", "PREMIUM"]

    try:
        df['price_band'] = pd.cut(df['price'], bins=bins, labels=labels, include_lowest=True, right=True)
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

    self.sum_df = df

