import pandas as pd
import numpy as np
import logging

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



