# %%

# create a series of parquet files for 10 days of October 2024, with same schema of
# 2 columns: a: float64, cat: category, date: fix value for each date
# each file will have 10000 rows

import polars as pl
import numpy as np
from datetime import datetime, timedelta

ROWS_COUNT = 50000
NUM_DAYS = 500

start_date = datetime(2024, 10, 1)

for i in range(NUM_DAYS):
    # Calculate the current date
    date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")

    # Create the DataFrame for the current date
    df = pl.DataFrame({
        "id": range(ROWS_COUNT),
        "a": np.random.rand(ROWS_COUNT),
        "date": [date] * ROWS_COUNT,
    })

    # Write the DataFrame to a Parquet file with high compression
    df.write_parquet(f"data/{date}.parquet", compression="zstd", compression_level=22)

for i in range(NUM_DAYS):
    # Calculate the current date
    date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")

    # Create the DataFrame for the current date
    df = pl.DataFrame({
        "id": range(ROWS_COUNT),
        "a": np.random.rand(ROWS_COUNT),
        "date": [date] * ROWS_COUNT,
    })

    # Write the DataFrame to a Parquet file with high compression
    df.write_parquet(f"data2/{date}.parquet", compression="zstd", compression_level=22)
