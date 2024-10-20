# %%

# create a series of parquet files for 10 days of October 2024, with same schema of
# 2 columns: a: float64, cat: category, date: fix value for each date
# each file will have 10000 rows

import polars as pl
import numpy as np

ROWS_COUNT = 10000

for i in range(1, 11):
    date = f"2024-10-{i:02}"
    df = pl.DataFrame({
        "a": np.random.rand(ROWS_COUNT),
        "cat": pl.Series(["a", "b", "c", "d", "e"]).sample(ROWS_COUNT, with_replacement=True),
        "date": [date] * ROWS_COUNT,
    })
    df.write_parquet(f"data/{date}.parquet", compression="zstd", compression_level=22)

