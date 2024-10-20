# %%

import polars as pl


# %%

pl.scan_parquet("data/*.parquet").collect()