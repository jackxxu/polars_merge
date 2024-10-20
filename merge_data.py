# %%

import polars as pl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

BINS_COUNT = 10

# %%

%%timeit

pl.scan_parquet("data/*.parquet").with_columns(
  pl.col('a').qcut(BINS_COUNT, labels=[str(i) for i in range(BINS_COUNT)]).cast(pl.Int8).alias('bins')
).collect()


# %%

# Step 1: Create sample Parquet files with random data
output_dir = Path("data")
parquet_files = list(output_dir.glob("*.parquet"))

def process_file(file_path):
    # print(f"Processing file: {file_path}")
    return (
        pl.scan_parquet(file_path)  # Lazily read the Parquet file
        .with_columns(
            pl.col('a').qcut(BINS_COUNT, labels=[str(i) for i in range(BINS_COUNT)]).cast(pl.Int8).alias('bins')
        )
    )

# %%
%%timeit

with ThreadPoolExecutor() as executor:
    # Execute the processing function for each file concurrently
    lazy_frames = list(executor.map(process_file, parquet_files))

pl.concat(
    lazy_frames,
    rechunk=True  # Rechunking for better memory usage
).collect()  # Collect to finalize the lazy execution
