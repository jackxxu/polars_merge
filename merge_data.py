# %%

import polars as pl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

BINS_COUNT = 10
BINS_COUNT2 = 5

# %%

%%timeit
(
    pl.scan_parquet("data/*.parquet")
    .join(pl.scan_parquet("data/*.parquet"), on=['date', 'id'], how='inner')
    .with_columns([
        pl.col('a').qcut(BINS_COUNT, labels=[str(i) for i in range(BINS_COUNT)]).cast(pl.Int8).over('date').alias('bins'),
        pl.col('a').qcut(BINS_COUNT2, labels=[str(i) for i in range(BINS_COUNT2)]).cast(pl.Int8).over('date').alias('bins2'),
    ])
    .collect()
)


# %%

# Step 1: Create sample Parquet files with random data
output_dir = Path("data")
parquet_files = list(output_dir.glob("*.parquet"))

def process_file(file_path):
    # print(f"Processing file: {file_path}")
    return (
        pl.scan_parquet(file_path)  # Lazily read the Parquet file
        .join(pl.scan_parquet(file_path), on=['id'], how='inner')
        .with_columns(
            pl.col('a').qcut(BINS_COUNT, labels=[str(i) for i in range(BINS_COUNT)]).cast(pl.Int8).alias('bins'),
            pl.col('a').qcut(BINS_COUNT2, labels=[str(i) for i in range(BINS_COUNT2)]).cast(pl.Int8).alias('bins_2')
        )
    )

# %%
%%timeit

with ThreadPoolExecutor() as executor:
    # Execute the processing function for each file concurrently
    lazy_frames = list(executor.map(process_file, parquet_files))

# lazy_frames = [process_file(file) for file in parquet_files]

pl.concat(
    lazy_frames,
    rechunk=True  # Rechunking for better memory usage
).collect()  # Collect to finalize the lazy execution
