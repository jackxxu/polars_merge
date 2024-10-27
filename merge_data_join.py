# %%

import polars as pl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import List

BINS_COUNT = 10
BINS_COUNT2 = 5


#%%

def process_file(file_paths):
    # print(f"Processing file: {file_path}")
    return (
        pl.scan_parquet(file_paths[0])  # Lazily read the Parquet file
        .join(pl.scan_parquet(file_paths[1]), on='id', how='left')
        .with_columns(
            pl.col('a').qcut(BINS_COUNT, labels=[str(i) for i in range(BINS_COUNT)]).cast(pl.Int8).alias('bins'),
            pl.col('a').qcut(BINS_COUNT2, labels=[str(i) for i in range(BINS_COUNT2)]).cast(pl.Int8).alias('bins_2')
        )
    )


def stitch_files(file_paths: List[List]):
    with ThreadPoolExecutor() as executor:
        # Execute the processing function for each file concurrently
        lazy_frames = list(executor.map(process_file, file_paths))

    return pl.concat(
        lazy_frames,
        rechunk=True  # Rechunking for better memory usage
    )


non_repeated_paths = list(zip(
    Path("data").glob("*.parquet"),
    Path("data2").glob("*.parquet")))


repeated_paths = list(zip(
    Path("data").glob("*.parquet"),
    ['data2/2025-01-22.parquet'] * len(list(Path("data").glob("*.parquet"))))) # 1 file repeated

# %%

%%timeit

stitch_files(non_repeated_paths).collect()

# %%

print(stitch_files(non_repeated_paths).explain())

# %%
%%timeit

stitch_files(repeated_paths).collect()

#%%

print(stitch_files(repeated_paths).explain())
