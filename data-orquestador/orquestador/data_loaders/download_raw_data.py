if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import gc

@data_loader
def load_data(*args, **kwargs):
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    
    # Only load needed columns
    COLUMNS = [
        'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'passenger_count', 'trip_distance', 'RatecodeID', 'PULocationID',
        'DOLocationID', 'payment_type', 'fare_amount', 'tip_amount',
        'tolls_amount', 'total_amount'
    ]

    # Cast to smaller types at read time
    DTYPES = {
        'VendorID': 'int8',
        'passenger_count': 'float32',
        'trip_distance': 'float32',
        'RatecodeID': 'float32',
        'PULocationID': 'float32',
        'DOLocationID': 'float32',
        'payment_type': 'float32',
        'fare_amount': 'float32',
        'tip_amount': 'float32',
        'tolls_amount': 'float32',
        'total_amount': 'float32',
    }

    years = range(2015, 2016)
    dfs = []

    for year in years:
        for month in range(2, 3):
            url = base_url.format(year=year, month=month)
            try:
                print(f"Downloading {year}-{month:02d}...")
                df = pd.read_parquet(url, columns=COLUMNS)
                
                # Apply dtypes immediately after load
                for col, dtype in DTYPES.items():
                    if col in df.columns:
                        df[col] = df[col].astype(dtype)

                df['source_year'] = pd.array([year] * len(df), dtype='int16')
                df['source_month'] = pd.array([month] * len(df), dtype='int8')
                
                dfs.append(df)
                print(f"✓ {year}-{month:02d}: {len(df)} rows, {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
                gc.collect()
            except Exception as e:
                print(f"✗ Skipping {year}-{month:02d}: {e}")
                continue

    combined = pd.concat(dfs, ignore_index=True)
    del dfs
    gc.collect()
    
    print(f"Total: {len(combined)} rows, {combined.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    return combined


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
