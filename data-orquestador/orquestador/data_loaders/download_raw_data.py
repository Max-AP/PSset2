if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@data_loader
def load_data(*args, **kwargs):
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    
    years = range(2015, 2016)
    dfs = []

    for year in years:
        for month in range(1, 13):
            url = base_url.format(year=year, month=month)
            try:
                logger.info(f"Downloading {year}-{month:02d}...")
                df = pd.read_parquet(url)
                df['source_year'] = year
                df['source_month'] = month
                dfs.append(df)
                logger.info(f"Loaded {year}-{month:02d}: {len(df)} rows")
            except Exception as e:
                logger.warning(f"Skipping {year}-{month:02d}: {e}")
                continue

    combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total rows loaded: {len(combined)}")
    return combined


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
