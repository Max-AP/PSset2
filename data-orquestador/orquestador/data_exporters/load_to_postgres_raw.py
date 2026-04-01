if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


import pandas as pd
from sqlalchemy import create_engine, text
from mage_ai.data_preparation.shared.secrets import get_secret_value

@data_exporter
def export_data(data, *args, **kwargs):
    if isinstance(data, list):
        data = data[0]
        
    user = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')
    host = get_secret_value('POSTGRES_HOST')
    port = get_secret_value('POSTGRES_PORT')
    db = get_secret_value('POSTGRES_DB')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS raw.yellow_taxi_trips AS
        SELECT * FROM raw.yellow_taxi_trips WHERE false;
    """))

    # Deduplication: delete existing data for this month before inserting
    for year in data['source_year'].unique():
        for month in data['source_month'].unique():
            with engine.begin() as conn:
                conn.execute(text(f"""
                    DELETE FROM raw.yellow_taxi_trips
                    WHERE source_year = {year} AND source_month = {month};
                """))

    with engine.begin() as conn:
        data.to_sql(
            name='yellow_taxi_trips',
            con=conn,
            schema='raw',
            if_exists='append',
            index=False,
            chunksize=10000
        )

    print(f"✓ Loaded {len(data)} rows into raw.yellow_taxi_trips")


