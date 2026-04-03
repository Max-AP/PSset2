if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import time
import pandas as pd
import psycopg2
from mage_ai.data_preparation.shared.secrets import get_secret_value

@data_loader
def load_data(*args, **kwargs):
    user = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')
    host = get_secret_value('POSTGRES_HOST')
    port = get_secret_value('POSTGRES_PORT')
    db = get_secret_value('POSTGRES_DB')

    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    cur = conn.cursor()

    try:
        start = time.time()
        # Validate raw schema exists
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'raw'
                AND table_name = 'yellow_taxi_trips'
            );
        """)
        exists = cur.fetchone()[0]
        assert exists, "❌ raw.yellow_taxi_trips does not exist — run Pipeline 1 first"

        # Return lightweight metadata only — no actual data loaded
        cur.execute("""
            SELECT source_year, source_month, COUNT(*) as row_count
            FROM raw.yellow_taxi_trips
            GROUP BY source_year, source_month
            ORDER BY source_year, source_month;
        """)
        results = cur.fetchall()
        print(f"⏱️ Metadata query took {round(time.time()-start, 2)}s")
    finally:
        cur.close()
        conn.close()

    df = pd.DataFrame(results, columns=['year', 'month', 'row_count'])
    print(f"📊 Raw data summary:")
    print(df.to_string())
    print(f"✅ Total rows in raw: {df['row_count'].sum():,}")
    return df


@test
def test_output(output, *args) -> None:
    assert output is not None, 'Output is undefined'
    assert len(output) > 0, 'No data found in raw'
    assert output['row_count'].sum() > 0, 'Raw table is empty'
    assert output['year'].between(2015, 2025).all(), 'Unexpected years in raw data'
    print(f"✓ Test passed: {len(output)} months, {output['row_count'].sum():,} total rows")
