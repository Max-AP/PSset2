if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import io
import time
import os
import shutil
import gc
import psycopg2
from sqlalchemy import create_engine
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

    year = int(data['source_year'].iloc[0])
    month = int(data['source_month'].iloc[0])
    total_rows = len(data)
    start = time.time()

    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)

    try:
        cur = conn.cursor()

        # Create schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        conn.commit()

        # Create table if not exists
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        data.head(0).to_sql('yellow_taxi_trips', engine, schema='raw', if_exists='append', index=False)
        engine.dispose()
        gc.collect()

        # Check how many rows already exist for this month
        try:
            cur.execute(f"""
                SELECT COUNT(*) FROM raw.yellow_taxi_trips
                WHERE source_year = {year} AND source_month = {month};
            """)
            existing_rows = cur.fetchone()[0]
        except Exception:
            existing_rows = 0
            conn.rollback()

        print(f"📊 {year}-{month:02d}: {total_rows} rows incoming, {existing_rows} already in DB")

        # Skip if already complete
        if existing_rows >= total_rows:
            print(f"✅ {year}-{month:02d} already complete, skipping")
            return

        # Resume from where we left off
        if existing_rows > 0:
            print(f"⏩ Resuming {year}-{month:02d} from row {existing_rows}")
            data = data.iloc[existing_rows:].reset_index(drop=True)
        else:
            print(f"🚀 Starting fresh load for {year}-{month:02d}")

        # Write remaining rows using COPY
        chunk_size = 50000
        total_to_write = len(data)
        written = 0

        for i in range(0, total_to_write, chunk_size):
            chunk = data.iloc[i:i+chunk_size]
            buffer = io.StringIO()
            chunk.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cur.copy_expert("COPY raw.yellow_taxi_trips FROM STDIN WITH CSV", buffer)
            conn.commit()
            written += len(chunk)
            print(f"✓ {year}-{month:02d}: {existing_rows + written}/{total_rows} rows ({round((existing_rows + written)/total_rows*100)}%)")
            del chunk
            gc.collect()

        # Quality check
        cur.execute(f"""
            SELECT
                COUNT(*)                                        AS total_rows,
                COUNT(*) FILTER (WHERE vendorid IS NULL)        AS null_vendors,
                COUNT(*) FILTER (WHERE payment_type IS NULL)    AS null_payments,
                ROUND(MIN(trip_distance)::numeric, 2)           AS min_distance,
                ROUND(MAX(total_amount)::numeric, 2)            AS max_amount
            FROM raw.yellow_taxi_trips
            WHERE source_year = {year} AND source_month = {month};
        """)
        qc = cur.fetchone()
        print(f"📊 Quality check for {year}-{month:02d}:")
        print(f"   total rows:     {qc[0]:,}")
        print(f"   null vendors:   {qc[1]}")
        print(f"   null payments:  {qc[2]}")
        print(f"   min distance:   {qc[3]}")
        print(f"   max amount:     {qc[4]}")

        print(f"✅ Done! {year}-{month:02d} complete in {round(time.time() - start, 2)}s")

        cur.close()

    except Exception as e:
        conn.rollback()
        print(f"❌ Exporter failed for {year}-{month:02d}: {e}")
        raise
    finally:
        conn.close()

        # Clean up Mage block cache for this run
        try:
            execution_partition = kwargs.get('execution_partition')
            dynamic_index = kwargs.get('dynamic_block_index')
            base = f'/home/src/mage_data/orquestador/pipelines/raw_ingestion_pipeline/.variables/{execution_partition}'

            for block_name in ['download_raw_data', 'prepare_raw_data', 'load_to_postgres_raw']:
                block_path = os.path.join(base, block_name, str(dynamic_index))
                if os.path.exists(block_path):
                    shutil.rmtree(block_path)
                    print(f"🧹 Cleared: {block_name}/{dynamic_index}")
        except Exception as e:
            print(f"⚠️ Cache cleanup failed (non-critical): {e}")