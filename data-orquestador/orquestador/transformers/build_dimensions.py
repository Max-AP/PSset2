if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import time
import psycopg2
from mage_ai.data_preparation.shared.secrets import get_secret_value

@transformer
def transform(data, *args, **kwargs):
    user = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')
    host = get_secret_value('POSTGRES_HOST')
    port = get_secret_value('POSTGRES_PORT')
    db = get_secret_value('POSTGRES_DB')

    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    print("🚀 Transformer started")

    try:
        cur = conn.cursor()
        start = time.time()

        # Create clean schema if not exists
        cur.execute("CREATE SCHEMA IF NOT EXISTS clean;")
        conn.commit()

        # --- dim_vendor ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean.dim_vendor (
                vendor_key  SERIAL PRIMARY KEY,
                vendor_id   INT UNIQUE,
                vendor_name TEXT
            );
        """)
        cur.execute("""
            INSERT INTO clean.dim_vendor (vendor_id, vendor_name)
            SELECT
                vendorid::int AS vendor_id,
                CASE vendorid::int
                    WHEN 1 THEN 'Creative Mobile Technologies'
                    WHEN 2 THEN 'VeriFone Inc'
                    ELSE 'Unknown'
                END AS vendor_name
            FROM (
                SELECT DISTINCT vendorid
                FROM raw.yellow_taxi_trips
                WHERE vendorid IS NOT NULL
            ) v
            ON CONFLICT (vendor_id) DO NOTHING;
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_vendor_id ON clean.dim_vendor (vendor_id);")

        # --- dim_payment_type ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean.dim_payment_type (
                payment_key         SERIAL PRIMARY KEY,
                payment_id          INT UNIQUE,
                payment_description TEXT
            );
        """)
        cur.execute("""
            INSERT INTO clean.dim_payment_type (payment_id, payment_description)
            SELECT
                payment_type::int AS payment_id,
                CASE payment_type::int
                    WHEN 1 THEN 'Credit Card'
                    WHEN 2 THEN 'Cash'
                    WHEN 3 THEN 'No Charge'
                    WHEN 4 THEN 'Dispute'
                    WHEN 5 THEN 'Unknown'
                    WHEN 6 THEN 'Voided Trip'
                    ELSE 'Unknown'
                END AS payment_description
            FROM (
                SELECT DISTINCT payment_type
                FROM raw.yellow_taxi_trips
                WHERE payment_type IS NOT NULL
            ) p
            ON CONFLICT (payment_id) DO NOTHING;
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_payment_id ON clean.dim_payment_type (payment_id);")

        # --- dim_pickup_location ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean.dim_pickup_location (
                pickup_key  SERIAL PRIMARY KEY,
                location_id INT UNIQUE
            );
        """)
        cur.execute("""
            INSERT INTO clean.dim_pickup_location (location_id)
            SELECT DISTINCT pulocationid::int
            FROM raw.yellow_taxi_trips
            WHERE pulocationid IS NOT NULL
            ON CONFLICT (location_id) DO NOTHING;
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_pickup_loc ON clean.dim_pickup_location (location_id);")

        # --- dim_dropoff_location ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean.dim_dropoff_location (
                dropoff_key SERIAL PRIMARY KEY,
                location_id INT UNIQUE
            );
        """)
        cur.execute("""
            INSERT INTO clean.dim_dropoff_location (location_id)
            SELECT DISTINCT dolocationid::int
            FROM raw.yellow_taxi_trips
            WHERE dolocationid IS NOT NULL
            ON CONFLICT (location_id) DO NOTHING;
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_dropoff_loc ON clean.dim_dropoff_location (location_id);")

        conn.commit()
        print(f"⏱️ Dimensions updated in {round(time.time() - start, 2)}s")

        # Log results
        for table in ['dim_vendor', 'dim_payment_type', 'dim_pickup_location', 'dim_dropoff_location']:
            cur.execute(f"SELECT COUNT(*) FROM clean.{table};")
            print(f"✅ clean.{table}: {cur.fetchone()[0]} rows")

        cur.close()

    except Exception as e:
        conn.rollback()
        print(f"❌ Transformer failed: {e}")
        raise
    finally:
        conn.close()

    print(f"✅ All dimensions ready in {round(time.time() - start, 2)}s total")
    return data