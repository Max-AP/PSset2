if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import psycopg2
from mage_ai.data_preparation.shared.secrets import get_secret_value

@transformer
def transform(data, *args, **kwargs):
    user = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')
    host = get_secret_value('POSTGRES_HOST')
    port = get_secret_value('POSTGRES_PORT')
    db = get_secret_value('POSTGRES_DB')
    print("🚀 Transformer started")

    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    cur = conn.cursor()

    # Single transaction for all dimension builds
    cur.execute("CREATE SCHEMA IF NOT EXISTS clean;")

    # dim_vendor
    cur.execute("DROP TABLE IF EXISTS clean.dim_vendor;")
    cur.execute("""
        CREATE TABLE clean.dim_vendor AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY vendor_id) AS vendor_key,
            vendor_id,
            CASE vendor_id
                WHEN 1 THEN 'Creative Mobile Technologies'
                WHEN 2 THEN 'VeriFone Inc'
                ELSE 'Unknown'
            END AS vendor_name
        FROM (
            SELECT DISTINCT vendorid::int AS vendor_id
            FROM raw.yellow_taxi_trips
            WHERE vendorid IS NOT NULL
        ) v;
    """)

    # dim_payment_type
    cur.execute("DROP TABLE IF EXISTS clean.dim_payment_type;")
    cur.execute("""
        CREATE TABLE clean.dim_payment_type AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY payment_id) AS payment_key,
            payment_id,
            CASE payment_id
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided Trip'
                ELSE 'Unknown'
            END AS payment_description
        FROM (
            SELECT DISTINCT payment_type::int AS payment_id
            FROM raw.yellow_taxi_trips
            WHERE payment_type IS NOT NULL
        ) p;
    """)

    # dim_pickup_location
    cur.execute("DROP TABLE IF EXISTS clean.dim_pickup_location;")
    cur.execute("""
        CREATE TABLE clean.dim_pickup_location AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY location_id) AS pickup_key,
            location_id
        FROM (
            SELECT DISTINCT pulocationid::int AS location_id
            FROM raw.yellow_taxi_trips
            WHERE pulocationid IS NOT NULL
        ) l;
    """)

    # dim_dropoff_location
    cur.execute("DROP TABLE IF EXISTS clean.dim_dropoff_location;")
    cur.execute("""
        CREATE TABLE clean.dim_dropoff_location AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY location_id) AS dropoff_key,
            location_id
        FROM (
            SELECT DISTINCT dolocationid::int AS location_id
            FROM raw.yellow_taxi_trips
            WHERE dolocationid IS NOT NULL
        ) l;
    """)

    # Single commit for all tables
    conn.commit()

    # Add primary keys and indexes for fast joins in fact table
    cur.execute("ALTER TABLE clean.dim_vendor ADD PRIMARY KEY (vendor_key);")
    cur.execute("CREATE INDEX ON clean.dim_vendor (vendor_id);")

    cur.execute("ALTER TABLE clean.dim_payment_type ADD PRIMARY KEY (payment_key);")
    cur.execute("CREATE INDEX ON clean.dim_payment_type (payment_id);")

    cur.execute("ALTER TABLE clean.dim_pickup_location ADD PRIMARY KEY (pickup_key);")
    cur.execute("CREATE INDEX ON clean.dim_pickup_location (location_id);")

    cur.execute("ALTER TABLE clean.dim_dropoff_location ADD PRIMARY KEY (dropoff_key);")
    cur.execute("CREATE INDEX ON clean.dim_dropoff_location (location_id);")

    conn.commit()

    # Log results
    for table in ['dim_vendor', 'dim_payment_type', 'dim_pickup_location', 'dim_dropoff_location']:
        cur.execute(f"SELECT COUNT(*) FROM clean.{table};")
        print(f"✅ clean.{table}: {cur.fetchone()[0]} rows")

    cur.close()
    conn.close()

    print("✅ All dimensions built and indexed successfully")
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
