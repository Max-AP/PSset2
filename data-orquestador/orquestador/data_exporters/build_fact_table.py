if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import psycopg2
from mage_ai.data_preparation.shared.secrets import get_secret_value

@data_exporter
def export_data(data, *args, **kwargs):
    print("🚀 Exporter started")

    user = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')
    host = get_secret_value('POSTGRES_HOST')
    port = get_secret_value('POSTGRES_PORT')
    db = get_secret_value('POSTGRES_DB')

    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    conn.autocommit = True
    cur = conn.cursor()

    # Index raw table first to speed up filter scan
    print("⏳ Indexing raw table...")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_trips_filter
        ON raw.yellow_taxi_trips (trip_distance, fare_amount, total_amount, passenger_count);
    """)
    print("✅ Raw index ready")

    # Drop old table and sequence
    cur.execute("DROP TABLE IF EXISTS clean.fact_trips;")
    cur.execute("DROP SEQUENCE IF EXISTS clean.fact_trips_trip_key_seq;")
    cur.execute("CREATE SEQUENCE clean.fact_trips_trip_key_seq;")

    # Build fact table using subquery instead of CTE to avoid materialization
    print("⏳ Building fact_trips — this may take a while for large datasets...")
    cur.execute("""
        CREATE TABLE clean.fact_trips AS
        SELECT
            NEXTVAL('clean.fact_trips_trip_key_seq')    AS trip_key,
            v.vendor_key,
            p.payment_key,
            pu.pickup_key,
            dl.dropoff_key,
            t.tpep_pickup_datetime,
            t.tpep_dropoff_datetime,
            EXTRACT(EPOCH FROM (
                t.tpep_dropoff_datetime - t.tpep_pickup_datetime
            )) / 60.0                                   AS trip_duration_minutes,
            t.trip_distance::float,
            t.fare_amount::float,
            t.tip_amount::float,
            t.tolls_amount::float,
            t.total_amount::float,
            t.passenger_count::int,
            t.source_year,
            t.source_month
        FROM (
            SELECT *
            FROM raw.yellow_taxi_trips
            WHERE
                trip_distance > 0
                AND fare_amount > 0
                AND total_amount > 0
                AND passenger_count > 0
                AND tpep_pickup_datetime IS NOT NULL
                AND tpep_dropoff_datetime IS NOT NULL
                AND tpep_dropoff_datetime > tpep_pickup_datetime
        ) t
        LEFT JOIN clean.dim_vendor v
            ON t.vendorid::int = v.vendor_id
        LEFT JOIN clean.dim_payment_type p
            ON t.payment_type::int = p.payment_id
        LEFT JOIN clean.dim_pickup_location pu
            ON t.pulocationid::int = pu.location_id
        LEFT JOIN clean.dim_dropoff_location dl
            ON t.dolocationid::int = dl.location_id;
    """)
    print("✅ fact_trips table created")

    # Add primary key
    cur.execute("ALTER TABLE clean.fact_trips ADD PRIMARY KEY (trip_key);")
    print("✅ Primary key added")

    # Indexes CONCURRENTLY — no table lock
    print("⏳ Building indexes...")
    cur.execute("CREATE INDEX CONCURRENTLY ON clean.fact_trips (vendor_key);")
    cur.execute("CREATE INDEX CONCURRENTLY ON clean.fact_trips (payment_key);")
    cur.execute("CREATE INDEX CONCURRENTLY ON clean.fact_trips (pickup_key);")
    cur.execute("CREATE INDEX CONCURRENTLY ON clean.fact_trips (dropoff_key);")
    cur.execute("CREATE INDEX CONCURRENTLY ON clean.fact_trips (source_year, source_month);")
    cur.execute("CREATE INDEX CONCURRENTLY ON clean.fact_trips (tpep_pickup_datetime);")
    print("✅ Indexes added")

    # Final row count
    cur.execute("SELECT COUNT(*) FROM clean.fact_trips;")
    count = cur.fetchone()[0]
    print(f"✅ fact_trips: {count:,} rows")

    # Quality check
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE vendor_key IS NULL)         AS null_vendors,
            COUNT(*) FILTER (WHERE payment_key IS NULL)        AS null_payments,
            COUNT(*) FILTER (WHERE trip_duration_minutes <= 0) AS invalid_duration,
            ROUND(MIN(trip_distance)::numeric, 2)              AS min_distance,
            ROUND(MAX(total_amount)::numeric, 2)               AS max_amount,
            ROUND(AVG(trip_duration_minutes)::numeric, 2)      AS avg_duration_mins
        FROM clean.fact_trips;
    """)
    qc = cur.fetchone()
    print(f"📊 Quality check:")
    print(f"   null vendors:       {qc[0]}")
    print(f"   null payments:      {qc[1]}")
    print(f"   invalid duration:   {qc[2]}")
    print(f"   min distance:       {qc[3]}")
    print(f"   max amount:         {qc[4]}")
    print(f"   avg duration (mins):{qc[5]}")

    cur.close()
    conn.close()
    print("✅ Pipeline 2 complete!")


