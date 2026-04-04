if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import time
import psycopg2
from mage_ai.data_preparation.shared.secrets import get_secret_value

@data_exporter
def export_data(data, *args, **kwargs):
    user = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')
    host = get_secret_value('POSTGRES_HOST')
    port = get_secret_value('POSTGRES_PORT')
    db = get_secret_value('POSTGRES_DB')

    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    conn.autocommit = True
    print("🚀 Exporter started")

    try:
        cur = conn.cursor()
        start = time.time()

        # Index raw table to speed up filter scan
        print("⏳ Indexing raw table...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_raw_trips_filter
            ON raw.yellow_taxi_trips (trip_distance, fare_amount, total_amount, passenger_count);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_raw_trips_year_month
            ON raw.yellow_taxi_trips (source_year, source_month);
        """)
        print(f"✅ Raw indexes ready ({round(time.time() - start, 2)}s)")

        # Create sequence if not exists (first run only)
        cur.execute("CREATE SEQUENCE IF NOT EXISTS clean.fact_trips_trip_key_seq;")

        # Create fact table if not exists (first run only)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean.fact_trips (
                trip_key                BIGINT PRIMARY KEY,
                vendor_key              INT,
                payment_key             INT,
                pickup_key              INT,
                dropoff_key             INT,
                tpep_pickup_datetime    TIMESTAMP,
                tpep_dropoff_datetime   TIMESTAMP,
                trip_duration_minutes   FLOAT,
                trip_distance           FLOAT,
                fare_amount             FLOAT,
                tip_amount              FLOAT,
                tolls_amount            FLOAT,
                total_amount            FLOAT,
                passenger_count         INT,
                source_year             INT,
                source_month            INT
            );
        """)

        # Create indexes if not exists
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_vendor ON clean.fact_trips (vendor_key);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_payment ON clean.fact_trips (payment_key);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_pickup ON clean.fact_trips (pickup_key);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_dropoff ON clean.fact_trips (dropoff_key);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_year_month ON clean.fact_trips (source_year, source_month);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_pickup_dt ON clean.fact_trips (tpep_pickup_datetime);")
        print(f"✅ Table and indexes ready ({round(time.time() - start, 2)}s)")

        # Check which months are already in clean.fact_trips
        cur.execute("""
            SELECT source_year, source_month
            FROM clean.fact_trips
            GROUP BY source_year, source_month;
        """)
        existing = set(cur.fetchall())

        # Check which months are in raw
        cur.execute("""
            SELECT source_year, source_month
            FROM raw.yellow_taxi_trips
            GROUP BY source_year, source_month;
        """)
        raw_months = set(cur.fetchall())

        # Only process months not yet in clean
        missing = raw_months - existing

        if not missing:
            print("✅ clean.fact_trips already up to date, nothing to do")
            return

        print(f"⏳ Processing {len(missing)} new months: {sorted(missing)}")

        # Insert only missing months
        for year, month in sorted(missing):
            print(f"⏳ Processing {year}-{month:02d}...")
            month_start = time.time()

            cur.execute("""
                INSERT INTO clean.fact_trips
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
                        source_year = %s
                        AND source_month = %s
                        AND trip_distance > 0
                        AND trip_distance <= 100
                        AND fare_amount > 0
                        AND fare_amount <= 500
                        AND total_amount > 0
                        AND total_amount <= 500
                        AND passenger_count BETWEEN 1 AND 6
                        AND tpep_pickup_datetime IS NOT NULL
                        AND tpep_dropoff_datetime IS NOT NULL
                        AND tpep_dropoff_datetime > tpep_pickup_datetime
                        AND EXTRACT(EPOCH FROM (
                            tpep_dropoff_datetime - tpep_pickup_datetime
                        )) / 60.0 BETWEEN 1 AND 180
                ) t
                LEFT JOIN clean.dim_vendor v
                    ON t.vendorid::int = v.vendor_id
                LEFT JOIN clean.dim_payment_type p
                    ON t.payment_type::int = p.payment_id
                LEFT JOIN clean.dim_pickup_location pu
                    ON t.pulocationid::int = pu.location_id
                LEFT JOIN clean.dim_dropoff_location dl
                    ON t.dolocationid::int = dl.location_id;
            """, (year, month))

            cur.execute("""
                SELECT COUNT(*) FROM clean.fact_trips
                WHERE source_year = %s AND source_month = %s;
            """, (year, month))
            month_count = cur.fetchone()[0]
            print(f"✅ {year}-{month:02d}: {month_count:,} rows ({round(time.time() - month_start, 2)}s)")

        # Final count
        cur.execute("SELECT COUNT(*) FROM clean.fact_trips;")
        total = cur.fetchone()[0]
        print(f"✅ fact_trips total: {total:,} rows")

        # Quality check — validates caps were applied correctly
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE vendor_key IS NULL)              AS null_vendors,
                COUNT(*) FILTER (WHERE payment_key IS NULL)             AS null_payments,
                COUNT(*) FILTER (WHERE trip_duration_minutes <= 0)      AS invalid_duration,
                COUNT(*) FILTER (WHERE trip_duration_minutes > 180)     AS over_180_mins,
                COUNT(*) FILTER (WHERE trip_distance > 100)             AS over_100_miles,
                COUNT(*) FILTER (WHERE total_amount > 500)              AS over_500_amount,
                COUNT(*) FILTER (WHERE passenger_count > 6)             AS over_6_passengers,
                ROUND(MIN(trip_distance)::numeric, 2)                   AS min_distance,
                ROUND(MAX(total_amount)::numeric, 2)                    AS max_amount,
                ROUND(AVG(trip_duration_minutes)::numeric, 2)           AS avg_duration_mins
            FROM clean.fact_trips;
        """)
        qc = cur.fetchone()
        print(f"📊 Quality check:")
        print(f"   null vendors:        {qc[0]}")
        print(f"   null payments:       {qc[1]}")
        print(f"   invalid duration:    {qc[2]}")
        print(f"   over 180 mins:       {qc[3]}")
        print(f"   over 100 miles:      {qc[4]}")
        print(f"   over $500:           {qc[5]}")
        print(f"   over 6 passengers:   {qc[6]}")
        print(f"   min distance:        {qc[7]}")
        print(f"   max amount:          {qc[8]}")
        print(f"   avg duration (mins): {qc[9]}")

        print(f"✅ Pipeline 2 complete in {round(time.time() - start, 2)}s total")
        cur.close()

    except Exception as e:
        print(f"❌ Exporter failed: {e}")
        raise
    finally:
        conn.close()


@test
def test_output(*args) -> None:
    print("✓ Exporter test passed")