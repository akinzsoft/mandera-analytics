import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

def get_mongo_records(batch_id=None):
    """Extract records from MongoDB"""
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('MONGO_DB')]
    collection = db[os.getenv('MONGO_COLLECTION')]

    query = {"batch_id": batch_id} if batch_id else {}
    records = list(collection.find(query))

    for record in records:
        record['_id'] = str(record['_id'])

    client.close()
    print(f"Extracted {len(records)} records from MongoDB")
    return records

def get_postgres_connection():
    """Create and return PostgreSQL connection"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

def insert_raw_records(records, conn):
    """Insert records into raw_transactions table"""
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO raw_transactions (
            transaction_id, batch_id, customer_id,
            customer_name, customer_email, region,
            product_id, product_name, quantity,
            unit_price, payment_method, status,
            transaction_date, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s
        )
    """

    rows = []
    for r in records:
        rows.append((
            r.get('transaction_id'),
            r.get('batch_id'),
            r.get('customer_id'),
            r.get('customer_name'),
            r.get('customer_email'),
            r.get('region'),
            r.get('product_id'),
            r.get('product_name'),
            r.get('quantity'),
            r.get('unit_price'),
            r.get('payment_method'),
            r.get('status'),
            r.get('transaction_date'),
            r.get('created_at')
        ))

    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    print(f"Inserted {len(rows)} records into raw_transactions")
    return len(rows)

def log_batch(batch_id, row_count, expected_count, conn):
    """Log batch metadata to batch_log table"""
    cursor = conn.cursor()
    variance = row_count - expected_count

    cursor.execute("""
        INSERT INTO batch_log (
            batch_id, source, row_count,
            expected_count, variance, status
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (batch_id) DO UPDATE
        SET row_count = EXCLUDED.row_count,
            variance = EXCLUDED.variance,
            status = EXCLUDED.status
    """, (
        batch_id,
        'mongodb',
        row_count,
        expected_count,
        variance,
        'loaded' if variance == 0 else 'variance_detected'
    ))

    conn.commit()
    cursor.close()

    print(f"Batch log updated:")
    print(f"  Batch ID: {batch_id}")
    print(f"  Row count: {row_count}")
    print(f"  Expected: {expected_count}")
    print(f"  Variance: {variance}")
    print(f"  Status: {'OK' if variance == 0 else 'VARIANCE DETECTED'}")

def run(batch_id=None):
    """Main extraction function MongoDB -> PostgreSQL"""
    print("Starting extraction: MongoDB -> PostgreSQL")

    # Extract from MongoDB
    records = get_mongo_records(batch_id)

    if not records:
        print("No records found to extract")
        return

    # Get batch_id from first record if not provided
    if not batch_id:
        batch_id = records[0].get('batch_id', 'UNKNOWN')

    # Connect to PostgreSQL
    conn = get_postgres_connection()

    # Insert raw records
    row_count = insert_raw_records(records, conn)

    # Log batch metadata
    expected = int(os.getenv('BATCH_SIZE', 100))
    log_batch(batch_id, row_count, expected, conn)

    conn.close()
    print(f"Extraction complete - {row_count} records in raw_transactions")
    return row_count

if __name__ == "__main__":
    run()
