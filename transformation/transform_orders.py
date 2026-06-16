import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_postgres_connection():
    """Create and return PostgreSQL connection"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

def get_raw_records(conn, batch_id=None):
    """Read records from raw_transactions"""
    cursor = conn.cursor()

    if batch_id:
        cursor.execute("""
            SELECT transaction_id, batch_id, customer_id,
                   customer_name, customer_email, region,
                   product_id, product_name, quantity,
                   unit_price, payment_method, status,
                   transaction_date, created_at
            FROM raw_transactions
            WHERE batch_id = %s
        """, (batch_id,))
    else:
        cursor.execute("""
            SELECT transaction_id, batch_id, customer_id,
                   customer_name, customer_email, region,
                   product_id, product_name, quantity,
                   unit_price, payment_method, status,
                   transaction_date, created_at
            FROM raw_transactions
        """)

    records = cursor.fetchall()
    cursor.close()
    print(f"Read {len(records)} records from raw_transactions")
    return records

def transform_record(record):
    """Apply transformations to a single record"""
    (
        transaction_id, batch_id, customer_id,
        customer_name, customer_email, region,
        product_id, product_name, quantity,
        unit_price, payment_method, status,
        transaction_date, created_at
    ) = record

    # Fix 1: Calculate total_amount
    total_amount = round(
        (quantity or 0) * float(unit_price or 0), 2
    )

    # Fix 2: Standardise payment method to lowercase
    payment_method = (payment_method or 'unknown').lower()

    # Fix 3: Standardise status to lowercase
    status = (status or 'unknown').lower()

    # Fix 4: Standardise region to title case
    region = (region or 'unknown').title()

    # Fix 5: Standardise customer name to title case
    customer_name = (customer_name or '').title()

    # Fix 6: Standardise email to lowercase
    customer_email = (customer_email or '').lower()

    # Fix 7: Extract date only from transaction_date
    if transaction_date:
        if isinstance(transaction_date, str):
            transaction_date = transaction_date[:10]
        else:
            transaction_date = str(transaction_date)[:10]

    return (
        transaction_id, batch_id, customer_id,
        customer_name, customer_email, region,
        product_id, product_name, quantity,
        unit_price, total_amount, payment_method,
        status, transaction_date, created_at
    )

def insert_staging_records(records, conn):
    """Insert transformed records into stg_transactions"""
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO stg_transactions (
            transaction_id, batch_id, customer_id,
            customer_name, customer_email, region,
            product_id, product_name, quantity,
            unit_price, total_amount, payment_method,
            status, transaction_date, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (transaction_id) DO NOTHING
    """

    transformed = [transform_record(r) for r in records]
    cursor.executemany(insert_sql, transformed)
    conn.commit()
    cursor.close()

    print(f"Inserted {len(transformed)} records into stg_transactions")
    return len(transformed)

def truncate_raw(conn):
    """Truncate raw_transactions after successful transformation"""
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE raw_transactions;")
    conn.commit()
    cursor.close()
    print("raw_transactions truncated successfully")

def update_batch_log(batch_id, stg_count, conn):
    """Update batch log with staging count"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE batch_log
        SET status = %s
        WHERE batch_id = %s
    """, ('transformed', batch_id))
    conn.commit()
    cursor.close()
    print(f"Batch log updated to: transformed")

def run(batch_id=None):
    """Main transformation function"""
    print("Starting transformation: raw -> staging")

    conn = get_postgres_connection()

    # Read raw records
    records = get_raw_records(conn, batch_id)

    if not records:
        print("No records found in raw_transactions")
        conn.close()
        return

    # Get batch_id from first record
    if not batch_id:
        batch_id = records[0][1]

    # Insert into staging
    stg_count = insert_staging_records(records, conn)

    # Update batch log
    update_batch_log(batch_id, stg_count, conn)

    # Truncate raw table
    truncate_raw(conn)

    conn.close()
    print(f"Transformation complete")
    print(f"  Batch: {batch_id}")
    print(f"  Records transformed: {stg_count}")
    print(f"  raw_transactions: truncated")
    return stg_count

if __name__ == "__main__":
    run()
