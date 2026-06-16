import os
import uuid
from datetime import datetime
from dotenv import load_dotenv
from faker import Faker
from pymongo import MongoClient

# Load environment variables
load_dotenv()

# Initialise Faker
fake = Faker()

def generate_batch_id():
    """Generate a unique batch identifier with timestamp"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"BATCH_{timestamp}"

def generate_transaction(batch_id):
    """Generate a single fake transaction record"""
    return {
        "transaction_id": str(uuid.uuid4()),
        "batch_id": batch_id,
        "customer_id": f"CUST_{fake.numerify('####')}",
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "region": fake.random_element([
            "North", "South", "East", "West", "Central"
        ]),
        "product_id": f"PROD_{fake.numerify('###')}",
        "product_name": fake.random_element([
            "Office Supplies", "Electronics", "Furniture",
            "Stationery", "Packaging", "Cleaning Products"
        ]),
        "quantity": fake.random_int(min=1, max=50),
        "unit_price": round(fake.random_number(digits=3) / 10, 2),
        "payment_method": fake.random_element([
            "card", "cash", "bank_transfer", "mobile_money"
        ]),
        "status": fake.random_element([
            "completed", "pending", "failed"
        ]),
        "transaction_date": fake.date_time_between(
            start_date="-30d", end_date="now"
        ).isoformat(),
        "created_at": datetime.now().isoformat()
    }

def run(batch_size=None):
    """Main function to generate and insert transactions"""

    # Connect to MongoDB
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('MONGO_DB')]
    collection = db[os.getenv('MONGO_COLLECTION')]

    # Generate batch
    batch_size = batch_size or int(os.getenv('BATCH_SIZE', 100))
    batch_id = generate_batch_id()

    print(f"Generating batch: {batch_id}")
    print(f"Batch size: {batch_size} records")

    # Generate records
    records = [generate_transaction(batch_id) for _ in range(batch_size)]

    # Insert into MongoDB
    result = collection.insert_many(records)

    print(f"Inserted {len(result.inserted_ids)} records into MongoDB")
    print(f"Database: {os.getenv('MONGO_DB')}")
    print(f"Collection: {os.getenv('MONGO_COLLECTION')}")
    print(f"Batch ID: {batch_id}")

    client.close()
    return batch_id

if __name__ == "__main__":
    run()
