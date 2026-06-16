import os
import json
import boto3
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from botocore.client import Config

load_dotenv()

def get_mongo_records(batch_id=None):
    """Extract records from MongoDB"""
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('MONGO_DB')]
    collection = db[os.getenv('MONGO_COLLECTION')]

    # Filter by batch_id if provided
    query = {"batch_id": batch_id} if batch_id else {}
    records = list(collection.find(query))

    # Convert ObjectId to string for JSON serialisation
    for record in records:
        record['_id'] = str(record['_id'])

    client.close()
    print(f"Extracted {len(records)} records from MongoDB")
    return records

def get_minio_client():
    """Create and return MinIO client"""
    return boto3.client(
        's3',
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT')}",
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def upload_to_minio(records, batch_id):
    """Upload records to MinIO with date partitioning"""
    minio_client = get_minio_client()
    bucket = os.getenv('MINIO_BUCKET')

    # Create partitioned path: year=YYYY/month=MM/day=DD/
    now = datetime.now()
    partition_path = (
        f"year={now.strftime('%Y')}/"
        f"month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/"
        f"{batch_id}.json"
    )

    # Convert records to JSON
    json_data = json.dumps(records, indent=2, default=str)

    # Upload to MinIO
    minio_client.put_object(
        Bucket=bucket,
        Key=partition_path,
        Body=json_data.encode('utf-8'),
        ContentType='application/json'
    )

    print(f"Uploaded to MinIO: s3://{bucket}/{partition_path}")
    print(f"Records in file: {len(records)}")
    return partition_path

def run(batch_id=None):
    """Main extraction function"""
    print("Starting extraction: MongoDB -> MinIO")

    # Extract from MongoDB
    records = get_mongo_records(batch_id)

    if not records:
        print("No records found to extract")
        return

    # Get batch_id from first record if not provided
    if not batch_id:
        batch_id = records[0].get('batch_id', 'UNKNOWN')

    # Upload to MinIO
    path = upload_to_minio(records, batch_id)

    print(f"Extraction complete - {len(records)} records saved")
    return path

if __name__ == "__main__":
    run()