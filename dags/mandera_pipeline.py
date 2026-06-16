from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

# Add scripts folder to path
sys.path.insert(0, '/opt/airflow/scripts')

# Import pipeline scripts
import generate_data
import extract_to_minio
import extract_to_postgres
import transform_to_staging

# Default arguments for all tasks
default_args = {
    'owner': 'mandera',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='mandera_batch_pipeline',
    default_args=default_args,
    description='Mandera Analytics batch pipeline',
    schedule_interval='0 6 * * *',  # Run daily at 6am
    catchup=False,
    tags=['mandera', 'batch', 'analytics'],
) as dag:

    # Task 1: Generate synthetic data
    task_generate = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data.run,
    )

    # Task 2: Extract to MinIO
    task_minio = PythonOperator(
        task_id='extract_to_minio',
        python_callable=extract_to_minio.run,
    )

    # Task 3: Extract to PostgreSQL raw
    task_postgres = PythonOperator(
        task_id='extract_to_postgres',
        python_callable=extract_to_postgres.run,
    )

    # Task 4: Transform raw to staging
    task_transform = PythonOperator(
        task_id='transform_to_staging',
        python_callable=transform_to_staging.run,
    )

    # Define task dependencies
    task_generate >> task_minio >> task_postgres >> task_transform
