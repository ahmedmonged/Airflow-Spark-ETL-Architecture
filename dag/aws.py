from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def list_files_in_s3(**kwargs):
    bucket_name = 'airflow-aws-data-source'
    prefix = 'your-folder/'  # optional: '' to list all files at root level

    s3_hook = S3Hook(aws_conn_id='aws-s3-conn')
    
    # List all keys (file names) under the specified prefix
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix='')

    if keys:
        print("Files found in bucket:")
        for key in keys:
            print(key)
    else:
        print(f"No files found in bucket '{bucket_name}'.")

default_args = {
    'start_date': datetime(2025, 4, 7),
}

with DAG(
    dag_id='list_s3_files',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['s3', 'list', 'aws']
) as dag:

    list_files_task = PythonOperator(
        task_id='list_files_in_s3_bucket',
        python_callable=list_files_in_s3,
        provide_context=True
    )

    list_files_task
