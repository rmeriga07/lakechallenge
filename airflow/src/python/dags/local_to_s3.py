from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook


def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('S3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG("s3_emr_sf", # Dag id
start_date=datetime(2022, 12 ,3), # start date, the 1st of January 2021 
schedule_interval='@daily',  # Cron expression, here it is a preset of Airflow, @daily means once every day.
catchup=False  # Catchup 
) as dag:

    # Upload the file
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/Users/RAHULMERIGA/Downloads/rmiller107-travel-time-uber-movement/Travel_Times - Paris.csv',
            'key': '/data/raw/travel_times_paris.csv',
            'bucket_name': 'mer-dlake-raw'
        }
    )