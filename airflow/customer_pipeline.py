from airflow import DAG
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["shashankad702@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def quality_check():
    print("Checking processed data...")
    print("Validation successful")


with DAG(
    dag_id="customer_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["aws", "glue", "airflow", "athena"],
) as dag:

    check_processed_file = S3KeySensor(
        task_id="check_processed_file",
        bucket_name="my-bucket-name-2026-123",
        bucket_key="processed/*",
        wildcard_match=True,
        timeout=300,
        poke_interval=30,
        aws_conn_id="aws_default",
    )

    start_crawler = GlueCrawlerOperator(
        task_id="start_crawler",
        config={
            "Name": "emp-project"
        },
        aws_conn_id="aws_default",
    )

    wait_for_crawler = GlueCrawlerSensor(
        task_id="wait_for_crawler",
        crawler_name="emp-project",
        aws_conn_id="aws_default",
    )

    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=quality_check,
    )

    athena_query = AthenaOperator(
        task_id="athena_query",
        query="""
        SELECT COUNT(*) AS total_records
        FROM processed;
        """,
        database="employess_db",
        output_location="s3://my-bucket-name-2026-123/athena-output/",
        aws_conn_id="aws_default",
    )

    success_email = EmailOperator(
        task_id="success_email",
        to="shashankad702@gmail.com",
        subject="AWS Pipeline Success",
        html_content="""
        <h3>Pipeline Completed Successfully</h3>
        <p>Processed file found in S3.</p>
        <p>Glue crawler completed successfully.</p>
        <p>Data quality check passed.</p>
        <p>Athena query executed successfully.</p>
        """,
    )

    (
        check_processed_file
        >> start_crawler
        >> wait_for_crawler
        >> data_quality_check
        >> athena_query
        >> success_email
    )