from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from scripts.linkedin_api import jobs_linkedin
from scripts.linkedin_job import create_linkedin_jobs
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "youremail@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "job_ads",
    start_date=datetime(2023, 6, 18),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    job_search_tasks = TaskGroup("job_search_tasks")
    jobs_search_locations = [
        "Berlin",
        "North Rhine-Westphalia",
        "Germany",
        "Mumbai",
        "Bangalore",
    ]
    jobs_positions = ["Data Analyst", "Data Scientist", "Data Engineer"]
    for location in jobs_search_locations:
        for position in jobs_positions:
            jobs_in_location = PythonOperator(
                task_id=f"jobs_{position.replace(' ', '_').lower()}_{location.replace(' ', '_').lower()}",
                python_callable=jobs_linkedin,
                op_kwargs={
                    "search_terms": position,
                    "location": location,
                    "page": "1",
                },
            )

    job_available = FileSensor(
        task_id="job_available",
        fs_conn_id="jobs_path",
        filepath=f"*_job.json",
        poke_interval=5,
        timeout=20,
    )

    combine_json_task = PythonOperator(
        task_id="combine_json_task",
        python_callable=create_linkedin_jobs,
        provide_context=True,
    )

    save_linkedin_jobs = BashOperator(
        task_id="save_linkedin_jobs",
        bash_command="""
            hdfs dfs -mkdir -p /job && \ 
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/job.csv /job
        """,
    )

    linkedin_jobs_table = HiveOperator(
        task_id="linkedin_jobs_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS job_posting(
                linkedin_job_url_cleaned STRING,
                company_name STRING,
                company_url STRING,
                job_title STRING,
                job_location STRING,
                posted_date STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """,
    )

    job_processing = SparkSubmitOperator(
        task_id="job_processing",
        application="/opt/airflow/dags/scripts/job_processing.py",
        conn_id="spark_conn",
        verbose=False,
    )

    (
        job_search_tasks
        >> job_available
        >> combine_json_task
        >> save_linkedin_jobs
        >> linkedin_jobs_table
        >> job_processing
    )
