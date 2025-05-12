from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define the function to be executed
def my_task():
    print("Hello, Airflow! This is my scheduled task.")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'airflow_dag',
    default_args=default_args,
    description='A simple DAG to run a Python function',
    schedule_interval='@hourly',  # Runs every hourly
    catchup=False
)

# Define the task
task = PythonOperator(
    task_id='run_task',
    python_callable=my_task,
    dag=dag
)

# Set task dependencies (if needed)
task  # This is a standalone task;