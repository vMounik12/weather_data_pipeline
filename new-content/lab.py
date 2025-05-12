from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'mouni',
    'start_date': datetime(2025, 2, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'lab_scheduler',
    default_args=default_args,
    description='Run lab.py every hour',
    schedule_interval=' 35 10-15 * * *',  # Runs every hour
    catchup=False,
)

# Task to execute lab.py from PyCharm folder
run_lab_script = BashOperator(
    task_id='run_lab_script',
    bash_command='python3 /Users/mvalleri/lucy/lab.py',
    dag=dag,
)