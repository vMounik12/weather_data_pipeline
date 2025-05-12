from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'postgres_backup_dag',
    default_args=default_args,
    description='DAG for scheduling PostgreSQL backups every 5 minutes',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
)

# Database connection details
db_host = "localhost"
db_name = "earth"
db_user = "postgres"
db_port = 5432

# Function to run the pg_dump command with the specified file path
def run_backup():
    try:
        backup_file_path = "/Users/mvalleri/Documents/eco.backup"

        # Ensure the directory exists
        directory = os.path.dirname(backup_file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # pg_dump command with the file path
        backup_command = [
            "pg_dump",
            "-h", db_host,
            "-U", db_user,
            "-p", str(db_port),
            "-d", db_name,
            "-F", "c",  # Custom format
            "-b",
            "-v",
            "-f", backup_file_path  # File path
        ]

        # Run the command
        result = subprocess.run(backup_command, check=True, capture_output=True, text=True)
        print("Backup completed successfully.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"An error occurred during the backup process: {e.output}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Task definition
backup_task = PythonOperator(
    task_id='run_backup',
    python_callable=run_backup,
    dag=dag,
)

# DAG structure
backup_task
