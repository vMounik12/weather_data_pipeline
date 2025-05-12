from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
import subprocess

# Define the function to be executed in Airflow DAG
def my_task():
    print("Hello, Airflow! This is my scheduled task.")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 18),  # Adjust as needed
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'myairflow_dag',
    default_args=default_args,
    description='A simple DAG to run a Python function',
    schedule_interval='@hourly',  # Runs every hourly
    catchup=False
)

# Define the task in the DAG
task = PythonOperator(
    task_id='run_mytask',
    python_callable=my_task,
    dag=dag
)

# Flask app to handle database backup
app = Flask(__name__)

# Database connection details
db_host = "localhost"
db_name = "mydatabase"
db_user = "postgres"
db_port = "5432"

# Endpoint to backup the database using a GET request
@app.route('/backup', methods=['GET'])
def backup_database():
    try:
        # Get the backup directory path from the query parameters
        backup_dir = request.args.get('backup_dir')
        if not backup_dir:
            return jsonify({"error": "Backup directory is required."}), 400

        # Set the full backup file path
        backup_file_name = f"{backup_dir}/mybacks.backup"

        # Run the pg_dump command with explicit connection options
        result = subprocess.run(
            [
                "pg_dump",
                "-h", db_host,
                "-U", db_user,
                "-p", db_port,
                "-d", db_name,
                "-F", "c",  # Custom format
                "-b",
                "-v",
                "-f", backup_file_name
            ],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            return jsonify({"message": "Backup completed successfully."}), 200
        else:
            return jsonify({"error": f"Backup failed: {result.stderr}"}), 500

    except subprocess.CalledProcessError as e:
        return jsonify({"error": f"An error occurred: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True)
