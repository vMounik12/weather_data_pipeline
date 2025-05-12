from flask import Flask, request, jsonify
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import threading

# --- Flask Application ---

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
        backup_file_name = f"{backup_dir}/my.backup"

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


# Start the Flask app in a separate thread so it doesn't block the Airflow tasks
def run_flask_app():
    app.run(debug=True, use_reloader=False, port=5000)


# --- Airflow DAG ---

# Function to trigger Flask /backup endpoint
def trigger_backup():
    flask_url = "http://localhost:5000/backup"  # Make sure Flask app is running on this URL
    backup_dir = "/path/to/backup"  # Specify the backup directory path

    # Send a GET request to the Flask /backup endpoint
    response = requests.get(f"{flask_url}?backup_dir={backup_dir}")

    # Check the response from Flask
    if response.status_code == 200:
        print("Backup completed successfully!")
    else:
        print(f"Error: {response.json().get('error')}")


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
    'flask_backup_airflow_dag',
    default_args=default_args,
    description='DAG to trigger Flask app for database backup',
    schedule_interval='@hourly',  # Runs every hour
    catchup=False
)

# Define the task to trigger the Flask backup
backup_task = PythonOperator(
    task_id='trigger_flask_backup',
    python_callable=trigger_backup,
    dag=dag
)

# Set task dependencies (if needed)
backup_task  # This is a standalone task; you can chain other tasks if needed

# --- Running Flask App & Airflow DAG ---

if __name__ == '__main__':
    # Run Flask app in a separate thread to allow Airflow tasks to run concurrently
    flask_thread = threading.Thread(target=run_flask_app)
    flask_thread.start()

    # Airflow-related code would typically be managed by the Airflow scheduler,
    # but for testing purposes, we can simulate it by manually running the DAG.
    from airflow.utils.dates import days_ago
    from airflow.models import DagBag

    # Load and run the DAG manually (for testing purposes)
    dagbag = DagBag()
    dag = dagbag.get_dag('flask_backup_airflow_dag')
    dag.run(start_date=days_ago(1), end_date=datetime.now())
