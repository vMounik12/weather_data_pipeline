from flask import Flask,jsonify
import psycopg2

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from dag_with_cron_expression import default_args

app = Flask(__name__)
db_host='localhost'
db_name='what'
db_user='postgres'
db_port=5432
def get_db_connection():
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        port=db_port
    )
    return conn

# Route to fetch all rows from the table
@app.route('/we', methods=['GET'])
def get_table_data():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Fetch all rows from the table
        cur.execute('SELECT * FROM we;')
        data = cur.fetchall()

        # Close the connection
        cur.close()
        conn.close()

        # Convert data into a JSON format
        data_list = [{'id': row[0], 'name': row[1]} for row in data]
        return jsonify(data_list)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

efault_args = {
        'owner': 'airflow',
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

with DAG(
    default_args=default_args,
    dag_id="base",
    start_date=datetime(2025, 2, 6),
    schedule_interval='30 1-5 * * *'
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo dag with cron exception!"
    )
    task1

# Main entry point
if __name__ == '__main__':
    app.run(debug=True)
