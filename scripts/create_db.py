import sqlite3
import os

def check_sqlite_connection():
    try:
        conn = sqlite3.connect("../database/weather.db")
        print("SQLite is working fine! Connection successful!")
        conn.close()
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")

def create_weather_table(db_file):
    # Ensure the directory for the database exists
    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS weather (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city_name TEXT,
                date_time TEXT,
                temp_C REAL,
                temp_F REAL,
                humidity INTEGER,
                weather_description TEXT
            )
        ''')
        conn.commit()
        print("Table created successfully!")
        conn.close()
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")

# Run the connection check and create the table
check_sqlite_connection()
create_weather_table("../database/weather.db")
