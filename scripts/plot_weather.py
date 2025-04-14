import matplotlib.pyplot as plt
import sqlite3

def plot_weather_data(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT date_time, temp_C FROM weather WHERE city_name='Bengaluru'")
    rows = cursor.fetchall()
    conn.close()
    
    dates, temps = zip(*rows)
    plt.plot(dates, temps, marker='o')
    plt.title("Temperature Trends")
    plt.xlabel("Date-Time")
    plt.ylabel("Temperature (°C)")
    plt.show()
