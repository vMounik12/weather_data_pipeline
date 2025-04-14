import requests
import pandas as pd
import sqlite3

# 1. Extract weather data
def fetch_weather_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()  # Assuming JSON format
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

# 2. Transform data
def clean_weather_data(raw_data):
    try:
        # Extract required fields
        weather_list = [{
            "temp_F": raw_data["main"]["temp"], 
            "humidity": raw_data["main"]["humidity"],
            "temp_C": (raw_data["main"]["temp"] - 32) * 5.0 / 9.0,  # Convert Fahrenheit to Celsius
            "city_name": raw_data["name"],
            "date_time": pd.Timestamp.now(),  # Add the current timestamp
            "weather_description": raw_data["weather"][0]["description"]
        }]
        # Create DataFrame
        df = pd.DataFrame(weather_list)
        df.drop_duplicates(inplace=True)
        return df
    except KeyError as e:
        print(f"Error transforming data: Missing key {e}")
        return pd.DataFrame()

# 3. Load data into SQLite database
def load_to_database(dataframe, db_file):
    try:
        conn = sqlite3.connect(db_file)
        dataframe.to_sql('weather', conn, if_exists='append', index=False)  # Append instead of replace
        print("Data successfully loaded into the database!")
    except sqlite3.Error as e:
        print(f"Error loading data into database: {e}")
    finally:
        conn.close()

# Run the pipeline
def main():
    # Using your working API key and URL
    city_name = "Bengaluru"  # You can replace this with any city name
    api_key = "574fe8c776faef5eb05d4f4a67808a75"  # Your provided API key
    api_url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&units=metric&appid={api_key}"

    db_file = "../database/weather.db"

    print("Fetching weather data...")
    raw_data = fetch_weather_data(api_url)

    if raw_data:  # Proceed only if data was successfully fetched
        print("Transforming data...")
        cleaned_data = clean_weather_data(raw_data)

        if not cleaned_data.empty:  # Proceed only if transformation was successful
            print("Loading data into database...")
            load_to_database(cleaned_data, db_file)

    print("Pipeline complete!")

if __name__ == "__main__":
    main()
