import requests

def fetch_weather_data_from_api(city_name):
    try:
        # API key and URL for OpenWeatherMap
        api_key = "574fe8c776faef5eb05d4f4a67808a75"  
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&units=metric&appid={api_key}"
        
        # Send request to the API
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        # Parse JSON response
        data = response.json()
        weather_data = {
            'city_name': city_name,
            'date_time': data['dt'],  # Unix timestamp, can convert to readable format later
            'temp_C': data['main']['temp'],
            'temp_F': data['main']['temp'] * 9/5 + 32,  # Convert Celsius to Fahrenheit
            'humidity': data['main']['humidity'],
            'weather_description': data['weather'][0]['description']
        }
        return weather_data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
        return None
    except KeyError as e:
        print(f"Missing data in API response: {e}")
        return None

# Example usage
city = "Bengaluru"
weather_data = fetch_weather_data_from_api(city)
if weather_data:
    print("Fetched Weather Data:")
    print(weather_data)
