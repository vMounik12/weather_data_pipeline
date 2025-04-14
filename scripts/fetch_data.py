import requests

def fetch_weather_data_from_api(city_name):
    api_key = "574fe8c776faef5eb05d4f4a67808a75" 
    api_url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&units=metric&appid={api_key}"
    response = requests.get(api_url)
    data = response.json()
    return {
        "city_name": city_name,
        "date_time": data["dt"],
        "temp_C": data["main"]["temp"],
        "temp_F": data["main"]["temp"] * 9/5 + 32,
        "humidity": data["main"]["humidity"],
        "weather_description": data["weather"][0]["description"]
    }
