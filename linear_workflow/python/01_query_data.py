import json
import os
from datetime import datetime

import requests
from FaaSr_py.client.py_client_stubs import faasr_get_file, faasr_log, faasr_put_file


def get_api_key(api_key_name: str, folder_name: str):
    faasr_get_file(
        local_file=api_key_name,
        remote_file=api_key_name,
        remote_folder=folder_name,
    )

    with open(api_key_name, "r") as f:
        api_key = f.read().strip()

    os.remove(api_key_name)

    return api_key


def query_data(
    api_key_name: str,
    folder_name: str,
    output_name: str,
    city: str,
    country_code: str,
):
    """
    Query weather data from OpenWeatherMap API

    Args:
        api_key (str): Your OpenWeatherMap API key (get from https://openweathermap.org/api)
        city (str): City name (default: "London")
        country_code (str): Country code (default: "GB")

    Returns:
        dict: Weather data including temperature, humidity, description, etc.
    """
    api_key = get_api_key(api_key_name, folder_name)

    # OpenWeatherMap API endpoint
    base_url = "http://api.openweathermap.org/data/2.5/weather"

    # API parameters
    params = {
        "q": f"{city},{country_code}",
        "appid": api_key,
        "units": "metric",  # Use metric units (Celsius)
    }

    try:
        # Make API request
        response = requests.get(base_url, params=params)
        response.raise_for_status()

        # Parse JSON response
        weather_data = response.json()

        # Extract and format key weather information
        formatted_data = {
            "city": weather_data["name"],
            "country": weather_data["sys"]["country"],
            "temperature": weather_data["main"]["temp"],
            "feels_like": weather_data["main"]["feels_like"],
            "humidity": weather_data["main"]["humidity"],
            "pressure": weather_data["main"]["pressure"],
            "description": weather_data["weather"][0]["description"],
            "wind_speed": weather_data["wind"]["speed"],
            "timestamp": datetime.now().isoformat(),
        }

        with open("output.json", "w") as f:
            json.dump(formatted_data, f, indent=2)

        faasr_put_file(
            local_file="output.json",
            remote_folder=folder_name,
            remote_file=output_name,
        )

    except requests.exceptions.RequestException as e:
        faasr_log(f"Error making API request: {e}")

    except KeyError as e:
        faasr_log(f"Error parsing API response: {e}")

    except Exception as e:
        faasr_log(f"Unexpected error: {e}")
