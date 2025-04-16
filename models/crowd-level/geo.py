
import json
from datetime import datetime
from meteostat import Point, Daily
import pandas as pd
import requests

def get_long_lat(park_id):
    """Use the park_id to get the longitude and latitude of the park from the park_locations.jsomn file.
    
    Args:
        park_id (str): The park_id of the park.
        
    Returns:
        tuple: A tuple containing the longitude and latitude of the park.
    """
    park_locations = json.load(open('models/park_locations.json'))
    if park_id in park_locations:
        longitude = park_locations[park_id]["longitude"]
        latitude = park_locations[park_id]["latitude"]
        return (longitude, latitude)
    else:
        raise ValueError("Invalid park_id provided.")

def get_weather_data(start_date, end_date, longitude, latitude, is_model_training=True):
    """
    Fetch weather data for a given date range and location. Get data dependent on whether it's for model training or inference.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.
        longitude (float): Longitude of the location.
        latitude (float): Latitude of the location.
        is_model_training (bool): Flag to indicate if the data is for model training or inference.
        
    Returns:
        dict: A dictionary containing the weather data.
    """
    def model_training(start_date, end_date, longitude, latitude):
        """
        Fetch historical weather data for model training using meteostat.
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.
            longitude (float): Longitude of the location.
            latitude (float): Latitude of the location.
        
        Returns:
            dict: A dictionary containing the weather data.
        """
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")

            if start_date > end_date:
                raise ValueError("Start date must be before end date.")
            if end_date > datetime.now():
                raise ValueError("End date must be before the current date.")
            
            location = Point(latitude, longitude)
            data = Daily(location, start_date, end_date)
            data = data.fetch()

            # Process data into dictionary
            weather_data = {}
            for date_idx, row in data.iterrows():
                date_str = date_idx.strftime("%Y-%m-%d")
                weather_data[date_str] = {
                    "temperature_c": float(row['tavg']) if pd.notna(row['tavg']) else None,
                    "precipitation_mm": float(row['prcp']) if pd.notna(row['prcp']) else None
                }
            
            return weather_data
        
        except ValueError as ve:
            print(f"ValueError: {ve}")
            return {}
    
    def model_inference(start_date, end_date, longitude, latitude):
        """
        Fetch predicted weather data for model inference using Open-Meteo.
        
        Args:
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.
            longitude: Longitude of the location.
            latitude: Latitude of the location.
        
        Returns:
            dict: A dictionary containing the predicted weather data.
        """
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")

            if start_date > end_date:
                raise ValueError("Start date must be before end date.")
            if start_date < datetime.now():
                raise ValueError("Start date must be in the future for inference.")
            
            # Open-Meteo API endpoint
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "daily": "temperature_2m_mean,precipitation_sum",
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "timezone": "auto"
            }

            # Make API request
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if "daily" not in data:
                print("No forecast data returned from API.")
                return {}

            # Process data into dictionary
            weather_data = {}
            for date, temp, precip in zip(
                data["daily"]["time"],
                data["daily"]["temperature_2m_mean"],
                data["daily"]["precipitation_sum"]
            ):
                weather_data[date] = {
                    "temperature_c": float(temp) if temp is not None else None,
                    "precipitation_mm": float(precip) if precip is not None else None
                }
            
            return weather_data
        
        except Exception as e:
            print(f"Error fetching weather data: {e}")
            return {}


    if is_model_training:
        return model_training(start_date, end_date, longitude, latitude)
    else:
        return model_inference(start_date, end_date, longitude, latitude)