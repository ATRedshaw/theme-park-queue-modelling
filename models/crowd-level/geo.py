
import json
from datetime import datetime
from meteostat import Point, Daily
import pandas as pd

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
        Fetch historical weather data for model inference using meteostat.
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.
            longitude (float): Longitude of the location.
            latitude (float): Latitude of the location.
        
        Returns:
            dict: A dictionary containing the weather data.
        """
        pass

    if is_model_training:
        return model_training(start_date, end_date, longitude, latitude)
    else:
        return model_inference(start_date, end_date, longitude, latitude)
    
if __name__ == "__main__":
    park_id = "2"
    start_date = "2021-03-01"
    end_date = "2025-04-15"
    longitude, latitude = get_long_lat(park_id)
    weather_data = get_weather_data(start_date, end_date, longitude, latitude)
    print(weather_data)