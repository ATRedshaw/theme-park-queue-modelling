
import json

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
    pass