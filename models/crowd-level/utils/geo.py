from datetime import datetime
from meteostat import Point, Daily
import pandas as pd
import requests

def get_lat_long(park_id, api_url='https://queue-times.com/parks.json'):
    """Use the park_id to get the longitude and latitude of the park from the park_locations.jsomn file.
    
    Args:
        park_id (int): The park_id of the park.
        
    Returns:
        tuple: A tuple containing the longitude and latitude of the park.
    """

    try:
        park_id = int(park_id)
    except:
        pass

    try:
        response = requests.get(api_url)
        response.raise_for_status()
        parks_data = response.json()

        for company in parks_data:
            for park in company.get("parks", []):
                if park["id"] == park_id:
                    latitude = float(park["latitude"])
                    longitude = float(park["longitude"])
                    return longitude, latitude

        print(f"Error: Park ID {park_id} not found.")
        return ()
    except requests.RequestException as e:
        print(f"Error fetching park data: {e}")
        return ()
    except (ValueError, KeyError) as e:
        print(f"Error processing park data: {e}")
        return ()

def get_weather_data(start_date, end_date, latitude, longitude, is_model_training=True):
    """
    Fetch weather data for a given date range and location. Get data dependent on whether it's for model training or inference.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        is_model_training (bool): Flag to indicate if the data is for model training or inference.
        
    Returns:
        dict: A dictionary containing the weather data.
    """
    def model_training(start_date, end_date, latitude, longitude):
        """
        Fetch historical weather data for model training using meteostat.
        
        Args:
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.
            latitude: Latitude of the location.
            longitude: Longitude of the location.
        
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
                    "precipitation_mm": float(row['prcp']) if pd.notna(row['prcp']) else None,
                    "wind_speed_kmh": float(row['wspd']) if pd.notna(row['wspd']) else None
                }
            
            return weather_data
        
        except ValueError as ve:
            print(f"ValueError in model_training: {ve}")
            return {}
        except ConnectionError as ce:
            print(f"ConnectionError in model_training: Failed to connect to Meteostat: {ce}")
            return {}
        except Exception as e:
            print(f"Unexpected error in model_training: {str(e)}")
            return {}
    
    def model_inference(start_date, end_date, latitude, longitude):
        """
        Fetch predicted weather data for model inference using Open-Meteo.
        
        Args:
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.
            latitude: Latitude of the location.
            longitude: Longitude of the location.
        
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
                "daily": "temperature_2m_mean,precipitation_sum,windspeed_10m_max",
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
            for date, temp, precip, wind in zip(
                data["daily"]["time"],
                data["daily"]["temperature_2m_mean"],
                data["daily"]["precipitation_sum"],
                data["daily"]["windspeed_10m_max"]
            ):
                weather_data[date] = {
                    "temperature_c": float(temp) if temp is not None else None,
                    "precipitation_mm": float(precip) if precip is not None else None,
                    "wind_speed_kmh": float(wind) if wind is not None else None
                }

            return weather_data
        
        except ValueError as ve:
            print(f"ValueError in model_inference: {ve}")
            return {}
        except requests.HTTPError as he:
            error_message = he.response.text
            try:
                error_json = he.response.json()
                reason = error_json.get("reason", "No specific reason provided")
            except ValueError:
                reason = error_message or "No specific reason provided"
            print(f"HTTPError in model_inference: Status {he.response.status_code}: {reason}")
            return {}
        except requests.ConnectionError as ce:
            print(f"ConnectionError in model_inference: Failed to connect to Open-Meteo: {ce}")
            return {}
        except requests.Timeout as te:
            print(f"Timeout in model_inference: Request to Open-Meteo timed out: {te}")
            return {}
        except requests.RequestException as re:
            print(f"RequestException in model_inference: Failed to fetch data from Open-Meteo: {re}")
            return {}
        except Exception as e:
            print(f"Unexpected error in model_inference: {str(e)}")
            return {}

    if is_model_training:
        return model_training(start_date, end_date, longitude, latitude)
    else:
        return model_inference(start_date, end_date, longitude, latitude)