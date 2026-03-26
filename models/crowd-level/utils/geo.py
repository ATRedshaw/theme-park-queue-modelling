from datetime import datetime, timedelta
import calendar
import re
import warnings
from meteostat import Point, Daily
import pandas as pd
import requests
# ----- Fix SSL Error -----
import ssl
import certifi
import os

# Set environment variables
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
os.environ['CURL_CA_BUNDLE'] = certifi.where()
# ------------------------

# Create and set default SSL context
ssl_context = ssl.create_default_context(cafile=certifi.where())
ssl._create_default_https_context = lambda: ssl_context


def get_historical_monthly_averages(latitude, longitude, months_needed, years_back=5):
    """
    Compute historical monthly climate averages for temperature, precipitation, and wind speed.

    Fetches `years_back` years of Meteostat daily data for each required calendar month
    and returns the mean value per month. Used as a fallback when the date range extends
    beyond the Open-Meteo forecast horizon.

    Args:
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        months_needed (set[int]): Calendar month numbers to compute averages for.
        years_back (int): Number of prior years to average over. Defaults to 5.

    Returns:
        dict[int, dict]: Month number mapped to average temperature_c, precipitation_mm,
            and wind_speed_kmh values.
    """
    try:
        today = datetime.now()
        location = Point(latitude, longitude)

        all_frames = []
        for year_offset in range(1, years_back + 1):
            year = today.year - year_offset
            for month in months_needed:
                _, last_day = calendar.monthrange(year, month)
                period_start = datetime(year, month, 1)
                period_end = datetime(year, month, last_day)
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=FutureWarning)
                    data = Daily(location, period_start, period_end).fetch()
                if not data.empty:
                    data = data[['tavg', 'prcp', 'wspd']].copy()
                    data['month'] = month
                    all_frames.append(data)

        if not all_frames:
            print('No historical data retrieved for monthly average calculation.')
            return {}

        combined = pd.concat(all_frames)
        averages = combined.groupby('month')[['tavg', 'prcp', 'wspd']].mean()

        result = {}
        for month, row in averages.iterrows():
            result[int(month)] = {
                'temperature_c': float(row['tavg']) if pd.notna(row['tavg']) else None,
                'precipitation_mm': float(row['prcp']) if pd.notna(row['prcp']) else None,
                'wind_speed_kmh': float(row['wspd']) if pd.notna(row['wspd']) else None,
            }
        return result

    except Exception as e:
        print(f'Error computing historical monthly averages: {e}')
        return {}


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
            
            print(f"Fetching weather data for {latitude}, {longitude} between {start_date} and {end_date}")
            location = Point(latitude, longitude)
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=FutureWarning)
                data = Daily(location, start_date, end_date)
                data = data.fetch()
            print(f"Data fetched for {latitude}, {longitude} between {start_date} and {end_date}")

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
        Fetch weather data for model inference.

        For dates within Open-Meteo's forecast window, uses the forecast API.
        If the requested end_date exceeds the API's allowed range, the allowed
        max is parsed from the error response and the request is retried
        automatically — no manual horizon constant needed.

        For dates beyond the forecast window, falls back to historical monthly
        averages computed from Meteostat data over the past 5 years.

        Args:
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.
            latitude: Latitude of the location.
            longitude: Longitude of the location.

        Returns:
            dict: Date string → weather value dict for every date in the range.
        """
        FORECAST_HORIZON_DAYS = 16

        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as ve:
            print(f"ValueError in model_inference: {ve}")
            return {}

        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        forecast_cutoff = today + timedelta(days=FORECAST_HORIZON_DAYS)

        weather_data = {}
        actual_forecast_end = None

        def _fetch_open_meteo(req_end):
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "daily": "temperature_2m_mean,precipitation_sum,windspeed_10m_mean",
                "start_date": start_dt.strftime("%Y-%m-%d"),
                "end_date": req_end.strftime("%Y-%m-%d"),
                "timezone": "auto"
            }
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            return resp.json()

        if start_dt <= forecast_cutoff:
            forecast_end = min(end_dt, forecast_cutoff)
            data = {}
            try:
                data = _fetch_open_meteo(forecast_end)
            except requests.HTTPError as he:
                # API's allowed end_date is tighter than our horizon — parse
                # the actual max from the error body and retry once.
                if he.response.status_code == 400:
                    match = re.search(r'to (\d{4}-\d{2}-\d{2})', he.response.text)
                    if match:
                        allowed_end = datetime.strptime(match.group(1), "%Y-%m-%d")
                        forecast_end = min(forecast_end, allowed_end)
                        try:
                            data = _fetch_open_meteo(forecast_end)
                        except Exception as retry_err:
                            print(f"Open-Meteo retry failed: {retry_err}")
                    else:
                        try:
                            reason = he.response.json().get("reason", he.response.text)
                        except ValueError:
                            reason = he.response.text
                        print(f"HTTPError in model_inference: Status {he.response.status_code}: {reason}")
                else:
                    print(f"HTTPError in model_inference: Status {he.response.status_code}")
            except requests.ConnectionError as ce:
                print(f"ConnectionError in model_inference: Failed to connect to Open-Meteo: {ce}")
            except requests.Timeout:
                print("Timeout in model_inference: Request to Open-Meteo timed out.")
            except requests.RequestException as re_err:
                print(f"RequestException in model_inference: {re_err}")
            except Exception as e:
                print(f"Unexpected error fetching Open-Meteo forecast: {e}")

            if "daily" in data:
                dates_returned = data["daily"]["time"]
                for date, temp, precip, wind in zip(
                    dates_returned,
                    data["daily"]["temperature_2m_mean"],
                    data["daily"]["precipitation_sum"],
                    data["daily"]["windspeed_10m_mean"]
                ):
                    weather_data[date] = {
                        "temperature_c": float(temp) if temp is not None else None,
                        "precipitation_mm": float(precip) if precip is not None else None,
                        "wind_speed_kmh": float(wind) if wind is not None else None
                    }
                if dates_returned:
                    actual_forecast_end = datetime.strptime(dates_returned[-1], "%Y-%m-%d")

        # Historical monthly averages for anything beyond the forecast data.
        fallback_start = (actual_forecast_end + timedelta(days=1)) if actual_forecast_end else start_dt
        beyond_start = max(fallback_start, start_dt)

        if beyond_start <= end_dt:
            months_needed = set()
            cursor = beyond_start
            while cursor <= end_dt:
                months_needed.add(cursor.month)
                if cursor.month == 12:
                    cursor = cursor.replace(year=cursor.year + 1, month=1, day=1)
                else:
                    cursor = cursor.replace(month=cursor.month + 1, day=1)

            print(f"Dates beyond forecast horizon — using historical monthly averages for months: {sorted(months_needed)}")
            monthly_avgs = get_historical_monthly_averages(latitude, longitude, months_needed)

            cursor = beyond_start
            while cursor <= end_dt:
                date_str = cursor.strftime("%Y-%m-%d")
                avg = monthly_avgs.get(cursor.month, {})
                weather_data[date_str] = {
                    "temperature_c": avg.get("temperature_c"),
                    "precipitation_mm": avg.get("precipitation_mm"),
                    "wind_speed_kmh": avg.get("wind_speed_kmh"),
                }
                cursor += timedelta(days=1)

        return weather_data

    if is_model_training:
        return model_training(start_date, end_date, longitude, latitude)
    else:
        return model_inference(start_date, end_date, longitude, latitude)
