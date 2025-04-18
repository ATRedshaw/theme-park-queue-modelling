import requests
import pandas as pd
from helpers import load_all_data, get_name_from_queuetimes_id, get_themeparks_id_from_queuetimes_id
from datetime import datetime

def get_opening_hours(park_id, dates):
    """
    Get the opening hours for a given park on a given date.
    
    Args:
        park_id (int): The ID of the park.
        dates (list): List of dates in YYYY-MM-DD format.
        
    Returns:
        dict: A dictionary containing the opening hours.
    """
    park_id = str(park_id)

    try:
        statements = {
            'queue_select': 'date, time_of_day',
            'park_where': f'park_id = {park_id}',
        }
        queue_data = load_all_data(statements=statements)['queue_data']
        
        # Sort queue data by date then time_of_day
        queue_data.sort_values(by=['date', 'time_of_day'], inplace=True)

        # List of the unique dates in the queue data
        unique_dates = queue_data['date'].unique()

        # Initialize the return dictionary
        all_dates_opening_hours = {}
        
        # Track dates not found in queue data
        unfound_dates = []
        for date in dates:
            if date in unique_dates:
                date_data = queue_data[queue_data['date'] == date]
                time_of_day = date_data['time_of_day'].values
                all_dates_opening_hours[date] = {
                    'opening_time': time_of_day[0],
                    'closing_time': time_of_day[-1]
                }
            else:
                unfound_dates.append(date)
                all_dates_opening_hours[date] = {
                    'opening_time': None,
                    'closing_time': None
                }

        # Group unfound dates by year and month
        dates_by_month = {}
        for date in unfound_dates:
            year, month, day = date.split('-')
            year_month = f"{year}-{month}"
            if year_month not in dates_by_month:
                dates_by_month[year_month] = []
            dates_by_month[year_month].append(date)

        # Fetch schedules for each year-month pair
        themepark_id = get_themeparks_id_from_queuetimes_id(get_name_from_queuetimes_id(park_id))
        if themepark_id is None:
            print(f"Error: Themepark ID not found for park ID {park_id}")
            return None

        for year_month, date_list in dates_by_month.items():
            year, month = year_month.split('-')
            month_schedule = get_themeparks_schedule(themepark_id, year, month)
            if month_schedule:
                for date in date_list:
                    if date in month_schedule:
                        all_dates_opening_hours[date] = {
                            'opening_time': month_schedule[date]['opening_time'],
                            'closing_time': month_schedule[date]['closing_time']
                        }
                    else:
                        print(f"Warning: Opening hours not found for park ID {park_id} on {date}")

        return all_dates_opening_hours
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def get_themeparks_schedule(themeparks_id, year, month, day=None):
    """
    
    Fetch the theme park's schedule for a specific year and month, optionally for a specific day.

    Args:
        themeparks_id (int): The ID of the theme park.
        year (int): The year for which to get the schedule.
        month (int): The month for which to get the schedule.
        day (int, optional): The day for which to get the schedule. Defaults to None.
    
    Returns:
        dict: A dictionary containing the opening and closing times for the specified date(s).
    """
    def normalize_iso_timestamp(timestamp):
        """
        Normalize ISO 8601 timestamp.

        Args:
            timestamp (str): The ISO 8601 timestamp.
        
        Returns:
            str: The normalized timestamp.
        """
        if timestamp.endswith('Z'):
            timestamp = timestamp[:-1] + '+00:00'
        return timestamp
    
    api_url = f'https://api.themeparks.wiki/v1/entity/{themeparks_id}/schedule/{year}/{month}'
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        schedule_data = response.json()

        if day:
            date = f"{year}-{month}-{day}"
            for entry in schedule_data.get('schedule', []):
                if entry['date'] == date:
                    opening_time = datetime.fromisoformat(normalize_iso_timestamp(entry['openingTime'])).strftime('%H:%M')
                    closing_time = datetime.fromisoformat(normalize_iso_timestamp(entry['closingTime'])).strftime('%H:%M')
                    return {
                        'opening_time': opening_time,
                        'closing_time': closing_time
                    }
            # Return None values if the specific day is not found
            return {
                'opening_time': None,
                'closing_time': None
            }
        else:
            all_days = {}
            for entry in schedule_data.get('schedule', []):
                date = entry['date']
                opening_time = datetime.fromisoformat(normalize_iso_timestamp(entry['openingTime'])).strftime('%H:%M')
                closing_time = datetime.fromisoformat(normalize_iso_timestamp(entry['closingTime'])).strftime('%H:%M')
                all_days[date] = {
                    'opening_time': opening_time,
                    'closing_time': closing_time
                }
            return all_days
    except Exception as e:
        print(f"Error fetching schedule: {e}")
        # Return a dictionary with None values in case of any errors
        if day:
            return {
                'opening_time': None,
                'closing_time': None
            }
        else:
            return {}

if __name__ == "__main__":
    park_id = 2
    date = ['2024-10-30', '2024-10-31', '2025-04-16', '2025-04-25', '2025-04-26', '2025-10-20', '2025-10-30', '2025-10-31']
    opening_hours = get_opening_hours(park_id, date)
    print(opening_hours)