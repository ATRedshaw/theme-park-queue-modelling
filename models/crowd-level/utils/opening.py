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

        # Prep the return json and the list of dates to check via the API
        all_dates_opening_hours = {}
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

        # For any remaining dates, retrieve values via the themeparks API
        for date in unfound_dates:
            year, month, day = date.split('-')
            themepark_id = get_themeparks_id_from_queuetimes_id(get_name_from_queuetimes_id(park_id))

            if themepark_id is None:
                print(f"Error: Themepark ID not found for park ID {park_id}")
                return None
            
            # Get the opening hours from the API
            day_opening_hours = get_themeparks_schedule(themepark_id, year, month, day)
            if day_opening_hours is None:
                print(f"Error: Opening hours not found for park ID {park_id} on {date}")
                all_dates_opening_hours[date] = {
                    'opening_time': None,
                    'closing_time': None
                }
            else:
                all_dates_opening_hours[date] = {
                    'opening_time': day_opening_hours['opening_time'],
                    'closing_time': day_opening_hours['closing_time']
                }

        return all_dates_opening_hours
    except Exception as e:
        print(f"Error loading data: {e}")
        return None
    
def get_themeparks_schedule(themeparks_id, year, month, day=None):
    api_url = f'https://api.themeparks.wiki/v1/entity/{themeparks_id}/schedule/{year}/{month}'
    response = requests.get(api_url)
    
    if response.status_code == 200:
        schedule_data = response.json()
        if day:
            date = f"{year}-{month}-{day}"
            # Find that date in the schedule
            for entry in schedule_data.get('schedule', []):
                if entry['date'] == date:
                    opening_time = datetime.fromisoformat(entry['openingTime']).strftime('%H:%M')
                    closing_time = datetime.fromisoformat(entry['closingTime']).strftime('%H:%M')
                    return {
                        'opening_time': opening_time,
                        'closing_time': closing_time
                    }
        else:
            # Return all available days in the format day: {opening, closing}
            all_days = {}
            for entry in schedule_data.get('schedule', []):
                day = entry['date'].split('-')[-1]
                opening_time = datetime.fromisoformat(entry['openingTime']).strftime('%H:%M')
                closing_time = datetime.fromisoformat(entry['closingTime']).strftime('%H:%M')
                all_days[f'{year}/{month}/{day}'] = {
                    'opening_time': opening_time,
                    'closing_time': closing_time
                }
            return all_days
    return None

if __name__ == "__main__":
    # Example usage
    park_id = 2
    date = ['2024-10-30', '2024-10-31', '2025-04-16', '2025-04-25']
    opening_hours = get_opening_hours(park_id, date)
    print(opening_hours)