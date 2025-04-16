import requests
import pandas as pd
from helpers import load_all_data
from datetime import datetime

def get_opening_hours(park_id, date):
    """
    Get the opening hours for a given park on a given date.
    
    Args:
        park_id (int): The ID of the park.
        date (str): The date in YYYY-MM-DD format.
        
    Returns:
        dict: A dictionary containing the opening hours.
    """
    def past_date_opening_hours(park_id, date):
        try:
            park_id = str(park_id)
            statements = {
                'queue_where': f"date = '{date}' and is_closed = 0",
                'queue_select': 'ride_id, time_of_day',
                'park_where': f"park_id = {park_id}",
                'park_select': '*'
            }
            sql_tables = load_all_data(statements=statements)
            # Load the queue_data and park_info tables
            queue_data = sql_tables['queue_data']
            park_info = sql_tables['park_info']

            # Add column for park_id using the ride_id and park_id in park_info, and the ride_id in queue_data
            queue_data['park_id'] = queue_data['ride_id'].map(park_info.set_index('ride_id')['park_id'])
            
            # Filter the queue_data for the specified park_id
            queue_data = queue_data[queue_data['park_id'] == park_id]
            # Drop the ride_id and park_id columns
            queue_data = queue_data.drop(columns=['ride_id', 'park_id'])

            # Order the data by time_of_day
            queue_data = queue_data.sort_values(by='time_of_day')

            # Return the first and last values
            opening_hours = {
                'opening_time': queue_data.iloc[0]['time_of_day'],
                'closing_time': queue_data.iloc[-1]['time_of_day']
            }
            return opening_hours
        except Exception as e:
            print(f"Error retrieving past opening hours: {e}")
            return None

    def future_date_opening_hours(park_id, date):
        pass

    # Try to convert the date to a datetime object
    try:
        date_obj = datetime.strptime(date, '%Y/%m/%d')
    except Exception as e:
        print(f"Error converting date: {e}")
        return None

    try:
        statements = {
            'queue_select': 'date',
            'park_where': f"park_id = {park_id}",
            'park_select': '*'
        }
        sql_tables = load_all_data(statements=statements)
        # Load the queue_data and park_info tables
        queue_data = sql_tables['queue_data']

        # Convert the column to datetime
        queue_data['date'] = pd.to_datetime(queue_data['date'], format='%Y/%m/%d')
        
        # Get the last date in the queue_data table
        last_date = queue_data['date'].max()
    except Exception as e:
        print(f"Error retrieving last date: {e}")
        return None
    
    if date_obj <= last_date:
        # If the date is in the past, get past opening hours
        return past_date_opening_hours(park_id, date)
    else:
        # If the date is in the future, get future opening hours
        return future_date_opening_hours(park_id, date)

if __name__ == "__main__":
    # Example usage
    park_id = 2
    date = '2025/04/16'
    opening_hours = get_opening_hours(park_id, date)
    print(opening_hours)