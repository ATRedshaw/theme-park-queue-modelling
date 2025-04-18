from helpers import load_all_data, get_all_park_id_countries
from holidays import get_bank_holidays
from opening import get_opening_hours
from geo import get_lat_long, get_weather_data
import yaml
import pandas as pd

def get_train_include_park_ids(config_path='config.yml'):
    def load_yaml(path='config.yml'):
        with open(path, 'r') as file:
            return yaml.safe_load(file)
        
    config = load_yaml(config_path)
    return_vals = config.get('models', {}).get('crowd-level', {}).get('train', {}).get('include_park_ids', [])
    # Convert to strings for database querying
    return_vals = [str(val) for val in return_vals]
    return return_vals

def generate_training():
    statements = {
        'queue_select': '*',
        'queue_where': 'is_closed = 0',
        'park_select': '*'
    }
    sql_tables = load_all_data(statements=statements)

    queue_data = sql_tables['queue_data']
    park_info = sql_tables['park_info']

    # Join the park_id to the queue_data using ride_id
    queue_data['park_id'] = queue_data['ride_id'].map(park_info.set_index('ride_id')['park_id'])

    # Filter the queue data for the specified park_ids
    include_park_ids = get_train_include_park_ids()
    queue_data = queue_data[queue_data['park_id'].isin(include_park_ids)]

    # Drop the 'id' column
    try:
        queue_data = queue_data.drop(columns=['id'])
    except:
        pass

    # Group by (average the queue_time) for each date and ride_id
    queue_data = queue_data.groupby(['date', 'ride_id']).agg({'queue_time': 'mean'}).reset_index()
    queue_data = queue_data.rename(columns={'queue_time': 'avg_queue_time'})

    # Aggregate the avg of the avg_queue_time for each date
    queue_data = queue_data.groupby(['date']).agg({'avg_queue_time': 'mean'}).reset_index()

    # Assign each data a crowd level percentage (0-100) based on the avg_queue_time, rounded to the nearest integer
    queue_data['crowd_level'] = ((queue_data['avg_queue_time'] / queue_data['avg_queue_time'].max()) * 100).round().astype(int)

    # Drop the avg_queue_time column
    queue_data = queue_data.drop(columns=['avg_queue_time'])

    return queue_data 

def extract_features_from_date(df):
    """
    Extract features from the date column of the DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame containing a 'date' column.
        
    Returns:
        pd.DataFrame: DataFrame with extracted features.
    """
    df['date'] = pd.to_datetime(df['date'])

    # Add day of week as one-hot encoded columns with True/False (1=Monday, 7=Sunday)
    for i in range(1, 8):
        df[f'day_of_week_{i}'] = df['date'].dt.dayofweek == (i - 1)

    # Add month as one-hot encoded columns with True/False
    for i in range(1, 13):
        df[f'month_{i}'] = df['date'].dt.month == i

    return df

def add_bank_holidays(df):
    """
    Add bank holidays to the DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame containing a 'date' column.
        
    Returns:
        pd.DataFrame: DataFrame with bank holidays added.
    """
    # Get bank holidays
    df['is_bank_holiday'] = False

    # Get the unique years from the date column
    years = df['date'].dt.year.unique()
    for year in years:
        bank_holidays = get_bank_holidays(year, country_name='United Kingdom')
        # Convert bank holidays to datetime
        bank_holidays = pd.to_datetime(bank_holidays)
        # Check if the date is a bank holiday
        df.loc[df['date'].isin(bank_holidays), 'is_bank_holiday'] = True

if __name__ == '__main__':
    queue_data = generate_training()
    queue_data = queue_data.drop(columns=['crowd_level'])
    queue_data = extract_features_from_date(queue_data)
    queue_data = add_bank_holidays(queue_data)
    
    # Print where is_bank_holiday is True
    print(queue_data[queue_data['is_bank_holiday'] == True])
