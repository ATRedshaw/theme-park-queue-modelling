from helpers import load_all_data, get_country_from_park_id
from holidays import get_bank_holidays
from opening import get_opening_hours
from geo import get_lat_long, get_weather_data
import yaml
import pandas as pd

def get_train_include_park_id(config_path='config.yml'):
    def load_yaml(path='config.yml'):
        with open(path, 'r') as file:
            return yaml.safe_load(file)
        
    config = load_yaml(config_path)
    return_val = config.get('models', {}).get('crowd-level', {}).get('train', {}).get('include_park_id', [])
    
    # Convert to string for database querying
    return_val = str(return_val) if return_val else None
    if return_val is None:
        raise ValueError("No park_id found in config.yml")
    
    return return_val   

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

    # Filter the queue data for the specified park_id
    include_park_id = get_train_include_park_id()
    queue_data = queue_data[queue_data['park_id'] == include_park_id]

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

    # Ensure data is sorted by date
    queue_data = queue_data.sort_values(by='date').reset_index(drop=True)

    return queue_data 

def extract_features_from_date(df):
    """
    Extract features from the date column of the DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame containing a 'date' column.
        
    Returns:
        pd.DataFrame: DataFrame with extracted features.
    """

    # Add day of week as one-hot encoded columns with True/False (1=Monday, 7=Sunday)
    for i in range(1, 8):
        df[f'day_of_week_{i}'] = df['date'].dt.dayofweek == (i - 1)

    # Add month as one-hot encoded columns with True/False
    for i in range(1, 13):
        df[f'month_{i}'] = df['date'].dt.month == i

    return df

def add_bank_holidays(df, park_id):
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
    country = get_country_from_park_id(park_id)
    for year in years:
        # Get the unique countries from the park_id
        bank_holidays = get_bank_holidays(year, country)

        # Check if the date is a bank holiday
        for holiday in bank_holidays:
            df.loc[df['date'] == holiday, 'is_bank_holiday'] = True
    
    return df

def add_opening_hours(df, park_id, dates=None):
    if dates is None:
        dates = df['date'].dt.strftime('%Y-%m-%d').tolist()
    
    opening_hours = get_opening_hours(park_id, dates)
    
    for index, row in df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        if date_str in opening_hours:
            opening_time = opening_hours[date_str]['opening_time']
            closing_time = opening_hours[date_str]['closing_time']
            
            # Convert opening and closing times to integers (hours)
            opening_hr = int(opening_time.split(':')[0])
            closing_hr = int(closing_time.split(':')[0])
            
            df.at[index, 'opening_hr'] = opening_hr
            df.at[index, 'closing_hr'] = closing_hr
            
            # Calculate the hours the park is open for
            df.at[index, 'hours_open_for'] = closing_hr - opening_hr
        else:
            df.at[index, 'opening_hr'] = None
            df.at[index, 'closing_hr'] = None
            df.at[index, 'hours_open_for'] = None

    return df

if __name__ == '__main__':
    park_id = get_train_include_park_id()
    queue_data = generate_training()
    queue_data = queue_data.drop(columns=['crowd_level'])
    queue_data = extract_features_from_date(queue_data)
    queue_data = add_bank_holidays(queue_data, park_id)
    queue_data = add_opening_hours(queue_data, park_id)

    print(queue_data.head())