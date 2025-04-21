from .helpers import load_all_data, get_country_from_park_id
from .holidays import get_bank_holidays
from .opening import get_opening_hours
from .geo import get_lat_long, get_weather_data
import yaml
import pandas as pd

def get_train_include_park_id(config_path='config.yml'):
    """
    Get the park_id to include in the training data from the config file.
    
    Args:
        config_path (str): Path to the configuration file.
    
    Returns:
        str: The park_id to include in the training data.
    """
    def load_yaml(path='config.yml'):
        with open(path, 'r') as file:
            return yaml.safe_load(file)
        
    config = load_yaml(config_path)
    return_val = config.get('models', {}).get('crowd-level', {}).get('train', {}).get('include_park_id', [])
    
    if return_val is None:
        raise ValueError("No park_id found in config.yml")
    
    # Convert to string for database querying
    print(F'Retrieved park_id from config: {return_val}')
    return_val = str(return_val) if return_val else None
    
    return return_val   

def generate_training(include_park_id):
    """
    Generate training data for the crowd level model.

    Args:
        include_park_id (str): The ID of the park to include in the training data.
    
    Returns:
        pd.DataFrame: DataFrame containing the training data.
    """
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

    print(f'Successfully aggregated and ranked queue data for park_id {include_park_id} with {len(queue_data)} rows.')

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

    print(f'Successfully extracted day of week and month features from date column.')

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

    print(f'Successfully added bank holidays to the DataFrame.')
    
    return df

def add_opening_hours(df, park_id, dates=None):
    """
    Adds park opening hours to the DataFrame for each date.
    
    Args:
        df (pd.DataFrame): DataFrame containing a 'date' column.
        park_id (str): The ID of the park.
        dates (list): List of dates in YYYY-MM-DD format.
    
    Returns:
        pd.DataFrame: DataFrame with opening hours added.
    """
    if dates is None:
        dates = df['date'].dt.strftime('%Y-%m-%d').tolist()
    
    opening_hours = get_opening_hours(park_id, dates)
    
    for index, row in df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        if date_str in opening_hours:
            opening_time = opening_hours[date_str]['opening_time']
            closing_time = opening_hours[date_str]['closing_time']
            
            # Convert opening and closing times to integers (hours), else default to None
            try:
                opening_hr = int(opening_time.split(':')[0])
                closing_hr = int(closing_time.split(':')[0])
            except:
                opening_hr = None
                closing_hr = None
            
            df.at[index, 'opening_hr'] = opening_hr
            df.at[index, 'closing_hr'] = closing_hr
            
            # Calculate the hours the park is open for
            try:
                df.at[index, 'hours_open_for'] = closing_hr - opening_hr
            except:
                df.at[index, 'hours_open_for'] = None
        else:
            df.at[index, 'opening_hr'] = None
            df.at[index, 'closing_hr'] = None
            df.at[index, 'hours_open_for'] = None

    print(f'Successfully added opening hours to the DataFrame.')

    return df

def add_weather_data(df, park_id, is_training=True):
    """
    Adds weather data to the DataFrame for each date.
    
    Args:
        df (pd.DataFrame): DataFrame containing a 'date' column.
        park_id (str): The ID of the park.
    
    Returns:
        pd.DataFrame: DataFrame with weather data added.
    """
    # Get latitude and longitude for the park
    lat_long = get_lat_long(park_id)
    if not lat_long:
        raise ValueError(f"Could not retrieve latitude and longitude for park_id {park_id}")
    
    latitude, longitude = lat_long
    start_date = df['date'].min().strftime('%Y-%m-%d')
    end_date = df['date'].max().strftime('%Y-%m-%d')
    
    weather_data = get_weather_data(start_date, end_date, latitude, longitude, is_model_training=is_training)
    
    # Add weather data to the DataFrame
    for index, row in df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        if date_str in weather_data:
            df.at[index, 'temperature_c'] = weather_data[date_str]['temperature_c']
            df.at[index, 'precipitation_mm'] = weather_data[date_str]['precipitation_mm']
            df.at[index, 'wind_speed_kmh'] = weather_data[date_str]['wind_speed_kmh']
        else:
            df.at[index, 'temperature_c'] = None
            df.at[index, 'precipitation_mm'] = None
            df.at[index, 'wind_speed_kmh'] = None

    print(f'Successfully added weather data to the DataFrame.')

    return df

def fill_missing_values_with_median(df):
    """
    Fill missing values in the DataFrame with the median for numerical columns.
    Then drop any rows that still have missing values.
    
    Args:
        df (pd.DataFrame): DataFrame with missing values.
    
    Returns:
        pd.DataFrame: DataFrame with missing values filled or dropped.
    """
    missing_data_length = len(df[df.isnull().any(axis=1)])
    # Fill numeric columns with median
    for column in df.columns:
        if df[column].isnull().any() and pd.api.types.is_numeric_dtype(df[column]):
            median_value = df[column].median()
            df[column] = df[column].fillna(median_value)
    
    post_imputation_length = len(df[df.isnull().any(axis=1)])
    # Drop rows that still have NaN values after imputation
    df_clean = df.dropna()

    print(f'Successfully handled missing values (initially {missing_data_length} rows - {post_imputation_length} null rows after median imputation - {len(df) - len(df_clean)} rows dropped.)')

    return df_clean

if __name__ == '__main__':
    park_id = get_train_include_park_id()
    queue_data = generate_training(park_id)
    queue_data = queue_data.drop(columns=['crowd_level'])
    queue_data = extract_features_from_date(queue_data)
    queue_data = add_bank_holidays(queue_data, park_id)
    queue_data = add_opening_hours(queue_data, park_id)
    queue_data = add_weather_data(queue_data, park_id)
    queue_data = fill_missing_values_with_median(queue_data)

    print(queue_data.head())