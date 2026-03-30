from .helpers import load_all_data, get_country_from_park_id
from .holidays import get_bank_holidays, get_school_holidays
from .opening import get_opening_hours
from .geo import get_lat_long, get_weather_data
import yaml
import pandas as pd

def get_train_include_park_ids(config_path='config.yml'):
    """
    Get the list of park IDs to include in training from the config file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        list[int] | None: List of park IDs to train on, or None to include all parks.
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    return_val = config.get('models', {}).get('crowd-level', {}).get('train', {}).get('include_park_ids', None)

    if return_val is None:
        print('No include_park_ids set in config — using all parks.')
        return None

    if isinstance(return_val, int):
        return_val = [return_val]
    else:
        return_val = [int(x) for x in return_val]

    print(f'Retrieved park_ids from config: {return_val}')
    return return_val

def generate_training(include_park_ids=None):
    """
    Generate training data for the crowd level model.

    Args:
        include_park_ids (list[int] | None): Park IDs to include. Uses all parks if None.

    Returns:
        pd.DataFrame: DataFrame with columns: date, park_id (str), crowd_level.
    """
    statements = {
        'queue_select': '*',
        'queue_where': 'is_closed = 0',
        'park_select': '*'
    }
    sql_tables = load_all_data(statements=statements)

    queue_data = sql_tables['queue_data']
    park_info = sql_tables['park_info']

    queue_data['park_id'] = queue_data['ride_id'].map(park_info.set_index('ride_id')['park_id'])

    # SQLite has no enforced types, so park_id may come back as str or float.
    # Normalise to int before filtering to guarantee isin matches.
    queue_data['park_id'] = pd.to_numeric(queue_data['park_id'], errors='coerce')
    queue_data = queue_data.dropna(subset=['park_id'])
    queue_data['park_id'] = queue_data['park_id'].astype(int)

    if include_park_ids is not None:
        if isinstance(include_park_ids, (int, str)):
            include_park_ids = [int(include_park_ids)]
        queue_data = queue_data[queue_data['park_id'].isin([int(x) for x in include_park_ids])]

    try:
        queue_data = queue_data.drop(columns=['id'])
    except KeyError:
        pass

    # Average per ride per day, then average across all rides per park per day.
    queue_data = queue_data.groupby(['date', 'park_id', 'ride_id']).agg({'queue_time': 'mean'}).reset_index()
    queue_data = queue_data.rename(columns={'queue_time': 'avg_queue_time'})
    queue_data = queue_data.groupby(['date', 'park_id']).agg({'avg_queue_time': 'mean'}).reset_index()

    # Per-park percentile rank: a score of 70 means busier than 70% of historical days
    # for that park. Stable across retrains and not sensitive to single outlier days.
    queue_data['crowd_level'] = (
        queue_data.groupby('park_id')['avg_queue_time']
        .rank(pct=True) * 100
    ).round().astype(int)

    queue_data = queue_data.drop(columns=['avg_queue_time'])

    # Store park_id as string so median imputation doesn't corrupt it.
    queue_data['park_id'] = queue_data['park_id'].astype(str)

    queue_data = queue_data.sort_values(by='date').reset_index(drop=True)

    park_ids_used = queue_data['park_id'].unique().tolist()
    print(f'Generated training data with {len(queue_data)} rows across parks: {park_ids_used}')

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

def add_bank_holidays(df):
    """
    Add a bank holiday flag to the DataFrame.

    Handles multiple parks via the park_id column, batching API calls by country
    to avoid redundant requests for parks in the same country.

    Args:
        df (pd.DataFrame): DataFrame with 'date' and 'park_id' columns.

    Returns:
        pd.DataFrame: DataFrame with 'is_bank_holiday' column added.
    """
    years = df['date'].dt.year.unique()
    unique_parks = df['park_id'].unique()

    # Fetch holiday sets once per country, not once per park.
    country_holidays: dict = {}
    park_country: dict = {}
    for park in unique_parks:
        country = get_country_from_park_id(park)
        park_country[park] = country
        if country not in country_holidays:
            holidays: set = set()
            for year in years:
                holidays.update(get_bank_holidays(year, country))
            country_holidays[country] = holidays

    # Build a per-park lookup then use a list comprehension rather than df.apply.
    # apply on an empty DataFrame with boolean columns causes pandas to infer a
    # multi-column return type, raising ValueError on assignment.
    park_holiday_lookup = {
        park: country_holidays.get(country, set())
        for park, country in park_country.items()
    }
    date_strs = df['date'].dt.strftime('%Y-%m-%d')
    df['is_bank_holiday'] = [
        d in park_holiday_lookup.get(p, set())
        for d, p in zip(date_strs, df['park_id'])
    ]

    print('Successfully added bank holidays to the DataFrame.')
    return df

def add_school_holidays(df):
    """
    Add a school holiday flag to the DataFrame.

    Uses Google AI Studio (Gemini) to estimate school holiday dates, batching
    by country to avoid redundant API calls for parks in the same country.

    Args:
        df (pd.DataFrame): DataFrame with 'date' and 'park_id' columns.

    Returns:
        pd.DataFrame: DataFrame with 'is_school_holiday' column added.
    """
    min_year = int(df['date'].dt.year.min())
    max_year = int(df['date'].dt.year.max())
    unique_parks = df['park_id'].unique()

    country_school_holidays: dict = {}
    park_country: dict = {}
    for park in unique_parks:
        country = get_country_from_park_id(park)
        park_country[park] = country
        if country not in country_school_holidays:
            country_school_holidays[country] = get_school_holidays(min_year, max_year, country)

    df['is_school_holiday'] = df.apply(
        lambda row: row['date'].strftime('%Y-%m-%d') in country_school_holidays.get(park_country.get(row['park_id'], ''), set()),
        axis=1
    )

    print('Successfully added school holidays to the DataFrame.')
    return df

def add_opening_hours(df):
    """
    Adds park opening hours to the DataFrame.

    Handles multiple parks via the park_id column. Uses dict-based mapping
    rather than iterrows for performance.

    Args:
        df (pd.DataFrame): DataFrame with 'date' and 'park_id' columns.

    Returns:
        pd.DataFrame: DataFrame with opening_hr, closing_hr, and hours_open_for columns.
    """
    unique_parks = df['park_id'].unique()
    result_frames = []

    for park in unique_parks:
        park_df = df[df['park_id'] == park].copy()
        dates = park_df['date'].dt.strftime('%Y-%m-%d').tolist()
        opening_hours = get_opening_hours(park, dates)

        opening_map: dict = {}
        for date_str, hours in opening_hours.items():
            try:
                opening_hr = int(hours['opening_time'].split(':')[0])
                closing_hr = int(hours['closing_time'].split(':')[0])
                hours_open = closing_hr - opening_hr
            except (AttributeError, TypeError, ValueError):
                opening_hr = closing_hr = hours_open = None
            opening_map[date_str] = (opening_hr, closing_hr, hours_open)

        date_strs = park_df['date'].dt.strftime('%Y-%m-%d')
        park_df['opening_hr'] = date_strs.map(lambda d: opening_map.get(d, (None, None, None))[0])
        park_df['closing_hr'] = date_strs.map(lambda d: opening_map.get(d, (None, None, None))[1])
        park_df['hours_open_for'] = date_strs.map(lambda d: opening_map.get(d, (None, None, None))[2])
        result_frames.append(park_df)

    df = pd.concat(result_frames).sort_values(by='date').reset_index(drop=True)

    print('Successfully added opening hours to the DataFrame.')
    df = df.dropna(subset=['opening_hr', 'closing_hr', 'hours_open_for'])
    return df

def add_weather_data(df, is_training=True):
    """
    Adds weather data to the DataFrame.

    Handles multiple parks via the park_id column, fetching a separate weather
    series per park location. Uses dict-based mapping rather than iterrows.

    Args:
        df (pd.DataFrame): DataFrame with 'date' and 'park_id' columns.
        is_training (bool): Fetch historical data (True) or forecast data (False).

    Returns:
        pd.DataFrame: DataFrame with temperature_c, precipitation_mm, and wind_speed_kmh columns.
    """
    unique_parks = df['park_id'].unique()
    result_frames = []

    for park in unique_parks:
        park_df = df[df['park_id'] == park].copy()
        lat_long = get_lat_long(park)
        if not lat_long:
            raise ValueError(f"Could not retrieve lat/long for park_id {park}")

        latitude, longitude = lat_long
        start_date = park_df['date'].min().strftime('%Y-%m-%d')
        end_date = park_df['date'].max().strftime('%Y-%m-%d')

        weather_data = get_weather_data(start_date, end_date, latitude, longitude, is_model_training=is_training)

        date_strs = park_df['date'].dt.strftime('%Y-%m-%d')
        park_df['temperature_c'] = date_strs.map(lambda d: weather_data.get(d, {}).get('temperature_c'))
        park_df['precipitation_mm'] = date_strs.map(lambda d: weather_data.get(d, {}).get('precipitation_mm'))
        park_df['wind_speed_kmh'] = date_strs.map(lambda d: weather_data.get(d, {}).get('wind_speed_kmh'))
        result_frames.append(park_df)

    df = pd.concat(result_frames).sort_values(by='date').reset_index(drop=True)

    print('Successfully added weather data to the DataFrame.')
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
    park_ids = get_train_include_park_ids()
    queue_data = generate_training(park_ids)
    queue_data = queue_data.drop(columns=['crowd_level'])
    queue_data = extract_features_from_date(queue_data)
    queue_data = add_bank_holidays(queue_data)
    queue_data = add_opening_hours(queue_data)
    queue_data = add_weather_data(queue_data)
    queue_data = fill_missing_values_with_median(queue_data)

    print(queue_data.head())