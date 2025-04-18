import sqlite3
import pandas as pd
import requests

def load_all_data(db_path='data/queue_Data.db', statements={}):
    conn = sqlite3.connect(db_path)
    
    queue_where = statements.get('queue_where', None)
    queue_select = statements.get('queue_select', '*')
    park_where = statements.get('park_where', None)
    park_select = statements.get('park_select', '*')

    # Load queue data into a pandas DataFrame
    if queue_where:
        queue_data = pd.read_sql_query(f"SELECT {queue_select} FROM queue_data WHERE {queue_where}", conn)
    else:
        queue_data = pd.read_sql_query(f"SELECT {queue_select} FROM queue_data", conn)
    
    # Load park info into a pandas DataFrame
    if park_where:
        park_info = pd.read_sql_query(f"SELECT {park_select} FROM park_info WHERE {park_where}", conn)
    else:
        park_info = pd.read_sql_query(f"SELECT {park_select} FROM park_info", conn)
    
    conn.close()

    queue_data['date'] = pd.to_datetime(queue_data['date'])

    return {
        'queue_data': queue_data,
        'park_info': park_info
    }

def get_name_from_queuetimes_id(park_id, api_url='https://queue-times.com/parks.json'):
    # Try to convert park_id to integer
    try:
        park_id = int(park_id)
    except ValueError:
        print("Error: Park ID must be an integer.")
        return None
    
    # Fetch park data from the API
    response = requests.get(api_url)
    if response.status_code == 200:
        parks_data = response.json()
        for company in parks_data:
            for park in company.get('parks', []):
                if park['id'] == park_id:
                    return park['name']
    return None

def get_themeparks_id_from_queuetimes_id(name, api_url='https://api.themeparks.wiki/v1/destinations'):
    response = requests.get(api_url)
    if response.status_code == 200:
        destinations_data = response.json()
        for destination in destinations_data.get('destinations', []):
            for park in destination.get('parks', []):
                if park['name'].lower() == name.lower():
                    return park['id']
    return None

def get_country_from_park_id(park_id, url='https://queue-times.com/parks.json'):
    # Try to convert park_id to integer
    try:
        park_id = int(park_id)
    except ValueError:
        print("Error: Park ID must be an integer.")
        return None

    response = requests.get(url)
    if response.status_code == 200:
        parks_data = response.json()
        for company in parks_data:
            for park in company.get('parks', []):
                if park['id'] == park_id:
                    return park['country']
        
        # Print warning if ID not found
        print(f"Warning: Park ID {park_id} not found")
    return None

if __name__ == "__main__":
    data = load_all_data()
    queue_data = data['queue_data']
    park_info = data['park_info']
    
    # Print the first few rows of each dataset
    print("Queue Data:")
    print(queue_data.head()) 
    
    print("\nPark Info:")
    print(park_info.head())  

    print("\nPark Name for ID 1:")
    park_name = get_name_from_queuetimes_id(1)
    print(park_name if park_name else "Park not found")

    park_id = 2
    country = get_country_from_park_id(park_id)
    print(f'Country for Park ID {park_id}: {country}' if country else f'Park ID {park_id} not found')