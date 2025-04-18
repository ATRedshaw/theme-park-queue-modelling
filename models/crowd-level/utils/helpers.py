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

    return {
        'queue_data': queue_data,
        'park_info': park_info
    }

def get_name_from_queuetimes_id(park_id, api_url='https://queue-times.com/parks.json'):
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

def get_unique_countries_from_park_ids(park_ids, url='https://queue-times.com/parks.json'):
    # Try to convert park_ids to integers
    try:
        park_ids = [int(park_id) for park_id in park_ids]
    except ValueError:
        print("Error: Park IDs must be integers.")
        return []

    response = requests.get(url)
    if response.status_code == 200:
        parks_data = response.json()
        countries = set()
        for company in parks_data:
            for park in company.get('parks', []):
                if park['id'] in park_ids:
                    countries.add(park['country'])
                    park_ids.remove(park['id'])  # Remove found ID to track unfound IDs
        
        # Print warnings for any IDs not found
        for park_id in park_ids:
            print(f"Warning: Park ID {park_id} not found")
            
        return list(countries)
    return []

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

    park_ids = [1, 2, 3, 57]
    countries = get_unique_countries_from_park_ids(park_ids)
    print("\nUnique Countries:")
    print(countries)