import sqlite3
import pandas as pd

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

if __name__ == "__main__":
    data = load_all_data()
    queue_data = data['queue_data']
    park_info = data['park_info']
    
    # Print the first few rows of each dataset
    print("Queue Data:")
    print(queue_data.head()) 
    
    print("\nPark Info:")
    print(park_info.head())  