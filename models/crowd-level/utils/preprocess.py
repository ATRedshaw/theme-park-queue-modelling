import sqlite3
import pandas as pd

def load_all_data(db_path='data/queue_Data.db'):
    conn = sqlite3.connect(db_path)
    
    # Load queue data into a pandas DataFrame
    queue_data = pd.read_sql_query("SELECT * FROM queue_data", conn)
    
    # Load park info into a pandas DataFrame
    park_info = pd.read_sql_query("SELECT * FROM park_info", conn)
    
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