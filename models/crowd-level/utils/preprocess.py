from helpers import load_all_data, get_name_from_queuetimes_id, get_themeparks_id_from_queuetimes_id
import yaml

def get_train_include_park_ids(config_path='config.yml'):
    def load_yaml(path='config.yml'):
        with open(path, 'r') as file:
            return yaml.safe_load(file)
        
    config = load_yaml(config_path)
    return_vals = config.get('models', {}).get('crowd-level', {}).get('train', {}).get('include_park_ids', [])
    # Convert to strings for database querying
    return_vals = [str(val) for val in return_vals]
    return return_vals

def preprocess():
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

if __name__ == '__main__':
    queue_data = preprocess()
    print(queue_data)
    print(queue_data.describe())
