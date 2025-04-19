from utils.pipeline import model_pipeline
import pandas as pd
import yaml
import os
import pickle

def load_model(config_path='config.yml'):
    # Load the configuration file
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Get the model name from the config
    model_name = config.get('models', {}).get('crowd-level', {}).get('inference', {}).get('model_name', 'random_forest_model')
    
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the path to the model file
    model_path = os.path.join(current_dir, 'model-exports', f"{model_name}.pkl")
    
    # Load and return the model
    with open(model_path, 'rb') as file:
        model = pickle.load(file)
    
    return model

if __name__ == "__main__":
    dates_list = ['2025-05-01']
    dates_df = pd.DataFrame({'date': pd.to_datetime(dates_list)})
    inference_data = model_pipeline(is_training=False, day_df=dates_df)
    print(inference_data)
    model = load_model()