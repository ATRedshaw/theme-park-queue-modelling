from utils.pipeline import model_pipeline
import pandas as pd
import yaml
import os
import joblib

def load_model(config_path='config.yml'):
    # Load the configuration file
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Get the model name from the config
    model_name = config.get('models', {}).get('crowd-level', {}).get('inference', {}).get('model_name', 'crowd-level-model')
    
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the path to the model file
    model_path = os.path.join(current_dir, 'model-exports', f"{model_name}.pkl")
    
    # Load and return the model
    with open(model_path, 'rb') as file:
        model = joblib.load(file)
    
    return model

def load_feature_columns(config_path='config.yml'):
    """
    Load the feature column names saved during training.

    Used to align one-hot encoded inference data to the exact columns
    the model was trained on.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        list[str]: Ordered list of feature column names.
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    model_name = config.get('models', {}).get('crowd-level', {}).get('inference', {}).get('model_name', 'crowd-level-model')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    columns_path = os.path.join(current_dir, 'model-exports', f'{model_name}_columns.pkl')
    with open(columns_path, 'rb') as file:
        return joblib.load(file)

if __name__ == "__main__":
    with open('config.yml', 'r') as f:
        _config = yaml.safe_load(f)
    park_id = _config.get('models', {}).get('crowd-level', {}).get('inference', {}).get('park_id', 2)

    horizon_days = _config.get('models', {}).get('crowd-level', {}).get('inference', {}).get('horizon_days', 14)

    todays_date = pd.to_datetime('today').strftime('%Y-%m-%d')
    future_date = (pd.to_datetime('today') + pd.Timedelta(days=horizon_days)).strftime('%Y-%m-%d')
    dates_list = pd.date_range(start=todays_date, end=future_date).strftime('%Y-%m-%d').tolist()

    dates_df = pd.DataFrame({
        'date': pd.to_datetime(dates_list),
        'park_id': str(park_id)
    })

    feature_columns = load_feature_columns()
    model = load_model()

    dates_df = model_pipeline(is_training=False, day_df=dates_df)

    # Build display metadata after the pipeline so it reflects any rows dropped
    # (e.g. dates with no opening hours data).
    meta_df = pd.DataFrame({
        'date': dates_df['date'],
        'park_id': str(park_id)
    })

    # Align to training columns — fills any missing park columns with 0.
    inference_data = dates_df.reindex(columns=feature_columns, fill_value=0)

    predictions = model.predict(inference_data)

    results_df = pd.DataFrame({
        'date': meta_df['date'],
        'park_id': meta_df['park_id'],
        'crowd_level_prediction': predictions.round().astype(int)
    })

    print('Predicted Crowd Levels:')
    print(results_df)