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
    model_name = config.get('models', {}).get('crowd-level', {}).get('inference', {}).get('model_name', 'random_forest_model')
    
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the path to the model file
    model_path = os.path.join(current_dir, 'model-exports', f"{model_name}.pkl")
    
    # Load and return the model
    with open(model_path, 'rb') as file:
        model = joblib.load(file)
    
    return model

if __name__ == "__main__":
    # Fix invalid future date returning empty
    # Generate a list of dates in the format YYYY-MM-DD for todays date to 14 days in the future
    todays_date = pd.to_datetime('today').strftime('%Y-%m-%d')
    future_date = pd.to_datetime('today') + pd.Timedelta(days=14)
    future_date = future_date.strftime('%Y-%m-%d')

    dates_list = pd.date_range(start=todays_date, end=future_date).strftime('%Y-%m-%d').tolist()
    dates_df = pd.DataFrame({'date': pd.to_datetime(dates_list)})
    dates_df = model_pipeline(is_training=False, day_df=dates_df)
    inference_data = dates_df.drop(columns=['date'])
    model = load_model()

    # Make predictions on the inference data
    predictions = model.predict(inference_data)

    # Create a DataFrame to display the results
    results_df = pd.DataFrame({
        'date': dates_df['date'],
        'crowd_level_prediction': predictions
    })

    print("Predicted Crowd Levels:")
    print(results_df)