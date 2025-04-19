from .preprocess import (
    get_train_include_park_id,
    generate_training,
    extract_features_from_date,
    add_bank_holidays,
    add_opening_hours,
    add_weather_data,
    fill_missing_values_with_median
)

def model_pipeline(is_training=True, day_df=None):
    """
    Complete the model preprocessing pipeline for training or inference.
    
    Args:
        is_training (bool): Flag to indicate if the pipeline is for training or inference.
        day_df (pd.DataFrame): DataFrame containing the data for inference.
    
    Returns:
        pd.DataFrame: Preprocessed DataFrame for training or inference.
    """
    def training_pipeline():
        """
        Prepare the training data by loading, preprocessing, and merging with additional features.
        
        Returns:
            pd.DataFrame: Preprocessed training DataFrame.
        """
        print('Running model training pipeline...')
        park_id = get_train_include_park_id()
        queue_data = generate_training(park_id)
        target_cols = queue_data.copy()
        queue_data = queue_data.drop(columns=['crowd_level'])
        queue_data = extract_features_from_date(queue_data)
        queue_data = add_bank_holidays(queue_data, park_id)
        queue_data = add_opening_hours(queue_data, park_id)
        queue_data = add_weather_data(queue_data, park_id)
        queue_data = fill_missing_values_with_median(queue_data)

        # Merge the target col (crowd_level) back into the main DataFrame on date
        queue_data = queue_data.merge(target_cols[['date', 'crowd_level']], on='date', how='left')

        # Drop date column as it is high cardinality
        queue_data = queue_data.drop(columns=['date'])

        print("Training data prepared successfully.")
        print('--' * 50)
        return queue_data

    def inference_pipeline(day_df):
        """
        Prepare the inference data by preprocessing and merging with additional features.
        
        Args:
            day_df (pd.DataFrame): DataFrame containing the data for inference.
        
        Returns:
            pd.DataFrame: Preprocessed inference DataFrame.
        """
        if day_df is None:
            raise ValueError("day_df cannot be None for inference - days must be provided.")
    
        print('Running model inference pipeline...')
        park_id = get_train_include_park_id()
        queue_data = day_df.copy()
        queue_data = extract_features_from_date(queue_data)
        queue_data = add_bank_holidays(queue_data, park_id)
        queue_data = add_opening_hours(queue_data, park_id)
        queue_data = add_weather_data(queue_data, park_id, is_training=False)
        # I think this needs to be changed for date handling - 
        # Maybe if a date doesnt exist use some average values for the month from the previous year or something.
        queue_data = fill_missing_values_with_median(queue_data)

        # Drop date column as it is high cardinality
        queue_data = queue_data.drop(columns=['date'])

        print("Inference data pipeline completed successfully.")
        print('--' * 50)
        return queue_data

    if is_training:
        return_df = training_pipeline()
        return return_df
    else:
        return_df = inference_pipeline(day_df)
        return return_df

if __name__ == "__main__":
    training_data = model_pipeline()
    queue_data = generate_training()
    print(f'{queue_data.head()}\n')
    print(f'{training_data.head()}\n')