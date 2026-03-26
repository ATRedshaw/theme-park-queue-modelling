import pandas as pd
from .preprocess import (
    get_train_include_park_ids,
    generate_training,
    extract_features_from_date,
    add_bank_holidays,
    add_school_holidays,
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
        park_ids = get_train_include_park_ids()
        queue_data = generate_training(park_ids)
        target_cols = queue_data[['date', 'park_id', 'crowd_level']].copy()

        queue_data = queue_data.drop(columns=['crowd_level'])
        queue_data = extract_features_from_date(queue_data)
        queue_data = add_bank_holidays(queue_data)
        queue_data = add_school_holidays(queue_data)
        queue_data = add_opening_hours(queue_data)
        queue_data = add_weather_data(queue_data)
        queue_data = fill_missing_values_with_median(queue_data)

        # Merge crowd_level while date and park_id are still raw columns.
        # One-hot encoding happens after so park_id is available for the join.
        queue_data = queue_data.merge(
            target_cols[['date', 'park_id', 'crowd_level']],
            on=['date', 'park_id'],
            how='left'
        )

        # One-hot encode park_id so the model learns park-specific baselines
        # without treating park_id as a continuous ordinal variable.
        queue_data = pd.get_dummies(queue_data, columns=['park_id'], prefix='park', dtype=int)

        queue_data = queue_data.drop(columns=['date'])

        print('Training data prepared successfully.')
        print('--' * 50)
        return queue_data

    def inference_pipeline(day_df):
        """
        Prepare the inference data by preprocessing and merging with additional features.

        Args:
            day_df (pd.DataFrame): DataFrame with 'date' and 'park_id' columns.

        Returns:
            pd.DataFrame: Preprocessed inference DataFrame (date column retained for display).
        """
        if day_df is None:
            raise ValueError('day_df cannot be None for inference — dates must be provided.')
        if 'park_id' not in day_df.columns:
            raise ValueError("day_df must contain a 'park_id' column for inference.")

        print('Running model inference pipeline...')
        queue_data = day_df.copy()

        # Ensure park_id is a string to match training dtype.
        queue_data['park_id'] = queue_data['park_id'].astype(str)

        queue_data = extract_features_from_date(queue_data)
        queue_data = add_bank_holidays(queue_data)
        queue_data = add_school_holidays(queue_data)
        queue_data = add_opening_hours(queue_data)
        queue_data = add_weather_data(queue_data, is_training=False)
        queue_data = fill_missing_values_with_median(queue_data)

        # One-hot encode park_id. Column alignment against training columns
        # is handled in inference.py using the saved feature column list.
        queue_data = pd.get_dummies(queue_data, columns=['park_id'], prefix='park', dtype=int)

        print('Inference data pipeline completed successfully.')
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