from holidays import get_bank_holidays
from preprocess import generate_training
from opening import get_opening_hours
from geo import get_lat_long, get_weather_data

def model_pipeline(is_training=True, day_df=None):
    def training_pipeline():
        day_df = generate_training()
        target_df = day_df.copy()
        day_df.drop(columns=['crowd_level'], inplace=True)


    def inference_pipeline(day_df):
        if day_df is None:
            raise ValueError("day_df cannot be None for inference - days must be provided.")

    if is_training:
        return_df = training_pipeline()
        return return_df
    else:
        return_df = inference_pipeline(day_df)
        return return_df

if __name__ == "__main__":
    model_pipeline()