from utils.pipeline import model_pipeline
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from skopt import BayesSearchCV
from skopt.space import Integer
import joblib
import yaml
import os

def load_and_split_data():
    """
    Load and split training data into train and test sets.
    
    Returns:
        X_train: Training features.
        X_test: Test features.
        y_train: Training labels.
        y_test: Test labels."""
    training_data = model_pipeline(is_training=True)
    X = training_data.drop('crowd_level', axis=1)
    y = training_data['crowd_level']
    return train_test_split(X, y, test_size=0.2, shuffle=True, random_state=104)

def optimize_random_forest(X_train, y_train):
    """
    Perform Bayesian optimisation to find best Random Forest parameters.
    
    Args:
        X_train: Training features.
        y_train: Training labels.
    
    Returns:
        model: The optimized Random Forest model.
    """
    rf_search_space = {
        'n_estimators': Integer(50, 300),
        'max_depth': Integer(5, 30),
        'min_samples_split': Integer(2, 10),
        'min_samples_leaf': Integer(1, 5),
        'max_features': ['sqrt', 'log2']
    }

    print("Tuning Random Forest...")
    rf_bayes = BayesSearchCV(
        RandomForestRegressor(random_state=42),
        rf_search_space,
        n_iter=20,
        cv=5,
        n_jobs=-1,
        scoring='neg_mean_squared_error',
        random_state=104
    )
    rf_bayes.fit(X_train, y_train)
    print(f"Best Random Forest parameters: {rf_bayes.best_params_}")
    
    return rf_bayes.best_estimator_

def evaluate_model(model, X_test, y_test):
    """
    Evaluate model performance on test data.
    
    Args:
        model: The trained model.
        X_test: Test features.
        y_test: True labels for test data.
        
    Returns:
        pred: Model predictions.
    """
    pred = model.predict(X_test)
    
    # Calculate metrics
    mse = mean_squared_error(y_test, pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, pred)
    mae = mean_absolute_error(y_test, pred)
    
    # Print metrics
    print("\nRandom Forest Metrics:")
    print(f"MSE: {mse:.4f}")
    print(f"RMSE: {rmse:.4f}")
    print(f"R²: {r2:.4f}")
    print(f"MAE: {mae:.4f}")
    
    return pred

def display_feature_importance(model, X):
    """
    Display feature importance from the model.
    
    Args:
        model: The trained model.
        X: The feature set used for training.
    """
    feature_importance = pd.DataFrame(
        {'feature': X.columns, 'importance': model.feature_importances_}
    ).sort_values('importance', ascending=False)
    print("\nFeature importance:")
    print(feature_importance.head(10))

def save_model(model, config_path='config.yml'):
    """
    Save the trained model to a file in the models folder.
    
    Args:
        model: The trained model to save.
        config_path (str): Path to the configuration file.
    """
    # Get the model name from the config file
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    model_name = config.get('models', {}).get('crowd-level', {}).get('train', {}).get('model_name', 'random_forest_model')

    # Get the directory this file is in
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create models directory if it doesn't exist
    models_dir = os.path.join(current_dir, 'model-exports')
    os.makedirs(models_dir, exist_ok=True)
    
    model_path = os.path.join(models_dir, f'{model_name}.pkl')
    # Check if the model already exists
    if os.path.exists(model_path):
        print(f"Model {model_name}.pkl already exists. Overwriting...")
    else:
        print(f"Saving model as {model_name}.pkl in model-exports folder...")
    
    # Save the model
    joblib.dump(model, model_path)

if __name__ == "__main__":
    X_train, X_test, y_train, y_test = load_and_split_data()
    rf_model = optimize_random_forest(X_train, y_train)
    rf_pred = evaluate_model(rf_model, X_test, y_test)
    display_feature_importance(rf_model, X_train)
    save_model(rf_model)