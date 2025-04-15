import os
from dotenv import load_dotenv

def load_credentials(logger):
    """
    Loads username and password from .env file in the root directory.
    
    Args:
        logger: Logger instance for logging actions
    
    Returns:
        tuple: (username, password)
    
    Raises:
        ValueError: If .env file or required variables are missing
    """
    logger.debug("Loading credentials from .env file")
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    
    if not os.path.exists(env_path):
        logger.error(f".env file not found at {env_path}")
        raise ValueError(f".env file not found at {env_path}")
    
    load_dotenv(env_path)
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    
    if not username or not password:
        logger.error("USERNAME or PASSWORD not found in .env file")
        raise ValueError("USERNAME and PASSWORD must be set in .env file")
    
    logger.info("Credentials loaded successfully")
    return username, password