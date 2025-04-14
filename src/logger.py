import logging
from datetime import datetime

def setup_logging():
    """
    Sets up logging to print to the terminal with a timestamped format.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger('QueueScraper')
    logger.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    logger.info("Logging initialized")
    return logger