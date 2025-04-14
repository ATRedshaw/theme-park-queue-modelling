import sqlite3
import os

def setup_database(logger):
    """
    Sets up the SQLite database and creates the queue_data table if it doesn't exist.
    
    Args:
        logger: Logger instance for logging actions
    
    Returns:
        sqlite3.Connection: Connection to the database
    """
    logger.debug("Setting up database")
    try:
        # Create folder if not exists data
        os.makedirs('data', exist_ok=True)
        conn = sqlite3.connect('data/queue_data.db')
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS queue_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                ride_id TEXT,
                time_of_day TEXT,
                queue_time INTEGER,
                is_closed INTEGER  -- 0 for False, 1 for True
            )
        """)
        conn.commit()
        logger.info("Database setup completed successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to setup database: {e}")
        raise

def store_data(conn, date, data, logger):
    """
    Stores the extracted queue time data into the SQLite database.
    
    Args:
        conn: SQLite connection object
        date (str): Date in 'YYYY/MM/DD' format
        data (list): List of ride data dictionaries
        logger: Logger instance for logging actions
    """
    logger.info(f"Storing data for date {date}")
    cursor = conn.cursor()
    try:
        for ride in data:
            ride_id = ride['ride_id']
            for point in ride['data_points']:
                cursor.execute("""
                    INSERT INTO queue_data (date, ride_id, time_of_day, queue_time, is_closed)
                    VALUES (?, ?, ?, ?, ?)
                """, (date, ride_id, point['time_of_day'], point['queue_time'], point['is_closed']))
                logger.debug(f"Inserted data point for ride {ride_id} at {point['time_of_day']}")
        conn.commit()
        logger.info(f"Successfully stored {len(data)} rides' data for {date}")
    except Exception as e:
        logger.error(f"Failed to store data for {date}: {e}")
        conn.rollback()
        raise