import sqlite3
import os

def setup_database(logger):
    """
    Sets up the SQLite database and creates the queue_data and park_info tables if they don't exist.
    
    Args:
        logger: Logger instance for logging actions
    
    Returns:
        sqlite3.Connection: Connection to the database
    """
    logger.debug("Setting up database")
    try:
        # Create folder if not exists
        os.makedirs('data', exist_ok=True)
        conn = sqlite3.connect('data/queue_data.db')
        cursor = conn.cursor()
        
        # Create queue_data table
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
        
        # Create park_info table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS park_info (
                ride_id TEXT,
                park_id TEXT,
                ride_name TEXT,  -- New column for ride name
                PRIMARY KEY (ride_id, park_id)
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
    logger.info(f"Storing queue data for date {date}")
    cursor = conn.cursor()
    try:
        for ride in data:
            ride_id = ride['ride_id']
            for point in ride['data_points']:
                cursor.execute("""
                    INSERT INTO queue_data (date, ride_id, time_of_day, queue_time, is_closed)
                    VALUES (?, ?, ?, ?, ?)
                """, (date, ride_id, point['time_of_day'], point['queue_time'], point['is_closed']))
                logger.debug(f"Inserted queue data point for ride {ride_id} at {point['time_of_day']}")
        conn.commit()
        logger.info(f"Successfully stored {len(data)} rides' queue data for {date}")
    except Exception as e:
        logger.error(f"Failed to store queue data for {date}: {e}")
        conn.rollback()
        raise

def store_park_info(conn, ride_id, park_id, ride_name, logger):
    """
    Stores the ride_id, park_id, and ride_name in the park_info table, avoiding duplicates.
    
    Args:
        conn: SQLite connection object
        ride_id (str): ID of the ride
        park_id (str): ID of the park
        ride_name (str): Name of the ride
        logger: Logger instance for logging actions
    """
    logger.debug(f"Storing park info for ride {ride_id} ({ride_name}) and park {park_id}")
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO park_info (ride_id, park_id, ride_name)
            VALUES (?, ?, ?)
        """, (ride_id, park_id, ride_name))
        conn.commit()
        if cursor.rowcount > 0:
            logger.debug(f"Inserted park info for ride {ride_id} ({ride_name}) and park {park_id}")
        else:
            logger.debug(f"Park info for ride {ride_id} ({ride_name}) and park {park_id} already exists")
    except Exception as e:
        logger.error(f"Failed to store park info for ride {ride_id} ({ride_name}) and park {park_id}: {e}")
        conn.rollback()
        raise

def get_last_scraped_date(conn, park_id, logger):
    """
    Retrieves the last scraped date for a given park_id.
    
    Args:
        conn: SQLite connection object
        park_id (str): ID of the park
        logger: Logger instance for logging actions
    
    Returns:
        str: The last scraped date in 'YYYY/MM/DD' format, or None if no data exists
    """
    logger.debug(f"Retrieving the last scraped date for park {park_id}")
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT MAX(qd.date)
            FROM queue_data qd
            JOIN park_info pi ON qd.ride_id = pi.ride_id
            WHERE pi.park_id = ?
        """, (park_id,))
        last_date = cursor.fetchone()[0]
        if last_date:
            logger.debug(f"Last scraped date for park {park_id} is {last_date}")
        else:
            logger.debug(f"No existing data found for park {park_id}")

        last_date = last_date.replace('-', '/')
        
        return last_date
    except Exception as e:
        logger.error(f"Failed to retrieve the last scraped date for park {park_id}: {e}")
        return None