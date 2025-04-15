import asyncio
import yaml
import os
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from config import load_credentials
from logger import setup_logging
from database import setup_database, store_data, store_park_info, get_existing_dates
from scraper import login, extract_data
from utils import filter_data_to_intervals, generate_date_range
import random

async def main():
    """
    Main function to orchestrate login, data extraction, filtering, and storage with delays.
    """
    logger = setup_logging()
    logger.info("Starting main process")
    
    # Load configuration from YAML file
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yml')
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        if not config or 'scraper' not in config:
            raise ValueError("Invalid config.yml: 'scraper' section missing")
        
        start_date = config['scraper'].get('start_date')
        end_date = config['scraper'].get('end_date')
        park_ids = config['scraper'].get('park_ids', [])
        
        if not start_date or not end_date or not park_ids:
            raise ValueError("config.yml missing required fields: start_date, end_date, or park_ids")
        
        logger.info(f"Loaded config: start_date={start_date}, end_date={end_date}, park_ids={park_ids}")
    except FileNotFoundError:
        logger.critical(f"Config file not found at {config_path}")
        return
    except yaml.YAMLError as e:
        logger.critical(f"Failed to parse config.yml: {e}")
        return
    except ValueError as e:
        logger.critical(f"Configuration error: {e}")
        return
    
    try:
        username, password = load_credentials(logger)
    except ValueError as e:
        logger.critical(f"Credential loading failed: {e}")
        return
    
    try:
        conn = setup_database(logger)
    except Exception as e:
        logger.critical(f"Database setup failed, exiting")
        logger.critical(e)
        return
    
    try:
        all_dates = generate_date_range(start_date, end_date, logger)
    except ValueError as e:
        logger.critical(f"Failed to generate date range: {e}")
        conn.close()
        return
    
    async with async_playwright() as p:
        logger.debug("Launching browser")
        try:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()
            page.on("console", lambda msg: logger.debug(f"Browser console: {msg.text}"))
            logger.info("Browser launched successfully")
        except Exception as e:
            logger.error(f"Failed to launch browser: {e}")
            conn.close()
            return
        
        try:
            await login(page, username, password, logger)
        except Exception as e:
            logger.critical(f"Login process failed, exiting: {e}")
            await browser.close()
            conn.close()
            return
        
        for park_id in park_ids:
            # Get existing dates for this park
            existing_dates = get_existing_dates(conn, park_id, logger)
            # Filter valid_dates to exclude existing dates
            valid_dates = [date for date in all_dates if date not in existing_dates]
            logger.info(f"Processing {len(valid_dates)} new dates for park {park_id}")
            
            for date in valid_dates:
                url = f'https://queue-times.com/parks/{park_id}/calendar/{date}'
                logger.info(f"Processing URL: {url}")
                try:
                    logger.debug(f"Navigating to {url}")
                    await page.goto(url)
                    delay = random.uniform(2, 4)
                    logger.debug(f"Waiting {delay:.2f}s after page load")
                    await asyncio.sleep(delay)
                    
                    logger.debug("Waiting for panels to load")
                    await page.wait_for_selector('.panel', timeout=5000)
                    panels = await page.query_selector_all('.panel')
                    if not panels:
                        logger.warning(f"No panels found for park {park_id} on {date}")
                        continue
                    
                    logger.debug("Starting data extraction")
                    data = await extract_data(page, date, park_id, logger)
                    if data:
                        logger.debug("Filtering data to 15-minute intervals")
                        try:
                            filtered_data = filter_data_to_intervals(data, date, logger)
                            if filtered_data:
                                logger.debug("Starting data storage")
                                for ride in filtered_data:
                                    store_park_info(conn, ride['ride_id'], ride['park_id'], ride.get('ride_name', 'Unknown'), logger)
                                store_data(conn, date, filtered_data, logger)
                                logger.info(f"Completed processing for park {park_id} on {date}")
                            else:
                                logger.warning(f"No valid data after filtering for park {park_id} on {date}")
                        except Exception as e:
                            logger.error(f"Failed to filter data for park {park_id} on {date}: {e}")
                            continue
                    else:
                        logger.warning(f"No valid data extracted for park {park_id} on {date}")
                except PlaywrightTimeoutError:
                    logger.error(f"Timeout waiting for panels for park {park_id} on {date}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing park {park_id} on {date}: {e}")
                    continue
        
        logger.debug("Closing browser")
        await browser.close()
        logger.info("Browser closed")
    
    logger.debug("Closing database connection")
    conn.close()
    logger.info("Database connection closed")
    logger.info("Processing complete")

if __name__ == "__main__":
    asyncio.run(main())