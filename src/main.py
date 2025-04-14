import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from config import load_credentials
from logger import setup_logging
from database import setup_database, store_data
from scraper import login, extract_data
import random

async def main():
    """
    Main function to orchestrate login, data extraction, and storage with delays.
    """
    logger = setup_logging()
    logger.info("Starting main process")
    
    try:
        username, password = load_credentials(logger)
    except ValueError as e:
        logger.critical(f"Credential loading failed: {e}")
        return
    
    try:
        conn = setup_database(logger)
    except Exception as e:
        logger.critical("Database setup failed, exiting")
        return
    
    valid_dates = ['2024/10/30', '2024/10/31', '2024/11/01']
    logger.info(f"Processing dates: {valid_dates}")
    
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
            logger.critical("Login process failed, exiting")
            await browser.close()
            conn.close()
            return
        
        for date in valid_dates:
            url = f'https://queue-times.com/parks/2/calendar/{date}'
            logger.info(f"Processing URL: {url}")
            try:
                logger.debug(f"Navigating to {url}")
                await page.goto(url)
                delay = random.uniform(4, 10)
                logger.debug(f"Waiting {delay:.2f}s after page load")
                await asyncio.sleep(delay)
                
                logger.debug("Waiting for panels to load")
                await page.wait_for_selector('.panel', timeout=10000)
                panels = await page.query_selector_all('.panel')
                if not panels:
                    logger.warning(f"No panels found for {date}")
                    continue
                
                logger.debug("Starting data extraction")
                data = await extract_data(page, date, logger)
                if data:
                    logger.debug("Starting data storage")
                    store_data(conn, date, data, logger)
                    logger.info(f"Completed processing for {date}")
                else:
                    logger.warning(f"No valid data extracted for {date}")
            except PlaywrightTimeoutError:
                logger.error(f"Timeout waiting for panels on {date}")
                continue
            except Exception as e:
                logger.error(f"Error processing {date}: {e}")
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