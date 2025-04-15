import asyncio
import random
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

async def type_with_delay(page, selector, text, logger):
    """
    Types text into an input field with random delays to simulate human typing.
    
    Args:
        page: Playwright page object
        selector: CSS selector of the input field
        text: Text to type
        logger: Logger instance for logging actions
    """
    logger.debug(f"Typing into selector {selector}")
    for char in text:
        await page.type(selector, char)
        delay = random.uniform(0.1, 0.3)
        logger.debug(f"Typing character '{char}' with delay {delay:.2f}s")
        await asyncio.sleep(delay)

async def accept_cookies(page, logger):
    """
    Clicks the 'Accept All' button on the cookie consent popup if it appears.
    
    Args:
        page: Playwright page object
        logger: Logger instance for logging actions
    """
    logger.info("Checking for cookie consent popup")
    cookie_button_selector = 'a.cmpboxbtn.cmpboxbtnyes.cmptxt_btn_yes'
    try:
        await page.wait_for_selector(cookie_button_selector, timeout=5000)
        logger.debug("Cookie consent button found, clicking 'Accept All'")
        await page.click(cookie_button_selector)
        await asyncio.sleep(random.uniform(0.5, 1))
        logger.info("Cookie consent accepted successfully")
    except PlaywrightTimeoutError:
        logger.info("No cookie consent popup found, proceeding")
    except Exception as e:
        logger.error(f"Failed to accept cookies: {e}")
        raise

async def login(page, username, password, logger):
    """
    Logs into the site with human-like typing and delays after accepting cookies.
    
    Args:
        page: Playwright page object
        username: Username for login
        password: Password for login
        logger: Logger instance for logging actions
    """
    logger.info("Initiating login process")
    try:
        logger.debug("Navigating to login page")
        await page.goto('https://queue-times.com/users/sign_in')
        delay = random.uniform(1, 2)
        logger.debug(f"Waiting {delay:.2f}s after page load")
        await asyncio.sleep(delay)
        
        await accept_cookies(page, logger)
        
        logger.debug("Typing username")
        await type_with_delay(page, '#user_email', username, logger)
        delay = random.uniform(0.5, 1)
        logger.debug(f"Waiting {delay:.2f}s before typing password")
        await asyncio.sleep(delay)
        
        logger.debug("Typing password")
        await type_with_delay(page, '#user_password', password, logger)
        delay = random.uniform(1, 2)
        logger.debug(f"Waiting {delay:.2f}s before clicking submit")
        await asyncio.sleep(delay)
        
        logger.debug("Clicking login submit button")
        await page.click('input[type="submit"][value="Log in"]')
        
        logger.debug("Waiting for DOM content to load after login")
        await page.wait_for_load_state('domcontentloaded', timeout=60000)
        
        current_url = page.url
        logger.debug(f"Current URL after login: {current_url}")
        if 'queue-times.com' not in current_url:
            logger.error(f"Unexpected URL after login: {current_url}")
            raise ValueError(f"Login redirected to unexpected URL: {current_url}")
        
        try:
            await page.wait_for_selector('body', timeout=5000)
            logger.debug("Post-login page body loaded")
        except PlaywrightTimeoutError:
            logger.warning("Could not confirm post-login page content")
        
        delay = random.uniform(2, 3)
        logger.debug(f"Waiting {delay:.2f}s after login")
        await asyncio.sleep(delay)
        logger.info("Login completed successfully")
    except PlaywrightTimeoutError:
        logger.error(f"Timeout waiting for page to load after login. Current URL: {page.url}, Title: {await page.title()}")
        raise
    except Exception as e:
        logger.error(f"Login failed: {e}. Current URL: {page.url}, Title: {await page.title()}")
        raise

async def extract_data(page, date, park_id, logger):
    """
    Extracts queue times reported by the park, closure status, and ride names from charts on the page.
    
    Args:
        page: Playwright page object
        date (str): Date in 'YYYY/MM/DD' format
        park_id (str): Park ID from URL
        logger: Logger instance for logging actions
    
    Returns:
        list: List of dictionaries containing ride data with park_id and ride_name
    """
    logger.info(f"Extracting data for date {date} and park {park_id}")
    js_code = """
    () => {
        const panels = document.querySelectorAll('.panel');
        const data = [];
        console.log(`Found ${panels.length} panels`);
        panels.forEach(panel => {
            const rideLink = panel.querySelector('h2 a');
            if (!rideLink) {
                console.log('No ride link found in panel');
                return;
            }
            const href = rideLink.getAttribute('href');
            const rideName = rideLink.textContent.trim();
            console.log(`Ride link found: name=${rideName}, href=${href}`);
            if (!href) {
                console.log('Ride link has no href attribute');
                return;
            }
            const rideId = href.split('/').pop();
            if (!rideId) {
                console.log(`Failed to extract ride ID from href: ${href}`);
                return;
            }
            console.log(`Extracted rideId: ${rideId}`);
            const canvas = panel.querySelector('canvas');
            if (!canvas) {
                console.log(`No canvas found in panel for ride ID: ${rideId}`);
                return;
            }
            console.log(`Canvas found for ride ID: ${rideId}`);
            const chart = Chart.getChart(canvas);
            if (!chart) {
                console.log(`No chart found for canvas in panel for ride ID: ${rideId}`);
                return;
            }
            console.log(`Chart found for ride ID: ${rideId}, labels count: ${chart.data.labels.length}`);
            const labels = chart.data.labels;
            const parkDataset = chart.data.datasets.find(ds => ds.label === 'Reported by park');
            if (!parkDataset) {
                console.log(`No "Reported by park" dataset found for ride ID: ${rideId}`);
                return;
            }
            console.log(`Park dataset found for ride ID: ${rideId}, data points: ${parkDataset.data.length}`);
            const rideData = labels.map((label, index) => {
                const date = new Date(label);
                const year = date.getFullYear();
                const month = String(date.getMonth() + 1).padStart(2, '0');
                const day = String(date.getDate()).padStart(2, '0');
                const hours = String(date.getHours()).padStart(2, '0');
                const minutes = String(date.getMinutes()).padStart(2, '0');
                const seconds = String(date.getSeconds()).padStart(2, '0');
                const formattedTime = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
                return {
                    time_of_day: formattedTime,
                    queue_time: parkDataset.data[index] || 0,
                    is_closed: (parkDataset.data[index] === 0) ? 1 : 0
                };
            });
            console.log(`Processed ${rideData.length} data points for ride ID: ${rideId}`);
            data.push({ ride_id: rideId, ride_name: rideName, data_points: rideData });
        });
        console.log(`Returning data with ${data.length} rides`);
        return data;
    }
    """
    try:
        await page.wait_for_selector('.panel', timeout=5000)
        extracted_data = await page.evaluate(js_code)
        # Add park_id to each ride's data
        for ride in extracted_data:
            ride['park_id'] = park_id
            # Log sample timestamps and ride name for debugging
            logger.debug(f"Extracted ride: ride_id={ride['ride_id']}, ride_name={ride.get('ride_name', 'Unknown')}, sample_data_points={ride['data_points'][:2]}")
        logger.info(f"Successfully extracted data for {len(extracted_data)} rides")
        return extracted_data
    except Exception as e:
        logger.error(f"Failed to extract data for {date}: {e}")
        return []