import json
import os
import requests
import pycountry
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google import genai
from google.genai import types

def get_bank_holidays(year, country_name):
    """
    Get the bank holidays for a given country in a given year.

    Args:
        year (int): The year for which to get the bank holidays.
        country_name (str): The name of the country.
    
    Returns:
        list: A list of bank holidays in the format YYYY-MM-DD.
    """
    def get_country_code(country_name):
        """
        Get the country code for a given country name.

        Args:
            country_name (str): The name of the country.

        Returns:
            str: The country code.
        """
        country_name = country_name.lower()
        try:
            country = pycountry.countries.search_fuzzy(country_name)[0]
            return country.alpha_2
        except Exception as e:
            print(f"Error retrieving country code for {country_name}: {e}")
            return None
        
    country_code = get_country_code(country_name)
    try:
        api_url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}"
        response = requests.get(api_url)
        if response.status_code == 200:
            results = response.json()
            # Create a list of dates and return
            holidays = []
            for holiday in results:
                if "date" in holiday:
                    holidays.append(holiday["date"])
            return holidays
        else:
            return []
    except Exception as e:
        print(f"Error fetching bank holidays: {e}")
        return []

def get_school_holidays(start_year, end_year, location):
    """
    Estimate school holiday dates for a given location and year range via Gemini.

    Makes a single API call per (location, year-range) pair and expands the
    returned holiday periods into a set of individual date strings, matching
    the interface of get_bank_holidays for use in add_school_holidays.

    Args:
        start_year (int): First year to cover.
        end_year (int): Last year to cover (inclusive).
        location (str): Country or region name (e.g. "England", "United Kingdom").

    Returns:
        set[str]: All individual dates (YYYY-MM-DD) that fall within a school holiday.
    """
    load_dotenv()
    api_key = os.environ.get('GOOGLE_AI_API_KEY')
    model_name = os.environ.get('GOOGLE_AI_MODEL', 'gemini-2.0-flash')

    if not api_key:
        print('Warning: GOOGLE_AI_API_KEY not set — skipping school holidays.')
        return set()

    prompt = (
        f'Return a JSON array of {location} school holiday date ranges '
        f'from {start_year} to {end_year}. '
        'Each object must have keys: "holiday_name", "start_date", "end_date". '
        'Dates in YYYY-MM-DD format, ordered chronologically by start_date. '
        'Include all major school holidays: e.g. summer, Christmas, Easter, and all half terms. '
        'Return only the JSON array.'
    )

    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
            config=types.GenerateContentConfig(response_mime_type='application/json')
        )
        periods = json.loads(response.text)
    except Exception as e:
        print(f'Error fetching school holidays from Gemini for {location}: {e}')
        return set()

    # Expand each holiday period into individual date strings.
    holiday_dates: set = set()
    for period in periods:
        try:
            start = datetime.strptime(period['start_date'], '%Y-%m-%d').date()
            end = datetime.strptime(period['end_date'], '%Y-%m-%d').date()
            day = start
            while day <= end:
                holiday_dates.add(day.strftime('%Y-%m-%d'))
                day += timedelta(days=1)
        except (KeyError, ValueError):
            continue

    print(f'Retrieved {len(holiday_dates)} school holiday dates for {location} ({start_year}–{end_year}).')
    return holiday_dates
    year = 2023
    print(f'UK Bank Holidays: {get_bank_holidays(year, "United Kingdom")}')
    print(f'US Bank Holidays: {get_bank_holidays(year, "United States")}')