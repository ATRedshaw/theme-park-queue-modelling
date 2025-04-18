import requests
import pycountry

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

if __name__ == "__main__":
    year = 2023
    print(f'UK Bank Holidays: {get_bank_holidays(year, "United Kingdom")}')
    print(f'US Bank Holidays: {get_bank_holidays(year, "United States")}')