from dateutil import parser as date_parser
from datetime import datetime, timedelta
import re

def filter_data_to_intervals(data, date, logger):
    """
    Filters the data to keep only the measurements closest to each 15-minute interval.
    
    Args:
        data (list): List of ride data dictionaries
        date (str): Date in 'YYYY/MM/DD' format
        logger: Logger instance for logging actions
    
    Returns:
        list: Filtered list of ride data with data_points aligned to intervals
    """
    filtered_data = []
    for ride in data:
        ride_id = ride['ride_id']
        park_id = ride['park_id']
        ride_name = ride.get('ride_name', 'Unknown')  # Preserve ride_name
        data_points = ride['data_points']
        
        # Parse timestamps from the scraped data
        parsed_points = []
        for point in data_points:
            time_str = point['time_of_day']
            try:
                # Handle case where time_of_day might already be a datetime
                if isinstance(time_str, datetime):
                    dt = time_str
                else:
                    # Try parsing the full string
                    try:
                        dt = date_parser.parse(time_str)
                    except ValueError:
                        # Fallback: Extract core timestamp (e.g., "Wed Oct 30 2024 10:02:00")
                        match = re.search(r'\w{3} \w{3} \d{2} \d{4} \d{2}:\d{2}:\d{2}', time_str)
                        if match:
                            dt = date_parser.parse(match.group(0))
                        else:
                            raise ValueError(f"Cannot parse timestamp: {time_str}")
                parsed_points.append((dt, point['queue_time'], point['is_closed']))
            except Exception as e:
                logger.warning(f"Invalid timestamp for ride {ride_id}: {time_str} - {e}")
                continue
        
        if not parsed_points:
            logger.debug(f"No valid timestamps for ride {ride_id}")
            continue
        
        # Sort data points by timestamp
        parsed_points.sort(key=lambda x: x[0])
        
        # Determine the range of intervals based on earliest and latest times
        start_dt = parsed_points[0][0]
        end_dt = parsed_points[-1][0]
        
        # Round start time down to the nearest 15-minute interval
        start_minutes = (start_dt.minute // 15) * 15
        start_interval = start_dt.replace(minute=start_minutes, second=0, microsecond=0)
        
        # Round end time up to the nearest 15-minute interval
        end_minutes = ((end_dt.minute + 14) // 15) * 15
        if end_minutes >= 60:
            end_interval = end_dt.replace(hour=end_dt.hour + 1, minute=0, second=0, microsecond=0)
        else:
            end_interval = end_dt.replace(minute=end_minutes, second=0, microsecond=0)
        
        # Generate 15-minute intervals within the range
        intervals = []
        current = start_interval
        while current <= end_interval:
            intervals.append(current)
            current += timedelta(minutes=15)
        
        # Find the closest measurement for each interval
        filtered_points = []
        for interval in intervals:
            closest = min(parsed_points, key=lambda x: abs(x[0] - interval))
            time_diff = abs(closest[0] - interval)
            # Only include if within 7.5 minutes
            if time_diff <= timedelta(minutes=7.5):
                interval_str = interval.strftime("%H:%M")
                filtered_points.append({
                    'time_of_day': interval_str,
                    'queue_time': closest[1],
                    'is_closed': closest[2]
                })
        
        if filtered_points:
            filtered_data.append({
                'ride_id': ride_id,
                'park_id': park_id,
                'ride_name': ride_name,
                'data_points': filtered_points
            })
    
    logger.debug(f"Filtered data contains {len(filtered_data)} rides")
    return filtered_data

def generate_date_range(start_date, end_date, exclude_months, logger):
    """
    Generates a list of dates between start_date and end_date (inclusive) in YYYY/MM/DD format.
    
    Args:
        start_date (str): Start date in YYYY/MM/DD format
        end_date (str): End date in YYYY/MM/DD format
        exclude_months (list): List of months to exclude (1-12)
        logger: Logger instance for logging actions
    
    Returns:
        list: Ordered list of dates in YYYY/MM/DD format
    
    Raises:
        ValueError: If dates are invalid or end_date is before start_date
    """
    try:
        start = datetime.strptime(start_date, '%Y/%m/%d')
        end = datetime.strptime(end_date, '%Y/%m/%d')
    except ValueError as e:
        logger.error(f"Invalid date format for start_date ({start_date}) or end_date ({end_date}): {e}")
        raise ValueError("Dates must be in YYYY/MM/DD format")
    
    if end < start:
        logger.error(f"end_date ({end_date}) is before start_date ({start_date})")
        raise ValueError("end_date must not be before start_date")
    
    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime('%Y/%m/%d'))
        current += timedelta(days=1)

    # Filter out excluded months 
    date_list = [date for date in date_list if int(date.split('/')[1]) not in exclude_months]
    # Sort dates in ascending order
    date_list.sort(key=lambda x: datetime.strptime(x, '%Y/%m/%d'))
    
    logger.debug(f"Generated {len(date_list)} dates from {start_date} to {end_date}")
    return date_list