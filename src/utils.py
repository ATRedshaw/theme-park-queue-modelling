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
                'data_points': filtered_points
            })
    
    logger.debug(f"Filtered data contains {len(filtered_data)} rides")
    return filtered_data