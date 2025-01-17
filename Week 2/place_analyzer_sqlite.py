import argparse
import sqlite3
from datetime import datetime as dt
from time import perf_counter_ns as pcn
from collections import Counter

INPUT_DT_FORMAT = '%Y-%m-%d %H'
DATA_DT_FORMAT = '%Y-%m-%d %H:%M:%S.%f UTC'

def parse_datetime_arg(dt_str):
    """
    Confirms that date given in program args is formatted correctly
    """
    try:
        return dt.strptime(dt_str, INPUT_DT_FORMAT)
    except ValueError:
        raise argparse.ArgumentTypeError(f'Invalid datetime format: {dt_str}. Use {INPUT_DT_FORMAT}')

def convert_date_format(parsed_date):
    """
    Convert a date from '%Y-%m-%d %H' format to '%Y-%m-%d %H:%M:%S.%f UTC' format
    """
    try:
        formatted_date = parsed_date.strftime('%Y-%m-%d %H:00:00.000000 UTC')
        return formatted_date
    except ValueError as e:
        return f"Error: Invalid date format. Please use YYYY-MM-DD HH format. {str(e)}"

def validate_time_range(start_time, end_time):
    """
    Given a start and end time checks that start time is before end time,
    if so returns formatted dates
    """
    if end_time <= start_time:
        raise argparse.ArgumentTypeError('End time must be after start time')
    return convert_date_format(start_time), convert_date_format(end_time)

def analyze_sqlite(db_path, start_time, end_time):
    """
    Analyzes SQLite database to count color and position frequencies within a time range.
    
    Args:
        db_path (str): Path to the SQLite database
        start_time (str): Start of the time range to analyze (in DATA_DT_FORMAT)
        end_time (str): End of the time range to analyze (in DATA_DT_FORMAT)
        
    Returns:
        tuple: A pair of Counter objects (color_counter, pos_counter)
    """
    color_counter = Counter()
    pos_counter = Counter()
    
    with sqlite3.connect(db_path) as conn:
        # Assuming the table is named 'rplace' with columns:
        # timestamp, user_id, pixel_color, coordinate
        
        # Get color counts
        color_query = """
            SELECT pixel_color, COUNT(*) as count
            FROM rplace
            WHERE timestamp BETWEEN ? AND ?
            GROUP BY pixel_color
        """
        
        for color, count in conn.execute(color_query, (start_time, end_time)):
            color_counter[color] = count
            
        # Get position counts
        pos_query = """
            SELECT coordinate, COUNT(*) as count
            FROM rplace
            WHERE timestamp BETWEEN ? AND ?
            GROUP BY coordinate
        """
        
        for pos, count in conn.execute(pos_query, (start_time, end_time)):
            pos_counter[pos] = count
            
    return color_counter, pos_counter

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='path to the SQLite database')
    parser.add_argument('start', type=parse_datetime_arg, help='Start time (YYYY-MM-DD HH)')
    parser.add_argument('end', type=parse_datetime_arg, help='End time (YYYY-MM-DD HH)')
    
    args = parser.parse_args()
    start_time, end_time = validate_time_range(args.start, args.end)
    print(f'Analyzing colors between {start_time} and {end_time}...')

    t0 = pcn()

    color_counter, pos_counter = analyze_sqlite(args.database, start_time, end_time)

    t1 = pcn()

    print("\nTop 10 Colors:")
    for color, count in color_counter.most_common(10):
        print(f"{color}: {count}")
    
    print("\nTop 10 Positions:")
    for pos, count in pos_counter.most_common(10):
        print(f"{pos}: {count}")

    elapsed_ms = (t1 - t0) / 1_000_000
    print(f"\nExecution time: {elapsed_ms:.2f} ms")

    print(f"\nSummary:")
    print(f"- **Timeframe:** {args.start} to {args.end}")
    print(f"- **Execution Time:** {elapsed_ms:.0f} ms")
    print(f"- **Most Placed Color:** {color_counter.most_common(1)[0]}")
    print(f"- **Most Placed Pixel Location:** {pos_counter.most_common(1)[0]}")

if __name__ == '__main__':
    main()