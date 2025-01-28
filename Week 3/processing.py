import argparse
from datetime import datetime as dt
from time import perf_counter_ns as pcn
from collections import Counter
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa
import pyarrow.dataset as ds

INPUT_DT_FORMAT = '%Y-%m-%d %H'

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
    Convert a date from '%Y-%m-%d %H' format to timestamp
    """
    try:
        return parsed_date
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

def read_parquet_data(file_path, start_time, end_time):
    """
    Read Parquet file with time range filter, selecting only needed columns
    """
    try:
        # Read the data
        table = pq.read_table(
            file_path,
            columns=['timestamp', 'user_id_encoded', 'pixel_color']
        )
        
        # Convert timestamps to compatible format and filter
        mask = pc.and_(
            pc.greater_equal(table['timestamp'], pa.scalar(start_time, type=pa.timestamp('us'))),
            pc.less(table['timestamp'], pa.scalar(end_time, type=pa.timestamp('us')))
        )
        
        filtered_table = table.filter(mask)
        
        # Drop the timestamp column as it's no longer needed
        return filtered_table.select(['user_id_encoded', 'pixel_color'])
        
    except Exception as e:
        print(f"Error reading Parquet file: {str(e)}")
        raise

def process_data(table):
    """
    Count distinct users per color using PyArrow's compute functions
    """
    # Create a dictionary to store color-wise unique users
    color_counts = {}
    
    # Get unique colors
    unique_colors = pc.unique(table['pixel_color'])
    
    # For each color, count distinct users
    for color in unique_colors:
        color_str = color.as_py()
        mask = pc.equal(table['pixel_color'], color)
        color_table = table.filter(mask)
        distinct_users = len(pc.unique(color_table['user_id_encoded']))
        color_counts[color_str] = distinct_users
    
    # Convert to sorted list of tuples
    results = list(color_counts.items())
    return sorted(results, key=lambda x: x[1], reverse=True)

def format_results(results):
    """
    Format the color ranking results
    """
    output = []
    output.append("\nColor Rankings by Distinct Users:")
    output.append("---------------------------------")
    
    for rank, (color, count) in enumerate(results, 1):
        output.append(f"{rank}. {color}: {count:,} distinct users")
    
    return "\n".join(output)

def main():
    parser = argparse.ArgumentParser(description='Analyze color usage by distinct users in r/place data')
    parser.add_argument('database', help='path to Parquet file')
    parser.add_argument('start', type=parse_datetime_arg, help='Start time (YYYY-MM-DD HH)')
    parser.add_argument('end', type=parse_datetime_arg, help='End time (YYYY-MM-DD HH)')
    
    args = parser.parse_args()
    start_time, end_time = validate_time_range(args.start, args.end)
    print(f'Analyzing color usage between {start_time} and {end_time}...')

    t0 = pcn()

    # Read and process data
    table = read_parquet_data(args.database, start_time, end_time)
    results = process_data(table)

    t1 = pcn()

    # Output results
    print(format_results(results))
    
    elapsed_ms = (t1 - t0) / 1_000_000
    print(f"\nExecution time: {elapsed_ms:.2f} ms")

if __name__ == '__main__':
    main()