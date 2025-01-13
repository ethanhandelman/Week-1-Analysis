import csv
import io
import argparse
from collections import Counter
from datetime import datetime as dt

INPUT_DT_FORMAT = '%Y-%m-%d %H'
DATA_DT_FORMAT = '%Y-%m-%d %H:%M:%S.%f UTC'

def parse_datetime_arg(dt_str):
    try:
        return dt.strptime(dt_str, INPUT_DT_FORMAT)
    except ValueError:
        raise argparse.ArgumentTypeError(f'Invalid datetime format: {dt_str}. Use {INPUT_DT_FORMAT}')
    
def parse_datetime_data(dt_str):
    try:
        return dt.strptime(dt_str, DATA_DT_FORMAT)
    except ValueError:
        raise argparse.ArgumentTypeError(f'Invalid datetime format: {dt_str}. Use {DATA_DT_FORMAT}')
    
def convert_date_format(parsed_date):
    """
    Convert a date from '%Y-%m-%d %H' format to '%Y-%m-%d %H:%M:%S.%f UTC' format
    
    Args:
        date_str (str): Date string in format '%Y-%m-%d %H'
        
    Returns:
        str: Date string in format '%Y-%m-%d %H:%M:%S.%f UTC'
    """
    try:
        # Parse the input date string
        
        # Convert to the target format
        # This will set minutes, seconds to 0 and microseconds to 000000
        formatted_date = parsed_date.strftime('%Y-%m-%d %H:00:00.000000 UTC')
        
        return formatted_date
    except ValueError as e:
        return f"Error: Invalid date format. Please use YYYY-MM-DD HH format. {str(e)}"

def validate_time_range(start_time, end_time):
    if end_time <= start_time:
        raise argparse.ArgumentTypeError('End time must be after start time')
    return convert_date_format(start_time), convert_date_format(end_time)

def main():
    parser = argparse.ArgumentParser(description='Analyze color placement frequency in time range')
    parser.add_argument('filename', help='filename to the CSV file')
    parser.add_argument('start', type=parse_datetime_arg, help='Start time (YYYY-MM-DD HH)')
    parser.add_argument('end', type=parse_datetime_arg, help='End time (YYYY-MM-DD HH)')
    
    args = parser.parse_args()
    start_time, end_time = validate_time_range(args.start, args.end)
    
    print(f'Analyzing colors between {start_time} and {end_time}...')

    color_counter = Counter()
    min_date = "2022-04-04 00:53:51.577 UTC"

    with open(args.filename, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader) #skip header
        for row in reader:
            # row[0] = timestamp, row[1] = user_id, row[2] = pixel_color, row[3] = coordinate
            # Adjust if columns differ
            #try:
            #    row_dt = parse_datetime_data(row[0])
            #except Exception:
            #    # Skip malformed or header lines
            #    continue
            if(row[0] < min_date):
                min_date = row[0]

            #if start_time <= row[0] <= end_time:
            #    # Count the color usage
            #    print("in interval")
            #    color_counter[row[2]] += 1
            #elif row[0] > end_time:
            #    print(f"out of interval: {row[0]}")
            #    break

    # Print results
    for color, count in color_counter.most_common(10):
        print(f"{color}: {count}")
    

if __name__ == '__main__':
    main()