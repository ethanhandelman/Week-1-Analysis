import csv
import multiprocessing
import argparse
from time import perf_counter_ns as pcn
from collections import Counter
from datetime import datetime as dt

INPUT_DT_FORMAT = '%Y-%m-%d %H'
DATA_DT_FORMAT = '%Y-%m-%d %H:%M:%S.%f UTC'

CHUNK_SIZE = 200_000
NUM_WORKERS = max(multiprocessing.cpu_count() - 1, 1)

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
        # Parse the input date string
        
        # Convert to the target format
        # This will set minutes, seconds to 0 and microseconds to 000000
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

def analyze_chunk(lines, start_time, end_time):
    """
    Given a list of CSV lines (as raw text), parse them and count
    only those whose timestamps are within [start_time, end_time].
    """
    color_counter = Counter()
    pos_counter = Counter()
    reader = csv.reader(lines)
    for row in reader:
        # row[0] = timestamp, row[1] = user_id, row[2] = pixel_color, row[3] = coordinate
        timestamp = row[0]
        if start_time <= timestamp <= end_time:
            color_counter[row[2]] += 1
            pos_counter[row[3]] += 1
    return color_counter, pos_counter

def analyze_parallel(filename, start_time, end_time):
    """
    Analyzes a CSV file in parallel to count color and position frequencies within a time range.
    Uses multiprocessing to distribute chunks of the file across multiple worker processes.
    
    The function reads the file line by line, groups lines into chunks, and processes each chunk
    in parallel using a worker pool. Results from all chunks are merged into final counters.
    
    Args:
        filename (str): Path to the CSV file to analyze
        start_time (datetime): Start of the time range to analyze
        end_time (datetime): End of the time range to analyze
        
    Returns:
        tuple: A pair of Counter objects (color_counter, pos_counter) containing:
            - color_counter: Frequency count of each color used
            - pos_counter: Frequency count of each position modified
            
    Note:
        - Assumes the input file has a header row that will be skipped
        - Uses global constants NUM_WORKERS for pool size and CHUNK_SIZE for chunk length
        - Requires an analyze_chunk() helper function to process individual chunks
    """
        
    color_counter = Counter()
    pos_counter = Counter()

    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        tasks=[]
        with open(filename, 'r', newline='') as f:
            # Skip header
            next(f)

            # Collect lines in chunks
            lines_chunk = []
            for line in f:
                lines_chunk.append(line)
                if len(lines_chunk) >= CHUNK_SIZE:
                    # Process this chunk
                    tasks.append(
                        pool.apply_async(analyze_chunk, (lines_chunk, start_time, end_time))
                    )
                    lines_chunk = []

            # Process any leftover lines
            if lines_chunk:
                tasks.append(
                    pool.apply_async(analyze_chunk, (lines_chunk, start_time, end_time))
                )

        # Wait for tasks to finish and merge counters
        for t in tasks:
            chunk_color_counter, chunk_pos_counter = t.get()
            color_counter.update(chunk_color_counter)
            pos_counter.update(chunk_pos_counter)

    return color_counter, pos_counter

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='filename to the CSV file')
    parser.add_argument('start', type=parse_datetime_arg, help='Start time (YYYY-MM-DD HH)')
    parser.add_argument('end', type=parse_datetime_arg, help='End time (YYYY-MM-DD HH)')
    
    args = parser.parse_args()
    start_time, end_time = validate_time_range(args.start, args.end)
    print(f'Analyzing colors between {start_time} and {end_time}...')

    t0 = pcn()

    color_counter, pos_counter = analyze_parallel(args.filename, start_time, end_time)

    t1 = pcn()

    for color, count in color_counter.most_common(10):
        print(f"{color}: {count}")
  
    for pos, count in pos_counter.most_common(10):
        print(f"{pos}: {count}")

    elapsed_ms = (t1 - t0) / 1_000_000
    print(f"Execution time (multicore, chunk size {CHUNK_SIZE}): {elapsed_ms:.2f} ms")

    print(f"- **Timeframe:** {args.start} to {args.end}")
    print(f"- **Execution Time:** {elapsed_ms:.0f} ms")
    print(f"- **Most Placed Color:** {color_counter.most_common(1)[0]}")
    print(f"- **Most Placed Pixel Location:** ({pos_counter.most_common(1)[0]})")

#minimum date: 2022-04-01 12:44:10.315 UTC
#maximum date: 2022-04-05 00:14:00.207 UTC

if __name__ == '__main__':
    main()