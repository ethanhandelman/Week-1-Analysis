import polars as pl
import argparse
from datetime import datetime as dt
from time import perf_counter_ns as pcn

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

def analyze_data(filename, start_time, end_time):
    """
    Analyzes a CSV file using Polars to count color and position frequencies within a time range.
    Takes advantage of Polars' lazy evaluation and parallel processing capabilities.
    
    Args:
        filename (str): Path to the CSV file to analyze
        start_time (str): Start of the time range to analyze (in DATA_DT_FORMAT)
        end_time (str): End of the time range to analyze (in DATA_DT_FORMAT)
        
    Returns:
        tuple: Two DataFrames containing:
            - color_counts: Frequency count of each color used
            - pos_counts: Frequency count of each position modified
    """
    # Create a lazy DataFrame with optimized schema
    df = pl.scan_csv(
        filename,
        schema={
            "timestamp": pl.Utf8,  # We'll parse this as datetime after filtering
            "user_id": pl.Utf8,    # Not used in analysis but included for schema
            "pixel_color": pl.Categorical,  # More efficient for repeated strings
            "coordinate": pl.Categorical    # More efficient for repeated coordinates
        }
    )
    
    # Create our analysis pipeline using lazy evaluation
    filtered_df = (
        df.filter(
            (pl.col("timestamp") >= start_time) &
            (pl.col("timestamp") <= end_time)
        )
    )
    
    # Calculate color frequencies
    color_counts = (
        filtered_df
        .group_by("pixel_color")
        .agg(pl.count().alias("count"))
        .sort("count", descending=True)
        .collect()
    )
    
    # Calculate position frequencies
    pos_counts = (
        filtered_df
        .group_by("coordinate")
        .agg(pl.count().alias("count"))
        .sort("count", descending=True)
        .collect()
    )
    
    return color_counts, pos_counts

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='filename to the CSV file')
    parser.add_argument('start', type=parse_datetime_arg, help='Start time (YYYY-MM-DD HH)')
    parser.add_argument('end', type=parse_datetime_arg, help='End time (YYYY-MM-DD HH)')
    
    args = parser.parse_args()
    start_time, end_time = validate_time_range(args.start, args.end)
    print(f'Analyzing colors between {start_time} and {end_time}...')

    t0 = pcn()
    
    color_counts, pos_counts = analyze_data(args.filename, start_time, end_time)
    
    t1 = pcn()
    
    # Print top 10 colors
    print("\nTop 10 Colors:")
    print(color_counts.head(10))
    
    # Print top 10 positions
    print("\nTop 10 Positions:")
    print(pos_counts.head(10))
    
    elapsed_ms = (t1 - t0) / 1_000_000
    
    # Print summary
    print(f"\nSummary:")
    print(f"- **Timeframe:** {args.start} to {args.end}")
    print(f"- **Execution Time:** {elapsed_ms:.0f} ms")
    
    top_color = color_counts.row(0)
    top_pos = pos_counts.row(0)
    print(f"- **Most Placed Color:** {top_color[0]}: {top_color[1]}")
    print(f"- **Most Placed Pixel Location:** {top_pos[0]}: {top_pos[1]}")

if __name__ == '__main__':
    main()