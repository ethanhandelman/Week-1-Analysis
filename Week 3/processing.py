import argparse
from datetime import datetime as dt, timedelta
from time import perf_counter_ns as pcn
from collections import defaultdict
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa
from statistics import mean

INPUT_DT_FORMAT = '%Y-%m-%d %H'
INACTIVITY_THRESHOLD = timedelta(minutes=15)

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
        # Read data
        table = pq.read_table(
            file_path,
            columns=['timestamp', 'user_id_encoded']
        )
        
        # Filter by time range
        mask = pc.and_(
            pc.greater_equal(table['timestamp'], pa.scalar(start_time, type=pa.timestamp('us'))),
            pc.less(table['timestamp'], pa.scalar(end_time, type=pa.timestamp('us')))
        )
        
        filtered_table = table.filter(mask)
        return filtered_table
        
    except Exception as e:
        print(f"Error reading Parquet file: {str(e)}")
        raise

def calculate_session_lengths(table):
    """
    Calculate session lengths for users with more than one pixel placement
    Returns list of session lengths in seconds
    """
    # Convert to Python for session processing
    # We'll process one user at a time to manage memory
    user_ids = pc.unique(table['user_id_encoded'])
    session_lengths = []
    
    for user_id in user_ids:
        # Get timestamps for this user
        mask = pc.equal(table['user_id_encoded'], user_id)
        user_table = table.filter(mask)
        
        if len(user_table) <= 1:
            continue
            
        # Sort timestamps
        timestamps = pc.array_sort(user_table['timestamp'])
        timestamps_py = [ts.as_py() for ts in timestamps]
        
        # Process sessions for this user
        current_session_start = timestamps_py[0]
        prev_timestamp = timestamps_py[0]
        user_sessions = []
        
        for timestamp in timestamps_py[1:]:
            time_diff = timestamp - prev_timestamp
            
            # If gap is more than threshold, end current session and start new one
            if time_diff > INACTIVITY_THRESHOLD:
                session_length = (prev_timestamp - current_session_start).total_seconds()
                if session_length > 0:  # Avoid zero-length sessions
                    user_sessions.append(session_length)
                current_session_start = timestamp
            
            prev_timestamp = timestamp
        
        # Add final session
        final_session_length = (prev_timestamp - current_session_start).total_seconds()
        if final_session_length > 0:
            user_sessions.append(final_session_length)
        
        # Add this user's sessions to overall list
        session_lengths.extend(user_sessions)
    
    return session_lengths

def format_session_results(session_lengths):
    """
    Format the session analysis results
    """
    if not session_lengths:
        return "\nNo valid sessions found in the specified time range."
    
    avg_session_length = mean(session_lengths)
    total_sessions = len(session_lengths)
    
    output = []
    output.append("\nSession Analysis Results:")
    output.append("-----------------------")
    output.append(f"Average Session Length: {avg_session_length:.2f} seconds")
    output.append(f"Total Sessions Analyzed: {total_sessions:,}")
    output.append(f"Session defined as: User activity within {INACTIVITY_THRESHOLD.seconds // 60} minutes of inactivity")
    
    return "\n".join(output)

def main():
    parser = argparse.ArgumentParser(description='Analyze user session lengths in r/place data')
    parser.add_argument('database', help='path to Parquet file')
    parser.add_argument('start', type=parse_datetime_arg, help='Start time (YYYY-MM-DD HH)')
    parser.add_argument('end', type=parse_datetime_arg, help='End time (YYYY-MM-DD HH)')
    
    args = parser.parse_args()
    start_time, end_time = validate_time_range(args.start, args.end)
    print(f'Analyzing sessions between {start_time} and {end_time}...')

    t0 = pcn()

    # Read and process data
    table = read_parquet_data(args.database, start_time, end_time)
    session_lengths = calculate_session_lengths(table)

    t1 = pcn()

    # Output results
    print(format_session_results(session_lengths))
    
    elapsed_ms = (t1 - t0) / 1_000_000
    print(f"\nExecution time: {elapsed_ms:.2f} ms")

if __name__ == '__main__':
    main()