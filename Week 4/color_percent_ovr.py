import duckdb

def get_color_percentage(parquet_file_path, target_color):
    # Connect to DuckDB
    con = duckdb.connect()
    
    # Query to calculate the percentage
    query = """
        WITH stats AS (
            SELECT 
                COUNT(*) as total_placements,
                COUNT(CASE WHEN LOWER(pixel_color) = LOWER(?) THEN 1 END) as color_placements
            FROM read_parquet(?)
        )
        SELECT 
            color_placements,
            total_placements,
            ROUND(100.0 * color_placements / total_placements, 2) as percentage
        FROM stats
    """
    
    # Execute the query and fetch results
    result = con.execute(query, [target_color, parquet_file_path]).fetchone()
    
    # Close the connection
    con.close()
    
    return {
        'color': target_color,
        'color_placements': result[0],
        'total_placements': result[1],
        'percentage': result[2]
    }

# Usage example
def print_color_stats(parquet_file, color):
    stats = get_color_percentage(parquet_file, color)
    print(f"Color Statistics for '{stats['color']}':")
    print(f"Number of placements: {stats['color_placements']:,}")
    print(f"Total placements: {stats['total_placements']:,}")
    print(f"Percentage: {stats['percentage']}%")

# Example usage
parquet_file = "../rplace_optimized.parquet"
color = "blue"  # Replace with your target color
print_color_stats(parquet_file, color)