import duckdb

def get_color_percentage_at_location(parquet_file_path, target_color, x_coord, y_coord):
    # Connect to DuckDB
    con = duckdb.connect()
    
    # Query to calculate both overall and location-specific percentages
    query = """
        WITH overall_stats AS (
            SELECT 
                COUNT(*) as total_placements,
                COUNT(CASE WHEN LOWER(pixel_color) = LOWER(?) THEN 1 END) as color_placements
            FROM read_parquet(?)
        ),
        location_stats AS (
            SELECT 
                COUNT(*) as location_total_placements,
                COUNT(CASE WHEN LOWER(pixel_color) = LOWER(?) THEN 1 END) as location_color_placements
            FROM read_parquet(?)
            WHERE x_coord = ? AND y_coord = ?
        )
        SELECT 
            o.color_placements,
            o.total_placements,
            ROUND(100.0 * o.color_placements / o.total_placements, 2) as overall_percentage,
            l.location_color_placements,
            l.location_total_placements,
            ROUND(CASE 
                WHEN l.location_total_placements > 0 
                THEN 100.0 * l.location_color_placements / l.location_total_placements
                ELSE 0 
            END, 2) as location_percentage
        FROM overall_stats o, location_stats l
    """
    
    # Execute the query and fetch results
    result = con.execute(query, [
        target_color, parquet_file_path,  # For overall stats
        target_color, parquet_file_path,  # For location stats
        x_coord, y_coord
    ]).fetchone()
    
    # Close the connection
    con.close()
    
    return {
        'color': target_color,
        'x_coord': x_coord,
        'y_coord': y_coord,
        'color_placements_overall': result[0],
        'total_placements_overall': result[1],
        'overall_percentage': result[2],
        'color_placements_at_location': result[3],
        'total_placements_at_location': result[4],
        'location_percentage': result[5]
    }

def print_color_location_stats(parquet_file, color, x, y):
    stats = get_color_percentage_at_location(parquet_file, color, x, y)
    
    print(f"Statistics for color '{stats['color']}' at coordinate ({stats['x_coord']}, {stats['y_coord']}):")
    print("\nOverall Statistics:")
    print(f"Total placements of this color: {stats['color_placements_overall']:,}")
    print(f"Total placements overall: {stats['total_placements_overall']:,}")
    print(f"Overall percentage: {stats['overall_percentage']}%")
    
    print("\nLocation-specific Statistics:")
    print(f"Color placements at this location: {stats['color_placements_at_location']:,}")
    print(f"Total placements at this location: {stats['total_placements_at_location']:,}")
    print(f"Percentage at this location: {stats['location_percentage']}%")

# Example usage
parquet_file = "../rplace_optimized.parquet"
color = "#FF4500"  # Replace with your target color
x_coordinate = 349  # Replace with your x coordinate
y_coordinate = 564  # Replace with your y coordinate

print_color_location_stats(parquet_file, color, x_coordinate, y_coordinate)