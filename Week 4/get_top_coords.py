import duckdb
import pandas as pd

def get_top_coordinates(parquet_file_path):
    # Connect to DuckDB
    con = duckdb.connect()
    
    # Create a query to count occurrences of each (x, y) coordinate pair
    query = """
        SELECT 
            x_coord,
            y_coord,
            COUNT(*) as placement_count
        FROM read_parquet(?)
        GROUP BY x_coord, y_coord
        ORDER BY placement_count DESC
        LIMIT 10
    """
    
    # Execute the query and fetch results
    result = con.execute(query, [parquet_file_path]).fetchdf()
    
    # Close the connection
    con.close()
    
    return result

# Usage example
parquet_file = "../rplace_optimized.parquet"
top_coordinates = get_top_coordinates(parquet_file)
print("Top 3 most frequently placed coordinates:")
print(top_coordinates)

'''
Top 3 most frequently placed coordinates:
   x_coord  y_coord  placement_count
0        0        0            98807
1      359      564            69198
2      349      564            55230
'''