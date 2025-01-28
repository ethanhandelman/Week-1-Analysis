import duckdb
import os
from datetime import datetime

def export_to_parquet(db_path, output_path="rplace_optimized.parquet"):
    # Ensure database path exists
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found at: {db_path}")
    
    # Connect to the database
    conn = duckdb.connect(db_path)
    
    try:
        # Create optimized view
        conn.execute("""
            CREATE OR REPLACE VIEW optimized_data AS
            SELECT 
                CAST(timestamp AS TIMESTAMP) as timestamp,
                DENSE_RANK() OVER (ORDER BY user_id) as user_id_encoded,
                pixel_color,
                CAST(SPLIT_PART(coordinate, ',', 1) AS INTEGER) as x_coord,
                CAST(SPLIT_PART(coordinate, ',', 2) AS INTEGER) as y_coord
            FROM rplace;
        """)
        
        # Export to Parquet
        conn.execute(f"""
            COPY (
                SELECT 
                    timestamp,
                    user_id_encoded,
                    x_coord,
                    y_coord,
                    pixel_color
                FROM optimized_data
            ) TO '{output_path}' (
                FORMAT 'parquet',
                COMPRESSION 'ZSTD',
                ROW_GROUP_SIZE 100000
            );
        """)
        
        print(f"Successfully exported data to {output_path}")
        
    except Exception as e:
        print(f"Error during export: {str(e)}")
        raise
    
    finally:
        conn.close()

if __name__ == "__main__":
    DB_PATH = "../RPDB-DUCK.db"
    
    # Create timestamp-based output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    OUTPUT_PATH = f"rplace_optimized_{timestamp}.parquet"
    
    export_to_parquet(DB_PATH, OUTPUT_PATH)