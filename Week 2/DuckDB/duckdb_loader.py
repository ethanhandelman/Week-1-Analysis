import duckdb

def load_csv_to_duckdb(csv_path, table_name, database_path=':memory:'):
    """
    Load a CSV file into a DuckDB database.
    
    Args:
        csv_path (str): Path to the CSV file
        table_name (str): Name for the table to be created
        database_path (str): Path to the DuckDB database file. Defaults to in-memory database.
    """
    # Connect to DuckDB (creates a new database if it doesn't exist)
    conn = duckdb.connect(database=database_path)
    
    try:
        # Create table and load CSV data
        # DuckDB will automatically infer the schema from the CSV
        conn.sql(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{csv_path}')
        """)
        
        # Verify the data was loaded
        row_count = conn.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Successfully loaded {row_count} rows into table '{table_name}'")
        
        # Show the table schema
        print("\nTable schema:")
        schema = conn.sql(f"DESCRIBE {table_name}").fetchall()
        for column in schema:
            print(f"Column: {column[0]}, Type: {column[1]}")
            
    finally:
        # Close the connection
        conn.close()

# Example usage
if __name__ == "__main__":
    
    # Or load into a persistent database file
    load_csv_to_duckdb(
        csv_path='../2022_place_canvas_history.csv',
        table_name='rplace',
        database_path='../RPDB-DUCK.db'
    )