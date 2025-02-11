from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, count, round as spark_round

def get_color_percentage(parquet_file_path, target_color):
    # Create a Spark session
    spark = SparkSession.builder.appName("ColorStats")  \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager") \
        .getOrCreate()
    
    # Read the parquet file into a DataFrame
    df = spark.read.parquet(parquet_file_path)
    
    # Total placements
    total_placements = df.count()
    
    # Count placements for the target color (case-insensitive)
    color_placements = df.filter(lower(col("pixel_color")) == target_color.lower()).count()
    
    # Calculate percentage (round to 2 decimal places)
    percentage = round(100.0 * color_placements / total_placements, 2) if total_placements > 0 else 0.0
    
    # Optionally, stop the Spark session if this is the only task
    spark.stop()
    
    return {
        'color': target_color,
        'color_placements': color_placements,
        'total_placements': total_placements,
        'percentage': percentage
    }

def print_color_stats(parquet_file, color):
    stats = get_color_percentage(parquet_file, color)
    print(f"Color Statistics for '{stats['color']}':")
    print(f"Number of placements: {stats['color_placements']:,}")
    print(f"Total placements: {stats['total_placements']:,}")
    print(f"Percentage: {stats['percentage']}%")

# Example usage
if __name__ == "__main__":
    parquet_file = "../rplace_optimized.parquet"
    color = "blue"  # Replace with your target color
    print_color_stats(parquet_file, color)
