import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set the style for seaborn
sns.set(style="whitegrid")

# Define the path to your Parquet file
parquet_file = "../rplace_optimized.parquet"  # Change this if your file has a different name or path

# DuckDB SQL query to bucket placements by hour for the top 3 pixels
query = f"""
WITH hourly_placements AS (
    SELECT 
        date_trunc('hour', timestamp) AS hour,
        x_coord,
        y_coord,
        COUNT(*) AS placement_count
    FROM '{parquet_file}'
    WHERE (x_coord = 0 AND y_coord = 0)
       OR (x_coord = 359 AND y_coord = 564)
       OR (x_coord = 349 AND y_coord = 564)
    GROUP BY 1, 2, 3
)
SELECT *
FROM hourly_placements
ORDER BY hour;
"""

# Execute the query using DuckDB and convert the result into a Pandas DataFrame
df = duckdb.query(query).df()

# Create a new column to label each pixel coordinate (for visualization)
df['pixel'] = df.apply(lambda row: f"({row['x_coord']}, {row['y_coord']})", axis=1)

# For a time series plot, it's useful to have the 'hour' column as a datetime type
df['hour'] = pd.to_datetime(df['hour'])

# If you prefer to have one record for every hour in the full time range, you can pivot and reindex.
# First, create a complete hourly range from min to max.
full_range = pd.date_range(start=df['hour'].min(), end=df['hour'].max(), freq='H')

# Pivot the data so that each pixel gets its own column
pivot_df = df.pivot(index='hour', columns='pixel', values='placement_count').fillna(0)

# Reindex the DataFrame to include every hour in the range (filling missing hours with 0)
pivot_df = pivot_df.reindex(full_range, fill_value=0)
pivot_df.index.name = 'hour'
pivot_df.reset_index(inplace=True)

# Melt the DataFrame back to long-form for seaborn plotting
melted_df = pd.melt(pivot_df, id_vars="hour", var_name="pixel", value_name="placement_count")

# Plotting: Use a line plot with markers to show the distribution over time.
plt.figure(figsize=(14, 8))
sns.lineplot(data=melted_df, x='hour', y='placement_count', hue='pixel', marker="o")

# Format the plot
plt.xlabel("Hour")
plt.ylabel("Number of Placements")
plt.title("Hourly Distribution of Placements for Top 3 Pixels")
plt.xticks(rotation=45)
plt.tight_layout()

# Display the plot
plt.show()
