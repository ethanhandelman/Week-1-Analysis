import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Define the path to your Parquet file
parquet_file = "../rplace_optimized.parquet"  # update path as needed

# DuckDB SQL query to get the top 3 colors (by count) for each of the top 3 placed pixels
query = f"""
WITH pixel_color_counts AS (
    SELECT 
        x_coord,
        y_coord,
        pixel_color,
        COUNT(*) AS placement_count
    FROM '{parquet_file}'
    WHERE (x_coord = 0 AND y_coord = 0)
       OR (x_coord = 359 AND y_coord = 564)
       OR (x_coord = 349 AND y_coord = 564)
    GROUP BY x_coord, y_coord, pixel_color
),
ranked_colors AS (
    SELECT
        x_coord,
        y_coord,
        pixel_color,
        placement_count,
        ROW_NUMBER() OVER (PARTITION BY x_coord, y_coord ORDER BY placement_count DESC) AS rn
    FROM pixel_color_counts
)
SELECT
    x_coord,
    y_coord,
    pixel_color,
    placement_count
FROM ranked_colors
WHERE rn <= 3
ORDER BY x_coord, y_coord, rn;
"""

# Execute the query using DuckDB and load the result into a pandas DataFrame
df = duckdb.query(query).df()

# Create a column for labeling the pixel coordinate group
df['pixel'] = df.apply(lambda row: f"({row['x_coord']}, {row['y_coord']})", axis=1)

# Create a palette that maps each pixel_color (assumed to be a valid color string) to itself.
# This ensures that the bar for each color is filled with that exact color.
unique_colors = df['pixel_color'].unique()
palette = {color: color for color in unique_colors}

# Set up the plot using Seaborn's barplot with dodge=True to create grouped bars.
plt.figure(figsize=(10, 6))
ax = sns.barplot(data=df, x='pixel', y='placement_count', hue='pixel_color', dodge=True, palette=palette)

# Formatting the plot
ax.set_title("Top 3 Colors for Each of the Top 3 Placed Pixels", fontsize=16)
ax.set_xlabel("Pixel Coordinate", fontsize=14)
ax.set_ylabel("Placement Count", fontsize=14)

# Annotate each bar with its placement count for clarity
for p in ax.patches:
    height = p.get_height()
    if height > 0:
        ax.text(p.get_x() + p.get_width() / 2, height, f'{int(height)}',
                ha='center', va='bottom', fontsize=10)

plt.legend(title="Pixel Color", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
