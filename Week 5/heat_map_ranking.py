import duckdb
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import string

# Define the path to your optimized parquet file.
parquet_file = "../rplace_optimized.parquet"

# DuckDB SQL query:
# Divide the coordinates by 100 to get 100×100 blocks and count placements per block.
query = f"""
SELECT
    CAST(floor(x_coord / 100) AS INTEGER) AS block_x,
    CAST(floor(y_coord / 100) AS INTEGER) AS block_y,
    COUNT(*) AS placements
FROM '{parquet_file}'
GROUP BY block_x, block_y
ORDER BY block_y, block_x;
"""

# Execute the query and load the results into a DataFrame.
df = duckdb.query(query).df()

# Pivot the DataFrame so that rows represent block_y and columns represent block_x.
# (Since the canvas is 2000×2000, there are 20 blocks along each axis.)
heatmap_data = df.pivot(index='block_y', columns='block_x', values='placements').fillna(0)
heatmap_data = heatmap_data.sort_index(ascending=True)

# Define a custom number formatting function.
def format_number(n):
    if n >= 1e6:
        value = n / 1e6
        suffix = "M"
    elif n >= 1e3:
        value = n / 1e3
        suffix = "K"
    else:
        value = n
        suffix = ""
    
    # Format with 4 significant digits.
    formatted = format(value, ".4g")
    # Ensure a maximum of 2 digits after the decimal.
    if "." in formatted:
        decimals = formatted.split(".")[1]
        if len(decimals) > 2:
            formatted = f"{value:.2f}".rstrip("0").rstrip(".")
    return formatted + suffix

# Create an annotation DataFrame for the heatmap.
annot_data = heatmap_data.applymap(format_number)

# Create the heatmap.
plt.figure(figsize=(12, 10))
ax = sns.heatmap(heatmap_data, annot=annot_data, fmt="", cmap="viridis",
                 cbar_kws={'label': 'Number of Placements'})

# Modify Y-axis tick labels to use letters (A, B, C, ...).
num_rows = heatmap_data.shape[0]
letters = list(string.ascii_uppercase)[:num_rows]
ax.set_yticklabels(letters, rotation=0)

# (Optionally, modify the X-axis tick labels if desired. Currently they show 0-indexed block numbers.)

# Set axis labels and title.
ax.set_xlabel("Block X (each block = 100 pixels)", fontsize=12)
ax.set_ylabel("Block Y (A, B, C, …)", fontsize=12)
ax.set_title("Heatmap of Placements per 100×100 Region on a 2000×2000 Grid", fontsize=14)

plt.tight_layout()

# --- Print the Top 10 Regions ---

# Unstack the heatmap_data so that each region (block_y, block_x) is an entry.
regions_series = heatmap_data.stack()  # MultiIndex: (block_y, block_x)
# Sort regions in descending order by number of placements.
top_regions = regions_series.sort_values(ascending=False).head(20)

print("Top 10 regions by pixel placements:")
for (block_y, block_x), placements in top_regions.items():
    # Convert block_y to a letter; for block_x, add 1 to make it more human-readable.
    region_label = f"{letters[block_y]}{block_x}"
    print(f"{region_label}: {placements:,} placements")

plt.show()
