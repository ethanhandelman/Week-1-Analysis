import duckdb
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import string

# Define the path to your optimized parquet file.
parquet_file = "../rplace_optimized.parquet"

# DuckDB SQL query:
# Determine the block indices by dividing coordinates by 100 and count placements.
query = f"""
SELECT
    CAST(floor(x_coord / 100) AS INTEGER) AS block_x,
    CAST(floor(y_coord / 100) AS INTEGER) AS block_y,
    COUNT(*) AS placements
FROM '{parquet_file}'
GROUP BY block_x, block_y
ORDER BY block_y, block_x;
"""

# Execute the query and load the results into a pandas DataFrame.
df = duckdb.query(query).df()

# Pivot the DataFrame so that rows are block_y and columns are block_x (2000/100 = 20 blocks per axis).
heatmap_data = df.pivot(index='block_y', columns='block_x', values='placements').fillna(0)
heatmap_data = heatmap_data.sort_index(ascending=True)

# Define a formatting function to scale large numbers to "K" or "M" with up to 4 total digits,
# and a maximum of 2 digits after the decimal point.
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
    # If more than 2 decimals, force .2f, then trim trailing zeros.
    if "." in formatted:
        decimals = formatted.split(".")[1]
        if len(decimals) > 2:
            formatted = f"{value:.2f}".rstrip("0").rstrip(".")
    return formatted + suffix

# Create an annotation DataFrame by applying the formatting function to each cell.
annot_data = heatmap_data.applymap(format_number)

# Create a heatmap with annotations.
plt.figure(figsize=(12, 10))
ax = sns.heatmap(heatmap_data, annot=annot_data, fmt="", cmap="viridis",
                 cbar_kws={'label': 'Number of Placements'})

# Modify Y-axis tick labels to use letters (A, B, C, ...).
num_rows = heatmap_data.shape[0]
letters = list(string.ascii_uppercase)[:num_rows]
ax.set_yticklabels(letters, rotation=0)

# Set axis labels and title.
ax.set_xlabel("Block X (each block = 100 pixels)", fontsize=12)
ax.set_ylabel("Block Y (A, B, C, …)", fontsize=12)
ax.set_title("Heatmap of Placements per 100×100 Region on a 2000×2000 Grid", fontsize=14)

plt.tight_layout()
plt.show()
