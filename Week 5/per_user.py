import duckdb
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import string

# Define the path to your optimized parquet file.
parquet_file = "../rplace_optimized.parquet"

# DuckDB SQL query:
# Divide the coordinates into 100×100 blocks, count total placements, and count unique users per block.
query = f"""
SELECT
    CAST(floor(x_coord / 100) AS INTEGER) AS block_x,
    CAST(floor(y_coord / 100) AS INTEGER) AS block_y,
    COUNT(*) AS placements,
    COUNT(DISTINCT user_id_encoded) AS unique_users
FROM '{parquet_file}'
GROUP BY block_x, block_y
ORDER BY block_y, block_x;
"""

# Execute the query and load the results into a DataFrame.
df = duckdb.query(query).df()

# Compute placements per user for each region.
df['ppu'] = df['placements'] / df['unique_users']

# Pivot the DataFrame so that rows represent block_y and columns represent block_x.
# This creates a 20x20 grid (since 2000 / 100 = 20) for the placements per user metric.
heatmap_data = df.pivot(index='block_y', columns='block_x', values='ppu').fillna(0)
heatmap_data = heatmap_data.sort_index(ascending=True)

# Define a simple formatting function for placements per user (2 decimals).
def format_ppu(n):
    return f"{n:.2f}"

# Create an annotation DataFrame for the heatmap.
annot_data = heatmap_data.applymap(format_ppu)

# Create the heatmap based on placements per user.
plt.figure(figsize=(12, 10))
ax = sns.heatmap(heatmap_data, annot=annot_data, fmt="", cmap="viridis",
                 cbar_kws={'label': 'Placements per User'})

# Modify Y-axis tick labels to use letters (A, B, C, ...).
num_rows = heatmap_data.shape[0]
letters = list(string.ascii_uppercase)[:num_rows]
ax.set_yticklabels(letters, rotation=0)

# Set axis labels and title.
ax.set_xlabel("Block X (each block = 100 pixels)", fontsize=12)
ax.set_ylabel("Block Y (A, B, C, …)", fontsize=12)
ax.set_title("Heatmap of Placements per User per 100×100 Region on a 2000×2000 Grid", fontsize=14)

plt.tight_layout()

# --- Print the Top 10 Regions by Placements per User ---
# Sort the aggregated DataFrame by placements per user in descending order.
top_regions = df.sort_values(by='ppu', ascending=False).head(10)

print("Top 10 regions by placements per user:")
for idx, row in top_regions.iterrows():
    block_y = int(row['block_y'])
    block_x = int(row['block_x'])
    placements = int(row['placements'])
    unique_users = int(row['unique_users'])
    ppu = row['ppu']
    # Convert block_y to a letter; for block_x add 1 for human-friendly numbering.
    region_label = f"{letters[block_y]}{block_x + 1}"
    print(f"{region_label}: {placements:,} placements, {unique_users:,} unique users, {ppu:.2f} placements per user")

plt.show()
