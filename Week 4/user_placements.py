import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import argparse

def main(x_target, y_target, parquet_file):
    # Query: For the specified pixel, count how many placements each user made,
    # then group by that placement count.
    query = f"""
    WITH user_placements AS (
        SELECT 
            user_id_encoded,
            COUNT(*) AS placements
        FROM '{parquet_file}'
        WHERE x_coord = {x_target} AND y_coord = {y_target}
        GROUP BY user_id_encoded
    )
    SELECT 
        placements, 
        COUNT(*) AS num_users
    FROM user_placements
    GROUP BY placements
    ORDER BY placements;
    """
    
    # Execute the query and load the result into a DataFrame.
    df = duckdb.query(query).df()
    
    if df.empty:
        print(f"No placements found for pixel ({x_target}, {y_target}).")
        return

    # Limit the maximum shown placements to 6 by grouping any count >=6 into a single bin.
    df['placements_bin'] = df['placements'].apply(lambda x: x if x < 6 else 6)
    
    # Group by the new bin and sum the number of users.
    df_grouped = df.groupby('placements_bin', as_index=False)['num_users'].sum()
    
    # Create a label column; use "6+" for the bin representing 6 or more placements.
    df_grouped['bin_label'] = df_grouped['placements_bin'].apply(lambda x: f"{x}" if x < 6 else "6+")
    
    # Calculate the total number of users (for percentage calculation).
    total_users = df_grouped['num_users'].sum()
    
    # Create the bar plot.
    plt.figure(figsize=(10, 6))
    ax = plt.gca()
    
    bars = ax.bar(df_grouped['bin_label'], df_grouped['num_users'], color='skyblue', edgecolor='black')
    
    # Annotate each bar with two lines: percentage (top) and raw count (bottom).
    for bar in bars:
        height = bar.get_height()
        percentage = (height / total_users) * 100
        # Place annotation slightly above the top of the bar.
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            height + total_users * 0.01,  # small vertical offset relative to the total for clarity
            f"{percentage:.1f}%\n{int(height)}",
            ha='center',
            va='bottom',
            fontsize=10
        )
    
    ax.set_xlabel("Number of Placements per User (Bin)", fontsize=12)
    ax.set_ylabel("Number of Users", fontsize=12)
    ax.set_title(f"({x_target}, {y_target})", fontsize=14)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Visualize the distribution of user placements at a specific pixel location.'
    )
    parser.add_argument('--x', type=int, required=True, help='x coordinate of the pixel')
    parser.add_argument('--y', type=int, required=True, help='y coordinate of the pixel')
    parser.add_argument(
        '--file', type=str, default="../rplace_optimized.parquet",
        help='Path to the Parquet file containing r/place data'
    )
    
    args = parser.parse_args()
    main(args.x, args.y, args.file)
