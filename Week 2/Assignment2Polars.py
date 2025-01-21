import polars as pl
import time
from datetime import datetime
from collections import Counter

# Load the entire CSV into a Polars DataFrame
df = pl.read_csv(
    "2022_place_canvas_history.csv",
    has_header=True,
    columns=["timestamp", "pixel_color", "coordinate"],  # Relevant columns
)

# Extract the hour from the timestamp for easier filtering
df = df.with_columns(
    pl.col("timestamp").str.slice(0, 13).alias("hour")  # Extract "YYYY-MM-DD HH"
)

def print_most_common(start_hour, end_hour):
    start_time_process = time.time()

    # Filter rows within the timeframe
    filtered_df = df.filter(
        (pl.col("hour") >= start_hour) & (pl.col("hour") <= end_hour)
    )

    if filtered_df.is_empty():
        print(f"No data found for the timeframe: {start_hour} to {end_hour}")
        return

    # Use Counters to keep track of pixel color and coords
    pixel_color_counts = Counter(filtered_df["pixel_color"].to_list())
    coordinate_counts = Counter(filtered_df["coordinate"].to_list())

    # Get the most common pixel color and coordinate
    most_common_color = pixel_color_counts.most_common(1)[0]
    most_common_coord = coordinate_counts.most_common(1)[0]

    # Calculate timeframe duration
    start_dt = datetime.strptime(start_hour, "%Y-%m-%d %H")
    end_dt = datetime.strptime(end_hour, "%Y-%m-%d %H")
    timeframe_hours = (end_dt - start_dt).total_seconds() / 3600

    # Print results
    print(f"## {timeframe_hours:.0f}-Hour Timeframe")
    print(f"- **Timeframe:** {start_hour} to {end_hour}")
    print(f"- **Execution Time:** {(time.time() - start_time_process) * 1000:.0f} ms")
    print(f"- **Most Placed Color:** {most_common_color}")
    print(f"- **Most Placed Coordinate:** {most_common_coord}")

# Example Usage
print_most_common("2022-04-04 01", "2022-04-04 02")
print_most_common("2022-04-03 11", "2022-04-03 14")
print_most_common("2022-04-02 11", "2022-04-02 17")
