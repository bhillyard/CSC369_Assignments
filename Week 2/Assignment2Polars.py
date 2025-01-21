import polars as pl
import time
from datetime import datetime
from collections import Counter

# Function to process data and find the most common pixel color and coordinate
def print_most_common(start_time, end_time, batch_size=100_000):
    file_path = "2022_place_canvas_history"
    start_time_process = time.time()

    # Convert input times to datetime
    start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    # Initialize separate Counters for pixel colors and coordinates
    pixel_color_counts = Counter()
    coordinate_counts = Counter()

    # Read the file in batches
    batches = pl.read_csv_batched(
        file_path,
        has_header=True,
        batch_size=batch_size,
        columns=["timestamp", "pixel_color", "coordinate"],  # Load only necessary columns
        try_parse_dates=True,  # Automatically parse dates
    )

    # Process each batch
    for batch in batches:
        # Filter rows within the specified time range
        filtered_batch = batch.filter(
            (pl.col("timestamp") >= pl.lit(start_dt)) & (pl.col("timestamp") <= pl.lit(end_dt))
        )

        # Update Counters with the current batch
        pixel_color_counts.update(filtered_batch["pixel_color"].to_list())
        coordinate_counts.update(filtered_batch["coordinate"].to_list())

    # Find the most common pixel color and coordinate
    if pixel_color_counts and coordinate_counts:
        most_common_color, color_count = pixel_color_counts.most_common(1)[0]
        most_common_coord, coord_count = coordinate_counts.most_common(1)[0]

        # Calculate timeframe duration
        timeframe_hours = (end_dt - start_dt).total_seconds() / 3600

        # End processing time
        end_time_process = time.time()

        # Print the results
        print(f"## {timeframe_hours:.0f}-Hour Timeframe")
        print(f"- **Timeframe:** {start_time} to {end_time}")
        print(f"- **Execution Time:** {(end_time_process - start_time_process) * 1000:.0f} ms")
        print(f"- **Most Placed Color:** {most_common_color} (Count: {color_count})")
        print(f"- **Most Placed Pixel Location:** {most_common_coord} (Count: {coord_count})")
    else:
        print("No data found for the specified timeframe.")

# Example Usage
print_most_common("2022_place_canvas_history.csv", "2022-04-04 01:00:00", "2022-04-04 02:00:00")
print_most_common("2022_place_canvas_history.csv", "2022-04-02 03:00:00", "2022-04-06 06:00:00")
print_most_common("2022_place_canvas_history.csv", "2022-04-03 11:00:00", "2022-04-03 17:00:00")
