import pandas as pd
import time
from datetime import datetime
from collections import Counter
import pytz  # For timezone handling

def print_most_common(start_time, end_time, chunksize=1_000_000):
    file_path = "2022_place_canvas_history.csv"
    start_time_process = time.time()

    # Add timezones to input so that it compares well with given csv file
    start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
    end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)

    # Make counters to count the pixel_colors and coords
    pixel_color_counts = Counter()
    coordinate_counts = Counter()

    # Process the file in chunks
    for chunk in pd.read_csv(
        file_path,
        parse_dates=["timestamp"],
        usecols=["timestamp", "pixel_color", "coordinate"],
        chunksize=chunksize
    ):
        # Filter rows for the given time range
        filtered_chunk = chunk[(chunk["timestamp"] >= start_dt) & (chunk["timestamp"] <= end_dt)]

        # Update the counters
        pixel_color_counts.update(filtered_chunk["pixel_color"])
        coordinate_counts.update(filtered_chunk["coordinate"])

    # Find the most common pixel color and coordinate
    if pixel_color_counts and coordinate_counts:
        most_common_color, color_count = pixel_color_counts.most_common(1)[0]
        most_common_coord, coord_count = coordinate_counts.most_common(1)[0]

        # Calculate timeframe duration
        timeframe_hours = (end_dt - start_dt).total_seconds() / 3600

        # Print the results
        print(f"## {timeframe_hours:.0f}-Hour Timeframe")
        print(f"- **Timeframe:** {start_time} to {end_time}")
        print(f"- **Execution Time:** {(time.time() - start_time_process) * 1000:.0f} ms")
        print(f"- **Most Placed Color:** {most_common_color} (Count: {color_count})")
        print(f"- **Most Placed Pixel Location:** {most_common_coord} (Count: {coord_count})")
    else:
        print("No data found for the specified timeframe.")

# Call the function with the file path and time range
print_most_common("2022-04-04 01:00:00", "2022-04-04 02:00:00")
print_most_common("2022-04-02 03:00:00", "2022-04-02 06:00:00")
print_most_common("2022-04-03 11:00:00", "2022-04-03 17:00:00")
