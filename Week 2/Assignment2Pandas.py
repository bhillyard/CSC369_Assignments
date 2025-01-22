import pandas as pd
import time
from datetime import datetime
from collections import Counter

# Preprocessing and indexing during the CSV load
def load_and_preprocess_csv(file_path):
    print("Loading and preprocessing the CSV...")
    start_time_load = time.time()

    # Specify data types for efficiency
    dtype = {"pixel_color": "category", "coordinate": "category"}

    # Read and preprocess the data
    df = pd.read_csv(
        file_path,
        parse_dates=["timestamp"],
        usecols=["timestamp", "pixel_color", "coordinate"],
        dtype=dtype,
    )
    # Extract the hour for filtering
    df["hour"] = df["timestamp"].dt.strftime("%Y-%m-%d %H")
    # Drop the original timestamp to save memory
    df = df.drop(columns=["timestamp"])

    print(f"Data loaded and preprocessed in {(time.time() - start_time_load) * 1000:.0f} ms")
    return df

# Load the data
file_path = "2022_place_canvas_history.csv"
df = load_and_preprocess_csv(file_path)

# Function to query the preprocessed DataFrame
def print_most_common(start_time, end_time):
    start_time_process = time.time()

    # Filter rows within the timeframe
    filtered_df = df[(df["hour"] >= start_time[:13]) & (df["hour"] <= end_time[:13])]

    # Initialize counters
    pixel_color_counts = Counter(filtered_df["pixel_color"])
    coordinate_counts = Counter(filtered_df["coordinate"])

    # Find the most common pixel color and coordinate
    most_common_color, color_count = pixel_color_counts.most_common(1)[0]
    most_common_coord, coord_count = coordinate_counts.most_common(1)[0]

    # Calculate timeframe duration
    start_dt = datetime.strptime(start_time[:13], "%Y-%m-%d %H")
    end_dt = datetime.strptime(end_time[:13], "%Y-%m-%d %H")
    timeframe_hours = (end_dt - start_dt).total_seconds() / 3600

    # Print results
    print(f"## {timeframe_hours:.0f}-Hour Timeframe")
    print(f"- **Timeframe:** {start_time[:13]} to {end_time[:13]}")
    print(f"- **Execution Time:** {(time.time() - start_time_process) * 1000:.0f} ms")
    print(f"- **Most Placed Color:** {most_common_color}")
    print(f"- **Most Placed Pixel Location:** {most_common_coord}")

# Example Queries
print_most_common("2022-04-04 01", "2022-04-04 02")
print_most_common("2022-04-02 03", "2022-04-02 06")
print_most_common("2022-04-03 11", "2022-04-03 17")
