import csv
from datetime import datetime
from collections import Counter
import time

# Parse the r/place data within the time window
def parse_place_data(start_time, end_time, file_path):
    color_counter = Counter()
    coords_counter = Counter()

    start_time_process = time.time()
    with open(file_path, mode='r', newline='') as file:
        csv_reader = csv.reader(file)
        # Skip header row
        next(csv_reader) 
        for row in csv_reader:
            # Truncate at the hour
            timestamp = datetime.strptime(row[0][:13], "%Y-%m-%d %H")
            if timestamp < start_time:
                continue
            if timestamp > end_time:
                break
            color = row[2]
            color_counter[color] += 1
            coords = tuple(row[3].split(","))
            coords_counter[coords] += 1

    # Calculate most common color and coordinate
    most_common_color = max(color_counter, key=color_counter.get)
    most_common_coord = max(coords_counter, key=coords_counter.get)

    end_time_process = time.time()

    timeframe_hours = (end_time - start_time).total_seconds() / 3600
    print(f"## {timeframe_hours:.0f}-Hour Timeframe")
    print(f"- **Timeframe:** {start_time.strftime('%Y-%m-%d %H')} to {end_time.strftime('%Y-%m-%d %H')}")
    print(f"- **Execution Time:** {(end_time_process - start_time_process) * 1000:.0f} ms")
    print(f"- **Most Placed Color:** {most_common_color}")
    print(f"- **Most Placed Pixel Location:** {most_common_coord}")

# Main code

file_path = '/Users/bretthillyard/Desktop/CSC369/CSC369_Assignments/Assignment1/2022_place_canvas_history.csv'

# Convert start and end times to datetime objects
start_time = datetime.strptime("2022-04-04 01", '%Y-%m-%d %H')
end_time = datetime.strptime("2022-04-04 02", '%Y-%m-%d %H')

# Measure for 1 hour time frame
parse_place_data(start_time, end_time, file_path)

# Convert start and end times to datetime objects
start_time = datetime.strptime("2022-04-04 01", '%Y-%m-%d %H')
end_time = datetime.strptime("2022-04-04 04", '%Y-%m-%d %H')

# Measure for 3 hour time frame
parse_place_data(start_time, end_time, file_path)

# Convert start and end times to datetime objects
start_time = datetime.strptime("2022-04-04 01", '%Y-%m-%d %H')
end_time = datetime.strptime("2022-04-04 07", '%Y-%m-%d %H')

# Measure for 6 hour time frame
parse_place_data(start_time, end_time, file_path)