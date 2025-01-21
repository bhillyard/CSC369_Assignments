import duckdb
import time
from datetime import datetime

# Load the CSV with headers
df = duckdb.read_csv(
    "2022_place_canvas_history.csv",
    header=True,
    dtype=["TIMESTAMP", "VARCHAR", "VARCHAR", "VARCHAR"]
)

def print_most_common(start_time, end_time):
    # Start processing time
    start_time_process = time.time()
    
    # Query for most common pixel color
    query_color = f"""
        SELECT 
            pixel_color,
            COUNT(*) AS count
        FROM df
        WHERE timestamp BETWEEN TIMESTAMP '{start_time}' AND TIMESTAMP '{end_time}'
        GROUP BY pixel_color
        ORDER BY count DESC
        LIMIT 1;
    """
    most_common_color_result = duckdb.execute(query_color).fetchall()
    most_common_color, color_count = most_common_color_result[0]

    # Query for most common coordinate
    query_coord = f"""
        SELECT 
            coordinate,
            COUNT(*) AS count
        FROM df
        WHERE timestamp BETWEEN TIMESTAMP '{start_time}' AND TIMESTAMP '{end_time}'
        GROUP BY coordinate
        ORDER BY count DESC
        LIMIT 1;
    """
    most_common_coord_result = duckdb.execute(query_coord).fetchall()
    most_common_coord, coord_count = most_common_coord_result[0]
    
    # End processing time
    end_time_process = time.time()
    
    # Calculate timeframe duration
    start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    timeframe_hours = (end_dt - start_dt).total_seconds() / 3600

    # Print the results
    print(f"## {timeframe_hours:.0f}-Hour Timeframe")
    print(f"- **Timeframe:** {start_time} to {end_time}")
    print(f"- **Execution Time:** {(end_time_process - start_time_process) * 1000:.0f} ms")
    print(f"- **Most Placed Color:** {most_common_color} (Count: {color_count})")
    print(f"- **Most Placed Pixel Location:** {most_common_coord} (Count: {coord_count})")
    print("- **Most Placed Pixel Location:** No data")

print_most_common("2022-04-04 01:00:00", "2022-04-04 02:00:00")
print_most_common("2022-04-03 11:00:00", "2022-04-03 14:00:00")
print_most_common("2022-04-02 11:00:00", "2022-04-02 17:00:00")
