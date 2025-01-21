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
    
    # Query using variable time frame
    query = f"""
        SELECT 
            pixel_color,
            coordinate,
            COUNT(*) AS count
        FROM df
        WHERE timestamp BETWEEN TIMESTAMP '{start_time}' AND TIMESTAMP '{end_time}'
        GROUP BY pixel_color, coordinate
        ORDER BY count DESC
        LIMIT 1;
    """
    result = duckdb.execute(query).fetchall()
    
    # End processing time
    end_time_process = time.time()
    
    # Check if result is not empty
    if result:
        # Unpack the result
        most_common_color, most_common_coord, count = result[0]
        
        # Calculate timeframe duration
        start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        timeframe_hours = (end_dt - start_dt).total_seconds() / 3600

        # Print the results
        print(f"## {timeframe_hours:.0f}-Hour Timeframe")
        print(f"- **Timeframe:** {start_time} to {end_time}")
        print(f"- **Execution Time:** {(end_time_process - start_time_process) * 1000:.0f} ms")
        print(f"- **Most Placed Color:** {most_common_color}")
        print(f"- **Most Placed Pixel Location:** {most_common_coord}")
        print(f"- **Placement Count:** {count}")
    else:
        print("No data found for the specified timeframe.")

print_most_common("2022-04-04 01:00:00", "2022-04-04 02:00:00")
print_most_common("2022-04-02 03:00:00", "2022-04-06 06:00:00")
print_most_common("2022-04-03 11:00:00", "2022-04-04 17:00:00")