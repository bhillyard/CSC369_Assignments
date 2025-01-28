import duckdb
import time
import os

file_path = "final_canvas.parquet"

# Get the file size
file_size_bytes = os.path.getsize(file_path)
file_size_gb = file_size_bytes / (1024**3)

start_time = time.time()

con = duckdb.connect()

# Set timeframe
start_timestamp = "2022-04-01 12:00:00"
end_timestamp = "2022-04-01 18:00:00"

# Query 1: Rank Colors by Distinct Users
print("Running Query 1: Rank Colors by Distinct Users...")
rank_colors_query = f"""
SELECT 
    ClosestColorName,
    COUNT(DISTINCT user_id) AS distinct_users
FROM 
    '{file_path}'
WHERE 
    timestamp >= '{start_timestamp}' AND timestamp <= '{end_timestamp}'
GROUP BY 
    ClosestColorName
ORDER BY 
    distinct_users DESC;
"""
rank_colors_result = con.execute(rank_colors_query).fetch_df()
print("Query 1 Completed!\n")

# Query 2: Pixel Placement Percentiles
print("Running Query 2: Pixel Placement Percentiles...")
pixel_placement_query = f"""
SELECT 
    PERCENTILE_CONT(ARRAY[0.5, 0.75, 0.9, 0.99]) 
    WITHIN GROUP (ORDER BY pixel_count) AS percentiles
FROM (
    SELECT 
        user_id, 
        COUNT(*) AS pixel_count
    FROM 
        '{file_path}'
    WHERE 
        timestamp >= '{start_timestamp}' AND timestamp <= '{end_timestamp}'
    GROUP BY 
        user_id
);
"""
pixel_percentiles_result = con.execute(pixel_placement_query).fetch_df()
print("Query 2 Completed!\n")

# Query 3: Count First-Time Users
print("Running Query 3: Count First-Time Users...")
first_time_users_query = f"""
WITH first_pixel AS (
    SELECT 
        user_id, 
        MIN(timestamp) AS first_pixel_time
    FROM 
        '{file_path}'
    GROUP BY 
        user_id
)
SELECT 
    COUNT(*) AS first_time_users
FROM 
    first_pixel
WHERE 
    first_pixel_time >= '{start_timestamp}' AND first_pixel_time <= '{end_timestamp}';
"""
first_time_users_result = con.execute(first_time_users_query).fetch_df()
print("Query 3 Completed!\n")

# Query 4: Calculate Average Session Length
print("Running Query 4: Calculate Average Session Length...")
average_session_length_query = f"""
WITH user_activity AS (
    SELECT 
        user_id,
        timestamp,
        LEAD(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) AS next_timestamp
    FROM 
        '{file_path}'
    WHERE 
        timestamp >= '{start_timestamp}' AND timestamp <= '{end_timestamp}'
),
session_lengths AS (
    SELECT 
        user_id,
        EXTRACT(EPOCH FROM (next_timestamp - timestamp)) AS time_diff
    FROM 
        user_activity
    WHERE 
        next_timestamp IS NOT NULL
),
sessions AS (
    SELECT 
        user_id,
        SUM(time_diff) AS total_session_length
    FROM 
        session_lengths
    WHERE 
        time_diff <= 900  -- Only include gaps within 15 minutes
    GROUP BY 
        user_id
),
valid_sessions AS (
    SELECT 
        total_session_length
    FROM 
        sessions
    WHERE 
        total_session_length > 0  -- Only include users with more than one placement
)
SELECT 
    AVG(total_session_length) AS average_session_length
FROM 
    valid_sessions;
"""
average_session_length_result = con.execute(average_session_length_query).fetch_df()
print("Query 4 Completed!\n")


# End timing
end_time = time.time()

# Print results including average session length
def print_results_with_sessions(file_size_gb, rank_colors, percentiles, first_time_users, average_session_length, execution_time):
    print("# Week 3 Results\n")
    print(f"## Size of pre-processed results\n{file_size_gb:.2f} GB\n")
    print(f"**Timeframe:** {start_timestamp} to {end_timestamp}\n")
    
    print("### Ranking of Colors by Distinct Users")
    print("- **Top Colors**")
    for i, row in rank_colors.iterrows():
        print(f"  {i + 1}. {row['ClosestColorName']}: {row['distinct_users']} users")
    
    print("\n### Percentiles of Pixels Placed")
    print("- **Output:**")
    percentiles_values = percentiles.iloc[0]['percentiles']
    print(f"  - 50th Percentile: {percentiles_values[0]} pixels")
    print(f"  - 75th Percentile: {percentiles_values[1]} pixels")
    print(f"  - 90th Percentile: {percentiles_values[2]} pixels")
    print(f"  - 99th Percentile: {percentiles_values[3]} pixels")
    
    print("\n### Count of First-Time Users")
    print(f"- **Output:** {first_time_users.iloc[0]['first_time_users']} users\n")
    
    print("### Average Session Length")
    avg_session_length = average_session_length.iloc[0]['average_session_length']
    print(f"- **Output:** {avg_session_length:.2f} seconds\n")
    
    print("### Runtime")
    print(f"{execution_time:.2f} seconds")

# Print all results
print_results_with_sessions(
    file_size_gb, 
    rank_colors_result, 
    pixel_percentiles_result, 
    first_time_users_result, 
    average_session_length_result, 
    end_time - start_time
)