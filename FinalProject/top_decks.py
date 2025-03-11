import duckdb

# Start and End Dates
start_date = '2023-07-06'
end_date = '2023-08-06'

# Path to the Parquet file
parquet_path = "working/clash_royale_all_data_corrected3.parquet"

# SQL Query to process decks separately
query = f"""
WITH deck_combinations AS (
    SELECT 
        info_datetime,
        list_sort(cast(team_cards AS INTEGER[])) AS sorted_deck
    FROM read_parquet('{parquet_path}')
    WHERE info_datetime BETWEEN '{start_date}' AND '{end_date}'
    
    UNION ALL
    
    SELECT 
        info_datetime,
        list_sort(cast(opponent_cards AS INTEGER[])) AS sorted_deck
    FROM read_parquet('{parquet_path}')
    WHERE info_datetime BETWEEN '{start_date}' AND '{end_date}'
)
SELECT 
    array_to_string(sorted_deck, ',') AS sorted_deck, 
    COUNT(*) AS frequency
FROM deck_combinations
GROUP BY sorted_deck
ORDER BY frequency DESC
LIMIT 10;
"""

# Execute the query
df_top_decks = duckdb.query(query).to_df()

# Display the top 10 most common decks
print(f"top decks from period {start_date} to {end_date}")
print(df_top_decks)
