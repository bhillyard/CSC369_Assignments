import duckdb

# File paths
input_parquet_file = "2022_place_canvas_with_color_names.parquet" 
user_mapping_file = "user_mapping.parquet"                        
output_file_prefix = "chunk_"                                     
final_output_file = "2022_place_canvas_history_preprocessed.parquet"    

# Connect to DuckDB
con = duckdb.connect()

# Use multiple threads
con.execute("PRAGMA threads = 8;") 

# Load the user mapping
print("Step 1: Loading user mapping...")
con.execute(f"CREATE TABLE user_mapping AS SELECT * FROM read_parquet('{user_mapping_file}');")
print("User mapping loaded successfully.")

# Get the total number of rows in the input dataset
print("Step 2: Getting total row count...")
total_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{input_parquet_file}')").fetchone()[0]
print(f"Total rows in the dataset: {total_rows}")

# Process the dataset in chunks
chunk_size = 5_000_000  # Number of rows per chunk
for start_offset in range(0, total_rows, chunk_size):
    output_chunk_file = f"{output_file_prefix}{start_offset}.parquet"
    print(f"Processing chunk starting at offset {start_offset}...")

    con.execute(f"""
        COPY (
            SELECT p.timestamp,
                   m.numeric_id AS user_id,
                   p.ClosestColorName
            FROM (
                SELECT timestamp, user_id, ClosestColorName
                FROM read_parquet('{input_parquet_file}')
                LIMIT {chunk_size} OFFSET {start_offset}
            ) p
            LEFT JOIN user_mapping m
            ON p.user_id = m.user_id
        ) TO '{output_chunk_file}' (FORMAT PARQUET);  -- Save without compression
    """)
    print(f"Chunk saved to {output_chunk_file} without compression.")

# Combine all chunks into a single Parquet file with compression
print("Combining chunks into final output with compression...")
con.execute(f"""
    COPY (
        SELECT * 
        FROM parquet_scan('{output_file_prefix}*.parquet')
    )
    TO '{final_output_file}' (FORMAT PARQUET, COMPRESSION ZSTD);  -- Apply compression here
""")
print(f"Final optimized dataset saved to {final_output_file} with compression.")
