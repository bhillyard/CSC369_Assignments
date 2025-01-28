import duckdb

# File paths
input_csv_file = "2022_place_canvas_with_color_names.csv"
output_parquet_file = "2022_place_canvas_with_color_names.parquet"

# Connect to DuckDB
con = duckdb.connect()

# Convert CSV to Parquet with compression
con.execute(f"""
    COPY (SELECT * FROM read_csv_auto('{input_csv_file}'))
    TO '{output_parquet_file}' (FORMAT 'parquet', COMPRESSION 'zstd');
""")

print(f"File converted and saved to {output_parquet_file} with ZSTD compression.")