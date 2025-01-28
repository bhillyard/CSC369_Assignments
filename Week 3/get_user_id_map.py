import polars as pl

# File paths
input_parquet_file = "2022_place_canvas_with_color_names.parquet"  # Input dataset
user_mapping_file = "user_mapping.parquet"  # Output mapping file

# Load the dataset
print("Loading the dataset...")
df = pl.read_parquet(input_parquet_file)

# Create unique `user_id` mappings
print("Generating unique user_id mappings...")
user_mapping = (
    df.select("user_id")
    .unique()
    .with_row_count("numeric_id")  # Assign a unique numeric ID starting from 0
)

# Save the mapping to a Parquet file
print("Saving user mapping to a Parquet file...")
user_mapping.write_parquet(user_mapping_file, compression="zstd")
print(f"User mapping saved to {user_mapping_file}")