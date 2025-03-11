import pandas as pd
import pyarrow.parquet as pq

# Paths to the Parquet files
input_parquet = "adjusted_card_usage_2023-08-09_to_2023-10-02.parquet"  # Update with actual file path
mapping_parquet = "card_id_mapping.parquet"  # Parquet file containing Card_ID → Card_Key mapping
balance_change = "10-3-23_"
before_after = "post_"

# List of card keys to search for
selected_card_keys = [
    "skeletons",
    "ice-golem",
    "goblin-cage",
    "giant",
    "knight",
    "barbarians",
    "bomb-tower",
    "magic-archer",
    "lava-hound",
    "giant-snowbll"
]

# Load the Parquet files
df_usage = pq.read_table(input_parquet).to_pandas()  # Card usage data
df_mapping = pq.read_table(mapping_parquet).to_pandas()  # Card ID to Card Key mapping

# Ensure required columns exist
if "Card_ID" not in df_usage.columns:
    raise ValueError("Error: Parquet file must contain 'Card_ID' column.")
if "Card_ID" not in df_mapping.columns or "Card_Key" not in df_mapping.columns:
    raise ValueError("Error: Mapping file must contain 'Card_ID' and 'Card_Key' columns.")

# Merge the usage data with the mapping file on Card_ID
df_merged = df_usage.merge(df_mapping, on="Card_ID", how="left")

# Filter the DataFrame to only include the selected card keys
filtered_df = df_merged[df_merged["Card_Key"].isin(selected_card_keys)][["Card_ID", "Card_Key", "Usage_Count", "Adjusted_Usage_Percentage"]]

# Display the results
print("Filtered Card Usage Data:")
print(filtered_df)

# Save to Parquet file
output_parquet = "balance_change_usage/"+before_after+balance_change+"usage_rate.parquet"
filtered_df.to_parquet(output_parquet, index=False)
print(f"Filtered card usage data saved to {output_parquet}")
