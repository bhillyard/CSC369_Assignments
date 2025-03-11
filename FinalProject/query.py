import pyarrow.parquet as pq
import pandas as pd
from collections import Counter

# Path to Parquet file
parquet_path = "working/clash_royale_all_data_corrected3.parquet"

# Open Parquet file
parquet_file = pq.ParquetFile(parquet_path)

# Initialize counter for card occurrences
card_usage = Counter()

# Read the file in chunks (by row groups)
for row_group in range(parquet_file.num_row_groups):
    print(f"Processing row group {row_group + 1}/{parquet_file.num_row_groups}...")
    
    # Read row group as DataFrame
    df = parquet_file.read_row_group(row_group).to_pandas()

    # Flatten team_cards and opponent_cards lists
    team_cards = df["team_cards"].explode().dropna().astype(int)
    opponent_cards = df["opponent_cards"].explode().dropna().astype(int)

    # Update card usage count
    card_usage.update(team_cards)
    card_usage.update(opponent_cards)

# Convert counts to DataFrame
card_usage_df = pd.DataFrame(card_usage.items(), columns=["Card_ID", "Usage_Count"])

# Calculate total card occurrences
total_card_uses = sum(card_usage.values())

# Calculate usage percentage
card_usage_df["Usage_Percentage"] = (card_usage_df["Usage_Count"] / total_card_uses) * 100

# Sort by most used cards
card_usage_df = card_usage_df.sort_values(by="Usage_Percentage", ascending=False)

# Save to Parquet file
output_path = "card_usage_rates.parquet"
card_usage_df.to_parquet(output_path, index=False)

print(f"Card usage data saved to {output_path}")
