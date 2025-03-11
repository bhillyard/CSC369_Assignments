import pandas as pd
import pyarrow.parquet as pq
from collections import Counter

# Path to Parquet file
input_parquet = "working/clash_royale_all_data_corrected3.parquet"  # Update with actual file path
mapping_parquet = "card_id_mapping.parquet"  # Card_ID to Card_Key mapping

# Open dataset
parquet_file = pq.ParquetFile(input_parquet)

# Load mapping file
df_mapping = pq.read_table(mapping_parquet).to_pandas()

# Dictionary to store daily usage counts
daily_usage = {}

# Process row groups (Parquet chunks)
for row_group in range(parquet_file.num_row_groups):
    print(f"Processing row group {row_group + 1}/{parquet_file.num_row_groups}...")

    # Read chunk
    df = parquet_file.read_row_group(row_group).to_pandas()

    # Convert timestamps & extract only date
    df["info_datetime"] = pd.to_datetime(df["info_datetime"], utc=True)
    df["date"] = df["info_datetime"].dt.date

    # Flatten team and opponent cards
    all_cards = pd.concat([df["team_cards"].explode(), df["opponent_cards"].explode()])
    all_cards = all_cards.dropna().astype(int)

    # Count occurrences per day
    daily_counts = all_cards.groupby(df["date"]).value_counts().unstack(fill_value=0)

    # Add up daily usage
    for date, row in daily_counts.iterrows():
        if date not in daily_usage:
            daily_usage[date] = row
        else:
            daily_usage[date] += row

# Convert to DataFrame
df_daily = pd.DataFrame(daily_usage).T
df_daily.index = pd.to_datetime(df_daily.index)

# Reshape DataFrame
df_daily = df_daily.reset_index().melt(id_vars="index", var_name="Card_ID", value_name="Usage_Count")

# Merge with mapping file to get Card Keys
df_daily = df_daily.merge(df_mapping, on="Card_ID", how="left")

# Compute total decks played per day
df_daily["Total_Decks"] = df_daily.groupby("index")["Usage_Count"].transform("sum") / 8
df_daily["Usage_Percentage"] = (df_daily["Usage_Count"] / df_daily["Total_Decks"]) * 100
df_daily = df_daily.drop(columns=["Total_Decks", "Usage_Count"])

# Save to Parquet
output_parquet = "daily_card_usage_rates.parquet"
df_daily.to_parquet(output_parquet, index=False)

print(f"Daily usage data saved to {output_parquet}")
