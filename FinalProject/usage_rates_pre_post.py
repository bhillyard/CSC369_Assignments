import pyarrow.parquet as pq
import pandas as pd
from collections import Counter

# Path to Parquet file
parquet_path = "working/clash_royale_all_data_corrected3.parquet"

# Define the two periods to process
periods = [
    ("2022-09-05", "2022-10-04"),  # First period before balance changes
    ("2023-10-03", "2023-12-04")   # Last period after balance changes
]

# Convert to Pandas datetime with UTC timezone
periods = [(pd.to_datetime(start).tz_localize("UTC"), pd.to_datetime(end).tz_localize("UTC")) for start, end in periods]

# Open Parquet file
parquet_file = pq.ParquetFile(parquet_path)

# Iterate through the two specific periods
for period_start, period_end in periods:
    print(f"Processing period: {period_start.date()} to {period_end.date()}...")

    # Initialize counter for card occurrences in this period
    card_usage = Counter()
    stop_processing = False  # Flag to stop reading row groups early

    # Read the file in chunks (by row groups)
    for row_group in range(parquet_file.num_row_groups):
        if stop_processing:
            break  # Stop early if all needed row groups are processed

        print(f"Processing row group {row_group + 1}/{parquet_file.num_row_groups}...")

        # Read row group as DataFrame
        df = parquet_file.read_row_group(row_group).to_pandas()

        # Convert `info_datetime` to Pandas datetime and ensure UTC timezone
        df["info_datetime"] = pd.to_datetime(df["info_datetime"], utc=True)

        # Filter data within the current period
        df_filtered = df[(df["info_datetime"] >= period_start) & (df["info_datetime"] <= period_end)]

        if df_filtered.empty:
            print(f"No matches found for this row group, skipping...")
            continue

        # Flatten team_cards and opponent_cards lists
        team_cards = df_filtered["team_cards"].explode().dropna().astype(int)
        opponent_cards = df_filtered["opponent_cards"].explode().dropna().astype(int)

        # Update card usage count
        card_usage.update(team_cards)
        card_usage.update(opponent_cards)

    # Convert counts to DataFrame
    card_usage_df = pd.DataFrame(card_usage.items(), columns=["Card_ID", "Usage_Count"])

    # Calculate total card occurrences
    total_card_uses = sum(card_usage.values())

    # Calculate usage percentage
    if total_card_uses > 0:
        card_usage_df["Usage_Percentage"] = (card_usage_df["Usage_Count"] / total_card_uses) * 100
    else:
        card_usage_df["Usage_Percentage"] = 0

    # Sort by most used cards
    card_usage_df = card_usage_df.sort_values(by="Usage_Percentage", ascending=False)

    # Save to Parquet file
    output_path = f"card_usage_{period_start.date()}_to_{period_end.date()}.parquet"
    card_usage_df.to_parquet(output_path, index=False)

    print(f"Finished processing {period_start.date()} to {period_end.date()}, saved to: {output_path}\n")

print("Processing for first and last periods completed")
