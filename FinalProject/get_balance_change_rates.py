import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# Define balance change date
balance_change = "12-7-22"

# Read in parquet files
pre_balance_change = "balance_change_usage/pre_" + balance_change + "_usage_rate.parquet"
post_balance_change = "balance_change_usage/post_" + balance_change + "_usage_rate.parquet"

selected_columns = ["Card_Key", "Adjusted_Usage_Percentage"]

df_pre = pd.read_parquet(pre_balance_change, columns=selected_columns)
df_post = pd.read_parquet(post_balance_change, columns=selected_columns)

# Merge DataFrames to Ensure Correct Order
df_merged = df_pre.merge(df_post, on="Card_Key", suffixes=("_pre", "_post"))

# Manually set buffed, nerfed, and reworked cards
nerfed_cards = ["phoenix", "monk"]
buffed_cards = ["minions", "minion-horde", "mighty-miner"]
reworked_cards = ["goblin-hut"]

# Assign categories
def categorize(card):
    if card in nerfed_cards:
        return "nerfed"
    elif card in buffed_cards:
        return "buffed"
    elif card in reworked_cards:
        return "reworked"
    else:
        return "none"

df_merged["Change"] = df_merged["Card_Key"].apply(categorize)

# Filter out cards with "none" changes
df_filtered = df_merged[df_merged["Change"] != "none"]

# Select and rename columns for saving
df_filtered = df_filtered[["Card_Key", "Change", "Adjusted_Usage_Percentage_pre", "Adjusted_Usage_Percentage_post"]]
df_filtered.rename(columns={"Adjusted_Usage_Percentage_pre": "Before_Usage", "Adjusted_Usage_Percentage_post": "After_Usage"}, inplace=True)

# Save to Parquet file
output_parquet = f"balance_change_rates/balance_change_summary_{balance_change}.parquet"
df_filtered.to_parquet(output_parquet, index=False)
print(f"Summary data saved to {output_parquet}")