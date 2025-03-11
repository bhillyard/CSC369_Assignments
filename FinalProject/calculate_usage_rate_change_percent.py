import pandas as pd
import os

# Define balance change dates and file structure
balance_changes = ["12-7-22", "2-7-23", "4-4-23", "6-6-23", "8-8-23", "10-3-23"]

base_folder = "balance_change_rates"

# Lists to store changes
buffed_absolute_changes = []
buffed_relative_changes = []
nerfed_absolute_changes = []
nerfed_relative_changes = []

# Iterate through balance change files
for date in balance_changes:
    file_path = f"{base_folder}/balance_change_summary_{date}.parquet"

    # Load the Parquet file
    df = pd.read_parquet(file_path)

    # Compute absolute and relative changes
    df["Absolute_Change"] = abs(df["After_Usage"] - df["Before_Usage"])
    df["Relative_Change"] = abs((df["After_Usage"] - df["Before_Usage"]) / df["Before_Usage"]) * 100

    # Separate buffed and nerfed cards
    buffed_df = df[df["Change"] == "buffed"]
    nerfed_df = df[df["Change"] == "nerfed"]

    # Collect values
    buffed_absolute_changes.extend(buffed_df["Absolute_Change"].tolist())
    buffed_relative_changes.extend(buffed_df["Relative_Change"].tolist())
    nerfed_absolute_changes.extend(nerfed_df["Absolute_Change"].tolist())
    nerfed_relative_changes.extend(nerfed_df["Relative_Change"].tolist())

# Compute final averages
buffed_absolute_avg = sum(buffed_absolute_changes) / len(buffed_absolute_changes) if buffed_absolute_changes else 0
buffed_relative_avg = sum(buffed_relative_changes) / len(buffed_relative_changes) if buffed_relative_changes else 0
nerfed_absolute_avg = sum(nerfed_absolute_changes) / len(nerfed_absolute_changes) if nerfed_absolute_changes else 0
nerfed_relative_avg = sum(nerfed_relative_changes) / len(nerfed_relative_changes) if nerfed_relative_changes else 0

# Output results
print("Final Summary of Usage Changes:")
print(f"Buffed Cards - Avg Absolute Change: {buffed_absolute_avg:.2f}%")
print(f"Buffed Cards - Avg Relative Change: {buffed_relative_avg:.2f}%")
print(f"Nerfed Cards - Avg Absolute Change: {nerfed_absolute_avg:.2f}%")
print(f"Nerfed Cards - Avg Relative Change: {nerfed_relative_avg:.2f}%")
