import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Load the daily card usage data
daily_usage_parquet = "daily_card_usage_rates.parquet"
df = pd.read_parquet(daily_usage_parquet)

# Convert index to datetime
df["index"] = pd.to_datetime(df["index"])

# Define the balance change date
balance_change_date = pd.to_datetime("2022-12-07")

# Define time range (2 months before & after)
start_date = balance_change_date - pd.DateOffset(months=1)
end_date = balance_change_date + pd.DateOffset(months=2)

# Filter data within the time range
df_filtered = df[(df["index"] >= start_date) & (df["index"] <= end_date)]

# List of selected cards to track
selected_cards = ["monk",
"phoenix",
"minion-horde",
"minions",
"mighty-miner",
"goblin-hut"
]

# Filter for selected cards
df_filtered = df_filtered[df_filtered["Card_Key"].isin(selected_cards)]

# Pivot DataFrame for time series plot
df_pivot = df_filtered.pivot(index="index", columns="Card_Key", values="Usage_Percentage")

# Plot the time series graph
plt.figure(figsize=(12, 6))
for card in selected_cards:
    plt.plot(df_pivot.index, df_pivot[card], label=card)

# Add balance change vertical line
plt.axvline(balance_change_date, color="black", linestyle="--", linewidth=1, label="Balance Change")

# Format x-axis labels as MM-DD-YYYY
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%m-%d-%Y"))
plt.xticks(rotation=45)  # Rotate for better readability

# Labels and formatting
plt.xlabel("Date")
plt.ylabel("Usage Rate (%)")
plt.title(f"Card Usage Rates: 1 Month Before & 2 Months After {balance_change_date.strftime('%m-%d-%Y')} Balance Change")
plt.legend()
plt.grid(True, linestyle="--", alpha=0.6)

# Save the figure
output_filename = f"time_series_usage_{balance_change_date.date()}.png"
plt.savefig(output_filename, dpi=300, bbox_inches="tight")
print(f"📊 Time series graph saved as {output_filename}")

# Show the graph
plt.show()
