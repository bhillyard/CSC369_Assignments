import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

# Define balance change date
balance_change = "10-3-23"

# Read in parquet files
pre_balance_change = "balance_change_usage/pre_" + balance_change + "_usage_rate.parquet"
post_balance_change = "balance_change_usage/post_" + balance_change + "_usage_rate.parquet"

selected_columns = ["Card_Key", "Adjusted_Usage_Percentage"]

df_pre = pd.read_parquet(pre_balance_change, columns=selected_columns)
df_post = pd.read_parquet(post_balance_change, columns=selected_columns)

# Merge DataFrames to Ensure Correct Order
df_merged = df_pre.merge(df_post, on="Card_Key", suffixes=("_pre", "_post"))

# Manually set buffed, nerfed, and reworked cards
nerfed_cards = ["knight", "barbarians", "bomb-tower", "magic-archer", "lava-hound"]
buffed_cards = ["skeletons", "ice-golem", "goblin-cage", "giant"]
reworked_cards = ["giant-snowball"]

# Sort so nerfs come first, then buffs, then reworks
df_merged["Category"] = df_merged["Card_Key"].apply(
    lambda x: 0 if x in nerfed_cards else (1 if x in buffed_cards else 2)
)
df_merged = df_merged.sort_values(by="Category")

# Extract sorted data
categories = df_merged["Card_Key"].tolist()
group1_values = df_merged["Adjusted_Usage_Percentage_pre"].round(2).tolist()  # Before (gray)
group2_values = df_merged["Adjusted_Usage_Percentage_post"].round(2).tolist()  # After (green/red)

# Determine colors based on change (red for nerf, green for buff, blue for rework)
colors = [
    "red" if card in nerfed_cards else "green" if card in buffed_cards else "blue" if card in reworked_cards else "gray"
    for card in categories
]

# Position of the bars on the x-axis
x = np.arange(len(categories))
width = 0.35

# Create figure
fig, ax = plt.subplots(figsize=(10, 6))

# Plot "before" bars in gray
ax.bar(x - width / 2, group1_values, width, color="gray", alpha=0.6, label="Before")

# Plot "after" bars in red (nerf), green (buff), or blue (rework)
ax.bar(x + width / 2, group2_values, width, color=colors, alpha=0.8, label="After")

# Add percentage labels above bars
for i in range(len(categories)):
    ax.text(x[i] - width / 2, group1_values[i] + 1, f"{group1_values[i]}%", ha="center", fontsize=10, color="black")
    ax.text(x[i] + width / 2, group2_values[i] + 1, f"{group2_values[i]}%", ha="center", fontsize=10, color=colors[i], fontweight="bold")

# Custom legend patches
gray_patch = mpatches.Patch(color="gray", alpha=0.6, label="Before")
red_patch = mpatches.Patch(color="red", alpha=0.8, label="After Nerf")
green_patch = mpatches.Patch(color="green", alpha=0.8, label="After Buff")
blue_patch = mpatches.Patch(color="blue", alpha=0.8, label="After Rework")

# Labels and formatting
ax.set_xlabel("Clash Royale Cards")
ax.set_ylabel("Usage Rates (%)")
ax.set_title(f"Clash Royale Card Usage: Before & After {balance_change} Balance Change")
ax.set_xticks(x)
ax.set_xticklabels(categories, rotation=45, ha="right")  # Rotate labels for better readability
ax.legend(handles=[gray_patch, red_patch, green_patch, blue_patch], labels=["Before", "After Nerf", "After Buff", "After Rework"])
plt.grid(axis="y", linestyle="--", alpha=0.5)

# Save the figure as a PNG file
output_filename = f"Clash_Royale_Usage_{balance_change}.png"
plt.savefig(output_filename, dpi=300, bbox_inches="tight")
print(f"Figure saved as {output_filename}")

# Show the chart
plt.tight_layout()
plt.show()
