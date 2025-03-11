import json
import pandas as pd

# Path to the cards.json file
cards_json_path = "clash-royale-cards/cards.json"
output_parquet_path = "card_id_mapping.parquet"

# Load JSON file
with open(cards_json_path, "r", encoding="utf-8") as f:
    cards = json.load(f)

# Extract ID (order in file) and card key
card_data = [{"Card_ID": idx, "Card_Key": card["key"]} for idx, card in enumerate(cards)]

# Convert to DataFrame
card_df = pd.DataFrame(card_data)

# Save to Parquet
card_df.to_parquet(output_parquet_path, index=False)

print(f"Card ID mapping saved to {output_parquet_path}")
