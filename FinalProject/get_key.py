import pandas as pd

# Load the mapping from the parquet file.
df = pd.read_parquet('card_id_mapping.parquet')

def get_card_id(card_key: str):
    # Filter the DataFrame for the given card_key.
    result = df.loc[df['Card_Key'] == card_key, 'Card_ID']
    if result.empty:
        return None  # or raise an error, or return a default value
    else:
        return result.iloc[0]

# Example usage:
card_key = 'archers'
card_id = get_card_id(card_key)
if card_id is not None:
    print(f"Card ID for '{card_key}' is {card_id}.")
else:
    print(f"Card Key '{card_key}' not found.")