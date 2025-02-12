import polars as pl
from PIL import Image

# Define file paths
input_file = "place_canvas_with_xy.parquet"
output_image = "suspicious_users_pixels.png"

# Load data using Polars for faster processing
df = pl.read_parquet(input_file)

# Identify Suspicious Users

# High Frequency of Pixel Placements (Fixed Intervals)
def detect_fixed_intervals(df):
    df = df.sort(["numeric_id", "timestamp"])
    df = df.with_columns(
        (pl.col("timestamp") - pl.col("timestamp").shift(1)).alias("time_diff")
    )

    user_intervals = df.group_by("numeric_id").agg([
        pl.col("time_diff").value_counts().alias("interval_counts"),
        pl.len().alias("total_placements")
    ])

    # Flag users with over 70% identical time intervals
    suspicious_fixed = []
    for row in user_intervals.iter_rows():
        interval_counts = dict(row[1]) if isinstance(row[1], dict) else {}
        if interval_counts:
            most_common_count = max(interval_counts.values())
            if most_common_count / int(row[2]) >= 0.7:
                suspicious_fixed.append(row[0])

    return suspicious_fixed

# Short Reaction Time to Color Changes
def detect_reaction_to_color_change(df):
    df = df.sort(["x", "y", "timestamp"])
    df = df.with_columns([
        (pl.col("pixel_color") != pl.col("pixel_color").shift(1)).alias("color_change"),
        (pl.col("timestamp") - pl.col("timestamp").shift(1)).alias("reaction_time")
    ])

    # Identify users who place pixels within 3 seconds of a color change
    fast_reactors = (
        df.filter((pl.col("color_change") == True) & (pl.col("reaction_time") <= 3000))
          .select("numeric_id")
          .unique()
          .to_series()
          .to_list()
    )

    return fast_reactors

# Identify suspicious users
suspicious_fixed = detect_fixed_intervals(df)
suspicious_reactors = detect_reaction_to_color_change(df)
suspicious_users = list(set(suspicious_fixed + suspicious_reactors))

print(f"Suspicious users detected (fixed intervals): {len(suspicious_fixed)}")
print(f"Suspicious users detected (fast reactions): {len(suspicious_reactors)}")
print(f"Total unique suspicious users: {len(suspicious_users)}")

if not suspicious_users:
    print("No suspicious users found. Adjust the filtering criteria.")
    exit()

# Retrieve All Pixels Placed by Suspicious Users
user_pixels = df.filter(pl.col("numeric_id").is_in(suspicious_users))
print(f"Number of pixels retrieved: {len(user_pixels)}")

# Visualize Using PIL
canvas_size = (2000, 2000)
img = Image.new("RGB", canvas_size, (255, 255, 255))
pixels = img.load()

# Color palette (indexed RGB)
indexed_rgb = [
    (0, 0, 0), (0, 117, 111), (0, 158, 170), (0, 163, 104),
    (0, 204, 120), (0, 204, 192), (36, 80, 164), (54, 144, 234),
    (73, 58, 193), (81, 82, 82), (81, 233, 244), (106, 92, 255),
    (109, 0, 26), (109, 72, 47), (126, 237, 86), (129, 30, 159),
    (137, 141, 144), (148, 179, 255), (156, 105, 38), (180, 74, 192),
    (190, 0, 57), (212, 215, 217), (222, 16, 127), (228, 171, 255),
    (255, 56, 129), (255, 69, 0), (255, 153, 170), (255, 168, 0),
    (255, 180, 112), (255, 214, 53), (255, 248, 184), (255, 255, 255)
]

# Plot pixels
for row in user_pixels.iter_rows():
    x, y, pixel_color = row[2], row[3], row[4]
    if 0 <= x < 2000 and 0 <= y < 2000:
        pixels[x, y] = indexed_rgb[pixel_color]

# Save the image
img = img.resize((4000, 4000), Image.NEAREST)
img.save(output_image)
print(f"Suspicious user pixels visualized and saved as: {output_image}")
