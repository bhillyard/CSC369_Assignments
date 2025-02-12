import duckdb
from PIL import Image

# Define file paths
input_file = "place_canvas_with_xy.parquet"
output_image = "last_pixels_before_whiteout.png"

# Connect to DuckDB
con = duckdb.connect()

# Coordinates defining the rectangular regions
areas = [
    (716, 733, 1830, 1862),  # Area 1
    (642, 659, 1768, 1800)    # Area 2
]

# Whiteout timestamp (end of r/Place 2022)
WHITEOUT_TIMESTAMP = 1649112240000  # Adjust this to the exact timestamp
WHITEOUT_TIMESTAMP -= 7200000*4

# Color palette (RGB values)
valid_rgb_colors = [
    (0, 0, 0), (0, 117, 111), (0, 158, 170), (0, 163, 104),
    (0, 204, 120), (126, 237, 86), (255, 248, 184), (255, 255, 255)
]

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

# Identify valid color indices
valid_color_indices = [i for i, color in enumerate(indexed_rgb) if color in valid_rgb_colors]

# Query to get users who placed at least one pixel in the specified areas
area_conditions = " OR ".join([
    f"(x BETWEEN {x_min} AND {x_max} AND y BETWEEN {y_min} AND {y_max})"
    for (x_min, x_max, y_min, y_max) in areas
])

user_query = f"""
    WITH filtered_pixels AS (
        SELECT DISTINCT numeric_id
        FROM '{input_file}'
        WHERE ({area_conditions})
          AND pixel_color IN ({', '.join(map(str, valid_color_indices))})
    )
    SELECT numeric_id
    FROM filtered_pixels
"""

# Execute query to get user IDs
user_ids = con.execute(user_query).fetchdf()["numeric_id"].tolist()

# Query to get the last placed pixel before the whiteout for these users
user_pixels_query = f"""
    WITH user_pixels AS (
        SELECT x, y, pixel_color, timestamp,
               ROW_NUMBER() OVER (PARTITION BY x, y ORDER BY timestamp DESC) AS rnk
        FROM '{input_file}'
        WHERE numeric_id IN ({', '.join(map(str, user_ids))})
          AND timestamp < {WHITEOUT_TIMESTAMP}
    )
    SELECT x, y, pixel_color
    FROM user_pixels
    WHERE rnk = 1
"""

# Execute query to get the last placed pixels
last_pixels = con.execute(user_pixels_query).fetchdf()

# Display the number of pixels retrieved
print(f"Number of last placed pixels before whiteout: {len(last_pixels)}")

# Step 4: Visualize Using PIL
canvas_size = (2000, 2000)  # Assuming canvas size is 2000x2000
img = Image.new("RGB", canvas_size, (255, 255, 255))  # White background
pixels = img.load()

# Plot the last placed pixels
for _, row in last_pixels.iterrows():
    x, y, pixel_color = row["x"], row["y"], row["pixel_color"]
    if 0 <= x < 2000 and 0 <= y < 2000:  # Ensure coordinates are valid
        pixels[x, y] = indexed_rgb[pixel_color]

# Save the image
img = img.resize((4000, 4000), Image.NEAREST)  # 2x zoom for better visibility
img.save(output_image)
print(f"Last placed pixels before whiteout visualized and saved as: {output_image}")
