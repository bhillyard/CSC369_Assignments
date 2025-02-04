import duckdb
from PIL import Image

# Define file path
file_path = "2022_place_canvas_history.parquet"

# Connect to DuckDB and enable multi-threading
con = duckdb.connect()
con.execute("PRAGMA threads=8;")  # Use 8 CPU threads for parallel processing

print("Running DuckDB query to compute the most commonly placed color for every pixel...")

query = f"""
WITH PixelCounts AS (
    SELECT x, y, pixel_color, COUNT(*) AS cnt
    FROM '{file_path}'
    GROUP BY x, y, pixel_color
),
RankedPixels AS (
    SELECT x, y, pixel_color,
           RANK() OVER (PARTITION BY x, y ORDER BY cnt DESC) AS rnk
    FROM PixelCounts
)
SELECT x, y, pixel_color FROM RankedPixels WHERE rnk = 1;
"""
result = con.execute(query).fetchall()

print(f"Query completed! Processing {len(result)} pixels for rendering...")

# Define color palette (index -> RGB)
indexed_rgb = [
    (0, 0, 0), (0, 117, 111), (0, 158, 170), (0, 163, 104), 
    (0, 204, 120), (0, 204, 192), (36, 80, 164), (54, 144, 234),
    (73, 58, 193), (81, 82, 82), (81, 233, 244), (106, 92, 255),
    (109, 0, 26), (109, 72, 47), (126, 237, 86), (129, 30, 159),
    (137, 141, 144), (148, 179, 255), (156, 105, 38), (180, 74, 192),
    (190, 0, 57), (212, 215, 217), (222, 16, 127), (228, 171, 255),
    (255, 56, 129), (255, 69, 0), (255, 153, 170), (255, 168, 0),
    (255, 180, 112), (255, 214, 53), (255, 248, 184), (255, 255, 255),
]

# Set correct canvas size (2000 x 2000)
canvas_width, canvas_height = 2000, 2000
print(f"Canvas size: {canvas_width} x {canvas_height}")

# Create a default white background
default_color = (255, 255, 255)  # White as default
img = Image.new("RGB", (canvas_width, canvas_height), default_color)
pixels = img.load()

# Fill in the most commonly placed color for each coordinate
for x, y, pixel_color in result:
    pixels[x, y] = indexed_rgb[pixel_color]  # Assign most frequent color
    
# Enlarge the image so pixels are more visible
img = img.resize((canvas_width * 2, canvas_height * 2), Image.NEAREST)  # 2x zoom

# Save the final image
img.save("full_canvas_all_time.png")
print("Image saved as full_canvas_all_time.png!")
