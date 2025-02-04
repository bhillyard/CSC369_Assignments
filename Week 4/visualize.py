import duckdb
from PIL import Image

# Define file path
file_path = "2022_place_canvas_history.parquet"

# Connect to DuckDB and enable multi-threading
con = duckdb.connect()
con.execute("PRAGMA threads=8;")  # Use 8 CPU threads for parallel processing

print("Running DuckDB query to compute most frequently placed color per pixel in the first 24 hours...")

query = f"""
WITH PixelCounts AS (
    SELECT x, y, pixel_color, COUNT(*) AS cnt
    FROM '{file_path}'
    WHERE timestamp < 86400000  -- First 24 hours in milliseconds
    GROUP BY x, y, pixel_color
),
RankedPixels AS (
    SELECT x, y, pixel_color, cnt,
           RANK() OVER (PARTITION BY x, y ORDER BY cnt DESC) AS rnk
    FROM PixelCounts
)
SELECT x, y, pixel_color FROM RankedPixels WHERE rnk = 1;
"""

# Execute the query and fetch results
result = con.execute(query).fetchall()

print(f"Query completed! Processing {len(result)} painted pixels for rendering...")

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

# Canvas size (1000 x 1000)
canvas_width, canvas_height = 1000, 1000
print(f"Canvas size: {canvas_width} x {canvas_height}")  # Debugging

# Use white background
default_color = (255, 255, 255)  # White as default
img = Image.new("RGB", (canvas_width, canvas_height), default_color)
pixels = img.load()  # Load pixels

# Fill in the most commonly placed color for each coordinate
for x, y, pixel_color in result:
    pixels[x, y] = indexed_rgb[pixel_color]  # Set color directly

# Enlarge the image so pixels are more visible
img = img.resize((canvas_width * 4, canvas_height * 4), Image.NEAREST)  # 4x zoom

# Save the final image
img.save("full_canvas_24h.png")
print("Image saved as full_canvas_24h.png!")