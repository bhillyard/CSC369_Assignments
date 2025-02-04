import duckdb
from PIL import Image

# Define file path
file_path = "2022_place_canvas_history.parquet"

# Connect to DuckDB and enable multi-threading
con = duckdb.connect()
con.execute("PRAGMA threads=8;")  # Use 8 CPU threads for parallel processing

print("Running DuckDB query to compute top 1% most painted pixels with their most common color...")

query = f"""
WITH PixelCounts AS (
    SELECT x, y, COUNT(*) AS total_edits
    FROM '{file_path}'
    GROUP BY x, y
),
Threshold AS (
    SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_edits) AS threshold
    FROM PixelCounts
),
FilteredPixels AS (
    SELECT pc.x, pc.y, p.pixel_color, COUNT(*) AS color_count
    FROM '{file_path}' p
    JOIN PixelCounts pc ON p.x = pc.x AND p.y = pc.y
    WHERE pc.total_edits >= (SELECT threshold FROM Threshold)
    GROUP BY pc.x, pc.y, p.pixel_color
),
RankedPixels AS (
    SELECT x, y, pixel_color,
           RANK() OVER (PARTITION BY x, y ORDER BY color_count DESC) AS rnk
    FROM FilteredPixels
)
SELECT x, y, pixel_color FROM RankedPixels WHERE rnk = 1;
"""
result = con.execute(query).fetchall()

print(f"Query completed! Processing {len(result)} top painted pixels for rendering...")

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

# Determine canvas size
canvas_width, canvas_height = 2000, 2000  # Assuming r/place is 1000x1000

# Create a transparent background image (only top 1% pixels shown)
img = Image.new("RGBA", (canvas_width, canvas_height), (200, 200, 200, 200))  # Transparent
pixels = img.load()

# Fill in the most commonly placed color for each of the top 1% most edited pixels
for x, y, pixel_color in result:
    if 0 <= pixel_color < len(indexed_rgb):  # Ensure valid color index
        pixels[x, y] = indexed_rgb[pixel_color] + (255,)  # Add full opacity

# Enlarge the image so pixels are more visible
img = img.resize((canvas_width * 4, canvas_height * 4), Image.NEAREST)  # 4x zoom

# Save the final image
img.save("top_1_percent_most_common_color.png")
print("Image saved as top_1_percent_most_common_color.png!")
