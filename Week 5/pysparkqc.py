from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import StorageLevel
from PIL import Image
from datetime import datetime
import time

# Initialize Spark session with optimized settings
spark = SparkSession.builder \
    .appName("rPlaceCanvas") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Start timing
start_time = time.time()

# Define file path
file_path = "place_canvas_with_xy.parquet"

# Read the Parquet file
df = spark.read.parquet(file_path)

# Timestamp to visualize
specified_timestamp = "2022-04-03 23:00:00"
START_TIME = 1648806250315  # Start time of r/Place 2022

def parse_timestamp(timestamp):
    date_format = "%Y-%m-%d %H:%M:%S"
    timestamp_obj = datetime.strptime(timestamp, date_format)
    timestamp_ms = int(timestamp_obj.timestamp() * 1000) - START_TIME
    return timestamp_ms

# Convert timestamp to milliseconds since the start of r/Place 2022
timestamp_ms = parse_timestamp(specified_timestamp)
timestamp_ms -= (7200000 * 2 + 4200000)

# Filter pixels placed before the specified timestamp
filtered_df = df.filter(df.timestamp <= timestamp_ms)

# Persist the filtered DataFrame to avoid recomputation
filtered_df.persist(StorageLevel.MEMORY_AND_DISK)

# Window to rank pixels by timestamp (latest first)
window_spec = Window.partitionBy("x", "y").orderBy(F.desc("timestamp"))

# Add row number for ranking
ranked_df = filtered_df.withColumn("rnk", F.row_number().over(window_spec))

# Select the latest pixel for each coordinate
result_df = ranked_df.filter(F.col("rnk") == 1).select("x", "y", "pixel_color")

# Collect all results
result = result_df.collect()

# Measure execution time
end_time = time.time()
execution_time = end_time - start_time

print(f"Query completed in {execution_time:.2f} seconds! Processing {len(result)} painted pixels for rendering...")

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

# Canvas size (2000 x 2000)
canvas_width, canvas_height = 2000, 2000
print(f"Canvas size: {canvas_width} x {canvas_height}")

# Use white background
default_color = (255, 255, 255)
img = Image.new("RGB", (canvas_width, canvas_height), default_color)
pixels = img.load()

# Fill in the last placed pixel color for each coordinate
for row in result:
    x, y, pixel_color = row['x'], row['y'], row['pixel_color']
    if 0 <= pixel_color < len(indexed_rgb):
        pixels[x, y] = indexed_rgb[pixel_color]
    else:
        print(f"Warning: pixel_color {pixel_color} at ({x}, {y}) is out of range.")

# Enlarge the image so pixels are more visible
img = img.resize((canvas_width * 2, canvas_height * 2), Image.NEAREST)

# Save the final image
img.save("canvas_at_timestamp.png")
print(f"Image saved as canvas_at_timestamp.png for timestamp {specified_timestamp}!")

# Stop the Spark session
spark.stop()
