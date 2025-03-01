Data Formatting:
The original dataset was uniformly formatted, making it easy to parse through the CSV files and convert them to Parquet. However, some formatting choices were inconvenient, so I made adjustments during conversion.

Originally, each card had a separate column for the "team" player and the "opponent" player, with True indicating the card was in the deck and False indicating it was not. Since there were over 120 cards, this resulted in around 250 columns, making it difficult to visualize and print the data.

To improve this, I converted the True/False values into lists of card numbers in the Parquet file. This allows for easier card usage calculations by simply checking whether a card is in a player's deck list.

Additionally, the dataset includes accurate date timestamps, which will be useful for future analysis, such as identifying card usage trends before and after balance patches that introduced buffs or nerfs.

Data Size & Composition:
The dataset spans over a year, divided into 12 monthly directories, with daily CSV files inside each. Initially, I tried loading all CSV files at once and then converting them to Parquet, but my computer couldn't handle that amount of data.

To solve this, I instead converted each CSV file individually and appended it incrementally to a large Parquet file.

I started by converting one day's CSV file to Parquet to verify the format before proceeding. Once the formatting looked correct, I looped through all directories, converting and appending the data into one large Parquet file.

This process took some time, but now my entire dataset is efficiently compressed in a single Parquet file, making it easy to query and analyze for my final analysis.