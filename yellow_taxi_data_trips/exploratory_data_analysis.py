# %%
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("YellowTripData") \
    .getOrCreate()

# %%
# Load the Parquet file into a DataFrame
file_path = "data/yellow_tripdata_2023-12.parquet"
yellow_tripdata_df = spark.read.parquet(file_path)

# %%
# Show the first few rows of the DataFrame
yellow_tripdata_df.show()

# %%
from pyspark.sql.functions import col, isnan, when, count

# Print the schema of the DataFrame
yellow_tripdata_df.printSchema()


# %%
# Create a list to handle missing values appropriately for each column type
missing_values = yellow_tripdata_df.select(
    [
        count(when(col(c).isNull(), c)).alias(c)
        for c in yellow_tripdata_df.columns
    ]
)

# Show the missing values count per column
missing_values.show()

# %%
import pyarrow.parquet as pq
from google.cloud import storage
import io

PROJECT_ID = "nyc-yellow-trips"
BUCKET_NAME = f"{PROJECT_ID}-data-buckets"
GCS_FOLDER = "dataset/trips/"

storage_client = storage.Client()

def inspect_parquet_schema(file_name):
    """Download a Parquet file from GCS and inspect its schema."""
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{GCS_FOLDER}{file_name}")
    
    file_stream = io.BytesIO()
    blob.download_to_file(file_stream)
    file_stream.seek(0)
    
    table = pq.read_table(file_stream)
    print(table.schema)

# Remplace par un fichier qui pose problème
inspect_parquet_schema("yellow_tripdata_2024-04.parquet")

# %%
