from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Get the active session managed by the pipeline runner
spark = SparkSession.active()

@dp.materialized_view
def mta_trips_cleaned() -> DataFrame:
    """
    Reads the raw JSONL files from the onsite storage (pipeline-storage)
    and performs initial type casting and cleaning.
    """
    # Path relative to the root of your 'sdp' directory
    raw_path = "pipeline-storage/"
    
    # Spark SDP automatically handles the partition discovery for date/hour
    df = spark.read.json(raw_path)
    
    return df.select(
        F.col("trip_id"),
        F.col("route_id"),
        F.col("stop_id"),
        # Convert ISO8601 strings to proper Timestamp types
        F.col("arrival_time").cast("timestamp"),
        F.col("departure_time").cast("timestamp"),
        F.col("ingestion_ts").cast("timestamp"),
        F.col("is_assigned"),
        F.col("actual_track"),
        # These columns are derived from the folder names
        F.col("date"),
        F.col("hour")
    )

@dp.materialized_view
def select_cleaned() -> DataFrame:
    """
    Downstream view that depends on mta_trips_cleaned.
    Calculates the volume of traffic per route.
    """
    # You can reference the previous view as a table
    df = spark.table("mta_trips_cleaned")
    
    return df.select(
        "trip_id", 
        "route_id", 
        "stop_id", 
        "arrival_time", 
        "date", 
        "hour"
    )