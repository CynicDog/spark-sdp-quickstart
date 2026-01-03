from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.active()

# 1. Bronze: Ingest raw JSON
input_path = "/app/sdp/pipeline-storage/"
inferred_schema = spark.read.json(input_path).limit(1).schema

@dp.table(name="raw_mta_data")
def ingest_jsonl() -> DataFrame:
    """Ingests raw JSONL files from the specified landing zone.

    Returns:
        pyspark.sql.DataFrame: A streaming DataFrame containing raw MTA records
        based on the inferred schema.
    """
    return (
        spark.readStream
        .format("json")
        .schema(inferred_schema)
        .option("multiLine", "false")
        .load(input_path)
    )

# 2. Silver: Clean and Prepare
@dp.table(name="cleaned_mta_data")
def clean_data() -> DataFrame:
    """Cleans raw MTA data by normalizing route IDs and adding timestamps.

    Returns:
        pyspark.sql.DataFrame: A streaming DataFrame with uppercased route IDs
        and a new processing timestamp column.
    """
    return (
        spark.readStream.table("raw_mta_data")
        .withColumn("route_id", F.upper(F.col("route_id")))
        .withColumn("timestamp", F.current_timestamp())
    )

# 3. Gold: Aggregate Top 5
@dp.materialized_view(name="top_5_routes_summary")
def calculate_top_routes() -> DataFrame:
    """Calculates a summary of the most frequent transit routes.

    Returns:
        pyspark.sql.DataFrame: A batch DataFrame (Materialized View) containing
        route IDs and their respective counts, ordered by frequency.
    """
    return (
        spark.table("cleaned_mta_data")
        .groupBy("route_id")
        .count()
        .orderBy(F.col("count").desc())
        .limit(5)
    )

# 4. Sink: Final Export
dp.create_streaming_table(name="mta_parquet_sink")

@dp.append_flow(
    target="mta_parquet_sink",
    name="export_to_parquet_flow"
)
def export_flow() -> DataFrame:
    """Routes the gold layer summary to a persistent streaming parquet sink.

    This flow bridges the batch Materialized View back into a streaming table
    to facilitate ongoing snapshots of route performance.

    Returns:
        pyspark.sql.DataFrame: A streaming relation reading from the top_5 summary view.
    """
    return spark.readStream.table("top_5_routes_summary")