from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DateType, TimestampType
from pyspark.sql import SparkSession

spark = SparkSession.active()

mta_schema = StructType([
    StructField('actual_track', StringType(), True),
    StructField('arrival_time', StringType(), True),   # Raw ISO8601 string
    StructField('departure_time', StringType(), True), # Raw ISO8601 string
    StructField('direction', StringType(), True),
    StructField('ingestion_ts', StringType(), True),
    StructField('is_assigned', BooleanType(), True),
    StructField('route_id', StringType(), True),
    StructField('scheduled_track', StringType(), True),
    StructField('stop_id', StringType(), True),
    StructField('train_id', StringType(), True),
    StructField('trip_id', StringType(), True),
    StructField('date', DateType(), True),
    StructField('hour', IntegerType(), True)
])

input_path = "/app/sdp/pipeline-storage/"

@dp.table(name="raw_mta_data")
def ingest_jsonl():
    return (spark.readStream
            .format("json")
            .schema(mta_schema)
            .option("multiLine", "false")
            .load(input_path))

@dp.table(name="cleaned_mta_data")
def clean_data():
    source_df = spark.readStream.table("raw_mta_data")

    # Convert 'arrival_time' to a proper Timestamp for future Watermarking
    cleaned_df = (source_df
                  .withColumn("route_id", F.upper(F.col("route_id")))
                  .withColumn("event_time", F.to_timestamp(F.col("arrival_time")))
                  .withColumn("processing_ts", F.current_timestamp()))

    return cleaned_df

@dp.materialized_view(name="top_5_routes_summary")
def calculate_top_routes():
    # Aggregate and rank
    silver_df = spark.table("cleaned_mta_data")

    ranked_df = (silver_df
                 .groupBy("route_id")
                 .count()
                 .orderBy(F.col("count").desc())
                 .limit(5))

    return ranked_df

dp.create_streaming_table(name="mta_parquet_sink")

@dp.append_flow(target="mta_parquet_sink", name="export_to_parquet_flow")
def export_flow():
    # Bridge batch view to streaming sink
    summary_stream = spark.readStream.table("top_5_routes_summary")

    return summary_stream