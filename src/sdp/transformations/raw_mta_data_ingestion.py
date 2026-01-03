from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.active()

input_path = "/app/sdp/pipeline-storage/"
inferred_schema = spark.read.json(input_path).limit(1).schema

@dp.table(name="raw_mta_data")
def ingest_jsonl():
    raw_stream = (spark.readStream
                  .format("json")
                  .schema(inferred_schema)
                  .option("multiLine", "false")
                  .load(input_path))

    return raw_stream

@dp.table(name="cleaned_mta_data")
def clean_data():
    # Transform raw to cleaned
    source_df = spark.readStream.table("raw_mta_data")

    cleaned_df = (source_df
                  .withColumn("route_id", F.upper(F.col("route_id")))
                  .withColumn("timestamp", F.current_timestamp()))

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