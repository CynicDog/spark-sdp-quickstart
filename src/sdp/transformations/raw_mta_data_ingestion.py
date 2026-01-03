from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.active()

input_path = "/app/sdp/pipeline-storage/"
inferred_schema = spark.read.json(input_path).limit(1).schema

@dp.table(
    name="raw_mta_data"
)
def ingest_jsonl():
    return (
        spark.readStream
        .format("json")
        .schema(inferred_schema)
        .option("multiLine", "false")
        .load(input_path)
    )