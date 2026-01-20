from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, count, desc
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 1. Configuration & Session Setup
CHECKPOINT_PATH = "/tmp/spark-checkpoints/logs-processing"

spark = (
    SparkSession.builder
    .appName("LogsProcessor")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) # after finishing a batch store progress here for recovery
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# 2. Define schema
schema = StructType([
    StructField("timestamp", LongType()), 
    StructField("status", StringType()),
    StructField("severity", StringType()),
    StructField("source_ip", StringType()),
    StructField("user_id", StringType()),
    StructField("content", StringType())
])

# 3. Read Stream
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "logs") # topic name
    .option("startingOffsets", "earliest") # read from the beginning of the topic
    .option("failOnDataLoss", "false") # do not fail if data is lost (e.g., topic deleted)
    .load()
)

# 4. Processing, Filtering & Aggregation
# We filter first to keep the state store small, then group and count.
analysis_df = (
    raw_df.select(from_json(col("value").cast("string"), schema).alias("data")) # kafka everything is a byte array
    .select("data.*")
    .filter(
        (lower(col("content")).contains("vulnerability")) & 
        (col("severity") == "High")
    )
    .groupBy("source_ip") # group by machine IP
    .agg(count("*").alias("match_count")) # count matches per source IP
    .orderBy(desc("match_count"))
)

# 5. Writing
# usually write back to kafka, here only console for demo purposes
query = (
    analysis_df.writeStream
    .outputMode("complete") # output the entire result table to the sink
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()