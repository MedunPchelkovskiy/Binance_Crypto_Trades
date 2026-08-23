from decouple import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

from ingestion.avro_ser_deser import trade_deserialize

spark = SparkSession.builder.appName("trades-stream-consumer").getOrCreate()


hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", config("MINIO_ENDPOINT"))
hadoop_conf.set("fs.s3a.access.key", config("MINIO_ACCESS_KEY"))
hadoop_conf.set("fs.s3a.secret.key", config("MINIO_SECRET_KEY"))
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# 1. Четене от Kafka
# raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", config("KAFKA_BROKER_ADDRESS")) \
#     .option("subscribe", "trade_streams") \
#     .load()

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config("KAFKA_BROKER_ADDRESS_DEV")) \
    .option("subscribe", "trade_streams_dev") \
    .load()

deserialize_udf = udf(trade_deserialize)

parsed = raw.select(
    deserialize_udf(col("value")).alias("data")
).select("data.*")

parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

# # 2. Parsing - value идва като binary, cast към string, после JSON parse
# parsed = raw.select(
#     from_json(col("value").cast("string"), trade_schema).alias("data")
# ).select("data.*")

# # 3. writeStream -
# query = parsed.writeStream \
#     .format("parquet") \
#     .option("path", "s3a://trades-raw/raw/") \
#     .option("checkpointLocation", "/tmp/spark-checkpoints/trades") \
#     .outputMode("append") \
#     .start()

#
# def handle_sigterm(signum, frame):
#     query.stop()
#     sys.exit(0)


# signal.signal(signal.SIGTERM, handle_sigterm)

# query.awaitTermination()
