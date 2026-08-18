from decouple import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

spark = SparkSession.builder.appName("trades-stream-consumer").getOrCreate()

trade_schema = StructType([
    StructField("e", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType()),
    StructField("a", LongType()),
    StructField("p", StringType()),
    StructField("q", StringType()),
    StructField("f", LongType()),
    StructField("l", LongType()),
    StructField("T", LongType()),
    StructField("m", BooleanType()),
    StructField("M", BooleanType()),
])
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", config("MINIO_ENDPOINT"))
hadoop_conf.set("fs.s3a.access.key", config("MINIO_ACCESS_KEY"))
hadoop_conf.set("fs.s3a.secret.key", config("MINIO_SECRET_KEY"))
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# 1. Четене от Kafka
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config("KAFKA_BROKER_ADDRESS")) \
    .option("subscribe", "trade_streams") \
    .load()

# 2. Parsing - value идва като binary, cast към string, после JSON parse
parsed = raw.select(
    from_json(col("value").cast("string"), trade_schema).alias("data")
).select("data.*")

# 3. writeStream -
# 3. Извеждане на конзолата за проверка
query = parsed.writeStream \
    .format("parquet") \
    .option("path", "s3a://trades-raw/raw/") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/trades") \
    .outputMode("append") \
    .start()

query.awaitTermination()