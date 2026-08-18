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

# 1. Четене от Kafka
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trade_streams") \
    .load()

# 2. Parsing - value идва като binary, cast към string, после JSON parse
parsed = raw.select(
    from_json(col("value").cast("string"), trade_schema).alias("data")
).select("data.*")

# 3. writeStream -
# 3. Извеждане на конзолата за проверка
query = parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()