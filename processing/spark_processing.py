from decouple import config
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("trades-stream-consumer").getOrCreate()

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", config("MINIO_ENDPOINT"))
hadoop_conf.set("fs.s3a.access.key", config("MINIO_ACCESS_KEY"))
hadoop_conf.set("fs.s3a.secret.key", config("MINIO_SECRET_KEY"))
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config("KAFKA_BROKER_ADDRESS_DEV")) \
    .option("subscribe", "trade_streams_avro_dev") \
    .load()

raw.writeStream \
.format("ne znam kakuv") \
.option("path", "s3a://trades-raw/raw/") \
.option("checkpointLocation", "/tmp/spark-checkpoints/trades") \
.outputMode("append") \
.start() \
.awaitTermination()
