from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaSparkPostgres") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("action", StringType()) \
    .add("timestamp", StringType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user-behavior") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

parsed_df = parsed_df.withColumn("event_time", to_timestamp("timestamp"))

view_counts = parsed_df \
    .filter(col("action") == "view") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("product_id")
    ) \
    .count() \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "product_id",
        col("count").alias("view_count")
    )

def write_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/testdb") \
        .option("dbtable", "product_views") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = view_counts \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "./checkpoint") \
    .start()

query.awaitTermination()