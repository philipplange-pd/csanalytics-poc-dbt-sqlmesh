# Define Kafka parameters
kafka_bootstrap_servers = "pkc-rxgnk.eu-west-1.aws.confluent.cloud:9092"
topic = "FRA_slfinrtl_inventory_vehicle_snapshot_PP"

# read kafka client credentials
dbutils = DBUtils(spark)
kafka_client_secret = dbutils.secrets.get(
    scope="slfinrtl_inventory", key="slfinrtlDatabricksIntegrationPreviewSecret"
)
kafka_client_username = "B4XQWS2DB6QTHMUL"

# Read from Kafka topics
raw_kafka_events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", topic)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("startingOffsets", "earliest")
    .option(
        "kafka.sasl.jaas.config",
        f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required
        username='{kafka_client_username}'
        password='{kafka_client_secret}';""",
    )
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("includeHeaders", "true")
    .load()
)


def raw_inventory_vehicles_preview():
    # Select the key, value, and topic as strings
    df = (
        raw_kafka_events.select(
            "key",
            "topic",
            "timestamp",
            "value",
        )
        .withColumn("key", col("key").cast("string"))
        .withColumn("value", col("value").cast("string"))
        .withColumn("topic", col("topic").cast("string"))
        .withColumn("processed_at", current_timestamp())
    )
    # Ensure the schema matches the Delta table schema
    df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/mnt/delta/checkpoints/raw_inventory_vehicles_preview").saveAsTable("raw_inventory_vehicles_preview")
