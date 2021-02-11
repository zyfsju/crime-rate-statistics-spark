import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType(
    [
        StructField("crime_id", StringType()),
        StructField("original_crime_type_name", StringType()),
        StructField("report_date", TimestampType()),
        StructField("call_date", TimestampType()),
        StructField("offense_date", TimestampType()),
        StructField("call_time", StringType()),
        StructField("call_date_time", TimestampType()),
        StructField("disposition", StringType()),
        StructField("address", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("agency_id", StringType()),
        StructField("address_type", StringType()),
        StructField("common_location", StringType()),
    ]
)


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "police.service.calls")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10)
        .option("maxRatePerPartition", 10)
        .load()
    )

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("cast(value as string) ")

    service_table = kafka_df.select(
        psf.from_json(psf.col("value"), schema).alias("DF")
    ).select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select(
        "original_crime_type_name", "disposition"
    ).distinct()

    # count the number of original crime type
    agg_df = distinct_table.groupBy("original_crime_type_name").count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df.writeStream.format("console").outputMode("complete").start()

    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df.join(
        radio_code_df, col("agg_df.disposition") == col("radio_code_df.disposition")
    )

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("KafkaSparkStructuredStreaming")
        .config("spark.ui.port", 3000)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("DEBUG")
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
