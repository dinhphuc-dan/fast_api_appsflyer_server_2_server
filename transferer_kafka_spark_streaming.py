import logging
from pathlib import Path
import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import (
    StreamingQuery,
    StreamingQueryListener,
    StreamingQueryManager,
)
from pyspark.sql.functions import split, col


class MyListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"'{event.name}' [{event.id}] got started!")

    def onQueryProgress(self, event):
        row = event.progress.observedMetrics.get("metric")
        if row is not None:
            if row.malformed / row.cnt > 0.5:
                print(
                    "ALERT! Ouch! there are too many malformed "
                    f"records {row.malformed} out of {row.cnt}!"
                )
            else:
                print(f"{row.cnt} rows processed!")

    def onQueryTerminated(self, event):
        print(f"{event.id} got terminated!")


listener = MyListener()


class KafkaSparkStreaming:
    def __init__(self):
        # setup env for spark
        os.environ["SPARK_HOME"] = os.path.join(
            Path.cwd(), ".venv\lib\site-packages\pyspark"
        )
        os.environ["PYSPARK_PYTHON"] = "python"

        self.kafka_sever = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.spark_session = self._create_spark_session()

    # Initialize SparkSession
    def _create_spark_session(self) -> SparkSession:
        spark_session = (
            SparkSession.builder.appName("Kafka Spark Streaming to Postgres")
            .config("spark.log.level", "WARN")
            # add packages work with kafka and postgres
            .config(
                "spark.jars.packages",
                "org.postgresql:postgresql:42.7.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            )
            .getOrCreate()
        )
        spark_session.conf.set("spark.sql.streaming.metricsEnabled", "true")
        return spark_session

    # Read from Kafka
    def read_from_one_kafka_topic(self, topic_name: str) -> DataFrame:
        df = (
            self.spark_session.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_sever)
            .option("subscribe", topic_name)  # Our topic name
            .option("includeHeaders", "true")
            .option(
                "startingOffsets", "latest"
            )  # Start from the beginning when we consume from kafka
            .load()
        )
        return df

    def write_to_console(self, df: DataFrame) -> StreamingQuery:
        query = (
            df.writeStream.outputMode("append")
            .format("console")
            .trigger(processingTime="1 minute")
            .start()
        )
        return query

    def write_to_csv(self, df: DataFrame) -> StreamingQuery:
        query = (
            df.writeStream.outputMode("append")
            .format("csv")
            .option("header", "true")
            .option("path", "./output")
            .option("checkpointLocation", "./checkpoint")
            .trigger(processingTime="1 minute")
            .start()
        )
        return query

    def foreach_batch_function(self, df: DataFrame, table_name: str) -> None:
        postgres_url = f'jdbc:postgresql://{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}/{os.getenv("POSTGRES_DB")}'
        df.write.mode("append").format("jdbc").option("url", postgres_url).option(
            "driver", "org.postgresql.Driver"
        ).option("dbtable", table_name).option(
            "user", os.getenv("POSTGRES_USER")
        ).option(
            "password", os.getenv("POSTGRES_PASSWORD")
        ).save()

    def write_to_postgres(self, df: DataFrame) -> StreamingQuery:
        query = (
            df.writeStream.foreachBatch(self.foreach_batch_function)
            .trigger(processingTime="1 minute")
            .start()
        )
        return query
