import logging
from pathlib import Path
import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.streaming import StreamingQuery


class KafkaSparkStreaming:
    def __init__(self):
        # setup env for spark
        os.environ["SPARK_HOME"] = os.path.join(
            Path.cwd(), ".venv\lib\site-packages\pyspark"
        )
        os.environ["PYSPARK_PYTHON"] = "python"

        self.kafka_sever = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.spark_session = self._create_spark_session()
        self.trigger_interval = os.getenv("TRIGGER_INTERVAL")

    # Initialize SparkSession
    def _create_spark_session(self) -> SparkSession:
        spark_session = (
            SparkSession.builder.appName("Kafka Spark Streaming to Postgres")
            .config("spark.log.level", "INFO")
            # add packages work with kafka and postgres
            .config(
                "spark.jars.packages",
                "org.postgresql:postgresql:42.7.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            )
            .getOrCreate()
        )
        return spark_session

    # Read from Kafka
    def read_from_one_kafka_topic(
        self, topic_name: str, read_method: str = "stream"
    ) -> DataFrame:
        """
        Arguments:
            topic_name: name of the topic
            read_method: enum batch or stream, default is stream
        """
        if read_method == "batch":
            df = (
                self.spark_session.read.format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_sever)
                .option("subscribe", topic_name)  # Our topic name
                .option("includeHeaders", "true")
                .load()
            )
            return df
        elif read_method == "stream":
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
            .trigger(processingTime=self.trigger_interval)
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
            .trigger(processingTime=self.trigger_interval)
            .start()
        )
        return query

    def _write_to_postgres_foreach_batch_function(
        self, table_name: str
    ) -> DataFrameWriter:
        def _do_work(df: DataFrame, batch_id):
            postgres_url = f'jdbc:postgresql://{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}/{os.getenv("POSTGRES_DB")}'
            df.write.mode("append").format("jdbc").option("url", postgres_url).option(
                "driver", "org.postgresql.Driver"
            ).option("dbtable", table_name).option(
                "user", os.getenv("POSTGRES_USER")
            ).option(
                "password", os.getenv("POSTGRES_PASSWORD")
            ).save()

        return _do_work

    def write_to_postgres(
        self, df: DataFrame, table_name: str, checkpoint_location: str
    ) -> StreamingQuery:
        # https://stackoverflow.com/questions/69626511/is-there-a-way-to-pass-an-additional-extra-parameter-in-foreachbatch-function
        query = (
            df.writeStream.foreachBatch(
                self._write_to_postgres_foreach_batch_function(table_name=table_name)
            )
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime=self.trigger_interval)
            .start()
        )
        return query
