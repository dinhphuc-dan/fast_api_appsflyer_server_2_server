from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os
from pathlib import Path

os.environ["SPARK_HOME"] = os.path.join(Path.cwd(), ".venv\lib\site-packages\pyspark")
os.environ["PYSPARK_PYTHON"] = "python"

spark = (
    SparkSession.builder.appName("Kafka Spark Streaming to Postgres")
    .config("spark.log.level", "WARN")
    # add packages work with kafka and postgres
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    )
    .getOrCreate()
)

data = [
    ("1", "{'json_col1': 'hello', 'json_col2': 32}", "1.0"),
    ("1", "{'json_col1': 'hello', 'json_col2': 'world'}", "1.0"),
    ("1", "{'json_col1': 'hello', 'json_col2': null}", "1.0"),
    ("1", "{'json_col1': 'hello'}", "1.0"),
    ("1", "{'json_col1': 'hello','json_col2': 0}", "1.0"),
    ("1", "{'json_col1': 'hello','json_col2': }", "1.0"),
]

schema = StructType(
    [
        StructField("id", StringType()),
        StructField("value", StringType()),
        StructField("token", StringType()),
    ]
)

df = spark.createDataFrame(data, schema)


def get_json_schema():
    x = ["json_col1", "json_col2"]
    schema = StructType()
    for i in x:
        schema.add(i, StringType(), True)
    return schema


def create_required_column():
    x = ["json_col1", "json_col2"]
    return concat(*x).alias("unique_key")


json_schema = StructType(
    [
        StructField("json_col1", StringType(), True),
        StructField("json_col2", IntegerType(), False),
    ]
)

df2 = (
    df.withColumn("json", from_json(col("value"), json_schema))
    .select("*", "json.*")
    .select("*", create_required_column())
)
df2.show()
