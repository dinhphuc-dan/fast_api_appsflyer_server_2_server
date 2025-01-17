import logging
from pathlib import Path
import os
from dotenv import load_dotenv
from utilities import *

from transferer_kafka_spark_streaming import KafkaSparkStreaming
from sqlalchemy import URL, Engine
from sqlmodel import create_engine, SQLModel, inspect, Session, Column, text
from sqlalchemy.sql import sqltypes
from sqlmodel.sql.sqltypes import AutoString
from sqlalchemy.sql.sqltypes import Integer, Float, Date, DateTime, Boolean

from pyspark.sql import DataFrame, types
from pyspark.sql import functions as spark_func
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DateType,
    DayTimeIntervalType,
    BooleanType,
)
import pandas as pd


class Transferer(KafkaSparkStreaming):
    """
    This class hold connection to postgres and kafka, which aims to check data quality while streaming
    If it detects bad data, which is not aligned with the predefined schema, it will write all unqualified data to a single table name bad_data
    Otherwise, it will record to a table name with relevant path
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_engine = self._create_postgres_engine()
        self.create_postgres_tables_and_add_new_columns()

    def _create_postgres_engine(self) -> Engine:
        postgres_url = URL.create(
            drivername="postgresql+psycopg2",
            username=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
        )
        return create_engine(postgres_url)

    def _init_all_database_if_not_exist(self):
        """
        Return
            None. But create all database if not exists
        """
        SQLModel.metadata.create_all(self.postgres_engine)

    def _getting_input_tables(self) -> list[dict]:
        """
        Return:
            list of dictionary contains expected table and its column object from sqlalchemy,
            ex [{"table_1": ["sqlalchemy.sql.schema.Column_1", "sqlalchemy.sql.schema.Column_2"]}]
        """
        input_tables = []
        for table_name in SQLModel.metadata.tables.keys():
            list_columns = SQLModel.metadata.tables[table_name].columns.values()
            input_tables.append({table_name: list_columns})
        return input_tables

    def _getting_existing_tables(self) -> list[dict]:
        """
        Return:
            list of dictionary contains existing table and its column name, ex [{"table_1": ["id", "name"]}]
        """
        existing_tables = []
        for table_name in inspect(self.postgres_engine).get_table_names():
            list_columns_name = inspect(self.postgres_engine).get_columns(table_name)
            existing_tables.append({table_name: list_columns_name})
        return existing_tables

    def _getting_collums_to_add(self) -> list[Column]:
        """
        Return:
            list of columns to add to database
        """
        input_schemas = self._getting_input_tables()
        output_schemas = self._getting_existing_tables()
        list_columns_to_add = []
        for input_table in input_schemas:
            for output_table in output_schemas:
                if list(input_table.keys())[0] == list(output_table.keys())[0]:
                    for input_column in list(input_table.values())[0]:
                        if input_column.name not in [
                            output_column["name"]
                            for output_column in list(output_table.values())[0]
                        ]:
                            list_columns_to_add.append(input_column)

        return list_columns_to_add

    def create_postgres_tables_and_add_new_columns(self):
        """
        Return:
            None. But it creates all tables based on input if not exists or add new columns if there is a schema change
        """
        try:
            self._init_all_database_if_not_exist()
            list_columns_to_alter = self._getting_collums_to_add()
            if list_columns_to_alter:
                with Session(self.postgres_engine) as session:
                    for column in list_columns_to_alter:
                        sql_text = f"alter table {column.table} add column {column.name} {column.type}"
                        session.exec(statement=text(sql_text))
                        session.commit()
                        logger.info(
                            f"added column {column.name} to table {column.table}"
                        )

            else:
                logger.info("No columns to alter")
        except Exception as e:
            raise e

    def _convert_table_schema_to_dataframe_schema(self, table_name: str) -> StructType:
        """
        Arguments:
            table_name: a SQLModel table name defined in utilities.py
        Return:
            - List of column name and type to be used in spark json schema
            - List of required columns name to help detect null row
                ex, StructType(
                    [
                        StructField("json_col1", StringType(), True),
                        StructField("json_col2", IntegerType(), False),
                    ]
                And ["json_col1", "json_col2"]
        """
        json_schema = StructType()
        list_required_columns: list[str] = []
        for table in self._getting_input_tables():
            if list(table.keys())[0] == table_name:
                for column in list(table.values())[0]:
                    if not column.primary_key:
                        json_schema.add(
                            column.name,
                            self._convert_datatype_from_sqlmodel_to_spark(column.type),
                            column.nullable,
                        )
                        if not column.nullable:
                            list_required_columns.append(column.name)
                return json_schema, list_required_columns

    def _convert_datatype_from_sqlmodel_to_spark(self, input_type: sqltypes) -> types:
        """
        Arguments:
            input_types: a sql data types
        Return:
            output_types: an spark data types.
        """
        if type(input_type) == Integer:
            output_type = IntegerType()
        elif type(input_type) == Float:
            output_type = FloatType()
        elif type(input_type) == Date:
            output_type = DateType()
        elif type(input_type) == DateTime:
            output_type = DayTimeIntervalType()
        elif type(input_type) == Boolean:
            output_type = BooleanType()
        else:
            output_type = StringType()
        return output_type

    def transform_data_from_kafka_topic(
        self, df: DataFrame, table_name: str
    ) -> DataFrame:
        """
        Arguments:
            df: Spark DataFrame
            table_name: table destination name
        Return:
            df: a dataframe which normalize all json data with table predesigned schema, and a required_fields columns for checking null

        """
        json_schema, list_required_columns = (
            self._convert_table_schema_to_dataframe_schema(table_name=table_name)
        )
        return df.withColumn(
            "json_data",
            spark_func.from_json(spark_func.expr("CAST(value as STRING)"), json_schema),
        ).select(
            "*",
            "json_data.*",
            spark_func.concat(*list_required_columns).alias("required_fields"),
        )

    def detect_good_data_based_on_schema_validation(self, df: DataFrame) -> DataFrame:
        """
        Arguments:
            df: a spark dataframe with required_fields
        Return:
            good_df: a spark dataframe which contains row that validate table schema
        """
        good_df = df.filter(df["required_fields"].isNotNull()).select("json_data.*")
        return good_df

    def detect_bad_data_based_on_schema_validation(self, df: DataFrame) -> DataFrame:
        """
        Arguments:
            df: a spark dataframe with required_fields
        Return:
            bad_df: a spark dataframe which contains row that NOT validate table schema, and its error reason
        """
        required_field_error_reason = "Required fields are missing"
        bad_df = df.filter(df["required_fields"].isNull()).select(
            spark_func.expr("CAST(value as STRING)").alias("json_data_string"),
            spark_func.lit(required_field_error_reason).alias("error_reason"),
            spark_func.expr("CAST(key as STRING)").alias("path"),
            spark_func.current_timestamp().alias("event_time"),
        )
        return bad_df


if __name__ == "__main__":
    # setup logging
    logger = load_logger(logger_name="transferer")
    if check_production_args():
        pass
    else:
        load_dotenv(dotenv_path=Path(".env.dev"), override=True, verbose=True)
        basic_transferer = Transferer()

        kafka_df = basic_transferer.read_from_one_kafka_topic(
            os.getenv("TRANSFERER_SOURCE"), read_method="stream"
        )
        # kafka_df.show()
        # kafka_df.printSchema()
        transformed_df = basic_transferer.transform_data_from_kafka_topic(
            df=kafka_df, table_name=os.getenv("TRANSFERER_SOURCE").replace("-", "")
        )
        good_df = basic_transferer.detect_good_data_based_on_schema_validation(
            transformed_df
        )

        # good_df.printSchema()

        bad_df = basic_transferer.detect_bad_data_based_on_schema_validation(
            transformed_df
        )

        # bad_df.printSchema()

        good_query = basic_transferer.write_to_postgres(
            df=good_df,
            table_name=os.getenv("TRANSFERER_SOURCE").replace("-", ""),
            checkpoint_location="./good_data_checkpoint",
        )
        bad_query = basic_transferer.write_to_postgres(
            df=bad_df,
            table_name="baddatatable",
            checkpoint_location="./bad_data_checkpoint",
        )
        basic_transferer.spark_session.streams.awaitAnyTermination()
