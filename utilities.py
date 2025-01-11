import logging
import logging.config
import yaml
import os
from functools import wraps
import argparse
from enum import Enum

from kafka.admin import KafkaAdminClient
from sqlalchemy import create_engine, Table, MetaData, URL, Column, String


def check_production_args():
    # basic setup for argument throug command line for local testing and development
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--production")
    _args = _parser.parse_args()
    if _args.production == "true":
        return True
    else:
        return False


def load_logger(logger_name) -> logging.Logger:
    with open("log_conf.yaml", "r") as f:
        logging.config.dictConfig(yaml.safe_load(f))
    return logging.getLogger(logger_name)


def check_kafka_server(func):
    """a decorator to check kafka connection"""

    @wraps(func)
    def decorated(*args, **kwargs):
        try:
            kafka_server = KafkaAdminClient(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS")
            )
            return func(kafka_server=kafka_server, *args, **kwargs)
        except Exception as e:
            raise e

    return decorated


def create_postgres_database_and_table(
    table_name: str = None, schema: str = None
) -> None:
    postgres_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
    )

    engine = create_engine(postgres_url)
    metadata_obj = MetaData()
    table = Table(
        table_name,
        metadata_obj,
        *(Column(col_name, String) for col_name in table_name),
    )
    metadata_obj.create_all(engine)


class ValidPath(str, Enum):
    """
    Class affects valid HTTP path from collectors and number of spark jobs in transferer
    """

    appsflyer_s2s = "appsflyer-s2s"
    google_play_store_transaction = "google-play-store-transaction"
    apple_store_transaction = "apple-store-transaction"
