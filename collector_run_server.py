import logging
from pathlib import Path
import os
from dotenv import load_dotenv
from utilities import load_logger, check_kafka_server, check_production_args, ValidPath

import uvicorn
from pyngrok import ngrok
from kafka.admin import KafkaAdminClient, NewTopic


@check_kafka_server
def setup_kafka(kafka_server) -> None:
    """
    check kafka connection and create topic if not exist. The topic name is defined in ValidPath
    Arguments:
        kafka_server: a KafkaAdminClient, which is created and passed to fuction by using decorator
    Return None
    """
    try:
        topics_list: list[str] = kafka_server.list_topics()
        for path in [p.value for p in ValidPath]:
            if path not in topics_list:
                topic = NewTopic(
                    name=path,
                    num_partitions=int(os.getenv("KAFKA_NUMBER_PARTITION_PER_TOPIC")),
                    replication_factor=int(os.getenv("KAFKA_REPLICATION_FACTOR")),
                )
                kafka_server.create_topics(new_topics=[topic])
                logger.info(f"Topic {path} is created")
    except Exception:
        raise RuntimeError("Kafka connection error")


if __name__ == "__main__":

    # setup logging
    logger = load_logger(logger_name="collectors")

    if check_production_args():
        pass
    else:
        load_dotenv(dotenv_path=Path(".env.dev"), override=True, verbose=True)

        # setup kafka
        setup_kafka()

        # setup ngrok and run server
        ngrok.set_auth_token(os.getenv("NGROK_AUTHTOKEN"))
        url = ngrok.connect(8080).public_url
        logger.info(f"ngrok url: {url}")
        uvicorn.run(
            "collectors:app",
            host=os.getenv("COLLECTOR_HOST"),
            port=int(os.getenv("COLLECTOR_PORT")),
            reload=False,
            log_config="log_conf.yaml",
        )
