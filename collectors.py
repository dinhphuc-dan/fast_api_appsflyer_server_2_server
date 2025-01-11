import os
import logging
import json

from typing import Annotated
from fastapi import (
    FastAPI,
    status,
    Body,
    HTTPException,
    BackgroundTasks,
)
from kafka import KafkaProducer
from utilities import ValidPath, load_logger


app = FastAPI()
logger = load_logger(logger_name="collectors")
number_request = 0
kafka_producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    key_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def write_data_to_kafka(message: dict, topic_name: str) -> None:
    key_for_partition = message.get("bundle_id", "no bundle id")
    logger.info(f"Sending message to topic: {topic_name}, key: {key_for_partition}")
    kafka_producer.send(topic=topic_name, value=message, key=key_for_partition)


@app.post("/{path_name}", status_code=status.HTTP_200_OK)
async def receive_realtime_message(
    path_name: ValidPath,
    message: Annotated[dict, Body()],
    background_tasks: BackgroundTasks,
) -> str:
    global number_request
    if path_name not in [p.value for p in ValidPath]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid path"
        )
    else:
        background_tasks.add_task(
            write_data_to_kafka, message, topic_name=path_name.value
        )
        number_request += 1
        logger.info(
            f"Recieved message: {len(message)}, request number: {number_request}"
        )
        return "OK"
