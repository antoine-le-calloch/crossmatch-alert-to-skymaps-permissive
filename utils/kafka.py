import io
import os
import fastavro

from dotenv import load_dotenv
from datetime import datetime, UTC
from confluent_kafka import Consumer
from utils.logger import log

load_dotenv()

def read_avro(msg):
    """
    Reads an Avro record from a Kafka message.

    Parameters
    ----------
    msg : Kafka message
        The Kafka message containing the Avro record.

    Returns
    -------
    dict or None
        The first record found in the Avro message, or None if no records are found.
    """
    bytes_io = io.BytesIO(msg.value())  # Get the message value as bytes
    bytes_io.seek(0)
    for record in fastavro.reader(bytes_io):
        return record  # Return the first record found
    return None  # Return None if no records are found or if an error occurs


def boom_consumer(topics=None):
    """
    Creates a Kafka consumer for the BOOM alert stream.

    Parameters
    ----------
    topics : list of str, optional
        A list of Kafka topics to subscribe to. If None, it will subscribe to
        the comma-separated topics specified in the BOOM_KAFKA_TOPICS environment variable.

    Returns
    -------
    Consumer
        A Kafka consumer configured to consume from the BOOM alert stream.
    """
    topics = topics or [topic.strip() for topic in os.getenv("BOOM_KAFKA_TOPICS", "").split(",") if topic.strip()]
    if not topics:
        raise ValueError("No Kafka topic to subscribe to: set BOOM_KAFKA_TOPICS")
    boom_config = {
        'bootstrap.servers': os.getenv("BOOM_KAFKA_SERVER"),
        'group.id': f'umn_boom_kafka_consumer_group_{datetime.now(UTC).strftime("%Y_%m_%d_%H_%M_%S")}',
        'auto.offset.reset': 'earliest',
        "enable.auto.commit": False,
        **({
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": os.getenv("BOOM_KAFKA_USERNAME"),
            "sasl.password": os.getenv("BOOM_KAFKA_PASSWORD"),
        } if os.getenv("BOOM_KAFKA_USERNAME") and os.getenv("BOOM_KAFKA_PASSWORD") else {
            "security.protocol": "PLAINTEXT"
        })
    }

    consumer = Consumer(boom_config)
    consumer.subscribe(topics)
    log(f"Subscribed to topics: {topics}")
    return consumer