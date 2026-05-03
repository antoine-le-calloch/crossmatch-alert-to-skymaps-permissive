import json

from gcn_kafka import Consumer

from utils.gcn import get_gcn_kafka_config
from utils.logger import log, RED, YELLOW, ENDC


def list_gcn_topics(topic_filter=None, testing_mode=None):
    config = get_gcn_kafka_config(testing_mode)
    consumer = Consumer(
        client_id=config["client_id"],
        client_secret=config["client_secret"],
        domain=config["domain"],
    )
    log(f"Listing available {topic_filter or ''} GCN topics:")
    for topic in consumer.list_topics().topics:
        if not topic_filter or topic_filter in topic:
            log(f"        {topic}")
    log("")


def gcn_notices_consumer(topics=None, offset='latest', testing_mode=None):
    """
    Consume GCN notices from specified topics and print them in a human-readable format.

    Parameters
    ----------
    topics : list of str, optional
        List of topics to subscribe to. If None, defaults to the configured topic and its heartbeat topic.
    offset : str, optional
        Offset reset policy for the consumer. Defaults to 'latest'. Other options include 'earliest' and 'none'.
    testing_mode : bool, optional
        If True, connects to the testing Kafka cluster. Defaults to None.
    """
    kafka_config = get_gcn_kafka_config(testing_mode)
    consumer = Consumer(
        client_id=kafka_config["client_id"],
        client_secret=kafka_config["client_secret"],
        domain=kafka_config["domain"],
        config={
            'auto.offset.reset': offset
        }
    )

    topic_list = consumer.list_topics().topics
    topics_to_consume = []
    for topic in topics or [kafka_config["topic"], kafka_config["heartbeat_topic"]]:
        if not any(available_topic == topic for available_topic in topic_list):
            log(f"{YELLOW}Topic '{topic}' not found in available topics. Skipping.{ENDC}")
        else:
            topics_to_consume.append(topic)

    if not topics_to_consume:
        log(f"{RED}No valid topics to consume. Exiting.{ENDC}")
        return

    consumer.subscribe(topics_to_consume)
    log(f"Subscribed to topic: {topics_to_consume}")
    while True:
        for message in consumer.consume(timeout=1):
            if message.error():
                log(f"{RED}{message.error()}{ENDC}")
                continue
            print("\n----------------------------------\n")
            log(f'topic={message.topic()}, offset={message.offset()}')

            try:
                value = message.value().decode("utf-8")
                data = json.loads(value)
                print(json.dumps(data, indent=2))

            except Exception as e:
                log(f"{RED}Failed to parse JSON: {e}{ENDC}")
                log(f"{RED}Raw message value: {message.value()}{ENDC}")