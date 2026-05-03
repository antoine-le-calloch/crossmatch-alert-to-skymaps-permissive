import argparse

from gcn.consume_gcn_notices import list_gcn_topics, gcn_notices_consumer

parser = argparse.ArgumentParser(description="Consume BOOM GCN notices.")
parser.add_argument(
    '--offset',
    choices=['latest', 'earliest', 'none'],
    default='latest',
    help="Offset reset policy for the Kafka consumer (default: latest).",
)
parser.add_argument(
    '--topics',
    nargs='+',
    help="List of Kafka topics to subscribe to. "
         "Defaults to the configured topic (GCN_KAFKA_TOPIC, or GCN_KAFKA_TEST_TOPIC with --testing) and its heartbeat topic.",
)
parser.add_argument(
    '--testing',
    action='store_true',
    help="Use testing mode configuration for Kafka connection (GCN_KAFKA_TEST_*). Default is False (production config).",
)
parser.add_argument(
    '--list-topics',
    action='store_true',
    help="List available GCN BOOM topics and exit.",
)
args = parser.parse_args()

if args.list_topics:
    list_gcn_topics(topic_filter='boom', testing_mode=args.testing)
else:
    gcn_notices_consumer(topics=args.topics, offset=args.offset, testing_mode=args.testing)
