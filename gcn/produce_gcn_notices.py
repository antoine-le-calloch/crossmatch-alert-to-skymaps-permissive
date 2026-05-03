import json
import urllib.request

from astropy.time import Time
from gcn_kafka import Producer
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from utils.gcn import SCHEMA, get_gcn_kafka_config
from utils.logger import log, RED, ENDC


def _retrieve_remote_schema(uri):
    with urllib.request.urlopen(uri) as response:
        return Resource.from_contents(json.load(response))


class GcnProducer:
    def __init__(self, testing_mode=None):
        self.config = get_gcn_kafka_config(testing_mode)
        self.producer = Producer(
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            domain=self.config["domain"],
        )
        self.validator = self._build_validator()

    @staticmethod
    def _build_validator():
        try:
            with urllib.request.urlopen(SCHEMA) as response:
                schema = json.load(response)
            registry = Registry(retrieve=_retrieve_remote_schema)
            return Draft202012Validator(schema, registry=registry)
        except Exception as e:
            log(f"{RED}Failed to build GCN notice payload validator: {e}{ENDC}")
            log(f"{RED}GCN notice payloads will not be validated against the schema before being sent to Kafka.{ENDC}")
            return None

    def produce(self, data, topic=None, validate=True):
        topic = topic or self.config["topic"]
        if validate and self.validator:
            self.validator.validate(data)
        self.producer.produce(topic, json.dumps(data).encode())
        self.producer.flush()

    def heartbeat(self):
        payload = {
            "timestamp": Time.now().isot + "Z",
            "status": "alive"
        }
        self.produce(payload, topic=self.config["heartbeat_topic"], validate=False)