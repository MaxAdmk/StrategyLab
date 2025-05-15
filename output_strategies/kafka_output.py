import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from .base import OutputStrategy

class KafkaOutput(OutputStrategy):
    def __init__(self, config):
        self.topic = config.get("topic", "air_quality")
        self.producer = KafkaProducer(
            bootstrap_servers=config.get("bootstrap_servers", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
        )

    def write(self, data):
        for row in data:
            try:
                future = self.producer.send(self.topic, row)
                result = future.get(timeout=10)  
            except KafkaError as e:
                pass

        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()
