from kafka import KafkaProducer, KafkaConsumer
import json


class KafkaConsumer(object):

    def __init__(self, kafka_brokers, zookeeper_connect):
        self.consumer = KafkaConsumer(
            'test',
            bootstrap_servers=kafka_brokers,
            zookeeper_connect=zookeeper_connect,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))
