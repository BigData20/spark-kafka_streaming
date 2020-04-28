from kafka import KafkaProducer, KafkaConsumer
import json


class Kafka_Consumer(object):

    def __init__(self, kafka_brokers, zookeeper_connect, topic, group_id):
        self.consumer = Kafka_Consumer(
            topic,
            bootstrap_servers=kafka_brokers,
            zookeeper_connect=zookeeper_connect,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))
