import logging

from kafka_consumer import KafkaConsumer


class Main(object):
    def __init__(self):
        self.kafka_consumer = KafkaConsumer(["localhost:9092"], ['localhost:2181'])


if __name__ == "__main__":
    logging.info("Initializing Kafka Producer")
    main = Main()
