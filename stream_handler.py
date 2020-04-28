import logging
import time

from kafka_streams import KafkaStreams


class Main(object):
    def __init__(self):
        self.kafkastreams_1 = KafkaStreams()

    def run(self):
        starttime = time.time()
        for i in range(100):
            self.kafkastreams_1.send_page_data({'producer_1': i})
        time.sleep(300.0 - ((time.time() - starttime) % 300.0))


if __name__ == "__main__":
    logging.info("Initializing Kafka Producer")
    main = Main()
    main.run()