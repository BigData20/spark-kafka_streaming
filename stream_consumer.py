import asyncio
import logging
import signal

from kafka_consumer import Kafka_Consumer

loop = asyncio.get_event_loop()


class Main(object):

    def __init__(self):
        self.kafka_consumer = Kafka_Consumer(["localhost:9092"], ['localhost:2181'], "test", "my-group")
        signal.signal(signal.SIGINT, self.signal_handler)

    def run(self, id):
        for message in self.kafka_consumer:
            message = message.value
            print("Consumer{}: {}\n", id, message)

    def signal_handler(signal, frame):
        loop.stop()


if __name__ == "__main__":
    logging.info("Initializing Kafka Consumer")
    main = Main()
    asyncio.ensure_future(Main().run('1'))
    asyncio.ensure_future(Main().run('2'))
    asyncio.ensure_future(Main().run('3'))
    loop.run_forever()
