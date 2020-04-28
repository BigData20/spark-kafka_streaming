import asyncio
import logging
import signal
import time

from kafka_streams import KafkaStreams

loop = asyncio.get_event_loop()


class Main(object):

    # client = aiohttp.ClientSession(loop=loop)

    def __init__(self):
        self.kafkastreams_1 = KafkaStreams(["localhost:9092"])
        signal.signal(signal.SIGINT, self.signal_handler)

    def run(self, producer_id):
        starttime = time.time()
        for i in range(100):
            self.kafkastreams_1.send_page_data({'producer' + producer_id: i})
        time.sleep(300.0 - ((time.time() - starttime) % 300.0))

    def signal_handler(signal, frame):
        loop.stop()


if __name__ == "__main__":
    logging.info("Initializing Kafka Producer")
    main = Main()
    asyncio.ensure_future(main.run('1'))
    asyncio.ensure_future(main.run('2'))
    asyncio.ensure_future(main.run('3'))
    loop.run_forever()
