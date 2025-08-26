import logging

logger = logging.getLogger(__name__)


class Manager:
    """
    Control data flow from reader to Kafka.
    """

    def __init__(self, data, producer):
        """
        Setup manager.
        data: DataRead object
        producer: Producer object
        """
        self.data = data
        self.producer = producer
        logger.info("Manager ready")

    def send_data(self, count=1):
        """
        Send data to Kafka.
        count: how many messages per category
        """
        logger.info(f"Starting data send process, count: {count}")

        results = self.data.get_data(count)

        if not results:
            logger.info("No more data to send")
            return

        sent_count = 0
        for result in results:
            topic = result["label"]
            for message in result["data"]:
                self.producer.send(topic, message)
                sent_count += 1

        logger.info(f"Sent {sent_count} messages to Kafka")
        return sent_count