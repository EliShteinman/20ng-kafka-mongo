import logging

logger = logging.getLogger(__name__)


class Manager:
    """
    Control data flow from reader to Kafka.
    Takes data from DataRead and sends it using Producer.
    """

    def __init__(self, data_reader, producer):
        """
        Set up the manager.

        Args:
            data_reader: DataRead object that gets news data
            producer: Producer object that sends to Kafka
        """
        self.data_reader = data_reader
        self.producer = producer
        logger.info("Manager ready")

    def send_data(self, count=1):
        """
        Get data and send it to Kafka.

        Args:
            count: How many messages per category to send

        Returns:
            Number of messages sent, or None if no data left
        """
        logger.info(f"Starting data send process, count: {count}")

        # Get data from the reader
        results = self.data_reader.get_data(count)

        if not results:
            logger.info("No more data to send")
            return

        sent_count = 0
        for result in results:
            topic = result["label"]
            for message in result["data"]:
                # Create message with all info
                message_data = {
                    "category": result["category"],
                    "label": result["label"],
                    "data": message,
                }
                # Send to Kafka
                self.producer.send(topic, message_data)
                sent_count += 1
        self.producer.producer.flush()
        logger.info(f"Sent {sent_count} messages to Kafka")
        return sent_count
