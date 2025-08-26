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

    def send_data(self):
        """
        Send all data types to Kafka.
        """
        logger.info("Starting data send process")
        self._get_not_interesting()
        self._get_interesting()
        logger.info("Data send process complete")

    def _get_not_interesting(self):
        """
        Get not interesting data and send to Kafka.
        """
        logger.debug("Processing not interesting data")

        try:
            not_interesting = self.data.get_not_interesting()
            count = 0
            for message in not_interesting:
                self.producer.send("not_interesting", message)
                count += 1
            logger.info(f"Sent {count} not interesting messages")
        except Exception as e:
            logger.error(f"Error processing not interesting data: {e}")
            raise

    def _get_interesting(self):
        """
        Get interesting data and send to Kafka.
        """
        logger.debug("Processing interesting data")

        try:
            interesting = self.data.get_interesting()
            count = 0
            for message in interesting:
                self.producer.send("interesting", message)
                count += 1
            logger.info(f"Sent {count} interesting messages")
        except Exception as e:
            logger.error(f"Error processing interesting data: {e}")
            raise