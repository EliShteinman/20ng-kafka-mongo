import logging

logger = logging.getLogger(__name__)


class Manager:
    """
    Control data flow from reader to Kafka.
    """

    def __init__(self, dal, consumer):
        """
        Setup manager.
        data: DataRead object
        producer: Producer object
        """
        self.dal = dal
        self.consumer = consumer
        logger.info("Manager ready")

    async def get_data_from_mongo(self):
        pass

    async def get_data_from_kafka(self):
        while self.consumer.consume():
            pass
