import logging
from sklearn.datasets import fetch_20newsgroups

logger = logging.getLogger(__name__)


class DataRead:
    """
    Read data from 20 newsgroups.
    Split data to interesting and not interesting.
    """

    def __init__(self):
        logger.info("Starting data read setup")

        # Categories we want
        self.interesting_categories = [
            "alt.atheism",
            "comp.graphics",
            "comp.os.ms-windows.misc",
            "comp.sys.ibm.pc.hardware",
            "comp.sys.mac.hardware",
            "comp.windows.x",
            "misc.forsale",
            "rec.autos",
            "rec.motorcycles",
            "rec.sport.baseball",
        ]

        # Categories we don't want
        self.not_interesting_categories = [
            "rec.sport.hockey",
            "sci.crypt",
            "sci.electronics",
            "sci.med",
            "sci.space",
            "soc.religion.christian",
            "talk.politics.guns",
            "talk.politics.mideast",
            "talk.politics.misc",
            "talk.religion.misc",
        ]

        logger.info(f"Loading {len(self.interesting_categories)} interesting categories")
        self.newsgroups_interesting = fetch_20newsgroups(
            subset="all", categories=self.interesting_categories
        )

        logger.info(f"Loading {len(self.not_interesting_categories)} not interesting categories")
        self.newsgroups_not_interesting = fetch_20newsgroups(
            subset="all", categories=self.not_interesting_categories
        )

        self.interesting = self._get_data(self.newsgroups_interesting.data)
        self.not_interesting = self._get_data(self.newsgroups_not_interesting.data)

        logger.info("Data read setup complete")

    @staticmethod
    def _get_data(data, batch_size=10):
        """
        Split data into small groups.
        Default size is 10 items.
        """
        for i in range(0, len(data), batch_size):
            yield data[i: i + batch_size]

    def get_interesting(self, batch_size=None):
        """
        Get next group of interesting data.
        """
        logger.debug("Getting interesting data batch")
        return (
            next(self.interesting, batch_size) if batch_size else next(self.interesting)
        )

    def get_not_interesting(self, batch_size=None):
        """
        Get next group of not interesting data.
        """
        logger.debug("Getting not interesting data batch")
        return (
            next(self.not_interesting, batch_size)
            if batch_size
            else next(self.not_interesting)
        )


if __name__ == "__main__":
    data = DataRead()
    message = data.get_not_interesting()
    for i in message:
        print(i)
    message = data.get_not_interesting()
    for i in message:
        print(i)
    message = data.get_not_interesting()
    for i in message:
        print(i)