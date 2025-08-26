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

        self.categories = {
            'alt.atheism': 'interesting',
            'comp.graphics': 'interesting',
            'comp.os.ms-windows.misc': 'interesting',
            'comp.sys.ibm.pc.hardware': 'interesting',
            'comp.sys.mac.hardware': 'interesting',
            'comp.windows.x': 'interesting',
            'misc.forsale': 'interesting',
            'rec.autos': 'interesting',
            'rec.motorcycles': 'interesting',
            'rec.sport.baseball': 'interesting',
            'rec.sport.hockey': 'not_interesting',
            'sci.crypt': 'not_interesting',
            'sci.electronics': 'not_interesting',
            'sci.med': 'not_interesting',
            'sci.space': 'not_interesting',
            'soc.religion.christian': 'not_interesting',
            'talk.politics.guns': 'not_interesting',
            'talk.politics.mideast': 'not_interesting',
            'talk.politics.misc': 'not_interesting',
            'talk.religion.misc': 'not_interesting',
        }
        self._get_categories()
        logger.info("Data read setup complete")

    def get_data(self, count=1):
        results = []
        for _ in range(count):
            for category, label_dict in self.categories.items():
                for label, gen in label_dict.items():
                    try:
                        batch = next(gen)
                        results.append({
                            "category": category,
                            "label": label,
                            "data": batch
                        })
                    except StopIteration:
                        logger.debug(f"No more data for category: {category}")
                        continue
        return results

    def _get_categories(self):
        for category, label in list(self.categories.items()):
            logger.debug(f"Loading category: {category}")
            bunch = fetch_20newsgroups(subset='all', categories=[category])
            self.categories[category] = {
                label: self._create_generator(bunch.data, 1)
            }
    @staticmethod
    def _create_generator(items, batch_size):
        for i in range(0, len(items), batch_size):
            yield items[i:i + batch_size]


if __name__ == "__main__":
    data = DataRead()
    results = data.get_data(3)
    for result in results:
        print(f"Category: {result['category']}")
        print(f"Label: {result['label']}")
        print(f"Data length: {len(result['data'])}")
        print(f"First message preview: {result['data'][0][:50]}...")
        print("-" * 50)