import logging
from typing import Dict, List, Any
from sklearn.datasets import fetch_20newsgroups

logger = logging.getLogger(__name__)


class NewsGroupsDAL:
    """
    Reads data from the 20 Newsgroups dataset and manages state.
    CHANGED: Now tracks index per category to be stateless and resilient to restarts.
    """

    def __init__(self):
        logger.info("Initializing NewsGroupsDAL...")
        self.categories_map = {
            "interesting": [
                'alt.atheism', 'comp.graphics', 'comp.os.ms-windows.misc',
                'comp.sys.ibm.pc.hardware', 'comp.sys.mac.hardware', 'comp.windows.x',
                'misc.forsale', 'rec.autos', 'rec.motorcycles', 'rec.sport.baseball'
            ],
            "not_interesting": [
                'rec.sport.hockey', 'sci.crypt', 'sci.electronics', 'sci.med',
                'sci.space', 'soc.religion.christian', 'talk.politics.guns',
                'talk.politics.mideast', 'talk.politics.misc', 'talk.religion.misc'
            ]
        }
        self.data_cache: Dict[str, List[str]] = {}
        self.category_indices: Dict[str, int] = {}
        self._load_data()
        logger.info("NewsGroupsDAL initialized successfully.")

    def _load_data(self):
        """Loads all data into memory and initializes indices."""
        for topic, categories in self.categories_map.items():
            for category in categories:
                logger.debug(f"Loading category: {category}")
                dataset = fetch_20newsgroups(subset='all', categories=[category],
                                             remove=('headers', 'footers', 'quotes'))
                self.data_cache[category] = dataset.data
                self.category_indices[category] = 0

    def get_next_batch(self, count: int = 1) -> List[Dict[str, Any]]:
        """
        Gets the next batch of messages, one from each category.
        Maintains an index to continue from where it left off.
        """
        results = []
        for _ in range(count):
            has_new_data = False
            for topic, categories in self.categories_map.items():
                for category in categories:
                    current_index = self.category_indices.get(category, 0)
                    if current_index < len(self.data_cache[category]):
                        message = self.data_cache[category][current_index]
                        results.append({
                            "topic": topic,
                            "payload": {
                                "category": category,
                                "data": message
                            }
                        })
                        self.category_indices[category] += 1
                        has_new_data = True

        if not has_new_data and results == []:
            logger.warning("All newsgroups data has been published.")
            return None  # Signal that we're done

        return results