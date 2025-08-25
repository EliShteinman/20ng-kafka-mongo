from sklearn.datasets import fetch_20newsgroups

class DataRead:
    def __init__(self):
        self.interesting_categories = [
            'alt.atheism',
            'comp.graphics',
            'comp.os.ms-windows.misc',
            'comp.sys.ibm.pc.hardware',
            'comp.sys.mac.hardware',
            'comp.windows.x',
            'misc.forsale',
            'rec.autos',
            'rec.motorcycles',
            'rec.sport.baseball',
        ]
        self.not_interesting_categories = [
            'rec.sport.hockey',
            'sci.crypt',
            'sci.electronics',
            'sci.med',
            'sci.space',
            'soc.religion.christian',
            'talk.politics.guns',
            'talk.politics.mideast',
            'talk.politics.misc',
            'talk.religion.misc',
        ]
        self.newsgroups_interesting = fetch_20newsgroups(subset='all', categories=self.interesting_categories)
        self.newsgroups_not_interesting = fetch_20newsgroups(subset='all', categories=self.not_interesting_categories)
        self.interesting = self._get_data(self.newsgroups_interesting.data)
        self.not_interesting = self._get_data(self.newsgroups_not_interesting.data)

    @staticmethod
    def _get_data(data, batch_size=10):
        for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]

    def get_interesting(self,batch_size=None):
        return next(self.interesting, batch_size) if batch_size else next(self.interesting)

    def get_not_interesting(self,batch_size=None):
        return next(self.not_interesting, batch_size) if batch_size else next(self.not_interesting)

