class Manager:
    def __init__(self, data, producer):
        self.data = data
        self.producer = producer

    def send_data(self):
        self._get_not_interesting()
        self._get_interesting()

    def _get_not_interesting(self):
        not_interesting = self.data.get_not_interesting()
        for message in not_interesting:
            self.producer.send("not_interesting", message)

    def _get_interesting(self):
        interesting = self.data.get_interesting()
        for message in interesting:
            self.producer.send("interesting", message)
