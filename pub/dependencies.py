import os

KAPKA_URL = os.environ.get('KAPKA_URL', "localhost")
KAPKA_PORT = int(os.environ.get('KAPKA_PORT', 9092))

