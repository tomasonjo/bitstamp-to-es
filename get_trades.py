import time
import sys
import logging
import json
from datetime import datetime

import pysher
from elasticsearch import Elasticsearch


# pysher logging
root = logging.getLogger()
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
root.addHandler(ch)

# Connect to bitstamp trades websocket
pusher = pysher.Pusher('de504dc5763aeef9ff52')

# Connect to ES
es = Elasticsearch()

# Save to ES
def save_to_es(*args):
    data = json.loads(args[0])
    id = data.pop('id')
    data['timestamp'] = datetime.utcfromtimestamp(float((data['timestamp'])))
    print(data)
    es.index(index="trades", id=id, doc_type='trade', body=data)

# We can't subscribe until we've connected, so we use a callback handler
# to subscribe when able
def connect_handler(data):
    channel = pusher.subscribe('live_trades')
    channel.bind('trade', save_to_es)

# Subscribe to websocket and store response in ES
pusher.connection.bind('pusher:connection_established', connect_handler)
pusher.connect()

while True:
    time.sleep(1)

