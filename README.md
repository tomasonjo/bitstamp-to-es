# bitstamp-to-es

pip install -r requirements.txt

A python service that:

1. get_trades.py : listens to bitstamp live trades websocket and stores it to ES
2. get_mv_average.py : exports average price of last 5 hours of transactions and stores it to ES