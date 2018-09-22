import requests
from elasticsearch import Elasticsearch
from datetime import datetime
from dateutil.relativedelta import relativedelta

current_time = datetime.utcnow()
offset_time = current_time - relativedelta(hours=5)
moving_average_array = []

r = requests.get('https://www.bitstamp.net/api/v2/transactions/btcusd/?time=day')
data = r.json()

es = Elasticsearch()

def get_average(array):
	if len(array) > 0:
		return sum(array) / len(array)
	else:
		return None

for row in data:
	transaction_date = datetime.utcfromtimestamp(float(row['date']))
	if  transaction_date > offset_time:
		id = row.pop('tid')
		row['date'] = datetime.utcfromtimestamp(float((row['date'])))
		row['price'] = float(row['price'])
 		es.index(index="trades", id=id, doc_type='trade', body=row)


while offset_time <= current_time:
	print('starting with' + str(offset_time))
	for row in data: 
		if not isinstance(row['date'], datetime):
			row['date'] = datetime.utcfromtimestamp(float(row['date']))

		if  (offset_time - relativedelta(hours=5)) < row['date'] < offset_time:
			moving_average_array.append(float(row['price']))

	body = {

		"date": offset_time,
		"smv_value": get_average(moving_average_array)
	}

	# Save to ES using current timestamp as id

	res = es.index(index='smv_avg', id=offset_time.strftime('%s'), doc_type='smv_avg', body=body)
	
	# reset loop
	moving_average_array = []
	offset_time = offset_time + relativedelta(minutes=10)