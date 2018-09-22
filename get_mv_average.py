import requests
from elasticsearch import Elasticsearch
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Params  
current_time = datetime.utcnow()
offset_time = current_time - relativedelta(hours=5)
moving_average_array = []
index = "smv_avg"

# Connect to ES

es = Elasticsearch()

# Fetch daily btc/usd transactions
r = requests.get('https://www.bitstamp.net/api/v2/transactions/btcusd/?time=day')
data = r.json()

# Calculate average
def get_average(array):
	if len(array) > 0:
		return sum(array) / len(array)
	else:
		return None


print('Average price of time window from {} to {}').format(offset_time, current_time)

for row in data:
	transaction_date = datetime.utcfromtimestamp(float(row['date']))
	if  transaction_date > offset_time:
		moving_average_array.append(float(row['price']))

body = {

	"date": current_time,
	"smv_value": get_average(moving_average_array)
}

# Save to ES using current timestamp as id

res = es.index(index=index, id=current_time.strftime('%s'), doc_type='smv_avg', body=body)

print('Exported smv to ES at: {} with status {}').format(datetime.now(), res)