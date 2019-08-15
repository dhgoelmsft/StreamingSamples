## README: For this sample to work, you need an account with iexcloud.io to get access to their financial data APIs
# Replace the TODOs with parameters specific to your setup

# imports
from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka.errors import KafkaError

import sys
import requests
#import json

## Configuration section
iextoken = '' #TODO: Replace with your iexToken
getStockApi = 'https://cloud.iexapis.com/stable/tops/last'
stockList = ['msft', 'ba', 'jnj', 'f', 'tsla', 'bac', 'ge', 'mmm', 'intc', 'wmt']
defaultLoops = 1000
kafkaBrokers = [] #TODO: Replace with your Kafak broker endpoint (including port)
kafkaTopic = 'stockVals'

## Read command line arguments
try:
	loops = int(sys.argv[1])
	print "Number of loops is ", loops
except:
	loops = defaultLoops
	print "No loops argument provided. Default loops are ", loops

## Construct parameters for the REST Request
requestParams = {'token':iextoken, 'symbols':",".join(stockList)}

# Configure Producer
#producer = KafkaProducer(bootstrap_servers=kafkaBrokers,key_serializer=lambda k: k.encode('utf-8'),value_serializer=lambda x: dumps(x).encode('utf-8'))
producer = KafkaProducer(bootstrap_servers=kafkaBrokers,key_serializer=lambda k: k.encode('ascii','ignore'),value_serializer=lambda x: dumps(x).encode('utf-8'))

#### Main execution
# send messages
for x in range(loops):
	## make a rest call from Python to get streaming data
	# construct the request URL
	req = requests.get(url=getStockApi, params=requestParams)
	print(req.json()) 
	for stock in req.json():
		future = producer.send(kafkaTopic, key=stock["symbol"], value=stock)
	   	response = future.get(timeout=10)
		print (response.topic)
	   	print (response.partition)
	   	print (response.offset)
	sleep(1)