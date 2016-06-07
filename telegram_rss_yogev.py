#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hmac
import os
import requests
import logging
import subprocess
import json
import time
import hashlib
import urllib
import time
import csv
import sys

def sign(key, path, expires):
	if not key:
		return None

	h = hmac.new(str(key), msg=path, digestmod=hashlib.sha1)
	h.update(str(expires))

	return h.hexdigest()

def get_query_results(query_id, secret_api_key, redash_url="http://data.obudget.org"):
	logging.info("Getting Re:dash query {0} with key {1}".format(query_id,secret_api_key))
	path = '/api/queries/{}/results.json'.format(query_id)
	expires = time.time()+900 # expires must be <= 3600 seconds from now
	signature = sign(secret_api_key, path, expires)
	full_path = "{0}{1}?signature={2}&expires={3}&api_key={4}".format(redash_url, path, signature, expires, secret_api_key)
	return requests.get(full_path).json()['query_result']['data']['rows']

#read the query from the redash
rows = get_query_results(553, '6614aaabc8f4479e62df20045dc04f2c59243374')
fn = sys.argv[1]

#read log file
with open(fn, 'rb') as f:
    reader = csv.reader(f)
    sent_id_list = list(reader)

sent_id_list_only = []
for row in sent_id_list:
	sent_id_list_only.append(str(row[0]))
start_number = len(sent_id_list_only)
	
#run among the query results
for r in rows:
	
	#check if sent already (if exists in log file)
	if str(r.get(u'publication_id')) not in sent_id_list_only:
		
		#if not sent already, create the message
		text_to_send = u''
		text_to_send += u'מפרסם: ' + r.get(u'publisher') 
		text_to_send += u'%0A%0Aסכום: ' + unicode("{:,}".format(r.get(u'volume'))) + u' '
		text_to_send += u'%0A%0Aספק: ' + r.get(u'supplier') 
		text_to_send += u'%0A%0Aהחלטה: ' + r.get(u'decision') 
		text_to_send += u'%0A%0Aעילה: ' + r.get(u'regulation')
		text_to_send += u'%0A%0Aלינק: ' + r.get(u'url') 
				
		text_to_send = text_to_send.replace(' ', '%20')
		text_to_send = text_to_send.encode('utf-8')
		
		#send
		url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001058537523&text='+text_to_send
		result = urllib.urlopen(url_adress)
		
		#add to log file
		sent_id_list_only.append(str(r.get(u'publication_id')))
		time.sleep(1)


#how many sent in this script running
print "SENT", len(sent_id_list_only) - start_number 

#write to log file
RESULT = sent_id_list_only
resultFile = open(fn,'wb')
wr = csv.writer(resultFile, dialect='excel')
for item in RESULT:
     wr.writerow([item,])
