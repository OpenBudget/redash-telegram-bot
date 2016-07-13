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
		
		if r.get(u'publisher') is not None:
			text_to_send += u'*מפרסם:* ' + r.get(u'full_publisher') 
		if r.get(u'supplier') is not None:
				text_to_send += u'%0A%0A*ספק:* ' + r.get(u'where_money_go_name')
		if r.get(u'description') is not None:
			if r.get(u'entity_id') is not None and r.get(u'entity_id') <> u'0':
				text_to_send += u'%0A%0A*נושא:* ' + u'[' + r.get(u'description')[0:120] + u']' + u'(' + 'http://www.obudget.org/#entity/'+unicode(r.get(u'entity_id'))+'/publication/'+unicode(r.get(u'publication_id'))+u')'
			else:
				text_to_send += u'%0A%0A*נושא:* ' + u'[' + r.get(u'description')[0:120] + u']' + u'(' + 'http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID='+unicode(r.get(u'publication_id'))+u')'
			#text_to_send += u'%0A%0A*נושא*: ' + r.get(u'description')[0:100] + u' ... '
		if r.get(u'decision') is not None:
				text_to_send += u'%0A%0A*סטאטוס:* ' + r.get(u'decision') 
		#if r.get(u'regulation') is not None:
		#	text_to_send += u'%0A%0Aתקנה: ' + r.get(u'regulation')
		if r.get(u'volume') is not None and r.get(u'source_currency') is not None:
				text_to_send += u'%0A%0A*היקף:* ' +  unicode("{:,}".format(r.get(u'volume'))) + u' ' + r.get(u'source_currency')
		
				
		text_to_send = text_to_send.replace(' ', '%20')
		text_to_send = text_to_send.encode('utf-8')
		
		#send
		url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001058537523&parse_mode=markdown&text='+text_to_send
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
