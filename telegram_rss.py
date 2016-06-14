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
rows = get_query_results(412, 'e5aef04beb8fc9ca144e6dc73fa4ae76c4796e43')
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
	if str(r.get(u'publication_id'))+"_"+str(r.get(u'entity_id'))+"_"+str(r.get(u'volume')) not in sent_id_list_only:
		
		#if not sent already, create the message
		text_to_send = u''
		if r.get(u'full_publisher') is not None:
			text_to_send += u'מפרסם: ' + r.get(u'full_publisher') 
		if r.get(u'where_money_go_name') is not None:
				text_to_send += u'%0A%0Aספק: ' + r.get(u'where_money_go_name')
		if r.get(u'description') is not None:
				text_to_send += u'%0A%0Aמהות ההתקשרות: ' + r.get(u'description')
		if r.get(u'decision') is not None:
				text_to_send += u'%0A%0Aסטטוס החלטה: ' + r.get(u'decision') 
		if r.get(u'regulation') is not None:
			text_to_send += u'%0A%0Aתקנה: ' + r.get(u'regulation')
		if r.get(u'volume') is not None and r.get(u'source_currency') is not None:
				text_to_send += u'%0A%0Aהיקף כספי: ' +  unicode("{:,}".format(r.get(u'volume'))) + u' ' + r.get(u'source_currency')
		if r.get(u'start_date') is not None:
				text_to_send += u'%0A%0Aתחילת התקשרות: ' + r.get(u'start_date')
		if r.get(u'end_date') is not None:
				text_to_send += u'%0A%0Aסוף התקשרות: ' + r.get(u'end_date')
		if r.get(u'entity_id') is not None and r.get(u'entity_id') <> u'0':
			text_to_send += u'%0A%0Aלינק: ' + u'http://www.obudget.org/#entity/'+unicode(r.get(u'entity_id'))+'/publication/'+unicode(r.get(u'publication_id'))
		else:
			text_to_send += u'%0A%0Aלינק: ' + u'http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID='+unicode(r.get(u'publication_id'))
		text_to_send = text_to_send.replace(' ', '%20')
		text_to_send = text_to_send.encode('utf-8')
		
		
		#sending  to telegram
		
		#the super group of all publishers
		url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001059426333&text='+text_to_send
		result = urllib.urlopen(url_adress)
		
		#if for each publisher
		if r.get(u'publisher').strip() == u'רשות מקרקעי ישראל':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=@-1001066530366&text='+text_to_send
			result = urllib.urlopen(url_adress)
		
		if r.get(u'publisher').strip() == u'מתאם פעולות הממשלה בשטחים':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001055675892&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'משרד ראש הממשלה':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001069447038&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'משרד התשתיות הלאומיות, האנרגיה והמים':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001056031481&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'משרד התקשורת':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001065370332&text='+text_to_send
			result = urllib.urlopen(url_adress)
		
		if r.get(u'publisher').strip() == u'משרד התיירות':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001064447266&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'משרד התחבורה והבטיחות בדרכים':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001066981703&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'משרד הרווחה':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001056438904&text='+text_to_send
			result = urllib.urlopen(url_adress)
		
		if r.get(u'publisher').strip() == u'משרד הפנים':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001061668988&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'משרד המשפטים':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001055602734&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'משרד המדע התרבות והספורט':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001057314432&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'משרד הכלכלה':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001066482093&text='+text_to_send
			result = urllib.urlopen(url_adress)
		
		if r.get(u'publisher').strip() == u'משרד החקלאות ופיתוח הכפר':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001066401725&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'משרד החינוך':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001063315751&text='+text_to_send
			result = urllib.urlopen(url_adress)
		
		if r.get(u'publisher').strip() == u'משרד החוץ':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001052816063&text='+text_to_send
			result = urllib.urlopen(url_adress)
		
		if r.get(u'publisher').strip() == u'משרד הבריאות':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001054458597&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'משרד הבינוי' or r.get(u'publisher').strip() == u'משרד הבינוי והשיכון':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001051955693&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'משרד האוצר':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001058412143&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'המשרד לאזרחים ותיקים' :
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001034718266&text='+text_to_send
			result = urllib.urlopen(url_adress)
		
		if r.get(u'publisher').strip() == u'הרשות לשירותים ציבוריים':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001056907676&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'המשרד לשיתוף פעולה אזורי':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001050803157&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'המשרד לשירותי דת':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001063222409&text='+text_to_send
			result = urllib.urlopen(url_adress)
		
		if r.get(u'publisher').strip() == u'המשרד לקליטת העליה':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001056926669&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'המשרד לפיתוח הנגב והגליל':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=:-1001052835690&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'המשרד לעניני מודיעין':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001051211436&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'המשרד לירושלים והתפוצות':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001052483462&text='+text_to_send
			result = urllib.urlopen(url_adress)
		
		if r.get(u'publisher').strip() == u'המשרד להגנת הסביבה':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001041701113&text='+text_to_send
			result = urllib.urlopen(url_adress)
		
		
		if r.get(u'publisher').strip() == u'המשרד לבטחון פנים' or r.get(u'publisher').strip() == u'המשרד לביטחון פנים':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001068199662&text='+text_to_send
			result = urllib.urlopen(url_adress)
			
		if r.get(u'publisher').strip() == u'ההסתדרות הציונית העולמית':
			url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001065174350&text='+text_to_send
			result = urllib.urlopen(url_adress)
		
		#add to log file
		sent_id_list_only.append(str(r.get(u'publication_id'))+"_"+str(r.get(u'entity_id'))+"_"+str(r.get(u'volume')))
		time.sleep(1)

#read the query from the procurement
rows = get_query_results(571, '873d2d8a3ffbc0cba37103d3d330408d2bb1799')
	
#run among the query results
for r in rows:
	#check if sent already (if exists in log file)
	
	if str(r.get(u'report_title')) not in sent_id_list_only:
		text_to_send = u'התפרסם דו"ח בשם '
		text_to_send += r.get(u'report_title') 
		text_to_send += u'. קישור:%0A%0Ahttps://foi.gov.il/he/search/site/'	
		text_to_send = text_to_send.replace(' ', '%20')
		text_to_send = text_to_send.encode('utf-8')
		
		url_adress = 'https://api.telegram.org/bot239254631:AAGwWlTJ152r07_ZLZELA5P8Bh3dTKQzqDk/sendmessage?chat_id=-1001059426333&text='+text_to_send
		result = urllib.urlopen(url_adress)
			
		#add to log file
		sent_id_list_only.append(str(r.get(u'report_title')))
		time.sleep(1)

#how many sent in this script running
print "SENT", len(sent_id_list_only) - start_number 

#write to log file
RESULT = sent_id_list_only
resultFile = open(fn,'wb')
wr = csv.writer(resultFile, dialect='excel')
for item in RESULT:
     wr.writerow([item,])
