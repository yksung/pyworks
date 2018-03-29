# -*- coding: utf-8 -*- 
'''
Created on 2017. 6. 13

@author: Holly
'''
import json
import http, urllib
from datetime import datetime as dt
import hashlib, math
import re
import com.wisenut.dao.esclient as esclient
import com.wisenut.dao.mariadbclient as db
import com.wisenut.dao.teaclient as teaclient
from com.wisenut.dao.esclient import es_ip, es_port
import com.wisenut.related_word_extractor as rwe
import copy
import logging
from com.wisenut.related_word_extractor import related_word_extractor
from sys import argv
import time
import random
from xml.etree.ElementTree import ParseError
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch import exceptions
from urllib.parse import quote
from com.wisenut.utils.http_util import urlEncodeNonAscii

import xml.etree.ElementTree as et
from com.wisenut.config import Config
import codecs
import sys
import asyncio
############# logger 세팅
conf = Config()
logging.config.fileConfig(conf.get_logconfig_path())
logger = logging.getLogger(__name__)

############# 감성분석 세팅
bica_host = "211.39.140.67"
bica_port = "21000"

tousflux_host = "52.78.92.26"
tousflux_port = 9090
tousflux_conn = http.client.HTTPConnection(tousflux_host, tousflux_port)

sentiment_score = {
	"POSITIVE" : 100,
	"NEGATIVE" : -100,
	"ETC" : 0
}

PAGE_SIZE=10

def insert_related_words():
	query = {
	  "query": {
	    "bool": {
	      "must_not": [
	        {
	          "exists": {
	            "field": "depth3"
	          }
	        }
	      ]
	    }
	  }
	}
	
	es_conn.request("GET", "/dmap_test/topics/_search", json.dumps(query), {"Content-Type":"application/json"})
	no_related = json.loads(es_conn.getresponse().read())
	
	if 'hits' in no_related and no_related['hits']['total']>0 :
		for topic in no_related['hits']['hits']:
			term = topic['_source']['topic']
			parent_docid = topic['_parent']
			
			related_words = related_word_extractor(parent_docid, term, False)
			if related_words: # 연관키워드가 있으면
				topic['_source']['related_words'] = related_words 
				
				es_conn.request("POST", "/dmap/topics/"+topic['_id']+"?parent="+parent_docid, json.dumps(query), {"Content-Type":"application/json"})


async def make_bulk(class_term_tuple, data):
	term = class_term_tuple[1]
	topic_class = class_term_tuple[0]
	
	inline_script = ""
	inline_script+="ctx._source.project_seq?.addAll(params.project_seq); Set hs = new HashSet(); hs.addAll(ctx._source.project_seq); ctx._source.project_seq.clear(); ctx._source.project_seq.addAll(hs);"
	inline_script+="ctx._source.topic_id=params.topic_id;"
	inline_script+="ctx._source.topic=params.topic;"
	inline_script+="ctx._source.topic_attr=params.topic_attr;"
	inline_script+="ctx._source.topic_class=params.topic_class;"
	inline_script+="ctx._source.related_words=params.related_words;"
	inline_script+="ctx._source.doc_id=params.doc_id;"
	inline_script+="ctx._source.doc_datetime=params.doc_datetime;"
	inline_script+="ctx._source.doc_url=params.doc_url;"
	inline_script+="ctx._source.doc_writer=params.doc_writer;"
	inline_script+="ctx._source.doc_title=params.doc_title;"
	inline_script+="ctx._source.doc_content=params.doc_content;"
	inline_script+="ctx._source.depth1_seq=params.depth1_seq;"
	inline_script+="ctx._source.depth2_seq=params.depth2_seq;"
	inline_script+="ctx._source.depth3_seq=params.depth3_seq;"
	inline_script+="ctx._source.depth1_nm=params.depth1_nm;"
	inline_script+="ctx._source.depth2_nm=params.depth2_nm;"
	inline_script+="ctx._source.depth3_nm=params.depth3_nm;"
	
	if len(term.strip())>0:
		topic_dict = {}
		
		topic = term.split(teaclient.WEIGHT_DELIMITER)[0]
		topic_id = md5Generator([data["_id"], topic])
		topic_attr = '' #esclient.get_topic_attr(topic)
		
		topic_dict =  {
			"topic_id" : topic_id,
			"topic" : topic,
			"topic_attr" : topic_attr,
			"topic_class" : 'NN' if topic_class == 'NN' else 'VV',
			"related_words" : related_word_extractor(data['_id'], topic) if topic_class == 'NN' else '',
			"doc_id" : data['_id'],
			"doc_datetime" : data['_source']['doc_datetime'],
			"doc_url" : data['_source']['doc_url'],
			"doc_writer" : data['_source']['doc_writer'],
			"doc_title" : data['_source']['doc_title'],
			"doc_content" : data['_source']['doc_content'],
			"depth1_seq" : data['_source']['depth1_seq'],
			"depth2_seq" : data['_source']['depth2_seq'],
			"depth3_seq" : data['_source']['depth3_seq'],
			"depth1_nm" : data['_source']['depth1_nm'],
			"depth2_nm" : data['_source']['depth2_nm'],
			"depth3_nm" : data['_source']['depth3_nm'],
			"project_seq" : data['_source']['project_seq']
		}
		
		bulk_header = { "update" : {"_id":"","_type":"doc","_index":"topics","_retry_on_conflict":3} }
		bulk_body ={
			"script" : {
		        "inline": inline_script,
		        "lang": "painless",
		        "params" : None 
		    },
		    "upsert" : None
		}
		
		bulk_header['update']['_id'] = topic_id
		bulk_body['script']['params'] = topic_dict
		#bulk_body['script']['params']["project_seq"] = project_seq
		bulk_body['upsert'] = topic_dict
		#bulk_body['upsert']['project_seq'] = [project_seq]
		
	await asyncio.sleep(1)
	return (bulk_header, bulk_body)

async def time_log():
	i = 0
	print("time log starts.")
	while True:
		await asyncio.sleep(1)
		i += 1
		print('...%02d sec.' % (i,))

async def insert_topics(data):
	#some_bulks = []
	
	try:
		result = teaclient.request(data)
		#print(result)
		
		root = et.fromstring(result)
		status = root.findall("./results/result[@name='status']")[0].text if len(root.findall("./results/result[@name='status']"))>0 else ''
		print(">>> Tea client response : %s" % status)
		
		if status == "success" and len(root.findall("./results/result[@name='keywords']"))>0:
			result_scd = root.findall("./results/result[@name='keywords']")[0].text
			
			terms=""
			verbs=""
			for line in result_scd.split("\n"):
				if line.startswith("<TERMS>"):
					terms = line.replace("<TERMS>", "") # 하늘:387^테스트:14^도움:11
				elif line.startswith("<VERBS>"): 
					verbs = line.replace("<VERBS>", "") # 하늘:387^테스트:14^도움:11
				#print("### terms : %s" % terms)
			
			# <TERMS>
			t = asyncio.ensure_future(time_log())
			terms = [ ('NN', term) for term in terms.split(teaclient.ITEM_DELIMITER)]
			verbs = [ ('VV', verb) for verb in verbs.split(teaclient.ITEM_DELIMITER)]
						
			fts = [ asyncio.ensure_future(make_bulk(t, data)) for t in (terms+verbs) ]
			
			result = await asyncio.gather(*fts)
			t.cancel()
			
			print(result)
					
	except UnicodeDecodeError as decode_error:
		logger.error("[insert_topics] TeaClient failed. (%s)"%str(decode_error))
	except ParseError as xmlerror:
		logger.error("[insert_topics] TeaClient failed. (%s)"%str(xmlerror))
		
	#return some_bulks
	
def insert_emotions(data, project_seq):
	#customer_id = data['_source']['customer_id'] <======== 여기 수정해야함!~!!
	title = data['_source']['doc_title']
	content = data['_source']['doc_content']
	
	bica_info = db.get_bicainfo(project_seq)
	
	bica_conn = http.client.HTTPConnection(bica_info[0], bica_info[1])
	bica_conn.connect()

	if bica_info[2] != 0:
		bica_request = { "data" : title+" "+content, "conceptID" : bica_info[2]  } # 이 데이터를 빅애널라이저에 분석 요청
	else:
		bica_request = { "data" : title+" "+content }
	
	bica_conn.request("POST", "/request.is?"+urllib.parse.urlencode(bica_request, 'utf-8'), "", { "Content-Type" : "application/json" })
	#print("/request.is?"+urllib.parse.urlencode(bica_request, 'utf-8'))
			
	add_query ={
		"script" : {
	        "inline": """
	        	ctx._source.project_seq?.add(params.project_seq);
		        ctx._source.emotion_id=params.emotion_id;
				ctx._source.emotion_type=params.emotion_type;
				ctx._source.emotion_score=params.emotion_score;
				ctx._source.kma=params.kma;
				ctx._source.conceptlabel=params.conceptlabel;
				ctx._source.lsp=params.lsp;
				ctx._source.sentence=params.sentence;
				ctx._source.matched_text=params.matched_text;
				ctx._source.variables=params.variables;
				ctx._source.categories=params.categories;
				ctx._source.conceptlevel1=params.conceptlevel1;
				ctx._source.conceptlevel2=params.conceptlevel2;
				ctx._source.conceptlevel3=params.conceptlevel3;
				ctx._source.begin_offset=params.begin_offset;
				ctx._source.emotional_words=params.emotional_words;
	        """,
	        "lang": "painless",
	        "params" : None 
	    },
	    "upsert" : None
	}
	
	bica_result = json.loads(bica_conn.getresponse().read())
	for result in bica_result['result']:
		# doc_datetime + customer_id + doc_id + lsp + matched_text.begin을 조합해 md5 만듦
		emotion_id = md5Generator([
			data['_id'],
			result['lsp'],
			result['matched_text']['begin']
		])
		
		es_insert_req = {}
		es_insert_req['emotion_id'] = emotion_id
		es_insert_req['emotion_type'] = get_emotion_type(emotion_id, result['sentence']['string']) # 골든플래닛 모듈에서 감정 리턴
		es_insert_req['emotion_score'] = 0.0
		es_insert_req['conceptlabel'] = result['info']['conceptlabel']
		es_insert_req['kma'] = result['kma']#.encode('utf-8')
		es_insert_req['lsp'] = result['lsp']#.encode('utf-8')
		
		es_insert_req['sentence'] = {}
		es_insert_req['sentence']['string'] = result['sentence']['string']#.encode('utf-8')
		es_insert_req['sentence']['offset'] = result['sentence']['offset']
		es_insert_req['sentence']['index'] = result['sentence']['index']
		#es_insert_req['sentence']['neighborhoods'] = result['sentence']['neighborhoods']
		
		es_insert_req['matched_text'] = {}
		es_insert_req['matched_text']['string'] = result['matched_text']['string']#.encode('utf-8')
		es_insert_req['matched_text']['begin'] = result['matched_text']['begin']
		es_insert_req['matched_text']['end'] = result['matched_text']['end']
		
		es_insert_req['variables'] = result['variables']
		es_insert_req['categories'] = result['categories']
				
		#es_insert_req['conceptlevel1'] = result['sentinfo'][0]['infoset'].split('>')[0] if( len(bica_result['sentinfo'][0]['infoset'].split('>'))>0 ) else ''
		#es_insert_req['conceptlevel2'] = result['sentinfo'][0]['infoset'].split('>')[1] if( len(bica_result['sentinfo'][0]['infoset'].split('>'))>1 ) else ''
		#es_insert_req['conceptlevel3'] = bica_result['sentinfo'][0]['infoset'].split('>')[2] if( len(bica_result['sentinfo'][0]['infoset'].split('>'))>2 ) else ''
		es_insert_req['begin_offset'] = 0
		
		add_query['script']['params'] = es_insert_req
		add_query['script']['params']['project_seq'] = project_seq
		add_query['upsert'] = es_insert_req
		add_query['upsert']['project_seq'] = [project_seq]
		
		es_conn.request("POST", "/dmap/emotions/"+emotion_id+"/_update?parent="+data['_id'], json.dumps(add_query), {"Content-Type":"application/json"})
		
		
def get_emotion_type(docid, sentence, instance_number):
	prefix_p = re.compile("^E")
	
	#tousflux_conn.connect()
	tousflux_url = "/SC_EvaluationService.svc/"+str(instance_number).rjust(2, '0')
	
	# 골든플래닛 모듈에 연결해서 감정 리턴 (comment 단위)
	#param_encoded = urllib.parse.urlencode({'authinit': 'WISENUT01_TC0001_'+docid, 'sentence' : sentence}, 'utf-8')
	param_encoded = urllib.parse.urlencode({'authinit': 'auth', 'sentence' : sentence}, 'utf-8')
	#print("tousflux_url >>> " + tousflux_url)
	#print("param_encoded >>> "+param_encoded)
	
	tousflux_conn.request("GET", tousflux_url+'?'+param_encoded) # 골든플래닛 모듈에 연결해서 감정 리턴
	
	emotion_result = str(tousflux_conn.getresponse().read())
	if len(emotion_result.split("|"))>=5 and emotion_result.split("|")[3]!="":
		return emotion_result.split("|")[3]
	else:
		return "NO RESULT"	
			
def get_current_datetime():
	current = str(dt.now())
	ymdhms = str(dt.now().strftime('%Y%m%d%H%M'))
	
	return ymdhms
	
def md5Generator(arr):
	m = hashlib.md5()
	#for e in arr:
	m.update(repr(arr).encode('utf-8'))
	return m.hexdigest()
	
def get_current_date():
	current = str(dt.now())
	ymd = str(dt.now().strftime('%Y%m%d'))
	
	return ymd

def get_current_time():
	current = str(dt.now())
	hms = str(dt.now().strftime('%H%M%S'))
	
	return hms

def delete_by_emotions(parent_id, project_seq):
	delete_emotions_request = {
		"query": {
	        "parent_id" : {
	            "type" : "emotions",
	            "id" : parent_id
	        }
	    }
	}
	
	es_conn.request("GET", "/dmap/emotions/_search", json.dumps(delete_emotions_request), { "Content-Type" : "application/json" })
	
	result = json.loads(es_conn.getresponse().read())
	if 'hits' in result and result['hits']['total']>0:
		for emotion in result['hits']['hits']:
			emotion_id = emotion['_id']
			remove_query = {
				"script" : {
			        "inline": """
			        	if(ctx._source.project_seq?.indexOf(params.project_seq)>0) ctx._source.project_seq?.remove(ctx._source.project_seq?.indexOf(params.project_seq));
			        	ctx._source.emotion_id=params.emotion_id;
						ctx._source.emotion_type=params.emotion_type;
						ctx._source.emotion_score=params.emotion_score;
						ctx._source.kma=params.kma;
						ctx._source.conceptlabel=params.conceptlabel;
						ctx._source.lsp=params.lsp;
						ctx._source.sentence=params.sentence;
						ctx._source.matched_text=params.matched_text;
						ctx._source.variables=params.variables;
						ctx._source.categories=params.categories;
						ctx._source.conceptlevel1=params.conceptlevel1;
						ctx._source.conceptlevel2=params.conceptlevel2;
						ctx._source.conceptlevel3=params.conceptlevel3;
						ctx._source.begin_offset=params.begin_offset;
						ctx._source.emotional_words=params.emotional_words;
			        """,
			        "lang": "painless",
			        "params" : {
						"emotion_id": emotion['_source']["emotion_id"],
						"emotion_type": emotion['_source']["emotion_type"],
						"emotion_score": emotion['_source']["emotion_score"],
						"kma": emotion['_source']["kma"],
						"conceptlabel": emotion['_source']["conceptlabel"],
						"lsp": emotion['_source']["lsp"],
						"sentence": emotion['_source']["sentence"],
						"matched_text": emotion['_source']["matched_text"],
						"variables": emotion['_source']["variables"],
						"categories": emotion['_source']["categories"],
						"conceptlevel1": emotion['_source']["conceptlevel1"],
						"conceptlevel2": emotion['_source']["conceptlevel2"],
						"conceptlevel3": emotion['_source']["conceptlevel3"],
						"begin_offset": emotion['_source']["begin_offset"],
						"emotional_words": emotion['_source']["emotional_words"],
			            "project_seq" : project_seq
			        }
			    }
			}
			
			es_conn.request("POST", "/dmap/emotions/"+emotion_id+"/_update?parent="+parent_id, json.dumps(remove_query), { "Content-Type" : "application/json" })


def delete_by_topics(parent_id, project_seq):
	delete_topic_request = {
		"query": {
	        "parent_id" : {
	            "type" : "topics",
	            "id" : parent_id
	        }
	    }
	}
	
	es_conn.request("GET", "/dmap/topics/_search", json.dumps(delete_topic_request), { "Content-Type" : "application/json" })
	
	result = json.loads(es_conn.getresponse().read())
	if 'hits' in result and result['hits']['total']>0:
		for topic in result['hits']['hits']:
			topic_id = topic['_id']
			remove_query = {
				"script" : {
			        "inline": """
			        	if(ctx._source.project_seq?.indexOf(params.project_seq)>0) ctx._source.project_seq?.remove(ctx._source.project_seq?.indexOf(params.project_seq));
			        	ctx._source.topic_id=params.topic_id;
			        	ctx._source.topic=params.topic;
			        	ctx._source.topic_attr=params.topic_attr;
			        	ctx._source.topic_class=params.topic_class;
			        	ctx._source.related_words=params.related_words;
			        """,
			        "lang": "painless",
			        "params" : {
						"topic_id" : topic['_source']['topic_id'],
						"topic" : topic['_source']['topic'],
						"topic_attr" : topic['_source']['topic_attr'],
						"topic_class" : topic['_source']['topic_class'],
						"related_words" : topic['_source']['related_words'],
			            "project_seq" : project_seq
			        }
			    }
			}
			
			es_conn.request("POST", "/dmap/topics/"+topic_id+"/_update?parent="+parent_id, json.dumps(remove_query), { "Content-Type" : "application/json" })
		

def main(project_seq):
	#1. emotions가 없거나, topics에 값이 없는 crawl_doc 데이터의 개수를 가져옴.
	es_request = {
	  "size" : 1,
	  "query": {
	    "term" : {
		  "project_seq" : project_seq
		}
	  }
	}
	
	es_conn = http.client.HTTPConnection(esclient.es_ip, esclient.es_port)	
	es_conn.request("POST", "/documents/doc/_search", json.dumps(es_request), { "Content-Type" : "application/json" })
	es_result = es_conn.getresponse().read()
	#total = json.loads(es_result)['hits']['total']
	total = 10 # 테스트
	
	if 'hits' not in json.loads(es_result):
		print(es_result)
		exit(1)
	elif total <= 0:
		print(es_result)
		exit(0)
		
	scroll_id = None
	
	# 2. 본격적으로  데이터 스캔
	for pageNo in range(math.ceil(total/PAGE_SIZE)):
		logger.info("<PAGE:%d>"%pageNo)
	#for _ in range(math.ceil(10000/PAGE_SIZE)): # 테스트
		
		# scroll_id 여부에 따라 request 데이터 값과 url 을 바꿈.
		if scroll_id:
			es_request_per_page = {
				"query" : {
					"scroll" : "1m",
					"scroll_id" : scroll_id
				}
			}
			es_url = "/documents/_scroll"
		else:
			es_request_per_page = copy.copy(es_request)
			es_request_per_page['size'] = PAGE_SIZE		# page size를 바꿈
			
			es_url = "/documents/doc/_search?scroll=1m"
		
		# 검색 요청
		logger.debug("[main] es_request_per_page > %s" % json.dumps(es_request_per_page))
		
		es_conn = http.client.HTTPConnection(esclient.es_ip, esclient.es_port)
		es_conn.request("POST", es_url, json.dumps(es_request_per_page), { "Content-Type" : "application/json" })
		es_result = json.loads(es_conn.getresponse().read())
		
		print(json.dumps(es_result))
		
		if 'hits' in es_result:
			scroll_id = es_result['_scroll_id']
			resultlist = es_result['hits']['hits']
			
			topics2upsert = ''
			logger.info("<Total:%d>"%es_result['hits']['total'] )
			for idx, doc in enumerate(resultlist):
				if idx % 100 == 0:
					logger.info("[%d]"%idx)
					
					print("[%d]"%idx, end="")
					sys.stdout.flush()
				else:
					print(".")
					sys.stdout.flush()
				# 2. 만약에 emotions나 topics 중 하나만 있으면 싹 지우고 다시 분석.
				# 지금은 project_seq를 임의로 1로 채우지만 나중에는 변경이 생긴 project에 대한 리스트를 받아와서 다른 project_seq를 넣어줘야함.
				
				#delete_by_topics(doc['_id'], project_seq)
				#delete_by_emotions(doc['_id'], project_seq)
				
				#insert_emotions(doc, project_seq)
				loop = asyncio.get_event_loop()
				loop.run_until_complete(insert_topics(doc))
				loop.close()
				
				#insert_topics(doc)
				'''
				for bulk in some_bulks:
					result_file.write(json.dumps(bulk[0]))
					result_file.write("\n")
					result_file.write(json.dumps(bulk[1]))
					result_file.write("\n")
						
					#es_conn.request("POST", "/_bulk", topics2upsert.replace("<new_line>", "\n"), {"Content-type" : "application/json"})
					#print(es_conn.getresponse().read())
					#topics2upsert = ''
								
				'''



if __name__ == '__main__':
	project_seq = argv[1]
	
	main(project_seq)
	
	