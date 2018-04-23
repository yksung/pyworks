# -*- coding : utf-8 -*-
import os
import codecs
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch import exceptions
import http, requests, urllib
from datetime import datetime as dt
import hashlib
import re
import sys, time
import logging
import myLogger
import math
import traceback

#===================== logger 설정 =====================
logger = myLogger.getMyLogger('bmw2.1', hasConsoleHandler=False, hasRotatingFileHandler=True, logLevel=logging.DEBUG)
#===================== logger 설정 =====================

#===================== ES 관련 ===================== 
es_host = "211.39.140.78"
es_port = 9200

INDEX_NAME="bmw_v2.1"
PAGE_SIZE=10000
#========================================================

 
#===================== BICAnalyzer 관련 =====================
bica_host = "211.39.140.106"
bica_port = "21000"

_1_RSA_CONCEPT_ID					= 7 # RSA Concept ID
_2_RSA_GENERAL_SATISFACTION			= 13
_3_OVERALL_RSA_FEEDBACK_CONCEPT_ID	= 10 # Overall RSA Feedback Concept ID
_4_SALES_CONCEPT_ID					= 8 # Sales/After Sales Concept ID
_5_SALES_GENERAL_SATISFACTION		= 12

ETC_CATEGORY_LABEL=re.compile("(가중치|브랜드)")

#========================================================


#===================== 긍부정 분석기 관련 =====================
#tousflux_host = "1.212.186.186"
#tousflux_port = 9099
tousflux_host = "58.229.163.171"
tousflux_port = 9095

sentiment_score = {
	"POSITIVE" : 100,
	"NEGATIVE" : -100,
	"ETC" : 0
}
#========================================================
	
def csv_check(filename):
	for num, line in enumerate(codecs.open(filename, "r", "utf-8")):
		if(len(line.split(";"))!=9):
			print("[Error] Line number {} is wrong format.".format(str(num+1)))
			
			
"""
1. csv2es : 우선 csv 데이터를 Elasticsearch에 넣음.
2. analyze_and_save2es : es에서 검색해서 가져온 값에 대해서 분석을 돌려서 분석 결과를 emotions type으로 저장.
"""



def get_current_datetime():
    ymdhms = str(dt.now().strftime('%Y-%m-%dT%H:%M:%S'))
    

    return ymdhms  






def read_from(path):
	if(os.path.isfile(path)):
		csv2es(path)
	else:
		print("################ {0} is directory. ################".format(path))
		for dirname, dirnames, fileList in os.walk(path):
			for f in fileList:
				filename = os.path.join(dirname, f)
				csv2es(filename)





def csv2es(path):
	es_client=Elasticsearch([{'host': es_host, 'port':es_port}])

	print(">>> CSV to Elasticsearch : {0}.".format(path))
	csv_date = path[path.rfind(os.sep)+1:path.rfind(".")].split("_")
	if(len(csv_date)<5):
		csv_date = [ '', '', get_current_date(), '14', '48', get_current_time() ]
		
	csv_datetime = csv_date[2]+csv_date[5] # 20170413183920
	
	csv_file = codecs.open(path, "r", "utf-8")
	#json_file = codecs.open(os.path.join(os.path.dirname(path), get_current_datetime()+".json"), "w", "utf-8")
	
	actions = []
	count = 0
	total_inserted = 0
	for line in csv_file.readlines():
		count += 1
		if(count==1): continue # 헤더는 건너뜀.
		
		arr = line.split(";")
		
		if(len(line.split(";"))<5):
			print("!!! csv format is not correct.")
			continue
		else:
			try:
				case_id		= arr[0]
				response_id = arr[1]
				answer_id	= arr[2]
				language_id = arr[3]
				comment		= arr[4]
				
				docid = "E{0}_{1}_{2}_{3}".format(case_id, response_id, answer_id, language_id)
				#print(">>>>>> docid : {}".format(docid))
				
				data = {
					"_index" : INDEX_NAME,
					"_type" : "crawl_doc",
					"_id" : docid,
					"_source" : {
						"doc_id" : docid,
						"doc_datetime" : csv_datetime,
						"doc_content" : comment.replace('\r', '').replace('\n', ''),
						"user_id" : "bmw",
						"upd_datetime" : get_current_datetime()
					}
				}
				
				insert_emotions(data) # BICA 분석은 따로 ES에 넣음.
				
				actions.append(data)

				if(count % 1000 == 0):
					total_inserted += helpers.bulk(es_client, actions)[0]
					print("[",count,"]", end="")
					sys.stdout.flush()
					actions = []
				elif(count%100 == 0):
					print(".", end="")
					sys.stdout.flush()
					
			except exceptions.ConnectionTimeout as timeoutError:
				retry = 0
				while retry <= 5:
					retry += 1
					print("10초 간 쉬었다가 다시!\n")
					time.sleep(10)
					
					try:
						print("색인 {0}번째 재시도..".format(retry))
						total_inserted += helpers.bulk(es_client, actions)[0]
						print("[",count,"]", end="")
						actions = []
						break
					except exceptions.ConnectionTimeout as timeoutError:
						continue
			except json.decoder.JSONDecodeError as jsonError :
				print("!!!! Error Message : {0}".format(jsonError))
				print("!!!! raised at \"{0} : {1}\"".format(count,line))
				pass
			except:
				print("error at line", count, end="")
				print("\n")
				print("Unexpected error:", sys.exc_info()[0])
				print("\n")
				raise

	try:
		total_inserted += helpers.bulk(es_client, actions)[0]
	except exceptions.ConnectionTimeout as timeoutError:
		retry = 0
		while retry <= 5:
			retry += 1
			print("10초 간 쉬었다가 다시!\n")
			time.sleep(10)
			
			try:
				print("색인 {0}번째 재시도..".format(retry))
				helpers.bulk(es_client, actions)[0]
				print("[",count,"]", end="")
				actions = []
				break
			except exceptions.ConnectionTimeout as timeoutError:
				continue
			
	csv_file.close()
	print("")
	print("<Finish>Totally {} inserted.".format(str(total_inserted)))
	
	
	
	
	
def insert_emotions(data):
	# 감정 분석 결과가 저장되어 있는지 확인
	es_conn = http.client.HTTPConnection(es_host, es_port)
	es_conn.connect()
	
	es_request = {
		"query" : { 
			"has_parent": {
			  "parent_type": "crawl_doc",
			  "query" :{
				  "term":{
					"_id": data['_id']
				  }
				}
			}
		}
	}
	
	es_conn.request("POST", "/"+INDEX_NAME+"/emotions/_search", json.dumps(es_request), { "Content-Type" : "application/json" })
	emotions_exists = json.loads(es_conn.getresponse().read())
	
	# 감정 분석결과가 존재하면 기존 감성분석 결과를 지우고 새로 넣음
	if 'hits' in emotions_exists and emotions_exists['hits']['total']>0 :
		logger.info("Delete old emotions for %s" % data['_id'])
		removeOldEmotions(data['_id'], es_request)
	
	bica_result = request2Bica(data, _1_RSA_CONCEPT_ID)
	if bica_result is not None and 'result' in bica_result and len(bica_result['result']):
		insertEmotionsBulk(data['_source']['user_id'], data['_source']['doc_id'], bica_result, _1_RSA_CONCEPT_ID)
	else:
		bica_result = request2Bica(data, _2_RSA_GENERAL_SATISFACTION)
		if bica_result is not None and 'result' in bica_result and len(bica_result['result']):
			insertEmotionsBulk(data['_source']['user_id'], data['_source']['doc_id'], bica_result, _2_RSA_GENERAL_SATISFACTION)
		else:
			bica_result = request2Bica(data, _3_OVERALL_RSA_FEEDBACK_CONCEPT_ID)
			if bica_result is not None and 'result' in bica_result and len(bica_result['result']):
				insertEmotionsBulk(data['_source']['user_id'], data['_source']['doc_id'], bica_result, _3_OVERALL_RSA_FEEDBACK_CONCEPT_ID)
			else:
				bica_result = request2Bica(data, _4_SALES_CONCEPT_ID)
				if bica_result is not None and 'result' in bica_result and len(bica_result['result']):
					insertEmotionsBulk(data['_source']['user_id'], data['_source']['doc_id'], bica_result, _4_SALES_CONCEPT_ID)
				else:
					bica_result = request2Bica(data, _5_SALES_GENERAL_SATISFACTION)
					if bica_result is not None and 'result' in bica_result and len(bica_result['result']):
						insertEmotionsBulk(data['_source']['user_id'], data['_source']['doc_id'], bica_result, _5_SALES_GENERAL_SATISFACTION)
				
				

def removeOldEmotions(docId, deleteRequest):
	es_conn = http.client.HTTPConnection(es_host, es_port)
	es_conn.connect()
	es_conn.request("POST", "/"+INDEX_NAME+"/emotions/_delete_by_query", json.dumps(deleteRequest), { "Content-Type" : "application/json" })

	removed = json.loads(es_conn.getresponse().read())
	
	logger.info("%d for document '%s' are deleted."%(removed['deleted'], docId))


		
		
def insertEmotionsBulk(userId, docId, bicaResult, conceptID):
	category_dict = {}
	es_insert_requests = []
	
	for result in bicaResult['result']:
		# BICA Result에서 sentence.index + matched_text.begin + lsp의 조합이 고유함.
		# 따라서, user_id + doc_id + sentence.index + matched_text.begin + lsp 를 조합해 md5 만들고, 이를 emotion_id로 사용.
		emotion_id = md5Generator([
			userId,
			docId,
			result['sentence']['index'],
			result['matched_text']['begin'],
			result['lsp']
		])
		
		es_insert_req = {}
		es_insert_req['emotion_id'] = emotion_id
		es_insert_req['conceptlabel'] = result['info']['conceptlabel']
		es_insert_req['kma'] = result['kma']#.encode('utf-8')
		es_insert_req['lsp'] = result['lsp']#.encode('utf-8')
		es_insert_req['sentence'] = result['sentence']
		es_insert_req['matched_text'] = result['matched_text']
		es_insert_req['upd_datetime'] = get_current_datetime()
		#es_insert_req['variables'] = [ var for variable in result['variables'] ]
							
		# topic이 존재하는 경우
		if conceptID in [_1_RSA_CONCEPT_ID, _2_RSA_GENERAL_SATISFACTION, _4_SALES_CONCEPT_ID, _5_SALES_GENERAL_SATISFACTION]:
			es_insert_req['categories'] = result['categories']
		else: # Sales, RSA의 Topic에 모두 해당하지 않는 경우, Overall RSA Feedback으로 간주.
			es_insert_req['categories'] = [{
				"label" : "0_GEN_RSAOV2.1",
				"weight" : 0,
				"entries" : result['categories'][0]['entries'] if len(result['categories'])>0 else [""]
			}]
			
		# 적당한 카테고리가 없는 경우, 그 result 자체를 저장하지 않음.
		#if(len(es_insert_req['categories'])==0): continue
		
		es_conn = http.client.HTTPConnection(es_host, es_port)
		es_conn.connect()				
		es_conn.request("POST", "/"+INDEX_NAME+"/emotions/"+emotion_id+"?parent="+docId, json.dumps(es_insert_req), {"Content-Type":"application/json"})
		
		
		
		
def request2Bica(data, conceptID):
	bica_conn = http.client.HTTPConnection(bica_host, bica_port)
	
	#1. RSA 먼저 검색
	bica_request = {
		"data" : data['_source']['doc_content'],
		"conceptID" : conceptID
	}
	bica_result = None
	
	try:
		bica_conn.connect()
		bica_conn.request("POST", "/request.is?"+urllib.parse.urlencode(bica_request, 'utf-8'), "", { "Content-Type" : "application/json" })
		bica_result = json.loads(bica_conn.getresponse().read())
	except:
		retry = 0
		ex = traceback.format_exc()
		while retry < 5:
			print("[request2Bica] Sleep for 30 seconds....", end="")
			time.sleep(30)
			try:
				ex = traceback.format_exc()
				print("[retry:%d] %s"%(retry+1, str(ex)))
				
				bica_conn.connect()
				bica_conn.request("POST", "/request.is?"+urllib.parse.urlencode(bica_request, 'utf-8'), "", { "Content-Type" : "application/json" })
				bica_result = json.loads(bica_conn.getresponse().read())
				if bica_result is not None: break
			except:
				retry += 1
				continue
		
		if retry >= 5:
			print("[FAILED][request2Bica] ", data, conceptID)
	
	logger.debug('---------------------------------------------------------------')
	logger.debug(json.dumps(bica_request))
	logger.debug(bica_result)
	logger.debug('---------------------------------------------------------------')
	
	return bica_result
		
		
		
'''
	- dir				: 저장할 파일 디렉토리
	- csvname			: csv파일명
	- start_datetime	: 검색해서 가져올 crawl_doc 시작일자 (마지막 실행했던 데이터의 doc_datetime)
	- toRevise			: 검증용 파일 여부
'''
def es2csv(dir, csvname, start_datetime, toRevise=False, end_datetime=None):
	# get query from es search result and send it to BICA to get analyzed result.
	es_search_request = {
		"query" : {
			"range" : {
				"doc_datetime" : {
					"gt" : start_datetime
				}
			}
		}
	}
	
	if end_datetime is not None:
		es_search_request['query']['range']['doc_datetime']['lt'] = end_datetime
		
	logger.debug("[es2csv] es_uri ::: " + "/"+INDEX_NAME+"/crawl_doc/_count")
	logger.debug("[es2csv] es_search_request ::: " + json.dumps(es_search_request))
	
	# 검색 결과 갯수 가져오기
	es_conn = http.client.HTTPConnection(es_host, es_port)
	es_conn.connect()
	es_conn.request("POST", "/"+INDEX_NAME+"/crawl_doc/_count", json.dumps(es_search_request), {"Content-Type" : "application/json"})
	
	es_count = json.loads(es_conn.getresponse().read())
	
	if 'count' in es_count and es_count['count']>0:
		totalCount = es_count['count']
		
		# CSV 생성
		if toRevise:
			csvfile = codecs.open(os.path.join(dir, 'to_revise', csvname), 'w', 'utf-8')
			csvfile.write("case_id;response_id;answer_id;language_id;comment;subtopic_code;sentence;category;sentiment;sentiment_case;english_translation")
			csvfile.write("\r\n")
		else:
			csvfile = codecs.open(os.path.join(dir, csvname), 'w', 'utf-8')
			csvfile.write("case_id;response_id;answer_id;language_id;comment;subtopic_code;sentiment;sentiment_case;english_translation")
			csvfile.write("\r\n")
			
		logger.debug("<Start (%d)>"%totalCount)
		print("<Start (%d)>"%totalCount)
		
		scroll_id = None
		for page in range(math.ceil(totalCount/PAGE_SIZE)):
			print("[PAGE:%d]"%page)
			
			requestUri = ""
			requestPerPage = None
			
			if scroll_id is not None:
				requestUri = "/_search/scroll"
				requestPerPage = { "scroll" : "1d", "scroll_id" : scroll_id }
			else:
				requestUri = "/"+INDEX_NAME+"/crawl_doc/_search?scroll=1d"
				requestPerPage = es_search_request
				requestPerPage['size'] = PAGE_SIZE
				
			logger.debug("[es2csv] requestUri ::: " + requestUri)
			logger.debug("[es2csv] requestPerPage ::: " + json.dumps(requestPerPage))
				
			# 본격적으로 검색
			es_conn2 = http.client.HTTPConnection(es_host, es_port)
			es_conn2.connect()
			es_conn2.request("POST", requestUri, json.dumps(requestPerPage), {"Content-Type" : "application/json"})
			
			scrolledResult = json.loads(es_conn2.getresponse().read())
			scroll_id = scrolledResult['_scroll_id']
			
			#############################################################################
			#                                 원문 단위 loop
			#############################################################################
			for num, hit in enumerate(scrolledResult["hits"]["hits"]):
				docid = hit['_id']
				comment = hit['_source']['doc_content']
				case_id, response_id, answer_id, language_id = re.sub("^E", "", docid).split('_')
	
				# 검색된 crawl_doc의 문서에 해당하는 emotions 검색
				emotions_req = {
					"query" : {
						"has_parent" : {
							"parent_type" : "crawl_doc",
							"query" : {
								"term" : {
									"_id" : docid
								}
							}
						}
					}
				}
				es_conn.request("GET", "/"+INDEX_NAME+"/emotions/_search", json.dumps(emotions_req), {
					"Content-Type" : "application/json"
				})
				emotions_result = json.loads(es_conn.getresponse().read())
				
				#############################################################################
				#                                 감정 분석 loop
				#############################################################################
				emotionsForThisDocument = {}
				if 'total' in emotions_result['hits'] and emotions_result['hits']['total']>0:
					# category의 label이 중복되지 않도록
					# emotionsForThisDocument에 중복을 체크하며 담는다.
					for emotion in emotions_result['hits']['hits']:
						for category in emotion['_source']['categories']:
							weight = None
							label = None
							
							# "가중치3", "경쟁브랜드_가중치3", "경쟁브랜드", "메르세데스벤츠" 이런 경우는 저장 X
							if(ETC_CATEGORY_LABEL.search(category['label']) is not None or category['label'].find('_')<0 ):
								continue # "label" :
							else:
								weight, label = category['label'].split("_", 1) # 3_PRO_QUALIT -> 3, PRO_QUALIT
								
							# category_dict가 비어있지 않고,
							# category_dict[label]이 존재하고,
							# category_dict[label]의 값이 현재 weight보다 작거나 같으면 skip
							if(label in emotionsForThisDocument and int(weight)>=int(emotionsForThisDocument[label]['weight'])):
								continue 
							else:
								emotionsForThisDocument[label] = {
									'id' : emotion['_id'],
									'weight' : int(weight),
									'entry' : category['entries'][0],
									'sentence' : emotion['_source']['matched_text']['string']
								}
					
				write2csv(csvfile, case_id, response_id, answer_id, language_id, comment, emotionsForThisDocument, toRevise)
					
				if(num+1 % 1000 == 0):
					print('[{}]'.format(str(num+1)), end="")
					sys.stdout.flush()
				if(num+1 % 100 == 0):
					print('.', end="")
					sys.stdout.flush()
		
		csvfile.close()
		
	else:# no data
		logger.debug("<No data>")
		print("<No data>")
	
	logger.debug("<Finish>")
	print("<Finish>")
	
	
	
	
	
def write2csv(csvfile, case_id, response_id, answer_id, language_id, comment, val, toRevise):
	if bool(val):
		# 중복이 제거된 val을 csv 파일에 쓴다.
		for subtopicCode, em in val.items():
			csvfile.write(case_id)
			csvfile.write(";")
			csvfile.write(response_id)
			csvfile.write(";")
			csvfile.write(answer_id)
			csvfile.write(";")
			csvfile.write(language_id)
			csvfile.write(";")
			csvfile.write(re.sub("[\"\'\+=\-/]", "", comment)) # comment
			csvfile.write(";")
			csvfile.write(subtopicCode) # subtopic_code, 2_LOC_OVERAL -> LOC_OVERAL
			csvfile.write(";")
			if toRevise:
				csvfile.write(re.sub("[\"\'\+=\-/]", "", em['sentence'])) # sentence
				csvfile.write(";")
				# (운전[L]+(/J_)?+즐거움[L]) -> 운전+(/J_)?+즐거움
				csvfile.write(re.sub("\[[a-zA-Z]\]", "", re.sub("\)$", "", re.sub("^\(", "", em['entry'])))) # variable
				csvfile.write(";")
			# TODO : 추후 이 부분은 matched_text로 교체해서 정확성이 더 올라가는지 확인 후 변경. --------------------------
			csvfile.write(str(get_sentiment(em['id'], em['sentence'])))
			# ----------------------------------------------------------------------------------------
			csvfile.write(";")
			csvfile.write(str(get_sentiment(answer_id, comment)))
			csvfile.write(";")
			csvfile.write("\r\n")
	else: # subtopic_code로 분류할만한 유효한 카테고리가 없거나 애초에 no topic인 경우
		#print("!!! No topic")
							
		csvfile.write(case_id)
		csvfile.write(";")
		csvfile.write(response_id)
		csvfile.write(";")
		csvfile.write(answer_id)
		csvfile.write(";")
		csvfile.write(language_id)
		csvfile.write(";")
		csvfile.write(re.sub("[\"\'\+=\-/]", "", comment)) # comment
		csvfile.write(";")
		csvfile.write('GEN_NONRSA2.1') # subtopic_code
		csvfile.write(";")
		if toRevise:
			csvfile.write("") # sentence
			csvfile.write(";")
			csvfile.write("") # category
			csvfile.write(";")
		csvfile.write(str(get_sentiment(answer_id, comment))) # 2018.03.20 전체 문장에 대해서 감정을 판단한다.
		csvfile.write(";")
		csvfile.write(str(get_sentiment(answer_id, comment))) # 2018.04.03 sentiment, sentiment_comment 동일
		csvfile.write(";")
		csvfile.write("\r\n")
		
		
		
		
def get_sentiment(emotionId, text):
	sentiment = 0
	
	# 파라미터 세팅
	req = urllib.parse.urlencode({'sentence' : text}, 'utf-8')
	
	# 긍부정 분석기에 연결
	try:
		tousflux_conn = http.client.HTTPConnection(tousflux_host, tousflux_port)
		tousflux_conn.connect()
		tousflux_conn.request("GET", "/?authinit={0}&{1}".format(emotionId, req))
	except:
		retry = 0
		ex = traceback.format_exc()
		while retry < 5:
			print("[get_sentiment] Sleep for 30 seconds....", end="")
			time.sleep(30)
			try:
				ex = traceback.format_exc()
				print("[retry:%d] %s"%(retry+1, str(ex)))
				
				tousflux_conn = http.client.HTTPConnection(tousflux_host, tousflux_port)
				tousflux_conn.connect()
				tousflux_conn.request("GET", "/?authinit={0}&{1}".format(emotionId, req))
				break
			except:
				retry += 1
				continue
		
		if retry >= 5:
			print("[FAILED][get_sentiment] ", emotionId, text)
	
	# 결과 처리
	result = str(tousflux_conn.getresponse().read())
	
	# response 문자열이 |(파이프)로 나누어 5개 이상일 때 정상.
	if len(result.split("|"))>=5 and result.split("|")[3]!="":
		sentiment = sentiment_score[ result.split("|")[3] ]
	
	return sentiment
		
			
			
			
			
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




if __name__ == '__main__':
	
	csv2es(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding\GeneralFeedbacks.csv")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Apr_2017.CSV", "20170401000000", False, "20170501000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "May_2017.CSV", "20170501000000", False, "20170601000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Jun_2017.CSV", "20170601000000", False, "20170701000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Jul_2017.CSV", "20170701000000", False, "20170801000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Aug_2017.CSV", "20170801000000", False, "20170901000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Sep_2017.CSV", "20170901000000", False, "20171001000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Oct_2017.CSV", "20171001000000", False, "20171101000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Nov_2017.CSV", "20171101000000", False, "20171201000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Dec_2017.CSV", "20171201000000", False, "20180101000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Jan_2018.CSV", "20180101000000", False, "20180201000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Feb_2018.CSV", "20180201000000", False, "20180301000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Mar_2018.CSV", "20180301000000", False, "20180401000000")
	#es2csv(r"E:\documents\# 회사 업무\# D-Map\05. 프로젝트\2017.03 BMW 텍스트 감성 분석\06. Coding 2.1 개편\과거 데이터 RE-Coding", "Apr_2018.CSV", "20180401000000", False, "20180501000000")
	
	'''
	insert_emotions({
        "_index": "bmw_v2.1",
        "_type": "crawl_doc",
        "_id": "E129801_3559056_3428535_48",
        "_score": 27899.535,
        "_source": {
          "doc_id": "E129801_3559056_3428535_48",
          "doc_datetime": "20170404093158",
          "doc_content": "네비게이션이 불편하다고 하심",
          "user_id": "bmw"
        }
    })
	'''