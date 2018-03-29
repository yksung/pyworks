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

es_host = "211.39.140.78"
es_port = 9200

bica_host = "211.39.140.67"
bica_port = "21000"

#tousflux_host = "1.212.186.186"
#tousflux_port = 9099

tousflux_host = "58.229.163.171"
tousflux_port = 9095

sentiment_score = {
	"POSITIVE" : 100,
	"NEGATIVE" : -100,
	"ETC" : 0
}
	
def csv_check(filename):
	for num, line in enumerate(codecs.open(filename, "r", "utf-8")):
		if(len(line.split(";"))!=9):
			print("[Error] Line number {} is wrong format.".format(str(num+1)))
			
			
"""
1. csv2es : 우선 csv 데이터를 Elasticsearch에 넣음.
2. analyze_and_save2es : es에서 검색해서 가져온 값에 대해서 분석을 돌려서 분석 결과를 emotions type으로 저장.
"""

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
		
		if(len(line.split(";"))!=5):
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
					"_index" : "bmw",
					"_type" : "crawl_doc",
					"_id" : docid,
					"_source" : {
						"doc_id" : docid,
						"doc_datetime" : csv_datetime,
						"doc_content" : comment.replace('\r', '').replace('\n', ''),
						"user_id" : "bmw"
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
	
	es_conn.request("POST", "/bmw/emotions/_search", json.dumps(es_request), { "Content-Type" : "application/json" })
	emotions_exists = json.loads(es_conn.getresponse().read())
	
	# 감정 분석결과가 존재하면 새로 insert 하지 않음.
	if 'hits' in emotions_exists and emotions_exists['hits']['total']>0 :
		print("Emotions for %s already exist. " % data['_id'])
		return
	else:
		bica_conn = http.client.HTTPConnection(bica_host, bica_port)
		bica_conn.connect()

		bica_request = { "data" : data['_source']['doc_content'] }# 이 데이터를 빅애널라이저에 분석 요청 
		bica_conn.request("POST", "/request.is?"+urllib.parse.urlencode(bica_request, 'utf-8'), "", { "Content-Type" : "application/json" })
		print("/request.is?"+urllib.parse.urlencode(bica_request, 'utf-8'))
		
		bica_result = json.loads(bica_conn.getresponse().read())
		print(bica_result)
		print('---------------------------------------------------------------')
		
		"""
		variable_dict의 형태
		variable_dict = {
			0: [
				{ 'subtopic_code' : 'COH_FUELC', 'sentence' : '~~~~', 'matched_text' : '~~~~', 'variable' : '~~~' },
				
			],
			2: [
				{ 'subtopic_code' : 'COH_ENJOYM', 'sentence' : '~~~~', 'matched_text' : '~~~~', 'variable' : '~~~' },
				{ 'subtopic_code' : 'PDN_EXTERI', 'sentence' : '~~~~', 'matched_text' : '~~~~', 'variable' : '~~~' }
			],
		}
		"""
		category_dict = {}
		es_insert_requests = []
		for result in bica_result['result']:
			# doc_datetime + user_id + doc_id + lsp + matched_text.begin을 조합해 md5 만듦
			emotion_id = md5Generator([
				data['_source']['user_id'],
				data['_source']['doc_id'],
				result['lsp'],
				result['matched_text']['begin']
			])
			
			es_insert_req = {}
			es_insert_req['emotion_id'] = emotion_id
			es_insert_req['emotion_type'] = ''
			es_insert_req['emotion_score'] = 0.0
			es_insert_req['conceptlabel'] = bica_result['sentinfo'][0]['infoset']
			es_insert_req['kma'] = result['kma']#.encode('utf-8')
			es_insert_req['lsp'] = result['lsp']#.encode('utf-8')
			
			es_insert_req['sentence'] = {}
			es_insert_req['sentence']['string'] = result['sentence']['string']#.encode('utf-8')
			es_insert_req['sentence']['offset'] = result['sentence']['offset']
			es_insert_req['sentence']['index'] = result['sentence']['index']
			es_insert_req['sentence']['neighborhoods'] = result['sentence']['neighborhoods']
			
			es_insert_req['matched_text'] = {}
			es_insert_req['matched_text']['string'] = result['matched_text']['string']#.encode('utf-8')
			es_insert_req['matched_text']['begin'] = result['matched_text']['begin']
			es_insert_req['matched_text']['end'] = result['matched_text']['end']
			
			es_insert_req['variables'] = []
			for variable in result['variables']:
				es_insert_req['variables'].append({
					'name' : variable['name'],
					'value' : variable['value']#.encode('utf-8')
				})
				
			es_insert_req['categories'] = []
			for category in result['categories']:
				weight = None
				label = None
				if(category['label'].find('_')<0):
					continue # "label" : "가중치3" 이런 경우는 저장X
				else:
					weight, label = category['label'].split("_", 1) # 3_PRO_QUALIT -> 3, PRO_QUALIT
				
				
				if(label in category_dict and int(weight)>=int(category_dict[label]['weight'])):
					continue # category_dict가 비어있지 않고, category_dict[label]이 존재하고, category_dict[label]의 값이 현재 weight보다 작으면 skip
				else:
					category_dict[label] = { 'weight' : int(weight), 'entry' : category['entries'][0] }
					
					
			print(category_dict)
						
			for label, new_cate in category_dict.items():
				es_insert_req['categories'].append({
					"label" : label,
					"weight" : new_cate['weight'],
					"entries" :[ new_cate['entry'] ] 
				})
					
			# 적당한 카테고리가 없는 경우, 그 result 자체를 저장하지 않음.
			if(len(es_insert_req['categories'])==0): continue
			
			es_insert_req['conceptlevel1'] = bica_result['sentinfo'][0]['infoset'].split('>')[0] if( len(bica_result['sentinfo'][0]['infoset'].split('>'))>0 ) else ''
			es_insert_req['conceptlevel2'] = bica_result['sentinfo'][0]['infoset'].split('>')[1] if( len(bica_result['sentinfo'][0]['infoset'].split('>'))>1 ) else ''
			es_insert_req['conceptlevel3'] = bica_result['sentinfo'][0]['infoset'].split('>')[2] if( len(bica_result['sentinfo'][0]['infoset'].split('>'))>2 ) else ''
			es_insert_req['begin_offset'] = 0
			
			print(es_insert_req)
			
			es_conn = http.client.HTTPConnection(es_host, es_port)
			es_conn.connect()				
			es_conn.request("POST", "/bmw/emotions/"+emotion_id+"?parent="+data['_source']['doc_id'], json.dumps(es_insert_req), {"Content-Type":"application/json"})
		
		
def es2csv(dir, csvname, start_datetime):
	prefix_p = re.compile("^E")
	
	sent4comment_conn = http.client.HTTPConnection(tousflux_host, tousflux_port)
	sent4comment_conn.connect()
	sentiment_url_for_comment = "/?authinit=WISENUT01_TC0001_{0}&{1}"
	
	sent4sentence_conn = http.client.HTTPConnection(tousflux_host, tousflux_port)
	sent4sentence_conn.connect()
	sentiment_url_for_sentence = "/?authinit=WISENUT01_TC0002_{0}&{1}"

	es_conn = http.client.HTTPConnection(es_host, es_port)
	es_conn.connect()
	
	# get query from es search result and send it to BICA to get analyzed result.
	count = 0
	es_search_request = json.dumps({
		"size" : 10000,
		"query" : {
			"range" : {
				"doc_datetime" : {
					"gt" : start_datetime
				}
			}
		}
	})
	es_conn.request("POST", "/bmw/crawl_doc/_search", es_search_request, {
		"Content-Type" : "application/json"
	})
	
	es_result = json.loads(es_conn.getresponse().read())
	
	if 'hits' in es_result and es_result['hits']['total']>0:
		csvfile = codecs.open(os.path.join(dir, csvname), 'w', 'utf-8')
		csvfile.write("case_id;response_id;answer_id;language_id;comment;subtopic_code;sentiment;sentiment_comment;english_translation")
		csvfile.write("\r\n")
		
		print("<Start ({})>".format(str(es_result["hits"]["total"])))
		for num, hit in enumerate(es_result["hits"]["hits"]):
			docid = hit['_id']
			comment = hit['_source']['doc_content']
			case_id, response_id, answer_id, language_id = prefix_p.sub("", docid).split('_')
			sentiment_comment = 0
			
			#print("# comment : {}".format(comment), end="")
			
			# 골든플래닛 모듈에 연결해서 감정 리턴 (comment 단위)
			req_comment = urllib.parse.urlencode({'sentence' : comment}, 'utf-8')
			sent4comment_conn.request("GET", sentiment_url_for_comment.format(docid, req_comment)) # 골든플래닛 모듈에 연결해서 감정 리턴
			sent4comment_result = str(sent4comment_conn.getresponse().read())
			if len(sent4comment_result.split("|"))>=5 and sent4comment_result.split("|")[3]!="":
				sentiment_comment = sentiment_score[sent4comment_result.split("|")[3]]
				
			#print(">> sentiment_comment : " + sent4comment_result)
			
			# 검색된 crawl_doc의 문서에 대해서 다시 한번 emotions 값이 있는지 검색
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
			es_conn.request("GET", "/bmw/emotions/_search", json.dumps(emotions_req), {
				"Content-Type" : "application/json"
			})
			emotions_result = json.loads(es_conn.getresponse().read())
			
			# 이미 들어간 subtopic이 중복되어 들어가지 않도록 걸러내는 dictionary 변수
			category_dict = {}
			if('total' in emotions_result['hits'] and emotions_result['hits']['total']>0):
				for emotion in emotions_result['hits']['hits']:
					for category in emotion['_source']['categories']:
						subtopic_code = re.compile("^[0-9]_").sub("", category['label'])
						# 이미 해당 subtopic_code가 있으면 skip.
						if subtopic_code in category_dict:
							continue
						else:
							category_dict[subtopic_code] = None
						
						sentence = emotion['_source']['sentence']['string']
						sentiment = 0
						
						#print("### "+sentence, end="")
						
						# 골든플래닛 모듈에 연결해서 감정 리턴 (문장단위)
						#time.sleep(3)
						req_sentence = urllib.parse.urlencode({'sentence' : sentence}, 'utf-8')
						sent4sentence_conn.request("GET", sentiment_url_for_sentence.format(emotion['_id'], req_sentence)) 
						sent4sentence_result = str(sent4sentence_conn.getresponse().read())
						if len(sent4sentence_result.split("|"))>=5 and sent4sentence_result.split("|")[3]!="":
							sentiment = sentiment_score[sent4sentence_result.split("|")[3]]
							
						#print(">> sentiment : " + sent4sentence_result)
												
						csvfile.write(case_id)
						csvfile.write(";")
						csvfile.write(response_id)
						csvfile.write(";")
						csvfile.write(answer_id)
						csvfile.write(";")
						csvfile.write(language_id)
						csvfile.write(";")
						csvfile.write(comment) # comment
						csvfile.write(";")
						csvfile.write(subtopic_code) # subtopic_code, 2_LOC_OVERAL -> LOC_OVERAL
						csvfile.write(";")
						csvfile.write(str(sentiment))
						csvfile.write(";")
						csvfile.write(str(sentiment_comment))
						csvfile.write(";")
						csvfile.write("\r\n")
					
			else: # No topic의 경우
				#print("!!! No topic")
									
				csvfile.write(case_id)
				csvfile.write(";")
				csvfile.write(response_id)
				csvfile.write(";")
				csvfile.write(answer_id)
				csvfile.write(";")
				csvfile.write(language_id)
				csvfile.write(";")
				csvfile.write(hit['_source']['doc_content']) # comment
				csvfile.write(";")
				csvfile.write('No topic') # subtopic_code
				csvfile.write(";")
				csvfile.write('') # sentiment : 값 없음.
				csvfile.write(";")
				csvfile.write(str(sentiment_comment))
				csvfile.write(";")
				csvfile.write("\r\n")
				
			
			if(num+1 % 1000 == 0):
				print('[{}]'.format(str(num+1)), end="")
				sys.stdout.flush()
			if(num+1 % 100 == 0):
				print('.', end="")
				sys.stdout.flush()
				
			#time.sleep(3)
			
		print("")
		print("<Finish>")
		
		csvfile.close()
	else: # no data
		print("<No data>")
		
		
def es2csv4revising(dir, csvname, start_datetime): # 검증용 csv 파일을 별도로 생성
	prefix_p = re.compile("^E")
	
	sent4comment_conn = http.client.HTTPConnection(tousflux_host, tousflux_port)
	sent4comment_conn.connect()
	sentiment_url_for_comment = "/?authinit=WISENUT01_TC0001_{0}&{1}"
	
	sent4sentence_conn = http.client.HTTPConnection(tousflux_host, tousflux_port)
	sent4sentence_conn.connect()
	sentiment_url_for_sentence = "/?authinit=WISENUT01_TC0002_{0}&{1}"

	es_conn = http.client.HTTPConnection(es_host, es_port)
	es_conn.connect()
	
	# get query from es search result and send it to BICA to get analyzed result.
	count = 0
	es_search_request = json.dumps({
		"size" : 10000,
		"query" : {
			"range" : {
				"doc_datetime" : {
					"gt" : start_datetime
				}
			}
		}
	})
	es_conn.request("POST", "/bmw/crawl_doc/_search", es_search_request, {
		"Content-Type" : "application/json"
	})
	
	es_result = json.loads(es_conn.getresponse().read())
	
	if 'hits' in es_result and es_result['hits']['total']>0:		
		csv_to_check = codecs.open(os.path.join(dir, 'to_revise', csvname), 'w', 'utf-8')
		csv_to_check.write("case_id;response_id;answer_id;language_id;comment;subtopic_code;sentence;variable;sentiment;sentiment_comment;english_translation")
		csv_to_check.write("\r\n")
	
		print("<Start ({})>".format(str(es_result["hits"]["total"])))
		for num, hit in enumerate(es_result["hits"]["hits"]):
			docid = hit['_id']
			comment = hit['_source']['doc_content']
			case_id, response_id, answer_id, language_id = prefix_p.sub("", docid).split('_')
			sentiment_comment = 0
			
			#print("# comment : {}".format(comment), end="")
			
			# 골든플래닛 모듈에 연결해서 감정 리턴 (comment 단위)
			req_comment = urllib.parse.urlencode({'sentence' : comment}, 'utf-8')
			sent4comment_conn.request("GET", sentiment_url_for_comment.format(docid, req_comment)) # 골든플래닛 모듈에 연결해서 감정 리턴
			sent4comment_result = str(sent4comment_conn.getresponse().read())
			if len(sent4comment_result.split("|"))>=5 and sent4comment_result.split("|")[3]!="":
				sentiment_comment = sentiment_score[sent4comment_result.split("|")[3]]
				
			#print(">> sentiment_comment : " + sent4comment_result)
			
			# 검색된 crawl_doc의 문서에 대해서 다시 한번 emotions 값이 있는지 검색
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
			es_conn.request("GET", "/bmw/emotions/_search", json.dumps(emotions_req), {
				"Content-Type" : "application/json"
			})
			emotions_result = json.loads(es_conn.getresponse().read())
			
			# 이미 들어간 subtopic이 중복되어 들어가지 않도록 걸러내는 dictionary 변수
			category_dict = {}
			if('total' in emotions_result['hits'] and emotions_result['hits']['total']>0):
				for emotion in emotions_result['hits']['hits']:
					for category in emotion['_source']['categories']:
						subtopic_code = re.compile("^[0-9]_").sub("", category['label'])
						# 이미 해당 subtopic_code가 있으면 skip.
						if subtopic_code in category_dict:
							continue
						else:
							category_dict[subtopic_code] = None
					
						sentence = emotion['_source']['sentence']['string']
						sentiment = 0
						
						#print("### "+sentence, end="")
						
						# 골든플래닛 모듈에 연결해서 감정 리턴 (문장단위)
						#time.sleep(3)
						req_sentence = urllib.parse.urlencode({'sentence' : sentence}, 'utf-8')
						sent4sentence_conn.request("GET", sentiment_url_for_sentence.format(emotion['_id'], req_sentence)) 
						sent4sentence_result = str(sent4sentence_conn.getresponse().read())
						if len(sent4sentence_result.split("|"))>=5 and sent4sentence_result.split("|")[3]!="":
							sentiment = sentiment_score[sent4sentence_result.split("|")[3]]
						
						#검증용 csv
						csv_to_check.write(case_id)
						csv_to_check.write(";")
						csv_to_check.write(response_id)
						csv_to_check.write(";")
						csv_to_check.write(answer_id)
						csv_to_check.write(";")
						csv_to_check.write(language_id)
						csv_to_check.write(";")
						csv_to_check.write(comment) # comment
						csv_to_check.write(";")
						csv_to_check.write(subtopic_code) # subtopic_code, 2_LOC_OVERAL -> LOC_OVERAL
						csv_to_check.write(";")
						csv_to_check.write(sentence) # sentence
						csv_to_check.write(";")
						# (운전[L]+(/J_)?+즐거움[L]) -> 운전+(/J_)?+즐거움
						csv_to_check.write(re.compile("\[[a-zA-Z]\]").sub("", re.compile("\)$").sub("", re.compile("^\(").sub("", category['entries'][0])))) # variable
						csv_to_check.write(";")
						csv_to_check.write(str(sentiment))
						csv_to_check.write(";")
						csv_to_check.write(str(sentiment_comment))
						csv_to_check.write(";")
						csv_to_check.write("\r\n")
			else: # No topic의 경우
				#검증용 csv
				csv_to_check.write(case_id)
				csv_to_check.write(";")
				csv_to_check.write(response_id)
				csv_to_check.write(";")
				csv_to_check.write(answer_id)
				csv_to_check.write(";")
				csv_to_check.write(language_id)
				csv_to_check.write(";")
				csv_to_check.write(hit['_source']['doc_content']) # comment
				csv_to_check.write(";")
				csv_to_check.write('No topic') # subtopic_code
				csv_to_check.write(";")
				csv_to_check.write('') # sentence
				csv_to_check.write(";")
				csv_to_check.write('') # variable
				csv_to_check.write(";")
				csv_to_check.write('') # sentiment : 값 없음.
				csv_to_check.write(";")
				csv_to_check.write(str(sentiment_comment))
				csv_to_check.write(";")
				csv_to_check.write("\r\n")
			
			if(num+1 % 1000 == 0):
				print('[{}]'.format(str(num+1)), end="")
				sys.stdout.flush()
			if(num+1 % 100 == 0):
				print('.', end="")
				sys.stdout.flush()
				
			#time.sleep(3)
			
		print("")
		print("<Finish>")
		
		csv_to_check.close()
	else: # no data
		print("<No data>")
	
		
		
def get_sentiment(text):
	return ''
		
			
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
	csv2es(r"E:\data\TTRMOVE_COMM_21001231_14_48_235959.CSV")