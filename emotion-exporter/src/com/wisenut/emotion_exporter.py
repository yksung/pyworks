# -*- coding: utf-8 -*-
from aioes import Elasticsearch
from aioes.client import IndicesClient
from aiohttp import ClientSession
from datetime import datetime as dt
from elasticsearch import helpers, exceptions
from com.wisenut.dao import mariadbclient
import aiohttp
import asyncio
import logging
import time
import json
import re
import math
import copy
import sys
import hashlib
import urllib
import urllib3
import slackweb
import traceback
import http.client
from com.wisenut import myLogger

############# setting elasticsearch
es_ip = "211.39.140.88"
es_port = 9200

############# setting slack
slack = slackweb.Slack("https://hooks.slack.com/services/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")
slackChannel = "#hankyung-error-alert"
slackUserName = "emotion_exporter-hankyung"
slackIconEmoji = ":ghost:"

############# setting search options
PAGE_SIZE=1000
INDEX_EMOTIONS="emotions"
INDEX_DOCUMENTS="documents"
TYPE_DOC="doc"

      
async def make_bulk(document, bica_result):
    bulk_body = None

    inline_script = ""
    inline_script+="ctx._source.project_seq?.addAll(params.project_seq); Set hs = new HashSet(); hs.addAll(ctx._source.project_seq); ctx._source.project_seq.clear(); ctx._source.project_seq.addAll(hs);"
    
    inline_script+="ctx._source.emotion_id=params.emotion_id;"
    inline_script+="ctx._source.emotion_score=params.emotion_score;"
    inline_script+="ctx._source.kma=params.kma;"
    inline_script+="ctx._source.conceptlabel=params.conceptlabel;"
    inline_script+="ctx._source.lsp=params.lsp;"
    inline_script+="ctx._source.sentence=params.sentence;"
    inline_script+="ctx._source.matched_text=params.matched_text;"
    inline_script+="ctx._source.variables=params.variables;"
    inline_script+="ctx._source.categories=params.categories;"
    inline_script+="ctx._source.conceptlevel1=params.conceptlevel1;"
    inline_script+="ctx._source.conceptlevel2=params.conceptlevel2;"
    inline_script+="ctx._source.conceptlevel3=params.conceptlevel3;"
    inline_script+="ctx._source.begin_offset=params.begin_offset;"
        
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
    inline_script+="ctx._source.upd_datetime=params.upd_datetime;"
    
    
    emotion_dict = {}
    
    emotion_id = md5Generator([document['_id'], bica_result['info']['conceptlabel'], bica_result['matched_text']['begin']])
    emotion_dict =  {
        "project_seq" : document['_source']['project_seq'],
        
        "emotion_id" : emotion_id,
        "emotion_score" : 0,
        "kma" : bica_result['kma'],
        "conceptlabel" : bica_result['info']['conceptlabel'],
        "lsp" : bica_result['lsp'],
        "sentence" : bica_result['sentence'],
        "matched_text" : bica_result['matched_text'],
        "variables" : bica_result['variables'],
        "categories" : bica_result['categories'],
        "conceptlevel1" : bica_result['info']['conceptlabel'].split(">")[1].strip() if len(bica_result['info']['conceptlabel'].split(">"))>1 else None,
        "conceptlevel2" : bica_result['info']['conceptlabel'].split(">")[2].strip() if len(bica_result['info']['conceptlabel'].split(">"))>2 else None,
        "conceptlevel3" : bica_result['info']['conceptlabel'].split(">")[3].strip() if len(bica_result['info']['conceptlabel'].split(">"))>3 else None,
        "begin_offset" : None,
        
        "doc_id" : document['_id'],
        "doc_datetime" : document['_source']['doc_datetime'],
        "doc_url" : document['_source']['doc_url'],
        "doc_writer" : document['_source']['doc_writer'],
        "doc_title" : document['_source']['doc_title'].replace("\\", "").replace("\n", "").replace("\r", ""),
        "doc_content" : document['_source']['doc_content'].replace("\\", "").replace("\n", "").replace("\r", ""),
        "depth1_seq" : document['_source']['depth1_seq'],
        "depth2_seq" : document['_source']['depth2_seq'],
        "depth3_seq" : document['_source']['depth3_seq'],
        "depth1_nm" : document['_source']['depth1_nm'],
        "depth2_nm" : document['_source']['depth2_nm'],
        "depth3_nm" : document['_source']['depth3_nm'],
        "upd_datetime" : get_current_datetime()
    }
    
    bulk_body = {
        "_op_type": "update",
        "_index": INDEX_EMOTIONS,
        "_type": TYPE_DOC,
        "_id": emotion_id,
        "_source": {
            "script": {
                "lang" : "painless",
                "inline": inline_script, 
                "params": emotion_dict
            },
            "upsert": emotion_dict
        }
    }
        
    #await asyncio.sleep(1)
    return bulk_body  # ======> 여기서 바로  bulk를 넣어버림.

async def time_log():
    i = 0
    print("time log starts.")
    while True:
        await asyncio.sleep(1)
        i += 1
        print('...%02d sec.' % (i,))
        
        

def request2bica(ip, port, concept_id, text):
    bica_conn = http.client.HTTPConnection(ip, port)
    
    bica_request = {
        'data' : text,
        'conceptID' : concept_id                    
    }
    bica_conn.request("POST", "/request.is?" + urllib.parse.urlencode(bica_request, 'utf-8'), "", { "Content-Type" : "application/json" })
    
    return bica_conn.getresponse().read()



def insert_emotions(project_seq, document_data):
    some_bulks = ''
    bulk_result=0
    
    try:
        bica_ip, bica_port, concept_id = mariadbclient.get_bica_info(project_seq)
        
        bica_result = request2bica(bica_ip, bica_port, concept_id, document_data['_source']['doc_title'] + ' ' + document_data['_source']['doc_content']) 
        
        if bica_result:
            json_result = json.loads(bica_result)
            
            from elasticsearch import Elasticsearch
            es_client=Elasticsearch(":".join([es_ip, str(es_port)]))
            try:
                fts = [ make_bulk(document_data, r) for r in json_result['result'] ]
                #t.cancel()
                some_bulks = yield from asyncio.gather(*fts)
                
                bulk_result += helpers.bulk(es_client, list(filter(lambda x:x and len(x)>0, some_bulks)))[0]
            except exceptions.ConnectionTimeout as timeoutError:
                retry = 0
                logger.error("[insert_emotions] %s (retry:%d)"%(str(timeoutError), retry))
                while retry <= 5:
                    retry += 1
                    print("10초 간 쉬었다가 다시!\n")
                    time.sleep(10)
                    
                    try:
                        print("색인 {0}번째 재시도..".format(retry))
                        bulk_result += helpers.bulk(es_client,  list(filter(lambda x:x and len(x)>0, some_bulks)))[0]
                        break
                    except exceptions.ConnectionTimeout as timeoutError:
                        logger.error("[insert_emotions] %s (retry:%d)"%(str(timeoutError), retry))
                        continue
            except aiohttp.client_exceptions.ClientConnectorError as connectError:
                retry = 0
                logger.error("[insert_emotions] %s (retry:%d)"%(str(connectError), retry))
                while retry <= 5:
                    retry += 1
                    print("10초 간 쉬었다가 다시!\n")
                    time.sleep(10)
                    
                    try:
                        print("색인 {0}번째 재시도..".format(retry))
                        bulk_result += helpers.bulk(es_client,  list(filter(lambda x:x and len(x)>0, some_bulks)))[0]
                        break
                    except aiohttp.client_exceptions.ClientConnectorError as connectError:
                        logger.error("[insert_emotions] %s (retry:%d)"%(str(connectError), retry))
                        continue
            except OSError as oserror:
                retry = 0
                logger.error("[insert_emotions] %s (retry:%d)"%(str(oserror), retry))
                while retry <= 5:
                    retry += 1
                    print("10초 간 쉬었다가 다시!\n")
                    time.sleep(10)
                    
                    try:
                        print("색인 {0}번째 재시도..".format(retry))
                        bulk_result += helpers.bulk(es_client,  list(filter(lambda x:x and len(x)>0, some_bulks)))[0]
                        break
                    except OSError as oserror:
                        logger.error("[insert_emotions] %s (retry:%d)"%(str(oserror), retry))
                        continue
            except urllib3.exceptions.NewConnectionError as connectionError:
                retry = 0
                logger.error("[insert_emotions] %s (retry:%d)"%(str(connectionError), retry))
                while retry <= 5:
                    retry += 1
                    print("10초 간 쉬었다가 다시!\n")
                    time.sleep(10)
                    
                    try:
                        print("색인 {0}번째 재시도..".format(retry))
                        bulk_result += helpers.bulk(es_client,  list(filter(lambda x:x and len(x)>0, some_bulks)))[0]
                        break
                    except urllib3.exceptions.NewConnectionError as connectionError:
                        logger.error("[insert_emotions] %s (retry:%d)"%(str(connectionError), retry))
                        continue
            except:
                ex = traceback.format_exc()
                logger.error("[insert_emotions] unknown error. Traceback >> %s " % ex)
            
            logger.debug("%d are successfully inserted."%bulk_result)
       
    except:
        ex = traceback.format_exc()
        logger.error("[insert_emotions] unknown error. Traceback >> %s " % ex)


def main(project_seq, start_date, end_date):
    scroll_id = None
    
    #1. 분석하고자 하는 document list를 가져옴.
    es_request = {
      "query": {
        "bool" :{
           "must" : [
                {
                 "range" : {
                    "doc_datetime" : {
                        "gte" : start_date,
                        "lte" : end_date
                    }
                 }
                },
                {
                 "term" : {
                    "project_seq" : project_seq
                  }
                }
            ]
        }
      }
    }
    
    logger.info("Start >>> %s" % es_request)
      
    try:
        es_conn = http.client.HTTPConnection(es_ip, es_port)
        es_conn.request("POST", "/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_count", json.dumps(es_request), { "Content-Type" : "application/json" })
        es_result = es_conn.getresponse().read()
        
        if 'count' not in json.loads(es_result):
            logger.error("[main] 'count' not in the result.")
            return
        elif json.loads(es_result)['count'] <= 0:
            logger.info("[main] result size 0.")
            return

        total = json.loads(es_result)['count']
                
        # 2. 본격적으로  데이터 스캔
        for pageNo in range(math.ceil(total/PAGE_SIZE)):
            logger.info("<PAGE:%d>"%pageNo)
            
            # scroll_id 여부에 따라 request 데이터 값과 url 을 바꿈.
            if scroll_id:
                es_request_per_page = {
                    "scroll" : "1d",
                    "scroll_id" : scroll_id
                }
                es_url = "/_search/scroll"
            else:
                es_request_per_page = copy.copy(es_request)
                es_request_per_page['size'] = PAGE_SIZE        # page size를 바꿈
                
                es_url = "/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_search?scroll=1d"
            
            # 검색 요청
            logger.debug("[main] es_request_per_page > %s" % json.dumps(es_request_per_page))
            
            try:
                es_conn = http.client.HTTPConnection(es_ip, es_port)
                es_conn.request("POST", es_url, json.dumps(es_request_per_page), { "Content-Type" : "application/json" })
                es_result = json.loads(es_conn.getresponse().read())
            except OSError as oserror:
                logger.error("[main] oserror.(%s)"%str(oserror))
                slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                             text="[main] oserror.(%s)\n-URL:%s\n-RequestBody:%s"%(str(oserror), es_url, json.dumps(es_request_per_page)))
                continue
            except:
                ex = traceback.format_exc()
                
                logger.error("[main] error. Traceback >> %s" % ex)
                slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                             text="[main] error.\n-URL:%s\n-RequestBody:%s \n traceback >>> %s "%(es_url, json.dumps(es_request_per_page), ex))
                continue
            
            if 'hits' in es_result:
                scroll_id = es_result['_scroll_id']
                resultlist = es_result['hits']['hits']
                
                logger.info("<Total:%d>"%es_result['hits']['total'] )
                for idx, doc in enumerate(resultlist):
                    if idx % 100 == 0:
                        logger.info("[%d]"%idx)
                        
                        print("[%d]"%idx, end="")
                        sys.stdout.flush()
                    else:
                        print(".", end="")
                        sys.stdout.flush()
                    # 2. 만약에 emotions나 topics 중 하나만 있으면 싹 지우고 다시 분석.
                    # 지금은 project_seq를 임의로 1로 채우지만 나중에는 변경이 생긴 project에 대한 리스트를 받아와서 다른 project_seq를 넣어줘야함.
                    
                    #delete_by_topics(doc['_id'], project_seq)
                    remove_project_seq(project_seq, doc['_id'])
                    
                    #insert_emotions(doc, project_seq)
                    
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(asyncio.ensure_future(insert_emotions(project_seq, doc)))
                    
    except OSError as oserror:
        logger.error("[main] es_conn error. (%s)"%str(oserror))
        slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                     text="[main] es_conn error.(%s)\n-URL:%s\n-RequestBody:%s"%(str(oserror), "/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_search", json.dumps(es_request)))
    except:
        ex = traceback.format_exc()
        
        logger.error("[main] error. Traceback >> %s" % ex)        
        slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                     text="[main] error.\n-URL:%s\n-RequestBody:%s\n traceback >>> %s"%("/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_search", json.dumps(es_request), ex))
    
    '''
    DELETE /_search/scroll
    {
        "scroll_id" : "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAD4WYm9laVYtZndUQlNsdDcwakFMNjU1QQ=="
    }
    '''
    if scroll_id:    
        try:
            es_conn = http.client.HTTPConnection(es_ip, es_port)
            es_conn.request("DELETE", "/_search/scroll", json.dumps({"scroll_id":scroll_id}), { "Content-Type" : "application/json" })
        except OSError as oserror:
            logger.error("[main] es_conn error. (%s)"%str(oserror))
            slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                         text="[main] es_conn error.(%s)\n-URL:%s\n-RequestBody:%s"%(str(oserror), "/_search/scroll", json.dumps({"scroll_id":scroll_id})))
        except:
            ex = traceback.format_exc()
        
            logger.error("[main] error. Traceback >>> %s" % ex)
            slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                         text="[main] error.\n-URL:%s\n-RequestBody:%s\n traceback >>> %s"%("/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_search", json.dumps(es_request), ex))
    
def delete_emotion(emotion_id):
    es_conn = http.client.HTTPConnection(es_ip, es_port)
    es_conn.request("DELETE", "/"+INDEX_EMOTIONS+"/doc/"+emotion_id, "", { "Content-Type" : "application/json" })
    
    
def remove_project_seq(project_seq, doc_id):
    remove_project_seq_query = {
        "script" : {
            "inline": "if(ctx._source.project_seq?.indexOf(params.project_seq)>0) ctx._source.project_seq?.remove(ctx._source.project_seq?.indexOf(params.project_seq));",
            "lang": "painless",
            "params" : {
                "project_seq" : project_seq,
                "upd_datetime" : get_current_datetime()
            }
        },
        "query": {
            "term" : {
                "doc_id" : doc_id
            }
        }
    }
    
    es_conn = http.client.HTTPConnection(es_ip, es_port)
    es_conn.request("POST", "/"+INDEX_EMOTIONS+"/_update_by_query", json.dumps(remove_project_seq_query), { "Content-Type" : "application/json" })                            


def get_current_datetime():
    ymdhms = str(dt.now().strftime('%Y-%m-%dT%H:%M:%S'))
    
    return ymdhms    
                


def md5Generator(arr):
    m = hashlib.md5()
    #for e in arr:
    m.update(repr(arr).encode('utf-8'))
    return m.hexdigest()


if __name__ == '__main__':
    process_name= sys.argv[1]
    project_seqs = sys.argv[2] # only one project_seq per emotional analysis
    start_date = sys.argv[3]
    end_date = sys.argv[4]
    
    ############# setting logging
    
    logger = myLogger.getMyLogger(process_name, hasConsoleHandler=False, hasRotatingFileHandler=True, logLevel=logging.DEBUG)
    
    logger.info("=================================================================")
    logger.info("- ES Connection %s %d" % (es_ip, es_port) )
    logger.info("- process_name\t:\t%s" % process_name)
    logger.info("- project_seqs\t:\t%s" % project_seqs)
    logger.info("- start_date\t:\t%s" % start_date)
    logger.info("- end_date\t:\t%s" % end_date)
    logger.info("=================================================================")
    
    
    for project_seq in project_seqs.split(","):
        logger.info("=================================================================")
        logger.info("- project_seq  %s" % project_seq)
        logger.info("=================================================================")
        print("=================================================================")
        print("- project_seq  %s" % project_seq)
        print("=================================================================")
        
        main(project_seq, start_date, end_date)
