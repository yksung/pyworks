# -*- coding: utf-8 -*-
from com.wisenut import myLogger
from elasticsearch_async import AsyncElasticsearch
from elasticsearch import exceptions
from datetime import datetime as dt
import sys
import asyncio
import logging
import logging.config
import json
import math
import copy
import urllib
import urllib3
import slackweb
import traceback
import http.client
import time

############# setting elasticsearch
es_ip = "211.39.140.49"
es_port = 9200

############# setting tousflux
sentiment_score = {
    "POSITIVE" : "긍정",
    "NEGATIVE" : "부정",
    "ETC" : "중립"
}

############# setting logging
logger = myLogger.getMyLogger('posneg', hasConsoleHandler=False, hasRotatingFileHandler=True, logLevel=logging.DEBUG)

logger.info("ES Connection %s %d" % (es_ip, es_port) )

############# setting slack
slack = slackweb.Slack("https://hooks.slack.com/services/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")
slackChannel = "#dmap_error_alert"
slackUserName = "posneg_classifier-dmap"
slackIconEmoji = ":ghost:"

############# setting search options
MAX_TOUSFLUX_NUM=10
PAGE_SIZE=10
INDEX_EMOTIONS="documents-*"
TYPE_DOC="doc"


@asyncio.coroutine      
def update_emotions(index_name, routing_id, emotion_id, pos_neg):
    logger.debug("[update] %s, %s" %(emotion_id, pos_neg))
    
    request_body = {
      "script":{
        "source": "ctx._source.put('emotion_type', params.emotion_type); ctx._source.put('upd_datetime', params.upd_datetime);",
        "params" : {
          "emotion_type" : pos_neg,
          "upd_datetime" : str(dt.now().strftime('%Y-%m-%dT%H:%M:%S'))
        }
      }
    }
    
    #es = AsyncElasticsearch(hosts=['%s:%d'%(es_ip, es_port)])
    es = AsyncElasticsearch(hosts=[es_ip])
    request_params= {
        'routing' : routing_id
    }
    r = yield from es.update(index_name, TYPE_DOC, emotion_id, body=request_body, params=request_params)
    
    es.transport.close()
    


def request2tousflux(tousflux_ip, tousflux_port, num, text):
    sentiment = None
    
    tousflux_conn = http.client.HTTPConnection(tousflux_ip, tousflux_port)
    
    request = {
        'sentence' : text,
        'authinit' : 'auth'                    
    }
    tousflux_conn.request("GET", "/SC_EvaluationService.svc/"+num+"?" + urllib.parse.urlencode(request, 'utf-8'), "", { "Content-Type" : "application/json; charset=utf-8" })
    
    result = str(tousflux_conn.getresponse().read())
    
    if len(result.split("|"))>=5 and result.split("|")[3]!="":
        sentiment = sentiment_score[result.split("|")[3]]
    
    return sentiment




def main(tousflux_ip, tousflux_port, number_of_modules, module_number):
    loop = None
    
    #1. 지속적으로 emotion_type이 없는 문서를 찾아서 업데이트.
    #   depth2_seq 를 모들의 개수만큼 나눴을 때 그 나머지가 일치하는 항목들만 업데이트.
    es_request = {
      "query": {
        "bool": {
          "must_not": [
            {
              "exists": {
                "field": "emotion_type"
              }
            }
          ],
          "filter": [
                {
                    "script": {
                      "script": {
                        "source": '''
                            def docDateTime = doc['_index'].getValue();
                            def val = Integer.parseInt(docDateTime.substring(docDateTime.lastIndexOf('.')+1));
                            val % params.number_of_modules == params.module_number
                        ''',
                        "lang": "painless",
                        "params": {
                          "number_of_modules" : number_of_modules,
                          "module_number" : module_number
                        }
                      }
                    }
                },
                {
                   "term" : {
                        "relation_name" : "emotions"
                    }
                }
            ]
        }
      }
    }
    
    print(json.dumps(es_request))
    
    try:
        es_conn = http.client.HTTPConnection(es_ip, es_port)
        es_conn.request("POST", "/"+INDEX_EMOTIONS+"/"+TYPE_DOC+"/_count", json.dumps(es_request), { "Content-Type" : "application/json" })
        es_result = es_conn.getresponse().read()
        
        if 'count' not in json.loads(es_result):
            logger.error("[main] 'count' not in the result.")
            return
        elif json.loads(es_result)['count'] <= 0:
            logger.info("[main] result size 0.")
            time.sleep(300)
            return

        total = json.loads(es_result)['count']
                
        # 2. 본격적으로  데이터 스캔
        logger.info("<TOTAL:%d>"%total)
        print("[%d]"%total, end="")
        sys.stdout.flush()
            
        # scroll_id 여부에 따라 request 데이터 값과 url 을 바꿈.
        es_request_per_page = copy.copy(es_request)
        es_request_per_page['size'] = PAGE_SIZE        # page size를 바꿈
        es_request_per_page['_source'] = ["depth2_seq", "matched_text"]
        '''
        "sort" : [
            {
                "doc_datetime": {
                  "order": "desc"
                }
            }
        ]
        '''
        es_request_per_page['sort'] = [{"doc_datetime" : { "order" : "desc" }}]
        
        
        es_url = "/"+INDEX_EMOTIONS+"/"+TYPE_DOC+"/_search"
            
        try:
            es_conn = http.client.HTTPConnection(es_ip, es_port)
            es_conn.request("POST", es_url, json.dumps(es_request_per_page), { "Content-Type" : "application/json" })

            es_result = json.loads(es_conn.getresponse().read())
            
            if 'hits' in es_result and es_result['hits']['total']>0:
                #scroll_id = es_result['_scroll_id']
                resultlist = es_result['hits']['hits']
                
                for idx, doc in enumerate(resultlist):
                    #print_process(idx)
                    
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(asyncio.ensure_future( update_emotions(doc['_index'], doc['_routing'], doc['_id'], request2tousflux(tousflux_ip, tousflux_port, str(idx+1).zfill(2), doc['_source']['matched_text']['string']) ) ))
    
                # update 후 refresh 해야 결과가 반영됨.
                #refresh_index(INDEX_EMOTIONS)
                # 결과 건수가 0이 될 때까지 무한하게 돈다.
            
        except OSError as oserror:
            logger.error("[main] oserror.(%s)"%str(oserror))
            slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                         text="[main] oserror.(%s)\n-URL:%s\n-RequestBody:%s"%(str(oserror), es_url, json.dumps(es_request_per_page)))
        except:
            ex = traceback.format_exc()
            
            logger.error("[main] error. Traceback >> %s" % ex)
            slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                         text="[main] error.\n-URL:%s\n-RequestBody:%s \n traceback >>> %s "%(es_url, json.dumps(es_request_per_page), ex))
            
    except OSError as oserror:
        logger.error("[main] es_conn error. (%s)"%str(oserror))
        slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                     text="[main] es_conn error.(%s)\n-URL:%s\n-RequestBody:%s"%(str(oserror), "/"+INDEX_EMOTIONS+"/"+TYPE_DOC+"/_search", json.dumps(es_request)))
    except:
        ex = traceback.format_exc()
        
        logger.error("[main] error. Traceback >> %s" % ex)        
        slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                     text="[main] error.\n-URL:%s\n-RequestBody:%s\n traceback >>> %s"%("/"+INDEX_EMOTIONS+"/"+TYPE_DOC+"/_search", json.dumps(es_request), ex))

        
  
  
def refresh_index(index):
    es_conn = http.client.HTTPConnection(es_ip, es_port)
    es_conn.request("POST", "/"+index+"/_refresh", "", { "Content-Type" : "application/json" })
    logger.info("[refresh_index] /"+index+"/_refresh >>> %s" % es_conn.getresponse().read())
  
                       
                       
                             
def print_process(idx):
    if idx % 10 == 0:
        logger.info("[%d]"%idx)
        
        print("[%d]"%idx, end="")
        sys.stdout.flush()
    else:
        print(".", end="")
        sys.stdout.flush()
        
        
        

if __name__ == '__main__':
    tousflux_ip = sys.argv[1]
    tousflux_port = int(sys.argv[2])
    number_of_modules = int(sys.argv[3])
    module_number = int(sys.argv[4])
    
    main(tousflux_ip, tousflux_port, number_of_modules, module_number)
