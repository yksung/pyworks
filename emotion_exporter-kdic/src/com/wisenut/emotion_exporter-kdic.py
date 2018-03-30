# -*- coding: utf-8 -*-
from com.wisenut.config import Config
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
from com.wisenut.enums.query import Query
from com.wisenut import myLogger

############# setting config
conf = Config()

############# setting elasticsearch
es_ip = conf.get_es_ip()
es_port = conf.get_es_port()

############# setting slack
slack = slackweb.Slack("https://hooks.slack.com/services/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")
slackChannel = "#kdic-error-alert"
slackUserName = "emotion_exporter-kdic"
slackIconEmoji = ":ghost:"

############# setting search options
PAGE_SIZE=1000
INDEX_EMOTIONS="emotions"
INDEX_DOCUMENTS="documents-*"
TYPE_DOC="doc"




def request2bica(ip, port, concept_id, text):
    bica_conn = http.client.HTTPConnection(ip, port)
    
    bica_request = {
        'data' : text,
        'conceptID' : concept_id                    
    }
    bica_conn.request("POST", "/request.is?" + urllib.parse.urlencode(bica_request, 'utf-8'), "", { "Content-Type" : "application/json" })
    
    return bica_conn.getresponse().read()



      
async def make_bulk(document, bica_result):
    bulk_body = None

    inline_script = ""
    inline_script+="ctx._source.project_seq?.addAll(params.project_seq); Set hs = new HashSet(); hs.addAll(ctx._source.project_seq); ctx._source.project_seq.clear(); ctx._source.project_seq.addAll(hs);"
    
    inline_script+="ctx._source.emotion_id=params.emotion_id;"
    inline_script+="ctx._source.emotion_score=params.emotion_score;"
    inline_script+="ctx._source.emotion_type=params.emotion_type;"
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
        "emotion_type" : None,
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
        "upd_datetime" : None
    }
    
    bulk_body = {
        "_op_type": "update",
        "_index": INDEX_EMOTIONS + "-" + document['_source']['doc_datetime'][0:document['_source']['doc_datetime'].find("T")].replace("-","."),
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




def get_request_query(params, scroll_id=None):
    queryObj = Query(params)

    if not scroll_id:
        request = {
            "query" : {
                "bool" : {
                }
            }
        }
    else:
        request = {
            "scroll" : "1d",
            "scroll_id" : scroll_id
        }

    if "query" in request:
        filter = []
        # 프로젝트 시퀀스 포함
        filter.append(queryObj.get_project_seq_query())
        filter.append(queryObj.get_project_filter_query(params['project_seq']))
    
        # 대상 채널
        if "channels" in params and params["channels"] and params["channels"] != 'all':
            filter.append(queryObj.get_channel_query())
    
        # 대상 기간
        if "start_date" in params and "end_date" in params:
            filter.append(queryObj.get_period_query(params['mode']))
            
        request["query"]["bool"]["filter"] = filter
        request["query"]["bool"]["must"] = queryObj.get_total_dataset_query(params['project_seq'])
    
    
    logger.debug("[get_documents] Query >>> %s " % json.dumps(request) )
    
    return request




def main(mode, project_seq, start_date, end_date):
    scroll_id = None
    
    #1. 분석하고자 하는 document list를 가져옴.
    es_request = get_request_query({ "mode":mode, "start_date":start_date, "end_date":end_date, "project_seq":project_seq }, scroll_id)
    
    logger.info("es_request >>> ") 
    logger.info(es_request)
    
    try:
        es_conn = http.client.HTTPConnection(es_ip, es_port)
        es_conn.request("POST", "/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_count", json.dumps(es_request), { "Content-Type" : "application/json" })
        es_result = es_conn.getresponse().read()
        
        if 'count' not in json.loads(es_result):
            logger.error("[main] 'count' not in the result.")
            logger.error('[main] es_result >>> %s ' % es_result)
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
                
                es_url = "/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_search?scroll=2d"
            
            # 검색 요청
            logger.debug("[main] es_request_per_page > %s %s" % (es_url, json.dumps(es_request_per_page)))
            
            try:
                es_conn = http.client.HTTPConnection(es_ip, es_port)
                es_conn.request("POST", es_url, json.dumps(es_request_per_page), { "Content-Type" : "application/json" })
                es_result = json.loads(es_conn.getresponse().read())
                
                if 'hits' in es_result:
                    scroll_id = es_result['_scroll_id']
                    resultlist = es_result['hits']['hits']
                    
                    logger.info("<Total:%d>"%es_result['hits']['total'] )
                    for idx, doc in enumerate(resultlist):
                        loop = asyncio.get_event_loop()
                        loop.run_until_complete(asyncio.ensure_future(insert_emotions(project_seq, doc)))
                        
                        if idx % 100 == 0:
                            logger.info("[%d]"%idx)
                            
                            print("[%d]"%idx, end="")
                            sys.stdout.flush()
                        else:
                            print(".", end="")
                            sys.stdout.flush()
                        
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
            
    except OSError as oserror:
        logger.error("[main] es_conn error. (%s)"%str(oserror))
        slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                     text="[main] es_conn error.(%s)\n-URL:%s\n-RequestBody:%s"%(str(oserror), "/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_search", json.dumps(es_request)))
    except:
        ex = traceback.format_exc()
        
        logger.error("[main] error. Traceback >> %s" % ex)        
        slack.notify(channel=slackChannel, username=slackUserName, icon_emoji=slackIconEmoji,
                     text="[main] error.\n-URL:%s\n-RequestBody:%s\n traceback >>> %s"%("/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_search", json.dumps(es_request), ex))
    
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
    
    
    logger.info("Finished successfully.")       
            
    
    
    
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
    mode = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    
    project_seqs = mariadbclient.get_all_projectseqs_of('kdic') # DB에서 kdic에 해당하는 project를 전체 가져와야함. 
    
    ############# setting logging
    logger = myLogger.getMyLogger('kdic-emotions-' + mode, hasConsoleHandler=False, hasRotatingFileHandler=True, logLevel=logging.DEBUG)
    
    logger.info("=================================================================")
    logger.info("- ES Connection %s %d" % (es_ip, es_port) )
    logger.info("- mode\t\t:\t%s" % mode)
    logger.info("- project_seqs\t:\t%s" % ','.join(str(seq[0]) for seq in project_seqs))
    logger.info("- start_date\t:\t%s" % start_date)
    logger.info("- end_date\t:\t%s" % end_date)
    logger.info("=================================================================")

    for project_seq in project_seqs:
        logger.info(">>>>> project_seq  %s" % str(project_seq[0]))
        print(">>>>> project_seq  %s" % str(project_seq[0]))
        
        main(mode, str(project_seq[0]), start_date, end_date)
    '''
    for project_seq in project_seqs.split(","):
        print(get_request_query({ "mode":mode, "start_date":start_date, "end_date":end_date, "project_seq":project_seq }))
    '''