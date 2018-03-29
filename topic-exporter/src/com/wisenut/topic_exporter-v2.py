# -*- coding: utf-8 -*-
'''
Created on 2017. 8. 4.

@author: Holly
'''
from com.wisenut.config import Config
from aioes import Elasticsearch
from aioes.client import IndicesClient
from aioes.client import CatClient
from aiohttp import ClientSession
from datetime import datetime as dt
from _elementtree import ParseError
from elasticsearch import helpers, exceptions
import com.wisenut.dao.teaclient as teaclient
import asyncio
import xml.etree.ElementTree as et
import logging
import time
import json
import re
import math
import copy
import http
import hashlib
import aiohttp
import urllib3
import slackweb
import traceback
import sys

slack = slackweb.Slack("https://hooks.slack.com/services/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")


PAGE_SIZE=1000
INDEX_DOCUMENTS="documents-*"
TYPE_DOC="doc"

es_ip = "211.39.140.59"
es_port = 9200

############# logger 세팅
conf = Config()
logging.config.fileConfig(conf.get_logconfig_path())
logger = logging.getLogger(__name__)

logger.info("ES Connection %s %d" % (es_ip, es_port) )


class EsError(Exception):
    pass
    
    
async def get_recent_index(index):
    es = Elasticsearch(['%s:%d'%(es_ip, es_port)])
    cat2es = CatClient(es)
    result = await cat2es.indices(index, h="index")
    '''
    es_conn = http.client.HTTPConnection(es_ip, es_port)
    es_conn.request("GET", "_cat/indices/"+index+"?h=index&s=index:desc", "", { "Content-Type" : "application/json" })
    es_result = es_conn.getresponse().read().decode('utf-8')
    '''
    es_result = result
    idx_list = sorted([ idx for idx in es_result.split("\n")], reverse=True)
    
    if len(es_result)>0:
        return idx_list[0].strip()
    else:
        raise EsError


async def url_request(url):
    async with ClientSession() as session:
        async with session.get(url) as response:
            r = await response.read()
            return r

async def related_word_extractor(parent_docid, term, debug=False):
    #print("%s %d" % ((es_ip, es_port)))
    highlight_req =  {
            "_source" : [""],
             "query": {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "_id": parent_docid
                      }
                    },
                    {
                      "query_string": {
                        "query": term,
                        "fields": ["doc_title", "doc_content"]
                      }
                    }
                  ]
                }
              },
              "highlight": {
                "fields": {
                  "_all" : {},
                  "doc_title": {
                    "fragment_size": 30,
                    "number_of_fragments": 1,
                    "fragmenter": "simple"
                  },
                  "doc_content": {
                    "fragment_size": 30,
                    "number_of_fragments": 3,
                    "fragmenter": "simple"
                  }
                }
              }
             }
    
    es = Elasticsearch(['%s:%d'%(es_ip, es_port)])

    result = await es.search(index=INDEX_DOCUMENTS, doc_type=TYPE_DOC, body=highlight_req)
        
    related = []
    if result['hits']['total']>0:
        title_fragments = []
        content_fragments = []
        for a in result['hits']['hits']:
            if 'doc_title' in a['highlight']:
                title_fragments = [ fragment for fragment in a['highlight']['doc_title']  ]
            if 'doc_content' in a['highlight']:
                content_fragments = [ fragment for fragment in a['highlight']['doc_content'] ]

        for f in (title_fragments+content_fragments):
            related += await get_close_word(f, debug)
                    
    return list(filter(lambda x:len(x)>1, list(sorted(set(related), key=lambda x:related.index(x)))))

async def get_close_word(text, debug=False):
    if debug : print("### text : {}".format(text))
    ret = []
    
    hl_p = re.compile("<em>[가-힣ㄱ-ㅎa-zA-Z\s]*</em>[가-힣ㄱ-ㅎa-zA-Z]*")
    not_word_p = re.compile("[^가-힣ㄱ-ㅎㅏ-ㅣa-zA-Z0-9]")
    
    for token in filter(lambda x:x, hl_p.split(text)): # 검색된 단어들(em으로 감싸인 단어)을 제외한 나머지 단어 묶음을 추출
        if debug : print("* "+token)
        for x in filter( lambda x:x, not_word_p.split(token.strip()) ): # 나머지 단어들을 기호나 스페이스로 분리
            word = await analyze(x.strip(), debug)
            if word.strip():
                ret.append(word)

    return ret

async def analyze(text, debug=False):
    
    es = Elasticsearch(['%s:%d'%(es_ip, es_port)])
    client = IndicesClient(es)
    
    index_name = await get_recent_index(INDEX_DOCUMENTS)

    result = await client.analyze(index=index_name, analyzer="korean", body={"text" :text})
    
    ret=[]
    for x in result['tokens']:
        token = x["token"]
        type = x["type"]
        # 명사 실질형태소와 외국어만 추출
        if type not in ["COMPOUND","EOJEOL","INFLECT","VV","VA","VX","VCP","VCN","NNB","E","JKS","JKC","JKG","JKO","JKB","JKV","JKQ","JX","JC","EP","EF","EC","ETN","ETM","XPN","XSN","XSV","XSA","SF","SE","SS","SN","SP","SO","SW","SH"]:
            if debug : print("{}==>{}/{}".format(text, token, type))
            if(type in ["VV","VA"]):
                ret.append(token[:token.find("/V")]+"다")
            else:
                ret.append(token)
        else:
            if debug : print("XXX {}==>{}/{}".format(text, token, type))

    return "".join(ret)
        
async def make_bulk(class_term_tuple, data):
    bulk_body = None

    term = class_term_tuple[1]
    topic_class = class_term_tuple[0]
    
    inline_script = ""
    inline_script+="ctx._source.project_seq?.addAll(params.project_seq); Set hs = new HashSet(); hs.addAll(ctx._source.project_seq); ctx._source.project_seq.clear(); ctx._source.project_seq.addAll(hs);"
    inline_script+="ctx._source.topic_id=params.topic_id;"
    inline_script+="ctx._source.topic=params.topic;"
    inline_script+="ctx._source.topic_attr=params.topic_attr;"
    inline_script+="ctx._source.topic_class=params.topic_class;"
    inline_script+="ctx._source.related_words=params.related_words;"
    inline_script+="ctx._source.upd_datetime=params.upd_datetime;"
    
    
    if len(term.strip())>0:
        topic_dict = {}
        
        topic = term.split(teaclient.WEIGHT_DELIMITER)[0]
        topic_id = md5Generator([data["_id"], topic])
        topic_attr = '' #get_topic_attr(topic)
        related_words = await related_word_extractor(data['_id'], topic) if topic_class == 'NN' else ''
        
        topic_dict =  {
            "project_seq" : data['_source']['project_seq'],
            "topic_id" : topic_id,
            "topic" : topic,
            "topic_attr" : topic_attr,
            "topic_class" : 'NN' if topic_class == 'NN' else 'VV',
            "related_words" : related_words,
            "upd_datetime" : get_current_datetime()
        }
        
        bulk_body = {
            "_op_type": "update",
            "_index": await find_to_which_index(data['_source']['doc_datetime']),
            "_type": TYPE_DOC,
            "_id": topic_id,
            "_routing" : data['_id'],
            "_source": {
                "script": {
                    "source": inline_script, 
                    "params": topic_dict
                },
                "upsert": topic_dict
            }
        }
        
    #await asyncio.sleep(1)
    return bulk_body  # ======> 여기서 바로  bulk를 넣어버림.


async def find_to_which_index(doc_datetime):   
    return "topics-" + re.sub("-", ".", doc_datetime[:doc_datetime.find('T')])




async def time_log():
    i = 0
    print("time log starts.")
    while True:
        await asyncio.sleep(1)
        i += 1
        print('...%02d sec.' % (i,))

def insert_topics(data):
    some_bulks = ''
    bulk_result=0
    
    try:
        result = teaclient.request(data)
        #print(result)
        
        root = et.fromstring(result)
        status = root.findall("./results/result[@name='status']")[0].text if len(root.findall("./results/result[@name='status']"))>0 else ''
        #print(">>> Tea client response : %s" % status)
        
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
            #t = asyncio.ensure_future(time_log())
            terms = [ ('NN', term) for term in terms.split(teaclient.ITEM_DELIMITER)]
            verbs = [ ('VV', verb) for verb in verbs.split(teaclient.ITEM_DELIMITER)]
                        
            
            from elasticsearch import Elasticsearch
            es_client=Elasticsearch(":".join([es_ip, str(es_port)]))
            try:
                fts = [ make_bulk(t, data) for t in (terms+verbs) ]
                #t.cancel()
                some_bulks = yield from asyncio.gather(*fts)
                
                bulk_result += helpers.bulk(es_client, list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                
            except EsError as e:
                retry = 0
                logger.error("[insert_topics] %s (retry:%d)"%(str(e), retry))
                while retry <= 5:
                    retry += 1
                    print("10초 간 쉬었다가 다시!\n")
                    time.sleep(10)
                    
                    try:
                        print("색인 {0}번째 재시도..".format(retry))
                        bulk_result += helpers.bulk(es_client,  list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                        break
                    except EsError as e:
                        logger.error("[insert_topics] %s (retry:%d)"%(str(e), retry))
                        continue    
            
            except exceptions.ConnectionTimeout as timeoutError:
                retry = 0
                logger.error("[insert_topics] %s (retry:%d)"%(str(timeoutError), retry))
                while retry <= 5:
                    retry += 1
                    print("10초 간 쉬었다가 다시!\n")
                    time.sleep(10)
                    
                    try:
                        print("색인 {0}번째 재시도..".format(retry))
                        bulk_result += helpers.bulk(es_client,  list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                        break
                    except exceptions.ConnectionTimeout as timeoutError:
                        logger.error("[insert_topics] %s (retry:%d)"%(str(timeoutError), retry))
                        continue
            except aiohttp.client_exceptions.ClientConnectorError as connectError:
                retry = 0
                logger.error("[insert_topics] %s (retry:%d)"%(str(connectError), retry))
                while retry <= 5:
                    retry += 1
                    print("10초 간 쉬었다가 다시!\n")
                    time.sleep(10)
                    
                    try:
                        print("색인 {0}번째 재시도..".format(retry))
                        bulk_result += helpers.bulk(es_client,  list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                        break
                    except aiohttp.client_exceptions.ClientConnectorError as connectError:
                        logger.error("[insert_topics] %s (retry:%d)"%(str(connectError), retry))
                        continue
            except OSError as oserror:
                retry = 0
                logger.error("[insert_topics] %s (retry:%d)"%(str(oserror), retry))
                while retry <= 5:
                    retry += 1
                    print("10초 간 쉬었다가 다시!\n")
                    time.sleep(10)
                    
                    try:
                        print("색인 {0}번째 재시도..".format(retry))
                        bulk_result += helpers.bulk(es_client,  list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                        break
                    except OSError as oserror:
                        logger.error("[insert_topics] %s (retry:%d)"%(str(oserror), retry))
                        continue
            except urllib3.exceptions.NewConnectionError as connectionError:
                retry = 0
                logger.error("[insert_topics] %s (retry:%d)"%(str(connectionError), retry))
                while retry <= 5:
                    retry += 1
                    print("10초 간 쉬었다가 다시!\n")
                    time.sleep(10)
                    
                    try:
                        print("색인 {0}번째 재시도..".format(retry))
                        bulk_result += helpers.bulk(es_client,  list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                        break
                    except urllib3.exceptions.NewConnectionError as connectionError:
                        logger.error("[insert_topics] %s (retry:%d)"%(str(connectionError), retry))
                        continue
            except:
                ex = traceback.format_exc()
                logger.error("[insert_topics] unknown error. Traceback >> %s " % ex)

            logger.debug("%d are successfully inserted."%bulk_result)
       
    except ParseError as xmlerror:
        logger.error("[insert_topics] TeaClient failed. (%s)"%str(xmlerror))
        logger.error("==============> teaclient's xml response : %s" % result)


def main(project_seqs, start_date, end_date):
    scroll_id = None
    
    #1. emotions가 없거나, topics에 값이 없는 crawl_doc 데이터의 개수를 가져옴.
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
                 "terms" : {
                    "project_seq" : [ seq for seq in project_seqs.split(",") ]
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
            time.sleep(300)
            return
        
        total = json.loads(es_result)['count']
                
        logger.info("<TOTAL:%d>"%total)
        print("[%d]"%total, end="")
        sys.stdout.flush()
        
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
                
                if 'hits' in es_result:
                    scroll_id = es_result['_scroll_id']
                    resultlist = es_result['hits']['hits']
                    
                    logger.info("<Total:%d>"%es_result['hits']['total'] )
                    for idx, doc in enumerate(resultlist):
                        loop = asyncio.get_event_loop()
                        loop.run_until_complete(asyncio.ensure_future(insert_topics(doc)))
                        
                        if idx % 100 == 0:
                            logger.info("[%d]"%idx)
                            
                            print("[%d]"%idx, end="")
                            sys.stdout.flush()
                        else:
                            print(".", end="")
                            sys.stdout.flush()
                        
            except OSError as oserror:
                logger.error("[main] oserror.(%s)"%str(oserror))
                slack.notify(text="[main] oserror.(%s)\n-URL:%s\n-RequestBody:%s"%(str(oserror), es_url, json.dumps(es_request_per_page)))
                continue
            except:
                ex = traceback.format_exc()
                
                logger.error("[main] error. Traceback >> %s" % ex)
                slack.notify(text="[main] error.\n-URL:%s\n-RequestBody:%s \n traceback >>> %s "%(es_url, json.dumps(es_request_per_page), ex))
                continue
            
            
                    
    except OSError as oserror:
        logger.error("[main] es_conn error. (%s)"%str(oserror))
        slack.notify(text="[main] es_conn error.(%s)\n-URL:%s\n-RequestBody:%s"%(str(oserror), "/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_search", json.dumps(es_request)))
    except:
        ex = traceback.format_exc()
        
        logger.error("[main] error. Traceback >> %s" % ex)        
        slack.notify(text="[main] error.\n-URL:%s\n-RequestBody:%s\n traceback >>> %s"%("/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_search", json.dumps(es_request), ex))
    
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
            slack.notify(text="[main] es_conn error.(%s)\n-URL:%s\n-RequestBody:%s"%(str(oserror), "/_search/scroll", json.dumps({"scroll_id":scroll_id})))
        except:
            ex = traceback.format_exc()
        
            logger.error("[main] error. Traceback >>> %s" % ex)
            slack.notify(text="[main] error.\n-URL:%s\n-RequestBody:%s\n traceback >>> %s"%("/"+INDEX_DOCUMENTS+"/"+TYPE_DOC+"/_search", json.dumps(es_request), ex))
                                


def get_current_datetime():
    ymdhms = str(dt.now().strftime('%Y-%m-%dT%H:%M:%S'))
    
    return ymdhms    
                


def md5Generator(arr):
    m = hashlib.md5()
    #for e in arr:
    m.update(repr(arr).encode('utf-8'))
    return m.hexdigest()


if __name__ == '__main__':
    
    project_seqs = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]

    main(project_seqs, start_date, end_date)
    '''
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.ensure_future(get_recent_index("topics*")))
    '''