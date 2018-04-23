# -*- coding: utf-8 -*-
'''
Created on 2017. 8. 4.

@author: Holly
'''
from aioes import Elasticsearch
from datetime import datetime as dt
from _elementtree import ParseError
from elasticsearch import helpers, exceptions
from com.wisenut.enums.query import Query
import com.wisenut.dao.teaclient as teaclient
import com.wisenut.myLogger as myLogger
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
from com.wisenut.dao import mariadbclient

############# setting slack
slack = slackweb.Slack("https://hooks.slack.com/services/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")
slackChannel = "#kdic-error-alert"
slackUserName = "topic_exporter-kdic"
slackIconEmoji = ":ghost:"

PAGE_SIZE=1000
TOPICS_TO_SEARCH="topics-*"
INDEX_DOCUMENTS="documents-*"
TYPE_DOC="doc"

es_ip = "211.39.140.96"
es_port = 9201

URL_PATTERN=re.compile("(http[s]*:\/\/[www.]*|\.co[m]*|\.net|\.kr|\.or[g]*|\.htm[l]*|\.php|\.edu|\.jsp|\.aspx|\.asp)")
ETC_PATTERN=re.compile("복사|번역|기타|기능|URL|본문|보기|RT|사주|naver")


class EsError(Exception):
    pass



class NoMecabAvailable(Exception):
    pass



async def related_word_extractor(parent_docid, doc_datetime, term, debug=False):
    es = Elasticsearch(['%s:%d'%(es_ip, es_port)])
    #print("%s %d" % ((es_ip, es_port)))
    highlight_req =  {
            "_source" : [""],
             "query": {
                "bool": {
                  "filter": [
                    {
                      "term": {
                        "_id": parent_docid
                      }
                    },
                    {
                      "query_string": {
                        "query": term,
                        "fields": ["doc_title", "doc_content"],
                        "default_operator": "AND"
                      }
                    }
                  ]
                }
              },
              "highlight": {
                "fields": {
                  "_all" : {},
                  "doc_title": {
                    "type": "plain",
                    "fragment_size": 30,
                    "number_of_fragments": 1,
                    "fragmenter": "simple"
                  },
                  "doc_content": {
                    "type": "plain",
                    "fragment_size": 30,
                    "number_of_fragments": 3,
                    "fragmenter": "simple"
                  }
                }
              }
             }
    

    result = await es.search(index=INDEX_DOCUMENTS+re.sub("-" , ".", doc_datetime[:doc_datetime.find("T")]), doc_type=TYPE_DOC, body=highlight_req)
        
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
            # url이 포함된 항목들은 문장에서 제거
            f = URL_PATTERN.sub("", f)
            f = ETC_PATTERN.sub("", f)
            related += await get_close_word(f, debug)
    
    es.close()
    
    return list(filter(lambda x:len(x)>1, list(sorted(set(related), key=lambda x:related.index(x)))))




async def get_close_word(text, debug=False):
    if debug : print("### text : {}".format(text))

    # <em> 태그로 감싸진 단어를 기준으로 문장을 나눔    
    HL_PATTERN = re.compile("<em>[가-힣ㄱ-ㅎa-zA-Z\s]*</em>[가-힣ㄱ-ㅎa-zA-Z]*")
    
    ret = []
    for token in filter(lambda x:x, HL_PATTERN.split(text)): # 검색된 단어들(em으로 감싸인 단어)을 제외한 나머지 단어 묶음을 추출
        ret = await analyze(token.strip(), debug)
        
    return ret




async def analyze(text, debug=False):
    from subprocess import run, PIPE
    
    mecabBin = ''
    mecabDic = ''
    if sys.platform.find('linux')>=0:
        mecabBin = '/usr/local/bin/mecab'
        mecabDic = '/usr/local/lib/mecab/dic/mecab-ko-dic/'
    elif sys.platform.find('win')>=0:
        mecabBin = 'C:\\mecab\\mecab'
        mecabDic = 'C:\\mecab\\mecab-ko-dic'
    else:
        raise NoMecabAvailable 
    
    p = run([mecabBin, '-d', mecabDic], stdout=PIPE, input=text, encoding="UTF-8")
    result = p.stdout.split("\n")
    
    if debug : print(p.stdout)
    
    ret=[]
    for x in result:
        if x.find("\t")>=0:
            word = x.split("\t")[0]
            tag = x.split("\t")[1]
            
            #if tag.split(",")[0] in ["MM", "NNG", "NNP", "XPN", "XR", "XSN", "SN", "NNBC",  "SL", ]:
            #if tag.split(",")[0] in ["MM", "NNG", "NNP", "XPN", "XR", "SL", ]:
            if tag.split(",")[0] in ["NNG", "NNP"]:
                ret.append(word)
            #elif tag.split(",")[0] in ["VV", "VA"]: #, "XSV"
            #    ret.append(word+"다")
        else:
            break

    #if debug : print("".join(ret))
    
    #return "".join(ret)
    return ret



        
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
    inline_script+="ctx._source.is_a_real_verb=params.is_a_real_verb;"
    
    
    if len(term.strip())>0:
        topic_dict = {}
        
        topic = term.split(teaclient.WEIGHT_DELIMITER)[0]
        topic_id = md5Generator([data["_id"], topic])
        topic_attr = '' #get_topic_attr(topic)
        related_words = await related_word_extractor(data['_id'], data['_source']['doc_datetime'], topic) if topic_class == 'NN' else ''
        
        topic_dict =  {
            "topic_id" : topic_id,
            "topic" : topic,
            "topic_attr" : topic_attr,
            "topic_class" : 'NN' if topic_class == 'NN' else 'VV',
            "related_words" : related_words,
            "doc_id" : data['_id'],
            "doc_datetime" : data['_source']['doc_datetime'],
            "doc_url" : data['_source']['doc_url'],
            "doc_writer" : data['_source']['doc_writer'],
            "doc_title" : data['_source']['doc_title'].replace("\\", "").replace("\n", "").replace("\r", ""),
            "doc_content" : data['_source']['doc_content'].replace("\\", "").replace("\n", "").replace("\r", ""),
            "depth1_seq" : data['_source']['depth1_seq'],
            "depth2_seq" : data['_source']['depth2_seq'],
            "depth3_seq" : data['_source']['depth3_seq'],
            "depth1_nm" : data['_source']['depth1_nm'],
            "depth2_nm" : data['_source']['depth2_nm'],
            "depth3_nm" : data['_source']['depth3_nm'],
            "project_seq" : data['_source']['project_seq'],
            "upd_datetime" : get_current_datetime(),
            "is_a_real_verb" : "N" if topic_class == 'NN' else 'Y'
        }
        
        bulk_body = {
            "_op_type": "update",
            "_index": await find_to_which_index(topic_id, data['_source']['doc_datetime']),
            "_type": TYPE_DOC,
            "_id": topic_id,
            "_source": {
                "script": {
                    "lang" : "painless",
                    "inline": inline_script, 
                    "params": topic_dict
                },
                "upsert": topic_dict
            }
        }
        '''
        bulk_body = [
            {
                "update" : {
                    "_index": find_to_which_index(topic_id, data['_source']['doc_datetime']),
                    "_type": TYPE_DOC,
                    "_id": topic_id
                }
            },
            {
                "script": {
                    "lang" : "painless",
                    "inline": inline_script, 
                    "params": topic_dict
                },
                "upsert": topic_dict
            }
        ]
        '''
        
    #await asyncio.sleep(1)
    return bulk_body  # ======> 여기서 바로  bulk를 넣어버림.




async def find_to_which_index(topic_id, doc_datetime):
    return "topics-" + re.sub("-", ".", doc_datetime[:doc_datetime.find('T')])




async def time_log():
    i = 0
    print("time log starts.")
    while True:
        await asyncio.sleep(1)
        i += 1
        print('...%02d sec.' % (i,))




def insert_topics(data):
    #es = Elasticsearch(['%s:%d'%(es_ip, es_port)])
    
    some_bulks = ''
    bulk_result=0
    #bulk_result = None
    
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
            
            # 2018.03.26 terms와 verbs에 모두 등장하면 명사형으로 간주.
            newDict = {}
            for cl, topic in terms+verbs:
                if topic in newDict:
                    newDict[topic]['cnt'] += 1
                else:
                    newDict[topic] = {'cnt' : 1, 'topic_class' : cl }
            
            newArr = []
            for x in newDict.items():
                if x[1]['cnt'] > 1 or x[1]['topic_class']=='NN':
                    newArr.append(('NN', x[0]))
                else:
                    newArr.append(('VV', x[0]))
                        
            
            from elasticsearch import Elasticsearch
            es_client=Elasticsearch(":".join([es_ip, str(es_port)]))
            
            try:
                #fts = [ make_bulk(t, data) for t in (terms+verbs) ]
                fts = [ make_bulk(t, data) for t in (newArr) ]
                #t.cancel()
                some_bulks = yield from asyncio.gather(*fts)
                '''
                thisBulk = [
                  [
                    {'update' : { '_index' : 'topics-2018.01.01', '_type' : 'doc', ....... },
                    {'topic' : '증권', 'topic_id' : ..... }
                  ],
                  [
                    {'update' : { '_index' : 'topics-2018.01.01', '_type' : 'doc', ....... },
                    {'topic' : '은행', 'topic_id' : ..... },
                  ]
                  ...
                ]
                
                thisBulk = yield from asyncio.gather(*fts)
                some_bulks = [ y for x in thisBulk for y in x ]
                '''
                
                bulk_result += helpers.bulk(es_client, list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                #bulk_result = yield from es.bulk(filter(lambda x:x and len(x)>0, some_bulks))
                
            except EsError as e:
                retry = 0
                logger.error("[insert_topics] %s (retry:%d)"%(str(e), retry))
                while retry <= 5:
                    retry += 1
                    print("10초 간 쉬었다가 다시!\n")
                    time.sleep(10)
                    
                    try:
                        print("색인 {0}번째 재시도..".format(retry))
                        bulk_result += helpers.bulk(es_client, list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                        #bulk_result = yield from es.bulk(filter(lambda x:x and len(x)>0, some_bulks))
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
                        bulk_result += helpers.bulk(es_client, list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                        #bulk_result = yield from es.bulk(filter(lambda x:x and len(x)>0, some_bulks))
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
                        bulk_result += helpers.bulk(es_client, list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                        #bulk_result = yield from es.bulk(filter(lambda x:x and len(x)>0, some_bulks))
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
                        bulk_result += helpers.bulk(es_client, list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                        #bulk_result = yield from es.bulk(filter(lambda x:x and len(x)>0, some_bulks))
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
                        bulk_result += helpers.bulk(es_client, list(filter(lambda x:x and len(x)>0, some_bulks)), refresh=True)[0]
                        #bulk_result = yield from es.bulk(filter(lambda x:x and len(x)>0, some_bulks))
                        break
                    except urllib3.exceptions.NewConnectionError as connectionError:
                        logger.error("[insert_topics] %s (retry:%d)"%(str(connectionError), retry))
                        continue
            except:
                ex = traceback.format_exc()
                logger.error("[insert_topics] unknown error. Traceback >> %s " % ex)

            logger.debug("%d are successfully inserted."%bulk_result)
            #logger.debug("%d are successfully inserted."%len(bulk_result['items']))
       
    except ParseError as xmlerror:
        logger.error("[insert_topics] TeaClient failed. (%s)"%str(xmlerror))
        logger.error("==============> teaclient's xml response : %s" % result)

    #es.close()
    
    



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
        # 여러 프로젝트 seq 가 들어오더라도 모두 filter keyword가 동일하므로 첫번째 project_seq만 사용.
        filter.append(queryObj.get_project_filter_query(params['project_seqs'].split(",")[0]))
    
        # 대상 채널
        if "channels" in params and params["channels"] and params["channels"] != 'all':
            filter.append(queryObj.get_channel_query())
    
        # 대상 기간
        if "start_date" in params and "end_date" in params:
            filter.append(queryObj.get_period_query(params['mode']))
            
        request["query"]["bool"]["filter"] = filter
        request["query"]["bool"]["must"] = queryObj.get_total_dataset_query(params['project_seqs'])
    
    
    logger.debug("[get_request_query] Query >>> %s " % json.dumps(request) )
    
    return request




def main(mode, project_seqs, start_date, end_date):
    scroll_id = None
    
    #1. emotions가 없거나, topics에 값이 없는 crawl_doc 데이터의 개수를 가져옴.
    es_request = get_request_query({ "mode":mode, "start_date":start_date, "end_date":end_date, "project_seqs":project_seqs }, scroll_id)
    
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
                        if idx % 100 == 0:
                            logger.info("[%d]"%idx)
                            
                            print("[%d]"%idx, end="")
                            sys.stdout.flush()
                        else:
                            print(".", end="")
                            sys.stdout.flush()
                            
                        loop = asyncio.get_event_loop()
                        loop.run_until_complete(asyncio.ensure_future(insert_topics(doc)))
                        
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
    logger = myLogger.getMyLogger('kdic-topics-' + mode, hasConsoleHandler=False, hasRotatingFileHandler=True, logLevel=logging.DEBUG)
    
    
    logger.info("=================================================================")
    logger.info("- ES Connection %s %d" % (es_ip, es_port) )
    logger.info("- mode\t\t:\t%s" % mode)
    logger.info("- project_seqs\t:\t%s" % ','.join(str(seq[0]) for seq in project_seqs))
    logger.info("- start_date\t:\t%s" % start_date)
    logger.info("- end_date\t:\t%s" % end_date)
    logger.info("=================================================================")

    
    #for project_seq in project_seqs.split(","):
        #logger.info(">>>>> project_seq  %s" % project_seq)
        #print(">>>>> project_seq  %s" % project_seq)
        
    main(mode, ','.join(str(seq[0]) for seq in project_seqs), start_date, end_date)
        
    '''
    print(get_request_query({ "mode":mode, "start_date":start_date, "end_date":end_date, "project_seqs":project_seqs }))
    '''