'''
Created on 2018. 3. 27.

@author: Holly
'''
from datetime import datetime
from aioes import Elasticsearch
from elasticsearch import helpers, exceptions
import http.client
import json
import sys
import re
import asyncio
from com.wisenut import myLogger
import logging
import time
import aiohttp
import urllib3
import traceback
import math

PAGE_SIZE=1000
# =========== elasticsearch ===========
es_ip = "211.39.140.96"
es_port = 9201

INDEX_TOPICS="topics"
INDEX_DOCUMENTS="documents"
TYPE_DOC="doc"

# =========== Logger ===========
logger = myLogger.getMyLogger('related_word_sticker', hasConsoleHandler=False, hasRotatingFileHandler=True, logLevel=logging.DEBUG)


class EsError(Exception):
    pass



class NoMecabAvailable(Exception):
    pass



async def isNoun(text, debug=False):
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
    
    '''
    mecab의 3-best 결과를 가져와서 모든 형태소 분석 결과 중 어느 하나라도 동사의 형태를 띄는 결과가 나오면 동사로 취급.
    
    p.stdout =>
            차이나다    차    NNG,*,F,차,*,*,*,*
            이나다    NNP,인명,F,이나다,*,*,*,*
        EOS
            차이    NNG,*,F,차이,*,*,*,*
            나    VV,*,F,나,*,*,*,*
            다    EC,*,F,다,*,*,*,*
        EOS
            차이    NNG,*,F,차이,*,*,*,*
            나    VV,*,F,나,Inflect,VV,VV,날/VV/*
            다    EC,*,F,다,*,*,*,*
        EOS
    '''
    p = run([mecabBin, '-d', mecabDic, '-N', '3'], stdout=PIPE, input=text, encoding="UTF-8")
    if debug: print(p.stdout)
    
    result = re.split("(\n|EOS)", p.stdout)    
    
    '''
    tags =>
        ['NNG', 'NNP', 'NNG', 'VV', 'EC', 'NNG', 'VV', 'EC']
    '''
    tags = []
    for x in result:
        if x.find("\t")>=0:
            tag = x.split("\t")[1].split(",")[0].strip()
            if tag.find("+")>=0:
                tags += tag.split("+")
            else:   
                tags.append(tag) 
   
    if debug: print(tags, end="\t")
    
    ######## 해당 단어가 동사 혹은 동사형으로 끝나는 단어인지 판단
    inTheseTags = ["JX", "VCP", "VV", "VA", "XSV", "EC", "MAG", "JKB", "XR"]
    if ifAny(tags, inTheseTags):
        if debug:print("False") 
        return False
    else:
        if debug:print("True")
        return True
    
    
def ifAny(arr1, arr2):
    chk = False
    for e in arr1:
        if e in arr2:
            chk = True
            break
       
    return chk
        
    
    
    
    
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
    

    result = await es.search(index=INDEX_DOCUMENTS+"-"+re.sub("-" , ".", doc_datetime[:doc_datetime.find("T")]), doc_type=TYPE_DOC, body=highlight_req)
        
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
    
    es.close()
    
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
    
    ret=[]
    for x in result:
        if x.find("\t")>=0:
            word = x.split("\t")[0]
            tag = x.split("\t")[1]
            
            if tag.split(",")[0] in ["MM", "NNG", "NNP", "XPN", "XR", "XSN", "SN", "NNBC",  "SL", ]:
                ret.append(word)
            elif tag.split(",")[0] in ["VV", "VA"]: #, "XSV"
                ret.append(word+"다")
        else:
            break

    if debug : print("".join(ret))
    
    return "".join(ret)



def get_current_datetime():
    ymdhms = str(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
    return ymdhms    
            


async def makeUpdateBulks(docMetaInfo):
    topic_id = docMetaInfo['_id']
    doc_id = docMetaInfo['_source']['doc_id']
    doc_datetime = docMetaInfo['_source']['doc_datetime']
    topic = docMetaInfo['_source']['topic']
      
    bulk_body = None      
    ######## topic이 진짜 동사가 아니라면 연관어를 생성하고 bulk를 만듦.
    if await isNoun(topic):
        bulk_body = {
            "_op_type": "update",
            "_index": "topics-" + re.sub("-", ".", doc_datetime[:doc_datetime.find('T')]),
            "_type": TYPE_DOC,
            "_id": topic_id,
            "_source": {
                "script": {
                    "lang" : "painless",
                    "inline": """
                        ctx._source.topic_class=params.topic_class;
                        ctx._source.related_words=params.related_words;
                        ctx._source.is_a_real_verb=params.is_a_real_verb;
                        ctx._source.upd_datetime=params.upd_datetime;
                    """, 
                    "params": {
                        "topic_class" : "NN",
                        "related_words" : await related_word_extractor(doc_id, doc_datetime, topic),
                        "is_a_real_verb" : "N",
                        "upd_datetime" : get_current_datetime()
                    }
                }
            }
        }
    else:
        ######## VV 타입 topic은 is_a_real_verb만 업데이트하고
        ######## upd_datetime은 업데이트 안함(리포트에 추가 반영 안하기 위해서)
        bulk_body = {
            "_op_type": "update",
            "_index": "topics-" + re.sub("-", ".", doc_datetime[:doc_datetime.find('T')]),
            "_type": TYPE_DOC,
            "_id": topic_id,
            "_source": {
                "script": {
                    "lang" : "painless",
                    "inline": """
                        ctx._source.related_words=params.related_words;
                        ctx._source.is_a_real_verb=params.is_a_real_verb;
                    """, 
                    "params": {
                        "related_words" : None,
                        "is_a_real_verb" : "Y"
                    }
                }
            }
        }
    
    return bulk_body



def insertTopics(es_result):
    if 'hits' in es_result:
        ######## topic_class가 VV인 항목들을 검색해서 그 결과를 넘겨주고 bulk를 만듦.
        fts = [ makeUpdateBulks(x) for x in es_result['hits']['hits'] ]
        some_bulks = yield from asyncio.gather(*fts)
        
        ######## BULK INSERT
        from elasticsearch import Elasticsearch
        es_client=Elasticsearch(":".join([es_ip, str(es_port)]))
    
        bulk_result=0
        try:
            bulk_result += helpers.bulk(es_client, list(filter(lambda x:x is not None, some_bulks)), refresh=True)[0]
        except EsError as e:
            retry = 0
            logger.error("[insertTopics] %s (retry:%d)"%(str(e), retry))
            while retry <= 5:
                retry += 1
                print("10초 간 쉬었다가 다시!\n")
                time.sleep(10)
                
                try:
                    print("색인 {0}번째 재시도..".format(retry))
                    bulk_result += helpers.bulk(es_client, list(filter(lambda x:x is not None, some_bulks)), refresh=True)[0]
                    break
                except EsError as e:
                    logger.error("[insertTopics] %s (retry:%d)"%(str(e), retry))
                    continue    
        
        except exceptions.ConnectionTimeout as timeoutError:
            retry = 0
            logger.error("[insertTopics] %s (retry:%d)"%(str(timeoutError), retry))
            while retry <= 5:
                retry += 1
                print("10초 간 쉬었다가 다시!\n")
                time.sleep(10)
                
                try:
                    print("색인 {0}번째 재시도..".format(retry))
                    bulk_result += helpers.bulk(es_client, list(filter(lambda x:x is not None, some_bulks)), refresh=True)[0]
                    break
                except exceptions.ConnectionTimeout as timeoutError:
                    logger.error("[insertTopics] %s (retry:%d)"%(str(timeoutError), retry))
                    continue
        except aiohttp.client_exceptions.ClientConnectorError as connectError:
            retry = 0
            logger.error("[insertTopics] %s (retry:%d)"%(str(connectError), retry))
            while retry <= 5:
                retry += 1
                print("10초 간 쉬었다가 다시!\n")
                time.sleep(10)
                
                try:
                    print("색인 {0}번째 재시도..".format(retry))
                    bulk_result += helpers.bulk(es_client, list(filter(lambda x:x is not None, some_bulks)), refresh=True)[0]
                    break
                except aiohttp.client_exceptions.ClientConnectorError as connectError:
                    logger.error("[insertTopics] %s (retry:%d)"%(str(connectError), retry))
                    continue
        except OSError as oserror:
            retry = 0
            logger.error("[insertTopics] %s (retry:%d)"%(str(oserror), retry))
            while retry <= 5:
                retry += 1
                print("10초 간 쉬었다가 다시!\n")
                time.sleep(10)
                
                try:
                    print("색인 {0}번째 재시도..".format(retry))
                    bulk_result += helpers.bulk(es_client, list(filter(lambda x:x is not None, some_bulks)), refresh=True)[0]
                    break
                except OSError as oserror:
                    logger.error("[insertTopics] %s (retry:%d)"%(str(oserror), retry))
                    continue
        except urllib3.exceptions.NewConnectionError as connectionError:
            retry = 0
            logger.error("[insertTopics] %s (retry:%d)"%(str(connectionError), retry))
            while retry <= 5:
                retry += 1
                print("10초 간 쉬었다가 다시!\n")
                time.sleep(10)
                
                try:
                    print("색인 {0}번째 재시도..".format(retry))
                    bulk_result += helpers.bulk(es_client, list(filter(lambda x:x is not None, some_bulks)), refresh=True)[0]
                    break
                except urllib3.exceptions.NewConnectionError as connectionError:
                    logger.error("[insertTopics] %s (retry:%d)"%(str(connectionError), retry))
                    continue
        except:
            ex = traceback.format_exc()
            logger.error("[insertTopics] unknown error. Traceback >> %s " % ex)

        logger.debug("%d are successfully inserted."%bulk_result)
        
        


def find_vv_word(_scroll):
    es_conn = http.client.HTTPConnection(es_ip, es_port)
    if _scroll is None:
        ##### is_a_real_verb 값이 없고 VV로 값이 매겨진 항목만 가져와서 업데이트.
        ##### 이 모듈을 통해 is_a_real_verb가 업데이트되면 다음 이 모듈을 수행 시에는 VV이더라도 검색되지 않는다.(완전하게 VV로 판단내려짐)
        request = {
            "size" : PAGE_SIZE,
            "_source" : ["topic", "doc_datetime", "doc_id"],
            "query": {
                "bool": {
                  "filter": [
                    {
                      "bool": {
                        "must_not": {
                          "exists": {
                            "field": "is_a_real_verb"
                          }
                        }
                      }
                    },
                    {
                      "term": {
                        "topic_class.keyword": {
                          "value": "VV"
                        }
                      }
                    }
                  ]
                }
              }
        }
        es_conn.request("POST", "/"+INDEX_TOPICS+"-*/"+TYPE_DOC+"/_search?scroll=1d", json.dumps(request), { "Content-Type" : "application/json" })
    else:
        request = {
            "scroll" : "1d",
            "scroll_id" : _scroll
        }
        es_conn.request("POST", "/_search/scroll", json.dumps(request), { "Content-Type" : "application/json" })
    
    es_result = es_conn.getresponse().read()
    
    return json.loads(es_result)
    
        
        

if __name__ == '__main__':
    es_result = find_vv_word(None)
    
    logger.info("<PAGE:0>")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.ensure_future(insertTopics(es_result)))
    
    for pageNo in range(1, math.ceil(es_result['hits']['total']/PAGE_SIZE)):
        logger.info("<PAGE:%d>"%pageNo)
        
        if '_scroll_id' in es_result:
            es_result = find_vv_word(es_result['_scroll_id'])
            
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.ensure_future(insertTopics(es_result)))
    
    logger.info("Related words for VV topics-but, they are nouns-update ends.")
    
    
        
    
    '''
    count = 0
    for text in sys.argv:
        count+=1
        if count == 1:
            continue
        
        print(text, end="\t")
        
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.ensure_future(isNoun(text, True)))
    '''