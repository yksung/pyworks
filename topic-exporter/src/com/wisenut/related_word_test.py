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
            print(f)
            # url이 포함된 항목들은 문장에서 제거
            f = URL_PATTERN.sub("", f)
            f = ETC_PATTERN.sub("", f)
            related += await get_close_word(f, debug)
    
    es.close()
    
    return list(filter(lambda x:len(x)>1, list(sorted(set(related), key=lambda x:related.index(x)))))




async def get_close_word(text, debug=False):
    if debug : print("### text : {}".format(text))
    ret = []
    
    hl_p = re.compile("<em>[가-힣ㄱ-ㅎa-zA-Z\s]*</em>[가-힣ㄱ-ㅎa-zA-Z]*")
    #not_word_p = re.compile("[^가-힣ㄱ-ㅎㅏ-ㅣa-zA-Z0-9]")
    
    for token in filter(lambda x:x, hl_p.split(text)): # 검색된 단어들(em으로 감싸인 단어)을 제외한 나머지 단어 묶음을 추출
        ret = await analyze(token.strip(), debug)
        '''
        for x in filter( lambda x:x, not_word_p.split(token.strip()) ): # 나머지 단어들을 기호나 스페이스로 분리
            word = 
            if word.strip():
                ret.append(word)
        '''
    return ret



'''
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
    
    p = run([mecabBin, '-d', mecabDic, '-N', '2'], stdout=PIPE, input=text, encoding="UTF-8")
    if debug: print(p.stdout)
    
    result = re.split("(\n|EOS)", p.stdout)      
    
    ret=[]
    word_tag_map = {}
    for x in result:
        if x.find("\t")>=0:
            word = x.split("\t")[0]
            tag = x.split("\t")[1]
            
            if tag.split(",")[4] == "Compound": # 복합명사일 경우 이 단어만 map에 넣고 끝냄.
                word_tag_map["Compound"] = word
                break
            elif re.match("(J|X).*", tag) is not None:
                break
            else:
                if word in word_tag_map:
                    word_tag_map[word].append(tag.split(",")[0]) # list에 합침
                else:
                    word_tag_map[word] = [tag.split(",")[0]]
        else:
            continue
        
    if "Compound" in word_tag_map:
        if debug : print("\t\t\t\t\t\t\t\t\t\t\t"+word_tag_map['Compound'])
        
        return word_tag_map['Compound']
    else:
        for item in word_tag_map.items():
            isMeaningfulTag = True
            
            for tag in item[1]:
                if tag not in ["MM", "NNG", "NNP", "XPN", "XR", "SL" ]:
                    isMeaningfulTag = False
                    break
                    
            if isMeaningfulTag and "".join(ret).find(item[0])<0:
                ret.append(item[0])

        if debug : print("\t\t\t\t\t\t\t\t\t\t\t"+"".join(ret))
        
        return "".join(ret)
'''

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
    term = class_term_tuple[1]
    topic_class = class_term_tuple[0]
   
    topic_dict = {}
    
    if len(term.strip())>0:
        
        topic = term.split(teaclient.WEIGHT_DELIMITER)[0]
        topic_id = md5Generator([data["_id"], topic])
        related_words = await related_word_extractor(data['_id'], data['_source']['doc_datetime'], topic) if topic_class == 'NN' else ''
        
        topic_dict =  {
            #"topic_id" : topic_id,
            "topic" : topic,
            #"topic_attr" : topic_attr,
            "topic_class" : 'NN' if topic_class == 'NN' else 'VV',
            "related_words" : related_words,
            "doc_id" : data['_id'],
            #"doc_datetime" : data['_source']['doc_datetime'],
            #"doc_url" : data['_source']['doc_url'],
            #"doc_writer" : data['_source']['doc_writer'],
            #"doc_title" : data['_source']['doc_title'].replace("\\", "").replace("\n", "").replace("\r", ""),
            #"doc_content" : data['_source']['doc_content'].replace("\\", "").replace("\n", "").replace("\r", ""),
            #"depth1_seq" : data['_source']['depth1_seq'],
            #"depth2_seq" : data['_source']['depth2_seq'],
            #"depth3_seq" : data['_source']['depth3_seq'],
            #"depth1_nm" : data['_source']['depth1_nm'],
            #"depth2_nm" : data['_source']['depth2_nm'],
            #"depth3_nm" : data['_source']['depth3_nm'],
            #"project_seq" : data['_source']['project_seq'],
            #"upd_datetime" : get_current_datetime(),
            #"is_a_real_verb" : "N" if topic_class == 'NN' else 'Y'
        }
        
    return topic_dict




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
            
            fts = [ make_bulk(t, data) for t in (newArr) ]
            #t.cancel()
            some_bulks = yield from asyncio.gather(*fts)
            
            for bulk in some_bulks:
                print(bulk)
       
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
        '''
        request["query"]["bool"]["must"] = {
            "bool" : {
                "should" : [
                    {
                        "query_string": {
                            "fields": ["doc_title^100", "doc_content"],
                            "query" : "신한은행",
                            "default_operator" : "AND",
                            "tie_breaker" : 0.0
                        }
                    }
                ]
            }
        }
        '''
        
    
    
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
    
    mode = "retroactive"
    start_date = "2018-04-09T08:57:23"
    end_date = "2018-04-09T08:57:24"
      
    #project_seqs = mariadbclient.get_all_projectseqs_of('kdic') # DB에서 kdic에 해당하는 project를 전체 가져와야함.
    project_seqs = ['177'] 
    
    ############# setting logging
    logger = myLogger.getMyLogger('kdic-topics-' + mode, hasConsoleHandler=False, hasRotatingFileHandler=True, logLevel=logging.DEBUG)
    
    
    logger.info("=================================================================")
    logger.info("- ES Connection %s %d" % (es_ip, es_port) )
    logger.info("- mode\t\t:\t%s" % mode)
    logger.info("- project_seqs\t:\t%s" % ','.join(project_seqs))
    logger.info("- start_date\t:\t%s" % start_date)
    logger.info("- end_date\t:\t%s" % end_date)
    logger.info("=================================================================")

    
    #for project_seq in project_seqs.split(","):
        #logger.info(">>>>> project_seq  %s" % project_seq)
        #print(">>>>> project_seq  %s" % project_seq)
        
    main(mode, ','.join(project_seqs), start_date, end_date)
        
    '''
    count = 0
    for text in sys.argv:
        count+=1
        if count == 1:
            continue
        
        print(">>> 원문 >>>" + text)
        
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.ensure_future(analyze(text, True)))
    '''