'''
Created on 2017. 6. 13.

@author: Holly
'''
import http.client as hc
import logging
import json
from com.wisenut.enums.query import Query
import traceback
from com.wisenut import myLogger

############# setting logging
logger = myLogger.getMyLogger('esclient', False, True, logging.DEBUG)

############# Elasticsearch 정보 세팅
es_ip="211.39.140.96"
es_port = 9201
es_conn = hc.HTTPConnection(es_ip, es_port, timeout=60)

attr_dict = {
    "brand" : "브랜드",
    "region" : "지명",
    "person" : "인명"
}

def clear_scroll(scroll_id):
    try:
        es_conn.request("DELETE", "/_search/scroll", json.dumps({"scroll_id":scroll_id}), { "Content-Type" : "application/json" })
    except OSError as oserror:
        ex = traceback.format_exc()
        logger.error("[clear_scroll] OS error : %s. Traceback >> %s " % (str(oserror), ex))
    except:
        ex = traceback.format_exc()
        logger.error("[clear_scroll] error. Traceback >> %s " % ex)



def get_topic_attr(topic):
    request = {
        "query": {
            "query_string": {
              "query": "(_type:brand name:({})) OR (_type:region  name:({})) OR (_type:person name:({}))".format(topic, topic, topic),
              "default_operator": "AND"
            }
          }
    }

    es_conn.request("GET", "/dmap/brand/_search", json.dumps(request), {"Content-type" : "application/json"})
    result = json.loads(es_conn.getresponse().read())

    if "hits" in result and result["hits"]["total"] >0:
        return attr_dict[result["hits"]["hits"][0]["_type"]]
    else:
        return ""



def get_aggregations(query, req, index):
    logger.info("[get_aggregations] query >>> %s" % json.dumps(query))

    es_conn = hc.HTTPConnection(es_ip, es_port, timeout=60)
    es_conn.request("GET", "/"+index+"/doc/_search", json.dumps(query), {"Content-type" : "application/json"})

    result = es_conn.getresponse().read()

    if 'hits' not in json.loads(result):
        logger.error("[get_aggregations] result >>> %s" % result)

    return json.loads(result)




def get_documents(params, size, index, scroll_id=None):
    queryObj = Query(params)

    if not scroll_id:
        es_uri = "/"+index+"/doc/_search?scroll=1d"
        request = {
            "size" : size,
            "query" : {
                "bool" : {
                }
            }
        }
    else:
        es_uri = "/_search/scroll"
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
            filter.append(queryObj.get_period_query())
            
        request["query"]["bool"]["filter"] = filter
    
        # 데이터셋의 포함 키워드
        if "datasets" in params and params["datasets"]:
            request["query"]["bool"]["must"] = queryObj.get_dataset_query(params['project_seq'], params["datasets"])
    
    
    logger.debug("[get_documents] Query >>> %s " % json.dumps(request) )
            
    es_conn = hc.HTTPConnection(es_ip, es_port, timeout=60)
    es_conn.request("POST", es_uri, json.dumps(request), {"Content-type" : "application/json"})
    result = es_conn.getresponse().read()

    if 'hits' in json.loads(result):
        logger.debug("[get_documents] result['hits']['total'] >>> %d" % int(json.loads(result)['hits']['total']) )
    else:
        logger.debug("[get_documents] result ::: " + str(result))

    return json.loads(result)



if __name__ == '__main__':
    params = {
        "start_date" : "2018-01-01T00:00:00",
        "end_date" : "2018-12-31T23:59:59",
        "project_seq" : 176,
        "compare_yn" : "N",
        "channels" : "all",
        "datasets" : "2852"
    }
    queryObj = Query(params)
    
    #print(queryObj.ALL_TOPICS_LIST("신한금융지주"))
    print(get_documents(params, 10, "documents"))