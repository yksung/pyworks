'''
Created on 2017. 5. 30.

@author: Holly
'''
# -*- coding : utf-8 -*-
import pymysql
import http.client as hc
import logging
import re
import json

############# logger 세팅
logger = logging.getLogger("crumbs")
logger.setLevel(logging.DEBUG)

# 1. 로그 포맷 세팅
formatter = logging.Formatter('[%(levelname)s][%(asctime)s] %(message)s')

# 2. 파일핸들러와 스트림핸들러 추가
file_handler = logging.FileHandler("./out.log")
stream_handler = logging.StreamHandler()

# 3. 핸들러에 포맷터 세팅
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# 4. 핸들러를 로깅에 추가
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

############# DB 정보 세팅
mariadb_ip="211.39.140.133"
mariadb_port=3306
mariadb_user="dmap"
mariadb_password="dmap#wisenut!"
mariadb_db="dmap_base"
mariadb_charset="utf8"

############# Elasticsearch 정보 세팅
es_ip = "211.39.140.65"
es_port = 9200

def get_es_data(params, size, scroll_id=None):
    es_conn = hc.HTTPConnection(es_ip, es_port)
    es_conn.connect()
    
    # 대상 채널, 기간,
    if not scroll_id:
        es_uri = "/dmap/crawl_doc/_search?scroll=1m"
        request = {
            "size" : size,
           "query" : {
               "bool" : {
                    "must" : []
                }
            }
        } 
    else:
        es_uri = "/_search?scroll"
        request = {
            "scroll" : "1m",
           "scroll_id" : scroll_id
        } 
    
    must = []
    # 대상 채널
    if "channel" in params and params["channel"]:
        channel = params["channel"]
        
        query = ''
        for c in channel.split(";"):
            depth1_seq = c.split("^")[0]
            depth2_seq = c.split("^")[1]
        
            query += "("
            query += "depth1_seq:"+depth1_seq
            query += " AND depth2_seq:("+" OR ".join(depth2_seq.split(","))+")"
            query += ")"
            
            query += " OR "
            
        must.append({
            "query_string": {
                "query": re.compile(" OR $").sub("", query)
            }
        })
     
    # 대상 기간   
    if "start_date" in params and "end_date" in params:
        must.append({
            "range": {
                "doc_datetime": {
                    "gte" : params["start_date"] if params["start_date"] else "1900-01-01 00:00:00",
                    "lte" : params["end_date"] if params["end_date"] else "2100-12-31 23:59:59"
                } 
            }
        })

    # 데이터셋의 포함 키워드
    if "datasets" in params and params["datasets"]: # 신라면,삼양라면,안성탕면
        should = [] # 신라면 OR 삼양라면 OR 안성탕면
        result_list = get_include_keywords(params["datasets"]) # dataset 시퀀스로 dataset_keyword 조회
        
        for result in result_list:
            this_must = {}
            this_must_not = []
            this_must = {
                "query_string": {
                    "fields": ["doc_title", "doc_content"],
                    "query" : ",".join([result["keyword"],result['sub_keywords']]).replace(",", " AND ") # (신라면 농심 nongshim) OR (辛라면 농심) OR (푸라면 놈심)
                }
            }
            # 제목 필터
            this_must_not.append({
                "query_string" : {
                    "query" : "doc_title:("+result['title_filter_keywords'].replace(","," OR ")+")"
                }
            })
            # 본문 필터
            this_must_not.append({
                "query_string" : {
                    "query" : "doc_content:("+result['content_filter_keywords'].replace(","," OR ")+")"
                }
            })
            # URL 필터
            bool_should = []
            for url in result['filter_urls'].split(","):
                bool_should.append({
                    "match_phrase" : {
                        "doc_url" : url
                    }
                })
            this_must_not.append({
                "bool" : {
                    "should" : bool_should
                }
            })
            
            should.append({
                "bool" : {
                    "must" : this_must,
                    "must_not" : this_must_not
                }
            })
        must.append({
            "bool" : {
                "should" : should
            }
        })
      
        
    # elif params["type_cd"] == "CCT002": # 소셜모니터링-문서통계
    # elif params["type_cd"] == "CCT003": # 소셜모니터링-감성분석
    # .....
    # 코드별로 request 필요한 형태로 변경해서 추가
    
    if "query" in request:
        request["query"]["bool"]["must"] = must
        
    logger.debug("get_es_data() ==> request : " )
    for k,v in request.items():
        logger.debug("\t{} : {}".format(k,v))
    
    es_conn.request("POST", es_uri, json.dumps(request), {"Content-type" : "application/json"})
    
    return json.loads(es_conn.getresponse().read())
       
def get_include_keywords(dataset_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor(pymysql.cursors.DictCursor)
    sql = """select keyword 
                      ,sub_keywords
                      ,title_filter_keywords
                      ,content_filter_keywords
                      ,filter_urls
                from tb_dataset_keyword
              where use_yn='Y' and dataset_seq=%s"""
     
    curs.execute(sql, dataset_seq)
    
    rows = curs.fetchall()
    
    conn.close()
    
    return rows

def get_csv_name(csv_type_cd):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor()
    sql = "select name from tb_common_code where type_cd=%s"
    curs.execute(sql, csv_type_cd)
   
    csv_type_name = curs.fetchone()[0]
    
    conn.close()
    
    return csv_type_name

def get_excel_request():
    req_list = []
    
    # maria db 연결
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    
    # connection으로부터 cursor 생성
    curs = conn.cursor(pymysql.cursors.DictCursor)
    
    # sql 문 실행
    sql = """select a.seq
                    , a.user_seq
                    , a.type_cd
                    , a.account_seq
                    , DATE_FORMAT(a.reg_dt, "%Y%m%d") as reg_dt
                    , a.datasets
                    , a.channel
                    , DATE_FORMAT(a.start_date, "%Y-%m-%d %T") as start_date
                    , DATE_FORMAT(a.end_date, "%Y-%m-%d %T") as end_date
                    , a.compare_yn
                    , a.campaign_seq
                from tb_csv_queue a, tb_user b
               where a.status='I'
                  and b.use_yn='Y'
                  and a.user_seq = b.seq """
    curs.execute(sql)
    
    rows = curs.fetchall()
    for row in rows:
        logger.debug("get_excel_request() ==> row : ")
        for k,v in row.items():
            logger.debug("\t{} : {}".format(k,v))
        req_list.append(row)
                
    conn.close()
    
    return req_list

def update_queue(data, flag):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor()
    sql = "update tb_csv_queue set status = %s where seq=%s" # 전달 받은 request seq에 해당하는 row의 status를 P로 변경.
    
    curs.execute(sql, [flag, data['seq']])
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    es_result = get_es_data({
                 "channel" : "2^11;3^15",
                 "reg_dt" : "20170530",
                 "datasets" : "6", # es에는 dataset seq이 한개씩만 넘어감.
                 "period" : "2016-01-01 00:00:00~2016-01-31 23:59:59",
                 }, 10)
    print(es_result)