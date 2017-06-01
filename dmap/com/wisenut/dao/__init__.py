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
    must_not = []
    # 대상 채널
    if "channel" in params and params["channel"]:
        channel = params["channel"]
        depth1_seqs, depth2_seqs = channel.split(";") if len(channel.split(";")) > 1 else channel.split(";"), None
        
        query = " OR ".join(depth1_seqs.split(","))
        query += " AND " + " OR ".join(depth2_seqs.split(",")) if depth2_seqs else ""
        
        must.append({
            "query_string": {
                "query": query
            }
        })
     
    # 대상 기간   
    if "period" in params and params["period"]:
        period = params["period"]
        start_date, end_date = period.split("~") if len(period.split("~")) > 1 else period.split("~"), "2100-12-31 23:59:59"
        
        must.append({
            "range": {
                "doc_datetime": {
                    "gte" : start_date,
                    "lte" : end_date
                } 
            }
        })

    # 데이터셋의 포함 키워드
    if "datasets" in params and params["datasets"]: # 신라면,삼양라면,안성탕면
        should = [] # 신라면 OR 삼양라면 OR 안성탕면
        query = get_include_keywords(params["datasets"]) # dataset 시퀀스로 dataset_keyword 조회
        
        should.append({
            "query_string": {
                "fields": ["doc_title", "doc_content"],
                "query" : query, # (신라면 농심 nongshim) OR (辛라면 농심) OR (푸라면 놈심)
                "default_operator": "AND"
            }
        })

        must.append({"bool" : { "should" : should } })
            
    # 캠페인과 데이터셋의 필터 키워드
    if "datasets" in params and params["datasets"]:
        query = get_exclude_keywords(params["datasets"]) # dataset 시퀀스로 dataset_keyword 조회
        
        must_not.append({
            "query_string": {
                "fields": ["doc_title", "doc_content"],
                "query" : query,
                "default_operator": "AND"
            }
        })       
        
    # elif params["type_cd"] == "CCT002": # 소셜모니터링-문서통계
    # elif params["type_cd"] == "CCT003": # 소셜모니터링-감성분석
    # .....
    # 코드별로 request 필요한 형태로 변경해서 추가
    
    request["query"]["bool"]["must"] = must
    
    es_conn.request("POST", es_uri, json.dumps(request), {"Content-type" : "application/json"})
    
    return json.loads(es_conn.getresponse().read())
       
def get_exclude_keywords(dataset_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor()
    sql = "select keyword, sub_keywords from tb_dataset_keyword where use_yn='Y' and dataset_seq=%s"
     
    dataset_keywords = curs.execute(sql, dataset_seq)
    
    conn.close()
    
    return ",".join(list(dataset_keywords)).replace(",", " ")


def get_include_keywords(dataset_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor()
    sql = "select keyword, sub_keywords from tb_dataset_keyword where use_yn='Y' and dataset_seq=%s"
     
    dataset_keywords = curs.execute(sql, dataset_seq)
    keywords = ""
    for dataset in dataset_keywords:
        keywords += "("
        keywords += ",".join(list(dataset))
        keywords += ");"

    keywords = re.compile(";$").sub("", keywords).replace(";", " OR ").replace(",", " ")
    
    conn.close()
    
    return keywords

def get_csv_name(csv_type_cd):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor()
    sql = "select name from tb_common_code where type_cd=%s"
     
    csv_type_name = curs.execute(sql, csv_type_cd)
    
    conn.close()
    
    return csv_type_name

def get_excel_request():
    req_list = []
    
    # maria db 연결
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    
    # connection으로부터 cursor 생성
    curs = conn.cursor(pymysql.cursors.DictCursor)
    
    # sql 문 실행
    sql = """select a.seq, a.user_seq, a.type_cd, a.account_seq, a.reg_dt, a.datasets, a.channel, a.period, a.compare_yn, a.campaign_seq
                from tb_csv_queue a, tb_user b
               where a.status='I'
                  and b.use_yn='Y'
                  and a.user_seq = b.seq """
    curs.execute(sql)
    
    rows = curs.fetchall()
    for row in rows:
        req_list.append(row)
                
    conn.close()
    
    return req_list

def update_queue(data):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor()
    sql = "update tb_csv_queue set status = 'P' where seq=%s" # 전달 받은 request seq에 해당하는 row의 status를 P로 변경.
    
    curs.execute(sql, data['seq'])
    
    conn.commit()
    conn.close()
    
def get_dataset_name(dataset_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor()
    sql = "select name from tb_dataset where seq = %s"
    
    row = curs.execute(sql, dataset_seq)
    
    conn.close()
    
    return row[0]

if __name__ == '__main__':
    get_es_data({
                 "channel" : "2,3;11,15",
                 ""
                 })