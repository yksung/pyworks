# -*- coding : utf-8 -*-
'''
Created on 2017. 5. 30.

@author: Holly
'''
import pymysql
import re
from com.wisenut import myLogger
import logging

############# setting logging
logger = myLogger.getMyLogger('mariadbclient', True, False, logging.DEBUG)

############# DB 정보 세팅
mariadb_ip="211.39.140.133"
mariadb_port=3306
mariadb_user="dmap"
mariadb_password="dmap#wisenut!"
mariadb_db="dmap_base"
mariadb_charset="utf8"

'''
    조회수 쿼리
'''
def get_data_for_report_count(type_cd, trend_grp_seq, start_date, end_date, trend_dataset_seq=0, trend_keyword_seq=0):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor()
    
    sql = '''
        SELECT TG.name AS trend_grp
             , TD.name AS trend_dataset
             , TK.keyword AS trend_keyword
             , SC.search_day
             , SCK.keyword
             , (SELECT TC.name FROM tb_trend_category TC WHERE TC.seq=SCK.category_seq) AS trend_category
             , (select name from tb_common_code where type_cd=SCK.cdj_type_cd) as cdj_name
             , SUM(SC.pc_count) + SUM(SC.mobile_count) AS total_count
             , SUM(SC.pc_count) AS pc_count
             , SUM(SC.mobile_count) AS mobile_count
        FROM tb_search_count SC, tb_search_count_keyword SCK, tb_trend_keyword TK, tb_trend_dataset TD, tb_trend_group TG
        WHERE TG.seq = %d ''' % trend_grp_seq
    
    if trend_dataset_seq != 0:
        sql += " AND TD.seq = %d" % trend_dataset_seq
        
    if trend_keyword_seq != 0:
        sql += " AND SCK.trend_keyword_seq = %d" % trend_keyword_seq
    
    sql += ''' 
          AND SCK.use_yn='Y'
          AND SCK.type_cd = '%s'
          AND SC.search_day BETWEEN '%s' AND '%s'
          AND TG.seq = TD.trend_grp_seq
          AND TD.seq = TK.dataset_seq
          AND TK.seq = SCK.trend_keyword_seq
          AND SCK.seq = SC.search_count_keyword_seq
        GROUP BY SCK.seq
        order by total_count desc''' % (type_cd, re.sub("[-:\s]", "", start_date)[0:8], re.sub("[-:\s]", "", end_date)[0:8])
        
    logger.debug(sql)
        
    curs.execute(sql)

    result = curs.fetchall()
    conn.close()
    
    return result

'''
    트렌트 리포트에 사용되는 MySQL 쿼리
  2017.09.06 trend_grp_seq만 받아서 그룹 전체의 내용을 포함하도록 변경.
'''
def get_data_for_report_trend(project_seq, today):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor()
    
    sql = '''
         select d.search_day as search_day
              , d.search_time as search_time
              , a.name as group_name
              , b.name as item_name
              , c.keyword
              , group_concat(distinct d.keyword order by d.rank asc separator '||') as naver_keywords
           from tb_trend_group a,
                tb_trend_dataset b,
                tb_trend_keyword c,
                tb_naver_keyword d
          where a.project_seq = %d
            and d.type_cd = 'SCT002'
            and concat(d.search_day, d.search_time, '0000') >= concat('%s', '000000')
            and concat(d.search_day, d.search_time, '0000') <= concat('%s', '235959')
            and a.seq = b.trend_grp_seq
            and b.seq = c.dataset_seq
            and c.seq = d.trend_keyword_seq
          group by a.seq, b.seq, c.seq, d.search_day, d.search_time
          order by d.search_day ASC, d.search_time ASC, a.seq ASC, b.seq asc, c.seq asc ''' % (
            project_seq, today, today
           )
        
    curs.execute(sql)
    
    result = curs.fetchall()
    conn.close()
    
    return result
    

def get_channel_name(depth, seq):
    #logger.debug("[get_channel_name] depth:%s, seq:%s" % (depth, seq))
    
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor()
    sql = """select (select B.name from tb_source B where B.seq=A.depth1_seq) as depth1_name
                  , (select C.name from tb_source C where C.seq=A.depth2_seq) as depth2_name
                  , A.name as depth3_name
               from tb_source A
            where A.source_depth=%s and A.seq=%s"""
     
    curs.execute(sql, [depth, seq])
    
    result = curs.fetchone()
    conn.close()
    
    return result

def get_bicainfo(project_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor()
    sql = "select bica_ip, bica_port, bica_concept_id from tb_project where seq=%s"
     
    curs.execute(sql, project_seq)
    
    bica_info = curs.fetchone() # tuple
    
    conn.close()
    
    return bica_info

def get_project_name(project_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor()
    sql = "select name from tb_project where seq=%s"
     
    curs.execute(sql, project_seq)
    
    project_name = curs.fetchone()[0]
    
    conn.close()
    
    return project_name


def get_every_dataset_for(project_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor()
    sql = """select seq, project_seq, name from tb_dataset where project_seq in (%s) and use_yn='Y'""" % project_seq
     
    curs.execute(sql)
    
    rows = curs.fetchall()
    
    conn.close()
    
    return rows


def get_dataset_name(dataset_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor()
    sql = """select name
                from tb_dataset
              where seq=%s"""
     
    curs.execute(sql, dataset_seq)
    
    dataset_name = curs.fetchone()
        
    conn.close()
    
    if dataset_name == None:
        return None
    else:
        return dataset_name[0]
       
def get_include_keywords(dataset_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor(pymysql.cursors.DictCursor)
    sql = """select keyword 
                  , standard_keyword
               from tb_dataset_keyword
              where use_yn='Y' and dataset_seq=%s"""
     
    curs.execute(sql, dataset_seq)
    
    rows = curs.fetchall()
    
    conn.close()
    
    return rows

def get_project_filter_keywords(project_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor(pymysql.cursors.DictCursor)
    sql = """SELECT PFK.title_filter_keywords
                  , PFK.content_filter_keywords
                  , PFK.filter_urls
               FROM tb_project_filter_keyword PFK
              WHERE PFK.project_seq = '%s'
                and PFK.use_yn = 'Y'
              ORDER BY PFK.seq DESC"""
     
    curs.execute(sql, project_seq)
    
    rows = curs.fetchone()
    
    conn.close()
    
    return rows

def get_exceltype_name(csv_type_cd):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor()
    sql = "select name from tb_group_code where grp_cd=%s"
    curs.execute(sql, csv_type_cd)
   
    csv_type_name = curs.fetchone()[0]
    
    conn.close()
    
    return csv_type_name

def get_excel_request():
    req_list = []
    
    # maria db 연결
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    
    # connection으로부터 cursor 생성
    curs = conn.cursor(pymysql.cursors.DictCursor)
    
    # sql 문 실행
    sql = """select a.seq
                    , a.account_seq
                    , a.project_seq
                    , DATE_FORMAT(a.reg_dt, "%Y%m%d") as reg_dt
                    , a.type_cd
                    , a.datasets
                    , a.trend_grp_seq
                    , a.trend_dataset_seq
                    , a.trend_keyword_seq
                    , a.channels
                    , a.status
                    , DATE_FORMAT(a.start_date, "%Y-%m-%dT%T") as start_date
                    , DATE_FORMAT(a.end_date, "%Y-%m-%dT%T") as end_date
                    , a.compare_yn
                from tb_exceldownload_queue a, tb_project b
               where a.status='I'
                  and b.use_yn='Y'
                  and a.project_seq = b.seq
                  and b.use_yn='Y' """
    curs.execute(sql)
    
    rows = curs.fetchall()
    for row in rows:
        logger.debug("get_excel_request() ==> row : ")
        for k,v in row.items():
            logger.debug("\t{} : {}".format(k,v))
        req_list.append(row)
                
    conn.close()
    
    return req_list

def update_queue(data):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor()
    sql = "update tb_exceldownload_queue set status = %s, excel_file_path=%s, excel_file_nm=%s where seq=%s" # 전달 받은 request seq에 해당하는 row의 status를 P로 변경.
    
    curs.execute(sql, [data['status'], data['excel_file_path'], data['excel_file_nm'], data['seq']])
    
    conn.commit()
    conn.close()
    
def get_excel_path(seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor()
    sql = "select excel_file_path from tb_exceldownload_queue where seq=%s"
    curs.execute(sql, seq)

    file_path=''
    result = curs.fetchone()
    if result:
        file_path = result[0]
    
    conn.close()
    
    return file_path

if __name__ == '__main__':
    print(get_every_dataset_for('176,178'))