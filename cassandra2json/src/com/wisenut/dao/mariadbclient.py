# -*- coding : utf-8 -*-
'''
Created on 2017. 5. 30.

@author: Holly
'''
import pymysql
import logging.config
from com.wisenut.config import Config
import re

############# logger 세팅
conf = Config()
logging.config.fileConfig(conf.get_logconfig_path())
logger = logging.getLogger(__name__)

############# DB 정보 세팅
mariadb_ip="211.39.140.133"
mariadb_port=3306
mariadb_user="dmap"
mariadb_password="dmap#wisenut!"
mariadb_db="dmap_base"
mariadb_charset="utf8"
   

def get_channel_name(_depth1=None, _depth2=None, _source_depth=None):
    #logger.debug("[get_channel_name] depth:%s, seq:%s" % (depth, seq))
    
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor(pymysql.cursors.DictCursor)
    sql = '''
        select A.name as source_nm,
               if(A.source_depth=1, A.seq, A.depth1_seq) as depth1_seq,
               if(A.source_depth=2, A.seq, A.depth2_seq) as depth2_seq,
               if(A.source_depth=3, A.seq, A.depth3_seq) as depth3_seq,
               if(A.source_depth=1, A.name, (select B.name from tb_source B where B.seq=A.depth1_seq)) as depth1_nm,
               if(A.source_depth=2, A.name, (select B.name from tb_source B where B.seq=A.depth2_seq)) as depth2_nm,
               if(A.source_depth=3, A.name, (select B.name from tb_source B where B.seq=A.depth3_seq)) as depth3_nm
         from tb_source A
         '''
        
    if _depth1!=None or _depth2!=None or _source_depth!=None:
        sql += "where"
        
    if _depth1!=None:
        if not sql.endswith('where'):
            sql += " and"
        sql += " A.depth1_seq=%s" % _depth1
        
    if _depth2!=None:
        if not sql.endswith('where'):
            sql += " and"
        sql += " A.depth2_seq=%s" % _depth2
        
    if _source_depth!=None:
        if not sql.endswith('where'):
            sql += " and"
        sql += " A.source_depth=%s" % _source_depth
    
    curs.execute(sql)
    
    result = curs.fetchall()
    conn.close()
    
    return result


def get_project_keyword_info(item_nm):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor(pymysql.cursors.DictCursor)
    sql = """
        SELECT PK.project_seq, P.customer_id, PK.keyword  
          FROM tb_project_keyword PK , tb_project P 
         WHERE PK.project_seq = P.seq 
           AND PK.use_yn = 'Y' 
           AND ( PK.keyword = '%s' or PK.keyword = '%s')
        """
        
    curs.execute(sql, [re.sub("^^", "", item_nm), item_nm])
    
    result = curs.fetchall()
    conn.close()
    
    return result