# -*- coding : utf-8 -*-
'''
Created on 2017. 5. 30.

@author: Holly
'''
import pymysql
import slackweb
from com.wisenut.config import Config
from com.wisenut import myLogger
import logging

############# setting config
conf = Config()

############# setting logging
logger = myLogger.getMyLogger('mariadbclient', True, False, logging.DEBUG)

############# setting slack
slack = slackweb.Slack("https://hooks.slack.com/services/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")

############# DB 정보 세팅
mariadb_ip=conf.get_mariadb_ip()
mariadb_port=conf.get_mariadb_port()
mariadb_user=conf.get_mariadb_user()
mariadb_password=conf.get_mariadb_password()
mariadb_db=conf.get_mariadb_db()
mariadb_charset=conf.get_mariadb_charset()
   
def get_all_dataset_seqs(_project_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor()
    sql = '''
        select TD.seq
          from tb_dataset TD
         where TD.project_seq = %s
           and TD.use_yn='Y'
         order by TD.dataset_order asc
         ''' % _project_seq
    
    curs.execute(sql)
    
    result = curs.fetchall()
    conn.close()
    
    return result  
   
   

def get_bica_info(_project_seq):
    #logger.debug("[get_channel_name] depth:%s, seq:%s" % (depth, seq))
    
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset)
    curs = conn.cursor()
    sql = '''
        select bica_ip, bica_port, bica_concept_id
          from tb_project A
         where seq = %s
         ''' % _project_seq
    
    curs.execute(sql)
    
    result = curs.fetchone()
    conn.close()
    
    return result



def get_all_projectseqs_of(customer_id):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor()
    
    sql = "select seq from tb_project where customer_id = '%s' and use_yn='Y'" % customer_id
    
    curs.execute(sql)
    
    rows = curs.fetchall()
    
    conn.close()
    
    return rows




def get_keywords_per_project(project_seqs):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor(pymysql.cursors.DictCursor)
    sql = """
        select keyword 
             , standard_keyword
          from tb_dataset_keyword
         where use_yn='Y'
           and dataset_seq in (
               select seq from tb_dataset
                where use_yn='Y'
                 and project_seq in (%s)
               )
        """ % project_seqs
    
    curs.execute(sql)
    
    rows = curs.fetchall()
    
    conn.close()
    
    return rows



def get_project_filter_keywords(project_seq):
    conn = pymysql.connect(host=mariadb_ip, port=mariadb_port, user=mariadb_user, password=mariadb_password, db=mariadb_db, charset=mariadb_charset, connect_timeout=60)
    curs = conn.cursor(pymysql.cursors.DictCursor)
    sql = """SELECT PFK.title_filter_keywords
                  , PFK.content_filter_keywords
                  , PFK.standard_title_filter_keywords
                  , PFK.standard_content_filter_keywords
                  , PFK.filter_urls
               FROM tb_project_filter_keyword PFK
              WHERE PFK.project_seq = %s
                and PFK.use_yn = 'Y'
              ORDER BY PFK.seq DESC"""
     
    curs.execute(sql, project_seq)
    
    rows = curs.fetchone()
    
    conn.close()
    
    return rows




if __name__ == '__main__':
    ip, port, concept_id = get_bica_info(33)
