'''
Created on 2017. 8. 10.

@author: Holly
'''
from cassandra.cluster import Cluster
import xml.etree.ElementTree as et
import com.wisenut.dao.mariadbclient as mariadb
import codecs
from _elementtree import ParseError
import json
import hashlib
import logging
from com.wisenut.config import Config
import sys
from cassandra.query import SimpleStatement
import cassandra
import time
import os

############# logger 세팅
conf = Config()
logging.config.fileConfig(conf.get_logconfig_path())
logger = logging.getLogger(__name__)

INDEX_DOCUMENTS="documents"
TYPE_DOC="doc"


cluster = Cluster(["211.39.140.63", "211.39.140.64", "211.39.140.65", "211.39.140.66"], connect_timeout=60)


def getRules():
    newsrules = []
    blogrules = []
    caferules = []
    knowledgerules = []
    guestrules = []
    with codecs.open(r"/home/wisenut/cassandra2json/Rules_new.xml", "r", "utf-8") as rulefile:
        try:
            root = et.fromstring(rulefile.read())
            for rule in root.findall("./newsRules/newsRule"):
                newsrules.append({
                    "type" : "news",
                    "site" : rule.find("site").text,
                    "url" : [ url.text for url in rule.findall("urlPatterns/urlPattern")]
                })
                
            for rule in root.findall("./blogRules/blogRule"):
                blogrules.append({
                    "type" : "blog",
                    "site" : rule.find("site").text,
                    "url" : [ url.text for url in rule.findall("urlPatterns/urlPattern")]
                })
                
            for rule in root.findall("./cafeRules/cafeRule"):
                caferules.append({
                    "type" : "cafe",
                    "site" : rule.find("site").text,
                    "url" : [ url.text for url in rule.findall("urlPatterns/urlPattern")]
                })
                
            for rule in root.findall("./knowledgeRules/knowledgeRule"):
                knowledgerules.append({
                    "type" : "knowledge",
                    "site" : rule.find("site").text,
                    "url" : [ url.text for url in rule.findall("urlPatterns/urlPattern")]
                })
                
            for rule in root.findall("./guestRules/guestRule"):
                guestrules.append({
                    "type" : "guest",
                    "site" : rule.find("site").text,
                    "url" : [ url.text for url in rule.findall("urlPatterns/urlPattern")]
                })
        except ParseError as xmlerror:
            logger.error(str(xmlerror))


    return newsrules, blogrules, caferules, knowledgerules, guestrules
#def getChannelInfo(url):
    
    
def getSourceInfo():
    newssources = mariadb.get_channel_name(_depth1=1, _source_depth=3)
    newsmap = {}
    for result in newssources:
        newsmap[result['source_nm']] = result
        
    blogsources = mariadb.get_channel_name(_depth1=2, _depth2=6, _source_depth=3)
    blogmap = {}
    for result in blogsources:
        blogmap[result['source_nm']] = result
        
    cafesources = mariadb.get_channel_name(_depth1=2, _depth2=7, _source_depth=3)
    cafemap = {}
    for result in cafesources:
        cafemap[result['source_nm']] = result
    
    knowledgesources = mariadb.get_channel_name(_depth1=2, _depth2=8, _source_depth=3)
    knowledgemap = {}
    for result in knowledgesources:
        knowledgemap[result['source_nm']] = result
    
    guestsources = mariadb.get_channel_name()
    guestmap = {}
    for result in guestsources:
        guestmap[result['source_nm']] = result
        
        
    return newsmap, blogmap, cafemap, knowledgemap, guestmap



def getCassandraData(user_seq, item_grp_seq, year, month, day, threshold):
    
    #cluster = Cluster(["211.39.140.65"], connect_timeout=60)
    session = cluster.connect("dmap")
    
    sql = '''
        select
            pub_year, pub_month, pub_day, pub_time,
            depth1_nm, depth1_seq, depth2_nm, depth2_seq, depth3_nm, depth3_seq,
            url as doc_url,
            second_url as doc_second_url,
            title as doc_title,
            content as doc_content,
            writer as doc_writer
          from crawl_doc
        where user_seq=%s and item_grp_seq=%s and pub_year='%s' and pub_month='%s' and pub_day='%s'
    '''%(user_seq, item_grp_seq, year, month, day)
    
    statement = SimpleStatement(sql, fetch_size=100)
    
    results = session.execute(statement, timeout=600.0)
    
    return results



def getCrawldocTotalCount(user_seq, item_grp_seq, year, month, day, threshold):
    
    #cluster = Cluster(["211.39.140.65"], connect_timeout=60)
    session = cluster.connect("dmap")
    
    sql = '''
        select count(*) as cnt
          from crawl_doc
        where user_seq=%s and item_grp_seq=%s and pub_year='%s' and pub_month='%s' and pub_day='%s'
    '''%(user_seq, item_grp_seq, year, month, day)
    
    statement = SimpleStatement(sql, fetch_size=100)
    
    totalCount = session.execute(statement, timeout=600.0)
    
    return totalCount



def getCassandraFilteredData(user_seq, item_grp_seq, year, month, day, threshold):
    
    #cluster = Cluster(["211.39.140.65"], connect_timeout=60)
    session = cluster.connect("dmap")
    
    sql = '''
        select
            pub_year, pub_month, pub_day, pub_time,
            depth1_nm, depth1_seq, depth2_nm, depth2_seq, depth3_nm, depth3_seq,
            url as doc_url,
            second_url as doc_second_url,
            title as doc_title,
            content as doc_content,
            writer as doc_writer
          from crawl_doc_filtered
        where user_seq=%s and item_grp_seq=%s and pub_year='%s' and pub_month='%s' and pub_day='%s'
    '''%(user_seq, item_grp_seq, year, month, day)
    
    statement = SimpleStatement(sql, fetch_size=100)
    
    results = session.execute(statement, timeout=600.0)
    
    return results


def getCrawldocFilteredTotalCount(user_seq, item_grp_seq, year, month, day, threshold):
    
    session = cluster.connect("dmap")
    
    sql = '''
        select count(*) as cnt
          from crawl_doc_filtered
        where user_seq=%s and item_grp_seq=%s and pub_year='%s' and pub_month='%s' and pub_day='%s'
    '''%(user_seq, item_grp_seq, year, month, day)
    
    statement = SimpleStatement(sql, fetch_size=100)
    
    totalCount = session.execute(statement, timeout=600.0)
    
    return totalCount



def dateFormatter(year, month, day, time):
    return "%s-%s-%sT%s:%s:%s"%(year, month, day, time[0:2], time[2:4], time[4:6])
    


def md5Generator(arr):
    m = hashlib.md5()
    #for e in arr:
    m.update(repr(arr).encode('utf-8'))
    return m.hexdigest()


def getDocPkStr(pub_year, pub_month, pub_day, url):
    pk = ''
    if len(pub_year.strip())>=4:
        pk += pub_year[0:4]
        
        if len(pub_month.strip())>=2:
            pk += pub_month[0:2]
            
            if len(pub_day.strip())>=2:
                pk += pub_day[0:2]
                
    pk+= url
    
    return pk



def resultset2json(resultset, jsonfilename):
    
    newsrules, blogrules, caferules, knowledgerules, guestrules = getRules()
    newsmap, blogmap, cafemap, knowledgemap, guestmap = getSourceInfo()
    
    inline_script = ""
    inline_script+="ctx._source.project_seq?.addAll(params.project_seq); Set hs = new HashSet(); hs.addAll(ctx._source.project_seq); ctx._source.project_seq.clear(); ctx._source.project_seq.addAll(hs);"
    inline_script+="ctx._source.doc_id=params.doc_id;"
    inline_script+="ctx._source.doc_datetime=params.doc_datetime;"
    inline_script+="ctx._source.doc_writer=params.doc_writer;"
    inline_script+="ctx._source.doc_title=params.doc_title;"
    inline_script+="ctx._source.doc_content=params.doc_content;"
    inline_script+="ctx._source.view_count=params.view_count;"
    inline_script+="ctx._source.comment_count=params.comment_count;"
    inline_script+="ctx._source.like_count=params.like_count;"
    inline_script+="ctx._source.dislike_count=params.dislike_count;"
    inline_script+="ctx._source.share_count=params.share_count;"
    inline_script+="ctx._source.locations=params.locations;"
    inline_script+="ctx._source.place=params.place;"
    inline_script+="ctx._source.upd_datetime=params.upd_datetime;"
    inline_script+="ctx._source.doc_url=params.doc_url;"
    inline_script+="ctx._source.doc_second_url=params.doc_second_url;"
    inline_script+="ctx._source.depth1_seq=params.depth1_seq;"
    inline_script+="ctx._source.depth2_seq=params.depth2_seq;"
    inline_script+="ctx._source.depth3_seq=params.depth3_seq;"
    inline_script+="ctx._source.depth1_nm=params.depth1_nm;"
    inline_script+="ctx._source.depth2_nm=params.depth2_nm;"
    inline_script+="ctx._source.depth3_nm=params.depth3_nm;"
    
    with codecs.open(jsonfilename, "w+", "utf-8") as jsonfile:
        for row in resultset:
            params = {
                "doc_id" : md5Generator([getDocPkStr(row.pub_year, row.pub_month, row.pub_day, row.doc_url)]),
                "doc_datetime": dateFormatter(row.pub_year, row.pub_month, row.pub_day, row.pub_time),
                "doc_writer": row.doc_writer,
                "doc_title": row.doc_title,
                "doc_content": row.doc_content,
                "view_count": 0,
                "comment_count": 0,
                "like_count": 0,
                "dislike_count": 0,
                "share_count": 0,
                "locations": "",
                "place": "",
                "doc_url": row.doc_url,
                "doc_second_url" : row.doc_second_url,
                "depth1_seq": -1,
                "depth2_seq": -1,
                "depth3_seq": -1,
                "depth1_nm": "",
                "depth2_nm": "",
                "depth3_nm": "",
                "project_seq": [80,81,99]                
            }
            
            #mariadb에서 소스 정보를 가져옴.
            source_info = None
            
            
            # 미디어
            for rule in newsrules:
                for url in rule['url']:
                    if url in row.doc_url:
                        source_info = newsmap[rule['site']] if rule['site'] in newsmap else None
                        #print(newsmap)
                        
                        break
                
            # 포털-블로그
            for rule in blogrules:
                for url in rule['url']:
                    if url in row.doc_url:
                        source_info = blogmap[rule['site']] if rule['site'] in blogmap else None
                        #print(blogmap)
                        
                        break
                    
            # 포털-카페
            for rule in caferules:
                for url in rule['url']:
                    if url in row.doc_url:
                        source_info = cafemap[rule['site']] if rule['site'] in cafemap else None
                        #print(cafemap)
                        
                        break
                    
            # 포털-지식
            for rule in knowledgerules:
                for url in rule['url']:
                    if url in row.doc_url:
                        source_info = knowledgemap[rule['site']] if rule['site'] in knowledgemap else None
                        #print(knowledgemap)
                        
                        break
                    
            # 기타
            for rule in guestrules:
                for url in rule['url']:
                    if url in row.doc_url:
                        source_info = guestmap[rule['site']] if rule['site'] in guestmap else None
                        #print(guestmap)
                        
                        break
                    
            
            if source_info == None:
                logger.error("Source info not found. (url: %s)" % row.doc_url)
            else:
                params['depth1_seq'] = source_info['depth1_seq']
                params['depth2_seq'] = source_info['depth2_seq']
                params['depth3_seq'] = source_info['depth3_seq']
                params['depth1_nm'] = source_info['depth1_nm']
                params['depth2_nm'] = source_info['depth2_nm']
                params['depth3_nm'] = source_info['depth3_nm']
            
            bulk_body = {
                "_op_type": "update",
                "_index": INDEX_DOCUMENTS,
                "_type": TYPE_DOC,
                "_id": params['doc_id'],
                "_source": {
                    "script": {
                        "lang" : "painless",
                        "inline": inline_script, 
                        "params": params
                    },
                    "upsert": params
                }
            }
            
            jsonfile.write(json.dumps(bulk_body, ensure_ascii=False))
            jsonfile.write("\n")



def search_create_directory(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)
        
    return dirname


    
def main(user_seq, item_grp_seq, year, month, threshold, day=None):
    logger.info(">>>>> Cassandra2json starts.")
    
    from calendar import monthrange    
    
    date_range = None
    if day:
        date_range = (int(day)-1, int(day))
    else:
        date_range = (0, monthrange(int(year), int(month))[1])
        
    for day in range(date_range[0], date_range[1]):
    
        day_as_str = str(day+1).rjust(2, '0')
        
        json_dir = search_create_directory("/backup/IBI/%s/%s/%s/%s/json" % (user_seq, item_grp_seq, year, month))
        json_filtered_dir = search_create_directory("/backup/IBI/%s/%s/%s/%s/json_filtered_dir" % (user_seq, item_grp_seq, year, month))
        
        #1. Crawldoc Data
        try:
            print(">>> crawl_doc(user_seq:%s, item_grp_seq:%s, %s-%s-%s) : %d" % (user_seq, item_grp_seq, year, month, day_as_str, getCrawldocTotalCount(user_seq, item_grp_seq, year, month, day_as_str, threshold)[0].cnt))
            logger.info(">>> crawl_doc(user_seq:%s, item_grp_seq:%s, %s-%s-%s) : %d" % (user_seq, item_grp_seq, year, month, day_as_str, getCrawldocTotalCount(user_seq, item_grp_seq, year, month, day_as_str, threshold)[0].cnt))
            
            crawldoc = getCassandraData(user_seq, item_grp_seq, year, month, day_as_str, threshold)
            resultset2json(crawldoc, os.path.join(json_dir, "%s_%s_%s_%s_%s.json"%(user_seq, item_grp_seq, year, month, day_as_str)))
        
        except cassandra.OperationTimedOut as timeoutError:
            retry = 0
            logger.error("[main] %s (retry:%d)"%(str(timeoutError), retry))
            while retry <= 5:
                retry += 1
                time.sleep(10)
                
                try:
                    crawldoc = getCassandraData(user_seq, item_grp_seq, year, month, day_as_str, threshold)
                    resultset2json(crawldoc, os.path.join(json_dir, "%s_%s_%s_%s_%s.json"%(user_seq, item_grp_seq, year, month, day_as_str)))
                    
                    retry = 6
                except cassandra.OperationTimedOut as timeoutError:
                    logger.error("[main] %s (retry:%d)"%(str(timeoutError), retry))
                    continue
                
        except cassandra.Unavailable as unavailableError:
            retry = 0
            logger.error("[main] %s (retry:%d)"%(str(unavailableError), retry))
            while retry <= 5:
                retry += 1
                time.sleep(10)
                
                try:
                    crawldoc = getCassandraData(user_seq, item_grp_seq, year, month, day_as_str, threshold)
                    resultset2json(crawldoc, os.path.join(json_dir, "%s_%s_%s_%s_%s.json"%(user_seq, item_grp_seq, year, month, day_as_str)))
                    
                    retry = 6
                except cassandra.Unavailable as unavailableError:
                    logger.error("[main] %s (retry:%d)"%(str(unavailableError), retry))
                    continue
                
        except cassandra.ReadTimeout as timeoutError:
            retry = 0
            logger.error("[main] %s (retry:%d)"%(str(timeoutError), retry))
            while retry <= 5:
                retry += 1
                time.sleep(10)
                
                try:
                    crawldoc = getCassandraData(user_seq, item_grp_seq, year, month, day_as_str, threshold)
                    resultset2json(crawldoc, os.path.join(json_dir, "%s_%s_%s_%s_%s.json"%(user_seq, item_grp_seq, year, month, day_as_str)))
                    
                    retry = 6
                except cassandra.ReadTimeout as timeoutError:
                    logger.error("[main] %s (retry:%d)"%(str(timeoutError), retry))
                    continue
        
        #2. Filtered Data 
        try:
            print(">>> crawl_doc_filtered(user_seq:%s, item_grp_seq:%s, %s-%s-%s) : %d" % (user_seq, item_grp_seq, year, month, day_as_str, getCrawldocFilteredTotalCount(user_seq, item_grp_seq, year, month, day_as_str, threshold)[0].cnt))
            logger.info(">>> crawl_doc_filtered(user_seq:%s, item_grp_seq:%s, %s-%s-%s) : %d" % (user_seq, item_grp_seq, year, month, day_as_str, getCrawldocFilteredTotalCount(user_seq, item_grp_seq, year, month, day_as_str, threshold)[0].cnt))
        
            crawldoc_filtered = getCassandraFilteredData(user_seq, item_grp_seq, year, month, day_as_str, threshold)
            resultset2json(crawldoc_filtered, os.path.join(json_filtered_dir, "%s_%s_%s_%s_%s_filtered.json"%(user_seq, item_grp_seq, year, month, day_as_str)))
            
        except cassandra.OperationTimedOut as timeoutError:
            retry = 0
            logger.error("[main] %s (retry:%d)"%(str(timeoutError), retry))
            while retry <= 5:
                retry += 1
                time.sleep(10)
                
                try:
                    crawldoc_filtered = getCassandraFilteredData(user_seq, item_grp_seq, year, month, day_as_str, threshold)
                    resultset2json(crawldoc_filtered, os.path.join(json_filtered_dir, "%s_%s_%s_%s_%s_filtered.json"%(user_seq, item_grp_seq, year, month, day_as_str)))
                    
                    retry = 6
                except cassandra.OperationTimedOut as timeoutError:
                    logger.error("[main] %s (retry:%d)"%(str(timeoutError), retry))
                    continue
                
        except cassandra.Unavailable as unavailableError:
            retry = 0
            logger.error("[main] %s (retry:%d)"%(str(unavailableError), retry))
            while retry <= 5:
                retry += 1
                time.sleep(10)
                
                try:
                    crawldoc_filtered = getCassandraFilteredData(user_seq, item_grp_seq, year, month, day_as_str, threshold)
                    resultset2json(crawldoc_filtered, os.path.join(json_filtered_dir, "%s_%s_%s_%s_%s_filtered.json"%(user_seq, item_grp_seq, year, month, day_as_str)))
                    
                    retry = 6
                except cassandra.Unavailable as unavailableError:
                    logger.error("[main] %s (retry:%d)"%(str(unavailableError), retry))
                    continue
                
        except cassandra.ReadTimeout as timeoutError:
            retry = 0
            logger.error("[main] %s (retry:%d)"%(str(timeoutError), retry))
            while retry <= 5:
                retry += 1
                time.sleep(10)
                
                try:
                    crawldoc_filtered = getCassandraFilteredData(user_seq, item_grp_seq, year, month, day_as_str, threshold)
                    resultset2json(crawldoc_filtered, os.path.join(json_filtered_dir, "%s_%s_%s_%s_%s_filtered.json"%(user_seq, item_grp_seq, year, month, day_as_str)))
                    
                    retry = 6
                except cassandra.OperationTimedOut as timeoutError:
                    logger.error("[main] %s (retry:%d)"%(str(timeoutError), retry))
                    continue

    
if __name__ == '__main__':
    user_seq = sys.argv[1]
    item_grp_seq = sys.argv[2]
    pub_year = sys.argv[3]
    pub_month = sys.argv[4]
    
    # 날짜 조건이 파라미터로 들어오면 해당 날짜만 json으로 만듦.
    # 날짜 조건이 없으면 월 단위로 json 만듦.
    pub_day = None
    if len(sys.argv)>5:
        pub_day = sys.argv[5]
        
    main(user_seq, item_grp_seq, pub_year, pub_month, 10000, pub_day)
    
    #print(md5Generator([getDocPkStr("2017", "05", "31", "http://cafe.naver.com/arenamasters/7246")]))
