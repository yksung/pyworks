# -*- coding : utf-8 -*-
import com.wisenut.dao.mariadbclient as db
import com.wisenut.dao.esclient as es
from com.wisenut.utils import file_util
import re, os
import math
from datetime import datetime as dt
from com.wisenut.enums.query import Query
from datetime import timedelta
from com.wisenut.config import Config
import sys
import xlsxwriter
import copy
import pymysql
import time
import socket
from com.wisenut import myLogger
import logging

############# setting logging
logger = myLogger.getMyLogger('kdic-report-maker', False, True, logging.DEBUG)

MODE_DOCUMENTS='documents'
MODE_TOPICS='topics'
MODE_EMOTIONS='emotions'
MODE_TREND='trend'

INDEX_DOCUMENTS="documents-*"
INDEX_TOPICS="topics-*"
INDEX_EMOTIONS="emotions-*"

FIELDS_DOCUMENTS  = [ 'doc_id', 'doc_datetime','doc_writer','doc_url','doc_title','doc_content','depth1_nm','depth2_nm','depth3_nm']
FIELDS_EMOTIONS   = [ 'conceptlevel1', 'conceptlevel2', 'conceptlevel3', 'emotion_type', 'matched_text.string', 'depth1_nm', 'depth2_nm', 'depth3_nm', 'doc_datetime', 'doc_writer', 'doc_url', 'doc_title']

KFIELDS_DOCUMENTS = [ 'ID', '게시일','작성자','URL','제목','내용','채널1','채널2','채널3']
KFIELDS_EMOTIONS  = [ '날짜', '대분류', '중분류', '소분류', '감성', '분석문장', '채널1', '채널2', '채널3', '게시일', '작성자', 'URL', '제목']

RETRY_TIMES=5
SLEEP_FOR_WHEN_RETRY=30 # seconds

class ReportKDICDocuments:
    mode = ""
    seq = -1
    reg_dt = ""
    report_day = ""
    report_time = ""
    report_type = ""
    project_name = ""
    channel = ""
    start_date = ""
    end_date = ""
    dataset_names = ""
    query = None
    compare = ''
    save_path = ""

    file_name = ""
    file_path = ""

    #BASE_EXCEL_DIRECTORY='/data/dmap-data/dmap-excel'
    conf = Config()
    BASE_EXCEL_DIRECTORY=conf.get_report_home()

    HEADER_FORMAT = {
        'bold' : True,
        'font_size' : 9,
        'bg_color' : '#F2F2F2',
        'align' : 'center',
        'border' : 1
    }
    DEFAULT_FORMAT = {
        'font_size' : 9,
        'border' : 1
    }

    def __init__(self, params):
        self.mode = params['mode']
        self.compare = True if params['compare_yn']=='Y' else False

        self.start_date = re.sub("[-:T\s]", "", params['start_date'])[:12]
        self.end_date = re.sub("[-:T\s]", "", params['end_date'])[:12]
        self.reg_dt = re.sub("[-:T\s]", "", params['reg_dt'])

        self.dataset_names = ",".join([db.get_dataset_name(x) if db.get_dataset_name(x)!=None else 'unknown' for x in str(params['datasets']).split("^")]) if params['datasets'] else '' # 6^7^15 -> 신라면,안성탕면,짜파게티
        self.query = Query(params)

        if mode == MODE_DOCUMENTS:
            self.file_name = "_".join(["SNS", self.dataset_names, self.start_date, self.end_date]) + ".xlsx"
        elif mode == MODE_TOPICS:
            self.file_name = "_".join(["화제어", self.dataset_names, self.start_date, self.end_date]) + ".xlsx"
        elif mode == MODE_EMOTIONS:
            self.file_name = "_".join(["감성분석", self.dataset_names, self.start_date, self.end_date]) + ".xlsx"
        elif mode == MODE_TREND:
            self.file_name = "_".join(["연관검색어", str(params['project_seq']), self.start_date, self.end_date]) + ".xlsx"


    def get_file_name(self):
        return self.file_name

    def create_file_path(self, path):
        self.file_path = path
        return file_util.search_create_directory( self.file_path )
        # if mode == 'documents':
        #     '''
        #     - documents는 report 폴더 아래 Social 디렉터리 아래 떨어지게 됨.
        #     '''
        #     self.file_path = os.path.join(self.BASE_EXCEL_DIRECTORY, self.reg_dt, 'raw')
        #     return file_util.search_create_directory( self.file_path )
        # else:
        #     '''
        #     - topics는 report 폴더 아래 Social_topics 디렉터리 아래 떨어지게 됨.
        #     '''
        #     self.file_path = os.path.join(self.BASE_EXCEL_DIRECTORY, self.reg_dt, 'topic')
        #     return file_util.search_create_directory( self.file_path )


    def topics_list(self, params):
        worksheet = self.workbook.add_worksheet("화제어(%s)"%"~".join([params['start_date'][0:10],params['end_date'][0:10]]))
        # 헤더
        # 날짜 형식은 YYYYMMDD 이어야 함
        worksheet.write(0, 0, '날짜', self.header)
        worksheet.write(0, 1, '순위', self.header)
        worksheet.write(0, 2, '화제어', self.header)
        worksheet.write(0, 3, '문서수', self.header)
        worksheet.write(0, 4, '연관어', self.header)
        worksheet.write(0, 5, '문서수', self.header)

        # 데이터
        result_topic = es.get_aggregations(self.query.ALL_TOPICS_LIST(params['dataset_name']), params, Query.INDEX_TOPICS)
        row=0
        seq=0 # topic의 순위
        #topics_date = params['start_date'][0:10].replace('-','')

        for bucket0 in result_topic['aggregations']['my_aggs0']['buckets']:
            for bucket1 in bucket0['my_aggs1']['buckets']:
                topic = re.sub("[\+=\-/]", "", str(bucket1['key']))
                seq += 1
                
                topics_date = bucket0['key_as_string']
                
                if len(bucket1['my_aggs2']['buckets'])>0:
                    for bucket2 in bucket1['my_aggs2']['buckets']:
                        str(startdate.strftime('%Y-%m-%dT%H:00:00'))
                        # worksheet.write(1+row, 0, params['start_date'][0:10].replace('-',''), self.default)
                        worksheet.write(1+row, 0, re.sub("-","", topics_date[:topics_date.find("T")]), self.default)
                        worksheet.write(1+row, 1, seq, self.default)
                        worksheet.write(1+row, 2, re.sub("[\[\]]", "", topic), self.default)
                        worksheet.write(1+row, 3, bucket1['doc_count'], self.default)
                        worksheet.write(1+row, 4, bucket2['key'], self.default)
                        worksheet.write(1+row, 5, bucket2['doc_count'], self.default)
                        #worksheet.write(1+row, 6, verb_list, self.default)
                        row += 1
                        
                else:
                    worksheet.write(1+row, 0, re.sub("-","", topics_date[:topics_date.find("T")]), self.default)
                    worksheet.write(1+row, 1, seq, self.default)
                    worksheet.write(1+row, 2, re.sub("[\[\]]", "", topic), self.default)
                    worksheet.write(1+row, 3, bucket1['doc_count'], self.default)
                    worksheet.write(1+row, 4, '', self.default)
                    worksheet.write(1+row, 5, '', self.default)
                    #worksheet.write(1+row, 6, '', self.default)
                    row += 1
        
        logger.info("<%s> Total Topics : %d" % (self.dataset_names, row) )



    def emotions_per_causes(self, params):
        worksheet = self.workbook.add_worksheet("강성분석(%s)"%"~".join([params['start_date'][0:10],params['end_date'][0:10]]))

        # 헤더
        # 날짜 형식은 YYYYMMDD 이어야 함
        worksheet.write(0, 0, '날짜', self.header)
        worksheet.write(0, 1, '채널1', self.header)
        worksheet.write(0, 2, '채널2', self.header)
        worksheet.write(0, 3, '채널3', self.header)
        worksheet.write(0, 4, '대분류', self.header)
        worksheet.write(0, 5, '중분류', self.header)
        worksheet.write(0, 6, '소분류', self.header)
        worksheet.write(0, 7, '긍부정', self.header)
        worksheet.write(0, 8, '문서수', self.header)

        # 데이터
        qdsl = self.query.EMOTIONS_PER_CAUSES()
        result = es.get_aggregations(copy.copy(qdsl), params, INDEX_EMOTIONS)
        #total = result['hits']['total']
        total = 0
        row = 0
        #emotions_date = params['start_date'][0:10].replace('-','')

        for bucket0 in result['aggregations']['my_aggs0']['buckets']:
            for bucket1 in bucket0['my_aggs1']['buckets']:
                for bucket2 in bucket1['my_aggs2']['buckets']:
                    for bucket5 in bucket2['my_aggs3']['my_aggs4']['my_aggs5']['buckets']:
                        # 2018.01.11 "(주)"가 포함된 경우에는 (주)를 뺀 나머지 이름이 포함됐는지 확인해야 하므로 변경.
                        if params['dataset_name'].find(bucket2['key']) >= 0 :
                            depth_level = bucket1['key'].split(">")
                            
                            #worksheet.write(1+row, 0, emotions_date, self.default)
                            emotions_date = bucket0['key_as_string']
                            worksheet.write(1+row, 0, re.sub("-", "", emotions_date[:emotions_date.find("T")]), self.default)
                            worksheet.write(1+row, 1, re.sub("[\[\]]", "", depth_level[0]) if len(bucket1['key'].split(">"))>=0 else '', self.default)
                            worksheet.write(1+row, 2, re.sub("[\[\]]", "", depth_level[1]) if len(bucket1['key'].split(">"))>=1 else '', self.default)
                            worksheet.write(1+row, 3, re.sub("[\[\]]", "", depth_level[2]) if len(bucket1['key'].split(">"))>=2 else '', self.default)
                            worksheet.write(1+row, 4, bucket2['key'], self.default)
                            worksheet.write(1+row, 5, '', self.default)
                            worksheet.write(1+row, 6, '', self.default)
                            worksheet.write(1+row, 7, bucket5['key'], self.default)
                            worksheet.write(1+row, 8, bucket5['doc_count'], self.default)
                            
                            total += int(bucket5['doc_count']) 
                            row += 1

        # 합꼐
        if len(params['datasets'].split("^"))==1:
            worksheet.write(row+1, 0, '합계', self.header)
            worksheet.write(row+1, 1, '', self.header)
            worksheet.write(row+1, 2, '', self.header)
            worksheet.write(row+1, 3, '', self.header)
            worksheet.write(row+1, 4, '', self.header)
            worksheet.write(row+1, 5, '', self.header)
            worksheet.write(row+1, 6, '', self.header)
            worksheet.write(row+1, 7, '', self.header)
            worksheet.write(row+1, 8, total, self.header)

        logger.info("<%s> Total Emotions : %d" % (self.dataset_names, row) )
        
        
        

    # 원문
    def create_documents_list(self, params, index):
        size = 10000 # 페이징 사이즈
        if params['mode'] == MODE_DOCUMENTS:
            output_fields_korean = KFIELDS_DOCUMENTS
            output_fields = FIELDS_DOCUMENTS
        elif params['mode'] == MODE_EMOTIONS:
            output_fields_korean = KFIELDS_EMOTIONS
            output_fields = FIELDS_EMOTIONS

        # 검색 시작
        result = es.get_documents(params, size, index, "")

        worksheet = self.workbook.add_worksheet("원문(%s)(0)"%"~".join([params['start_date'][0:10],params['end_date'][0:10]]))

        # 엑셀 헤더
        for colidx, field in enumerate(output_fields_korean):
            worksheet.write(0, colidx, field, self.header)
        
        # 정확도(Score) 추가
        worksheet.write(0, len(output_fields_korean), '정확도', self.header)    
        
        logger.info("<%s> Total Documents : %d" % (self.dataset_names, result["hits"]["total"]))
        
        if "hits" in result and result["hits"]["total"] > 0:

            for row, this_result in enumerate(result["hits"]["hits"]):
                for col, field in enumerate(output_fields):
                    if field == 'doc_id':
                        val = this_result["_id"]
                        worksheet.write(row+1, col, val, self.default)

                        continue

                    if "." in field:
                        field, subfield = field.split(".")

                        val = this_result["_source"][field][subfield] if field in this_result["_source"] and subfield in this_result["_source"][field] else "null"
                        if field in ['doc_writer', 'doc_title', 'doc_content']:
                            val = re.sub("[\+=\-/]", "", str(val))
                            
                        worksheet.write(row+1, col, val, self.default)
                    else:
                        val = this_result["_source"][field] if field in this_result["_source"] else "null"
                        if field in ['doc_writer', 'doc_title', 'doc_content']:
                            val = re.sub("[\+=\-/]", "", str(val))
                        
                        worksheet.write(row+1, col, val, self.default)
                
                # 검색정확도(Score) 필드 추가       
                worksheet.write(row+1, len(output_fields), this_result['_score'], self.default)

            # 결과건수가 한 페이지 사이즈보다 큰 경우, scroll을 이용해서 paging하며 결과를 가져옴.
            # 용량이 클 것으로 예상하여 엑셀 파일도 새로 생성.
            if "hits" in result and result["hits"]["total"] > size:
                for page in range(1, math.ceil(result["hits"]["total"]/size)): # 0, 1, 2, ....
                    worksheet = self.workbook.add_worksheet("원문(%s)(%d)"%("~".join([params['start_date'][0:10],params['end_date'][0:10]]),page))
                    
                    scrolled_result = es.get_documents(params, size, index, scroll_id=result["_scroll_id"])
                    for row, this_result in enumerate(scrolled_result["hits"]["hits"]):
                        for col, field in enumerate(output_fields):
                            if field == 'doc_id':
                                val = this_result["_id"]
                                worksheet.write(row+1, col, val, self.default)
        
                                continue
        
                            if "." in field:
                                field, subfield = field.split(".")
        
                                val = this_result["_source"][field][subfield] if field in this_result["_source"] and subfield in this_result["_source"][field] else "null"
                                if field in ['doc_writer', 'doc_title', 'doc_content']:
                                    val = re.sub("[\+=\-/]", "", str(val))
                                    
                                worksheet.write(row+1, col, val, self.default)
                            else:
                                val = this_result["_source"][field] if field in this_result["_source"] else "null"
                                if field in ['doc_writer', 'doc_title', 'doc_content']:
                                    val = re.sub("[\+=\-/]", "", str(val))
                                    
                                worksheet.write(row+1, col, val, self.default)
                        
                        # 검색정확도(Score) 필드 추가       
                        worksheet.write(row+1, len(output_fields), this_result['_score'], self.default)


                    # 마지막 페이지를 처리하고 나면 scroll을 clear
                    if page == math.ceil(result["hits"]["total"]/size)-1: 
                        if result["_scroll_id"]:
                            es.clear_scroll(result["_scroll_id"])



    def make_trend_report(self, params):
        logger.info("============================= \"make_trend_report\" starts.")

        today = re.sub("[-]", "", params['start_date'][0:10])

        worksheet = self.workbook.add_worksheet("연관어(%s)"%"~".join([params['start_date'][0:10],params['end_date'][0:10]]))

        # 헤더
        # 날짜 형식은 YYYYMMDD 이어야 함
        worksheet.write(0, 0, '날짜', self.header)
        worksheet.write(0, 1, '시간', self.header)
        worksheet.write(0, 2, '검색그룹', self.header)
        worksheet.write(0, 3, '검색아이템', self.header)
        worksheet.write(0, 4, '검색키워드', self.header)
        worksheet.write(0, 5, '키워드', self.header)

        # 데이터
        result = db.get_data_for_report_trend(params['project_seq'], today)
        for idx, row in enumerate(result, 1):
            worksheet.write(idx, 0, row[0], self.default)
            worksheet.write(idx, 1, row[1], self.default)
            worksheet.write(idx, 2, row[2], self.default)
            worksheet.write(idx, 3, row[3], self.default)
            worksheet.write(idx, 4, row[4], self.default)
            worksheet.write(idx, 5, row[5], self.default)




    def create_report(self, params):
        self.workbook = xlsxwriter.Workbook(os.path.join(self.file_path.replace("/", os.path.sep), self.file_name), options={'strings_to_urls': False, 'strings_to_numbers': True} )
        self.header = self.workbook.add_format(self.HEADER_FORMAT)
        self.default = self.workbook.add_format(self.DEFAULT_FORMAT)

        if self.mode == MODE_TOPICS:
            self.topics_list(params)
        elif self.mode == MODE_DOCUMENTS:
            self.create_documents_list(params, INDEX_DOCUMENTS)
        elif self.mode == MODE_EMOTIONS:
            self.emotions_per_causes(params)
        elif self.mode == MODE_TREND:
            self.make_trend_report(params)


        self.close_workbook()

    
    
    
    def close_workbook(self):
        self.workbook.close()



if __name__ == '__main__':

    if len(sys.argv) < 4:
        print("[ Usage ]")
        print("\report_kdic_doc <mode> <project_seqs> <save_path>")
        print("")

        exit

    mode = sys.argv[1]
    project_seqs = sys.argv[2] # , 로 연결해서 여러 프로젝트를 받을 수 있음.
    path = sys.argv[3]
    report_day = sys.argv[4]
    report_time = sys.argv[5]

    typecd = ''
    #enddate = dt.now() if len(sys.argv)==4 else datetime.strptime(sys.argv[4], '%Y%m%d')
    enddate = dt.strptime(report_day + report_time, '%Y%m%d%H')

    logger.info("========================================================================")
    logger.info('mode\t\t:\t' + mode)
    logger.info('project_seqs\t:\t' + project_seqs)
    logger.info('path\t\t:\t' + path)
    logger.info("========================================================================")
    
    print('mode : ' + mode)
    print('project_seqs : ' + project_seqs)
    print('path : ' + path)


    #1. 해당 프로젝트의 모든 데이터셋 시퀀스를 가져옴.
    datasets_row_tuples = db.get_every_dataset_for(project_seqs)

    if mode == MODE_DOCUMENTS:
        '''
        - documents는 1시간치씩 문서를 만듦.
        - 현재 시간을  x시라고 했을 때, x-1시  0분부터 x-1시 59분까지 ES에 insert된(upd_datetime) 원문들을 기준으로 함.
        '''
        startdate = enddate - timedelta(hours=1)
        print('type : MODE_DOCUMENTS')
        print('date : ' + str(startdate.strftime('%Y-%m-%dT%H:00:00')) + '~' + str(enddate.strftime('%Y-%m-%dT%H:00:00')))
        typecd = "RSS"
    elif mode == MODE_TOPICS:
        '''
        - topics는 하루치씩 문서를 만듦.
        - 현재 시각의 전날치 00시부터 24시까지의 화제어를 모아서 문서로 만듦.
        '''
        startdate = enddate - timedelta(days=1)
        print('type : MODE_TOPICS')
        print('date : ' + str(startdate.strftime('%Y-%m-%dT%H:00:00')) + '~' + str(enddate.strftime('%Y-%m-%dT%H:00:00')))
        typecd = "RSS"
    elif mode == MODE_EMOTIONS:
        '''
        - emotions 하루치씩 문서를 만듦.
        - 현재 시각의 전날치 00시부터 24시까지의 감성분석 결과를 모아서 문서로 만듦.
        '''
        startdate = enddate - timedelta(days=1)
        print('type : MODE_EMOTIONS')
        print('date : ' + str(startdate.strftime('%Y-%m-%dT%H:00:00')) + '~' + str(enddate.strftime('%Y-%m-%dT%H:00:00')))
        typecd = "RSE"
    elif mode == MODE_TREND:
        '''
        - trend 하루치씩 문서를 만듦.
        - 현재 시각의 전날치 00시부터 24시까지의 연관검색어를 모아서 문서로 만듦.
        '''
        startdate = enddate - timedelta(days=1)
        print('type : MODE_TREND')
        print('date : ' + str(startdate.strftime('%Y-%m-%dT%H:00:00')) + '~' + str(enddate.strftime('%Y-%m-%dT%H:00:00')))
        typecd = "RTT"
    else:
        print("\"mode\" should be one of <documents,emotions,topics,trend>")
        print("")

        exit

    #2. 데이터셋별로 원문 리포트를 만든다.
    if mode in [ MODE_DOCUMENTS, MODE_EMOTIONS, MODE_TOPICS ]:
        for this_dataset_seq, this_project_seq, dataset_name in datasets_row_tuples:
            req = {
                "mode" : mode,
                "seq": 0,
                "reg_dt" : str(dt.now().strftime('%Y%m%d%H')),
                "type_cd" : typecd,
                "channels" : "all",
                "start_date" : str(startdate.strftime('%Y-%m-%dT%H:00:00')) if mode=='documents' else str(startdate.strftime('%Y-%m-%dT00:00:00')),
                # "end_date" : str(enddate.strftime('%Y-%m-%dT%H:59:59')) if mode=='documents' else str(enddate.strftime('%Y-%m-%dT00:00:00')),
                "end_date" : str(enddate.strftime('%Y-%m-%dT%H:00:00')) if mode=='documents' else str(enddate.strftime('%Y-%m-%dT00:00:00')),
                "datasets" : str(this_dataset_seq),
                "project_seq" : int(this_project_seq),
                "compare_yn" : "N",
                "dataset_name" : dataset_name,
                "trend_grp_seq" : 0,
                "trend_dataset_seq" : 0,
                "trend_keyword_seq" : 0
            }

            report = ReportKDICDocuments(req)

            req['excel_file_path'] = report.create_file_path(path)
            req['excel_file_nm'] = report.get_file_name()
            
            try:    
                report.create_report(req)
            except TimeoutError as err:
                print("[ERROR] %s" % err)
                logger.error("[ERROR] %s" % err)
                retry = 0 
                while retry < RETRY_TIMES:
                    retry += 1
                    print("[Retry:%d]" % retry)
                    logger.error("[Retry:%d]" % retry)
                    try:
                        report.create_report(req)
                    except TimeoutError as err:
                        if retry >= RETRY_TIMES:
                            print("[Retry failed-ERROR] %s" % err)
                            logger.error("[Retry failed-ERROR] %s" % err)
                            report.close_workbook()
                            
                            raise err
                        
                        time.sleep(SLEEP_FOR_WHEN_RETRY)
                        pass
            except pymysql.err.OperationalError as err:
                print("[ERROR] %s" % err)
                logger.error("[ERROR] %s" % err)
                retry = 0 
                while retry < RETRY_TIMES:
                    retry += 1
                    print("[Retry:%d]" % retry)
                    logger.error("[Retry:%d]" % retry)
                    try:
                        report.create_report(req)
                    except pymysql.err.OperationalError as err:
                        if retry >= RETRY_TIMES:
                            print("[Retry failed-ERROR] %s" % err)
                            logger.error("[Retry failed-ERROR] %s" % err)
                            report.close_workbook()
                            
                            raise err
                        
                        time.sleep(SLEEP_FOR_WHEN_RETRY)
                        pass
                    
            except socket.timeout as err:
                print("[ERROR] %s" % err)
                logger.error("[ERROR] %s" % err)
                retry = 0 
                while retry < RETRY_TIMES:
                    retry += 1
                    print("[Retry:%d]" % retry)
                    logger.error("[Retry:%d]" % retry)
                    try:
                        report.create_report(req)
                    except socket.timeout as err:
                        if retry >= RETRY_TIMES:
                            print("[Retry failed-ERROR] %s" % err)
                            logger.error("[Retry failed-ERROR] %s" % err)
                            report.close_workbook()
                            
                            raise err
                        
                        time.sleep(SLEEP_FOR_WHEN_RETRY)
                        pass
                    
                    
    elif mode in [ MODE_TREND ]:
        for pseq in project_seqs.split(","):
            print(pseq)
            req = {
                "mode" : mode,
                "seq": 0,
                "reg_dt" : str(dt.now().strftime('%Y%m%d%H')),
                "type_cd" : typecd,
                "channels" : "all",
                "start_date" : str(startdate.strftime('%Y-%m-%dT%H:00:00')) if mode=='documents' else str(startdate.strftime('%Y-%m-%dT00:00:00')),
                "end_date" : str(enddate.strftime('%Y-%m-%dT%H:00:00')) if mode=='documents' else str(enddate.strftime('%Y-%m-%dT00:00:00')),
                "datasets" : '',
                "project_seq" : int(pseq),
                "compare_yn" : "N"
            }

            report = ReportKDICDocuments(req)

            req['excel_file_path'] = report.create_file_path(path)
            req['excel_file_nm'] = report.get_file_name()
            
            try:    
                report.create_report(req)
            except TimeoutError as err:
                print("[ERROR] %s" % err)
                logger.error("[ERROR] %s" % err)
                retry = 0 
                while retry < RETRY_TIMES:
                    retry += 1
                    print("[Retry:%d]" % retry)
                    logger.error("[Retry:%d]" % retry)
                    try:
                        report.create_report(req)
                    except TimeoutError as err:
                        if retry >= RETRY_TIMES:
                            print("[Retry failed-ERROR] %s" % err)
                            logger.error("[Retry failed-ERROR] %s" % err)
                            report.close_workbook()
                            
                            raise err
                        
                        time.sleep(SLEEP_FOR_WHEN_RETRY)
                        pass
            except pymysql.err.OperationalError as err:
                print("[ERROR] %s" % err)
                logger.error("[ERROR] %s" % err)
                retry = 0 
                while retry < RETRY_TIMES:
                    retry += 1
                    print("[Retry:%d]" % retry)
                    logger.error("[Retry:%d]" % retry)
                    try:
                        report.create_report(req)
                    except pymysql.err.OperationalError as err:
                        if retry >= RETRY_TIMES:
                            print("[Retry failed-ERROR] %s" % err)
                            logger.error("[Retry failed-ERROR] %s" % err)
                            report.close_workbook()
                            
                            raise err
                        
                        time.sleep(SLEEP_FOR_WHEN_RETRY)
                        pass
                    
            except socket.timeout as err:
                print("[ERROR] %s" % err)
                logger.error("[ERROR] %s" % err)
                retry = 0 
                while retry < RETRY_TIMES:
                    retry += 1
                    print("[Retry:%d]" % retry)
                    logger.error("[Retry:%d]" % retry)
                    try:
                        report.create_report(req)
                    except socket.timeout as err:
                        if retry >= RETRY_TIMES:
                            print("[Retry failed-ERROR] %s" % err)
                            logger.error("[Retry failed-ERROR] %s" % err)
                            report.close_workbook()
                            
                            raise err
                        
                        time.sleep(SLEEP_FOR_WHEN_RETRY)
                        pass

    print(str(enddate.strftime('%Y-%m-%dT%H:00:00')))
    logger.info("Finished.")
