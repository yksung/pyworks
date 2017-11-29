# -*- coding : utf-8 -*-
import com.wisenut.dao.mariadbclient as db
import com.wisenut.dao.esclient as es
from com.wisenut.utils import file_util
import re, os
import math
from datetime import datetime as dt
from com.wisenut.enums.query import Query
from com.wisenut.config import Config
import sys
import codecs

MODE_DOCUMENTS='documents'
MODE_TOPICS='topics'
MODE_EMOTIONS='emotions'
MODE_TREND='trend'

INDEX_DOCUMENTS="documents*"
INDEX_TOPICS="topics*"
INDEX_EMOTIONS="emotions*"

FIELDS_DOCUMENTS  = [ 'doc_id', 'doc_datetime','doc_writer','doc_url','doc_title','doc_content','depth1_nm','depth2_nm','depth3_nm']
FIELDS_EMOTIONS   = [ 'conceptlevel1', 'conceptlevel2', 'conceptlevel3', 'emotion_type', 'matched_text.string', 'depth1_nm', 'depth2_nm', 'depth3_nm', 'doc_datetime', 'doc_writer', 'doc_url', 'doc_title']

KFIELDS_DOCUMENTS = [ 'ID', '게시일','작성자','URL','제목','내용','채널1','채널2','채널3']
KFIELDS_EMOTIONS  = [ '대분류', '중분류', '소분류', '감성', '분석문장', '채널1', '채널2', '채널3', '게시일', '작성자', 'URL', '제목']

class ES2SCD:
    seq = -1
    reg_dt = ""
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

    def __init__(self, params):
        self.seq = params['seq']
        self.compare = True if params['compare_yn']=='Y' else False

        self.start_date = re.sub("[-:T\s]", "", params['start_date'])[:12]
        self.end_date = re.sub("[-:T\s]", "", params['end_date'])[:12]
        self.reg_dt = re.sub("[-:T\s]", "", params['reg_dt'])

        self.dataset_names = ",".join([db.get_dataset_name(x) if db.get_dataset_name(x)!=None else 'unknown' for x in str(params['datasets']).split("^")]) if params['datasets'] else '' # 6^7^15 -> 신라면,안성탕면,짜파게티
        self.query = Query(params)

        self.file_name = "B-%d-%s-I-C.SCD" % (self.seq, get_current_datetime())


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





    # 원문
    def create_documents_list(self, params, index):
        size = 10000 # 페이징 사이즈
       
        # 검색 시작
        result = es.get_documents(params, size, index, "")

        #worksheet = self.workbook.add_worksheet("원문(%s)"%"~".join([params['start_date'][0:10],params['end_date'][0:10]]))

        # 엑셀 헤더
        '''
        for colidx, field in enumerate(output_fields_korean):
            worksheet.write(0, colidx, field, self.header)
        '''

        if "hits" in result and result["hits"]["total"] > 0:
            scdfile = codecs.open(os.path.join(self.file_path, self.file_name), 'w', 'utf-8')
            
            for this_result in result["hits"]["hits"]:
                for field in FIELDS_DOCUMENTS:
                    if field == 'doc_id':
                        val = this_result["_id"]
                        #worksheet.write(row+1, col, val, self.default)
                        scdfile.write("<DOCID>%s"%val)
                        scdfile.write("\r\n")
                        
                        continue
                        
                    val = this_result["_source"][field] if field in this_result["_source"] else "null"
                    #worksheet.write(row+1, col, val, self.default)
                    scdfile.write("<%s>%s" % (field, val))
                    scdfile.write("\r\n")


            # 결과건수가 한 페이지 사이즈보다 큰 경우, scroll을 이용해서 paging하며 결과를 가져옴.
            # 용량이 클 것으로 예상하여 엑셀 파일도 새로 생성.
            if "hits" in result and result["hits"]["total"] > size:
                for page in range(1, math.ceil(result["hits"]["total"]/size)): # 0, 1, 2, ....
                    scrolled_result = es.get_documents(params, size, index, scroll_id=result["_scroll_id"])
                    for this_result in scrolled_result["hits"]["hits"]:
                        for field in FIELDS_DOCUMENTS:
                            if field == 'doc_id':
                                val = this_result["_id"]
                                #worksheet.write(row+1, col, val, self.default)
                                scdfile.write("<DOCID>%s"%val)
                                scdfile.write("\r\n")
                                
                                continue
                                
                            val = this_result["_source"][field] if field in this_result["_source"] else "null"
                            #worksheet.write(row+1, col, val, self.default)
                            scdfile.write("<%s>%s" % (field, val))
                            scdfile.write("\r\n")

                    if page == math.ceil(result["hits"]["total"]/size)-1: # 마지막 페이지를 처리하고 나면 scroll을 clear
                        if result["_scroll_id"]:
                            es.clear_scroll(result["_scroll_id"])
                            
            scdfile.close()




def get_current_datetime():
    ymdhms = str(dt.now().strftime('%Y%m%d%H%M-%S%f')[:-3])
    
    return ymdhms




if __name__ == '__main__':
    #print(get_current_datetime())
    dir = sys.argv[1]
    
    print("Start es2scd.py")
    
    req = {
        "seq": 99,
        "reg_dt" : "20171129",
        "channels" : "all",
        "start_date" : "2017-07-01T00:00:00",
        "end_date" : "2017-10-31T23:59:59",
        "datasets" : "41",
        "project_seq" : 5,
        "compare_yn" : "N",
        "trend_grp_seq" : 0,
        "trend_dataset_seq" : 0,
        "trend_keyword_seq" : 0
    }
    print('%s %s' % (req['start_date'], req['end_date']))
    
    report = ES2SCD(req)
            
    req['excel_file_path'] = report.create_file_path(dir)
    req['excel_file_nm'] = report.get_file_name()
    report.create_documents_list(req, INDEX_DOCUMENTS)
