'''
Created on 2017. 5. 30.

@author: Holly
'''
# -*- coding : utf-8 -*-
import os
import com.wisenut.dao as dao
import logging
import math
import re

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

BASE_EXCEL_DIRECTORY='/data/dmap-data/dmap-excel'
crawl_doc_fields = [ 'doc_id','doc_datetime','doc_writer','doc_url','doc_title','doc_content','user_id','view_count','comment_count','like_count','dislike_count','share_count','locations','depth1_nm','depth1_seq','depth2_nm','depth2_seq','depth3_nm','depth3_seq','item_grp_nm','item_grp_seq','item_nm','item_seq' ]


    
def search_create_directory(dirname):
    if not os.path.exists(dirname):
        os.mkdir(dirname)
        
    return dirname
    

      
import xlrd, xlsxwriter

if __name__ == '__main__':
    logger.info("excel maker starts.")
    #01. 엑셀 다운로드 요청 목록을 테이블에서 가져옴.
    for req in dao.get_excel_request():
        logger.debug(req)
        req_seq = req['seq']
        reg_dt = re.compile("[-:\s]").sub("",req['reg_dt'])
        csv_type_name = dao.get_csv_name(req['type_cd'])
        channel = '전채널' if not req['channel'] or req['channel']=='all' else "채널일부"
        period = re.compile("[-:\s]").sub("",req['period'])
        
        #02. 해당 엑셀에 대해서는 table의 상태값을 바꿈.
        dao.update_queue(req)
        
        #03. 각 요청에 해당하는 디렉토리를 찾고(요청시퀀스) 없으면 생성. 파일이름 만듦.
        # 디렉토리 구성 ==> /data/dmap-data/dmap-excel/20170601/12'
        file_path = search_create_directory( os.path.join( BASE_EXCEL_DIRECTORY, reg_dt, req_seq ) )
        file_name = "_".join(req_seq, csv_type_name, channel, period)
        
        if 'datasets' in req:
            for dataset_seq in req['datasets'].split(","): # dataset 하나당 엑셀 하나를 만듦. 따라서 dataset_seq는 한개씩만 넘어감.
                dataset_name = dao.get_dataset_name(dataset_seq)
                file_name += "_"+dataset_name
        
                #03. 데이터 가져오기
                workbook = xlsxwriter.Workbook(file_name+".xlsx", options={'strings_to_urls': False} )
                worksheet = workbook.add_worksheet()
                
                # 엑셀 헤더
                for colidx, field in enumerate(crawl_doc_fields):
                    worksheet.write(0, colidx, field)
                
                req['datasets'] = dataset_seq # req에서 dataset_seq를 한개 값으로만 다시 세팅해서 넘김.
                size = 10000 # 페이징 사이즈
                result = dao.get_es_data(req, size)
                if "hits" in result and result["hits"]["total"] > 0:
                    for row, result in enumerate(result["hits"]["hits"]):
                        for col, field in enumerate(crawl_doc_fields):
                            worksheet.write(row, col, result["_source"][field])
                            
                    workbook.close()
                            
                    # 결과건수가 한 페이지 사이즈보다 큰 경우, scroll을 이용해서 paging하며 결과를 가져옴.
                    # 용량이 클 것으로 예상하여 엑셀 파일도 새로 생성.            
                    if  result["hits"]["total"] > size:
                        for page in range(1, math.ceil(result["hits"]["total"]/size)): # 0, 1, 2, ....
                            # 파일이름 => 12_소셜모니터링_요인분석_all_20170101~20170131_신라면(1).xlsx
                            workbook = xlsxwriter.Workbook(file_name+"("+str(page)+")"+".xlsx", options={'strings_to_urls': False} )
                            worksheet = workbook.add_worksheet()
                            
                            # 엑셀 헤더
                            for colidx, field in enumerate(crawl_doc_fields):
                                worksheet.write(0, colidx, field)
                                
                            result = dao.get_es_data(req, size, scroll_id=result["_scroll_id"])
                            for row, result in enumerate(result["hits"]["hits"]):
                                for col, field in enumerate(crawl_doc_fields):
                                    worksheet.write(row, col, result["_source"][field])
                            
                            workbook.close()
                            
        
        
        