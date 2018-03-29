# -*- coding : utf-8 -*-
'''
Created on 2017. 5. 30.

@author: Holly
'''
import com.wisenut.dao.mariadbclient as db
from com.wisenut.reports.report_emotions import ReportEmotions
from com.wisenut.reports.report_stats import ReportStatistics
from com.wisenut.reports.report_count import ReportCount
from com.wisenut.reports.report_trend import ReportTrend
import traceback
import logging
from com.wisenut.config import Config

############# logger 세팅
conf = Config()
logging.config.fileConfig(conf.get_logconfig_path())
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    #1. 엑셀 다운로드 요청 목록을 테이블에서 가져옴.
    for req in db.get_excel_request():
        print(req)
        #2. 리포트 타입별로 엑셀 꾸미기
        if req['type_cd']=='RSS': # 수집문서통계
            report = ReportStatistics(req)
        elif req['type_cd']=='RSE': # 감성분석
            report = ReportEmotions(req)
        elif req['type_cd']=='RTC': # 검색트렌드 - 조회수
            report = ReportCount(req)
        elif req['type_cd']=='RTT': # 검색트렌드 - 트렌드
            report = ReportTrend(req)
            
        # queue 상태 업데이트
        req['status'] = "P"
        req['excel_file_path'] = report.create_file_path()
        req['excel_file_nm'] = report.get_file_name()
        
        print("excel_file_nm : %s" % report.get_file_name())
        db.update_queue(req)
        
        # 리포트 생성                
        try:
            report.create_report(req)
            
            #04. 완료 플래그로 바꿈.
            req['status'] = "C"
            db.update_queue(req)
        except:
            ex = traceback.format_exc()
            logger.error("[excel_maker] errer. Traceback >>> %s" % ex)
            
            req['status'] = "F"
            db.update_queue(req)
    
            