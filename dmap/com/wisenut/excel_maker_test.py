'''
Created on 2017. 6. 30.

@author: Holly
'''
from com.wisenut.reports.report_stats import ReportStatistics
from com.wisenut.reports.report_emotions import ReportEmotions
from com.wisenut.reports.report_trend import ReportTrend
from com.wisenut.reports.report_count import ReportCount
if __name__ == '__main__':
    req = {
        "seq": 0,
        "reg_dt" : "20171208",
        "type_cd" : "RSS",
        "channels" : "all",
        "start_date" : "2017-11-09T00:00:00",
        "end_date" : "2017-12-08T23:59:59",
        #"datasets" : "934",
        #"datasets" : "1427~1440",
        #"datasets" : "1422^1441^1442^1443^1444^1445^1446^1447^1448^1449^1450^1451^1452^1453^1454",
        #"datasets" : "1694~1696",
        "datasets" : "1678",
        "project_seq" : 173,
        "compare_yn" : "N",
        "trend_grp_seq" : 0,
        "trend_dataset_seq" : 0,
        "trend_keyword_seq" : 0
    }
    print('%s %s' % (req['start_date'], req['end_date']))
    
    if req['type_cd']=='RSS': # 수집문서통계
        report = ReportStatistics(req)
    elif req['type_cd']=='RSE': # 감성분석
        report = ReportEmotions(req)
    elif req['type_cd']=='RTC': # 검색트렌드 - 조회수
        report = ReportCount(req)
    elif req['type_cd']=='RTT': # 검색트렌드 - 트렌드
        report = ReportTrend(req)
            
    req['excel_file_path'] = report.create_file_path()
    req['excel_file_nm'] = report.get_file_name()
    report.create_report(req)
    
'''
- **G70(1440)** : (2017년 7월 25일 ~ 2017년 08월 24일 : 8월. 총 1개월치)
- **스팅어(1659), C Class(1560), 3시리즈(1533)** : (2017년 7월 25일 ~ 2017년 10월 24일 : 8, 9, 10월. 총 3개월 치)
싼타페 

'''    