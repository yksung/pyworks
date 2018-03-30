# -*- coding: utf-8 -*-
import sys
import urllib
import http.client
import time
import xlsxwriter
import xlrd
from com.wisenut import myLogger
import logging

############# setting elasticsearch
tousflux_ip='127.0.0.1'
tousflux_port='9099'
tousflux_conn = http.client.HTTPConnection(tousflux_ip, tousflux_port)

############# setting tousflux
sentiment_score = {
    "POSITIVE" : "긍정",
    "NEGATIVE" : "부정",
    "ETC" : "중립"
}

NUMBER_OF_MODULES=10

############# setting logging
logger = myLogger.getMyLogger('posneg-excel', hasConsoleHandler=False, hasRotatingFileHandler=True, logLevel=logging.DEBUG)



def request2tousflux(num, text):
    sentiment = None
    
    request = {
        'sentence' : text,
        'authinit' : 'auth'                    
    }
    tousflux_conn.request("GET", "/SC_EvaluationService.svc/"+num+"?" + urllib.parse.urlencode(request, 'utf-8'), "", { "Content-Type" : "application/json; charset=utf-8" })
    
    result = str(tousflux_conn.getresponse().read())
    
    if len(result.split("|"))>=5 and result.split("|")[3]!="":
        sentiment = sentiment_score[result.split("|")[3]]
    
    return sentiment




def main(sheetnum):
    excel2read = xlrd.open_workbook(r"F:\dev\ibi-emotions_analysis\20180308\180308_기업과차종_2월_디맵감성분석요청.xlsx", encoding_override='UTF-8')
    excel2write = xlsxwriter.Workbook(r"F:\dev\ibi-emotions_analysis\20180308\180308_기업과차종_2월_디맵감성분석요청_결과(%s).xlsx"%sheetnum)
    
    
    for sidx, sheet in enumerate(excel2read.sheets()):
        print(sidx, sheet.name)
        if str(sidx) == sheetnum:
            worksheet = excel2write.add_worksheet(name=sheet.name)
            
            logger.info("==================================================================================================")
            logger.info("- sheet.num\t:\t%s" % str(sheetnum) )
            logger.info("- sheet.name\t:\t%s" % sheet.name )
            logger.info("- row\t\t:\t%s" % str(sheet.nrows) )
            logger.info("==================================================================================================")
            
            for ridx in range(sheet.nrows):
                if(ridx<1):continue # 첫번째 행은 컬럼 이름
                
                moduleNum = (ridx % NUMBER_OF_MODULES)+1 
                
                text = sheet.cell_value(ridx, sheet.ncols-1)
                
                # 원래 있던 컬럼 내용 그대로 write
                for cidx in range(sheet.ncols):
                    worksheet.write(ridx, cidx, sheet.cell_value(ridx, cidx))
                
                # 긍부정 분석
                posneg = ''
                try:
                    posneg = request2tousflux(str(moduleNum).zfill(2), text)
                except:
                    logger.error("[main] Exception 발생")
                    retry = 0
                    while retry < 10:
                        logger.debug("[main] ~~~~~~~~~~~~~~~~~~~ retry : %d " % retry)
                        retry += 1
                        try:
                            posneg = request2tousflux(str(moduleNum).zfill(2), text)
                        except TimeoutError as err:
                            time.sleep(10)
                            continue
                        
                    if retry>=10 :
                        logger.error("[main] %s Failed. (%s)" % (sheet.name, err))
                        excel2write.close()
                        
                        return
                
                # 긍부정 분석 결과를 가장 마지막 컬럼에 write.
                worksheet.write(ridx, sheet.ncols, posneg)
                
                if ridx % 100 == 0:
                    logger.debug("[%d]"%ridx)
        
        
            logger.info("%s finished successfully." % sheet.name)
    
    excel2write.close()
    

        
        

if __name__ == '__main__':
    sheetnum = sys.argv[1]
    
    main(sheetnum)
    #print(request2tousflux('01', '미주시장 공략형 코나는 감마(Gamma) 1. 6 터보 엔진과 7단 듀얼 클러치 트랜스미션(DCT)이 탑재된 1. 6T 모델 및 누(Nu) 2. 0 앳킨슨(Atkinson) 엔진과 6단 자동변속기가 탑재된 2. 0 모델 등 '))
