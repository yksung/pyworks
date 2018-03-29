# -*- coding : utf-8 -*-
'''
Created on 2017. 7. 27.
조회수 리포트

@author: Holly
*** params에 변경이 생겨서 원본이 훼손될 가능성이 있다면, parameter에 params 값을 넘길 때, copy.copy(params)로 넘겨준다.(call-by-value 방식)
'''
import xlsxwriter
import os
import com.wisenut.dao.mariadbclient as mariadb
from com.wisenut.reports.report import Report 
import copy
from datetime import timedelta, date
import re

class ReportCount(Report):
    workbook = None
    header = None
    default = None
    
    def make_count_report(self, params, sheettype=0):
        print("making count report starts.")
        
        start_date = re.sub("[-]", "", params['start_date'][0:10])
        end_date = re.sub("[-]", "", params['end_date'][0:10])
        
        sheet_name = ''
        type_cd = ''
        if sheettype == 0:
            sheet_name = '자동완성어 조회수'
            type_cd = 'SCT001'
        else:
            sheet_name = '연관검색어 조회수'
            type_cd = 'SCT002'

        if self.compare:
            sheet_name += "(%s~%s)" % (start_date, end_date)
        
        worksheet = self.workbook.add_worksheet(sheet_name)
        
        # 헤더
        worksheet.write(0, 0, '검색그룹', self.header)
        worksheet.write(0, 1, '검색아이템', self.header)
        worksheet.write(0, 2, '검색데이터셋', self.header)
        worksheet.write(0, 3, '날짜', self.header)
        if sheettype == 0:
            worksheet.write(0, 4, '자동완성어', self.header)
        else:
            worksheet.write(0, 4, '연관검색어', self.header)
        worksheet.write(0, 5, '카테고리', self.header)
        worksheet.write(0, 6, 'CDJ', self.header)
        worksheet.write(0, 7, 'Total', self.header)
        worksheet.write(0, 8, 'PC', self.header)
        worksheet.write(0, 9, 'Mobile', self.header)
            
        # 데이터
        result = mariadb.get_data_for_report_count(type_cd, params['trend_grp_seq'], params['start_date'], params['end_date'], params['trend_dataset_seq'], params['trend_keyword_seq'])
        for idx, row in enumerate(result, 1):
            worksheet.write(idx, 0, row[0], self.default)
            worksheet.write(idx, 1, row[1], self.default)
            worksheet.write(idx, 2, row[2], self.default)
            worksheet.write(idx, 3, row[3], self.default)
            worksheet.write(idx, 4, row[4], self.default)
            worksheet.write(idx, 5, row[5], self.default)
            worksheet.write(idx, 6, row[6], self.default)
            worksheet.write(idx, 7, row[7], self.default)
            worksheet.write(idx, 8, row[8], self.default)
            worksheet.write(idx, 9, row[9], self.default)
                
        
    def create_report(self, params):
        self.workbook = xlsxwriter.Workbook(os.path.join(self.BASE_EXCEL_DIRECTORY, self.file_path.replace("/", os.path.sep), self.file_name), options={'strings_to_urls': False, 'strings_to_numbers': True} )
        self.header = self.workbook.add_format(self.HEADER_FORMAT)
        self.default = self.workbook.add_format(self.DEFAULT_FORMAT)
        
        if self.compare:
            print('compare ', self.compare)
            # 기준날짜
            start_date = date(int(params['start_date'][0:4]), int(params['start_date'][5:7]), int(params['start_date'][8:10]))
            end_date = date(int(params['end_date'][0:4]), int(params['end_date'][5:7]), int(params['end_date'][8:10]))
            for i in range(4):
                time_interval = end_date-start_date
                # 비교 날짜들(1time_interval before)
                this_end_date = end_date - (time_interval+timedelta(days=1))*i # 곱해진 간격만큼 이전 날짜를 구함
                
                new_params = copy.copy(params) 
                new_params['start_date'] = (this_end_date-time_interval).strftime('%Y-%m-%dT00:00:00') 
                new_params['end_date'] = this_end_date.strftime('%Y-%m-%dT23:59:59')
                
                self.make_count_report(new_params, sheettype=0)
                self.make_count_report(new_params, sheettype=1)
        else:
            self.make_count_report(copy.copy(params), sheettype=0)
            self.make_count_report(copy.copy(params), sheettype=1)
                
        
        self.workbook.close()