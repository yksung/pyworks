# -*- coding : utf-8 -*-
'''
Created on 2017. 6. 20.

@author: Holly
*** params에 변경이 생겨서 원본이 훼손될 가능성이 있다면, parameter에 params 값을 넘길 때, copy.copy(params)로 넘겨준다.(call-by-value 방식)
'''
import xlsxwriter
import os
import com.wisenut.dao.esclient as es
import com.wisenut.dao.mariadbclient as mariadb
from com.wisenut.reports.report import Report 
import copy
from datetime import timedelta, date
import re

class ReportEmotions(Report):
    workbook = None
    header = None
    default = None
    INDEX_NAME="emotions*"
    
    # 데이터셋이 여러개일 때 데이터셋별로 emotions(index)의 추이
    def dataset_count_per_day_in_emotions(self, params):
        worksheet = self.workbook.add_worksheet('분석량 추이')
        sum_per_dataset = {}
        
        # 헤더
        worksheet.write(0, 0, '일자', self.header)
        col_header = 0
        for dataset_name in self.dataset_names.split(","):
            worksheet.write(0, 1+col_header, dataset_name, self.header)
            col_header += 1
        worksheet.write(0, 1+col_header, '합계', self.header)
            
        # 데이터
        qdsl = self.query.DATASET_COUNT_PER_DAY(self.compare)
        result = es.get_aggregations(copy.copy(qdsl), params, self.INDEX_NAME)
        
        if 'hits' in result and result['hits']['total']>0:
            row = 0
            for bucket in result['aggregations']['my_aggs1']['buckets']:
                if self.compare:
                    worksheet.write(1+row, 0, bucket['key'], self.header)
                else:
                    worksheet.write(1+row, 0, bucket['key_as_string'], self.header)
                    
                sum_per_day = 0
                col_body = 0
                for dataset_seq in params['datasets'].split("^"):
                    count_of_this_dataset = bucket['my_aggs2']['buckets'][dataset_seq]['doc_count']
                    sum_per_day += count_of_this_dataset
                    sum_per_dataset[dataset_seq] = count_of_this_dataset if dataset_seq not in sum_per_dataset else sum_per_dataset[dataset_seq]+count_of_this_dataset
                    
                    worksheet.write(1+row, 1+col_body, count_of_this_dataset, self.default)
                    col_body += 1
                    
                worksheet.write(1+row, 1+col_body, sum_per_day, self.default)
                row += 1
                
            # 합계
            if len(params['datasets'].split("^"))==1:
                worksheet.write(1+row, 0, '합계', self.header)
                col_footer = 0
                for dataset_seq in params['datasets'].split("^"):
                    worksheet.write(1+row, 1+col_footer, '', self.header)
                    col_footer += 1
                worksheet.write(1+row, 1+col_footer, sum_per_dataset[dataset_seq], self.header)
                
                
                
    def dataset_occupations_per_depth1_in_emotions(self, params):
        worksheet = self.workbook.add_worksheet('채널분석량')
        # 헤더
        worksheet.write(0, 0, '데이터셋', self.header)
        worksheet.write(0, 1, '채널', self.header)
        if not self.compare:
            worksheet.write(0, 2, '문서수', self.header)
        else:
            worksheet.write(0, 2, '날짜범위', self.header)
            worksheet.write(0, 3, '문서수', self.header)
            
        # 데이터
        qdsl = self.query.DATASET_OCCUPATIONS_PER_DEPTH1(self.compare)
        result = es.get_aggregations(copy.copy(qdsl), params, self.INDEX_NAME)
        total = result['hits']['total']
        row = 0
        
        if 'hits' in result and result['hits']['total']>0:
            if not self.compare:
                for dataset_seq in params['datasets'].split("^"):
                    for d1 in result['aggregations']['my_aggs1']['buckets'][dataset_seq]['my_aggs2']['buckets']:
                        dataset_name = mariadb.get_dataset_name(dataset_seq) if mariadb.get_dataset_name(dataset_seq)!=None else 'unknown'
                        
                        worksheet.write(1+row, 0, dataset_name, self.default) # 데이터셋 이름
                        worksheet.write(1+row, 1, d1['key'], self.default) # 데이터셋 이름
                        worksheet.write(1+row, 2, d1['doc_count'], self.default) # 데이터셋 이름
                        row += 1
            else:
                for dataset_seq in params['datasets'].split("^"):
                    for bucket1 in result['aggregations']['my_aggs1']['buckets'][dataset_seq]['my_aggs2']['buckets']:
                        for bucket2 in bucket1['my_aggs3']['buckets']:
                            dataset_name = mariadb.get_dataset_name(dataset_seq) if mariadb.get_dataset_name(dataset_seq)!=None else 'unknown'
                            
                            worksheet.write(1+row, 0, mariadb.get_dataset_name(dataset_seq), self.default) # 데이터셋 이름
                            worksheet.write(1+row, 1, bucket1['key'], self.default) # 데이터셋 이름
                            worksheet.write(1+row, 2, bucket2['key'], self.default) # 데이터셋 이름
                            worksheet.write(1+row, 3, bucket2['doc_count'], self.default) # 데이터셋 이름
                            row += 1
                         
            if len(params['datasets'].split("^"))==1:   
                worksheet.write(row+1, 0, '합계', self.header)
                worksheet.write(row+1, 1, '', self.header)
                if not self.compare:
                    worksheet.write(row+1, 2, total, self.header)
                else:
                    worksheet.write(row+1, 2, '', self.header)
                    worksheet.write(row+1, 3, total, self.header)
                
        
    def dataset_occupations_per_depth3_in_emotions(self, params):
        worksheet = self.workbook.add_worksheet('채널분석량 상세')
        #arr_dataset_names = self.dataset_names.split(",")
        # 헤더
        worksheet.write(0, 0, '데이터셋', self.header)
        worksheet.write(0, 1, '1Depth', self.header)
        worksheet.write(0, 2, '2Depth', self.header)
        worksheet.write(0, 3, '3Depth', self.header)
        worksheet.write(0, 4, '문서수', self.header)
            
        # 데이터
        qdsl = self.query.DATASET_OCCUPATIONS_PER_DEPTH3(self.compare)
        result = es.get_aggregations(copy.copy(qdsl), params, self.INDEX_NAME)
        total = result['hits']['total']
        row = 0
        
        if 'hits' in result and result['hits']['total']>0:
            for dataset_seq in params['datasets'].split("^"):
                for d1 in result['aggregations']['my_aggs1']['buckets'][dataset_seq]['my_aggs2']['buckets']:
                    dataset_name = mariadb.get_dataset_name(dataset_seq) if mariadb.get_dataset_name(dataset_seq)!=None else 'unknown'
                    
                    if len(d1['key'].split(">"))>2:
                        depth1, depth2, depth3 = d1['key'].split(">")
                    else:
                        depth1, depth2 = d1['key'].split(">")
                        depth3 = ''
                        
                    worksheet.write(1+row, 0, dataset_name, self.default) # 데이터셋 이름
                    worksheet.write(1+row, 1, re.sub("[\[\]]", "", depth1), self.default) # 데이터셋 이름
                    worksheet.write(1+row, 2, re.sub("[\[\]]", "", depth2), self.default) # 데이터셋 이름
                    worksheet.write(1+row, 3, re.sub("[\[\]]", "", depth3), self.default) # 데이터셋 이름
                    worksheet.write(1+row, 4, d1['doc_count'], self.default) # 데이터셋 이름
                    row += 1
                    
            if len(params['datasets'].split("^"))==1:      
                worksheet.write(row+1, 0, '합계', self.header)
                worksheet.write(row+1, 1, '', self.header)
                worksheet.write(row+1, 2, '', self.header)
                worksheet.write(row+1, 3, '', self.header)
                worksheet.write(row+1, 4, total, self.header)
            
    # 긍부정 분석        
    def occupation_per_emotions(self, params):
        worksheet = self.workbook.add_worksheet('감성분석 점유율')
        #arr_dataset_names = self.dataset_names.split(",")
        
        if not self.compare:
            # 헤더
            worksheet.write(0, 0, '긍부정', self.header)
            worksheet.write(0, 1, '문서수', self.header)
            worksheet.write(0, 2, '점유율(%)', self.header)
            
            # 데이터
            qdsl = self.query.EMOTIONS_OCCUPATIONS(self.compare)
            result = es.get_aggregations(copy.copy(qdsl), params, self.INDEX_NAME)
            total = result['hits']['total']
            total_percentage = 0.0
            row = 0
            for bucket in result['aggregations']['my_aggs1']['buckets']:
                worksheet.write(1+row, 0, bucket['key'], self.default) # 데이터셋 이름
                worksheet.write(1+row, 1, bucket['doc_count'], self.default) # 데이터셋 이름
                
                total_percentage += bucket['doc_count']/total*100
                worksheet.write(1+row, 2, bucket['doc_count']/total*100, self.default) # 데이터셋 이름
                row += 1
            
            worksheet.write(row+1, 0, '합계', self.header)
            worksheet.write(row+1, 1, total, self.header)
            worksheet.write(row+1, 2, total_percentage, self.header)
        else:
            # 헤더
            worksheet.write(0, 0, '날짜', self.header)
            worksheet.write(0, 1, '긍부정', self.header)
            worksheet.write(0, 2, '문서수', self.header)
            
            # 데이터
            qdsl = self.query.EMOTIONS_OCCUPATIONS(self.compare)
            result = es.get_aggregations(copy.copy(qdsl), params, self.INDEX_NAME)
            total = result['hits']['total']
            total_percentage = 0.0
            row = 0
            for bucket1 in result['aggregations']['my_aggs1']['buckets']:
                for bucket2 in bucket1['my_aggs2']['buckets']:
                    worksheet.write(1+row, 0, bucket1['key'], self.default) # 데이터셋 이름
                    worksheet.write(1+row, 1, bucket2['key'], self.default) # 데이터셋 이름
                    worksheet.write(1+row, 2, bucket2['doc_count'], self.default) # 데이터셋 이름
                    row += 1
            
            # 합계
            if len(params['datasets'].split("^"))==1:
                worksheet.write(row+1, 0, '합계', self.header)
                worksheet.write(row+1, 1, '', self.header)
                worksheet.write(row+1, 2, total, self.header)
                
    def emotions_per_day(self, params): 
        worksheet = self.workbook.add_worksheet('감성분석 추이')
        #arr_dataset_names = self.dataset_names.split(",")
        # 헤더
        worksheet.write(0, 0, '긍부정', self.header)
        worksheet.write(0, 1, '일자', self.header)
        worksheet.write(0, 2, '문서수', self.header)
        
        # 데이터
        qdsl = self.query.EMOTIONS_PROGRESS(self.compare)
        result = es.get_aggregations(copy.copy(qdsl), params, self.INDEX_NAME)
        total = result['hits']['total']
        row = 0
        for bucket1 in result['aggregations']['my_aggs1']['buckets']:
            for bucket2 in bucket1['my_aggs2']['buckets']:
                worksheet.write(1+row, 0, bucket1['key'], self.default)
                worksheet.write(1+row, 1, bucket2['key_as_string'], self.default)
                worksheet.write(1+row, 2, bucket2['doc_count'], self.default)
                row += 1
        
        # 합꼐
        if len(params['datasets'].split("^"))==1:
            worksheet.write(row+1, 0, '합계', self.header)
            worksheet.write(row+1, 1, '', self.header)
            worksheet.write(row+1, 2, total, self.header)
        
    # 채널별 수집량        
    def emotions_per_channel(self, params):
        worksheet = self.workbook.add_worksheet('채널별 감성분석')
        #arr_dataset_names = self.dataset_names.split(",")
        
        # 데이터
        qdsl = self.query.EMOTIONS_PER_DEPTH1(self.compare)
        result = es.get_aggregations(copy.copy(qdsl), params, self.INDEX_NAME)
        total = result['hits']['total']
        row = 0
        
        # 헤더
        if not self.compare:
            worksheet.write(0, 0, '채널', self.header)
            worksheet.write(0, 1, '긍부정', self.header)
            worksheet.write(0, 2, '문서수', self.header)
            
            for bucket1 in result['aggregations']['my_aggs1']['buckets']:
                for bucket2 in bucket1['my_aggs2']['buckets']:
                    worksheet.write(1+row, 0, mariadb.get_channel_name(1, bucket1['key'])[0], self.default)
                    worksheet.write(1+row, 1, bucket2['key'], self.default)
                    worksheet.write(1+row, 2, bucket2['doc_count'], self.default)
                    row += 1
                    
            # 합꼐
            if len(params['datasets'].split("^"))==1:
                worksheet.write(row+1, 0, '합계', self.header)
                worksheet.write(row+1, 1, '', self.header)
                worksheet.write(row+1, 2, total, self.header)
        else:
            worksheet.write(0, 0, '일자', self.header)
            worksheet.write(0, 1, '채널', self.header)
            worksheet.write(0, 2, '긍부정', self.header)
            worksheet.write(0, 3, '문서수', self.header)
            
            for bucket1 in result['aggregations']['my_aggs1']['buckets']:
                for bucket2 in bucket1['my_aggs2']['buckets']:
                    for bucket3 in bucket2['my_aggs3']['buckets']:
                        worksheet.write(1+row, 0, bucket1['key'], self.default)
                        worksheet.write(1+row, 1, mariadb.get_channel_name(1, bucket2['key'])[0], self.default)
                        worksheet.write(1+row, 2, bucket3['key'], self.default)
                        worksheet.write(1+row, 3, bucket3['doc_count'], self.default)
                        row += 1
        
            # 합꼐
            if len(params['datasets'].split("^"))==1:
                worksheet.write(row+1, 0, '합계', self.header)
                worksheet.write(row+1, 1, '', self.header)
                worksheet.write(row+1, 2, '', self.header)
                worksheet.write(row+1, 3, total, self.header)
    
    def emotions_per_causes(self, params):
        worksheet = self.workbook.add_worksheet('언급원인별 분석')
        #arr_dataset_names = self.dataset_names.split(",")
        if not self.compare:
            # 헤더
            worksheet.write(0, 0, '1Depth', self.header)
            worksheet.write(0, 1, '2Depth', self.header)
            worksheet.write(0, 2, '3Depth', self.header)
            worksheet.write(0, 3, '대분류', self.header)
            worksheet.write(0, 4, '중분류', self.header)
            worksheet.write(0, 5, '소분류', self.header)
            worksheet.write(0, 6, '긍부정', self.header)
            worksheet.write(0, 7, '문서수', self.header)
                
            # 데이터
            qdsl = self.query.EMOTIONS_PER_CAUSES(self.compare)
            result = es.get_aggregations(copy.copy(qdsl), params, self.INDEX_NAME)
            total = result['hits']['total']
            row = 0
            for bucket1 in result['aggregations']['my_aggs1']['buckets']:
                for bucket2 in bucket1['my_aggs2']['buckets']:
                    for bucket3 in bucket2['my_aggs3']['buckets']:
                        for bucket4 in bucket3['my_aggs4']['buckets']:
                            for bucket5 in bucket4['my_aggs5']['buckets']:
                                depth_level = bucket1['key'].split(">")
                                worksheet.write(1+row, 0, re.sub("[\[\]]", "", depth_level[0]) if len(bucket1['key'].split(">"))>=0 else '', self.default)
                                worksheet.write(1+row, 1, re.sub("[\[\]]", "", depth_level[1]) if len(bucket1['key'].split(">"))>=1 else '', self.default)
                                worksheet.write(1+row, 2, re.sub("[\[\]]", "", depth_level[2]) if len(bucket1['key'].split(">"))>=2 else '', self.default)
                                worksheet.write(1+row, 3, bucket2['key'], self.default)
                                worksheet.write(1+row, 4, bucket3['key'], self.default)
                                worksheet.write(1+row, 5, bucket4['key'], self.default)
                                worksheet.write(1+row, 6, bucket5['key'], self.default)
                                worksheet.write(1+row, 7, bucket5['doc_count'], self.default)
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
                worksheet.write(row+1, 7, total, self.header)
        else:
            # 헤더
            worksheet.write(0, 0, '1Depth', self.header)
            worksheet.write(0, 1, '2Depth', self.header)
            worksheet.write(0, 2, '3Depth', self.header)
            worksheet.write(0, 3, '대분류', self.header)
            worksheet.write(0, 4, '중분류', self.header)
            worksheet.write(0, 5, '소분류', self.header)
            worksheet.write(0, 6, '긍부정', self.header)
            worksheet.write(0, 7, '날짜', self.header)
            worksheet.write(0, 8, '문서수', self.header)
                
            # 데이터
            qdsl = self.query.EMOTIONS_PER_CAUSES(self.compare)
            result = es.get_aggregations(copy.copy(qdsl), params, self.INDEX_NAME)
            total = result['hits']['total']
            row = 0
            for bucket1 in result['aggregations']['my_aggs1']['buckets']:
                for bucket2 in bucket1['my_aggs2']['buckets']:
                    for bucket3 in bucket2['my_aggs3']['buckets']:
                        for bucket4 in bucket3['my_aggs4']['buckets']:
                            for bucket5 in bucket4['my_aggs5']['buckets']:
                                for bucket6 in bucket5['my_aggs6']['buckets']:
                                    depth_level = bucket1['key'].split(">")
                                    worksheet.write(1+row, 0, re.sub("[\[\]]", "", depth_level[0]) if len(bucket1['key'].split(">"))>=0 else '', self.default)
                                    worksheet.write(1+row, 1, re.sub("[\[\]]", "", depth_level[1]) if len(bucket1['key'].split(">"))>=1 else '', self.default)
                                    worksheet.write(1+row, 2, re.sub("[\[\]]", "", depth_level[2]) if len(bucket1['key'].split(">"))>=2 else '', self.default)
                                    worksheet.write(1+row, 3, bucket2['key'], self.default)
                                    worksheet.write(1+row, 4, bucket3['key'], self.default)
                                    worksheet.write(1+row, 5, bucket4['key'], self.default)
                                    worksheet.write(1+row, 6, bucket5['key'], self.default)
                                    worksheet.write(1+row, 7, bucket6['key'], self.default)
                                    worksheet.write(1+row, 8, bucket6['doc_count'], self.default)
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
      
    
    
    def create_report(self, params):
        self.workbook = xlsxwriter.Workbook(os.path.join(self.BASE_EXCEL_DIRECTORY, self.file_path.replace("/", os.path.sep), self.file_name), options={'strings_to_urls': False, 'strings_to_numbers': True} )
        self.header = self.workbook.add_format(self.HEADER_FORMAT)
        self.default = self.workbook.add_format(self.DEFAULT_FORMAT)
        
        self.cover_page(copy.copy(params))
        if 'datasets' in params and len(params['datasets'].split("^"))>1:
            self.dataset_count_per_day_in_emotions(copy.copy(params)) # 이게 새로운거
            self.dataset_occupations_per_depth1_in_emotions(copy.copy(params)) # 이게 새로운거(copy.copy(params))
            if not self.compare:
                self.dataset_occupations_per_depth3_in_emotions(copy.copy(params)) # 이게 새로운거(copy.copy(params))
        else:
            self.occupation_per_emotions(copy.copy(params))
            if not self.compare:
                self.emotions_per_day(copy.copy(params))
            self.emotions_per_channel(copy.copy(params))
            self.emotions_per_causes(copy.copy(params))
               
            # 2017.07.24 데이터셋이 한 개일 때만 원문 리스트를 만든다.
            if not self.compare:
                self.create_documents_list(copy.copy(params), self.INDEX_NAME)
            else:
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
                    
                    self.create_documents_list(new_params, self.INDEX_NAME)
        
        
        self.workbook.close()