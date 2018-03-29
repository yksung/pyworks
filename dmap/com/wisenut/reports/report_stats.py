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
from com.wisenut.enums.channel import Channel
import re

class ReportStatistics(Report):
    workbook = None
    header = None
    default = None
    INDEX_NAME="documents*"
    INDEX_TOPICS="topics*"
    
    # 일자별 수집량
    def dataset_count_per_day_in_documents(self, params):
        worksheet = self.workbook.add_worksheet('일자별 수집량')
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
        
        if 'hits' in result:
            total = result['hits']['total']
        else:
            total = 0
            
        row = 0
        
        if total > 0:
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
                
        
                
    def dataset_occupations_per_depth1_in_documents(self, params):
        worksheet = self.workbook.add_worksheet('채널별 문서점유율')
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
        if total>0:
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
                            
                            worksheet.write(1+row, 0, dataset_name, self.default) # 데이터셋 이름
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
        
    def dataset_occupations_per_depth3_in_documents(self, params):
        worksheet = self.workbook.add_worksheet('채널별 수집량')
        #arr_dataset_names = self.dataset_names.split(",")
        if not self.compare:
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
            
            if total>0:
                for dataset_seq in params['datasets'].split("^"):
                    for d1 in result['aggregations']['my_aggs1']['buckets'][dataset_seq]['my_aggs2']['buckets']:
                        depth_level = d1['key'].split(">")
                        dataset_name = mariadb.get_dataset_name(dataset_seq) if mariadb.get_dataset_name(dataset_seq)!=None else 'unknown'
                        
                        worksheet.write(1+row, 0, dataset_name, self.default) # 데이터셋 이름
                        worksheet.write(1+row, 1, re.sub("[\[\]]", "", depth_level[0]) if len(depth_level)>=0 else "", self.default) # 데이터셋 이름
                        worksheet.write(1+row, 2, re.sub("[\[\]]", "", depth_level[1]) if len(depth_level)>=1 else "", self.default) # 데이터셋 이름
                        worksheet.write(1+row, 3, re.sub("[\[\]]", "", depth_level[2]) if len(depth_level)>=2 else "", self.default) # 데이터셋 이름
                        worksheet.write(1+row, 4, d1['doc_count'], self.default) # 데이터셋 이름
                        row += 1
                
                if len(params['datasets'].split("^"))==1:        
                    worksheet.write(row+1, 0, '합계', self.header)
                    worksheet.write(row+1, 1, '', self.header)
                    worksheet.write(row+1, 2, '', self.header)
                    worksheet.write(row+1, 3, '', self.header)
                    worksheet.write(row+1, 4, total, self.header)
        else:
            # 헤더
            worksheet.write(0, 0, '데이터셋', self.header)
            worksheet.write(0, 1, '1Depth', self.header)
            worksheet.write(0, 2, '2Depth', self.header)
            worksheet.write(0, 3, '3Depth', self.header)
            worksheet.write(0, 4, '날짜', self.header)
            worksheet.write(0, 5, '문서수', self.header)
                
            # 데이터
            qdsl = self.query.DATASET_OCCUPATIONS_PER_DEPTH3(self.compare)
            result = es.get_aggregations(copy.copy(qdsl), params, self.INDEX_NAME)
            total = result['hits']['total']
            row = 0
            
            if total>0:
                for dataset_seq in params['datasets'].split("^"):
                    for d1 in result['aggregations']['my_aggs1']['buckets'][dataset_seq]['my_aggs2']['buckets']:
                        for d2 in d1['my_aggs3']['buckets']:
                            depth_level = d1['key'].split(">")
                            dataset_name = mariadb.get_dataset_name(dataset_seq) if mariadb.get_dataset_name(dataset_seq)!=None else 'unknown'
                            
                            worksheet.write(1+row, 0, dataset_name, self.default) # 데이터셋 이름
                            worksheet.write(1+row, 1, re.sub("[\[\]]", "", depth_level[0]) if len(depth_level)>=0 else "", self.default) # 데이터셋 이름
                            worksheet.write(1+row, 2, re.sub("[\[\]]", "", depth_level[1]) if len(depth_level)>=1 else "", self.default) # 데이터셋 이름
                            worksheet.write(1+row, 3, re.sub("[\[\]]", "", depth_level[2]) if len(depth_level)>=2 else "", self.default) # 데이터셋 이름
                            worksheet.write(1+row, 4, d2['key'], self.default) # 데이터셋 이름
                            worksheet.write(1+row, 5, d2['doc_count'], self.default) # 데이터셋 이름
                            row += 1
                
                if len(params['datasets'].split("^"))==1:        
                    worksheet.write(row+1, 0, '합계', self.header)
                    worksheet.write(row+1, 1, '', self.header)
                    worksheet.write(row+1, 2, '', self.header)
                    worksheet.write(row+1, 3, '', self.header)
                    worksheet.write(row+1, 4, '', self.header)
                    worksheet.write(row+1, 5, total, self.header)
            
    # 채널별 문서 점유율        
    def depth1_channel_occupations_in_documents(self, params):
        worksheet = self.workbook.add_worksheet('채널별 문서점유율')
        
        result = es.get_aggregations(self.query.DEPTH1_CHANNEL_OCCUPATIONS(self.compare), params, self.INDEX_NAME)
        total = result['hits']['total']
            
        if not self.compare:
            # 헤더
            worksheet.write(0, 0, '채널별', self.header)
            worksheet.write(0, 1, '문서수', self.header)
            worksheet.write(0, 2, '비율(%)', self.header)
            
            # 데이터
            total_percentage = 0.0
            row=1
            for bucket in result['aggregations']['my_aggs1']['buckets']:
                worksheet.write(row, 0, bucket['key'], self.default) # Depth1
                worksheet.write(row, 1, bucket['doc_count'], self.default)
                worksheet.write(row, 2, bucket['doc_count']/total*100, self.default)
                total_percentage += bucket['doc_count']/total*100
                row+=1
                
            # 합계
            if len(params['datasets'].split("^"))==1:
                worksheet.write(row, 0, "합계", self.header)
                worksheet.write(row, 1, total, self.header)
                worksheet.write(row, 2, total_percentage, self.header)
        else:
            worksheet.write(0, 0, '채널별', self.header)
            worksheet.write(0, 1, '날짜', self.header)
            worksheet.write(0, 2, '문서수', self.header)
        
            # 데이터
            row=1
            for bucket1 in result['aggregations']['my_aggs1']['buckets']:
                for bucket2 in bucket1['my_aggs2']['buckets']:
                    worksheet.write(row, 0, bucket1['key'], self.default) # Depth1
                    worksheet.write(row, 1, bucket2['key'], self.default)
                    worksheet.write(row, 2, bucket2['doc_count'], self.default)
                    row+=1
                
            # 합계
            if len(params['datasets'].split("^"))==1:
                worksheet.write(row, 0, "합계", self.header)
                worksheet.write(row, 1, '', self.header)
                worksheet.write(row, 2, total, self.header)
        
        
                    
    # 포털 문서량
    def depth2_channel_occupations_in_documents(self, params, depth1_seq):
        sheet_name = ''
        if depth1_seq is Channel.COMMUNITY:
            sheet_name = '커뮤니티 문서량'
        elif depth1_seq is Channel.MEDIA:
            sheet_name = '미디어 문서량'
        elif depth1_seq is Channel.SNS:
            sheet_name = 'SNS 문서량'
        elif depth1_seq is Channel.PORTAL:
            sheet_name = '포털 문서량'
            
        print("sheet_name :: %s" % sheet_name)
        worksheet = self.workbook.add_worksheet(sheet_name)
        
        result = es.get_aggregations(self.query.DEPTH2_CHANNEL_OCCUPATIONS(depth1_seq.value, self.compare), params, self.INDEX_NAME)
        total = result['hits']['total']
            
        if not self.compare:
            # 헤더
            worksheet.write(0, 0, '채널별', self.header)
            worksheet.write(0, 1, '문서수', self.header)
            worksheet.write(0, 2, '비율(%)', self.header)
            
            # 데이터
            total_percentage = 0.0
            row=1
            for bucket in result['aggregations']['my_aggs1']['buckets']:
                worksheet.write(row, 0, bucket['key'], self.default) # Depth1
                worksheet.write(row, 1, bucket['doc_count'], self.default)
                worksheet.write(row, 2, bucket['doc_count']/total*100, self.default)
                total_percentage += bucket['doc_count']/total*100
                row+=1
                
            # 합계
            if len(params['datasets'].split("^"))==1:
                worksheet.write(row, 0, "합계", self.header)
                worksheet.write(row, 1, total, self.header)
                worksheet.write(row, 2, total_percentage, self.header)
        else:
            worksheet.write(0, 0, '채널별', self.header)
            worksheet.write(0, 1, '날짜', self.header)
            worksheet.write(0, 2, '문서수', self.header)
        
            # 데이터
            row=1
            for bucket1 in result['aggregations']['my_aggs1']['buckets']:
                for bucket2 in bucket1['my_aggs2']['buckets']:
                    worksheet.write(row, 0, bucket1['key'], self.default) # Depth1
                    worksheet.write(row, 1, bucket2['key'], self.default)
                    worksheet.write(row, 2, bucket2['doc_count'], self.default)
                    row+=1
                
            # 합계
            if len(params['datasets'].split("^"))==1:
                worksheet.write(row, 0, "합계", self.header)
                worksheet.write(row, 1, '', self.header)
                worksheet.write(row, 2, total, self.header)
            
        
    def topics_list(self, params):
        worksheet = self.workbook.add_worksheet("화제어_명사(%s)"%"~".join([params['start_date'][0:10],params['end_date'][0:10]]))
        # 헤더
        worksheet.write(0, 0, '순위', self.header)
        worksheet.write(0, 1, '화제어', self.header)
        worksheet.write(0, 2, '문서수', self.header)
        if not self.compare:
            worksheet.write(0, 3, '연관어', self.header)
            worksheet.write(0, 4, '문서수', self.header)
            #worksheet.write(0, 6, '표현어', self.header)
            
        # 데이터
        result_topic = es.get_aggregations(self.query.TOPICS_LIST(), params, self.INDEX_TOPICS)
        if not self.compare:
            row=0
            for seq, bucket1 in enumerate(result_topic['aggregations']['my_aggs1']['buckets']):
                topic = bucket1['key']
                if len(bucket1['my_aggs2']['buckets'])>0:
                    for bucket2 in bucket1['my_aggs2']['buckets']:
                        #phrase = re.sub("[\[\]]", "", topic) + " AND " + re.sub("[\[\]]", "", bucket2['key'])
                        
                        #result_verb = es.get_aggregations(self.query.TOPICS_VERBS_LIST(), params, Query.INDEX_TOPICS)
                        #verb_list = ",".join([ bucket3['key'] for bucket3 in result_verb['aggregations']['my_aggs3']['buckets'] ])
                        
                        worksheet.write(1+row, 0, 1+seq, self.default)
                        worksheet.write(1+row, 1, re.sub("[\[\]]", "", topic), self.default)
                        worksheet.write(1+row, 2, bucket1['doc_count'], self.default)
                        worksheet.write(1+row, 3, bucket2['key'], self.default)
                        worksheet.write(1+row, 4, bucket2['doc_count'], self.default)
                        #worksheet.write(1+row, 6, verb_list, self.default)
                        row += 1    
                else:
                    worksheet.write(1+row, 0, 1+seq, self.default)
                    worksheet.write(1+row, 1, re.sub("[\[\]]", "", topic), self.default)
                    worksheet.write(1+row, 2, bucket1['doc_count'], self.default)
                    worksheet.write(1+row, 3, '', self.default)
                    worksheet.write(1+row, 4, '', self.default)
                    #worksheet.write(1+row, 6, '', self.default)
                    row += 1
                
        else:    
            row=0
            for seq, bucket1 in enumerate(result_topic['aggregations']['my_aggs1']['buckets']):
                topic = bucket1['key']
                
                worksheet.write(1+row, 0, 1+seq, self.default)
                worksheet.write(1+row, 1, re.sub("[\[\]]", "", topic), self.default)
                worksheet.write(1+row, 2, bucket1['doc_count'], self.default)
                row += 1
                
                
    def topics_verb_list(self, params):
        worksheet = self.workbook.add_worksheet("화제어_동사(%s)"%"~".join([params['start_date'][0:10],params['end_date'][0:10]]))
        # 헤더
        worksheet.write(0, 0, '순위', self.header)
        worksheet.write(0, 1, '화제어', self.header)
        worksheet.write(0, 2, '문서수', self.header)
            
        # 데이터
        result_topic = es.get_aggregations(self.query.TOPICS_VERBS_LIST(), params, self.INDEX_TOPICS)
        row=0
        for seq, bucket1 in enumerate(result_topic['aggregations']['my_aggs1']['buckets']):
            topic = bucket1['key']
            
            worksheet.write(1+row, 0, 1+seq, self.default)
            worksheet.write(1+row, 1, re.sub("[\[\]]", "", topic), self.default)
            worksheet.write(1+row, 2, bucket1['doc_count'], self.default)
            row += 1
                
    def create_report(self, params):
        self.workbook = xlsxwriter.Workbook(os.path.join(self.BASE_EXCEL_DIRECTORY, self.file_path.replace("/", os.path.sep), self.file_name), options={'strings_to_urls': False, 'strings_to_numbers': True} )
        self.header = self.workbook.add_format(self.HEADER_FORMAT)
        self.default = self.workbook.add_format(self.DEFAULT_FORMAT)
        
        self.cover_page(copy.copy(params))              # 커버페이지
        if 'datasets' in params and len(params['datasets'].split("^"))>1: # 데이터셋이 한 개일 때
            self.dataset_count_per_day_in_documents(copy.copy(params))
            self.dataset_occupations_per_depth1_in_documents(copy.copy(params))
            self.dataset_occupations_per_depth3_in_documents(copy.copy(params))
        else:
            if not self.compare:
                self.dataset_count_per_day_in_documents(copy.copy(params))
            self.depth1_channel_occupations_in_documents(copy.copy(params))    
            self.depth2_channel_occupations_in_documents(copy.copy(params), Channel.PORTAL)
            self.depth2_channel_occupations_in_documents(copy.copy(params), Channel.MEDIA)
            self.depth2_channel_occupations_in_documents(copy.copy(params), Channel.COMMUNITY)
            self.depth2_channel_occupations_in_documents(copy.copy(params), Channel.SNS)
            self.dataset_occupations_per_depth3_in_documents(copy.copy(params))
               
            if not self.compare:
                self.topics_list(copy.copy(params))
                self.topics_verb_list(copy.copy(params))
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
                    
                    self.topics_list(new_params)
                    self.topics_verb_list(new_params)
                    self.create_documents_list(new_params, self.INDEX_NAME)
        
        self.workbook.close()