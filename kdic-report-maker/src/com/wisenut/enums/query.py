'''
Created on 2017. 6. 22.

@author: Holly
채널 코드 관리를 위한 Enum 클래스
'''
#from enum import Enum, auto
import json, re
from datetime import date, timedelta
import com.wisenut.dao.mariadbclient as mariadb

#class Query(Enum):
class Query():
    INDEX_DOCUMENTS = "documents-*"
    INDEX_EMOTION = "emotions-*"
    INDEX_TOPICS = "topics-*"
    TYPE = "docs"

    params = None
    start_date = None
    end_date = None
    str_start_date = ''
    str_end_date = ''
    time_interval = None
    
    
  
    
    def __init__(self, params):
        self.params = params
        self.start_date = date(int(params['start_date'][0:4]), int(params['start_date'][5:7]), int(params['start_date'][8:10]))
        self.end_date = date(int(params['end_date'][0:4]), int(params['end_date'][5:7]), int(params['end_date'][8:10]))

        self.str_start_date = self.start_date.strftime('%Y-%m-%dT00:00:00')
        self.str_end_date = self.end_date.strftime('%Y-%m-%dT23:59:59')
        self.time_interval = self.end_date-self.start_date+timedelta(days=1)

  
  

    def ALL_TOPICS_LIST(self, dataset_name):
        query = {
          "size": 0,
          "query": {
            "bool" : {
                "filter" : [
                    self.get_period_query(),
                    self.get_project_seq_query(),
                    self.get_project_filter_query(self.params['project_seq']),
                    {
                        "exists": {
                            "field": "related_words" # related_words가 null이거나 Array가 비어있는 경우를 제외한 나머지
                        }
                    },
                    {
                        "bool" : {
                            "must_not" : [
                                {
                                    "term" : {
                                        "topic_class.keyword" : "VV"
                                    }
                                },
                                {
                                  "terms" : {
                                        "topic.keyword" : [ "증권", "은행", "저축은행", "보험" ]
                                    }
                                },
                                {
                                  "term": {
                                    "related_words.keyword": ""
                                  }
                                },
                                { 
                                  "script": {
                                    "script" : {
                                        "inline": "doc['topic.keyword'].value.replace(' ', '')==params.dataset_name.replace(' ', '')",
                                        "lang" : "painless",
                                        "params" : {
                                            "dataset_name" : dataset_name
                                        }
                                    }
                                  }
                                }
                            ]
                        }
                    }
                ]
            }
          },
         "aggs": {
            "my_aggs0": {
              "date_histogram": {
                "field": "doc_datetime",
                "interval": "day"
              },
              "aggs": {
                "my_aggs1": {
                  "terms": {
                    "field": "topic.keyword",
                    "size": 1000
                  },
                  "aggs": {
                    "my_aggs2": {
                      "terms": {
                        "field": "related_words.keyword",
                        "size": 5
                      }
                    }
                  }
                }
              }
            }
          }
        }

        if self.get_channel_query():
            query['query']['bool']['filter'].append(self.get_channel_query())


        # 2017.07.26 데이터셋 조건도 검색에 걸어줘야 날짜별 doc_count가 제한된 데이터셋 조건 내에서 합산되어 나옴.
        query['query']['bool']['filter'].append({ 'bool' : {'should': self.get_dataset_query(self.params['project_seq'], self.params['datasets'])}})

        return query
    
    
    
    
    def EMOTIONS_PER_CAUSES(self):
        query = {
          "size": 0,
          "query": {
            "bool" : {
                "filter" : [
                    self.get_period_query(),
                    self.get_project_seq_query(),
                    self.get_project_filter_query(self.params['project_seq']),
                ]
            }
          },
          "aggs": {
            "my_aggs0": {
              "date_histogram": {
                "field": "doc_datetime",
                "interval": "day"
              },
              "aggs": {
                "my_aggs1": {
                  "terms": {
                    "script": "doc['depth1_nm.keyword'] + '>' + doc['depth2_nm.keyword'] + '>' + doc['depth3_nm.keyword']",
                    "size": 1000
                  },
                  "aggs": {
                    "my_aggs2": {
                      "terms": {
                        "field": "conceptlevel1.keyword",
                        "size": 1000
                      },
                      "aggs": {
                        "my_aggs3": {
                          "missing": {
                            "field": "conceptlevel2.keyword"
                          },
                          "aggs": {
                            "my_aggs4": {
                              "missing": {
                                "field": "conceptlevel3.keyword"
                              },
                              "aggs": {
                                "my_aggs5": {
                                  "terms": {
                                    "field": "emotion_type.keyword",
                                    "size": 10
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
       
        if self.get_channel_query():
            query['query']['bool']['must'].append(self.get_channel_query())

        query['query']['bool']['filter'].append({ 'bool' : {'should': self.get_dataset_query(self.params['project_seq'], self.params['datasets'])}})


        return query
        



    def get_period_query(self):
        return {
            "range": {
                "upd_datetime": {
                    "gte" : self.params['start_date'] if self.params["start_date"] else "1900-01-01T00:00:00",
                    "lt" : self.params['end_date'] if self.params["end_date"] else "2100-12-31T23:59:59"
                }
            }
        }
        
        
        
        
    def get_project_seq_query(self):
        return {
            "term" : {
                "project_seq" : self.params['project_seq']
            }
        }




    def get_channel_query(self):
        channel = re.sub(";$", "", self.params["channels"]) # 1^5,6,7,8;2^11,12,13,14

        if not channel or channel == "all":
            return None
        else:
            query = ''
            for c in channel.split(";"):
                depth1_seq = c.split("^")[0]

                query += "("
                query += "depth1_seq:"+depth1_seq
                if len(c.split("^"))>1:
                    query += " AND depth2_seq:("+" OR ".join(c.split("^")[1].split(","))+")"
                query += ")"

                query += " OR "

            return {
                    "query_string": {
                        "query": re.compile(" OR $").sub("", query)
                    }
                }




    def get_project_filter_query(self, project_seq):
        project_filter_keywords = mariadb.get_project_filter_keywords(project_seq)
        
        project_title_filter = project_filter_keywords['title_filter_keywords'].strip() if project_filter_keywords and 'title_filter_keywords' in project_filter_keywords else ''
        project_content_filter = project_filter_keywords['content_filter_keywords'].strip() if project_filter_keywords and 'content_filter_keywords' in project_filter_keywords else ''
        project_url_filter = project_filter_keywords['filter_urls'].strip() if project_filter_keywords and 'filter_urls' in project_filter_keywords else ''
        project_regex_filter = project_filter_keywords['regex_filter_keywords'].strip() if project_filter_keywords and 'regex_filter_keywords' in project_filter_keywords else ''
        
        must_not = []
        
        #2-1. 제목 필터 쿼리
        if len(project_title_filter)>0:
            must_not.append({
                "query_string" : {
                    "fields": ["doc_title"],
                    "query" : "\""+re.sub("\s\s", " ", re.sub("[\\^!@#\\$%&\\*\\(\\)\\-_\\+=`~\\.\\?\\/]", " ", re.sub(",$", "", project_title_filter)).replace(",", "\" OR \""))+"\"",
                    "default_operator" : "AND"
                }
            })

        #2-2. 본문 필터 쿼리
        if len(project_content_filter)>0:
            must_not.append({
                "query_string" : {
                    "fields": ["doc_content"],
                    "query" : "\""+re.sub("\s\s", " ", re.sub("[\\^!@#\\$%&\\*\\(\\)\\-_\\+=`~\\.\\?\\/]", " ", re.sub(",$", "", project_content_filter)).replace(",", "\" OR \""))+"\"",
                    "default_operator" : "AND"
                }
            })

        #2-3.URL 필터 쿼리
        for url in project_url_filter.strip().split(","):
            if len(url)>0:
                must_not.append({
                    "match_phrase" : {
                        "doc_url" : url
                    }
                })
                
        #2-4. 패턴 일치 필터 쿼리
        if len(project_regex_filter)>0:
            must_not.append({
                "regexp" : {
                    "doc_title" : ".*("+re.sub(",", "|", project_regex_filter) + ").*"
                }
            })
            must_not.append({
                "regexp" : {
                    "doc_content" : ".*("+re.sub(",", "|", project_regex_filter) + ").*"
                }
            })
                
        return {
            "bool" : { "must_not" : must_not }
        }




    def get_dataset_query(self, project_seq, dataset_seq):
        dataset_keyword_list = mariadb.get_include_keywords(dataset_seq) # dataset 시퀀스로 dataset_keyword 조회

        keywordSetQuery = { "bool" : { 'should' : [] } }
        keywords = ""
        standardKeywords = ""
        
        for result in dataset_keyword_list:
            keywords += "," + result["keyword"].strip() if result["keyword"] and len(result["keyword"].strip()) else ""
            standardKeywords += "," + result['standard_keyword'].strip() if result['standard_keyword'] and len(result["standard_keyword"].strip()) else ""
            
        if len(keywords)>0: 
            keywords = "(" + re.sub("^,", "", keywords).replace(",", ") OR (") +")"
            keywordsQuery = {
                "query_string": {
                    "fields": ["doc_title^100", "doc_content"],
                    "query" : keywords,
                    "default_operator" : "AND",
                    "tie_breaker" : 0.0
                }
            }
            keywordSetQuery['bool']['should'].append(keywordsQuery)
            
            
        if len(standardKeywords)>0:
            standardKeywords = "(" + re.sub("^,", "", standardKeywords).replace(",", ") OR (") +")"
            standardKeywordsQuery = {
                "query_string": {
                    "fields": ["doc_title^100", "doc_content"],
                    "query" : standardKeywords,
                    "default_operator" : "AND",
                    "tie_breaker" : 0.0,
                    "analyzer" : "standard"
                }
            }
            keywordSetQuery['bool']['should'].append(standardKeywordsQuery)
        


        return keywordSetQuery




if __name__ == '__main__':
    params = {
        "start_date" : "2018-03-28T00:00:00",
        "end_date" : "2018-03-28T23:59:59",
        "project_seq" : 186,
        "compare_yn" : "N",
        "channels" : "all",
        "datasets" : "956"
    }
    queryObj = Query(params)
    
    print(queryObj.ALL_TOPICS_LIST("저축은행"))
    #print(queryObj.get_dataset_query(183, 3129))