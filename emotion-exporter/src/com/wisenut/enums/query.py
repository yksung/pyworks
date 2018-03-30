'''
Created on 2017. 6. 22.

@author: Holly
채널 코드 관리를 위한 Enum 클래스
'''
#from enum import Enum, auto
import re
from datetime import date, timedelta
import com.wisenut.dao.mariadbclient as mariadb

#class Query(Enum):
class Query():
    params = None
    start_date = None
    end_date = None
    str_start_date = ''
    str_end_date = ''
    time_interval = None
    #SOCIAL_COMPARE_DAY=auto()
    #SOCIAL_COMPARE_EMOTIONS=auto()
    #SOCIAL_COMPARE_CHANNEL=auto()
    #
    #SOCIAL_PROCESS_DATASET=auto()
    #SOCIAL_PROCESS_CHANNEL=auto()
    #SOCIAL_PROCESS_PORTAL=auto()
    #SOCIAL_PROCESS_MEDIA=auto()
    #SOCIAL_PROCESS_COMMUNITY=auto()
    #SOCIAL_PROCESS_SNS=auto()
    #SOCIAL_PROCESS_CLUB=auto()
    #SOCIAL_PROCESS_EMOTIONS=auto()
    #SOCIAL_PROCESS_CAUSES=auto()
    #
    #SOCIAL_OCCUPATION_DATASET=auto()
    #SOCIAL_OCCUPATION_CHANNEL=auto()
    #SOCIAL_OCCUPATION_PORTAL=auto()
    #SOCIAL_OCCUPATION_MEDIA=auto()
    #SOCIAL_OCCUPATION_COMMUNITY=auto()
    #SOCIAL_OCCUPATION_SNS=auto()
    #SOCIAL_OCCUPATION_CLUB=auto()
    #SOCIAL_OCCUPATION_EMOTIONS=auto()
    #SOCIAL_OCCUPATION_CAUSES=auto()
    #
    #SOCIAL_TOPICS_LIST=auto()
    def __init__(self, params):
        self.params = params
        self.start_date = date(int(params['start_date'][0:4]), int(params['start_date'][5:7]), int(params['start_date'][8:10]))
        self.end_date = date(int(params['end_date'][0:4]), int(params['end_date'][5:7]), int(params['end_date'][8:10]))
        
        self.str_start_date = self.start_date.strftime('%Y-%m-%dT00:00:00')
        self.str_end_date = self.end_date.strftime('%Y-%m-%dT23:59:59')
        self.time_interval = self.end_date-self.start_date+timedelta(days=1)
        
    
       
    def DATASET_COUNT_PER_DAY(self, compare=False):
        if not compare:
            query =  {
              "size" : 0,
              "query" : {
                 "bool" : {
                    "must" : [
                       self.get_period_query(),
                       self.get_project_seq_query(),
                    ]
                 }
              },
              "aggs" : {
                "my_aggs1" : {
                  "date_histogram": {
                    "field": "doc_datetime",
                    "interval": "day"
                  },
                  "aggs": {
                    "my_aggs2": {
                      "filters" : { "filters" : {}}
                    }
                  }
                }
              }
            }
        else:
            query =  {
              "size" : 0,
              "query" : {
                 "bool" : {
                    "must" : [
                       self.get_period_query(),
                       self.get_project_seq_query(),
                    ]
                 }
              },
              "aggs" : {
                "my_aggs1" : {
                  "date_range" : {
                    "field" : "doc_datetime",
                    "ranges": [
                      {
                        "from": (self.start_date-self.time_interval*3).strftime('%Y-%m-%dT00:00:00'),
                        "to": (self.end_date-self.time_interval*3).strftime('%Y-%m-%dT23:59:59')
                      },
                      {
                       "from": (self.start_date-self.time_interval*2).strftime('%Y-%m-%dT00:00:00'),
                       "to": (self.end_date-self.time_interval*2).strftime('%Y-%m-%dT23:59:59')
                       },
                      {
                       "from": (self.start_date-self.time_interval*1).strftime('%Y-%m-%dT00:00:00'),
                       "to": (self.end_date-self.time_interval*1).strftime('%Y-%m-%dT23:59:59')
                       },
                      {
                       "from": self.str_start_date,
                       "to": self.str_end_date
                       }
                    ]
                  },
                  "aggs": {
                    "my_aggs2": {
                      "filters" : { "filters" : {}}
                    }
                  }
                }
              }
            }
            
        if self.get_channel_query():
            query['query']['bool']['must'].append(self.get_channel_query())
        
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
            query["aggs"]["my_aggs1"]["aggs"]["my_aggs2"]["filters"]["filters"][dataset_seq] = self.get_dataset_query(self.params['project_seq'], dataset_seq)
        
        # 2017.07.26 데이터셋 조건도 검색에 걸어줘야 날짜별 doc_count가 제한된 데이터셋 조건 내에서 합산되어 나옴.
        query['query']['bool']['must'].append({ 'bool' : {'should': should}})
        
        print("@@@@@@@@@@@@@@@@@ DATASET_COUNT_PER_DAY")
        print(query)
            
        return query
        
        
    def DATASET_OCCUPATIONS_PER_DEPTH1(self, compare=False):
        if not compare:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "filters" : {
                        "filters" : {}
                    },
                    "aggs" : {
                        "my_aggs2" : {
                            "terms" : {
                                "field" : "depth1_nm.keyword",
                                "size" : 1000
                            }
                        }
                    }
                }
              }
            }
        else:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "filters" : {
                        "filters" : {}
                    },
                    "aggs" : {
                      "my_aggs2" : {
                        "terms" : {
                            "field" : "depth1_nm.keyword",
                            "size" : 10
                        },
                        "aggs" : {
                         "my_aggs3" : {
                          "date_range" : {
                            "field" : "doc_datetime",
                            "ranges": [
                              {
                                "from": (self.start_date-self.time_interval*3).strftime('%Y-%m-%dT00:00:00'),
                                "to": (self.end_date-self.time_interval*3).strftime('%Y-%m-%dT23:59:59')
                              },
                              {
                               "from": (self.start_date-self.time_interval*2).strftime('%Y-%m-%dT00:00:00'),
                               "to": (self.end_date-self.time_interval*2).strftime('%Y-%m-%dT23:59:59')
                               },
                              {
                               "from": (self.start_date-self.time_interval*1).strftime('%Y-%m-%dT00:00:00'),
                               "to": (self.end_date-self.time_interval*1).strftime('%Y-%m-%dT23:59:59')
                               },
                              {
                               "from": self.str_start_date,
                               "to": self.str_end_date
                               }
                            ]
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
                    
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
            query["aggs"]["my_aggs1"]["filters"]["filters"][dataset_seq] = self.get_dataset_query(self.params['project_seq'], dataset_seq)
        
        # 2017.07.26 데이터셋 조건도 검색에 걸어줘야 날짜별 doc_count가 제한된 데이터셋 조건 내에서 합산되어 나옴.
        query['query']['bool']['must'].append({ 'bool' : {'should': should}})
        
        return query
    
    def DEPTH1_CHANNEL_OCCUPATIONS(self, compare=False):
        if not compare:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
             "aggs" : {
                 "my_aggs1" : {
                     "terms" : {
                        "field" : "depth1_nm.keyword",
                        "size" : 1000
                    }
                  }
              }
            }
        else:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
                "aggs" : {
                  "my_aggs1" : {
                    "terms" : {
                        "field" : "depth1_nm.keyword",
                        "size" : 10
                    },
                    "aggs" : {
                     "my_aggs2" : {
                      "date_range" : {
                        "field" : "doc_datetime",
                        "ranges": [
                          {
                            "from": (self.start_date-self.time_interval*3).strftime('%Y-%m-%dT00:00:00'),
                            "to": (self.end_date-self.time_interval*3).strftime('%Y-%m-%dT23:59:59')
                          },
                          {
                           "from": (self.start_date-self.time_interval*2).strftime('%Y-%m-%dT00:00:00'),
                           "to": (self.end_date-self.time_interval*2).strftime('%Y-%m-%dT23:59:59')
                           },
                          {
                           "from": (self.start_date-self.time_interval*1).strftime('%Y-%m-%dT00:00:00'),
                           "to": (self.end_date-self.time_interval*1).strftime('%Y-%m-%dT23:59:59')
                           },
                          {
                           "from": self.str_start_date,
                           "to": self.str_end_date
                           }
                        ]
                      }
                     }
                    }
                  }
                }
            }
        
        if self.get_channel_query():
            query['query']['bool']['must'].append(self.get_channel_query())
                    
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
        
        # 2017.07.26 데이터셋 조건도 검색에 걸어줘야 날짜별 doc_count가 제한된 데이터셋 조건 내에서 합산되어 나옴.
        query['query']['bool']['must'].append({ 'bool' : {'should': should}})
        
        return query
    
    def DEPTH2_CHANNEL_OCCUPATIONS(self, depth1_seq, compare=False):
        if not compare:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query(),
                        {
                         "term" : {
                            "depth1_seq" : depth1_seq
                         }
                        }
                    ]
                }
              },
             "aggs" : {
                 "my_aggs1" : {
                     "terms" : {
                        "field" : "depth2_nm.keyword",
                        "size" : 1000
                    }
                  }
              }
            }
        else:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query(),
                        {
                         "term" : {
                            "depth1_seq" : depth1_seq
                         }
                        }
                    ]
                }
              },
                "aggs" : {
                  "my_aggs1" : {
                    "terms" : {
                        "field" : "depth2_nm.keyword",
                        "size" : 10
                    },
                    "aggs" : {
                     "my_aggs2" : {
                      "date_range" : {
                        "field" : "doc_datetime",
                        "ranges": [
                          {
                            "from": (self.start_date-self.time_interval*3).strftime('%Y-%m-%dT00:00:00'),
                            "to": (self.end_date-self.time_interval*3).strftime('%Y-%m-%dT23:59:59')
                          },
                          {
                           "from": (self.start_date-self.time_interval*2).strftime('%Y-%m-%dT00:00:00'),
                           "to": (self.end_date-self.time_interval*2).strftime('%Y-%m-%dT23:59:59')
                           },
                          {
                           "from": (self.start_date-self.time_interval*1).strftime('%Y-%m-%dT00:00:00'),
                           "to": (self.end_date-self.time_interval*1).strftime('%Y-%m-%dT23:59:59')
                           },
                          {
                           "from": self.str_start_date,
                           "to": self.str_end_date
                           }
                        ]
                      }
                     }
                    }
                  }
                }
            }
        
        if self.get_channel_query():
            query['query']['bool']['must'].append(self.get_channel_query())
                    
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
        
        # 2017.07.26 데이터셋 조건도 검색에 걸어줘야 날짜별 doc_count가 제한된 데이터셋 조건 내에서 합산되어 나옴.
        query['query']['bool']['must'].append({ 'bool' : {'should': should}})
        
        return query
    
    def TOPICS_LIST(self):
        query = {
          "size": 0,
          "query": {
            "bool" : {
                "must" : [
                    self.get_period_query(),
                    self.get_project_seq_query(),
                    {
                     "term" : {
                       "topic_class.keyword" : "NN"
                     }
                    }
                ]
            }
          },
         "aggs" : {
           "my_aggs1" : {
             "terms" : {
                "field": "topic.keyword",
                "size" : 500
              },
              "aggs" : {
                "my_aggs2" : {
                  "terms" : {
                    "field" : "related_words.keyword",
                    "size" : 5
                  }
                }
              }
           }
         }
        }
        
        if self.get_channel_query():
            query['query']['bool']['must'].append(self.get_channel_query())
                    
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
        
        # 2017.07.26 데이터셋 조건도 검색에 걸어줘야 날짜별 doc_count가 제한된 데이터셋 조건 내에서 합산되어 나옴.
        query['query']['bool']['must'].append({ 'bool' : {'should': should}})
        
        return query
    
    
    def ALL_TOPICS_LIST(self):
        query = {
          "size": 0,
          "query": {
            "bool" : {
                "must" : [
                    self.get_period_query(),
                    self.get_project_seq_query()
                ]
            }
          },
         "aggs" : {
           "my_aggs1" : {
             "terms" : {
                "field": "topic.keyword",
                "size" : 500
              },
              "aggs" : {
                "my_aggs2" : {
                  "terms" : {
                    "field" : "related_words.keyword",
                    "size" : 5
                  }
                }
              }
           }
         }
        }
        
        if self.get_channel_query():
            query['query']['bool']['must'].append(self.get_channel_query())
                    
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
        
        # 2017.07.26 데이터셋 조건도 검색에 걸어줘야 날짜별 doc_count가 제한된 데이터셋 조건 내에서 합산되어 나옴.
        query['query']['bool']['must'].append({ 'bool' : {'should': should}})
        
        return query
    
    
    
    
    def TOPICS_VERBS_LIST(self):
        query = {
          "size": 0,
          "query": {
            "bool" : {
                "must" : [
                    self.get_period_query(),
                    self.get_project_seq_query(),
                    {
                      "term": {
                        "topic_class.keyword": "VV"
                      }
                    }
                ]
            }
          },
          "aggs": {
            "my_aggs1": {
              "terms": {
                "field" : "topic.keyword",
                "size" : 500
              }
            }
          }
        }
        
        if self.get_channel_query():
            query['query']['bool']['must'].append(self.get_channel_query())
                    
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
        
        # 2017.07.26 데이터셋 조건도 검색에 걸어줘야 날짜별 doc_count가 제한된 데이터셋 조건 내에서 합산되어 나옴.
        query['query']['bool']['must'].append({ 'bool' : {'should': should}})
        
        return query
        
    
    def BMW_SUBTOPIC_PER_MONTH(self, compare=False):
        query = {
              "size": 0,
              "aggs": {
                "monthly":{
                  "date_histogram" : { 
                    "field": "doc_datetime",
                    "interval": "month"
                  },
                  "aggs": {
                    "emotion" : { 
                      "children": {
                        "type": "emotions"
                      },
                      "aggs" : {
                        "subtopic" : {
                          "terms": {
                            "script": "doc['categories.label.keyword'].getValue().substring(2)", 
                            "size": 1000
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
        
        return query
    
    
        
    def DATASET_OCCUPATIONS_PER_DEPTH3(self, compare=False):
        if not compare:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "filters" : {
                        "filters" : {}
                    },
                    "aggs" :{
                      "my_aggs2" : {
                        "terms" : {
                            "script": "doc['depth1_nm.keyword'] + '>' + doc['depth2_nm.keyword'] + '>' + doc['depth3_nm.keyword']", 
                            "size" : 1000
                        }
                      }
                    }
                }
              }
            }
        else:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "filters" : {
                        "filters" : {}
                    },
                    "aggs" : {
                      "my_aggs2" : {
                        "terms" : {
                            "script": "doc['depth1_nm.keyword'] + '>' + doc['depth2_nm.keyword'] + '>' + doc['depth3_nm.keyword']",
                            "size" : 1000
                        },
                        "aggs" :  {
                         "my_aggs3" : {
                          "date_range" : {
                            "field" : "doc_datetime",
                            "ranges": [
                              {
                                "from": (self.start_date-self.time_interval*3).strftime('%Y-%m-%dT00:00:00'),
                                "to": (self.end_date-self.time_interval*3).strftime('%Y-%m-%dT23:59:59')
                              },
                              {
                               "from": (self.start_date-self.time_interval*2).strftime('%Y-%m-%dT00:00:00'),
                               "to": (self.end_date-self.time_interval*2).strftime('%Y-%m-%dT23:59:59')
                               },
                              {
                               "from": (self.start_date-self.time_interval*1).strftime('%Y-%m-%dT00:00:00'),
                               "to": (self.end_date-self.time_interval*1).strftime('%Y-%m-%dT23:59:59')
                               },
                              {
                               "from": self.str_start_date,
                               "to": self.str_end_date
                               }
                            ]
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
                    
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
            query["aggs"]["my_aggs1"]["filters"]["filters"][dataset_seq] = self.get_dataset_query(self.params['project_seq'], dataset_seq)
        
        # 2017.07.26 데이터셋 조건도 검색에 걸어줘야 날짜별 doc_count가 제한된 데이터셋 조건 내에서 합산되어 나옴.
        query['query']['bool']['must'].append({ 'bool' : {'should': should}})
        return query
    
    def EMOTIONS_OCCUPATIONS(self, compare=False):
        if not compare:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "terms" : {
                        "field" : "emotion_type.keyword",
                        "size" : 10
                    }
                }
              }
            }
        else:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "date_range" : {
                        "field" : "doc_datetime",
                        "ranges": [
                          {
                            "from": (self.start_date-self.time_interval*3).strftime('%Y-%m-%dT00:00:00'),
                            "to": (self.end_date-self.time_interval*3).strftime('%Y-%m-%dT23:59:59')
                          },
                          {
                           "from": (self.start_date-self.time_interval*2).strftime('%Y-%m-%dT00:00:00'),
                           "to": (self.end_date-self.time_interval*2).strftime('%Y-%m-%dT23:59:59')
                           },
                          {
                           "from": (self.start_date-self.time_interval*1).strftime('%Y-%m-%dT00:00:00'),
                           "to": (self.end_date-self.time_interval*1).strftime('%Y-%m-%dT23:59:59')
                           },
                          {
                           "from": self.str_start_date,
                           "to": self.str_end_date
                           }
                        ]
                    },
                    "aggs" : {
                        "my_aggs2" : {
                            "terms" : {
                                "field" : "emotion_type.keyword",
                                "size" : 10
                            }  
                        }
                    }
                }
              }
            }
        
        if self.get_channel_query():
            query['query']['bool']['must'].append(self.get_channel_query())
        
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
        
        query['query']['bool']['must'].append({ 'bool' : {'should': should}})
        
        return query
        
        
    def EMOTIONS_PROGRESS(self, compare=False):
        if not compare:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "terms": {
                       "field": "emotion_type.keyword",
                       "size": 10
                    },
                  "aggs": {
                    "my_aggs2":{
                        "date_histogram" : {
                            "field" : "doc_datetime",
                            "interval" : "day"
                        }
                    }
                  }
                }
              }
            }
        else:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "date_range" : {
                        "field" : "doc_datetime",
                        "ranges": [
                          {
                            "from": (self.start_date-self.time_interval*3).strftime('%Y-%m-%dT00:00:00'),
                            "to": (self.end_date-self.time_interval*3).strftime('%Y-%m-%dT23:59:59')
                          },
                          {
                           "from": (self.start_date-self.time_interval*2).strftime('%Y-%m-%dT00:00:00'),
                           "to": (self.end_date-self.time_interval*2).strftime('%Y-%m-%dT23:59:59')
                           },
                          {
                           "from": (self.start_date-self.time_interval*1).strftime('%Y-%m-%dT00:00:00'),
                           "to": (self.end_date-self.time_interval*1).strftime('%Y-%m-%dT23:59:59')
                           },
                          {
                           "from": self.str_start_date,
                           "to": self.str_end_date
                           }
                        ]
                      } ,
                  "aggs": {
                    "my_aggs2":{
                        "terms": {
                           "field": "emotion_type.keyword",
                           "size": 10
                        }
                    }
                  }
                }
              }
            }
        
        if self.get_channel_query():
            query['query']['bool']['must'].append(self.get_channel_query())
        
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
        
        query['query']['bool']['must'].append({ 'bool' : {'should': should}})
        
        return query
        
    def EMOTIONS_PER_DEPTH1(self, compare=False):
        if not compare:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "terms" : {
                        "field" : "depth1_seq",
                        "size" : 10
                    },
                  "aggs": {
                    "my_aggs2":{
                        "terms": {
                           "field": "emotion_type.keyword",
                           "size": 10
                        }
                    }
                  }
                }
              }
            }
        else:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "date_range" : {
                        "field" : "doc_datetime",
                        "ranges": [
                          {
                            "from": (self.start_date-self.time_interval*3).strftime('%Y-%m-%dT00:00:00'),
                            "to": (self.end_date-self.time_interval*3).strftime('%Y-%m-%dT23:59:59')
                          },
                          {
                           "from": (self.start_date-self.time_interval*2).strftime('%Y-%m-%dT00:00:00'),
                           "to": (self.end_date-self.time_interval*2).strftime('%Y-%m-%dT23:59:59')
                           },
                          {
                           "from": (self.start_date-self.time_interval*1).strftime('%Y-%m-%dT00:00:00'),
                           "to": (self.end_date-self.time_interval*1).strftime('%Y-%m-%dT23:59:59')
                           },
                          {
                           "from": self.str_start_date,
                           "to": self.str_end_date
                           }
                        ]
                      },
                    "aggs" : {
                        "my_aggs2" : {
                          "terms" : {
                            "field" : "depth1_seq",
                            "size" : 100
                          },
                          "aggs": {
                            "my_aggs3":{
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
    
        if self.get_channel_query():
            query['query']['bool']['must'].append(self.get_channel_query())
        
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
        
        query['query']['bool']['must'].append({ 'bool' : {'should': should}})
        
        return query
        
        
    def EMOTIONS_PER_CAUSES(self, compare=False):
        if not compare:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "terms" : {
                        "script": "doc['depth1_nm.keyword'] + '>' + doc['depth2_nm.keyword'] + '>' + doc['depth3_nm.keyword']",
                        "size" : 1000
                    },
                  "aggs": {
                    "my_aggs2":{
                        "terms": {
                           "field": "conceptlevel1.keyword",
                           "size": 1000
                        },
                        "aggs": {
                            "my_aggs3":{
                                "terms": {
                                   "field": "conceptlevel2.keyword",
                                   "size": 1000
                                },
                                "aggs": {
                                    "my_aggs4":{
                                        "terms": {
                                           "field": "conceptlevel3.keyword",
                                           "size": 1000
                                        },
                                        "aggs": {
                                            "my_aggs5":{
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
        else:
            query = {
              "size": 0,
              "query": {
                "bool" : {
                    "must" : [
                        self.get_period_query(),
                        self.get_project_seq_query()
                    ]
                }
              },
              "aggs": {
                "my_aggs1" :{
                    "terms" : {
                        "script": "doc['depth1_nm.keyword'] + '>' + doc['depth2_nm.keyword'] + '>' + doc['depth3_nm.keyword']",
                        "size" : 1000
                    },
                  "aggs": {
                    "my_aggs2":{
                        "terms": {
                           "field": "conceptlevel1.keyword",
                           "size": 1000
                        },
                        "aggs": {
                            "my_aggs3":{
                                "terms": {
                                   "field": "conceptlevel2.keyword",
                                   "size": 1000
                                },
                                "aggs": {
                                    "my_aggs4":{
                                        "terms": {
                                           "field": "conceptlevel3.keyword",
                                           "size": 1000
                                        },
                                        "aggs": {
                                            "my_aggs5":{
                                                "terms": {
                                                   "field": "emotion_type.keyword",
                                                   "size": 10
                                                },
                                                "aggs": {
                                                    "my_aggs6":{
                                                        "date_range" : {
                                                        "field" : "doc_datetime",
                                                        "ranges": [
                                                          {
                                                            "from": (self.start_date-self.time_interval*3).strftime('%Y-%m-%dT00:00:00'),
                                                            "to": (self.end_date-self.time_interval*3).strftime('%Y-%m-%dT23:59:59')
                                                          },
                                                          {
                                                           "from": (self.start_date-self.time_interval*2).strftime('%Y-%m-%dT00:00:00'),
                                                           "to": (self.end_date-self.time_interval*2).strftime('%Y-%m-%dT23:59:59')
                                                           },
                                                          {
                                                           "from": (self.start_date-self.time_interval*1).strftime('%Y-%m-%dT00:00:00'),
                                                           "to": (self.end_date-self.time_interval*1).strftime('%Y-%m-%dT23:59:59')
                                                           },
                                                          {
                                                           "from": self.str_start_date,
                                                           "to": self.str_end_date
                                                           }
                                                        ]
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
        
        should = []        
        for dataset_seq in self.params['datasets'].split("^"):
            should.append(self.get_dataset_query(self.params['project_seq'], dataset_seq))
        
        query['query']['bool']['must'].append({'bool' : {'should': should}})
        
        
        return query
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
    def get_period_query_by_upd_dt(self):
        str_start_date = self.params['start_date'] if self.params["start_date"] else "1900-01-01T00:00:00"
        str_end_date = self.params['end_date'] if self.params["end_date"] else "2100-12-31T23:59:59"
        if self.params['compare_yn']=='Y':
            # 기준날짜
            start_date = date(int(self.params['start_date'][0:4]), int(self.params['start_date'][5:7]), int(self.params['start_date'][8:10]))
            end_date = date(int(self.params['end_date'][0:4]), int(self.params['end_date'][5:7]), int(self.params['end_date'][8:10]))
            time_interval = end_date-start_date
            # 비교 날짜들(1time_interval before)
            this_end_date = end_date - (time_interval+timedelta(days=1))*3 # 곱해진 간격만큼 이전 날짜를 구함
            
            str_start_date = (this_end_date-time_interval).strftime('%Y-%m-%dT00:00:00')
            
            
        return {
            "range": {
                "upd_datetime": {
                    "gte" : str_start_date,
                    "lte" : str_end_date
                } 
            }
        }
        
        
        
        
        
        
    
    def get_period_query(self):
        str_start_date = self.params['start_date'] if self.params["start_date"] else "1900-01-01T00:00:00"
        str_end_date = self.params['end_date'] if self.params["end_date"] else "2100-12-31T23:59:59"
        if self.params['compare_yn']=='Y':
            # 기준날짜
            start_date = date(int(self.params['start_date'][0:4]), int(self.params['start_date'][5:7]), int(self.params['start_date'][8:10]))
            end_date = date(int(self.params['end_date'][0:4]), int(self.params['end_date'][5:7]), int(self.params['end_date'][8:10]))
            time_interval = end_date-start_date
            # 비교 날짜들(1time_interval before)
            this_end_date = end_date - (time_interval+timedelta(days=1))*3 # 곱해진 간격만큼 이전 날짜를 구함
            
            str_start_date = (this_end_date-time_interval).strftime('%Y-%m-%dT00:00:00')
            
            
        return {
            "range": {
                "doc_datetime": {
                    "gte" : str_start_date,
                    "lte" : str_end_date
                } 
            }
        }
        
    def get_project_seq_query(self):
        return {
            "terms" : {
                "project_seq" : [ self.params['project_seq'] ] 
            }
        }
        
    def get_channel_query(self):
        channel = self.params["channels"] # 1^5,6,7,8;2^11,12,13,14
        
        if not channel or channel == "all":
            return None
        else:
            query = ''
            for c in re.sub(";$", "", channel).split(";"):
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
        
        must_not = []
        
        if len(project_title_filter)>0:
            must_not.append({
                "query_string" : {
                    "fields": ["doc_title"],
                    "query" : "\""+re.sub("\s\s", " ", re.sub("[\\^!@#\\$%&\\*\\(\\)\\-_\\+=`~\\.\\?\\/]", " ", re.sub(",$", "", project_title_filter)).replace(",", "\" OR \""))+"\"",
                    "default_operator" : "AND"
                }
            })

            #2-3. 본문 필터 쿼리
        if len(project_content_filter)>0:
            must_not.append({
                "query_string" : {
                    "fields": ["doc_content"],
                    "query" : "\""+re.sub("\s\s", " ", re.sub("[\\^!@#\\$%&\\*\\(\\)\\-_\\+=`~\\.\\?\\/]", " ", re.sub(",$", "", project_content_filter)).replace(",", "\" OR \""))+"\"",
                    "default_operator" : "AND"
                }
            })

        #2-4.URL 필터 쿼리
        for url in project_url_filter.strip().split(","):
            if len(url)>0:
                must_not.append({
                    "match_phrase" : {
                        "doc_url" : url
                    }
                })
                
        return {
            "bool" : { "must_not" : must_not }
        }
            
            
            
    def get_dataset_query_kdic(self, project_seq, dataset_seq):
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
                    "fields": ["doc_title^5", "doc_content"],
                    "query" : keywords,
                    "default_operator" : "AND",
                    "tie_breaker" : 1.0
                }
            }
            keywordSetQuery['bool']['should'].append(keywordsQuery)
            
            
        if len(standardKeywords)>0:
            standardKeywords = "(" + re.sub("^,", "", standardKeywords).replace(",", ") OR (") +")"
            standardKeywordsQuery = {
                "query_string": {
                    "fields": ["doc_title^5", "doc_content"],
                    "query" : standardKeywords,
                    "default_operator" : "AND",
                    "tie_breaker" : 1.0,
                    "analyzer" : "standard"
                }
            }
            keywordSetQuery['bool']['should'].append(standardKeywordsQuery)
        


        return keywordSetQuery
            
            
    
    def get_dataset_query(self, project_seq, dataset_seq):
        dataset_keyword_list = mariadb.get_include_keywords(dataset_seq) # dataset 시퀀스로 dataset_keyword 조회
        project_filter_keywords = mariadb.get_project_filter_keywords(project_seq)
        
        project_title_filter = project_filter_keywords['title_filter_keywords'].strip() if project_filter_keywords and 'title_filter_keywords' in project_filter_keywords and project_filter_keywords['title_filter_keywords']else ''
        project_content_filter = project_filter_keywords['content_filter_keywords'].strip() if project_filter_keywords and 'content_filter_keywords' in project_filter_keywords and project_filter_keywords['content_filter_keywords']else ''
        project_url_filter = project_filter_keywords['filter_urls'].strip() if project_filter_keywords and 'filter_urls' in project_filter_keywords and project_filter_keywords['filter_urls'] else ''
        # 2017.11.23 추가
        project_standard_title_filter = project_filter_keywords['standard_title_filter_keywords'].strip() if project_filter_keywords and 'standard_title_filter_keywords' in project_filter_keywords and project_filter_keywords['standard_title_filter_keywords'] else ''
        project_standard_content_filter = project_filter_keywords['standard_content_filter_keywords'].strip() if project_filter_keywords and 'standard_content_filter_keywords' in project_filter_keywords and project_filter_keywords['standard_content_filter_keywords'] else ''
        
        keyword_sets_should = []
        for result in dataset_keyword_list:
            this_must = {}
            this_must_not = []
            
            keyword = result["keyword"].strip() if result["keyword"] else ""
            subkeywords = result["sub_keywords"].strip() if result["sub_keywords"] else ""
            title_filter_keywords = result['title_filter_keywords'].strip() if result['title_filter_keywords'] else ""
            content_filter_keywords = result['content_filter_keywords'].strip() if result['content_filter_keywords'] else ""
            url_filter = result['filter_urls'].strip() if result['filter_urls'] else ""
            # 2017.11.23 추가
            standard_keyword = result['standard_keyword'].strip() if result['standard_keyword'] else ""
            standard_title_filter_keywords = result['standard_title_filter_keywords'].strip() if result['standard_title_filter_keywords'] else ""
            standard_content_filter_keywords = result['standard_content_filter_keywords'].strip() if result['standard_content_filter_keywords'] else ""
            
            #1.키워드 세팅
            #1-1. 키워드 세팅
            if len(subkeywords)>0:
                keyword += "," + subkeywords
                
            keyword = keyword.replace(",", " ")
            
            #1-2. 제목 필터 키워드 
            if len(title_filter_keywords)>0:
                title_filter_keywords += "," + project_title_filter
            else:
                title_filter_keywords += project_title_filter
                    
            #1-3. 본문 필터 키워드
            if len(content_filter_keywords)>0:
                content_filter_keywords += "," + project_content_filter
            else:
                content_filter_keywords+= project_content_filter
                    
            #1-4. URL 필터
            if len(url_filter)>0:
                url_filter += "," + project_url_filter
            else:
                url_filter += project_url_filter
                
            #1-5. standard 키워드 세팅(형태소 분석X)
            standard_keyword = standard_keyword.replace(",", " ")
            
            #1-6. 제목 필터 키워드(형태소 분석X) 
            if len(standard_title_filter_keywords)>0:
                standard_title_filter_keywords += "," + project_standard_title_filter
            else:
                standard_title_filter_keywords += project_standard_title_filter
                
            #1-7. 내용 필터 키워드(형태소 분석X) 
            if len(standard_content_filter_keywords)>0:
                standard_content_filter_keywords += "," + project_standard_content_filter
            else:
                standard_content_filter_keywords += project_standard_content_filter
                
            #2.쿼리 세팅
            #2-1. 키워드 쿼리
            if len(keyword.strip())>0:
                this_must = {
                    "query_string": {
                        "fields": ["doc_title", "doc_content"],
                        "query" : keyword, # (신라면 농심 nongshim) OR (辛라면 농심) OR (푸라면 놈심)
                        "default_operator" : "AND"
                    }
                }        
            
            #2-2. 제목 필터 쿼리
            if len(title_filter_keywords)>0:
                this_must_not.append({
                    "query_string" : {
                        "fields": ["doc_title"],
                        "query" : "\""+re.sub("\s\s", " ", re.sub("[\\^!@#\\$%&\\*\\(\\)\\-_\\+=`~\\.\\?\\/]", " ", re.sub(",$", "", title_filter_keywords)).replace(",", "\" OR \""))+"\"",
                        "default_operator" : "AND"
                    }
                })
            
            #2-3. 본문 필터 쿼리
            if len(content_filter_keywords)>0:
                this_must_not.append({
                    "query_string" : {
                        "fields": ["doc_content"],
                        "query" : "\""+re.sub("\s\s", " ", re.sub("[\\^!@#\\$%&\\*\\(\\)\\-_\\+=`~\\.\\?\\/]", " ", re.sub(",$", "", content_filter_keywords)).replace(",", "\" OR \""))+"\"",
                        "default_operator" : "AND"
                    }
                })
                
            #2-4.URL 필터 쿼리
            bool_should = []
            for url in url_filter.split(","):
                if len(url)>0:
                    bool_should.append({
                        "match_phrase" : {
                            "doc_url" : url
                        }
                    })
                    
            #2-5. 키워드 쿼리(형태소분석X)
            if len(standard_keyword.strip())>0:
                this_must = {
                    "query_string": {
                        "fields": ["doc_title", "doc_content"],
                        "query" : standard_keyword, # (신라면 농심 nongshim) OR (辛라면 농심) OR (푸라면 놈심)
                        "default_operator" : "AND",
                        "analyzer" : "standard"
                    }
                }   
                    
            #2-6. 제목  필터 쿼리(형태소분석 X)
            if len(standard_title_filter_keywords)>0:
                this_must_not.append({
                    "query_string" : {
                        "fields": ["doc_title"],
                        "query" : "\""+re.sub("\s\s", " ", re.sub("[\\^!@#\\$%&\\*\\(\\)\\-_\\+=`~\\.\\?\\/]", " ", re.sub(",$", "", standard_title_filter_keywords)).replace(",", "\" OR \""))+"\"",
                        "default_operator" : "AND",
                        "analyzer" : "standard"
                    }
                })
                
            #2-7. 내용  필터 쿼리(형태소분석 X)
            if len(standard_content_filter_keywords)>0:
                this_must_not.append({
                    "query_string" : {
                        "fields": ["doc_content"],
                        "query" : "\""+re.sub("\s\s", " ", re.sub("[\\^!@#\\$%&\\*\\(\\)\\-_\\+=`~\\.\\?\\/]", " ", re.sub(",$", "", standard_content_filter_keywords)).replace(",", "\" OR \""))+"\"",
                        "default_operator" : "AND",
                        "analyzer" : "standard"
                    }
                })
                    
            if len(bool_should)>0:
                this_must_not.append({
                    "bool" : {
                        "should" : bool_should
                    }
                })
            
            keyword_sets_should.append({
                "bool" : {
                    "must" : this_must,
                    "must_not" : this_must_not
                }
            })
            
        return {
            "bool" : {
                "should" : keyword_sets_should
            }
        }
        
        
        
    
    def get_documents_query(self):
        query = {
           "query" : {
               "bool" : {
                    "must" : []
                }
            }
        }
        
        must = []
        # 프로젝트 시퀀스 포함
        must.append(self.get_project_seq_query())
    
        # 대상 채널
        if "channels" in self.params and self.params["channels"] and self.params["channels"] != 'all':
            must.append(self.get_channel_query())
    
        # 원문의 대상 기간
        if "start_date" in self.params or "end_date" in self.params:
            must.append(self.get_period_query())
             
        # 데이터셋의 포함 키워드
        if "datasets" in self.params and self.params["datasets"]: # 신라면,삼양라면,안성탕면
            if len(self.params["datasets"].split("^"))>1:
                should = []
                for dataset in self.params["datasets"].split("^"):
                    should.append(self.get_dataset_query(self.params['project_seq'], dataset))
    
                must.append({
                    "bool" : {
                        "should" : should
                    }
                })
            else:
                must.append(self.get_dataset_query(self.params['project_seq'], self.params["datasets"]))
    
    
        # elif params["type_cd"] == "CCT002": # 소셜모니터링-문서통계
        # elif params["type_cd"] == "CCT003": # 소셜모니터링-감성분석
        # .....
        # 코드별로 request 필요한 형태로 변경해서 추가
    
        if "query" in query:
            query["query"]["bool"]["must"] = must
            
            
        return query