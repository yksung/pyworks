'''
Created on 2017. 5. 22.

@author: Holly
'''
# -*- coding : utf-8 -*-
"""
analyze.py 사용 예

>>> import analyze
>>> analyze.get_verbs(0, 100, "가격할인이 좋을 수도 나쁠 수도 있음  -  730d 살때 거의 다 주고 샀지만 요즈음 차 값은 내차 중고 차 정도 임530d 는 신차 이유로 싸게 샀지만 다른 사람 중고차값이요 ㅜㅜ  뭐가 뭔지 ....")
{'좋다': '긍정2'}
{'나쁘다': '부정2'}
{'사다': ''}
{'주다': '부정2'}
{'사다': ''}
{'싸다': '부정2'}
{'사다': ''}
"""
import os, time, datetime, sys
import codecs
import xlrd, xlsxwriter
import http.client, json
import xml.etree.ElementTree as et
import json
import re
from math import ceil
from hangul_utils import split_syllables, join_jamos
from socket import *
import http, requests, urllib

fileseparator = "\\"

es_host = "211.39.140.65"
es_port = 9200

arr = []

def store_kma_result(path, file_to_append=""):
    split_pattern = re.compile("[\s\+]")

    dir = path[:path.rfind(os.sep)]
    resultfile = codecs.open(path, "r", "utf-8")
    corpus_file = None
    if(len(file_to_append)>0):
        print(">>> {0}->{1}".format(path, file_to_append))
        corpus_file = codecs.open(file_to_append, "a", "utf-8")
    else:
        print(">>> {0}->{1}".format(path, os.path.join(dir, get_current_datetime()+".txt")))
        corpus_file = codecs.open(os.path.join(dir, get_current_datetime()+".txt"), "w", "utf-8")
        
    unpretty_xml = resultfile.read()
    pretty_xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?><Result>"+unpretty_xml+"</Result>"
    
    root = et.fromstring(pretty_xml)
    for didx, child in enumerate(root):
        docid = child.findtext("DocId").replace("\r", "").replace("\n", "").replace(" ", "").strip()
        comment = child.findtext("Content").replace("\r", "").replace("\n", "").strip()
        
        try:
            json_data = json.loads("{" + child.findtext("JSON") + "}")
            if("result" in json_data and len(json_data['result'])>0 ):
                kma = json_data["result"][0]["kma"]
                for corpus in split_pattern.split(kma):
                    corpus_file.write(corpus)
                    corpus_file.write(" ")
            else: continue
        except json.decoder.JSONDecodeError as jsonError :
            print("!!!! Error Message : {0}".format(jsonError))
            print("!!!! raised at DOCID \"{0}\"".format(docid))
            pass
        except WrongDocidError as wde:
            #print(wde)
            pass
        except Exception as e:
            print("!!!! Error Message : {0}".format(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print("!!!! raised at DOCID \"{0}\"".format(docid))
            pass
        
        corpus_file.write("\n")
        
    

    resultfile.close()
    corpus_file.close()


def make_wellformed_xml(path):
    to_read = codecs.open(path, "r", "cp949")
    text = to_read.read()
    to_write = codecs.open(path+"_utf8", "w", "utf-8")
    to_write.write("<?xml version=\"1.0\" encoding=\"utf-8\"?><Result>"+text+"</Result>")
    
    to_read.close()
    to_write.close()
    

def get_json_from_xml(path):
    text = ''
    with codecs.open(path, "r", "utf-8") as output:
        text = output.read()
        output.close()
        
    p = re.compile("\.[_0-9a-zA-Z]+$") # 마지막에 나타나는 확장자를 찾는다
    o_file = p.sub(".result", path) # parameter로 받은 input 파일의 확장자를 out으로 교체.
    
    analysis_result = codecs.open(o_file, "w", "utf-8")
    
    root = et.fromstring(text)
    for didx, child in enumerate(root):
        if(didx % 1000 == 0):
            time.sleep(5)
        docid = child.findtext("DocId").replace("\r", "").replace("\n", "").replace(" ", "")
        content = child.findtext("Content").replace("\r", "").replace("\n", "")
        
        causes_per_doc = []
        try:
            
            json_data = json.loads("{" + child.findtext("JSON") + "}")
            if("result" in json_data):
                print('=================================================')
                print("DOCID : {0}".format(docid))
                print("CONTENT : {0}".format(content))
                
                for result in json_data["result"]:
                    for variable in result['variables']:
                        c = {
                            "sentence" : result['sentence']['string'],
                            "matched_text" : result['matched_text']['string'],
                            "start" : ceil(result['matched_text']['begin']/2),
                            "value" : variable['value']
                        }
                        
                        causes_per_doc.append(c)
                
                causes_per_doc = sorted(causes_per_doc, key=lambda k:k['start'] )
                for idx, cause in enumerate(causes_per_doc):
                    print("idx : {0}".format(idx))
                    if( idx != len(causes_per_doc)-1 ):
                        causes_per_doc[idx]['verbs'] = get_verbs(0, causes_per_doc[idx+1]['start']-cause['start'], cause['matched_text'])
                        for verb in causes_per_doc[idx]['verbs'].keys():
                            analysis_result.write(docid)
                            analysis_result.write('\t')
                            analysis_result.write(content)
                            analysis_result.write('\t')
                            analysis_result.write(causes_per_doc[idx]['value'])
                            analysis_result.write('\t')
                            analysis_result.write(verb)
                            analysis_result.write('\t')
                            analysis_result.write(causes_per_doc[idx]['verbs'][verb])
                            analysis_result.write("\n")
                    else:
                        causes_per_doc[idx]['verbs'] = get_verbs(0, len(cause['sentence'])-cause['start'], cause['matched_text'])
                        for verb in causes_per_doc[idx]['verbs'].keys():
                            analysis_result.write(docid)
                            analysis_result.write('\t')
                            analysis_result.write(content.strip())
                            analysis_result.write('\t')
                            analysis_result.write(causes_per_doc[idx]['value'])
                            analysis_result.write('\t')
                            analysis_result.write(verb)
                            analysis_result.write('\t')
                            analysis_result.write(causes_per_doc[idx]['verbs'][verb])
                            analysis_result.write("\n")
                print('=================================================')
            
            
        except json.decoder.JSONDecodeError as jsonError :
            print("!!!! JSONDecodeError at \'{0}\'({1})".format(docid, content))
            pass
        
    analysis_result.close()
    
def get_scd_from_xml(path):
    text = ''
    with codecs.open(path, "r", "utf-8") as output:
        text = output.read()
        output.close()
        
    p = re.compile("\.[_0-9a-zA-Z]+$") # 마지막에 나타나는 확장자를 찾는다
    o_file_path = p.sub(".SCD", path) # parameter로 받은 input 파일의 확장자를 out으로 교체.
    
    o_file = codecs.open(o_file_path, "w", "utf-8")
    
    root = et.fromstring(text)
    for didx, child in enumerate(root):
        if(didx % 1000 == 0):
            time.sleep(5)
        docid = child.findtext("DocId").replace("\r", "").replace("\n", "").replace(" ", "")
        content = child.findtext("Content").replace("\r", "").replace("\n", "")
        
        print('=================================================')
        print("DOCID : {0}".format(docid))
        print("CONTENT : {0}".format(content))
        
        for sentence in content.split("."):
            if( sentence.strip() != "" ):
                get_unit_sentence(sentence.strip())
                for rdx, e in enumerate(arr):
                    o_file.write("<DOCID>"+docid+"_"+rdx)
                    o_file.write("<DATE>20170327")
                    o_file.write("<CONTENT>"+content)
                    o_file.write("")
                    o_file.write(e['subject'])
                    o_file.write('\t')
                    o_file.write(e['verb'])
                    o_file.write('\t')
                    o_file.write(e['sentiment'])
                    o_file.write('\t')
                    o_file.write(e['category'])
                    o_file.write("\n")
                    
                    o_file.reset()
                    init_arr() # 전역변수 arr 초기화
        print('=================================================')
        
        
    o_file.close()
    
def kma2json(path):
    text = ''
    with codecs.open(path, "r", "utf-8") as output:
        text = output.read()
        output.close()
        
    p = re.compile("\.[_0-9a-zA-Z]+$") # 마지막에 나타나는 확장자를 찾는다
    o_file = p.sub(".json", path) # parameter로 받은 input 파일의 확장자를 out으로 교체.
    
    analysis_result = codecs.open(o_file, "w", "utf-8")
    
    root = et.fromstring(text)
    for didx, child in enumerate(root):
        if(didx % 1000 == 0):
            time.sleep(5)
        docid = child.findtext("DocId").replace("\r", "").replace("\n", "").replace(" ", "")
        content = child.findtext("Content").replace("\r", "").replace("\n", "")
        
        print('=================================================')
        print("DOCID : {0}".format(docid))
        print("CONTENT : {0}".format(content))
        
        for sentence in content.split("."):
            if( sentence.strip() != "" ):
                get_kma(sentence.strip())
                if(len(arr)>0):
                    for i, e in enumerate(arr):
                        result = {
                            "_index" : "emotion",
                            "_type" : "analysis",
                            "_id" : "{0}_{1}".format(docid, i),
                            "_source" : {
                                "sentence" : sentence.strip(),
                                "sentence_kma" : e['sentence_kma'],
                                "sentiment" : pos_neg(e["attributes"])
                            }
                        }
                                                    
                        analysis_result.write(str(result).replace('\'','\"'))
                        analysis_result.write('\n')
                init_arr() # 전역변수 arr 초기화
        print('=================================================')
        
        
    analysis_result.close()
    
    
def pos_neg(attr_arr):
    pos_exists = False
    neg_exists = False
    for attr in attr_arr:
        if(attr['sentiment'].strip()=='긍정'): pos_exists=True
        elif(attr['sentiment'].strip()=='부정'): neg_exists=True
        
    if(neg_exists):
        return "부정"
    elif(not neg_exists and pos_exists):
        return "긍정"
    else:
        return "중립"
        
        
def kma_groups(sentence):
    tag_pattern = re.compile("\/[A-Z]+[_]*[\+]*")

    text = sentence.replace("\r", "").replace("\n", "")
    while True:
        #sentence_kma = get_kma_result(text)
        sentence_kma = kma_from_tomcat(text)
        if(len(text)!=0 and sentence_kma!=''):
            break
        else:
            print(".", end="")
        
    groups = []
    for word in sentence_kma.split(" "):
        if(word.find("/NNG")==-1 and word.find("/XSV")!=-1):
            d = word[:word.find("/XSV")]
            groups.append(tag_pattern.sub("", d)+"다")
        elif(word.find("/NNG")!=-1):
            nng = []
            for d in word.split("+"):
                if(d.find("/NNG")!=-1):
                    nng.append(tag_pattern.sub("", d))
            groups.append("".join(nng))
        elif(word.find("/NNP")!=-1):
            nnp = []
            for d in word.split("+"):
                if(d.find("/NNP")!=-1):
                    nnp.append(tag_pattern.sub("", d))
            groups.append("".join(nnp))
        #elif(word.find("/NFG")!=-1):
        elif(word.find("/SL")!=-1):
            nfg = []
            for d in word.split("+"):
                #if(d.find("/NFG")!=-1):
                if(d.find("/SL")!=-1):
                    nfg.append(tag_pattern.sub("", d))
            groups.append("".join(nfg))
        elif(word.find("/NFU")!=-1):
            nfu = []
            for d in word.split("+"):
                if(d.find("/NFU")!=-1):
                    nfu.append(tag_pattern.sub("", d))
            groups.append("".join(nfu))
        elif(word.find("/NFG")!=-1):
            nfg = []
            for d in word.split("+"):
                if(d.find("/NFG")!=-1):
                    nfg.append(tag_pattern.sub("", d))
            groups.append("".join(nfg))
        elif(word.find("/FL")!=-1):
            fl = []
            for d in word.split("+"):
                if(d.find("/FL")!=-1):
                    fl.append(tag_pattern.sub("", d))
            groups.append("".join(fl))
        elif(word.find("/VV")!=-1):
            vv = []
            for d in word.split("+"):
                if(d.find("/VV")!=-1):
                    vv.append(tag_pattern.sub("", d))
            groups.append("".join(vv)+"다")
        elif(word.find("/VA")!=-1):
            va = []
            for d in word.split("+"):
                if(d.find("/VA")!=-1):
                    va.append(tag_pattern.sub("", d))
            groups.append("".join(va)+"다")
                    
    return groups
                
    
        
def kma2excel(path, column_length):
    dir = path[:path.rfind(fileseparator)]

    to_read = xlrd.open_workbook(path, encoding_override='UTF-8')
    to_write = xlsxwriter.Workbook(dir+fileseparator+"분석결과.xlsx")
    w_worksheet = to_write.add_worksheet()
    
    for sidx, sheet in enumerate(to_read.sheets()):
        #colnames = { sheet.cell_value(0, cidx):cidx for cidx in range(sheet.ncols) }
        #print(colnames)
        print("Total({})".format(str(sheet.nrows)), end="")
        sys.stdout.flush()
        for ridx in range(1, sheet.nrows):
            if(ridx % 100 == 0):
                print("[{}]".format(str(ridx)), end='', flush=True)
                sys.stdout.flush()
                time.sleep(5)
        
            data = []
            groups = []
            for colnum in range(column_length):
                if( colnum == 0 ): #문장
                    sentence = str(sheet.cell_value(ridx, colnum))
                    
                    w_worksheet.write(ridx, colnum, sentence)# 원본 데이터들(문장 등)
                    groups = kma_groups(sentence)
                else: # 기타 필요한 컬럼들
                    w_worksheet.write(ridx, colnum, sheet.cell_value(ridx, colnum))# 원본 데이터들(문장 등)
            
            # 원본 데이터 뒤 쪽에 화제어 리스트를 붙임
            col = column_length
            for g in groups:
                w_worksheet.write(ridx, col, g)
                col = col+1
                    
    to_write.close()
            
        
def analyze2json(path):
    print("---------------------------" + path + "---------------------------")
    workbook = xlrd.open_workbook(path, encoding_override='UTF-8')
    
    s_pattern = re.compile("[\.\-\_~\^?!]+")
    
    ext_pattern = re.compile("\.[_0-9a-zA-Z]+$") # 마지막에 나타나는 확장자를 찾는다
    o_file = ext_pattern.sub(".json", path) # parameter로 받은 input 파일의 확장자를 out으로 교체.
    
    analysis_result = codecs.open(o_file, "w", "utf-8")
    
    for sidx, sheet in enumerate(workbook.sheets()):

        colnames = { sheet.cell_value(0, cidx):cidx for cidx in range(sheet.ncols) }
        print(colnames)
        
        for ridx in range(1, sheet.nrows):
            if(ridx % 1000 == 0):
                time.sleep(5)
            
            case_id = str(sheet.cell_value(ridx, colnames['case_id']))
            response_id = str(sheet.cell_value(ridx, colnames['response_id']))
            answer_id = str(sheet.cell_value(ridx, colnames['answer_id']))
            language_id = str(sheet.cell_value(ridx, colnames['language_id']))
            content = str(sheet.cell_value(ridx, colnames['comment']))
    
            print('=================================================')
            print("case_id : {0}".format(case_id))
            print("response_id : {0}".format(response_id))
            print("answer_id : {0}".format(answer_id))
            print("language_id : {0}".format(language_id))
            print("content : {0}".format(content))
    
            for sentence in s_pattern.split(content):
                if( sentence.strip() != "" ):
                    get_kma(sentence.strip())
                    if(len(arr)>0):
                        for e in arr:
                            result = {
                                "_index" : "emotion",
                                "_type" : "analysis",
                                "_id" : "{0}_{1}_{2}_{3}".format(case_id, response_id, answer_id, language_id),
                                "_source" : {
                                    "content" : content,
                                    "sentence" : sentence.strip(),
                                    "sentence_kma" : e['sentence_kma'],
                                    "sentiment" : pos_neg(e["attributes"])
                                }
                            }
                            
                            analysis_result.write(str(result))
                            analysis_result.write('\n')
                            
                            analysis_result.reset()
                            init_arr() # 전역변수 arr 초기화
                    else:
                        result = {
                            "_index" : "emotion",
                            "_type" : "analysis",
                            "_id" : "{0}_{1}_{2}_{3}".format(case_id, response_id, answer_id, language_id),
                            "_source" : {
                                "content" : content,
                                "sentence" : sentence.strip(),
                                "sentence_kma" : '',
                                "sentiment" : ''
                            }
                        }
                        analysis_result.write(str(result))
                        analysis_result.write('\n')
                        analysis_result.reset()
            print('=================================================')
        
    analysis_result.close()
        
def analyze2csv(path):
    print("---------------------------" + path + "---------------------------")
    workbook = xlrd.open_workbook(path, encoding_override='UTF-8')
    
    s_pattern = re.compile("[\.\-\_~\^?!]+")
    
    ext_pattern = re.compile("\.[_0-9a-zA-Z]+$") # 마지막에 나타나는 확장자를 찾는다
    o_file = ext_pattern.sub(".result", path) # parameter로 받은 input 파일의 확장자를 out으로 교체.
    
    analysis_result = codecs.open(o_file, "w", "utf-8")
    
    for sidx, sheet in enumerate(workbook.sheets()):

        colnames = { sheet.cell_value(0, cidx):cidx for cidx in range(sheet.ncols) }
        print(colnames)
        
        for ridx in range(1, sheet.nrows):
            if(ridx % 1000 == 0):
                time.sleep(5)
            
            case_id = str(sheet.cell_value(ridx, colnames['case_id']))
            response_id = str(sheet.cell_value(ridx, colnames['response_id']))
            answer_id = str(sheet.cell_value(ridx, colnames['answer_id']))
            language_id = str(sheet.cell_value(ridx, colnames['language_id']))
            content = str(sheet.cell_value(ridx, colnames['comment']))
    
            print('=================================================')
            print("case_id : {0}".format(case_id))
            print("response_id : {0}".format(response_id))
            print("answer_id : {0}".format(answer_id))
            print("language_id : {0}".format(language_id))
            print("content : {0}".format(content))
    
            for sentence in s_pattern.split(content):
                if( sentence.strip() != "" ):
                    get_kma(sentence.strip())
                    if(len(arr)>0):
                        for e in arr:
                            for yoin_num, attr in enumerate(e['attributes']):
                                analysis_result.write(case_id)
                                analysis_result.write(';')
                                analysis_result.write(response_id)
                                analysis_result.write(';')
                                analysis_result.write(answer_id)
                                analysis_result.write(';')
                                analysis_result.write(language_id)
                                analysis_result.write(';')
                                analysis_result.write(content.strip())
                                analysis_result.write(';')
                                analysis_result.write(e['sentence'])
                                analysis_result.write(';')
                                analysis_result.write(str(yoin_num))
                                analysis_result.write(';')
                                analysis_result.write(attr['subject'])
                                analysis_result.write(';')
                                analysis_result.write(attr['verb'])
                                analysis_result.write(';')
                                analysis_result.write(attr['sentiment'])
                                analysis_result.write(';')
                                analysis_result.write(attr['category'])
                                analysis_result.write('\n')
                                
                                analysis_result.reset()
                                init_arr() # 전역변수 arr 초기화
                    else:
                        analysis_result.write(case_id)
                        analysis_result.write(';')
                        analysis_result.write(response_id)
                        analysis_result.write(';')
                        analysis_result.write(answer_id)
                        analysis_result.write(';')
                        analysis_result.write(language_id)
                        analysis_result.write(';')
                        analysis_result.write(content.strip())
                        analysis_result.write('\n')
            print('=================================================')
        
    analysis_result.close()
    
        
def kma2tsv(path):
    text = ''
    with codecs.open(path, "r", "utf-8") as output:
        text = output.read()
        output.close()
        
    p = re.compile("\.[_0-9a-zA-Z]+$") # 마지막에 나타나는 확장자를 찾는다
    o_file = p.sub(".result", path) # parameter로 받은 input 파일의 확장자를 out으로 교체.
    
    analysis_result = codecs.open(o_file, "w", "utf-8")
    
    root = et.fromstring(text)
    for didx, child in enumerate(root):
        if(didx % 1000 == 0):
            time.sleep(5)
        docid = child.findtext("DocId").replace("\r", "").replace("\n", "").replace(" ", "")
        content = child.findtext("Content").replace("\r", "").replace("\n", "")
        
        print('=================================================')
        print("DOCID : {0}".format(docid))
        print("CONTENT : {0}".format(content))
        
        for sentence in content.split("."):
            if( sentence.strip() != "" ):
                get_kma(sentence.strip())
                if(len(arr)>0):
                    for i, e in enumerate(arr):
                        for j, attr in enumerate(e['attributes']):
                            analysis_result.write(docid)
                            analysis_result.write('\t')
                            analysis_result.write(sentence.strip())
                            analysis_result.write('\t')
                            analysis_result.write(e['sentence_kma'])
                            analysis_result.write('\t')
                            analysis_result.write(attr['subject'])
                            analysis_result.write('\t')
                            analysis_result.write(attr['verb'])
                            analysis_result.write('\t')
                            analysis_result.write(attr['sentiment'])
                            analysis_result.write('\t')
                            analysis_result.write(attr['category'])
                            analysis_result.write('\n')
                            
                            analysis_result.reset()
                            init_arr() # 전역변수 arr 초기화
                else:
                    analysis_result.write(docid)
                    analysis_result.write('\t')
                    analysis_result.write(sentence.strip())
                    analysis_result.write('\t')
                    analysis_result.write(e['sentence_kma'])
                    analysis_result.write('\n')
        print('=================================================')
        
        
    analysis_result.close()

def init_arr():
    global arr
    arr = []
    global sentence_arr
    sentence_arr = []
    
def read_from(path):
    if(os.path.isfile(path)):
        #get_json_from_xml(path)
        #kma2tsv(path)
        #kma2json(path)
        analyze2csv(path)
    else:
        print("################ {0} is directory. ################".format(path))
        for dirname, dirnames, fileList in os.walk(path):
            for f in fileList:
                filename = os.path.join(dirname, f)
                #get_json_from_xml(filename)
                #kma2tsv(filename)
                analyze2csv(filename)
                #kma2json(filename)
                
def kma_from_tomcat(text):
    connection = http.client.HTTPConnection("localhost", 8080)
    connection.connect()
    
    param = urllib.parse.urlencode({"query" : text}, 'utf-8')
    
    connection.request("POST", "/GetKmaResult/kma_result.jsp?"+param, "", {
        "Content-Type" : "application/json"
    })
    
    return connection.getresponse().read().decode('utf-8')
                
def get_kma_result(text):
    host = '211.39.140.71'
    port = 5000
    bufsize = 10240
    addr = (host, port)
    
    clientSocket = socket(AF_INET, SOCK_STREAM)
    clientSocket.settimeout(30)
    
    del_pattern = re.compile("[^가-힣a-zA-Z0-9\s]")

    to_return = ''
    try:
        clientSocket.connect(addr)
        
        ptxt = del_pattern.sub('', text).replace('\n', '').replace('\r','').strip()
        if(len(ptxt)>0): 
            clientSocket.send(ptxt.encode())
            data = clientSocket.recv(bufsize).decode()
            
            if(type(data) is ConnectionResetError):  
                raise Exception('!!! ConnectionResetError occurred.')
                
            if(type(data) is ConnectionRefusedError):
                raise Exception('!!! ConnectionRefusedError occurred.')
                
            #if(type(data) is timeout):
            #    raise Exception('!!! Timeout occurred.')
            to_return = str(data).strip()
            
    except Exception as e:
        print("Error!!!" + str(e))
        #retry = 0
        #while retry <= 5:
        #    retry += 1
        #    print("10초 간 쉬었다가 다시!\n")
        #    time.sleep(10)
        #    
        #    try:
        #        print("분석 {0}번째 재시도..".format(retry))
        #        
        #        clientSocket.connect(addr)
        #        clientSocket.send(text.encode())
        #        data = clientSocket.recv(bufsize).decode()
        #        
        #        to_return = str(data)
        #    except Exception as e:
        #        continue
    finally:
        clientSocket.close()
    
    return to_return
    
def get_kma(text):
    subject_pattern = re.compile("[은는이가도]\/J_")
    conj_pattern = re.compile("[와과및]+\/[(J_|MA)]+")
    tag_pattern = re.compile("\/[A-Z]+[_]*[\+]*")
    comma_pattern = re.compile("[.!_\+\*\&\^\%\$#@`~\:;,，]")
    verb_pattern = re.compile("[ㄱ-ㅎ가-힣a-zA-Z]+\/[A-Z]+[_]*[\+]*")
    
    text = comma_pattern.sub(" ", text.replace("\r\n", "").strip())
    kma = get_kma_result(text)
    print("# {0} : {1}".format(text, kma))
    
    if(len(text)>0):
        if(kma.find("/EM ")!=-1): #문장을 접속어를 기준으로 쪼갠다.
            idx = kma.find("/EM ")
            
            get_kma(join_jamo(tag_pattern.sub("", kma[:idx]).strip()))
            get_kma(join_jamo(tag_pattern.sub("", kma[idx+len("/EM "):]).strip()))
        else:
            el = {
                "sentence" : text,
                "sentence_kma" : kma.replace("\r\n", ""),
                "attributes" : []
            }
            
            number_of_composition = len(subject_pattern.split(kma))
            if(number_of_composition == 2): #조사가 포함된 경우
                subject = subject_pattern.split(kma)[0]
                #for wdx, word in enumerate(subject_pattern.split(kma)[1].strip().split(" ")): # 주어 뒤에 등장하는 용언 찾기
                verb = join_jamo(tag_pattern.sub("", subject_pattern.split(kma)[1].strip()))
                for subj in conj_pattern.split(subject):
                    el["attributes"].append({
                        "subject" : join_jamo(tag_pattern.sub("", subj)).strip(),
                        "verb" : verb,
                        "sentiment" : get_emotion(verb),
                        "category" : get_category(join_jamo(tag_pattern.sub("", subj)).strip())
                    })
            elif(number_of_composition > 2): #조사가 포함된 경우
                subject = subject_pattern.split(kma)[number_of_composition-2] # 마지막에서 두번째 항목을 주어로 간주.
                verb = join_jamo(tag_pattern.sub("", subject_pattern.split(kma)[1].strip()))
                #for wdx, word in enumerate(subject_pattern.split(kma)[number_of_composition-1].strip().split(" ")): # 주어 뒤에 등장하는 용언 찾기
                for subj in conj_pattern.split(subject):
                    el["attributes"].append({
                        "subject" : join_jamo(tag_pattern.sub("", subj)).strip(),
                        "verb" : verb,
                        "sentiment" : get_emotion(verb),
                        "category" : get_category(join_jamo(tag_pattern.sub("", subj)).strip())
                    })
            else: #조사가 포함된 명사가 없는 경우
                verb = ''
                nouns = []
                # 우선 동사인지 명사인지 구분
                for word in kma.split(" "):
                    if(word.find("/VA")!=-1 or word.find("/VV")!=-1 or word.find("/XSD")!=-1 or word.find("/EM")!=-1): #동사형 어절인 경우
                        verb += " " + join_jamo(tag_pattern.sub("", word.strip()))
                    else:
                        nouns.append(join_jamo(tag_pattern.sub("", word.strip())))
                # 명사형으로 추측되는 애들을 요인으로 간주하고 요인 별로 긍부정을 체크.
                for noun in nouns:
                    el["attributes"].append({
                        "subject" : noun,
                        "verb" : verb,
                        "sentiment" : get_emotion(verb),
                        "category" : get_category(noun, False)
                    })
            arr.append(el)
    else:
        print("Text length is 0.")

"""
    current : 요인의 위치
    next : 그 다음 요인 위치
    
    진짜 어절을 다음과 같이 생각한다.
    EOJEOL에 명사형와 조사의 결합형이 등장하면 어절의 시작...
    이 어절이 다음 명사형과 조사의 결합형이 나타날 때까지는 한 요인에 대한 설명임.
    그동안은 is_eojeol 을 on 해놓고 다음 명사형이 나타나면 off.
    on인 동안의 요소는 원형 그대로 합체.
"""
def get_verbs(current, next, text):
    print(current, next)
    result = analyze(text)
    verbs = {}

    eojeol_starts_at = -1
    eojeol = []
    for idx, val in enumerate(result['tokens']):
        if(current <= val['start_offset'] < next and current <= val['end_offset']-1 < next): # and val['type'] in ['EOJEOL', 'MAG', 'XR']):
            if(val['type']=='EOJEOL' and get_kma_result(val['token']).find('/J_')>0 ):
                eojeol_starts_at = val['position']
                eojeol.append(val['token'])
                continue
            if(eojeol_starts_at != -1 and eojeol_starts_at != val['position']): # 은/는/이/가 로 끝나는 어절의 경우 이번 요인에 해당하는 말이 아니라고 가정하고 pass
                e = eojeol[len(eojeol)-1] + val['token']
                eojeol[len(eojeol)-1] = e
            else:
                if(val['type']=='VV' or val['type'] == 'VA'): # 동사나 형용사의 경우 '-다'를 붙여서 term 검색
                    v = val['token'].split('/')[0]+'다'
                    verbs[val['token']] = get_emotion(v, False)
                if(val['type']=='XR'):
                    xr = val['token'].split('/')[0]+'하다'
                    verbs[val['token']] = get_emotion(xr, False)
                if(val['type']=='NNG'):
                    verbs[val['token']] = get_emotion(val['token'])
            print(eojeol[len(eojeol)-1].strip())
                

    #for set in verbs:
    #    print(set)
    return verbs
    
    
def get_emotion(verb, match=True):
    connection = http.client.HTTPConnection(es_host, es_port)
    connection.connect()

    if(match):
        connection.request("POST", "/emotion/verb_dict/_search", json.dumps({
            "query": {
                "match": {
                  "verb" : verb
                 }
            }
        }), {
            "Content-Type" : "application/json"
        })
    else:
        connection.request("POST", "/emotion/verb_dict/_search", json.dumps({
            "query": {
                "term": {
                  "verb" : verb
                 }
            }
        }), {
            "Content-Type" : "application/json"
        })
    
    result = json.loads(connection.getresponse().read())

    return result['hits']['hits'][0]['_source']['emotion'] if result is not None and 'hits' in result and result['hits']['total'] > 0 else ''

def get_category(query, match=True):
    connection = http.client.HTTPConnection(es_host, es_port)
    connection.connect()

    if(match):
        connection.request("POST", "/emotion/category/_search", json.dumps({
            "query":{
                "multi_match": {
                  "query": query,
                  "operator": "or",
                  "type": "cross_fields",
                  "fields": [
                    "category_eng",
                    "category_kor",
                    "synonym"
                  ],
                  "tie_breaker": 0.3
                }
            }
        }), {
            "Content-Type" : "application/json"
        })
    else:
        connection.request("POST", "/emotion/category/_search", json.dumps({
            "query": {
                "bool": {
                  "should": [
                    {"term": {
                      "category_eng": {
                        "value": query
                      }
                    }},
                    {"term": {
                      "category_kor": {
                        "value": query
                      }
                    }},
                    {"term": {
                      "synonym": {
                        "value": query
                      }
                    }}
                  ]
                }
              }        
        }), {
            "Content-Type" : "application/json"
        })
    
    result = json.loads(connection.getresponse().read())
    
    category = ''
    if( result is not None and 'hits' in result and result['hits']['total'] > 0 ):
        category_eng = result['hits']['hits'][0]['_source']['category_eng']
        category_kor = result['hits']['hits'][0]['_source']['category_kor']
        subtopic_code = result['hits']['hits'][0]['_source']['subtopic_code']
        
        category = subtopic_code

    return category
    
    
def analyze(text):
    connection = http.client.HTTPConnection(es_host, es_port)
    connection.connect()

    connection.request("POST", "/dmap/_analyze?analyzer=korean", json.dumps({
        "text" : text
    }), {
        "Content-Type" : "application/json"
    })
    result = json.loads(connection.getresponse().read())

    return result
    
def join_jamo(str):
    jamo = split_syllables(str)
    chars = list(set(jamo))
    char_to_ix = { ch:i for i, ch in enumerate(chars) }
    ix_to_char = { i:ch for i, ch in enumerate(chars) }
    jamo_numbers = [ char_to_ix[x] for x in jamo ]
    restored_jamo = ''.join([ix_to_char[x] for x in jamo_numbers])
    
    return join_jamos(restored_jamo)
    
    
def get_current_datetime():
    current = str(datetime.datetime.now())
    ymdhms = str(datetime.datetime.now().strftime('%Y%m%d%H%M'))
    
    return ymdhms


if __name__ == '__main__':
    result = analyze('무상보증기간')
    
    print(result)