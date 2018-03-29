# -*- coding: utf-8 -*- 
'''
Created on 2017. 6. 13.

@author: Holly
'''
from socket import *
from com.wisenut.config import Config
import logging
import time
from com.wisenut import myLogger

############# logger 세팅
logger = myLogger.getMyLogger('teaclient', hasConsoleHandler=False, hasRotatingFileHandler=True, logLevel=logging.DEBUG)

tea_host = "211.39.140.71"
tea_port = 11000
ITEM_DELIMITER = "^"
WEIGHT_DELIMITER = ":"

def request(params, timeout=2):
    
    #1. send data
    content = "<![CDATA[<DOCID>\n<TITLE>"+params['_source']['doc_title'].strip()+"\n<CONTENT>"+params['_source']['doc_content'].strip()+"\n<TERMS>\n<TOPIC>]]>"

    body_head = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><request><command>extractor</command><request_type>realtime</request_type><request_id>900000</request_id><params><param name=\"collection_id\">dmap_data</param><param name=\"content\">"
    body_tail = "</param><param name=\"item_delimiter\">"+ITEM_DELIMITER+"</param><param name=\"weight_delimiter\">"+WEIGHT_DELIMITER+"</param></params></request>"
    
    body_length = len(body_head) + len( content.encode() ) + len(body_tail)
    
    """
    12       544<?xml version="1.0" encoding="utf-8" ?>
    <request>
            <command>extractor</command>
            <request_type>realtime</request_type>
            <request_id>900000</request_id>
            <params>
                    <param name="collection_id">sample_terms</param>
                    <param name="content"><![CDATA[<DOCID>
    <TITLE>
    <CONTENT>
    <TERMS>
    <TOPIC>]]></param>
                    <param name="item_delimiter">|</param>
                    <param name="weight_delimiter">,</param>
            </params>
    </request>
    """
    query = "12%10d%s%s%s" % ( body_length, body_head, content, body_tail )
    
    try:
        client = socket( AF_INET, SOCK_STREAM )
        client.connect( (tea_host, tea_port) )
        client.send( bytes(query.encode('utf-8')) )
    except socket.error:
        print("[teaclient>request] Send failed. %s " % str(socket.error))
        return ''
    
    #2. receive data
    total_data = []
    data = ''
    
    begin = time.time()
    while 1 :
        #2-1. if all parts of data got received,
        if "</response>" in "".join(total_data):
            break
        
        if total_data and time.time()-begin>timeout:
            break 
        
        elif time.time()-begin > timeout*2:
            break
            
        try:
            data = client.recv(8192)
            """ Response will be like this....
            455<?xml version="1.0" encoding="utf-8" ?>
            <response>
                    <command>extractor</command>
                    <request_type>realtime</request_type>
                    <request_id></request_id>
                    <results>
                            <result name="status">success</result>
                            <result name="keywords"><![CDATA[<DOCID>
                                <TITLE>
                                <CONTENT>
                                <TERMS>
                                <VERB>
                                <TERMS2>
                                <TERMS3>
                                <TOPIC>
                            ]]></result>
                    </results>
            </response>
            """
            if data:
                total_data.append(data.decode("utf-8", "replace"))
                
                begin=time.time()
            else:
                time.sleep(0.1)
        
        except UnicodeDecodeError as decode_error:
            logger.error("[teaclient>request] data decode failed. (%s)"%str(decode_error))
            logger.error(data)
            pass
        except:
            logger.error("[teaclient>request] unknown exception.")
            logger.error(data)
            pass
        
    
    #print("".join(total_data)[10:])
    return "".join(total_data)[10:]



if __name__ == '__main__':
    params = {
              "_source" : {}
              }
    params['_source']['doc_title'] = "오케이저축은행대출"
    params['_source']['doc_content'] = "오케이저축은행에서 대출 1000만받았는데 400만남았어요 매월 3일마다 25만 내야하는데 지금 16일인데 못냇어요 다음달 3일에 두개를 같이내야 하는방법밖에 없는데 저는 어떡케되나요? 이런적은 첨이에요 최고오래연체됫던게 7일이에요  여신금융협회에 등록된 정식대출상담사입니다.     연체기록이 반영되실뿐더러, 추심이 들어오실수있습니다. 결과적으로 연체를 하게되시는 사항이시고, 등급이 하락되시고 연체기록이 남게됩니다 ㅠㅠ              진행여부 확인 및 추가 상담을 원하시면, 하단에 네임카드or 1:1질문을 활용하시면 1:1맞춤 상담이 가능합니다.   정식등록된 상담사인지 확인하시어, 금융피해가 없도록 주의바랍니다."
    
    print ( request(params) )