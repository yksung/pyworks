# -*- coding: utf-8 -*- 
'''
Created on 2017. 6. 13.

@author: Holly
'''
from socket import *
import codecs
from com.wisenut.config import Config
import logging.config
from _elementtree import ParseError
import time

############# logger 세팅
conf = Config()
logging.config.fileConfig(conf.get_logconfig_path())
logger = logging.getLogger(__name__)

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
    params['_source']['doc_title'] = "과천 수학과외 (갈현동,별양동,원문동) 영어과외, 방문과외 성적향상의 지름길 !"
    params['_source']['doc_content'] = "교육정보 과천 수학과외 (갈현동,별양동,원문동) 영어과외, 방문과외 성적향상의 지름길 ! 2017.07.01. 13:23 URL 복사 본문 기타 기능 번역보기 http://blog.naver.com/sweetbaby33/221041665835 과천 수학과외 (갈현동,별양동,원문동) 영어과외, 방문과외 성적향상의 지름길 ! 과천 방문과외 (갈현동,별양동,원문동) 초등, 중등, 고등, 과외비 수학과외, 영어과외, 국어과외, 과학과외 전문과외, 과외선생님 추천! 과천 방문과외 일대일 전문과외 선생님이에요~ 초등학생, 중학생, 고등학생, 대학생, 성인까지 수업을 합니다~! 많은 학생들이 같은 공간, 한 명의 선생님에게 강의식 수업을 받아 일대일로 정확한 진단 약점보완이 힘든 수학학원, 영어학원, 공부방만 찾고 계신가요? 일대일 테스트, 심층 상담, 개별커리큘럼으로 학습진단, 약점체크 보완까지 해 줄 수 있는 일대일 방문과외입니다~ 과천 방문과외 갈현동, 별양동, 원문동 일대일 개인과외를 하고 있어요~ (갈현동, 별양동, 원문동 외 다른동 수업가능 합니다) 갈현동과외, 별양동과외, 원문동과외는 과외수업만을 전문적으로 하고 있는 남자선생님, 여자선생님이 수업을 진행합니다~^^ 과천 방문과외는 중간고사, 기말고사 대비를 철저하게 해서 내신성적 관리는 물론 부족한 부분과 약점을 보완하는 수업과 선행학습을 합니다. 수학과외, 영어과외, 국어과외, 과학과외는 물론 일본어, 중국어, 독일어, 프랑스어 회화 어학자격증 대비 수업도 하고 있어요~ 공무원시험, 검정고시 대비, 고등입시, 대학입시, 수시, 정시, 논술, 적성고사, 진로, 입시컨설팅도 갈현동, 별양동, 원문동 과외 선생님에게 연락주세요~^^ 과천방문국어과외는 문학,작문,비문학,문법,고전의 체계적인 장리와 개별맞춤 커리큘럼으로 어휘력, 문장이해력까지 올려줍니다~ 국어 수업은 국어성적 상승은 물론 다른 과목 성적도 조금씩 같이 오르는 효과가 있습니다~ 논술, 독서논술, 한글, 한자, 한문 과외수업도 갈현동국어과외, 별양동국어과외 과천방문국어과외 선생님에게 문의주세요~^^ 영어내신, 수능영어, 영어회화, 영어듣기 수업도 과천방문영어과외 선생님이 잘합니다~   초등학생 초등영어부터 중학생 중등영어, 고등학생 고등영어 편입생의 편입영어, 성인영어회화까지!   기초영어, 파닉스, 단어암기, 영어문법, 독해, 작문수업과 영어면접 수업도 하고 있어요~ 수학과외비,영어과외비,과학과외비,초등과외비,중등과외비,고등과외비,대학생과외비로 고민이 많다면 부담없이 상담과 1회 무료수업을 받아보고 수업을 결정해보세요~^^ 우리 과천지역 학생들을 개별 커리큘럼, 학습플래너 구축후 오답노트를 적극 활용하여 우리 아이들에게 공부의 흥미를 갖게 해주는 효과가 있습니다.   혼자서 스스로 공부하는것도 어렵지 않아요. 수업 중에는 전화를 받을 수 없어요~ㅠㅠ 전화연결이 안되면 문자를 남겨주세요! 수업이 끝난 뒤 바로 연락드리겠습니다^^   과천 방문과외(갈현동, 별양동, 원문동) 초등학생, 중학생, 고등학생 수학과외,영어과외,국어과외,과학과외, 과외수업, 일대일 전문과외, 방문과외, 과외선생님 추천!"
    
    request(params)