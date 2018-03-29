'''
Created on 2017. 5. 30.

@author: Holly
'''
# -*- coding : utf-8 -*-
from com.wisenut.excel_maker import BASE_EXCEL_DIRECTORY
import logging
import sys, os
import zipfile
from com.wisenut.dao import mariadbclient as mariadb
from com.wisenut import file_util

############# logger 세팅
logger = logging.getLogger("crumbs")
logger.setLevel(logging.INFO)

# 1. 로그 포맷 세팅
formatter = logging.Formatter('[%(levelname)s][%(asctime)s] %(message)s')

# 2. 파일핸들러와 스트림핸들러 추가
file_handler = logging.FileHandler("./out.log")
stream_handler = logging.StreamHandler()

# 3. 핸들러에 포맷터 세팅
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# 4. 핸들러를 로깅에 추가
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# zip file directory
ZIP_DIRECTORY = file_util.search_create_directory(os.path.join(BASE_EXCEL_DIRECTORY, "zips"))

if __name__ == '__main__':
    logger.info("excel downloader starts.")
    
    if len(sys.argv) < 3:
        print("[ Usage ]")
        print("\texcel_downloader <zip-file-name> <sequence numbers to zip>")
        print("")
        
        exit
    
    zipfile_name = sys.argv[1]
    zip_target_seq = sys.argv[2]
    
    zip_to_download = zipfile.ZipFile(os.path.join(ZIP_DIRECTORY, zipfile_name), "w")
    logger.debug("{} created in {}".format(zipfile_name, ZIP_DIRECTORY))
    
    for seq in zip_target_seq.split("^"):
        filepath = mariadb.get_excel_path(seq);
        for folders, subfolders, files in os.walk(filepath):
            for file in files:
                zip_to_download.write(os.path.join(filepath, file), file, compress_type=zipfile.ZIP_DEFLATED)
                logger.debug("\tTarget file : {}".format(os.path.join(filepath, file)))
                
    zip_to_download.close()
    