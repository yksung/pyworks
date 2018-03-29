'''
Created on 2017. 5. 30.

@author: Holly
'''
# -*- coding : utf-8 -*-
import logging
import sys, os
import zipfile
from com.wisenut.utils import file_util
from com.wisenut.config import Config
from com.wisenut import myLogger

############# setting logging
logger = myLogger.getMyLogger('excel_downloader', True, False, logging.DEBUG)


# zip file directory
conf = Config()
BASE_EXCEL_DIRECTORY=conf.get_report_home()

if __name__ == '__main__':
    logger.info("excel downloader starts.")

    if len(sys.argv) < 4:
        print("[ Usage ]")
        print("\texcel_downloader <target_file_path> <save_file_path> <save_file_name>")
        print("")

        exit

    print(sys.argv);

    #1. 압축할 대상 확정
    #1-1. 압축할 대상 디렉토리
    targetfilepath = sys.argv[1]
    print("target path : " + targetfilepath)

    #1-2. 압축할 대상 파일명
    filepath = sys.argv[2]
    file_util.search_create_directory(filepath)
    print("save path : " + filepath)

    #1-3. 압축할 대상 파일명
    filename = sys.argv[3]
    print("save name : " + filename)

    zip_to_download = zipfile.ZipFile(os.path.join(filepath, filename), "w")
    logger.debug("[excel_downloader] {} created in {}".format(filepath, filename))

    #2. 압축할 대상 파일들.
    for folders, subfolders, files in os.walk(targetfilepath):
        for file in files:
            # xlsx 파일의 중간 디렉토리를 포함하여 압축
            if file.endswith(".xlsx") or file.endswith(".file"):
                #print('execute file name : ' + file)
                abs_path = os.path.join(folders, file)

                print("abs_path : " + abs_path)
                print("rel_path : " + os.path.relpath(abs_path, filepath))

                zip_to_download.write(abs_path, os.path.relpath(abs_path, targetfilepath), compress_type=zipfile.ZIP_DEFLATED)
                logger.debug("\tTarget file : {}".format(os.path.join(folders, file)))

    zip_to_download.close()
