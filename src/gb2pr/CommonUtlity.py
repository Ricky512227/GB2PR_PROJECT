import zipfile
import sys
from .logMonitor import logger
import gzip
import time
import shutil
import os
import json
import subprocess as sp
from datetime import datetime

def isvalidDate(test_str, format):
    try:
        logger.info("RECEIVED date from the systemArguments :: {0}".format(test_str))
        res = True
        try:
            res = bool(datetime.strptime(test_str, format))
            logger.info("Valid Date Format received [YYYY-MM-DD]:: {0}".format(test_str))
        except ValueError:
            res = False
            logger.error("InValid Date Format received [YYYY-MM-DD] :: {0}".format(test_str))
        return res
    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))

def executeCmd(cmdtoexecute):
    try:
        logger.info("Command to execute ==>"+str(cmdtoexecute))
        proc = sp.Popen(cmdtoexecute, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        stdout, stderr = proc.communicate()
        returnCode = proc.returncode
        # Decode bytes to string when needed
        output = None
        if stdout:
            try:
                output = stdout.decode('utf-8', errors='ignore')
            except AttributeError:
                output = str(stdout)
        elif stderr:
            try:
                output = stderr.decode('utf-8', errors='ignore')
            except AttributeError:
                output = str(stderr)
        return returnCode, output
    except Exception as ex:
        logger.error("FAILED to execute cmd ==> " + str(cmdtoexecute))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        return 1, None

def extractZipFile(sourceZipFilePath, targetDirectoryToExtractZipFile):
    try:
        if zipfile.is_zipfile(sourceZipFilePath):
            try:
                with zipfile.ZipFile(sourceZipFilePath, mode = 'r', allowZip64 = True) as file:
                    print('File size is compatible :: Extracting all files...[STARTED]')
                    file.extractall(targetDirectoryToExtractZipFile)
                    print('Extracting all files...[COMPLETED]')
                    return file.namelist()
            except zipfile.LargeZipFile:
                print('Error: File size if too large')
                logger.error('Error: File size if too large')
                return []
        else:
            print('Not a zip File :: {0}'.format(sourceZipFilePath))
            logger.error('Not a zip File :: {0}'.format(sourceZipFilePath))
            return []
    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))


def untarGZfile(source_filepath):
    try:
        logger.info("RECEIVED .GZ File Name :: {0}".format(source_filepath))
        head ,tail = os.path.split(source_filepath)
        dest_filepath = os.path.join(head,".".join(tail.split(".")[:-1]))
        try:
            with gzip.open(source_filepath, 'rb') as infile:
                with open(dest_filepath, 'wb') as outfile:
                    for lIndex, line in enumerate(infile):
                        if lIndex!=0:
                            outfile.write(line)
                logger.info("PROCESSED  .GZ File Name :: SUCCESS")
                time.sleep(5)
                return True
        except:
            logger.info("PROCESSED  .GZ File Name :: FAILED")
            return False
    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))


# Process result
def readJsonFile(tempJsonFilePath):
    try:
        logger.info("RECEIVED Json Message File Name :: {0}".format(tempJsonFilePath))
        with open(tempJsonFilePath) as f:
            data = json.load(f)
            return data
    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))

def getS3UrlsFromJsonData(tempMessageData):
    try:
        logger.info("RECEIVED Json Data :: {0}".format(tempMessageData))
        if not tempMessageData:
            return None
        messageName = tempMessageData.get('Message')
        if messageName != 'BKFS':
            return None
        datasets = tempMessageData.get('datasets') or []
        if not isinstance(datasets, (list, tuple)) or len(datasets) == 0:
            return None
        # Try to get the first dataset's datasetPrefix value safely
        first = datasets[0]
        if not isinstance(first, dict):
            return None
        prefix = first.get('datasetPrefix')
        if isinstance(prefix, (list, tuple)) and prefix:
            return prefix[0]
        return prefix
    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
