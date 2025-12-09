from .logMonitor import logger
import sys
import os
from datetime import datetime
import glob

#Mapping the content of the control FILE {FILENAME:FILEZIE}
def convertControlDataToControlFileMap(tempControlFileData):
    try:
        controlFileMapofItems = {}
        for line in tempControlFileData:
            controlFileMapofItems[line.split('\t')[0]] = int(line.split('\t')[1])
        return controlFileMapofItems
    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        sys.exit()

#Compare the filenames and its sizes.
def compareSourceandControlFile(tempControlFileContent,temp_controlFileContentSize, temp_SourcefileDict):
    try:
        print("RECEIVED//Searching   tempControlFileContent :: {0}".format(tempControlFileContent))
        for eachItem in temp_SourcefileDict.keys():
            absPathofSourceFile = os.path.abspath(eachItem)
            head, tail = os.path.split(absPathofSourceFile)
            if tempControlFileContent.replace(" ", "") == tail.replace(" ", ""):
                logger.info("ControlFileContent {0} is PRESENT in the source Path :: {1}".format(tempControlFileContent, head))
                logger.info("Calling for the Byte Validation.")
                if temp_controlFileContentSize == temp_SourcefileDict[eachItem]:
                    return "FileSizeMatches" , absPathofSourceFile
                else:
                    return "FileSizeNotMatches", absPathofSourceFile

    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        sys.exit()
