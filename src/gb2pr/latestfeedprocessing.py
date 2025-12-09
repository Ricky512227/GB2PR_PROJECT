import loop_config
from .logMonitor import logger
import sys
import os
from datetime import datetime
import glob
from .compareFileNameSize import compareSourceandControlFile,convertControlDataToControlFileMap
from .CommonUtlity import extractZipFile, untarGZfile,readJsonFile,getS3UrlsFromJsonData,isvalidDate
from .recordCount import feedRunner
from .WatchDog import *
import argparse
import re

def getExtractedDirName(tmpPath):
    dirName = r"(Managed_[^>]*?_[^>]*?)[\d]+\.zip"
    exDirName = re.findall(dirName, str(tmpPath))[0]
    return exDirName

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('aws_app_bucket', help="Application Bukcet")
    parser.add_argument('statemachine_name', help="StateMachine")
    parser.add_argument('source_name', help="SourceName")
    parser.add_argument('s3_bucket', help="Bucket where source file/files is located")
    parser.add_argument('s3_prefixes', help="s3 path/prefix to all files")
    return parser.parse_args()

if __name__ == '__main__':
    # left as package module — runtime logic remains unchanged
    pass
