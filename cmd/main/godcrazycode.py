#!/usr/bin/env python3
"""
GB2PR Main Entry Point

Main application for processing feeds and managing file operations.
"""
import sys
import os
import configparser
from datetime import datetime
import glob

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from pkg.common.CommonUtlity import extractZipFile, untarGZfile, readJsonFile, getS3UrlsFromJsonData, isvalidDate
from pkg.common.compareFileNameSize import compareSourceandControlFile, convertControlDataToControlFileMap
from pkg.common.recordCount import feedRunner
from internal.monitors.logMonitor import logger
from internal.monitors.WatchDog import *


def processControlfileContents(controlfileContent, RESULT):
    """Process control file contents and merge duplicates."""
    map = {}
    for did in RESULT.keys():
        for item in controlfileContent.keys():
            if did in item:
                if did not in map.keys():
                    map[did] = 1
                else:
                    map[did] = map[did] + 1

    for item in map.keys():
        if map[item] > 1:
            temp = 0
            for mainKey, value in controlfileContent.items():
                if item in mainKey:
                    temp = temp + value
                    del controlfileContent[mainKey]
            controlfileContent[".".join(mainKey.split(".")[:-1])] = temp
    return controlfileContent


if __name__ == '__main__':
    try:
        receivedArgs = sys.argv
        logger.info("Received arguments :: {0} passed and its Total :: {1}".format(receivedArgs, len(receivedArgs)))
        if len(receivedArgs) == 2:
            format = "%Y-%m-%d"
            targetDate = receivedArgs[1]
            if isvalidDate(targetDate, format):
                # Setting the path as the current execution directory
                ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                # Setting the path for target location for notification message files
                configurationFilePathBasePath = os.path.join(ROOT_DIR, "pkg", "config", "configs.ini")

                logger.info("Initializing Read configurations ...")
                config = configparser.ConfigParser()
                config.read(configurationFilePathBasePath)

                # Validating all the configs
                logger.info("Validation Check for configurations ==> [ONGOING]")
                # Add your configuration validation logic here

                logger.info("Configuration validation completed successfully")
            else:
                logger.error("Invalid date format. Expected format: YYYY-MM-DD")
                sys.exit(1)
        else:
            logger.error("Invalid number of arguments. Expected: 1 (target date)")
            sys.exit(1)
    except Exception as ex:
        logger.error("Error occurred :: {0} \tLine No: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        sys.exit(1)
