"""
Logging Module

Provides centralized logging configuration for the application.
"""
import logging
import sys
import os
import datetime

logger = logging

try:
    scriptPath = os.path.dirname(os.path.realpath(__file__))
    # Go up to project root (from internal/monitors/)
    projectRoot = os.path.dirname(os.path.dirname(os.path.dirname(scriptPath)))
    logsPath = os.path.join(projectRoot, 'logs')
    if not os.path.exists(logsPath):
        os.makedirs(logsPath)
    logFileName = str(datetime.datetime.now().strftime("%Y%m%d_%H")) + ".log"
    logFilePath = os.path.join(logsPath, logFileName)
    print("Log File Path ::" + logFilePath)
    timeFormat = str(datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"))
    logger.basicConfig(
        filename=logFilePath,
        format='%(asctime)s - %(levelname)s - %(filename)s :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        filemode='w'
    )
except Exception as ex:
    print('Error occurred :: {0} \tLine No: {1}'.format(ex, sys.exc_info()[2].tb_lineno))
    logger.error('Error occurred :: {0} \tLine No: {1}'.format(ex, sys.exc_info()[2].tb_lineno))
    sys.exit()