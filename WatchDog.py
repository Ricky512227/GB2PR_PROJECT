import os
import time,sys
from logMonitor import logger
from datetime import datetime
from CommonUtlity import executeCmd

#Fucntion to construct the Message Notification path 
def constructS3PathForMessage(monitoringBucketName,MonitoringBucketRegionName,objPath1,objPath2,objPath3,currentDate,tempDataSetID):
	try:
		#s3://gfgpr1-devl-edl2-us-east-1/prepare/cin/dflt/DSET000XXXXX/SQs STATEMACHINETRIGGER/2021-08-25/BKES/
		s3MessagePath = "s3://"+monitoringBucketName+"/"+MonitoringBucketRegionName+"/"+objPath1+"/"+tempDataSetID+"/"+objPath2+"/"+currentDate+"/"+objPath3+"/"
		logger.info("Constructed s3Path for Message :: {0}".format(s3MessagePath))
		return s3MessagePath
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))

def isFileExists(fileType, filePath):
	try:
		# -z: if the file is zero length, return 0.
		# -f: if the path is a file, return 0.
		#"hdfs dfs -fz s3://gfgpr1-devl-edl2-us-east-1/prepare/cin/dflt/DSET000XXXXX/SQs STATEMACHINETRIGGER/2021-08-25/BKES/"
		cmdToCheckFile = "hdfs dfs -f"+" "+filePath
		logger.info("Searching for the {0} File".format(fileType))
		returnCode, _ = executeCmd(cmdToCheckFile)
		if returnCode == 0:
			logger.info("[{0}] File_Found ==> {1}".format(fileType, filePath))
			return True
		else:
			logger.info("[{0}] File_NOT_Found ==> {1}".format(fileType, filePath))
			return False
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))

def isDirExists(fileType,objName):
	try:
		# -d: f the path is a directory, return 0.
		# -e: if the path exists, return 0.
		# -s: if the path is not empty, return 0.
		#"hdfs dfs -de s3://gfgpr1-devl-edl2-us-east-1/prepare/cin/dflt/DSET000XXXXX/SQs STATEMACHINETRIGGER/2021-08-25/BKES/"
		cmdToCheckDir = "hdfs dfs -de"+" "+objName
		returnCode, _ = executeCmd(cmdToCheckDir)
		if returnCode == 0:
			logger.info("[{0}] S3_Object_Found ==> {1}".format(fileType, objName))
			return True
		else:
			logger.info("[{0}] S3_Object_Not_Found ==> {1}".format(fileType, objName))
			return False
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))

def isSourcFileMessageExists(fileType, sFilePath,currentDate):
	try:
		sFilePath = os.path.join(sFilePath,currentDate,".json") 
		objName, fileName = os.path.split(sFilePath)
		logger.info("Searching for the {1} S3Object_Path :: {1}".format(fileType, objName))
		if isDirExists(fileType,objName):
			if isFileExists(fileType,sFilePath):
				return True
			else:
				return False
		else:
			return False
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))

#First Search the Controller, S3object and Message file arraived or not.
def searchControlMessageFile(fileType, filepath, currentDate):
	try:
		
		filepath = os.path.join(filepath,currentDate,".json") 
		objName, fileName = os.path.split(filepath)
		#Checking here for  controller S3Object path.
		if isDirExists(fileType,objName):
			#If s3Object exists then search for the messagefile.json
			if isFileExists(fileType, filepath):
				return True
			else:
				searchControlMessageFile(fileType, filepath, currentDate)
		else:
			searchControlMessageFile(fileType, filepath, currentDate)
	except Exception as ex:
		print("Error occurred :: {0}\tLine No :: {1}".format(ex, sys.exc_info()[2].tb_lineno))


def searchSourceMessageFile(fileType, monitoringInterval,listOfSoruceMessageObjPaths,currentDate):
	try:
		#Checking here for all sourceFiles Messages whether it exists or not
		sourceFileCounter = 0
		for sFilePath in listOfSoruceMessageObjPaths:	
			if isSourcFileMessageExists(fileType, sFilePath,currentDate):
				sourceFileCounter = sourceFileCounter+1
				time.sleep(1)
		#If the total Soruces Messages are not received then again begin the ticker.
		if sourceFileCounter != len(listOfSoruceMessageObjPaths):
			print (" Fetched Data and [Received Count vs Target Count] ==> {0} {1}".format(sourceFileCounter, len(listOfSoruceMessageObjPaths)))
			print("Sleeping for {0}secs".format(monitoringInterval))
			time.sleep(monitoringInterval)
			searchSourceMessageFile(fileType, monitoringInterval,listOfSoruceMessageObjPaths)
			return True
		else:
			print ( "Fetched Data and [Received Count vs Target Count] ==> {0} {1}".format(sourceFileCounter, len(listOfSoruceMessageObjPaths)))
			return True
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))









