import zipfile,sys
from logMonitor import logger
import gzip
import time
import shutil,os
import json
import subprocess as sp
import subprocess

def executeCmd(cmdtoexecute):
	try:
		logger.info("Command to execute ==>"+str(cmdtoexecute))
		pipe = sp.Popen(cmdtoexecute, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
		result = pipe.communicate()
		try:
			if result[0] != "":
				result = result[1]
			returnCode = pipe.returncode
			return returnCode, result
		except:
			returnCode = 1
			return returnCode, None
	except subprocess.CalledProcessError as ex:
		logger.error("FAILED to execute cmd ==> " + str(cmdtoexecute))
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))

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
		if tempMessageData is not None:
			messageName =  tempMessageData['Message']
			if messageName == 'BKFS':
				totalDataSets = len(tempMessageData['datasets'])
				if totalDataSets> 0:
					tempListofS3Urls = [tempMessageData['datasets'][eachDataSet]['datasetPrefix'][0] for eachDataSet in range (0,totalDataSets)]
					return tempListofS3Urls[0]
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))



