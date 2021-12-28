import configparser
from logMonitor import logger
import sys,os
from datetime import datetime
import glob
from compareFileNameSize import compareSourceandControlFile,convertControlDataToControlFileMap
from CommonUtlity import extractZipFile, untarGZfile,readJsonFile,getS3UrlsFromJsonData
from recordCount import feedRunner
from WatchDog import *


if __name__ == '__main__':
	try:
		#Setting the path as the currnet exection direcotry.
		ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
		#Setting the path  for target location for notification message files [NotificationMessagesFromS3/{CurrentTimStamp}].
		configurationFilePathBasePath = os.path.join(ROOT_DIR,"configs.ini")


		logger.info("Initializing Read configurations ...")
		config = configparser.ConfigParser()
		config.read(configurationFilePathBasePath)
		
		#Validating all the configs
		logger.info ("Valiation Check for configurations ==> [ONGOING]") 
		#Collecting Monitoring Configs to search for MessagE Notification Files on interval basis.
		monitoringInterval = int(config.get('MONITORING','Monitoring_Interval'))
		monitoringBucketName = config.get('MONITORING','Monitoring_BucketName')
		MonitoringBucketRegionName = config.get('MONITORING','Monitoring_Bucket_RegionName')
		objPath1 = config.get('MONITORING','Obj_Path1')
		objPath2 = config.get('MONITORING','Obj_Path2')
		objPath3 = config.get('MONITORING','Obj_Path3')
		messageName = config.get('MONITORING','Message_Name')
		componentName = config.get('MONITORING','Component_Name')
		#Collecting DataSetId's Configs .
		refreshAsmtDataSetId = config.get('DataSet_ID','Managed_Refresh_ASMT_DSET')
		refreshDeedDataSetId = config.get('DataSet_ID','Managed_Refresh_DEED_DSET')
		updateAsmtDataSetId = config.get('DataSet_ID','Managed_Update_ASMT_DSET')
		updateDeedDataSetId = config.get('DataSet_ID','Managed_Update_DEED_DSET')
		controllerId = config.get('DataSet_ID','Controller_ID')
		logger.info ("Valiation Check for configurations ==> [SUCCESS]") 


		current_year_month_day = datetime.today().strftime('%Y-%m-%d')

		#Setting the path  for target location for notification message files [NotificationMessagesFromS3/{CurrentTimStamp}].
		downloadedNotificationMessageFilesBasePath = os.path.join(ROOT_DIR,"NotificationMessagesFromS3")
		logger.info("downloadedNotificationMessageFilesBasePath :: {0}".format(downloadedNotificationMessageFilesBasePath))
		downloadedNotificationMessageFilesBasePathWithCurrentDate = os.path.join(downloadedNotificationMessageFilesBasePath,current_year_month_day)
		logger.info("downloadedNotificationMessageFilesBasePathWithCurrentDate :: {0}".format(downloadedNotificationMessageFilesBasePathWithCurrentDate))
		if not os.path.exists(downloadedNotificationMessageFilesBasePathWithCurrentDate):
			logger.info("Directory not exists, Creating New Directory :: {0}".format(downloadedNotificationMessageFilesBasePathWithCurrentDate))
			os.makedirs(downloadedNotificationMessageFilesBasePathWithCurrentDate)


		#Setting the path  for target location for  source files [ManagedTypeFilesFromS3/{CurrentTimStamp}].
		downloadedZip_SourceFileBasePath = os.path.join(ROOT_DIR,"ManagedTypeFilesFromS3")
		logger.info("downloadedZip_SourceFileBasePath :: {0}".format(downloadedZip_SourceFileBasePath))
		downloadedSourceFilesBasePathWithCurrentDate = os.path.join(downloadedZip_SourceFileBasePath,current_year_month_day)
		logger.info("downloadedSourceFilesBasePathWithCurrentDate :: {0}".format(downloadedSourceFilesBasePathWithCurrentDate))
		if not os.path.exists(downloadedSourceFilesBasePathWithCurrentDate):
			logger.info("Directory not exists, Creating New Directory :: {0}".format(downloadedSourceFilesBasePathWithCurrentDate))
			os.makedirs(downloadedSourceFilesBasePathWithCurrentDate)
		

		#Setting the path  for target location for conroller files [ControllerFilesFromS3/{CurrentTimStamp}].
		downloadedControlFileBasePath = os.path.join(ROOT_DIR,"ControllerFilesFromS3")
		logger.info("downloadedControlFileBasePath :: {0}".format(downloadedControlFileBasePath))
		downloadedControlFileBasePathWithCurrentDate = os.path.join(downloadedControlFileBasePath,current_year_month_day)
		logger.info("downloadedControlFileBasePathWithCurrentDate :: {0}".format(downloadedControlFileBasePathWithCurrentDate))
		if not os.path.exists(downloadedControlFileBasePathWithCurrentDate):
			logger.info("Directory not exists, Creating New Directory :: {0}".format(downloadedControlFileBasePathWithCurrentDate))
			os.makedirs(downloadedControlFileBasePathWithCurrentDate)

		#Prepare the s3Url for Messaging Bucket
		#1.Validatie the s3Object [Exists or not]
		#2.Check if the Messaging File Arraived.
		#3.If arraived,
		#Read the Message.json the file and pick the s3 urls for ControlFile, Source File.

		#Defining the fileTypes, currentDate [i.e messageFile naming convention should be yyyymmdd.json]
		controlType = "Control"
		sourceType = "Source"
		currentDate= datetime.today().strftime('%Y-%m-%d')

		controlMessageObjPath = constructS3PathForMessage(monitoringBucketName,MonitoringBucketRegionName,objPath1,objPath2,objPath3,currentDate,controllerId)
		listOfSoruceMessageObjPaths = [constructS3PathForMessage(monitoringBucketName,MonitoringBucketRegionName,objPath1,objPath2,objPath3,currentDate,sourceDataSetId) for sourceDataSetId in [refreshAsmtDataSetId,refreshDeedDataSetId,updateAsmtDataSetId,updateDeedDataSetId]]
		print ("Initiated search to get the Message Files ....")
		isAllMessageReceived = False
		while True:
			if not searchControlMessageFile(controlType,controlMessageObjPath,currentDate):
				print("Sleeping for {0}secs".format(monitoringInterval))
				time.sleep(monitoringInterval)
			else:
				if searchSourceMessageFile(sourceType, monitoringInterval,listOfSoruceMessageObjPaths,currentDate):
					isAllMessageReceived = True
					print ("All Messages are Arrived .... Stopping the Monitor...")
					break

		#IF all the message files are received then , copy the message and get the read the S3URLS
		if isAllMessageReceived :
			failedUrlsDataSetCount = 0
			targetLocation = downloadedNotificationMessageFilesBasePathWithCurrentDate
			listofS3Urls = []
			for messageFileName in listOfSoruceMessageObjPaths.extend([controlMessageObjPath]):
				messageFileName = os.path.join(messageFileName, ".json")
				#Cmd to download the file to local from s3
				cmdToGetS3FileCopyToLocal = "hdfs dfs -copyToLocal"+" "+messageFileName+" "+ targetLocation
				returnCode, cmdData = executeCmd(cmdToGetS3FileCopyToLocal)
				if returnCode ==0:
					#Using the downloaded message.json fetching the s3 url. 
					tempMessageData = readJsonFile(messageFileName)
					s3Url = getS3UrlsFromJsonData(tempMessageData)
					if  s3Url is not None:
						logger.info("Checking URLS ==> {0} :: URLS_FOUND".format(messageFileName))
						listofS3Urls.append(s3Url)		
					else:
						logger.info("Checking URLS ==> {0} :: URLS_NOT_FOUND".format(messageFileName))
						failedUrlsDataSetCount = failedUrlsDataSetCount+1
				else:
					#logging the error message
					logger.error("File {0} Failed to  Download/Copy to Local :: {1}\nReason :: {2}".format(messageFileName, targetLocation, cmdData))

		#Once all the s3URLs are fetched then download the controller files and source files to local.
		if failedUrlsDataSetCount == 0:
			failedDownloadedCount = 0 
			#Downloading/Copying the source, controller files from s3
			for s3Url in listofS3Urls:
				_, tail = os.path.split(s3Url)
				if tail.startswith("BKI"):
					targetLocation = downloadedControlFileBasePathWithCurrentDate
				else:
					targetLocation = downloadedSourceFilesBasePathWithCurrentDate
				cmdToGetS3FileCopyToLocal = "hdfs dfs -copyToLocal"+" "+s3Url+" "+targetLocation
				returnCode, cmdData = executeCmd(cmdToGetS3FileCopyToLocal)
				if returnCode ==0:
					logger.info("File {0} Download/Copy to Local :: {1}".format(s3Url, targetLocation))
				else:
					#logging the error message
					failedDownloadedCount = failedDownloadedCount+1
					logger.error("File {0} Failed to  Download/Copy to Local :: {1}\nReason :: {2}".format(s3Url, targetLocation, cmdData))
		
		#Once all the files are downloaded then send those file to File Validations
		if failedDownloadedCount == 0:
			controlFilePath = glob.glob(os.path.join(downloadedControlFileBasePathWithCurrentDate,"*.txt"))
			controlFilePath = controlFilePath[0]
			logger.info("controlFilePath :: {0}".format(controlFilePath))
			#Open the control file and read the contents.
			with open(controlFilePath , 'r') as file: 
				data = file.read()
				controlFileData =  (filter(None, data.split('\n')))
				control_file_Dict =  (convertControlDataToControlFileMap(controlFileData))
				if len(control_file_Dict) != 0:
					logger.info('Control File Content Collected  :: {0}'.format(control_file_Dict))
					#Collect the downloaded ZIP file Names[Source(ASMT/DEED)]
					SourcefileDict = {}
					for zfile in  glob.glob(downloadedSourceFilesBasePathWithCurrentDate+"/*"):
						if ".zip" in zfile:
							SourcefileDict[zfile] =  int(os.stat(zfile).st_size)
					logger.info("Source ZIP Files (Deed and ASMT Files Collected )  :: {0} , {1}".format(SourcefileDict.keys(), len(SourcefileDict.keys())))
					if len(SourcefileDict.keys())!=0:
						ByteValidationPassedFiles = []
						#Compare the controlfile contents(filename and filesize) withe the source filename and filesize
						for controlFileName, controlFileSize in control_file_Dict.items():
							reason, absPathofSourceFile =  compareSourceandControlFile(controlFileName,controlFileSize,SourcefileDict)
							logger.info('Received vaidation reason for controlFileName :: {0} {1}'.format(controlFileName, reason))
							if reason ==  "FileSizeMatches":
								logger.info("Checking  - ControlFileData [FileName-Size] [{0} - {1}] ==> BOTH_FILEZSIZES_ARE_MATCHING with  SourceFile [FileName-Size] [{0} - {1}] ] ".format(controlFileName, controlFileSize, controlFileName, controlFileSize))
								
								#STORE ALL the files Names :: Which was passed during the file size Validations, the collected file will send to be extract and get the record Count
								ByteValidationPassedFiles.append(absPathofSourceFile)
							
							elif reason ==  "FileSizeNotMatches":
								logger.info("Checking  - ControlFileData [FileName-Size] [{0} - {1}] ==> BOTH_FILEZSIZES_ARE_NOT_MATCHING  with  SourceFile [FileName-Size] [{2} - {3}] ]".format(controlFileName, controlFileSize, controlFileName, SourcefileDict[controlFileName]))

							elif reason ==  "FileNotAvailable":
								logger.info("Checking  - ControlFileData [FileName-Size] [{0} - {1}] ==> FILE_NOT_AVAILABLE in Downloaded Path :: {2} ".format(controlFileName, controlFileSize,downloadedSourceFilesBasePathWithCurrentDate))
					
						if len(ByteValidationPassedFiles) >0:
							logger.info("s3- Valid ZipFiles :: {0}".format(ByteValidationPassedFiles))
							for eachS3Zipfile in ByteValidationPassedFiles:
								extractedFileList=  extractZipFile(eachS3Zipfile, downloadedSourceFilesBasePathWithCurrentDate)
								if len(extractedFileList) > 0:
									logger.info("Travesing through the unzip file and find the.gz files.. if exists then extract that as well")
									getGzFiles = [gzFile for gzFile in extractedFileList if ".gz" in  gzFile]
									for eachGZfile in getGzFiles:
										logger.info("Found and sent for extraction :: {0}".format(eachGZfile))
										if untarGZfile(os.path.join(downloadedSourceFilesBasePathWithCurrentDate,eachGZfile)):
											logger.info("SUCCESS Extracted the .gz file:: {0}".format(eachGZfile))

											#AfterUnzip the Directory and its sub .gz Successfully then moving towards for those current extraction folder recordCount Vlaidations
											extractedDirNameSplit = extractedFileList[0].split("_")
											feedName = "_".join(extractedDirNameSplit[0:2])
											feedType = extractedDirNameSplit[-1]
											currentFeedValidationDirName = os.path.join(extractedFileList[0],feedName,feedType)
											print("currentFeedValidationDirName :: {0}".format(currentFeedValidationDirName))
											print("#########################################################")
											feedRunner(currentFeedValidationDirName,downloadedSourceFilesBasePathWithCurrentDate)
											print("#########################################################")

										else:
											logger.info(" failed Extracted the .gz file:: {0}".format(eachGZfile))

					else:
						print ('No ZIP files found inside the Source Directory :: {0}'.format(downloadedSourceFilesBasePathWithCurrentDate))
						logger.error('No ZIP files found inside the Source Directory :: {0}'.format(downloadedSourceFilesBasePathWithCurrentDate))
				else:
					print ('No Data found inside the Control File :: {0}'.format(controlFilePath))
					logger.error('No Data found inside the Control File :: {0}'.format(controlFilePath))
		else:
			logger.error("Not all files downloaded/copied to local..")

	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		sys.exit()





