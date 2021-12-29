from logMonitor import logger
import glob,os,sys

#Open the control file and get its contents based on seprator
def readTheFileBasedonSeperator(tempFileName):
	try:
		logger.info("RECEIVED Controller File Name :: {0}".format(tempFileName))
		with open(tempFileName) as file: 
			data = file.read()
		controlFileData =  (filter(None, data.split('\n')))
		logger.info("PROCESSED Controller File Name :: SUCCESS")
		return controlFileData
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		sys.exit()

#Rerurn MAP OF FILENAME AND THE RECORD COUNT FO MetaData
def getMetadataFileRecordCount(tempMetadataContent):
	try:
		logger.info("Received Non-MetaData FileName to get the RecordCound :: {0}".format(tempMetadataContent))
		metadataContentMap = {}
		for lineIndex, line in enumerate(tempMetadataContent):
			if lineIndex!= 0:
				if line !="\r":
					if line.split('\t')[8] != '""':
						recordCount = int(line.split('\t')[8].strip('"'))
						metadataContentMap[line.split('\t')[0].strip('"')] = recordCount
					else:
						recordCount = 0 
						metadataContentMap[line.split('\t')[0].strip('"')] = recordCount
		logger.info("PROCESSED tempMetadataContent :: {0}".format(metadataContentMap))
		return metadataContentMap
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		sys.exit()


def getNonMetadataFileRecordCount(nonMetadataFileName):
	try:
		logger.info("Received Non-MetaData FileName to get the RecordCound :: {0}".format(nonMetadataFileName))
		totalLines = sum(1 for line in open(nonMetadataFileName))
		logger.info("Fetched RecordCount SUCCESS -:: {0}".format(totalLines))
		return totalLines
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		sys.exit()


def compareRecordCounts(metadataContentMap,nonMetadataContentMap):
	try:
		logger.info ("For Comparision Received MetaDataConentMap :: {0}".format(metadataContentMap))
		logger.info ("For Comparision Received NonMetaDataConentMap :: {0}".format(nonMetadataContentMap))
		for metaDataItemName, metaDataRecordCount in metadataContentMap.items():
			if metaDataItemName.endswith(".gz"):
				metaDataItemName = metaDataItemName.strip(".gz")
			if metaDataItemName in nonMetadataContentMap.keys():
				if metaDataRecordCount == nonMetadataContentMap[metaDataItemName]:
					print ("MATCHED_RECORD_COUNT :: [METADATA-FILE] {0}, {1} <==> [SOURCE] {2}, {3}".format(metaDataItemName, metaDataRecordCount, metaDataItemName, nonMetadataContentMap[metaDataItemName]))
					logger.info("MATCHED_RECORD_COUNT :: [METADATA-FILE] {0}, {1} <==> [SOURCE] {2}, {3}".format(metaDataItemName, metaDataRecordCount, metaDataItemName, nonMetadataContentMap[metaDataItemName]))

				else:
					print ("NOT_MATCHED_RECORD_COUNT :: [METADATA-FILE] {0}, {1} <==> [SOURCE]  {2}, {3}".format(metaDataItemName, metaDataRecordCount, metaDataItemName, nonMetadataContentMap[metaDataItemName]))
					logger.error("NOT_MATCHED_RECORD_COUNT :: [METADATA-FILE] {0}, {1} <==> [SOURCE]  {2}, {3}".format(metaDataItemName, metaDataRecordCount, metaDataItemName, nonMetadataContentMap[metaDataItemName]))

			else:
				print ("FILE_NOTAVAILALBE_TO_FETCH_RECORD_COUNT :: [METADATA-FILE] {0}, {1} <==> in [SOURCE] NA ".format(metaDataItemName, metaDataRecordCount))
				logger.error ("FILE_NOTAVAILALBE_TO_FETCH_RECORD_COUNT :: [METADATA-FILE] {0}, {1} <==> in [SOURCE] NA ".format(metaDataItemName, metaDataRecordCount))

	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		sys.exit()

def comparisionDecider(tmpFeedTypeBasePath,metaDataFileName):
	try:
		logger.info("Received FeedTypeBasePath :: {0}".format(tmpFeedTypeBasePath))
		print("Checking FeedTypeBasePath  for :: {0}".format(tmpFeedTypeBasePath))
		nonMetadataFileNames = 	[nonMetadataFileName for nonMetadataFileName in glob.glob(tmpFeedTypeBasePath+"/*") if nonMetadataFileName]
		# nonMetadataFileNames = 	[nonMetadataFileName for nonMetadataFileName in glob.glob(tmpFeedTypeBasePath+"\*") if nonMetadataFileName]
		logger.info("nonMetadataFileNames :: {0}".format(nonMetadataFileNames))
		metadataContent = (readTheFileBasedonSeperator(metaDataFileName))
		metadataContentMap = getMetadataFileRecordCount(metadataContent)
		nonMetadataContentMap = {}
		for nonMetadataItem in nonMetadataFileNames:
			_, nonMetadataFileName = os.path.split(nonMetadataItem)
			if not nonMetadataFileName.startswith("metadata_"):
				nonMetadataContentMap[nonMetadataFileName] = getNonMetadataFileRecordCount(nonMetadataItem)
		return compareRecordCounts(metadataContentMap,nonMetadataContentMap)
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		sys.exit()

def feedRunner(currentFeedValidationDirName, currentFeedValidationDirNameWithDateTime):
	try:
		
		logger.info("currentFeedValidationDirNameWithDateTime :: {0}".format(currentFeedValidationDirNameWithDateTime))
		# if not os.path.exists(currentFeedValidationDirNameWithDateTime):
		# 	os.makedirs(current_year_month_day)
		currentFeedValidationDirName = os.path.join(currentFeedValidationDirNameWithDateTime, currentFeedValidationDirName)
		logger.info ("currentFeedValidationDirName :: {0}".format(currentFeedValidationDirName))

		metadataBasePath = os.path.join(currentFeedValidationDirName,"metadata_*")
		logger.info("metadataBasePath :: {0}".format(metadataBasePath))
		metaDataFileName = glob.glob(metadataBasePath)
		if len(metaDataFileName) !=0:
			logger.info("metaDataFileName :: {0}".format(metaDataFileName[0]))
			#PASS THE FEED TYPE PATH[DEED/ASMT]
			comparisionDecider(currentFeedValidationDirName,metaDataFileName[0])
			
		else:
			print("MetaData File doesnt Exists [Path] :: {0}".format(currentFeedValidationDirName))
			logger.error("MetaData File doesnt Exists [Path] :: {0}".format(currentFeedValidationDirName))
	
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		sys.exit()
		


