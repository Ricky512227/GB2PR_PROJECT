import loop_config
from logMonitor import logger
import sys,os
from datetime import datetime
import glob
from compareFileNameSize import compareSourceandControlFile,convertControlDataToControlFileMap
from CommonUtlity import extractZipFile, untarGZfile,readJsonFile,getS3UrlsFromJsonData,isvalidDate
from recordCount import feedRunner
from WatchDog import *
import argparse
import re

def getExtractedDirName(tmpPath):
	dirName = "(Managed_[^>]*?_[^>]*?)[\d]+\.zip"
	exDirName = re.findall(dirName,str(tmpPath))[0]
	return exDirName

def parse_args():
    """Method to define and ingest command line arguments.

    Returns:
        argparse: arguments object with cli arguments
    """
    parser = argparse.ArgumentParser()
    #gfqpr1-devl-edl2-us-east-1====> aws_app_bucket
    #etl ====> statemachine_name
    #BKFS ====> source_name
    #fnma-gb2pr-devl-edl-us-east-1-edl ====> s3_bucket
    # prepare/cin/dflt/DSET00009999/SQS_STATEMACHINE_TRIGGER/2022-01-06/BKFS/BKI_Control20220105.txt,prepare/cin/dflt/DSET00009999/SQS_STATEMACHINE_TRIGGER/2022-01-06/BKFS/BlackKnightPublicDataReport20220105.txt,prepare/cin/dflt/DSET00009999/SQS_STATEMACHINE_TRIGGER/2022-01-06/BKFS/Managed_Refresh_ASMT20220105.ccccczip,prepare/cin/dflt/DSET00009999/SQS_STATEMACHINE_TRIGGER/2022-01-06/BKFS/Managed_Refresh_Deed20220105.zip,prepare/cin/dflt/DSET00009999/SQS_STATEMACHINE_TRIGGER/2022-01-06/BKFS/Managed_Update_ASMT20220105.zip,prepare/cin/dflt/DSET00009999/SQS_STATEMACHINE_TRIGGER/2022-01-06/BKFS/Managed_Update_Deed20220105.zip ====> s3_prefixes
    parser.add_argument('aws_app_bucket', help="Application Bukcet")
    parser.add_argument('statemachine_name', help="StateMachine")
    parser.add_argument('source_name', help="SourceName")
    parser.add_argument('s3_bucket', help="Bucket where source file/files is located")
    parser.add_argument('s3_prefixes', help="s3 path/prefix to all files")

    return parser.parse_args()

if __name__ == '__main__':
    try:
        args = parse_args()
        # print("Received system args :: {0}\n Length {1}\n Type :: {2}".format(args, len(args, type(args))))
        received_AwsAppBucketName = args.aws_app_bucket
        received_StatemachineName = args.statemachine_name
        received_SourceName = args.source_name
        received_S3BucketName = args.s3_bucket
        received_S3Prefixes = args.s3_prefixes
        """Setting the path as the currnet exection direcotry."""
        ROOT_DIR = '/mnt/var/'
        """Initialize the loop_config object"""
        logger.info("Initializing Read configurations ...")
        config = loop_config.loop_config(args.aws_app_bucket, args.statemachine_name, args.source_name)
        config_variables = config.generate_properties_global_source_args()
        """Validating all the configs"""
        logger.info ("Valiation Check for configurations ==> [ONGOING]")
        refreshAsmtDataSetId = config_variables['Managed_Refresh_ASMT_DSET']
        refreshDeedDataSetId = config_variables['Managed_Refresh_Deed_DSET']
        updateAsmtDataSetId = config_variables['Managed_Update_ASMT_DSET']
        updateDeedDataSetId = config_variables['Managed_Update_Deed_DSET']
        controllerDataSetId = config_variables['FN2_DEED_DSET']
        """Collecting Monitoring Configs to search for MessagE Notification Files on interval basis."""
        monitoringInterval = int(config_variables['Monitoring_Interval'])
        monitoringBucketName = received_S3BucketName
        messageName = config_variables['Message_Name']
        componentName = config_variables['Component_Name']
        logger.info ("Valiation Check for configurations ==> [SUCCESS]")

        """Prepare the s3Url for Messaging Bucket
            1.Validatie the s3Object [Exists or not]
            2.Check if the Messaging File Arraived.
            3.If arraived,
            4.Read the Message.json the file and pick the s3 urls for ControlFile, Source File."""

        """Preparing the UR's to fetch the NOTIFICATION MESSAGES
            messageFile naming convention should be yyyymmdd.json"""
        controlType = "Control"
        sourceType = "Source"
        listOfDataSets = [refreshAsmtDataSetId,refreshDeedDataSetId,updateAsmtDataSetId,updateDeedDataSetId,controllerDataSetId]
        s3prefix = args.s3_prefixes
        receivedS3Prefixs = s3prefix.split(",")

        listofS3Urls = []
        for s3_prefix in receivedS3Prefixs:
            listofS3Urls.append('s3://{}/{}'.format(received_S3BucketName, s3_prefix))
        logger.info("Collected S3 URLS :: {0}".format(listofS3Urls))

        currentTimeStamp = datetime.datetime.today().strftime('%Y%m%d%H%M')
        #Setting the path  for target location for  source files [FilesFromS3/SourceFiles/{CurrentTimStamp}].
        downloadedZip_SourceFileBasePath = os.path.join(ROOT_DIR,"FilesFromS3","SourceFiles")
        logger.info("downloadedZip_SourceFileBasePath :: {0}".format(downloadedZip_SourceFileBasePath))
        downloadedSourceFilesBasePathWithCurrentDate = os.path.join(downloadedZip_SourceFileBasePath,currentTimeStamp)
        logger.info("downloadedSourceFilesBasePathWithCurrentDate :: {0}".format(downloadedSourceFilesBasePathWithCurrentDate))
        if not os.path.exists(downloadedSourceFilesBasePathWithCurrentDate):
            logger.info("Directory not exists, Creating New Directory :: {0}".format(downloadedSourceFilesBasePathWithCurrentDate))
            os.makedirs(downloadedSourceFilesBasePathWithCurrentDate)

        #Setting the path  for target location for conroller files [FilesFromS3/ControllerFiles/{CurrentTimStamp}].
        downloadedControlFileBasePath = os.path.join(ROOT_DIR,"FilesFromS3","ControllerFiles")
        logger.info("downloadedControlFileBasePath :: {0}".format(downloadedControlFileBasePath))
        downloadedControlFileBasePathWithCurrentDate = os.path.join(downloadedControlFileBasePath,currentTimeStamp)
        logger.info("downloadedControlFileBasePathWithCurrentDate :: {0}".format(downloadedControlFileBasePathWithCurrentDate))
        if not os.path.exists(downloadedControlFileBasePathWithCurrentDate):
            logger.info("Directory not exists, Creating New Directory :: {0}".format(downloadedControlFileBasePathWithCurrentDate))
            os.makedirs(downloadedControlFileBasePathWithCurrentDate)

        #Setting the path  for target location for Data Report [FilesFromS3/dataReportFiles/{CurrentTimStamp}].
        downloadedReportFileBasePath = os.path.join(ROOT_DIR,"FilesFromS3","DataReportFiles")
        logger.info("downloadedReportFileBasePath :: {0}".format(downloadedReportFileBasePath))
        downloadedReportFileBasePathWithCurrentDate = os.path.join(downloadedReportFileBasePath,currentTimeStamp)
        logger.info("downloadedReportFileBasePathWithCurrentDate :: {0}".format(downloadedReportFileBasePathWithCurrentDate))
        if not os.path.exists(downloadedReportFileBasePathWithCurrentDate):
            logger.info("Directory not exists, Creating New Directory :: {0}".format(downloadedReportFileBasePathWithCurrentDate))
            os.makedirs(downloadedReportFileBasePathWithCurrentDate)

        # for s3prefix in receivedS3Prefix:
        #     if s3prefix.split("/")[3] ==  controllerDataSetId:
        #         tmps3Prefix = "/".join(s3prefix.split("/")[:-1])
        #         controlMessageObjPath = constructS3PathForNotificationMsg(controlType, monitoringBucketName,tmps3Prefix)
        #     elif  s3prefix.split("/")[3] in [refreshAsmtDataSetId,refreshDeedDataSetId,updateAsmtDataSetId,updateDeedDataSetId]:
        #         tmps3Prefix = "/".join(s3prefix.split("/")[:-1])
        #         listOfSoruceMessageObjPaths = [constructS3PathForNotificationMsg(sourceType,monitoringBucketName,tmps3Prefix)]

        # Download the files based on the inputs through s3_bucket and s3_prefixes
        failedDownloadedCount = 0
        for s3Url in listofS3Urls:
            s3FilePath, s3fileName = "/".join(s3Url.split("/")[:-1]) , s3Url.split("/")[-1]
            logger.info("s3FilePath :: {0}".format(s3FilePath))
            logger.info("s3fileName :: {0}".format(s3fileName))
            #targetLocation = /mnt/var/FilesFromS3/{targetnamelocaiton}/{timestamp}
            if s3fileName.startswith("BKI"):
                targetLocation = downloadedControlFileBasePathWithCurrentDate+"/"
            elif s3fileName.startswith("BlackKnightPublicDataReport"):
                targetLocation = downloadedReportFileBasePathWithCurrentDate+"/"
            else:
                targetLocation = downloadedSourceFilesBasePathWithCurrentDate+"/"
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
            controlFilePath = glob.glob(os.path.join(downloadedControlFileBasePathWithCurrentDate,"BKI*.txt"))
            controlFilePath = controlFilePath[0]
            logger.info("controlFilePath :: {0}".format(controlFilePath))
            #Open the control file and read the contents.
            with open(controlFilePath , 'r') as file:
                data = file.read()
                controlFileData =  (filter(None, data.split('\n')))
                control_file_Dict =  convertControlDataToControlFileMap(controlFileData)
                if len(control_file_Dict) != 0:
                    logger.info('Control File Content Collected  :: {0}'.format(control_file_Dict))
                    #Collect the downloaded ZIP file Names[Source(ASMT/DEED)]
                    SourcefileDict = {}
                    for zfile in  glob.glob(downloadedSourceFilesBasePathWithCurrentDate+"/*"):
                        logger.info('SOURCE_FILE : {0}'.format(zfile))
                        if ".zip" in zfile:
                            SourcefileDict[zfile] =  int(os.stat(zfile).st_size)
                    logger.info("Source ZIP Files (Deed and ASMT Files Collected )  :: {0} , {1}".format(SourcefileDict.keys(), len(SourcefileDict.keys())))
                    if len(SourcefileDict.keys())!=0:
                        ByteValidationPassedFiles = []
                        #Compare the controlfile contents(filename and filesize) withe the source filename and filesize
                        for controlFileName, controlFileSize in control_file_Dict.items():
                            reason, absPathofSourceFile =  compareSourceandControlFile(controlFileName,controlFileSize,SourcefileDict)
                            logger.info('Received Byte_Validation reason for controlFileName :: {0} {1}'.format(controlFileName, reason))
                            if reason ==  "FileSizeMatches":
                                logger.info("CHECKING  - ControlFileData [FileName-Size] [{0} - {1}] ==> BOTH_FILEZSIZES_ARE_MATCHING with  SourceFile [FileName-Size] [{0} - {1}] ] ".format(controlFileName, controlFileSize, controlFileName, controlFileSize))
                                #STORE ALL the files Names :: Which was passed during the file size Validations, the collected file will send to be extract and get the record Count
                                ByteValidationPassedFiles.append(absPathofSourceFile)

                            elif reason ==  "FileSizeNotMatches":
                                logger.info("CHECKING  - ControlFileData [FileName-Size] [{0} - {1}] ==> BOTH_FILEZSIZES_ARE_NOT_MATCHING  with  SourceFile [FileName-Size] [{2} - {3}] ]".format(controlFileName, controlFileSize, controlFileName, SourcefileDict[controlFileName]))

                            elif reason ==  "FileNotAvailable":
                                logger.info("CHECKING  - ControlFileData [FileName-Size] [{0} - {1}] ==> FILE_NOT_AVAILABLE in Downloaded Path :: {2} ".format(controlFileName, controlFileSize,downloadedSourceFilesBasePathWithCurrentDate))

                        if len(ByteValidationPassedFiles) >0:
                            logger.info("s3- Valid ZipFiles :: {0}".format(ByteValidationPassedFiles))
                            for eachS3Zipfile in ByteValidationPassedFiles:
                                logger.info("getExtractedDirName :: {0} ::".format(getExtractedDirName(eachS3Zipfile)))
                                downloadedSourceFilesBasePathWithCurrentDate = downloadedSourceFilesBasePathWithCurrentDate+getExtractedDirName(eachS3Zipfile)
                                extractedFileList=  extractZipFile(eachS3Zipfile, downloadedSourceFilesBasePathWithCurrentDate)
                                logger.info("extractedFileList :: {0}".format(extractedFileList))
                                if len(extractedFileList) > 0:
                                    logger.info("Travesing through the unzip file and find the.gz files.. if exists then extracting that as well")
                                    getGzFiles = [gzFile for gzFile in extractedFileList if ".gz" in  gzFile]
                                    for eachGZfile in getGzFiles:
                                        logger.info("Found and sent for extraction :: {0}".format(eachGZfile))
                                        if untarGZfile(os.path.join(downloadedSourceFilesBasePathWithCurrentDate,eachGZfile)):
                                            logger.info("SUCCESS Extracted the .gz file:: {0}".format(eachGZfile))

                                            #AfterUnzip the Directory and its sub .gz Successfully then moving towards for those current extraction folder recordCount Vlaidations
                                            extractedDirNameSplit = extractedFileList[0].split("_")
                                            logger.info("extractedDirNameSplit :: {0}".format(extractedDirNameSplit))
                                            feedName = "_".join(extractedDirNameSplit[0:2])
                                            logger.info("feedName :: {0}".format(feedName))
                                            feedType = extractedDirNameSplit[-1]
                                            logger.info("feedType :: {0}".format(feedType))
                                            currentFeedValidationDirName = os.path.join(extractedFileList[0],feedName,feedType)
                                            logger.info("currentFeedValidationDirName :: {0}".format(currentFeedValidationDirName))
                                            print("#########################################################")
                                            feedRunner(currentFeedValidationDirName,downloadedSourceFilesBasePathWithCurrentDate)
                                            print("#########################################################")
                                        else:
                                            print("Failed Extracted the .gz file:: {0}".format(eachGZfile))
                                            logger.error("Failed Extracted the .gz file:: {0}".format(eachGZfile))

                    else:
                        print ('No ZIP files found inside the Source Directory :: {0}'.format(downloadedSourceFilesBasePathWithCurrentDate))
                        logger.error('No ZIP files found inside the Source Directory :: {0}'.format(downloadedSourceFilesBasePathWithCurrentDate))
                else:
                    print ('No Data found inside the Control File :: {0}'.format(controlFilePath))
                    logger.error('No Data found inside the Control File :: {0}'.format(controlFilePath))
        else:
            print("Not all files downloaded/copied to local..")
            logger.error("Not all files downloaded/copied to local..")
    # else:
    #     print("Failed to collect the Notification Messages ..")
    #     logger.error("Failed to collect the Notification Messages ..")
    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        sys.exit()


