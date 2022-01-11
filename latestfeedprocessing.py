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

        listofS3Urls = list()
        for s3_prefix in receivedS3Prefixs:
            listofS3Urls.append('s3://{}/{}'.format(received_S3BucketName, s3_prefix))

        
        
        # for s3prefix in receivedS3Prefix:
        #     if s3prefix.split("/")[3] ==  controllerDataSetId:
        #         tmps3Prefix = "/".join(s3prefix.split("/")[:-1])
        #         controlMessageObjPath = constructS3PathForNotificationMsg(controlType, monitoringBucketName,tmps3Prefix)
        #     elif  s3prefix.split("/")[3] in [refreshAsmtDataSetId,refreshDeedDataSetId,updateAsmtDataSetId,updateDeedDataSetId]:
        #         tmps3Prefix = "/".join(s3prefix.split("/")[:-1])
        #         listOfSoruceMessageObjPaths = [constructS3PathForNotificationMsg(sourceType,monitoringBucketName,tmps3Prefix)]

        # """1.Initialising the search for the notification messages
        #     2.Based on the monitoring interval if will monitor and search for the message file if its not exists.
        #     3.If all the messaged file exists then it will exit from the monitoring process """

        # print ("Initiated search to fetch the Notification Messages ....")
        # isAllMessageReceived = False
        # while True:
        #     #Search the control file obj and its file based on the targertdate which was passed in sys args
        #     if not searchControlNotificationMsgFile(controlType,controlMessageObjPath):
        #         print("Sleeping for {0}secs".format(monitoringInterval))
        #         time.sleep(monitoringInterval)
        #     else:
        #         #Search the source objs and its files based on the targertdate which was passed in sys args
        #         if searchSourceNotificationMsgFile(sourceType, monitoringInterval,listOfSoruceMessageObjPaths):
        #             isAllMessageReceived = True
        #             print ("All Messages are Arrived .... Stopping the Monitor...")
        #             break

        # currentTimeStamp = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
        # #Setting the path  for target location for notification message files [FilesFromS3/NotificationMessages/{CurrentTimStamp}].
        # downloadedNotificationMessageFilesBasePath = os.path.join(ROOT_DIR,"FilesFromS3","NotificationMessages")
        # logger.info("downloadedNotificationMessageFilesBasePath :: {0}".format(downloadedNotificationMessageFilesBasePath))
        # downloadedNotificationMessageFilesBasePathWithCurrentDate = os.path.join(downloadedNotificationMessageFilesBasePath,currentTimeStamp)
        # logger.info("downloadedNotificationMessageFilesBasePathWithCurrentDate :: {0}".format(downloadedNotificationMessageFilesBasePathWithCurrentDate))
        # if not os.path.exists(downloadedNotificationMessageFilesBasePathWithCurrentDate):
        #     logger.info("Directory not exists, Creating New Directory :: {0}".format(downloadedNotificationMessageFilesBasePathWithCurrentDate))
        #     os.makedirs(downloadedNotificationMessageFilesBasePathWithCurrentDate)

        currentTimeStamp = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
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

        #IF all the message files are received then , copy the message and get the read the S3URLS
        # if isAllMessageReceived :
        #     failedUrlsDataSetCount = 0
        #     listofS3Urls = []
        #     for notificationMessageS3Obj in listOfSoruceMessageObjPaths+controlMessageObjPath.split(","):
        #         notificationMessageS3FileName = datetime.datetime.strptime(targetDate, '%Y-%m-%d').strftime('%Y%m%d')+"*.json"
        #         notificationMessageS3FilePath = os.path.join(notificationMessageS3Obj,notificationMessageS3FileName)
        #         print ("Received notificationMessageS3Obj :: {0} :: {1}".format(notificationMessageS3Obj, type(notificationMessageS3Obj)))
        #         targetFileName = "NotificationMessage_"+notificationMessageS3Obj.split("/")[6]+".json"
        #         targetFielPath = os.path.join(downloadedNotificationMessageFilesBasePathWithCurrentDate,targetFileName)
        #         print("Received targetFielPath :: {0}".format(targetFielPath))
        #         #Cmd to download the file to local from s3
        #         cmdToGetS3FileCopyToLocal = "hdfs dfs -copyToLocal"+" "+notificationMessageS3FilePath+" "+ targetFielPath
        #         returnCode, cmdData = executeCmd(cmdToGetS3FileCopyToLocal)
        #         if returnCode ==0:
        #             #Using the downloaded message.json fetching the s3 url. 
        #             tempMessageData = readJsonFile(targetFielPath)
        #             s3Url = getS3UrlsFromJsonData(tempMessageData)
        #             if  s3Url is not None:
        #                 logger.info("Checking URLS ==> {0} :: URLS_FOUND".format(notificationMessageS3FileName))
        #                 listofS3Urls.append(s3Url)		
        #             else:
        #                 logger.info("Checking URLS ==> {0} :: URLS_NOT_FOUND".format(notificationMessageS3FileName))
        #                 failedUrlsDataSetCount = failedUrlsDataSetCount+1
        #         else:
        #             #logging the error message
        #             logger.error("File {0} Failed to  Download/Copy to Local :: {1}\nReason :: {2}".format(notificationMessageS3FileName, targetFielPath, cmdData))

            ## TODO: 
            # Download the files based on the inputs through s3_bucket and s3_prefixes
            
            failedDownloadedCount = 0
            # #Once all the s3URLs are fetched then download the controller files and source files to local.
            # if failedUrlsDataSetCount == 0:
            #     failedDownloadedCount = 0 
            #     #Downloading/Copying the source, controller files from s3
            for s3Url in listofS3Urls:
                _, tail = os.path.split(s3Url)
                if tail.startswith("BKI"):
                    targetLocation = downloadedControlFileBasePathWithCurrentDate+"/"
                else:
                    targetLocation = downloadedSourceFilesBasePathWithCurrentDate+"/"
                print ("targetLocation :: {0}".format(targetLocation))
                #Copy file from s3 to local target path
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
                                logger.info('SOURCE_FILE = {}'.format(zfile))
                                if ".zip.001" in zfile:
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
                                        extractedFileList=  extractZipFile(eachS3Zipfile, downloadedSourceFilesBasePathWithCurrentDate)
                                        if len(extractedFileList) > 0:
                                            logger.info("Travesing through the unzip file and find the.gz files.. if exists then extracting that as well")
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
                                                    print("Failed Extracted the .gz file:: {0}".format(eachGZfile))
                                                    logger.info("Failed Extracted the .gz file:: {0}".format(eachGZfile))

                            else:
                                print ('No ZIP files found inside the Source Directory :: {0}'.format(downloadedSourceFilesBasePathWithCurrentDate))
                                logger.error('No ZIP files found inside the Source Directory :: {0}'.format(downloadedSourceFilesBasePathWithCurrentDate))
                        else:
                            print ('No Data found inside the Control File :: {0}'.format(controlFilePath))
                            logger.error('No Data found inside the Control File :: {0}'.format(controlFilePath))
                else:
                    print("Not all files downloaded/copied to local..")
                    logger.error("Not all files downloaded/copied to local..")
            else:
                print("Failed to collect the Notification Messages ..")
                logger.error("Failed to collect the Notification Messages ..")
    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        sys.exit()



