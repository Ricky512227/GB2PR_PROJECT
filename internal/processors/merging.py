import os
import sys
import glob
import re

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from pkg.common.CommonUtlity import executeCmd
from internal.monitors.logMonitor import logger

def getSourceType(tmp_source_path):
	"""
		Function to find/extract the getSourceType from the given source file path 
			1. Example :: tmp_source_path = /mnt/var/FilesFromS3/SourceFiles/202201121925/Managed_Refresh_ASMT20211102.zip
			2. returns :: Managed_Refresh_DEED/Managed_Refresh_ASMT/Managed_Update_ASMT/Managed_Update_DEED
		Parameters :: tmp_source_path : str
		Returns :: a string of source type 
	"""
	try:
		source_type = r"(Managed_[^>]*?_[^>]*?)[\d]+\.zip"
		source_type = re.findall(source_type, str(tmp_source_path))[0]
		return source_type
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))

def combinePartitionedFiles(partioned_files):
	"""
		Function to collect the identified partitioned zip files and Merged them as one zip file. 
		Example Cmd :: cat test.zip* > ~/test.zip
		Parameters :: partioned_files : list
		Returns :: a merged zip file path
	"""
	try:
		logger.info("Received partitioned files and MERGING -- [IN-PROCESS ]..")
		merged_fileName = ".".join(partioned_files[0].split(".")[:-1])
		#combineZipFileCmd = "cat "+ ".".join(partioned_files[0].split(".")[:-1]) +"* > "
		# returnCode, cmdData = executeCmd(combineZipFileCmd)
		# if returnCode == 0:
		# 	logger.info("MERGING is [SUCCESS]...")
		# else:
		# 	logger.info("MERGING is [FAILED]...")
		return merged_fileName
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))

def findPartitionedFiles(dataset_id,partioned_files):
	"""
		Function to find the partitioned zip files from the given list of files 
		Parameters :: 
			partioned_files : list of strings
			dataset_id : str
		Returns :: a merged zip file path
	"""
	try:
		if len(partioned_files) > 1 :
			print("Partitioned files [FOUND]  for  DataSet :: [{0}]".format(dataset_id))
			# Merging the partitioned file into Single File
			reveived_mergedFileName = combinePartitionedFiles(partioned_files)
		else:
			print("Partitioned files [NOT_FOUND] for  DataSet :: [{0}]".format(dataset_id))
			reveived_mergedFileName = partioned_files[0]
		return reveived_mergedFileName
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))



def getSourceTypeandSourceFileMap(zip_filepath):
	"""
		Function to map/dict the source name to source map.
		Parameters :: partioned_files : list of strings
		Reutrns Example :: {'Managed_Refresh_DEED': lisofcorrespondingsourceFiles, 'Managed_Refresh_ASMT': lisofcorrespondingsourceFiles,'Managed_Update_ASMT': lisofcorrespondingsourceFiles,'Managed_Update_DEED': lisofcorrespondingsourceFiles,}
		Returns :: a Dict/Map of SourceName and its list of files.
	"""
	try:
		source_zipfiles = glob.glob(os.path.join(zip_filepath, "*"))
		mapOf_source_id_file = {}
		for source_zipfile in source_zipfiles:
			souce_zipfile_key = getSourceType(source_zipfile)
			if souce_zipfile_key not in mapOf_source_id_file.keys():
				mapOf_source_id_file[souce_zipfile_key] = []
				mapOf_source_id_file[souce_zipfile_key].append(source_zipfile)
			else:
				mapOf_source_id_file[souce_zipfile_key].append(source_zipfile)
		return mapOf_source_id_file
	except Exception as ex:
		print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
		logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))



	"""
	After, all the source files are successfully downloaded from the s3 bucket to local. 
	Then check,
		1. If there are multiple/partitioned files are present for each source id.
		2. If exists, then merged them as single file else continue the process.
	Once the above process is done, then return files to next step.
	"""
zip_filepath = r"C:\Users\kamalsai\Desktop\GB2PR_Project\CombinedTest"
result_idfile_map = getSourceTypeandSourceFileMap(zip_filepath)
for dataset_id, partioned_files in result_idfile_map.items():
	reveived_mergedFileName = findPartitionedFiles(dataset_id,partioned_files)
	result_idfile_map[dataset_id] = reveived_mergedFileName

print(result_idfile_map)

