import os
import sys
import glob
import re
from .CommonUtlity import executeCmd
from .logMonitor import logger

def getSourceType(tmp_source_path):
    """
        Function to find/extract the getSourceType from the given source file path
    """
    try:
        source_type = r"(Managed_[^>]*?_[^>]*?)[\d]+\.zip"
        source_type = re.findall(source_type, str(tmp_source_path))[0]
        return source_type
    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))

def combinePartitionedFiles(partioned_files):
    try:
        logger.info("Received partitioned files and MERGING -- [IN-PROCESS ]..")
        merged_fileName = ".".join(partioned_files[0].split(".")[:-1])
        return merged_fileName
    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))

def findPartitionedFiles(dataset_id,partioned_files):
    try:
        if len(partioned_files) > 1 :
            print("Partitioned files [FOUND]  for  DataSet :: [{0}]".format(dataset_id))
            reveived_mergedFileName = combinePartitionedFiles(partioned_files)
        else:
            print("Partitioned files [NOT_FOUND] for  DataSet :: [{0}]".format(dataset_id))
            reveived_mergedFileName = partioned_files[0]
        return reveived_mergedFileName
    except Exception as ex:
        print("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))
        logger.error("Error occurred :: {0}\tLine No:: {1}".format(ex, sys.exc_info()[2].tb_lineno))


def getSourceTypeandSourceFileMap(zip_filepath):
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


if __name__ == '__main__':
    zip_filepath = os.path.join(os.path.expanduser('~'), 'gb2pr_test_combined')
    result_idfile_map = getSourceTypeandSourceFileMap(zip_filepath)
    for dataset_id, partioned_files in result_idfile_map.items():
        reveived_mergedFileName = findPartitionedFiles(dataset_id,partioned_files)
        result_idfile_map[dataset_id] = reveived_mergedFileName

    print(result_idfile_map)
