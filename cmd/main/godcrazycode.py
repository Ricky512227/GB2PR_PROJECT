

def processControlfileContents(controlfileContent,RESULT):
	map = {}
	for did in RESULT.keys():
		for item in controlfileContent.keys():
			if did in item:
				if did not in  map.keys():
					map[did] = 1
				else:
					map[did] = map[did]+1

	for item in  map.keys():
		if map[item] >1:
			temp = 0 
			for mainKey, value in controlfileContent.items():
				if item in mainKey:
					temp = temp+value
					del controlfileContent[mainKey]		
			controlfileContent[".".join(mainKey.split(".")[:-1])] = temp
	return controlfileContent

RESULT = {	'Managed Refresh ASMT': '/MNT/VAR/FILEFROMS3/soruce/date/Managed Refresh ASMT20211102.zip',
			'Managed Refresh Deed': '/MNT/VAR/FILEFROMS3/soruce/date/Managed Refresh Deed20211102.zip.001',
			'Managed Update ASMT': '/MNT/VAR/FILEFROMS3/soruce/date/Managed Update ASMT20211102.zip.001',
			'Managed_Update _Deed': '/MNT/VAR/FILEFROMS3/soruce/date/Managed_Update _Deed20211102.zip'
		}


controlfileContent = {'Managed Refresh ASMT20211102.zip.001': 2369167417,
						'Managed Refresh ASMT20211102.zip.002': 2369167417,
						'Managed Refresh Deed20211102.zip.001': 3259771034,
						'Managed Update ASMT20211102.zip.001': 766,
						"Managed_Update _Deed20211102.zip": 730
					}
					
controlfileContent = processControlfileContents(controlfileContent,RESULT)
print ("Before controlfileContent :: ")
for item,key in controlfileContent.items():
	print (item,key)
