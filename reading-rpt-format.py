import pandas as pd
import os


# PROJ = 'A:\\Actuarl\\PRICING\\2017\\Repricing\\Business Mix Analysis\\Repricing_python\\PROJ'
# print PROJ
PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data')
RESULT = os.path.join(PROJ, 'results')

def convert_Prophet_tocsv():
	filepath = os.path.join(PROJ, DATA, 'converting to csv')
	print filepath

	# saving files RPT as csv
	for filename in os.listdir(os.path.join(filepath, 'rpt')):
		if filename[:3] == 'PAO':
			df = pd.read_table(os.path.join(filepath, 'rpt') + '\\' +\
				filename, skiprows = [0,1,2,3], sep = ',', skipinitialspace = True)
			df.to_csv(filepath + '\\' + filename[:6] + '.csv', index = False)
		elif filename[:3] == 'COC':
			df = pd.read_table(os.path.join(filepath, 'rpt') + '\\' +\
				filename, skiprows = [0,1,2,3,4], sep = ',', skipinitialspace = True)
			df.to_csv(filepath + '\\' + filename[:6] + '.csv', index = False)

	# saving files PRO as csv
	for filename in os.listdir(os.path.join(filepath, 'PRO')):
		if filename[:3] == 'PAO':
			df = pd.read_table(os.path.join(filepath, 'PRO') + '\\' +\
				filename, skiprows = [0,1,2], sep = ',', skipinitialspace = True)
			df.to_csv(filepath + '\\' + filename[:6] + '.PRO.csv', index = False)
		elif filename[:3] == 'COC':
			df = pd.read_table(os.path.join(filepath, 'PRO') + '\\' +\
				filename, skiprows = [0,1,2,3], sep = ',', skipinitialspace = True)
			df.to_csv(filepath + '\\' + filename[:6] + '.PRO.csv', index = False)

	return 

convert_Prophet_tocsv()
