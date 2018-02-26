# stitching 4 benefits together, finding corrresponding BEl and summary statistics per unique life id.


import pandas as pd
import numpy as np
import os

run_name = '0.repricing-run02'   
# run_name = '0.repricing-run06'


policy_subset = 'ordinary-cover'
# policy_subset = 'super-cover'

#indicator switch for extra columns, ordinary policies at the moment
ind_extra_col = True

# if extra columns are used, please add extra columns names to the list extra_col
if ind_extra_col == True:
	extra_col = ['BE_GRRESERVE'] # ,  'PV_GR_CARR']


PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data', policy_subset)
RESULT = os.path.join(PROJ, 'results', policy_subset)


f_run_name = run_name[-5:]#last 5 letters
print f_run_name

fpath_res = os.path.join(RESULT, 'intermediate-' + f_run_name)
print fpath_res


def as_float(value):
	try:
		float(value)
		return float(value)
	except:
		return np.nan

def as_integer(value):
	try:
		int(value)
		return int(value)
	except:
		return np.nan

# rename cols and keep structure, no aggreg
def return_result_2(data):
	data['SA'] = data['SUM_ASSURED'] # same to: data['SA'] = data.groupby(aggregations_on_string)['SUM_ASSURED'].transform(lambda x:x)
	data['BASE_PREM'] = data['B_OFF_APREM']
	data['A_PREM1'] = data['ANNUAL_PREM_1']
	data['A_PREM_13'] = data['ANNUAL_PREM_13']
	data['A_PREM_25'] = data['ANNUAL_PREM_25']
	data['BEL'] = data['BE_RESERVE']
	data['POL_FEE'] = data['POLICY_FEE']
	# data = data.drop_duplicates(['L_LIFE_ID'])
	
	# if ind_extra_col == True:
	# 	for i in extra_col:
	# 		data[i] =  data[i]

	data = data.drop(['B_OFF_APREM', 'BE_RESERVE',
				'ANNUAL_PREM_1', 'ANNUAL_PREM_13', 'ANNUAL_PREM_25'],
												axis = 1)
	data = data.drop(['SUM_ASSURED', 'POLICY_FEE'], axis = 1)
	

	return(data)
# _____________________________________________________________________________________________

print ('start of program')
# sets cwd to location of the script
os.chdir(os.path.dirname(__file__))

# Reading files
death_cmb = pd.read_csv(fpath_res + '\\' + 'Death' + run_name[-2:] + '.csv')
tpd_cmb = pd.read_csv(fpath_res + '\\' + 'TPD' + run_name[-2:] + '.csv')
ip_cmb = pd.read_csv(fpath_res + '\\' + 'Income secure' + run_name[-2:] + '.csv')

# Creating columns, no aggregation is done
death_cmb = return_result_2(death_cmb)
tpd_cmb = return_result_2(tpd_cmb)
ip_cmb = return_result_2(ip_cmb)
# to get binning of SA IP, because LS has different distribution of bins
ip_cmb.rename(columns = {'SA':'SA_IP'}, inplace = True)

# Creating column to show where benefit came from  
death_cmb['Death'] = 'LIFE'
tpd_cmb['TPD'] = 'TPD'
ip_cmb['IP'] = 'IP'

print ip_cmb.info()


# print ip_cmb['ANNUAL_PREM_1'].sum()



death_cmb['AGE'] = (death_cmb.AGE_AT_ENTRY + death_cmb.DURATIONIF_M/12).round()
tpd_cmb['AGE'] = (tpd_cmb.AGE_AT_ENTRY + tpd_cmb.DURATIONIF_M/12).round()
ip_cmb['AGE'] = (ip_cmb.AGE_AT_ENTRY + ip_cmb.DURATIONIF_M/12).round()


death_cmb = death_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)
tpd_cmb = tpd_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)
ip_cmb = ip_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)
ip_cmb.drop(['B_BEN_NO','MOS_PV_PREM'], axis = 1, inplace = True)

if os.path.exists(os.path.join(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv')) == True:
		tra_cmb = pd.read_csv(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv')
		tra_cmb['L_LIFE_ID'] = tra_cmb['L_LIFE_ID'].astype(int) 
		# # cleaning data_______________________
		if type(tra_cmb.ix[0,'POL_NUMBER']) == str:
			tra_cmb.ix[:,'POL_NUMBER'] = [x.replace('"', '') for x in tra_cmb.ix[:,'POL_NUMBER']]
			tra_cmb.ix[:,'POL_NUMBER'] = [as_integer(x) for x in tra_cmb.ix[:, 'POL_NUMBER']]
		tra_cmb['POL_NUMBER'] = tra_cmb['POL_NUMBER'].astype(int)

		tra_cmb = return_result_2(tra_cmb)
		tra_cmb['TRA'] = 'TRA'
		tra_cmb['AGE'] = (tra_cmb.AGE_AT_ENTRY + tra_cmb.DURATIONIF_M/12).round()
		
		# tra_cmb['Mnth mod'] = tra_cmb.DURATIONIF_M % 12
		# tra_cmb['Mnth Inception'] = pd.cut(tra_cmb['Mnth mod'], bins_mnth, labels = group_names_mnth)
		tra_cmb = tra_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)

		
		result_frame = pd.concat([death_cmb, tra_cmb, tpd_cmb, ip_cmb])

else:
		result_frame = pd.concat([death_cmb, tpd_cmb, ip_cmb])


print result_frame.info()



result_frame.reset_index(drop = True)
print ('merged into one frame')
print result_frame.info()

print ('finished modifications')
# print('Att to check:')
# # print ip_cmb.info()
# # print tpd_cmb.info()
# # print tra_cmb.info()



print ('before adding age bins')
bins_age = [0, 31, 36, 41,  46, 51, 56, 61, 130]
group_names_age = ['30 & below', '31 - 35', '36 - 40','41 - 45','46 - 50','51 - 55', '56 - 60', '61 & above']


bins_ls = [0, 99999, 249999, 499999, 749999, 999999, 1499999, 1999999, 4999999,1000000000]
group_names_ls = ['Below $100K', '$100K - $250K', '$250K - $500K', '$500K - $750K', '$750K-$1M',
					'$1M - $1.5M','$1.5M - $2M', '$2M - $5M', '$5M & above']

bins_ip = [0, 999, 2499, 4999, 7499, 9999, 5000000]
group_names_ip = ['Below $1K', '$1K - $2.5K', '$2.5K - $5K', '$5K - $7.5K', '$7.5K - $10K',
					'$10K & above']

result_frame['AGE-GROUP'] = pd.cut(result_frame['AGE'], bins_age, labels = group_names_age)
result_frame['SI_LS_BAND'] = pd.cut(result_frame['SA'], bins_ls, labels = group_names_ls)
result_frame['SI_IP_BAND'] = pd.cut(result_frame['SA_IP'], bins_ip, labels = group_names_ip)
result_frame['ENTRY_DATE'] = result_frame.apply(lambda x: '01/' + str(x['ENTRY_MONTH']) +'/'+ str(x['ENTRY_YEAR']), axis = 1)



cols = result_frame.columns.tolist()
print result_frame.columns.values
# result_frame.to_csv(r'C:\\galati_files\\pyscripts\\callo-repricing\\compare-runs\\Resv0.csv')

if policy_subset == 'ordinary-cover':
	cols = ['L_LIFE_ID','POL_NUMBER', 'SEX', 'SMOKER_IND', 'AGE', 'AGE-GROUP', 'TOTAL_SI', 
		'Death', 'TRA', 'TPD','IP', 'ENTRY_MONTH', 'ENTRY_YEAR', 'ENTRY_DATE',
		'SA', 'SA_IP',
		'A_PREM1', 'A_PREM_13', 'A_PREM_25', 'BEL',
		'BENEFIT_CODE', 'BEN_PERIOD', 'B_BEN_NO', 'PREMIUM_TYPE',  
		'DEFER_PER_MP', 'DII_TYPE', 'OCC_CLASS',
		'TOTAL_SI', 'OTR_ANNPHIBEN', 'DCS_REIN_SI','CHANNEL', 'BROKER', 
		 'SI_LS_BAND', 'SI_IP_BAND', 
		 ]
else:
	cols = ['L_LIFE_ID','POL_NUMBER', 'SEX', 'SMOKER_IND', 'AGE', 'AGE-GROUP', 'TOTAL_SI', 
		'Death', 'TPD','IP', 'ENTRY_MONTH', 'ENTRY_YEAR',
		'SA', 'SA_IP', 
		'A_PREM1', 'A_PREM_13', 'A_PREM_25', 'BEL',
		'BENEFIT_CODE', 'BEN_PERIOD', 'B_BEN_NO', 'PREMIUM_TYPE',  
		'DEFER_PER_MP', 'DII_TYPE', 'OCC_CLASS',
		'TOTAL_SI', 'OTR_ANNPHIBEN', 'DCS_REIN_SI','CHANNEL', 'BROKER', 
		 'SI_LS_BAND', 'SI_IP_BAND']

# for ord policies
if ind_extra_col == True:
	cols.extend(extra_col)



result_frame = result_frame[cols]

#Save output to dataframe
fpath_res = os.path.join(RESULT, 'final')

if os.path.exists(fpath_res) == False:
	 os.makedirs(fpath_res)

result_frame.to_csv(fpath_res + '\\' +'Result_' + f_run_name + '_byLine.csv')
print 'file saved to: ', fpath_res
print ('end of script')
