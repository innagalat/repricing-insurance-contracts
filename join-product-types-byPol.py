# stitching 4 benefits together, finding corrresponding BEl and summary statistics per unique life id.
import pandas as pd
import numpy as np
import os
from sets import Set
import sys
from datetime import datetime 

# run_name = '0.repricing-run02'   
run_name = '0.repricing-run06'

policy_subset = 'super-cover'
# policy_subset = 'ordinary-cover'

PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data', policy_subset)
RESULT = os.path.join(PROJ, 'results', policy_subset)

# DATA = os.path.join(PROJ, 'data', 'super-cover')
# RESULT = os.path.join(PROJ, 'results\\super-cover')

f_run_name = run_name[-5:]#last 5 letters
print f_run_name

fpath_res = os.path.join(RESULT, 'intermediate-' + f_run_name)
print fpath_res


def get_file_path(foldname, filename):
	cwd = os.getcwd()
	filepath = os.path.join(cwd, foldname, filename)
	return(filepath)

def read_if_exists(fpath):
	if os.path.exists(fpath) == True:
		df = pd.read_csv(fpath)
		return(df)
	else:
		print 'File ' + fpath + ' not found'
		exit(1)

def read_and_slice(nfold, nfile, col_list):
	path = get_file_path(nfold, nfile)
	df = read_if_exists(path)
	df = df.ix[:, col_list]
	return(df)

def merge_by_id(df_a, df_b):
	return(pd.merge(df_a, df_b, how = 'outer', 
			on = ['POL_NUMBER','SUM_ASSURED','B_BEN_NO','AGE_AT_ENTRY']))

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


def define_par_per_life1(col_name, col_list):
	result_frame[col_name] = result_frame[col_list].max(axis =1, skipna = True)
	result_frame.drop(col_list, axis = 1, inplace = True)
	return(result_frame)

def aggr_by_id(data, aggregations_on_string):
	data['SA'] = data.groupby(aggregations_on_string)['SUM_ASSURED'].transform('sum')
	data['BASE_PREM'] = data.groupby(aggregations_on_string)['B_OFF_APREM'].transform('sum')
	data['A_PREM'] = data.groupby(aggregations_on_string)['ANNUAL_PREM_1'].transform('sum')
	data['A_PREM_13'] = data.groupby(aggregations_on_string)['ANNUAL_PREM_13'].transform('sum')
	data['BEL'] = data.groupby(aggregations_on_string)['BE_RESERVE'].transform('sum')
	data = data.drop_duplicates([aggregations_on_string])
	data = data.drop(['B_OFF_APREM', 'BE_RESERVE','SUM_ASSURED',
					'ANNUAL_PREM_1', 'ANNUAL_PREM_13'], axis = 1)
	data = data.drop('POL_NUMBER', axis=1)
	return(data)


# # _________________________
# old_f = sys.stdout
# class F:
#     def write(self, x):
#         old_f.write(x.replace("\n", " [%s]\n" % str(datetime.now())))
# sys.stdout = F()
# # ___________________________

print ('start of program')
# sets cwd to location of the script
os.chdir(os.path.dirname(__file__))


death_cmb = pd.read_csv(fpath_res + '\\' + 'Death' + run_name[-2:] + '.csv',
						dtype = {'L_LIFE_ID':np.int32, 'POL_NUMBER': np.int32})
tpd_cmb = pd.read_csv(fpath_res + '\\' + 'TPD' + run_name[-2:] + '.csv',
						dtype = {'L_LIFE_ID':np.int32, 'POL_NUMBER': np.int32})
#tpd_cmb['L_LIFE_ID'] = tpd_cmb['L_LIFE_ID'].astype(int)
#tpd_cmb['POL_NUMBER'] = tpd_cmb['POL_NUMBER'].astype(int) 

ip_cmb = pd.read_csv(fpath_res + '\\' + 'Income secure' + run_name[-2:] + '.csv',
						dtype = {'L_LIFE_ID':np.int32, 'POL_NUMBER': np.int32})


# __________________________
def create_pol_l_id_identif(data):
	data['POL_L_ID'] = data.apply(
								lambda x:'%s%s' % (x['POL_NUMBER'], x['L_LIFE_ID']),
								axis=1)
	return data

death_cmb = create_pol_l_id_identif(death_cmb)
tpd_cmb = create_pol_l_id_identif(tpd_cmb)
ip_cmb = create_pol_l_id_identif(ip_cmb)



# ____________________________________________

death_cmb = aggr_by_id(death_cmb, 'POL_L_ID' )
tpd_cmb = aggr_by_id(tpd_cmb, 'POL_L_ID')
ip_cmb = aggr_by_id(ip_cmb, 'POL_L_ID')

death_cmb['Death'] = 'LIFE'
tpd_cmb['TPD'] = 'TPD'
ip_cmb['IP'] = 'IP'



death_cmb['AGE'] = (death_cmb.AGE_AT_ENTRY + death_cmb.DURATIONIF_M/12).round()
tpd_cmb['AGE'] = (tpd_cmb.AGE_AT_ENTRY + tpd_cmb.DURATIONIF_M/12).round()
ip_cmb['AGE'] = (ip_cmb.AGE_AT_ENTRY + ip_cmb.DURATIONIF_M/12).round()
death_cmb = death_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)
tpd_cmb = tpd_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)
ip_cmb = ip_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)
ip_cmb.drop(['B_BEN_NO','MOS_PV_PREM'], axis = 1, inplace = True)


if os.path.exists(os.path.join(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv')) == True:
	tra_cmb = pd.read_csv(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv')
	tra_cmb = tra_cmb.dropna(axis = 0)
	tra_cmb['L_LIFE_ID'] = tra_cmb['L_LIFE_ID'].astype(int) 
			# # cleaning data_______________________
	if type(tra_cmb.ix[0,'POL_NUMBER']) == str:
			tra_cmb.ix[:,'POL_NUMBER'] = [x.replace('"', '') for x in tra_cmb.ix[:,'POL_NUMBER']]
			tra_cmb.ix[:,'POL_NUMBER'] = [as_integer(x) for x in tra_cmb.ix[:, 'POL_NUMBER']]
	tra_cmb['POL_NUMBER'] = tra_cmb['POL_NUMBER'].astype(int) 

	tra_cmb = create_pol_l_id_identif(tra_cmb)
	tra_cmb = aggr_by_id(tra_cmb, 'POL_L_ID' )
	tra_cmb['TRA'] = 'TRA'
	tra_cmb['AGE'] = (tra_cmb.AGE_AT_ENTRY + tra_cmb.DURATIONIF_M/12).round()
	tra_cmb = tra_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)

print ('finished modifications')


def create_uniq_identif_keycol(unique_ident_str):
	# creating lists of unique life_id
	id_1 = death_cmb[unique_ident_str].unique() #DON'T REALLY USE id_1, because use a whole death_cmb
	id_2 = ip_cmb[unique_ident_str].unique()
	id_3 = tpd_cmb[unique_ident_str].unique()
	
	# creating df with unique life_id
	id_frame_2 = pd.DataFrame(columns = [unique_ident_str]) # 'col1', 'col2', 'col3','col4','col5','col6','col7'
	id_frame_2[unique_ident_str] = id_2
	id_frame_3 =pd.DataFrame(id_3, columns= [unique_ident_str])

	if os.path.exists(os.path.join(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv')) == True:
		id_4 = tra_cmb[unique_ident_str].unique()
		id_frame_4 =pd.DataFrame(id_4, columns= [unique_ident_str])
		result_frame = pd.concat([death_cmb, id_frame_2, id_frame_3, id_frame_4])
	else: result_frame = pd.concat([death_cmb, id_frame_2, id_frame_3])
	
	result_frame = result_frame.drop_duplicates([unique_ident_str])
	return result_frame

result_frame = create_uniq_identif_keycol('POL_L_ID')


def merging_based_on_ident(df_frame,unique_ident_str):
	if os.path.exists(os.path.join(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv')) == True:
			df_frame = pd.merge(df_frame, tpd_cmb, on = unique_ident_str, how ='left', suffixes=('_LIFE','_TPD'))
			df_frame = pd.merge(df_frame, tra_cmb, on = unique_ident_str, how ='left')
			df_frame = pd.merge(df_frame, ip_cmb, on = unique_ident_str, how ='left', suffixes=('_TRA', '_IP'))
			df_frame = df_frame.rename(columns= {'DCS_REIN_SI':'DCS_REIN_SI_TRA',
							'B_BEN_NO':'B_BEN_NO_TRA', 'BENEFIT_CODE':'BENEFIT_CODE_IP',
							'DEFER_PER_MP':'DEFER_PER_MP_IP','BEN_PERIOD':'BEN_PERIOD_IP', 
							'OCC_CLASS':'OCC_CLASS_IP', 'OTR_ANNPHIBEN':'OTR_ANNPHIBEN_IP'})
			if unique_ident_str == 'POL_NUMBER':
				df_frame.drop(['L_LIFE_ID_TRA', 'L_LIFE_ID_TPD', 'L_LIFE_ID_IP', 'L_LIFE_ID_LIFE'],
											axis = 1, inplace = True)
	else:
			df_frame = pd.merge(df_frame, tpd_cmb, on = unique_ident_str, how ='left', suffixes=('_LIFE', '_TPD'))
			df_frame = pd.merge(df_frame, ip_cmb, on = unique_ident_str, how ='left',  suffixes=('','_IP'))
			df_frame = df_frame.rename(columns= {'SEX':'SEX_IP', 'SMOKER_IND':'SMOKER_IND_IP',
													'PREMIUM_TYPE':'PREMIUM_TYPE_IP', 'L_LIFE_ID':'L_LIFE_ID_IP',
													'BROKER':'BROKER_IP', 'CHANNEL':'CHANNEL_IP',
													'SA':'SA_IP', 'BASE_PREM':'BASE_PREM_IP', 'A_PREM':'A_PREM_IP',
													'A_PREM_13':'A_PREM_13_IP', 'BEL':'BEL_IP', 'AGE':'AGE_IP',
													'BENEFIT_CODE':'BENEFIT_CODE_IP', 'DEFER_PER_MP':'DEFER_PER_MP_IP',
													'BEN_PERIOD':'BEN_PERIOD_IP', 'OCC_CLASS':'OCC_CLASS_IP',
													'OTR_ANNPHIBEN':'OTR_ANNPHIBEN_IP'})
			if unique_ident_str == 'POL_NUMBER':
				df_frame.drop(['L_LIFE_ID_TPD', 'L_LIFE_ID_IP', 'L_LIFE_ID_LIFE'],
											axis = 1, inplace = True)
	df_frame.reset_index(drop = True)
	return df_frame

result_frame = merging_based_on_ident(result_frame, 'POL_L_ID')



#exporting L_life id and pol number
print result_frame['POL_L_ID'].tail(5)

def extract_id_n_pol():
	result_frame['L_LIFE_ID'] = result_frame['POL_L_ID'].astype(str).str[-8:]
	result_frame['POL_NUMBER'] = result_frame['POL_L_ID'].astype(str).str[0:10]
	return result_frame 

result_frame = extract_id_n_pol()


print ('merged into one frame')


def total_figures_sum(df_frame):
	df_frame['Death'] = df_frame['Death'].fillna('')
	df_frame['TPD'] = df_frame['TPD'].fillna('')
	df_frame['IP'] = df_frame['IP'].fillna('')

	if os.path.exists(os.path.join(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv')) == True:

		df_frame.ix[:,'TOT_BEL'] = df_frame.ix[:,'BEL_LIFE'].fillna(0) +\
										df_frame.ix[:,'BEL_IP'].fillna(0) +\
										df_frame.ix[:,'BEL_TPD'].fillna(0) +\
										df_frame.ix[:,'BEL_TRA'].fillna(0)
		df_frame.ix[:,'TOT_ANN_PREM'] = df_frame.ix[:,'A_PREM_LIFE'].fillna(0) +\
										df_frame.ix[:,'A_PREM_IP'].fillna(0) +\
										df_frame.ix[:,'A_PREM_TPD'].fillna(0) +\
										df_frame.ix[:,'A_PREM_TRA'].fillna(0)
		df_frame.ix[:,'TOT_PREM_13'] = df_frame.ix[:,'A_PREM_13_LIFE'].fillna(0) +\
										df_frame.ix[:,'A_PREM_13_IP'].fillna(0) +\
										df_frame.ix[:,'A_PREM_13_TPD'].fillna(0) +\
										df_frame.ix[:,'A_PREM_13_TRA'].fillna(0)
		df_frame['TRA'] = df_frame['TRA'].fillna('')
		df_frame['PACKAGE'] = df_frame['Death'].astype(str) + ' ' + df_frame['TRA'].astype(str) +\
							' ' + df_frame['TPD'].astype(str) + ' ' + df_frame['IP'].astype(str)
		df_frame['PACKAGE'] = df_frame['PACKAGE'].fillna(0)
		df_frame = df_frame.drop(['Death', 'TRA', 'TPD', 'IP'], axis =1)

	else:

		df_frame.ix[:,'TOT_BEL'] = df_frame.ix[:,'BEL_LIFE'].fillna(0) +\
										df_frame.ix[:,'BEL_IP'].fillna(0) +\
										df_frame.ix[:,'BEL_TPD'].fillna(0)
		df_frame.ix[:,'TOT_ANN_PREM'] = df_frame.ix[:,'A_PREM_LIFE'].fillna(0) +\
										df_frame.ix[:,'A_PREM_IP'].fillna(0) +\
										df_frame.ix[:,'A_PREM_TPD'].fillna(0)
		df_frame.ix[:,'TOT_PREM_13'] = df_frame.ix[:,'A_PREM_13_LIFE'].fillna(0) +\
										df_frame.ix[:,'A_PREM_13_IP'].fillna(0) +\
										df_frame.ix[:,'A_PREM_13_TPD'].fillna(0)
		df_frame['PACKAGE'] = df_frame['Death'].astype(str) + ' ' + df_frame['TPD'].astype(str) +\
						' ' + df_frame['IP'].astype(str)
		df_frame['PACKAGE'] = df_frame['PACKAGE'].fillna(0)
		df_frame = df_frame.drop(['Death', 'TPD', 'IP'], axis =1)

	return df_frame

result_frame = total_figures_sum(result_frame)



def adding_bins(df_frame):
	bins_age = [0, 31, 36, 41,  46, 51, 56, 61, 130]
	group_names_age = ['30 & below', '31 - 35', '36 - 40','41 - 45','46 - 50','51 - 55', '56 - 60', '61 & above']
		
	bins_ls = [0, 99999, 249999, 499999, 749999, 999999, 1499999, 1999999, 4999999,1000000000]
	group_names_ls = ['Below $100K', '$100K - $250K', '$250K - $500K', '$500K - $750K', '$750K-$1M',
							'$1M - $1.5M','$1.5M - $2M', '$2M - $5M', '$5M & above']
		
	bins_ip = [0, 999, 2499, 4999, 7499, 9999, 5000000]
	group_names_ip = ['Below $1K', '$1K - $2.5K', '$2.5K - $5K', '$5K - $7.5K', '$7.5K - $10K', 
							'$10K & above']

	if os.path.exists(os.path.join(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv')) == True:
		print ('before adding AGE')
		define_par_per_life1('AGE', ['AGE_LIFE', 'AGE_IP', 'AGE_TPD','AGE_TRA'])
		print ('before adding SEX')
		define_par_per_life1('SEX', ['SEX_LIFE','SEX_IP', 'SEX_TPD','SEX_TRA'])
		# print ('before adding CHANNEL')
		# define_par_per_life1('CHANNEL', ['CHANNEL_LIFE','CHANNEL_IP', 'CHANNEL_TPD','CHANNEL_TRA'])
		print ('before adding Smoking Indic')
		define_par_per_life1('SMOKER_IND', ['SMOKER_IND_LIFE','SMOKER_IND_IP',
							'SMOKER_IND_TPD','SMOKER_IND_TRA'])
		print ('before adding age bins')
		df_frame['AGE-GROUP'] = pd.cut(df_frame['AGE'], bins_age, labels = group_names_age)     
		df_frame['LIFE_SI_BAND'] = pd.cut(df_frame['SA_LIFE'], bins_ls, labels = group_names_ls)
		df_frame['TRA_SI_BAND'] = pd.cut(df_frame['SA_TRA'], bins_ls, labels = group_names_ls)
		df_frame['TPD_SI_BAND'] = pd.cut(df_frame['SA_TPD'], bins_ls, labels = group_names_ls)
		df_frame['IP_SI_BAND'] = pd.cut(df_frame['SA_IP'], bins_ip, labels = group_names_ip)
	else:
		print ('before adding AGE')
		define_par_per_life1('AGE', ['AGE_LIFE', 'AGE_IP', 'AGE_TPD'])
		print ('before adding SEX')
		define_par_per_life1('SEX', ['SEX_LIFE','SEX_IP', 'SEX_TPD'])
		# print ('before adding CHANNEL')
		# define_par_per_life1('CHANNEL', ['CHANNEL_LIFE','CHANNEL_IP', 'CHANNEL_TPD','CHANNEL_TRA'])
		print ('before adding Smoking Indic')
		define_par_per_life1('SMOKER_IND', ['SMOKER_IND_LIFE','SMOKER_IND_IP',
							'SMOKER_IND_TPD'])
		print ('before adding age bins')
		df_frame['AGE-GROUP'] = pd.cut(df_frame['AGE'], bins_age, labels = group_names_age)
		df_frame['LIFE_SI_BAND'] = pd.cut(df_frame['SA_LIFE'], bins_ls, labels = group_names_ls)
		df_frame['TPD_SI_BAND'] = pd.cut(df_frame['SA_TPD'], bins_ls, labels = group_names_ls)
		df_frame['IP_SI_BAND'] = pd.cut(df_frame['SA_IP'], bins_ip, labels = group_names_ip)
	
	return df_frame

result_frame = adding_bins(result_frame)
result_frame.to_csv('test.csv')
print result_frame.columns.values


if os.path.exists(os.path.join(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv')) == True:

	cols = ['POL_L_ID',
		'L_LIFE_ID', 'POL_NUMBER',
		'PACKAGE', 'AGE', 'AGE-GROUP', 'SEX', 'SMOKER_IND',

		'TOT_BEL','TOT_ANN_PREM', 'TOT_PREM_13', 

		'SA_LIFE', 'A_PREM_LIFE', 'A_PREM_13_LIFE', 'BEL_LIFE',
		'BROKER_LIFE','CHANNEL_LIFE', 'DCS_REIN_SI_LIFE',

		'SA_IP', 'A_PREM_IP', 'A_PREM_13_IP','BEL_IP',
		'BROKER_IP', 'CHANNEL_IP', 'OTR_ANNPHIBEN_IP', 'DII_TYPE', 'OCC_CLASS_IP',

		'SA_TPD', 'A_PREM_TPD', 'A_PREM_13_TPD', 'BEL_TPD',
		'BROKER_TPD', 'CHANNEL_TPD', 'DCS_REIN_SI_TPD',

		'SA_TRA', 'A_PREM_TRA', 'A_PREM_13_TRA', 'BEL_TRA',
		'BROKER_TRA','CHANNEL_TRA','DCS_REIN_SI_TRA',

		'LIFE_SI_BAND', 'TRA_SI_BAND', 'TPD_SI_BAND', 'IP_SI_BAND']
else:

	cols = [ 'POL_L_ID',
		'L_LIFE_ID', 'POL_NUMBER',
		'PACKAGE', 'AGE', 'AGE-GROUP', 'SEX', 'SMOKER_IND',

		'TOT_BEL','TOT_ANN_PREM', 'TOT_PREM_13', 
		
		'SA_LIFE', 'A_PREM_LIFE', 'A_PREM_13_LIFE', 'BEL_LIFE',
		'BROKER_LIFE','CHANNEL_LIFE', 'DCS_REIN_SI_LIFE',
		
		'SA_IP', 'A_PREM_IP', 'A_PREM_13_IP','BEL_IP',
		'BROKER_IP', 'CHANNEL_IP', 'OTR_ANNPHIBEN_IP', 'DII_TYPE', 'OCC_CLASS_IP',

		'SA_TPD', 'A_PREM_TPD', 'A_PREM_13_TPD', 'BEL_TPD',
		'BROKER_TPD', 'CHANNEL_TPD', 'DCS_REIN_SI_TPD',

		'LIFE_SI_BAND', 'TPD_SI_BAND', 'IP_SI_BAND']


result_frame = result_frame[cols]

#Save output to dataframe

fpath_res = os.path.join(RESULT, 'final')
print fpath_res


if os.path.exists(fpath_res) == False:
	 os.makedirs(fpath_res)

result_frame.to_csv(fpath_res + '\\' +'Result_'+f_run_name+'_byPol.csv')
print 'file saved to: ', fpath_res
print ('end of script')

#look into :

# 'BASE_PREM_LIFE' 
# 'PREMIUM_TYPE_LIFE'

#  'BASE_PREM_IP'
# 'PREMIUM_TYPE_IP' 

# 'BASE_PREM_TPD'
#  'PREMIUM_TYPE_TPD' 

#  'BASE_PREM_TRA'
# 'PREMIUM_TYPE_TRA'



#  'B_BEN_NO_x'  'DCS_REIN_SI_x' 

#  'POLICY_FEE_x' 'POLICY_FEE'  

#  'POL_L_ID'

 
#  'BENEFIT_CODE'

#  'BEN_PERIOD' 
#  'OCC_CLASS' 
#   

#  'OTR_ANNPHIBEN'
 
 

#  'B_BEN_NO_y' 

#  'POLICY_FEE_y'
#  'DCS_REIN_SI_y'  
  
#  'B_BEN_NO' 
#  'DCS_REIN_SI'  