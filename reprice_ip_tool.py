

import pandas as pd
import numpy as np
import math

file_name = 'C:\galati_files\pyscripts\callo-repricing\compare-runs\OneCare Rates 2017 - 5A series v4 - Text.xlsx'
wait_period = [	
				'14d_noICB', 	'14d_ICB', 	'30d_noICB', 	'30d_ICB',
				'60d_noICB', 	'60d_ICB', 	'90d_noICB', 	'90d_ICB',
	 			'180d_noICB', '180d_ICB', 	'360d_noICB', '360d_ICB',
	   			'720d_noICB', '720d_ICB', 'empty1', 'empty2']

sex_list = ['m','f']
bene_period = ['BP70', 'BP65', 'BP60', 'BP55', 'BP6', 'BP2']	
col_names_final = []


for gender in sex_list:
	for benefit in bene_period:
		for waiting in wait_period:
			col_names_final.append(gender + benefit +'_' + waiting)

#delete last 2 elements in column_names list (empty1 and empty2) 
#to match number of elements in list and number of column in df
col_names_final= col_names_final[:-2]




sex = 'm'

si0 = 10000
y0 = 46
wp0 = 30
bp0 = 70

si_now = 15000
y_now = 50
wp_want = 30
bp_want = 65

age_index = []
rate_to_use_list = []
comparison_table = pd.DataFrame()
# ip_type = 'Basic'




def calc_average_cpi(si_now, si_commenc, age_now, age_commenc):
	dur =  (age_now - age_commenc) # calculation of Duration
	# print ('duration: '), dur
	cpi = math.pow( float(si_now) / float(si_commenc), 1/float(dur) ) - 1
	return cpi


cpi = calc_average_cpi(si_now, si0, y_now, y0) # calculation of CPI
print ('cpi: '), cpi



# create age column that is set as index column.
# It starts from the commencement age to the maximum for level, which is set at 64 
for year in range(y0, 65):
	age_index.append(year)
df_results = pd.DataFrame(index = age_index)


cpi = 0.015

rate_to_use0 = sex + "BP" + str(bp0) +"_" + str(wp0) + "d" + "_ICB"
# mBP70_30d_ICB

# rate_to_use1 = sex + "BP" + str(bp_want) +"_" + str(wp_want) + "d" + "_noICB"
# print rate_to_use1



	#find SI by slices and total SI for each age 
for i in age_index:
	df_results.ix[i,'SI'] = si0 * math.pow( 1+ cpi, i-y0 )
	df_results.ix[i + 1,'SI_temp'] = si0 * math.pow( 1+ cpi, i-y0)	
df_results['SI_slice'] = df_results['SI'] - df_results['SI_temp'].fillna(0)
df_results = df_results.drop('SI_temp', axis = 1).dropna() #remove temporary col and a row at age 70



def find_premium(df, ip_name, rate_to_use):
	df_results[ ('Prem_slice'+ rate_to_use[1:9] + ip_name) ] = \
								df_results['SI_slice'] * df[rate_to_use][age_index]/100
	for i in range(0,len(age_index)):
		df_results.ix[age_index[i], ('Tot_BPrem' + rate_to_use[1:9] + ip_name)] = \
								df_results.ix[ y0 : (y0 + i), ('Prem_slice'+ rate_to_use[1:9] + ip_name) ].sum()
	return df_results




# mBP65_30d_noICB
options =  [					'BP70_30d',
								'BP70_60d',
								'BP70_90d',
								'BP65_30d',
								'BP65_60d',
								'BP65_90d']







for item in options:
	rate_to_use_list.append (sex + item + "_ICB")

#_______________________________________________________________________

ip_names = ['BasicIndemnity(L)', 		'SpecialRiskIndemnity(L)', 'StandardIndemnity(L)', 
			'ComprehensiveIndemnity(L)', 'ProfessionalIndemnity(L)', 'StandardGuaranteed(L)',
			 'ComprehensiveGuaranteed(L)', 'ProfessionalGuaranteed(L)']

df_l_BasicI = pd.read_excel(file_name, sheetname = ip_names[0], skiprows = [1,2,3,4,5,6,7], 
								names = col_names_final,	 index_col = 0, nrows = 19)

df_l_StandardI = pd.read_excel(file_name, sheetname = ip_names[2], skiprows = [1,2,3,4,5,6,7], 
								names = col_names_final,	 index_col = 0, nrows = 19)

df_l_StandardG = pd.read_excel(file_name, sheetname = ip_names[5], skiprows = [1,2,3,4,5,6,7], 
								names = col_names_final,	 index_col = 0, nrows = 19)

df_l_ComprehI= pd.read_excel(file_name, sheetname = ip_names[3], skiprows = [1,2,3,4,5,6,7], 
								names = col_names_final,	 index_col = 0 , nrows = 19)

df_l_ComprehG = pd.read_excel(file_name, sheetname = ip_names[6], skiprows = [1,2,3,4,5,6,7], 
								names = col_names_final,	 index_col = 0, nrows = 19)

df_l_ProfI = pd.read_excel(file_name, sheetname = ip_names[4], skiprows = [1,2,3,4,5,6,7], 
								names = col_names_final,	 index_col = 0, nrows = 19)

df_l_ProfG = pd.read_excel(file_name, sheetname = ip_names[7], skiprows = [1,2,3,4,5,6,7], 
								names = col_names_final,	 index_col = 0, nrows = 19)

print ('_______________________________________________________________')


column_table = ['BasicI', 'StandardI', 'StandardG', 'ComprehI', 'ComprehG', 'ProfI', 'ProfG' ]
for item in rate_to_use_list:

	df_results = find_premium(	df_l_BasicI,		column_table[0], item)
	df_results = find_premium(	df_l_StandardI, 	column_table[1], item)
	df_results = find_premium(	df_l_StandardG, 	column_table[2], item)
	df_results = find_premium(	df_l_ComprehI, 		column_table[3], item)
	df_results = find_premium(	df_l_ComprehG, 		column_table[4], item)
	df_results = find_premium(	df_l_ProfI, 		column_table[5], item)
	df_results = find_premium(	df_l_ProfG, 		column_table[6], item)




# BasicIndemnity(L)
# SpecialRiskIndemnity(L)
# StandardIndemnity(L)
# ComprehensiveIndemnity(L)
# ProfessionalIndemnity(L)
# StandardGuaranteed(L)
# ComprehensiveGuaranteed(L)
# ProfessionalGuaranteed(L)








# columns = ['', 'Basic', 
# 				'Standard Indemnity', 'Standard Guaranteed', 
# 				'Comprehensive Indemnity', 'Comprehensive Guaranteed',
# 				'Professonal Indemnity', 'Professional Guaranteed']
# 								 )

# comparison_table[''] = [		'BP = 70 ; WP = 30d',
# 								'BP = 70 ; WP = 60d',
# 								'BP = 70 ; WP = 90d',
# 								'BP = 65 ; WP = 30d',
# 								'BP = 65 ; WP = 60d',
# 								'BP = 65 ; WP = 90d']




for i in options:
	for j in column_table:
		comparison_table.ix[i,j] = int(df_results.ix[y_now, ('Tot_BPrem' + i + j)])
	
print comparison_table