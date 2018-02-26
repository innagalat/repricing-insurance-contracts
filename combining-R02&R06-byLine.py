#combining by Line for all policies! 


import pandas as pd
import numpy as np
import os
from heapq import nlargest

def percCalc(x,y):
	if x<1e-9:
		return 0
	else:
		return (y - x)/x

def DollarDiffCalc(x,y):
   return (y - x)

policy_subset = 'ordinary-cover'
# policy_subset = 'super-cover'


# uncomment indicator_print if needed all columns to be printed
indicator_print = 'full print'
# indicator_print = 'short print'
ind_run_no = 2

PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data', policy_subset)
RESULT = os.path.join(PROJ, 'results', policy_subset)


fpath_res = os.path.join(RESULT, 'final')


if os.path.exists(fpath_res) == False:
	os.makedirs(fpath_res)


result_run02 = pd.read_csv(fpath_res + '\\' +'Result_run02_byLine.csv')
result_run06 = pd.read_csv(fpath_res + '\\' +'Result_run06_byLine.csv')




result_run02 = result_run02.rename(columns = {	'A_PREM1': 'A_PREM1_R02',
												'A_PREM_13': 'A_PREM_13_R02', 
												'A_PREM_25': 'A_PREM_25_R02',
												'BEL':'BEL_R02'})
result_run06 = result_run06.rename(columns = {	'A_PREM1': 'A_PREM1_R06',
												'A_PREM_13': 'A_PREM_13_R06',
												'A_PREM_25': 'A_PREM_25_R06',
												'BEL':'BEL_R06'})

print result_run02.info()
df = pd.concat([result_run02, result_run06], axis = 1)
df = df.loc[:,~df.columns.duplicated()]



# no Package column here

# merging didn't work because too many rows and there are some repetitions, so after merging number of the rows increased. 
# I needed to drop some rows, but it would have taken time to understand what causes repetions

# df = pd.merge(result_run02, result_run06, 
				# on = ['L_LIFE_ID','POL_NUMBER', 'AGE', 'SEX', 'SMOKER_IND','AGE-GROUP', 'BENEFIT_CODE', 'BEN_PERIOD', 'B_BEN_NO',
				# 'PREMIUM_TYPE', 'DEFER_PER_MP', 'DII_TYPE', 'OCC_CLASS',  'TOTAL_SI.1', 'OTR_ANNPHIBEN', 'DCS_REIN_SI',
				# 'CHANNEL','BROKER',
				# 'Death', 'TRA', 'TPD', 'IP',  
				# 'SI_LS_BAND','SI_IP_BAND'],
				# how = 'outer', suffixes=('_R02', '_R06'))

#Total SI dragging from stitching from ip file....... need to remove but too much work at the moment
# 'TOTAL_SI' - unnecesary, needs to be removed

df = df.drop('TOTAL_SI.1', axis = 1)
# df = df.drop_duplicates(['L_LIFE_ID','POL_NUMBER'])


# df.drop(['SUM_ASSURED_3','B_BEN_NO_3', 'B_OFF_APREM_3','PREM_BAS_TAB_3'], axis = 1, inplace = True)
# df = df.drop_duplicates(['POL_NUMBER','L_LIFE_ID','SUM_ASSURED','DESCRIPTION2','B_BEN_NO'])
print df.shape[0], df.shape[1]
print result_run02.shape[0]



bins_perc_ch = [-10,-0.3, -0.2, -0.1, 0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 10]
perc_label = ['(> -30%)', '[-30%;-20%)', '[-20% ;-10%)', '[-10%; 0%)',
						'[0%; 10%)', '[10%; 20%)', '[20%; 30%)', '[30%; 40%)', '[40%; 50%)',
						'[50%; 60%)', '[> 60%)']

print ('Find % changes')
# rates change from x to x+1 anniversary on 4A series
df['%Inc TPrem1(R02) to TPrem13(R02)'] = df.apply(lambda row:
						percCalc(row['A_PREM1_R02'], row['A_PREM_13_R02']), axis=1)
df['$Inc TPrem1(R02) to TPrem13(R02)'] = df.apply(lambda row:
						DollarDiffCalc(row['A_PREM1_R02'], row['A_PREM_13_R02']), axis=1)
# assigning discounts according to specified bins
df.ix[:,'%bin_Prem1(R02)_toPrem13(R02)'] = 	pd.cut(df['%Inc TPrem1(R02) to TPrem13(R02)'],
                                                    bins_perc_ch, labels = perc_label)

# rates change from x+1 anniversary to x+2 anniversary on 4A series
df['%Inc TPrem_13(R02) to TPrem25(R02)'] = df.apply(lambda row:
						percCalc(row['A_PREM_13_R02'], row['A_PREM_25_R02']), axis=1)
df['$Inc TPrem13(R02) to TPrem25(R02)'] = df.apply(lambda row:
						DollarDiffCalc(row['A_PREM_13_R02'], row['A_PREM_25_R02']), axis=1)
# assigning discounts according to specified bins
df.ix[:, '%bin_Prem13(R02)_toPrem25(R02)'] =pd.cut(df['%Inc TPrem_13(R02) to TPrem25(R02)'],
                                                    bins_perc_ch, labels = perc_label)


# rates change from x age on 4A to x+1 anniversary on 5A
df['%Inc TPrem1(R02) to TPrem13(R06)'] = df.apply(lambda row:
						percCalc(row['A_PREM1_R02'], row['A_PREM_13_R06']), axis=1)
df['$Inc TPrem1(R02) to TPrem13(R06)'] = df.apply(lambda row:
						DollarDiffCalc(row['A_PREM1_R02'], row['A_PREM_13_R06']), axis=1)
# assigning discounts according to specified bins
df.ix[:, '%bin_Prem1(R02)_toPrem13(R06)']  = pd.cut(df['%Inc TPrem1(R02) to TPrem13(R06)'],
                                                    bins_perc_ch, labels = perc_label)

# rates change from x+1 on 4A to x+2 anniversary on 5A
df['%Inc TPrem13(R02) to TPrem25(R06)'] = df.apply(lambda row:
						percCalc(row['A_PREM_13_R02'], row['A_PREM_25_R06']), axis=1)
df['$Inc TPrem13(R02) to TPrem25(R06)'] = df.apply(lambda row:
						DollarDiffCalc(row['A_PREM_13_R02'], row['A_PREM_25_R06']), axis=1)
# assigning discounts according to specified bins
df.ix[:, '%bin_Prem13(R02)_toPrem25(R06)'] = pd.cut(df['%Inc TPrem13(R02) to TPrem25(R06)'],
                                                    bins_perc_ch, labels = perc_label)


# rates change from 4A next anniversary to 5A next anniversary
df['%Inc TPrem13(R02vsR06)'] = df.apply(lambda row:
						percCalc(row['A_PREM_13_R02'], row['A_PREM_13_R06']), axis=1)
df['$Inc TPrem13(R02vsR06)'] = df.apply(lambda row:
						DollarDiffCalc(row['A_PREM_13_R02'], row['A_PREM_13_R06']), axis=1)
# assigning discounts according to specified bins
df.ix[:, '%bin_Prem_13(R02vsR06)'] = pd.cut(df['%Inc TPrem13(R02vsR06)'],
                                                    bins_perc_ch, labels = perc_label)


# rates change from 4A x+2 anniversary to 5A x+2 anniversary
df['%Inc TPrem25(R02vsR06)'] = df.apply(lambda row:
						percCalc(row['A_PREM_25_R02'], row['A_PREM_25_R06']), axis=1)
df['$Inc TPrem25(R02vsR06)'] = df.apply(lambda row:
						DollarDiffCalc(row['A_PREM_25_R02'], row['A_PREM_25_R06']), axis=1)
# assigning discounts according to specified bins
df.ix[:, '%bin_Prem_25(R02vsR06)'] = pd.cut(df['%Inc TPrem25(R02vsR06)'],
                                                    bins_perc_ch, labels = perc_label)


df.reset_index()
print df.columns.values

if ind_run_no == 2:

	if indicator_print == 'full print':

		if policy_subset == 'ordinary-cover':
			
			cols = ['L_LIFE_ID', 'POL_NUMBER',
					'ENTRY_MONTH', 'ENTRY_YEAR', 'ENTRY_DATE',
					'AGE', 'AGE-GROUP', 'SEX', 'SMOKER_IND', 

					'PREMIUM_TYPE', 'Death', 'TRA', 'TPD', 'IP',

					'A_PREM1_R02', 'A_PREM_13_R02', 'A_PREM_25_R02',
					'A_PREM1_R06', 'A_PREM_13_R06', 'A_PREM_25_R06',
					'BEL_R02', 'BEL_R06',
					
					
					'%Inc TPrem1(R02) to TPrem13(R02)', 	'$Inc TPrem1(R02) to TPrem13(R02)', 	'%bin_Prem1(R02)_toPrem13(R02)',
					'%Inc TPrem_13(R02) to TPrem25(R02)', 	'$Inc TPrem13(R02) to TPrem25(R02)', 	'%bin_Prem13(R02)_toPrem25(R02)',
					'%Inc TPrem1(R02) to TPrem13(R06)', 	'$Inc TPrem1(R02) to TPrem13(R06)', 	'%bin_Prem1(R02)_toPrem13(R06)',
					'%Inc TPrem13(R02) to TPrem25(R06)', 	'$Inc TPrem13(R02) to TPrem25(R06)', 	'%bin_Prem13(R02)_toPrem25(R06)',

					'%Inc TPrem13(R02vsR06)', '$Inc TPrem13(R02vsR06)', '%bin_Prem_13(R02vsR06)',
					'%Inc TPrem25(R02vsR06)', '$Inc TPrem25(R02vsR06)', '%bin_Prem_25(R02vsR06)',


			 		# 'TOT_BE_GRRESERVE_R02',

					'SA', 'SA_IP',
					'A_PREM1_R02', 'A_PREM_13_R02', 'A_PREM_25_R02',  	'BEL_R02',
					'A_PREM1_R06', 'A_PREM_13_R06', 'A_PREM_25_R02',	'BEL_R06',

					'BENEFIT_CODE', 'BEN_PERIOD', 'B_BEN_NO', 'DEFER_PER_MP', 'DII_TYPE', 'OCC_CLASS',
					'OTR_ANNPHIBEN', 'DCS_REIN_SI',

					'SI_LS_BAND', 'SI_IP_BAND',

					'CHANNEL', 'BROKER']

		# elif policy_subset == 'super-cover':
		# 	cols = ['L_LIFE_ID', 'POL_NUMBER',
		# 	'AGE', 'SEX', 'SMOKER_IND', 'AGE-GROUP', 
		# 	'A_PREM_R02', 'A_PREM_R06','A_PREM_13_R02', 'A_PREM_13_R06','BEL_R02', 'BEL_R06',
		# 	'PREMIUM_TYPE', 'Death', 'TPD', 'IP',
			
		# 	'%Ch_Total_Prem(R02vsR06)', '$Diff_Total_Prem(R02vsR06)', '%bin_Total_Prem(R02vsR06)',
		# 	'%Inc TPrem13(R02vsR06)', '$Inc TPrem13(R02vsR06)', '%bin_Total_Prem_13(R02vsR06)',
		# 	'%Ch_Tot_Prem(R02)_toPrem13(R06)', '$Diff_Tot_Prem(R02)_toPrem13(R06)', '%bin_Tot_Prem(R02)_toPrem13(R06)',
		# 	'%Inc TPrem(R02) to TPrem13(R02)','$Inc TPrem(R02) to TPrem13(R02)', '%bin_Tot_Prem(R02)_toPrem13(R02)',
		# 	'%Ch_Tot_Prem(R06)_toPrem13(R06)','$Diff_Tot_Prem(R06)_toPrem13(R06)', '%bin_Tot_Prem(R06)_toPrem13(R06)',

		# 	'SA', 'SA_IP',  
		# 	'A_PREM1_R02', 'A_PREM_13_R02', 'BEL_R02',
		# 	'A_PREM1_R06', 'A_PREM_13_R06', 'BEL_R06',

		# 	'BENEFIT_CODE', 'BEN_PERIOD', 'B_BEN_NO', 'DEFER_PER_MP', 'DII_TYPE', 'OCC_CLASS',
		# 	'OTR_ANNPHIBEN', 'DCS_REIN_SI',

		# 	'SI_LS_BAND', 'SI_IP_BAND',

		# 	'CHANNEL', 'BROKER']

		# 'BROKER_LIFE_R02', 'CHANNEL_LIFE_R02', 'BROKER_IP_R02', 'CHANNEL_IP_R02',
		# 'BROKER_TPD_R02', 'CHANNEL_TPD_R02', 'BROKER_TRA_R02', 'CHANNEL_TRA_R02']

	df = df[cols]

# if ind_run_no == 1:
# 	df = df.rename(columns= {

# 						'%Inc TPrem13(R02vsR06)':'%Inc_AP13_to_AP25',
# 						'$Inc TPrem13(R02vsR06)':'$Inc_AP13_to_AP25',
# 						'%bin_Total_Prem_13(R02vsR06)':'%BandInc_AP13_to_AP25',
						
# 						'A_PREM_R02':'TOT_PREM1',
# 						'A_PREM_13_R02':'TOT_PREM_13',
# 						'A_PREM_13_R06':'TOT_PREM_25',
# 						'BEL_R02':'BEL',

# 						'%Inc TPrem(R02) to TPrem13(R02)':'%Inc_AP1_to_AP13',
# 						'$Inc TPrem(R02) to TPrem13(R02)':'$Inc_AP1_to_AP13',
# 						'%bin_Tot_Prem(R02)_toPrem13(R02)':'%BandInc_AP1_to_AP13',

# 					})


# 	if policy_subset == 'super-cover':
# 		df['TRA'] = np.nan

# 	cols = ['L_LIFE_ID', 'POL_NUMBER', 
# 					'SEX', 'SMOKER_IND', 'AGE', 'AGE-GROUP', 'BEL',
# 					'PREMIUM_TYPE', 'Death', 'TRA', 'TPD', 'IP',

# 					'TOT_PREM1', 'TOT_PREM_13', 'TOT_PREM_25',

# 					'%Inc_AP13_to_AP25',
# 					'$Inc_AP13_to_AP25',
# 					'%BandInc_AP13_to_AP25',


# 					'%Inc_AP1_to_AP13',
# 					'%Inc_AP1_to_AP13',
# 					'%Inc_AP1_to_AP13',

					
# 					# 'BE_GRRESERVE', 'PV_GR_CARR',


# 					'SA', 'SA_IP',

# 					'SI_LS_BAND', 'SI_IP_BAND',

# 					'BROKER',
# 					'CHANNEL']
	
# 	df = df[cols]

df.sort_values(['L_LIFE_ID'], inplace = True)



if policy_subset == 'ordinary-cover':
	df.to_csv(fpath_res + '\\' +'Combined(R02&R06)_byLine-ord.csv', index = False)
elif policy_subset == 'super-cover':
	df.to_csv(fpath_res + '\\' +'Combined(R02&R06)_byLine-sup.csv', index = False)


print 'file saved to: ', fpath_res
print ('end of script')