#combining by Line for all policies! 
# super to do

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


PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data', policy_subset)
RESULT = os.path.join(PROJ, 'results', policy_subset)


fpath_res = os.path.join(RESULT, 'final')

if os.path.exists(fpath_res) == False:
     os.makedirs(fpath_res)


result_run02 = pd.read_csv(fpath_res + '\\' +'Result_run02_byLine.csv')
result_run06 = pd.read_csv(fpath_res + '\\' +'Result_run06_byLine.csv')


a = result_run02['A_PREM'][result_run02.ix[:,'IP']=='IP']
print a.sum()
print a.count()


result_run02 = result_run02.rename(columns = {'A_PREM': 'A_PREM_R02', 'A_PREM_13': 'A_PREM_13_R02', 
												'BEL':'BEL_R02'})
result_run06 = result_run06.rename(columns = {'A_PREM': 'A_PREM_R06', 'A_PREM_13': 'A_PREM_13_R06', 
												'BEL':'BEL_R06'})

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

a2 = df['A_PREM_R02'][df.ix[:,'IP']=='IP']
print a2.sum()
print a2.count()


# df.drop(['SUM_ASSURED_3','B_BEN_NO_3', 'B_OFF_APREM_3','PREM_BAS_TAB_3'], axis = 1, inplace = True)
# df = df.drop_duplicates(['POL_NUMBER','L_LIFE_ID','SUM_ASSURED','DESCRIPTION2','B_BEN_NO'])
print df.shape[0], df.shape[1]
print result_run02.shape[0]


df['%Ch_Total_Prem(R02vsR06)'] = df.apply(lambda row:
						percCalc(row['A_PREM_R02'], row['A_PREM_13_R02']), axis=1)

df['$Diff_Total_Prem(R02vsR06)'] = df.apply(lambda row:
						DollarDiffCalc(row['A_PREM_R02'], row['A_PREM_R06']), axis=1)


df['%Ch_Total_Prem_13(R02vsR06)'] = df.apply(lambda row:
						percCalc(row['A_PREM_13_R02'], row['A_PREM_13_R06']), axis=1)
df['$Diff_Total_Prem_13(R02vsR06)'] = df.apply(lambda row:
						DollarDiffCalc(row['A_PREM_13_R02'], row['A_PREM_13_R06']), axis=1)


df['%Ch_Tot_Prem(R02)_toPrem13(R06)'] = df.apply(lambda row:
						percCalc(row['A_PREM_R02'], row['A_PREM_13_R06']), axis=1)
df['$Diff_Tot_Prem(R02)_toPrem13(R06)'] = df.apply(lambda row:
						DollarDiffCalc(row['A_PREM_R02'], row['A_PREM_13_R06']), axis=1)


df['%Ch_Tot_Prem(R02)_toPrem13(R02)'] = df.apply(lambda row:
						percCalc(row['A_PREM_R02'], row['A_PREM_13_R02']), axis=1)
df['$Diff_Tot_Prem(R02)_toPrem13(R02)'] = df.apply(lambda row:
						DollarDiffCalc(row['A_PREM_R02'], row['A_PREM_13_R02']), axis=1)

df['%Ch_Tot_Prem(R06)_toPrem13(R06)'] = df.apply(lambda row:
						percCalc(row['A_PREM_R06'], row['A_PREM_13_R06']), axis=1)
df['$Diff_Tot_Prem(R06)_toPrem13(R06)'] = df.apply(lambda row:
						DollarDiffCalc(row['A_PREM_R06'], row['A_PREM_13_R06']), axis=1)


# df['%Ch_LIFE_PV_Prem'] = df.apply(lambda row:
# 						percCalc(row['ANNUM_PV_PREM_New_LIFE_R02'], row['ANNUM_PV_PREM_New_LIFE_R06']),
# 													axis=1)
# df['%Ch_LIFE_Prem13(Newto13)'] = df.apply(lambda row:
# 						percCalc(row['ANNUM_PREM_New_LIFE_R02'], row['ANNUM_PREM_13_LIFE_R06']),
# 													axis=1)
# df['%Ch_TRA_Prem13(R1toR2)'] = df.apply(lambda row:
# 						percCalc(row['ANNUM_PREM_New_TRA_R02'], row['ANNUM_PV_PREM_New_TRA_R06']),
# 													axis=1)
# df['%Ch_TPD_Prem13(R1toR2)'] = df.apply(lambda row:
# 						percCalc(row['ANNUM_PV_PREM_New_TPD_R02'], row['ANNUM_PV_PREM_New_TPD_R06']),
# 													axis=1)
# df['%Ch_IP_Prem13(R1toR2)'] = df.apply(lambda row:
# 						percCalc(row['ANNUM_PV_PREM_New_IP_R02'], row['ANNUM_PV_PREM_New_IP_R06']),
													# axis=1)

# ______________


bins_perc_ch = [-10,-0.3, -0.2, -0.1, 0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 10]
perc_label = ['(> -30%)', '[-30%;-20%)', '[-20% ;-10%)', '[-10%; 0%)',
						'[0%; 10%)', '[10%; 20%)', '[20%; 30%)', '[30%; 40%)', '[40%; 50%)',
						'[50%; 60%)', '[> 60%)']

# assigning discounts according to specified bins
df.ix[:,'%bin_Total_Prem(R02vsR06)'] = pd.cut(df['%Ch_Total_Prem(R02vsR06)'],
                                                    bins_perc_ch, labels = perc_label)
df.ix[:,'%bin_Total_Prem_13(R02vsR06)'] = pd.cut(df['%Ch_Total_Prem_13(R02vsR06)'],
                                                    bins_perc_ch, labels = perc_label)
df.ix[:,'%bin_Tot_Prem(R02)_toPrem13(R06)'] = pd.cut(df['%Ch_Tot_Prem(R02)_toPrem13(R06)'],
                                                    bins_perc_ch, labels = perc_label)
df.ix[:,'%bin_Tot_Prem(R02)_toPrem13(R02)'] = pd.cut(df['%Ch_Tot_Prem(R02)_toPrem13(R02)'],
                                                    bins_perc_ch, labels = perc_label)
df.ix[:,'%bin_Tot_Prem(R06)_toPrem13(R06)'] = pd.cut(df['%Ch_Tot_Prem(R06)_toPrem13(R06)'],
                                                    bins_perc_ch, labels = perc_label)
df.reset_index()
# print df.to_csv('test.csv')




print df.columns.values

cols = ['L_LIFE_ID', 'POL_NUMBER',
	'AGE', 'SEX', 'SMOKER_IND', 'AGE-GROUP', 
	'A_PREM_R02', 'A_PREM_R06','A_PREM_13_R02', 'A_PREM_13_R06','BEL_R02', 'BEL_R06',
	'PREMIUM_TYPE', 'Death', 'TRA', 'TPD', 'IP',
	
	'%Ch_Total_Prem(R02vsR06)', '$Diff_Total_Prem(R02vsR06)', '%bin_Total_Prem(R02vsR06)',
 	'%Ch_Total_Prem_13(R02vsR06)', '$Diff_Total_Prem_13(R02vsR06)', '%bin_Total_Prem_13(R02vsR06)',
 	'%Ch_Tot_Prem(R02)_toPrem13(R06)', '$Diff_Tot_Prem(R02)_toPrem13(R06)', '%bin_Tot_Prem(R02)_toPrem13(R06)',
 	'%Ch_Tot_Prem(R02)_toPrem13(R02)','$Diff_Tot_Prem(R02)_toPrem13(R02)', '%bin_Tot_Prem(R02)_toPrem13(R02)',
 	'%Ch_Tot_Prem(R06)_toPrem13(R06)','$Diff_Tot_Prem(R06)_toPrem13(R06)', '%bin_Tot_Prem(R06)_toPrem13(R06)',

	'SA',  'A_PREM_R02', 'A_PREM_13_R02', 'BEL_R02',
			'A_PREM_R06', 'A_PREM_13_R06', 'BEL_R06',

	'BENEFIT_CODE', 'BEN_PERIOD', 'B_BEN_NO', 'DEFER_PER_MP', 'DII_TYPE', 'OCC_CLASS',
	'OTR_ANNPHIBEN', 'DCS_REIN_SI',

	'SI_LS_BAND', 'SI_IP_BAND',

	'CHANNEL', 'BROKER']

	# 'BROKER_LIFE_R02', 'CHANNEL_LIFE_R02', 'BROKER_IP_R02', 'CHANNEL_IP_R02',
	# 'BROKER_TPD_R02', 'CHANNEL_TPD_R02', 'BROKER_TRA_R02', 'CHANNEL_TRA_R02']

df = df[cols]

df.to_csv(fpath_res + '\\' +'Combined(R02&R06)_byLine.csv')
print 'file saved to: ', fpath_res
print ('end of script')