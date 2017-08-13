# will need to delete this file
# Combining by id now includes super as well

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

PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data')
RESULT = os.path.join(PROJ, 'results\\super-cover')


fpath_res = os.path.join(RESULT, 'final')
print fpath_res
if os.path.exists(fpath_res) == False:
     os.makedirs(fpath_res)


result_run02 = pd.read_csv(fpath_res + '\\' +'Result_run02_byID.csv')
result_run06 = pd.read_csv(fpath_res + '\\' +'Result_run06_byID.csv')

print result_run02.info()
df = pd.merge(result_run02, result_run06, 
				on = ['L_LIFE_ID', 'AGE', 'SEX', 'SMOKER_IND','AGE-GROUP', 'TERM_SI_BAND','TPD_SI_BAND',
					'IP_SI_BAND', 'PACKAGE'],
				how = 'outer', suffixes=('_R02', '_R06'))
# df.drop(['Package_R06'], axis = 1, inplace = True)


# df.drop(['SUM_ASSURED_3','B_BEN_NO_3', 'B_OFF_APREM_3','PREM_BAS_TAB_3'], axis = 1, inplace = True)
# df = df.drop_duplicates(['POL_NUMBER','L_LIFE_ID','SUM_ASSURED','DESCRIPTION2','B_BEN_NO'])
print df.shape[0], df.shape[1]
print result_run02.shape[0]
print df.info()

df['%Ch_Total_Prem(R02vsR06)'] = df.apply(lambda row:
						percCalc(row['TOT_ANN_PREM_R02'], row['TOT_ANN_PREM_R06']), axis=1)

df['$Diff_Total_Prem(R02vsR06)'] = df.apply(lambda row:
						DollarDiffCalc(row['TOT_ANN_PREM_R02'], row['TOT_ANN_PREM_R06']), axis=1)

df['%Ch_Total_Prem_13(R02vsR06)'] = df.apply(lambda row:
						percCalc(row['TOT_PREM_13_R02'], row['TOT_PREM_13_R06']), axis=1)
df['$Diff_Total_Prem_13(R02vsR06)'] = df.apply(lambda row:
						DollarDiffCalc(row['TOT_PREM_13_R02'], row['TOT_PREM_13_R06']), axis=1)

df['%Ch_Tot_Prem(R02)_toPrem13(R06)'] = df.apply(lambda row:
						percCalc(row['TOT_ANN_PREM_R02'], row['TOT_PREM_13_R06']), axis=1)
df['$Diff_Tot_Prem(R02)_toPrem13(R06)'] = df.apply(lambda row:
						DollarDiffCalc(row['TOT_ANN_PREM_R02'], row['TOT_PREM_13_R06']), axis=1)

df['%Ch_Tot_Prem(R02)_toPrem13(R02)'] = df.apply(lambda row:
						percCalc(row['TOT_ANN_PREM_R02'], row['TOT_PREM_13_R02']), axis=1)
df['$Diff_Tot_Prem(R02)_toPrem13(R02)'] = df.apply(lambda row:
						DollarDiffCalc(row['TOT_ANN_PREM_R02'], row['TOT_PREM_13_R02']), axis=1)

df['%Ch_Tot_Prem(R06)_toPrem13(R06)'] = df.apply(lambda row:
						percCalc(row['TOT_ANN_PREM_R06'], row['TOT_PREM_13_R06']), axis=1)
df['$Diff_Tot_Prem(R06)_toPrem13(R06)'] = df.apply(lambda row:
						DollarDiffCalc(row['TOT_ANN_PREM_R06'], row['TOT_PREM_13_R06']), axis=1)


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
print df.head(2)



# df = df.rename(columns =
# 			{'Package_R02':'Package'})
print df.columns.values

cols = ['L_LIFE_ID', 'PACKAGE', 'AGE', 'SEX', 'SMOKER_IND', 'AGE-GROUP', 
	'TOT_ANN_PREM_R02', 'TOT_ANN_PREM_R06','TOT_PREM_13_R02', 'TOT_PREM_13_R06','TOT_BEL_R02', 'TOT_BEL_R06',
	'%Ch_Total_Prem(R02vsR06)', '$Diff_Total_Prem(R02vsR06)', '%bin_Total_Prem(R02vsR06)',
 	'%Ch_Total_Prem_13(R02vsR06)', '$Diff_Total_Prem_13(R02vsR06)', '%bin_Total_Prem_13(R02vsR06)',
 	'%Ch_Tot_Prem(R02)_toPrem13(R06)', '$Diff_Tot_Prem(R02)_toPrem13(R06)', '%bin_Tot_Prem(R02)_toPrem13(R06)',
 	'%Ch_Tot_Prem(R02)_toPrem13(R02)','$Diff_Tot_Prem(R02)_toPrem13(R02)', '%bin_Tot_Prem(R02)_toPrem13(R02)',
 	'%Ch_Tot_Prem(R06)_toPrem13(R06)','$Diff_Tot_Prem(R06)_toPrem13(R06)', '%bin_Tot_Prem(R06)_toPrem13(R06)',

	'SA_LIFE_R02',  'A_PREM_LIFE_R02', 'A_PREM_13_LIFE_R02', 'BEL_LIFE_R02',
	'SA_LIFE_R06', 'A_PREM_LIFE_R06', 'A_PREM_13_LIFE_R06', 'BEL_LIFE_R06',
	
	'SA_TPD_R02', 'A_PREM_TPD_R02', 'A_PREM_13_TPD_R02', 'BEL_TPD_R02',
	'SA_TPD_R06', 'A_PREM_TPD_R06', 'A_PREM_13_TPD_R06', 'BEL_TPD_R06',
	'SA_IP_R02', 'A_PREM_IP_R02', 'A_PREM_13_IP_R02', 'BEL_IP_R02',  
 	'SA_IP_R06', 'A_PREM_IP_R06', 'A_PREM_13_IP_R06', 'BEL_IP_R06',
	'TERM_SI_BAND', 'TPD_SI_BAND', 'IP_SI_BAND']

df = df[cols]

df.to_csv(fpath_res + '\\' +'Combined(R02&R06)_byID-Sup.csv')
exit('0')
print ('end of script')