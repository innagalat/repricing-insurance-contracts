# added entry month and year for comb by policy

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

def findLifeID(x):
	return x[-8:]

def findPolID(x):
	return x[10:]


policy_subset = 'ordinary-cover'
# policy_subset = 'super-cover'

# uncomment indicator_print if needed all columns to be printed
# indicator_print = 'full print'
indicator_print = 'short print'
ind_run_no = 1

extra_col = ['BE_GRRESERVE',  'PV_GR_CARR', 'ANNUAL_PREM_25']


PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data', policy_subset)
RESULT = os.path.join(PROJ, 'results', policy_subset)


fpath_res = os.path.join(RESULT, 'final')

if os.path.exists(fpath_res) == False:
	 os.makedirs(fpath_res)


if policy_subset == 'ordinary-cover':
	result_run02 = pd.read_csv(fpath_res + '\\' +'Result_run02_byPol.csv')
	result_run06 = pd.read_csv(fpath_res + '\\' +'Result_run06_byPol.csv')

	df = pd.merge(result_run02, result_run06,
				on = ['POL_L_ID', 'AGE', 'SEX', 'SMOKER_IND','AGE-GROUP', 'LIFE_SI_BAND','TPD_SI_BAND',
					'TRA_SI_BAND','IP_SI_BAND', 'L_LIFE_ID', 'POL_NUMBER'],
				how = 'outer', suffixes=('_R02', '_R06'))


elif policy_subset == 'super-cover':
	result_run02 = pd.read_csv(fpath_res + '\\' +'Result_run02_byPol.csv')
	result_run06 = pd.read_csv(fpath_res + '\\' +'Result_run06_byPol.csv')

	df = pd.merge(result_run02, result_run06, 
				on = ['POL_L_ID', 'AGE', 'SEX', 'SMOKER_IND','AGE-GROUP', 'LIFE_SI_BAND','TPD_SI_BAND',
					'IP_SI_BAND', 'L_LIFE_ID', 'POL_NUMBER'],
				how = 'outer', suffixes=('_R02', '_R06'))


df = df.rename(columns={'PACKAGE_R02': 'PACKAGE'})

# rates change from x to x+1 anniversary on 4A series
df['%Ch_Tot_Prem1(R02)_toPrem13(R02)'] = df.apply(lambda row:
						percCalc(row['TOT_PREM1_R02'], row['TOT_PREM_13_R02']), axis=1)
df['$Diff_Tot_Prem1(R02)_toPrem13(R02)'] = df.apply(lambda row:
						DollarDiffCalc(row['TOT_PREM1_R02'], row['TOT_PREM_13_R02']), axis=1)

# rates change from x+1 anniversary to x+2 anniversary on 4A series
df['%Ch_Tot_Prem13(R02)_toPrem25(R02)'] = df.apply(lambda row:
						percCalc(row['TOT_PREM13_R02'], row['TOT_PREM_25_R02']), axis=1)
df['$Diff_Tot_Prem13(R02)_toPrem25(R02)'] = df.apply(lambda row:
						DollarDiffCalc(row['TOT_PREM13_R02'], row['TOT_PREM_25_R02']), axis=1)


# rates change from x on 4A to x+1 anniversary on 5A
df['%Ch_Tot_Prem1(R02)_toPrem13(R06)'] = df.apply(lambda row:
						percCalc(row['TOT_PREM1_R02'], row['TOT_PREM_13_R06']), axis=1)
df['$Diff_Tot_Prem1(R02)_toPrem13(R06)'] = df.apply(lambda row:
						DollarDiffCalc(row['TOT_PREM1_R02'], row['TOT_PREM_13_R06']), axis=1)

# rates change from x+1 on 4A to x+2 anniversary on 5A
df['%Ch_Tot_Prem13(R02)_toPrem25(R06)'] = df.apply(lambda row:
						percCalc(row['TOT_PREM13_R02'], row['TOT_PREM_25_R06']), axis=1)
df['$Diff_Tot_Prem13(R02)_toPrem25(R06)'] = df.apply(lambda row:
						DollarDiffCalc(row['TOT_PREM13_R02'], row['TOT_PREM_25_R06']), axis=1)


# rates change from 4A next anniversary to 5A next anniversary
df['%Ch_Total_Prem_13(R02vsR06)'] = df.apply(lambda row:
						percCalc(row['TOT_PREM_13_R02'], row['TOT_PREM_13_R06']), axis=1)
df['$Diff_Total_Prem_13(R02vsR06)'] = df.apply(lambda row:
						DollarDiffCalc(row['TOT_PREM_13_R02'], row['TOT_PREM_13_R06']), axis=1)

# rates change from 4A next anniversary to 5A next anniversary
df['%Ch_Total_Prem_25(R02vsR06)'] = df.apply(lambda row:
						percCalc(row['TOT_PREM_25_R02'], row['TOT_PREM_25_R06']), axis=1)
df['$Diff_Total_Prem_25(R02vsR06)'] = df.apply(lambda row:
						DollarDiffCalc(row['TOT_PREM_25_R02'], row['TOT_PREM_25_R06']), axis=1)

# rates change from 4A x+2 anniversary to 5A x+2 anniversary


bins_perc_ch = [-10,-0.3, -0.2, -0.1, 0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 10]
perc_label = ['(> -30%)', '[-30%;-20%)', '[-20% ;-10%)', '[-10%; 0%)',
						'[0%; 10%)', '[10%; 20%)', '[20%; 30%)', '[30%; 40%)', '[40%; 50%)',
						'[50%; 60%)', '[> 60%)']

# assigning discounts according to specified bins
df.ix[:,'%bin_Tot_Prem(R02)_toPrem13(R02)'] = pd.cut(df['%Ch_Tot_Prem1(R02)_toPrem13(R02)'],
                                                    bins_perc_ch, labels = perc_label)
df.ix[:,'%bin_Total_Prem_13(R02vsR06)'] = pd.cut(df['%Ch_Total_Prem_13(R02vsR06)'],
                                                    bins_perc_ch, labels = perc_label)
df.ix[:,'%bin_Total_Prem_13(R06vsR02)'] = pd.cut(df['%Ch_Total_Prem_13(R06vsR02)'],
                                                    bins_perc_ch, labels = perc_label)


df.reset_index()
df = df.rename(columns= {'Mnth Inception_R02':'Mnth Benef start', 'TOT_POL_FEE_R02':'TOT_POL_FEE',
						'Entry Mnth_R02':'Entry Mnth', 'Entry Year_R02':'Entry Year'})


if ind_run_no == 0:

	if indicator_print == 'full print':

		# # rates change from 4A to 5A
		# df['%Ch_Total_Prem(R02vsR06)'] = df.apply(lambda row:
		# 						percCalc(row['TOT_PREM1_R02'], row['TOT_PREM1_R06']), axis=1)
		# df['$Diff_Total_Prem(R02vsR06)'] = df.apply(lambda row:
		# 						DollarDiffCalc(row['TOT_PREM1_R02'], row['TOT_PREM1_R06']), axis=1)


		# # rates change from 4A to 5A next anniversary
		# df['%Ch_Tot_Prem(R02)_toPrem13(R06)'] = df.apply(lambda row:
		# 						percCalc(row['TOT_PREM1_R02'], row['TOT_PREM_13_R06']), axis=1)
		# df['$Diff_Tot_Prem(R02)_toPrem13(R06)'] = df.apply(lambda row:
		# 						DollarDiffCalc(row['TOT_PREM1_R02'], row['TOT_PREM_13_R06']), axis=1)


		# rates change from 5A to 5A next anniversary
		# df['%Ch_Tot_Prem(R06)_toPrem13(R06)'] = df.apply(lambda row:
		# 						percCalc(row['TOT_PREM1_R06'], row['TOT_PREM_13_R06']), axis=1)
		# df['$Diff_Tot_Prem(R06)_toPrem13(R06)'] = df.apply(lambda row:
		# 						DollarDiffCalc(row['TOT_PREM1_R06'], row['TOT_PREM_13_R06']), axis=1)

		# 	assigning bins
		# df.ix[:,'%bin_Tot_Prem(R02)_toPrem13(R06)'] = pd.cut(df['%Ch_Tot_Prem(R02)_toPrem13(R06)'],
		#                                                     bins_perc_ch, labels = perc_label)

		# df.ix[:,'%bin_Tot_Prem(R06)_toPrem13(R06)'] = pd.cut(df['%Ch_Tot_Prem(R06)_toPrem13(R06)'],
		#                                                     bins_perc_ch, labels = perc_label)
		# df.ix[:,'%bin_Total_Prem(R02vsR06)'] = pd.cut(df['%Ch_Total_Prem(R02vsR06)'],
		#                                                     bins_perc_ch, labels = perc_label)

		if policy_subset == 'ordinary-cover':
			
			cols = ['L_LIFE_ID', 'POL_NUMBER', 'Mnth Benef start', 'Entry Mnth', 'Entry Year', 'TOT_POL_FEE',
				 'PACKAGE', 'AGE', 'SEX', 'SMOKER_IND', 'AGE-GROUP', 
				'TOT_PREM1_R02', 'TOT_PREM1_R06','TOT_PREM_13_R02', 'TOT_PREM_13_R06','TOT_BEL_R02', 'TOT_BEL_R06',
				'%Ch_Total_Prem(R02vsR06)', '$Diff_Total_Prem(R02vsR06)', '%bin_Total_Prem(R02vsR06)',
			 	'%Ch_Total_Prem_13(R02vsR06)', '$Diff_Total_Prem_13(R02vsR06)', '%bin_Total_Prem_13(R02vsR06)',
			 	'%Ch_Total_Prem_13(R06vsR02)', '$Diff_Total_Prem_13(R06vsR02)', '%bin_Total_Prem_13(R06vsR02)',
			 	'%Ch_Tot_Prem(R02)_toPrem13(R06)', '$Diff_Tot_Prem(R02)_toPrem13(R06)', '%bin_Tot_Prem(R02)_toPrem13(R06)',
			 	'%Ch_Tot_Prem1(R02)_toPrem13(R02)','$Diff_Tot_Prem1(R02)_toPrem13(R02)', '%bin_Tot_Prem(R02)_toPrem13(R02)',
			 	'%Ch_Tot_Prem(R06)_toPrem13(R06)','$Diff_Tot_Prem(R06)_toPrem13(R06)', '%bin_Tot_Prem(R06)_toPrem13(R06)',

			 	'TOT_BE_GRRESERVE_R02', 'TOT_PV_GR_CARR_R02',


				'SA_LIFE_R02',  'A_PREM_LIFE_R02', 'A_PREM_13_LIFE_R02', 'BEL_LIFE_R02',
				'SA_LIFE_R06', 'A_PREM_LIFE_R06', 'A_PREM_13_LIFE_R06', 'BEL_LIFE_R06',
				'SA_TRA_R02', 'A_PREM_TRA_R02', 'A_PREM_13_TRA_R02',  'BEL_TRA_R02',
				'SA_TRA_R06', 'A_PREM_TRA_R06', 'A_PREM_13_TRA_R06', 'BEL_TRA_R06',
				'SA_TPD_R02', 'A_PREM_TPD_R02', 'A_PREM_13_TPD_R02', 'BEL_TPD_R02',
				'SA_TPD_R06', 'A_PREM_TPD_R06', 'A_PREM_13_TPD_R06', 'BEL_TPD_R06',
				'SA_IP_R02', 'A_PREM_IP_R02', 'A_PREM_13_IP_R02', 'BEL_IP_R02',  
			 	'SA_IP_R06', 'A_PREM_IP_R06', 'A_PREM_13_IP_R06', 'BEL_IP_R06',
				'LIFE_SI_BAND', 'TRA_SI_BAND', 'TPD_SI_BAND', 'IP_SI_BAND',

				'BROKER_LIFE_R02', 'CHANNEL_LIFE_R02', 'BROKER_IP_R02', 'CHANNEL_IP_R02',
				'BROKER_TPD_R02', 'CHANNEL_TPD_R02', 'BROKER_TRA_R02', 'CHANNEL_TRA_R02']


		elif policy_subset == 'super-cover':

			cols = ['L_LIFE_ID','POL_NUMBER', 'Mnth Benef start',  'Entry Mnth', 'Entry Year',
				'PACKAGE', 'AGE', 'SEX', 'SMOKER_IND', 'AGE-GROUP', 
				'TOT_PREM1_R02', 'TOT_PREM1_R06','TOT_PREM_13_R02', 'TOT_PREM_13_R06','TOT_BEL_R02', 'TOT_BEL_R06',
				'%Ch_Total_Prem(R02vsR06)', '$Diff_Total_Prem(R02vsR06)', '%bin_Total_Prem(R02vsR06)',
			 	'%Ch_Total_Prem_13(R02vsR06)', '$Diff_Total_Prem_13(R02vsR06)', '%bin_Total_Prem_13(R02vsR06)',
			 	'%Ch_Tot_Prem(R02)_toPrem13(R06)', '$Diff_Tot_Prem(R02)_toPrem13(R06)', '%bin_Tot_Prem(R02)_toPrem13(R06)',
			 	'%Ch_Tot_Prem1(R02)_toPrem13(R02)','$Diff_Tot_Prem1(R02)_toPrem13(R02)', '%bin_Tot_Prem(R02)_toPrem13(R02)',
			 	
			 	'TOT_BE_GRRESERVE_R02', 'TOT_PV_GR_CARR_R02',

				'SA_LIFE_R02',  'A_PREM_LIFE_R02', 'A_PREM_13_LIFE_R02', 'BEL_LIFE_R02',
				'SA_LIFE_R06', 'A_PREM_LIFE_R06', 'A_PREM_13_LIFE_R06', 'BEL_LIFE_R06',
				
				'SA_TPD_R02', 'A_PREM_TPD_R02', 'A_PREM_13_TPD_R02', 'BEL_TPD_R02',
				'SA_TPD_R06', 'A_PREM_TPD_R06', 'A_PREM_13_TPD_R06', 'BEL_TPD_R06',
				'SA_IP_R02', 'A_PREM_IP_R02', 'A_PREM_13_IP_R02', 'BEL_IP_R02',  
			 	'SA_IP_R06', 'A_PREM_IP_R06', 'A_PREM_13_IP_R06', 'BEL_IP_R06',
				'LIFE_SI_BAND', 'TPD_SI_BAND', 'IP_SI_BAND']
	else:
		if policy_subset == 'ordinary-cover':
			cols = ['L_LIFE_ID', 'POL_NUMBER', 'Mnth Benef start', 'Entry Mnth', 'Entry Year', 'TOT_POL_FEE',
				 'PACKAGE', 'AGE', 'SEX', 'SMOKER_IND', 'AGE-GROUP', 
				
				'TOT_PREM1_R02', 'TOT_PREM_13_R02', 'TOT_PREM_13_R06',
				'%Ch_Tot_Prem1(R02)_toPrem13(R02)','$Diff_Tot_Prem1(R02)_toPrem13(R02)', '%bin_Tot_Prem(R02)_toPrem13(R02)',
			 	'%Ch_Total_Prem_13(R02vsR06)', '$Diff_Total_Prem_13(R02vsR06)', '%bin_Total_Prem_13(R02vsR06)',
			 	'%Ch_Total_Prem_13(R06vsR02)', '$Diff_Total_Prem_13(R06vsR02)', '%bin_Total_Prem_13(R06vsR02)',
				
				'TOT_BE_GRRESERVE_R02', 'TOT_PV_GR_CARR_R02',
			 	
			 	'SA_LIFE_R02',  'A_PREM_LIFE_R02', 'A_PREM_13_LIFE_R02', 
				'SA_LIFE_R06', 'A_PREM_LIFE_R06', 'A_PREM_13_LIFE_R06', 

				'SA_TRA_R02', 'A_PREM_TRA_R02', 'A_PREM_13_TRA_R02',  
				'SA_TRA_R06', 'A_PREM_TRA_R06', 'A_PREM_13_TRA_R06', 

				'SA_TPD_R02', 'A_PREM_TPD_R02', 'A_PREM_13_TPD_R02', 
				'SA_TPD_R06', 'A_PREM_TPD_R06', 'A_PREM_13_TPD_R06', 

				'SA_IP_R02', 'A_PREM_IP_R02', 'A_PREM_13_IP_R02', 
			 	'SA_IP_R06', 'A_PREM_IP_R06', 'A_PREM_13_IP_R06',
			 	 
				'LIFE_SI_BAND', 'TRA_SI_BAND', 'TPD_SI_BAND', 'IP_SI_BAND',

				'BROKER_LIFE_R02', 'CHANNEL_LIFE_R02', 'BROKER_IP_R02', 'CHANNEL_IP_R02',
				'BROKER_TPD_R02', 'CHANNEL_TPD_R02', 'BROKER_TRA_R02', 'CHANNEL_TRA_R02']


		elif policy_subset == 'super-cover':
			cols = ['L_LIFE_ID', 'POL_NUMBER', 'Mnth Benef start', 'Entry Mnth', 'Entry Year', 'TOT_POL_FEE',
				 'PACKAGE', 'AGE', 'SEX', 'SMOKER_IND', 'AGE-GROUP', 
				
				'TOT_PREM1_R02', 'TOT_PREM_13_R02', 'TOT_PREM_13_R06',
				'%Ch_Tot_Prem1(R02)_toPrem13(R02)','$Diff_Tot_Prem1(R02)_toPrem13(R02)', '%bin_Tot_Prem(R02)_toPrem13(R02)',
			 	'%Ch_Total_Prem_13(R02vsR06)', '$Diff_Total_Prem_13(R02vsR06)', '%bin_Total_Prem_13(R02vsR06)',
			 	# '%Ch_Total_Prem_13(R06vsR02)', '$Diff_Total_Prem_13(R06vsR02)', '%bin_Total_Prem_13(R06vsR02)',

			 	'TOT_BE_GRRESERVE_R02', 'TOT_PV_GR_CARR_R02',
			 
				'SA_LIFE_R02',  'A_PREM_LIFE_R02', 'A_PREM_13_LIFE_R02',
				'SA_LIFE_R06', 'A_PREM_LIFE_R06', 'A_PREM_13_LIFE_R06',
				'SA_TPD_R02', 'A_PREM_TPD_R02', 'A_PREM_13_TPD_R02',
				'SA_TPD_R06', 'A_PREM_TPD_R06', 'A_PREM_13_TPD_R06',
				'SA_IP_R02', 'A_PREM_IP_R02', 'A_PREM_13_IP_R02',
			 	'SA_IP_R06', 'A_PREM_IP_R06', 'A_PREM_13_IP_R06',
				'LIFE_SI_BAND', 'TPD_SI_BAND', 'IP_SI_BAND']


	df = df[cols]

	df = df.rename(columns= {
							# '%Ch_Total_Prem_13(R06vsR02)':'%Ch_Prem25_to_Prem13',
							# '$Diff_Total_Prem_13(R06vsR02)':'$Diff_Prem25_to_Prem13',
							# '%bin_Total_Prem_13(R06vsR02)':'%bin_Prem25_to_Prem13',
						

							'%Ch_Total_Prem_13(R02vsR06)':'%Ch_Prem13_to_Prem25',
							'$Diff_Total_Prem_13(R02vsR06)':'$Diff_Prem13_to_Prem25',
							'%bin_Total_Prem_13(R02vsR06)':'%bin_Prem13_to_Prem25',
							
							'TOT_PREM1_R02':'TOT_PREM1',
							'TOT_PREM_13_R02':'TOT_PREM_13',
							'TOT_PREM_13_R06':'TOT_PREM_25',

							'%Ch_Tot_Prem1(R02)_toPrem13(R02)':'%Ch_Tot_Prem1_toPrem13',
							'$Diff_Tot_Prem1(R02)_toPrem13(R02)':'$Diff_Tot_Prem1_toPrem13',
							'%bin_Tot_Prem(R02)_toPrem13(R02)':'%bin_Tot_Prem1_toPrem13',


							'SA_LIFE_R02':'SA_LIFE',  'A_PREM_LIFE_R02':'A_PREM_LIFE', 'A_PREM_13_LIFE_R02':'A_PREM_13_LIFE',  'A_PREM_13_LIFE_R06':'A_PREM_25_LIFE', 
							'SA_TRA_R02':'SA_TRA', 'A_PREM_TRA_R02':'A_PREM_TRA', 'A_PREM_13_TRA_R02':'A_PREM_13_TRA', 'A_PREM_13_TRA_R06':'A_PREM_25_TRA', 
							'SA_TPD_R02':'SA_TPD', 'A_PREM_TPD_R02':'A_PREM_TPD', 'A_PREM_13_TPD_R02':'A_PREM_13_TPD', 'A_PREM_13_TPD_R06':'A_PREM_25_TPD',
							'SA_IP_R02':'SA_IP', 'A_PREM_IP_R02':'A_PREM_IP', 'A_PREM_13_IP_R02':'A_PREM_13', 'A_PREM_13_IP_R06':'A_PREM_25_IP',

			 

						})

if ind_run_no == 1:
	df = df.rename(columns= {

						'TOT_BE_GRRESERVE_R02':'TOT_BE_GRRESERVE', 
						'TOT_PV_GR_CARR_R02':'TOT_PV_GR_CARR',


						'%Ch_Total_Prem_13(R02vsR06)':'%Ch_Prem13_to_Prem25',
						'$Diff_Total_Prem_13(R02vsR06)':'$Diff_Prem13_to_Prem25',
						'%bin_Total_Prem_13(R02vsR06)':'%bin_Prem13_to_Prem25',
						
						'TOT_PREM1_R02':'TOT_PREM1',
						'TOT_PREM_13_R02':'TOT_PREM_13',
						'TOT_PREM_13_R06':'TOT_PREM_25',

						'%Ch_Tot_Prem1(R02)_toPrem13(R02)':'%Ch_Tot_Prem1_toPrem13',
						'$Diff_Tot_Prem1(R02)_toPrem13(R02)':'$Diff_Tot_Prem1_toPrem13',
						'%bin_Tot_Prem(R02)_toPrem13(R02)':'%bin_Tot_Prem1_toPrem13',


						'SA_LIFE_R02':'SA_LIFE',  'A_PREM_LIFE_R02':'A_PREM_LIFE', 'A_PREM_13_LIFE_R02':'A_PREM_13_LIFE',  'A_PREM_13_LIFE_R06':'A_PREM_25_LIFE', 
						'SA_TRA_R02':'SA_TRA', 'A_PREM_TRA_R02':'A_PREM_TRA', 'A_PREM_13_TRA_R02':'A_PREM_13_TRA', 'A_PREM_13_TRA_R06':'A_PREM_25_TRA', 
						'SA_TPD_R02':'SA_TPD', 'A_PREM_TPD_R02':'A_PREM_TPD', 'A_PREM_13_TPD_R02':'A_PREM_13_TPD', 'A_PREM_13_TPD_R06':'A_PREM_25_TPD',
						'SA_IP_R02':'SA_IP', 'A_PREM_IP_R02':'A_PREM_IP', 'A_PREM_13_IP_R02':'A_PREM_13', 'A_PREM_13_IP_R06':'A_PREM_25_IP',

					})


	if policy_subset == 'ordinary-cover':
			cols = ['L_LIFE_ID', 'POL_NUMBER', 'Mnth Benef start', 'Entry Mnth', 'Entry Year', 'TOT_POL_FEE',
				 'PACKAGE', 'AGE', 'SEX', 'SMOKER_IND', 'AGE-GROUP', 
				
				'TOT_PREM1', 'TOT_PREM_13', 'TOT_PREM_25',

				'%Ch_Tot_Prem1_toPrem13',
				'$Diff_Tot_Prem1_toPrem13',
				'%bin_Tot_Prem1_toPrem13',


				'%Ch_Prem13_to_Prem25',
				'$Diff_Prem13_to_Prem25',
				'%bin_Prem13_to_Prem25',

				
				'TOT_BE_GRRESERVE', 'TOT_PV_GR_CARR',


				'SA_LIFE', 'A_PREM_LIFE', 'A_PREM_13_LIFE', 'A_PREM_25_LIFE', 
				'SA_TRA', 'A_PREM_TRA', 'A_PREM_13_TRA', 'A_PREM_25_TRA', 
				'SA_TPD', 'A_PREM_TPD', 'A_PREM_13_TPD', 'A_PREM_25_TPD',
				'SA_IP', 'A_PREM_IP', 'A_PREM_13', 'A_PREM_25_IP',

				'LIFE_SI_BAND', 'TRA_SI_BAND', 'TPD_SI_BAND', 'IP_SI_BAND',

				'BROKER_LIFE_R02', 'CHANNEL_LIFE_R02', 'BROKER_IP_R02', 'CHANNEL_IP_R02',
				'BROKER_TPD_R02', 'CHANNEL_TPD_R02', 'BROKER_TRA_R02', 'CHANNEL_TRA_R02']


	elif policy_subset == 'super-cover':

			cols = ['L_LIFE_ID', 'POL_NUMBER', 'Mnth Benef start', 'Entry Mnth', 'Entry Year', 'TOT_POL_FEE',
				 'PACKAGE', 'AGE', 'SEX', 'SMOKER_IND', 'AGE-GROUP', 
				
				'TOT_PREM1', 'TOT_PREM_13', 'TOT_PREM_25',

				'%Ch_Tot_Prem1_toPrem13',
				'$Diff_Tot_Prem1_toPrem13',
				'%bin_Tot_Prem1_toPrem13',


				'%Ch_Prem13_to_Prem25',
				'$Diff_Prem13_to_Prem25',
				'%bin_Prem13_to_Prem25',

				
				'TOT_BE_GRRESERVE', 'TOT_PV_GR_CARR',


				'SA_LIFE', 'A_PREM_LIFE', 'A_PREM_13_LIFE', 'A_PREM_25_LIFE', 
				'SA_TPD', 'A_PREM_TPD', 'A_PREM_13_TPD', 'A_PREM_25_TPD',
				'SA_IP', 'A_PREM_IP', 'A_PREM_13', 'A_PREM_25_IP',

				'LIFE_SI_BAND', 'TPD_SI_BAND', 'IP_SI_BAND',

				'BROKER_LIFE_R02', 'CHANNEL_LIFE_R02', 'BROKER_IP_R02', 'CHANNEL_IP_R02',
				'BROKER_TPD_R02', 'CHANNEL_TPD_R02']

	df = df[cols]

df.sort_values(['L_LIFE_ID'], inplace = True)

if policy_subset == 'ordinary-cover':
	df.to_csv(fpath_res + '\\' +'Combined_byPol_wBROKER.csv', index = False)
elif policy_subset == 'super-cover':
	df.to_csv(fpath_res + '\\' +'Combined_byPol_wBROKER-sup.csv', index = False)

print 'file saved to: ', fpath_res
print ('end of script')