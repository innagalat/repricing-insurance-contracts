#!/usr/bin/env python

import pandas as pd
import numpy as np
import os

# choose the run that is required and run them separately
run_name = '0.repricing-run02'   
# run_name = '0.repricing-run06'
 
policy_subset = 'super-cover'
# policy_subset = 'ordinary-cover'

ind_extra_col = True
# ind_extra_col = False

if ind_extra_col == True:
    extra_col = ['BE_GRRESERVE', 'PV_GR_CARR', 'ANNUAL_PREM_25' ]

if run_name == '0.repricing-run02': #a condition on run02
    # run2
    col_list_res_ls = ['AGE_AT_ENTRY','ANNUAL_PREM_1','BE_RESERVE', 'L_LIFE_ID',
                    'POL_NUMBER','PREMIUM_TYPE','SUM_ASSURED','OAP', 
                    'POL_FEE_PP','ENTRY_MONTH', 'ENTRY_YEAR', 'ANNUAL_PREM_25',
                    'ANNUAL_PREM_13']

    col_list_res_ip = ['AGE_AT_ENTRY', 'POL_NUMBER','SUM_INS', 'B_BEN_NO',
                    'MOS_PV_PREM', 'BE_RESERVE', 'L_LIFE_ID', 'ANNUAL_PREM_1', 
                    'POLICY_FEE','ENTRY_MONTH', 'ENTRY_YEAR', 'ANNUAL_PREM_24',
                    'ANNUAL_PREM_12']

    columns_to_rename_ls = {'SUM_ASSD_PP':'SUM_ASSURED','OAP':'B_OFF_APREM','POL_FEE_PP':'POLICY_FEE'}

# Since ls and ip uses different names for the same columns,
# we need to rename columns to bring them to the same standard

    columns_to_rename_ip_res = {'SUM_INS':'SUM_ASSURED','ANNUAL_PREM':'B_OFF_APREM',
                                'ANNUAL_PREM_01':'ANNUAL_PREM_1','ANN_PHI_BEN':'SUM_ASSURED',
                                'ANNUAL_PREM_12':'ANNUAL_PREM_13'}

    columns_to_rename_ip_mpf = {'SUM_INS':'SUM_ASSURED','ANNUAL_PREM':'B_OFF_APREM',
                                'ANNUAL_PREM_12':'ANNUAL_PREM_13','ANNUAL_PREM_01':'ANNUAL_PREM_1',
                                'ANN_PHI_BEN':'SUM_ASSURED'}

    #end run2

if run_name == '0.repricing-run06':
    col_list_res_ls = ['AGE_AT_ENTRY','ANNUAL_PREM_1','BE_RESERVE', 'L_LIFE_ID',
                    'POL_NUMBER','PREMIUM_TYPE','SUM_ASSURED','OAP', 
                    'POL_FEE_PP','ENTRY_MONTH', 'ENTRY_YEAR', 'ANNUAL_PREM_25']

    col_list_res_ip = ['AGE_AT_ENTRY', 'POL_NUMBER','SUM_INS', 'B_BEN_NO',
                    'MOS_PV_PREM', 'BE_RESERVE', 'L_LIFE_ID', 'ANNUAL_PREM_1',  
                    'POLICY_FEE','ENTRY_MONTH', 'ENTRY_YEAR', 'ANNUAL_PREM_24']

# Since ls and ip uses different names for the same columns,
# we need to rename columns to bring them to the same standard

    columns_to_rename_ls = {'SUM_ASSD_PP':'SUM_ASSURED','OAP':'B_OFF_APREM','POL_FEE_PP':'POLICY_FEE', 
                            'ANNUAL_PREM_25':'ANNUAL_PREM_13'}

    columns_to_rename_ip_res = {'SUM_INS':'SUM_ASSURED','ANNUAL_PREM':'B_OFF_APREM',
                                'ANNUAL_PREM_01':'ANNUAL_PREM_1','ANN_PHI_BEN':'SUM_ASSURED',
                                'ANNUAL_PREM_24':'ANNUAL_PREM_13'}

    columns_to_rename_ip_mpf = {#'SUM_INS':'SUM_ASSURED',
                                'ANNUAL_PREM':'B_OFF_APREM',
                                #'ANNUAL_PREM_12':'ANNUAL_PREM_13','ANNUAL_PREM_01':'ANNUAL_PREM_1',
                                'ANN_PHI_BEN':'SUM_ASSURED'} 

# adding extra columns
if ind_extra_col == True:
    for i in range(len(extra_col)):
        col_list_res_ls.append(extra_col[i])
        col_list_res_ip.append(extra_col[i])


print ('col list res ls: '),col_list_res_ls
print ('col list res ip: '),col_list_res_ip


col_list_mpf_ls = ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY',
                'DURATIONIF_M', 'SEX','SMOKER_IND', 'SUM_ASSURED', 'B_BEN_NO',
                'B_OFF_APREM','PREMIUM_TYPE', 'POLICY_FEE',
                'BROKER', 'DCS_REIN_SI', 'CHANNEL']

col_list_mpf_ip = ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY', 'PREMIUM_TYPE',
                'DURATIONIF_M', 'SEX','SMOKER_IND',
                'ANN_PHI_BEN', 'B_BEN_NO', 'ANNUAL_PREM',
                'BENEFIT_CODE', 'DEFER_PER_MP', 'TOTAL_SI', 'BEN_PERIOD', 'OCC_CLASS', 
                'DII_TYPE', 'BROKER', 'OTR_ANNPHIBEN', 'CHANNEL']


# Specify main path of the project
PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data', policy_subset)
RESULT = os.path.join(PROJ, 'results', policy_subset)

f_run_name = run_name[-5:]#last 5 letters
print f_run_name

def as_integer(value):
    try:
        int(value)
        return int(value)
    except:
        return np.nan

# This function checks that specified path 'fpath' exists and if it isn't,
# prints a msg that path is not found
def read_if_exists(fpath):
    if os.path.exists(fpath) == True:
        df = pd.read_csv(fpath)
        return(df)
    else:
        print 'File ' + fpath + ' not found'
        exit(1)

# This function calls function that checks path existence,
# reads files into data frame using the specified columns (col_list), given that path exists

def read_and_slice(nfold, nfile, col_list):
    path = os.path.join(PROJ, DATA, nfold, nfile)
    df = read_if_exists(path)
    
    missing_col_list = []
    for i in col_list:
        if i not in df.columns:
            missing_col_list.append(i)
    
    if missing_col_list:     
        print ('Please check data') 
        print (' columns that do not exist in the file: '), nfile
        print missing_col_list

    df = df.ix[:, col_list]
    return(df)


# This function checks 4 columns: POL_NUMBER, B_BEN_NO, PREMIUM_TYPE, L_LIFE_ID
# and removes unnecessary symbols by converting PREMIUM_TYPE column to string type
# and other three columns to integer type 

def read_and_check(run_name, filename, col_list1):    
    df1 = read_and_slice(run_name, filename, col_list1)
    
    if type(df1.ix[0,'POL_NUMBER']) == str:  
        if '"' in df1.ix[0,'POL_NUMBER']:
            df1.ix[:,'POL_NUMBER'] = [x.replace('"', '') for x in df1.ix[:,'POL_NUMBER']]
        if ' ' in df1.ix[0,'POL_NUMBER']:
            df1.ix[:,'POL_NUMBER'] = [x.replace(' ', '') for x in df1.ix[:,'POL_NUMBER']]

        df1.ix[:,'POL_NUMBER'] = [as_integer(x) for x in df1.ix[:, 'POL_NUMBER']]
    
    if 'B_BEN_NO' in col_list1:
        if type(df1.ix[0,'B_BEN_NO']) == str:
            if '"' in df1.ix[0,'B_BEN_NO']:
                df1.ix[:,'B_BEN_NO'] = [x.replace('"', '') for x in df1.ix[:,'B_BEN_NO']]
            if ' ' in df1.ix[0,'B_BEN_NO']:
                df1.ix[:,'B_BEN_NO'] = [x.replace(' ', '') for x in df1.ix[:,'B_BEN_NO']]
        df1.ix[:,'B_BEN_NO'] = [as_integer(x) for x in df1.ix[:, 'B_BEN_NO']]

    if 'L_LIFE_ID' in col_list1:
        if type(df1.ix[0,'L_LIFE_ID']) == str:
            if '"' in df1.ix[0,'L_LIFE_ID']:
                df1.ix[:,'L_LIFE_ID'] = [x.replace('"', '') for x in df1.ix[:,'L_LIFE_ID']]
            if ' ' in df1.ix[0,'L_LIFE_ID']:
                df1.ix[:,'L_LIFE_ID'] = [x.replace(' ', '') for x in df1.ix[:,'L_LIFE_ID']]

        if type(df1.ix[0,'L_LIFE_ID']) != int:
            df1.ix[:,'L_LIFE_ID'] = [as_integer(x) for x in df1.ix[:, 'L_LIFE_ID']]

    if 'PREMIUM_TYPE' in col_list1:
        if type(df1.ix[0,'PREMIUM_TYPE']) == str:
            if '"' in df1.ix[0,'PREMIUM_TYPE']:
                df1.ix[:,'PREMIUM_TYPE'] = [x.replace('"', '') for x in df1.ix[:,'PREMIUM_TYPE']]
            if ' ' in df1.ix[0,'PREMIUM_TYPE']:
                df1.ix[:,'PREMIUM_TYPE'] = [x.replace(' ', '') for x in df1.ix[:,'PREMIUM_TYPE']]

    return(df1)


# This function is used to add values to main df from second df
# using key = ['POL_NUMBER', 'L_LIFE_ID','SUM_ASSURED','B_BEN_NO','AGE_AT_ENTRY'])
def merge_by_id(df_a, df_b):
    return(pd.merge(df_a, df_b, how = 'outer', 
            on = ['POL_NUMBER', 'L_LIFE_ID','SUM_ASSURED','B_BEN_NO','AGE_AT_ENTRY']))

 



# Main body of the script
print ('start of program')
# Setting path to the location of the script 
print os.getcwd()

# Specifying columns that are used when data is read.
# ATT! These are the columns that are used by script,
# therefore, they must be present in the data files and named exactly as here.
# If there is a discrepancy in naming, script might give incorrect results.

# Specyfing columns allows to truncate initial files and to use less memory
# col_list_res_ls = ['AGE_AT_ENTRY','ANNUAL_PREM_1','BE_RESERVE', 'L_LIFE_ID',
#                 'POL_NUMBER','PREMIUM_TYPE','SUM_ASSURED','OAP', 'ANNUAL_PREM_13', 
#                 'POL_FEE_PP','ENTRY_MONTH', 'ENTRY_YEAR']
# col_list_mpf_ls = ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY',
#                 'DURATIONIF_M', 'SEX','SMOKER_IND', 'SUM_ASSURED', 'B_BEN_NO',
#                 'B_OFF_APREM','PREMIUM_TYPE', 'POLICY_FEE',
#                 'BROKER', 'DCS_REIN_SI', 'CHANNEL']

# col_list_res_ip = ['AGE_AT_ENTRY', 'POL_NUMBER','SUM_INS', 'B_BEN_NO',
#                 'MOS_PV_PREM','ANNUAL_PREM_12', 'BE_RESERVE', 'L_LIFE_ID', 'ANNUAL_PREM_1',  
#                 'POLICY_FEE','ENTRY_MONTH', 'ENTRY_YEAR'] 

#TOTAL_SI needs to be removed from all files.. do it later


# Creating empty dictionaries to add values later on
death_res_csv_list = []
tra_res_csv_list= []
tpd_res_csv_list = []
ip_res_csv_list = []

death_mpf_csv_list = []
tra_mpf_csv_list = []
tpd_mpf_csv_list = []
ip_mpf_csv_list = []

# Rather than specifying number of the initial files with data, we use FOR LOOP to read dynamically all files in the directory/folder.
# Categorising csv files based on the file type, such as death, tra, tpd, ip.
# Adding them to emply dictionaries/lists
print os.path.join(PROJ, DATA, run_name)
for filename in os.listdir(os.path.join(PROJ, DATA, run_name)):
  if filename[:4] == 'COCD':
      death_res_csv_list.append(filename)
  if filename[:4] == 'COCT':
      tra_res_csv_list.append(filename)
  if filename[:4] == 'COCP':
      tpd_res_csv_list.append(filename)
  if filename[:4] == 'PAOC':
      ip_res_csv_list.append(filename)
  # add a print message saying that :print('check that files are located at '), os.path.join(PROJ, DATA, run_name) 

for filename in os.listdir(os.path.join(PROJ, DATA, '0.mpf')):
  if filename[:4] == 'COCD':
      death_mpf_csv_list.append(filename)
  if filename[:4] == 'COCT':
      tra_mpf_csv_list.append(filename)
  if filename[:4] == 'COCP':
      tpd_mpf_csv_list.append(filename)
  if filename[:4] == 'PAOC':
      ip_mpf_csv_list.append(filename) 
 # add a print message saying that :print('check that files are located at '), os.path.join(PROJ, DATA, run_name) 


# printing lists for the visual check
print death_res_csv_list, tra_res_csv_list, tpd_res_csv_list, ip_res_csv_list
print death_mpf_csv_list, tra_mpf_csv_list, tpd_mpf_csv_list, ip_mpf_csv_list




# This is main function that calls 'read_and_check' function for the each file
# and merges every file to one summary file (Death, TPD, TRA, IP)
def concat_by_type(folderdir, csv_list, col_list, columns_to_rename):
    appended_data = []
    size_count = 0

    for item in csv_list:
        df_res = read_and_check(folderdir, item, col_list)
        appended_data.append(df_res)
        #additional check-variables
        size_count = size_count + df_res.shape[0]
    
    df = pd.concat(appended_data, axis=0, ignore_index=True)
    df = df.rename(columns = columns_to_rename)
    print
    print item
    print ('count of rows: '), size_count
    print ('shape-rows   : '), df.shape[0], ('shape-columns: '), df.shape[1]
    return df

# ls columns :                          df = df.rename(columns = {'SUM_ASSD_PP':'SUM_ASSURED','OAP':'B_OFF_APREM',
                                        # 'POL_FEE_PP':'POLICY_FEE')
# ip columns :                           df_ip = df_ip.rename(columns = {'SUM_INS':'SUM_ASSURED','ANNUAL_PREM':'B_OFF_APREM',
                                        #'ANNUAL_PREM_12':'ANNUAL_PREM_13','ANNUAL_PREM_01':'ANNUAL_PREM_1'})



print run_name
df_death = concat_by_type(run_name, death_res_csv_list, col_list_res_ls, columns_to_rename_ls)
df_death_mpf = concat_by_type('0.mpf', death_mpf_csv_list, col_list_mpf_ls, columns_to_rename_ls)
print('after concat:')
print('shape parameter RESULTS VS MPF'), df_death.shape[0], ('='), df_death_mpf.shape[0] 

df_tpd = concat_by_type(run_name, tpd_res_csv_list, col_list_res_ls, columns_to_rename_ls)
df_tpd_mpf = concat_by_type('0.mpf', tpd_mpf_csv_list, col_list_mpf_ls, columns_to_rename_ls)
print('after concat:')
print('shape parameter RESULTS VS MPF'), df_tpd.shape[0], ('='), df_tpd_mpf.shape[0] 

print os.path.join(DATA, run_name)
print tra_res_csv_list

df_ip = concat_by_type(run_name, ip_res_csv_list, col_list_res_ip, columns_to_rename_ip_res)
df_ip_mpf = concat_by_type('0.mpf', ip_mpf_csv_list, col_list_mpf_ip, columns_to_rename_ip_mpf)
print('after concat:')
print('shape parameter RESULTS VS MPF'), df_ip.shape[0], ('='), df_ip_mpf.shape[0] 

# if Trauma policies 
if tra_res_csv_list:
    print ('path exists')
    df_tra = concat_by_type(run_name, tra_res_csv_list, col_list_res_ls, columns_to_rename_ls)
    df_tra_mpf = concat_by_type('0.mpf', tra_mpf_csv_list, col_list_mpf_ls, columns_to_rename_ls)
    print('after concat:')
    print('shape parameter RESULTS VS MPF'), df_tra.shape[0], ('='), df_tra_mpf.shape[0] 




# stitch together MPF file

fpath = RESULT
print fpath


# df_death.to_csv(fpath + '\\' + 'Death.csv', index=False)
# df_tpd.to_csv(fpath + '\\' + 'TPD.csv', index=False)
# df_tra.to_csv(fpath + '\\' + 'Trauma.csv', index=False)
# df_ip.to_csv(fpath + '\\' + 'Income secure.csv', index=False)


# df_death_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'Death mpf.csv', index=False)
# df_tpd_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'TPD mpf.csv', index=False)
# df_tra_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'Trauma mpf.csv', index=False)
# df_ip_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'Income secure mpf.csv', index=False)


print ('__________________________________________________________________________________')
print ('Checking stage. Checking length of files before concat RES and MPF')
def check_length(df_1, df_2):
    if df_1.shape[0] != df_2.shape[0]:
        print ("CHECK length before concat. Length is different")
        print ('file1:'), df_1.shape[0]
        print ('file2:'), df_2.shape[0]

check_length(df_death_mpf,df_death)
check_length(df_tpd_mpf,df_tpd)
check_length(df_ip_mpf, df_ip)

# if os.path.exists(os.path.join(DATA,run_name, tra_res_csv_list[0])) == True:
if tra_res_csv_list:
        check_length(df_tra_mpf,df_tra)


print ("Stiching Results and MPF file by benefit")
death_cmb =pd.concat([df_death_mpf, df_death], axis = 1)
tpd_cmb = pd.concat([df_tpd_mpf,df_tpd], axis = 1)
ip_cmb = merge_by_id(df_ip_mpf, df_ip)

# if os.path.exists(os.path.join(DATA,run_name, tra_res_csv_list[0])) == True:
if tra_res_csv_list:
    tra_cmb = pd.concat([df_tra_mpf,df_tra], axis = 1)

death_cmb = death_cmb.loc[:,~death_cmb.columns.duplicated()]
tpd_cmb = tpd_cmb.loc[:,~tpd_cmb.columns.duplicated()]
ip_cmb = ip_cmb.drop_duplicates(['POL_NUMBER', 'L_LIFE_ID','SUM_ASSURED', 'AGE_AT_ENTRY', 'B_BEN_NO'])

# if os.path.exists(os.path.join(DATA,run_name, tra_res_csv_list[0])) == True:
if tra_res_csv_list:
    tra_cmb = tra_cmb.loc[:,~tra_cmb.columns.duplicated()]


print ('__________________________________________________________________________________')
print ('Checking stage. Checking length of files length after concat')

# Check that merged correctly:
if df_death.shape[0] != death_cmb.shape[0]:
    print ("CHECK MERGING of death_cmb. Length before and after is different")
    print ('before:'), df_death.shape[0]
    print ('after:'), death_cmb.shape[0]
if df_death['SUM_ASSURED'].sum() != death_cmb['SUM_ASSURED'].sum():
    print ("CHECK MERGING of death_cmb. Total Sum Assured before and after is different")
# if os.path.exists(os.path.join(DATA,run_name, tra_res_csv_list[0])) == True:
if tra_res_csv_list:
    if df_tra.shape[0] != tra_cmb.shape[0]:
        print ("CHECK MERGING of tra_cmb. Length before and after is different")
        print ('before:'), df_tra.shape[0]
        print ('after:'), tra_cmb.shape[0]
    if df_tra['SUM_ASSURED'].sum() != tra_cmb['SUM_ASSURED'].sum():
        print ("CHECK MERGING of tra_cmb. Total Sum Assured before and after is different")
if df_tpd.shape[0] != tpd_cmb.shape[0]:
    print ("CHECK MERGING of tpd_cmb. Length before and after is different")
    print ('before:'), df_tpd.shape[0]
    print ('after:'), tpd_cmb.shape[0]
if df_tpd['SUM_ASSURED'].sum() != tpd_cmb['SUM_ASSURED'].sum():
    print ("CHECK MERGING of tpd_cmb. Total Sum Assured before and after is different")
if df_ip.shape[0] != ip_cmb.shape[0]:
    print ("CHECK MERGING of ip_cmb. Length before and after is different")
    print ('before:'), df_ip.shape[0]
    print ('after:'), ip_cmb.shape[0]
if df_ip['SUM_ASSURED'].sum() != ip_cmb['SUM_ASSURED'].sum():
    print ("CHECK MERGING of ip_cmb. Total Sum Assured before and after is different")


print f_run_name, ('has been done, saving stage')

fpath_res = os.path.join(RESULT, 'intermediate-' + f_run_name)
print fpath_res

if os.path.exists(fpath_res) == False:
     os.makedirs(fpath_res)

ip_cmb= ip_cmb.rename(columns = {'ANNUAL_PREM_12':'ANNUAL_PREM_13', 'ANNUAL_PREM_24':'ANNUAL_PREM_25'})

death_cmb.to_csv(fpath_res + '\\' + 'Death' + run_name[-2:] + '.csv', index=False)
tpd_cmb.to_csv(fpath_res + '\\' + 'TPD' + run_name[-2:] + '.csv', index=False)
ip_cmb.to_csv(fpath_res + '\\' + 'Income secure' + run_name[-2:] + '.csv', index=False)

if tra_res_csv_list:
# if os.path.exists(os.path.join(DATA,run_name, tra_res_csv_list[0])) == True:
    tra_cmb.to_csv(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv', index=False)