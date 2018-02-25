#checking consistency of files

import pandas as pd
import rpt
import numpy as np
import os
from sets import Set
import sys




run_name = '0.repricing-run02'   
# run_name = '0.repricing-run06'

# policy_subset = 'super-cover'
policy_subset = 'ordinary-cover'

PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data', policy_subset)
RESULT = os.path.join(PROJ, 'results', policy_subset)


# check when same sized files
# for filename in os.listdir(os.path.join(PROJ, DATA, run_name)):
# 	print 
#   	for filename2 in os.listdir(os.path.join(PROJ, DATA, '0.mpf')):
#   		if filename[:6] == filename2[:6]:
# 	  		df = pd.read_csv(os.path.join(PROJ, DATA, run_name, filename))
# 	  		df2 = pd.read_csv(os.path.join(PROJ, DATA, '0.mpf', filename2))
#   			print filename, ('   '), df.shape[0]
#   			print filename2, df2.shape[0]

# Check when different sized resuts and mpf files
count1 = 0
count2 = 0
for filename in os.listdir(os.path.join(PROJ, DATA, run_name)):
    # print ('Reading '), filename
    df = pd.read_csv(os.path.join(PROJ, DATA, run_name, filename))
    print filename, ('   '), df.shape[0]
    count1 = count1+ df.shape[0]

for filename2 in os.listdir(os.path.join(PROJ, DATA, '0.mpf')):
            # print ('Reading '), filename2
            df2 = pd.read_csv(os.path.join(PROJ, DATA, '0.mpf', filename2))
            print filename2, df2.shape[0]
            count2 = count2+ df2.shape[0]

print ("# rows in RPT: "),count1, ("# rows in MPF: "), count2

def as_integer(value):
    try:
        int(value)
        return int(value)
    except:
        return np.nan



def get_file_path(foldname, filename):
    filepath = os.path.join(PROJ, DATA, foldname, filename)
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
    # print 'path:', path
    df = df.ix[:, col_list]
    return(df)



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

    if 'PREMIUM_TYPE' in col_list1:
        if type(df1.ix[0,'PREMIUM_TYPE']) ==str:
            if '"' in df1.ix[0,'PREMIUM_TYPE']:
                df1.ix[:,'PREMIUM_TYPE'] = [x.replace('"', '') for x in df1.ix[:,'PREMIUM_TYPE']]
            if ' ' in df1.ix[0,'PREMIUM_TYPE']:
                df1.ix[:,'PREMIUM_TYPE'] = [x.replace(' ', '') for x in df1.ix[:,'PREMIUM_TYPE']]

    if 'L_LIFE_ID' in col_list1:
        if type(df1.ix[0,'L_LIFE_ID']) == str:
            if '"' in df1.ix[0,'L_LIFE_ID']:
                df1.ix[:,'L_LIFE_ID'] = [x.replace('"', '') for x in df1.ix[:,'L_LIFE_ID']]
            if ' ' in df1.ix[0,'L_LIFE_ID']:
                df1.ix[:,'L_LIFE_ID'] = [x.replace(' ', '') for x in df1.ix[:,'L_LIFE_ID']]

        if type(df1.ix[0,'L_LIFE_ID']) != int:
            df1.ix[:,'L_LIFE_ID'] = [as_integer(x) for x in df1.ix[:, 'L_LIFE_ID']]
    return(df1)



def concat_by_type(folderdir, csv_list, col_list, columns_to_rename):
    appended_data = []
    size_count = 0
    prem_count = 0
    prem_count1 = 0
    prem_count13 = 0
    prem_count25 = 0
    si = 0    

    for item in csv_list:
        df_res = read_and_check(folderdir, item, col_list)
        appended_data.append(df_res)
        #additional check-variables
        size_count = size_count + df_res.shape[0]
        # prem_count = prem_count + df_res.ANNUAL_PREM.sum()
        prem_count1 = prem_count1 + df_res.ANNUAL_PREM_1.sum()
        prem_count13 = prem_count13 + df_res.ANNUAL_PREM_13.sum()
        prem_count25 = prem_count25 + df_res.ANNUAL_PREM_25.sum()
        si = si + df_res.SUM_ASSURED.sum()
        

    
    df = pd.concat(appended_data, axis=0, ignore_index=True)
    # df = df.rename(columns = columns_to_rename)
    print
    # print item
    # print ('count:      '), size_count
    # print ('shape-rows: '), df.shape[0], ('shape-columns: '), df.shape[1]
    # print ('prem      '), prem_count
    # print ('prem1     '), prem_count1
    # print ('prem13    '), prem_count13
    # print ('prem25    '), prem_count25
    # print ('sum of SI:'), si

    return df



def concat_by_type_ip(folderdir, csv_list, col_list, columns_to_rename):
    appended_data = []
    size_count = 0
    prem_count = 0
    prem_count1 = 0
    prem_count13 = 0
    prem_count25 = 0
    si = 0    

    for item in csv_list:
        df_res = read_and_check(folderdir, item, col_list)
        appended_data.append(df_res)
        #additional check-variables
        size_count = size_count + df_res.shape[0]
        # prem_count = prem_count + df_res.ANNUAL_PREM.sum()
        prem_count1 = prem_count1 + df_res.ANNUAL_PREM_1.sum()
        prem_count13 = prem_count13 + df_res.ANNUAL_PREM_12.sum()
        prem_count25 = prem_count25 + df_res.ANNUAL_PREM_24.sum()
        # si = si + df_res.SUM_ASSURED.sum()
        

    
    df = pd.concat(appended_data, axis=0, ignore_index=True)
    df = df.rename(columns = columns_to_rename)
    print
    # print item
    # print ('count:      '), size_count
    # print ('shape-rows: '), df.shape[0], ('shape-columns: '), df.shape[1]
    # # print ('prem      '), prem_count
    # print ('prem1     '), prem_count1
    # print ('prem13    '), prem_count13
    # print ('prem25    '), prem_count25
    # print ('sum of SI:'), si

    return df




col_list_res_ls = ['AGE_AT_ENTRY','ANNUAL_PREM_1','BE_RESERVE', 'L_LIFE_ID',
                'POL_NUMBER','PREMIUM_TYPE','SUM_ASSURED','OAP', 'POL_FEE_PP', 'ANNUAL_PREM_13', 'ANNUAL_PREM_25']
columns_to_rename_ls = {'SUM_ASSD_PP':'SUM_ASSURED','OAP':'B_OFF_APREM','POL_FEE_PP':'POLICY_FEE'}



death_res_csv_list = ['COCDL1.csv', 'COCDS1.csv', 'COCDS2.csv', 'COCDS3.csv']
df_death = concat_by_type(run_name, death_res_csv_list, col_list_res_ls, columns_to_rename_ls)

print ('________________________Life:') 
print ('|sum ANNUAL_PREM_1 |'), df_death.ANNUAL_PREM_1.sum()
print ('|sum ANNUAL_PREM_13|'),df_death.ANNUAL_PREM_13.sum()
print ('|sum ANNUAL_PREM_25|'),df_death.ANNUAL_PREM_25.sum()
print ('|sum SUM ASSURED   |'),df_death.SUM_ASSURED.sum()

tpd_res_csv_list = ['COCPL1.csv', 'COCPS1.csv']
df_tpd = concat_by_type(run_name, tpd_res_csv_list, col_list_res_ls, columns_to_rename_ls)

print ('________________________TPD:')
print ('|sum ANNUAL_PREM_1 |'), df_tpd.ANNUAL_PREM_1.sum()
print ('|sum ANNUAL_PREM_13|'),df_tpd.ANNUAL_PREM_13.sum()
print ('|sum ANNUAL_PREM_25|'),df_tpd.ANNUAL_PREM_25.sum()
print ('|sum SUM ASSURED   |'),df_tpd.SUM_ASSURED.sum()

if policy_subset == 'ordinary-cover':
    tra_res_csv_list = ['COCTL1.csv', 'COCTS1.csv']
    df_tra = concat_by_type(run_name, tra_res_csv_list, col_list_res_ls, columns_to_rename_ls)

    print ('________________________TRA:')
    print ('|sum ANNUAL_PREM_1 |'), df_tra.ANNUAL_PREM_1.sum()
    print ('|sum ANNUAL_PREM_13|'),df_tra.ANNUAL_PREM_13.sum()
    print ('|sum SUM ASSURED   |'),df_tra.SUM_ASSURED.sum()




col_list_res_ip = ['AGE_AT_ENTRY', 'POL_NUMBER','SUM_INS', 'B_BEN_NO',
                'MOS_PV_PREM','ANNUAL_PREM_12', 'BE_RESERVE', 'L_LIFE_ID', 'ANNUAL_PREM_1',  
                'POLICY_FEE','ENTRY_MONTH', 'ENTRY_YEAR', 'ANNUAL_PREM_24']

columns_to_rename_ip = {'SUM_INS':'SUM_ASSURED','ANNUAL_PREM':'B_OFF_APREM',
    'ANNUAL_PREM_12':'ANNUAL_PREM_13','ANNUAL_PREM_01':'ANNUAL_PREM_1', 'ANN_PHI_BEN':'SUM_ASSURED', 
    'ANNUAL_PREM_24':'ANNUAL_PREM_25'}

ip_res_csv_list = ['PAOCL1.csv', 'PAOCL2.csv', 'PAOCL3.csv', 'PAOCL4.csv', 'PAOCS1.csv', 'PAOCS2.csv', 'PAOCS3.csv', 'PAOCS4.csv', 'PAOCS5.csv', 'PAOCS6.csv', 'PAOCS7.csv', 'PAOCS8.csv', 'PAOCS9.csv']

df_ip = concat_by_type_ip(run_name, ip_res_csv_list, col_list_res_ip, columns_to_rename_ip)


print ('________________________IP:')
print ('|sum ANNUAL_PREM_1 |'), df_ip.ANNUAL_PREM_1.sum()
print ('|sum ANNUAL_PREM_13|'),df_ip.ANNUAL_PREM_13.sum()
print ('|sum ANNUAL_PREM_25|'),df_ip.ANNUAL_PREM_25.sum()



if policy_subset == 'ordinary-cover':
    print ('|TOTAL sum ANNUAL_PREM_1 |'), df_death.ANNUAL_PREM_1.sum() +\
                                            df_tpd.ANNUAL_PREM_1.sum() +\
                                            df_tra.ANNUAL_PREM_1.sum() +\
                                            df_ip.ANNUAL_PREM_1.sum()

    print ('|TOTAL sum ANNUAL_PREM_13 |'), df_death.ANNUAL_PREM_13.sum() +\
                                            df_tpd.ANNUAL_PREM_13.sum() +\
                                            df_tra.ANNUAL_PREM_13.sum() +\
                                            df_ip.ANNUAL_PREM_13.sum()

    print ('|TOTAL sum ANNUAL_PREM_25 |'), df_death.ANNUAL_PREM_25.sum() +\
                                            df_tpd.ANNUAL_PREM_25.sum() +\
                                            df_tra.ANNUAL_PREM_25.sum() +\
                                            df_ip.ANNUAL_PREM_25.sum()




if policy_subset == 'super-cover':
    print ('|TOTAL sum ANNUAL_PREM_1 |'), df_death.ANNUAL_PREM_1.sum() +\
                                            df_tpd.ANNUAL_PREM_1.sum() +\
                                            df_ip.ANNUAL_PREM_1.sum()

    print ('|TOTAL sum ANNUAL_PREM_13 |'), df_death.ANNUAL_PREM_13.sum() +\
                                            df_tpd.ANNUAL_PREM_13.sum() +\
                                            df_ip.ANNUAL_PREM_13.sum()

    print ('|TOTAL sum ANNUAL_PREM_25 |'), df_death.ANNUAL_PREM_25.sum() +\
                                            df_tpd.ANNUAL_PREM_25.sum() +\
                                            df_ip.ANNUAL_PREM_25.sum()


