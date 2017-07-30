import pandas as pd
import rpt
import numpy as np
import os
from sets import Set
import sys
from datetime import datetime 

PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data', 'super-cover')
RESULT = os.path.join(PROJ, 'results\\super-cover')
print DATA
# run_name = '0.repricing-run02'   
run_name = '0.repricing-run06'

f_run_name = run_name[-5:]#last 5 letters
print f_run_name

# sets cwd to location of the script
# os.chdir(os.path.dirname(__file__))

# ___________________adds a timestamp to each line________________________
#old_f = sys.stdout
#class F:
#    def write(self, x):
#        old_f.write(x.replace("\n", " [%s]\n" % str(datetime.now())))
#sys.stdout = F()
# _________________________________________________________________________


def as_integer(value):
    try:
        int(value)
        return int(value)
    except:
        return np.nan


def read(nfold, nfile):
    path = get_file_path(nfold, nfile)
    df = read_if_exists(path)
    return(df)

# def get_file_path(foldname, filename):
#     cwd = os.getcwd()
#     filepath = os.path.join(cwd, foldname, filename)
#     return(filepath)

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
    df = df.ix[:, col_list]
    return(df)



def read_and_check(run_name, filename, col_list1):
    df1 = read_and_slice(run_name, filename, col_list1)
    
    if type(df1.ix[0,'POL_NUMBER']) == str:
        # df1.ix[:,'POL_NUMBER'] = [x.replace('"', '') for x in df1.ix[:,'POL_NUMBER']]
        # df1.ix[:,'POL_NUMBER'] = [x.replace(' ', '') for x in df1.ix[:,'POL_NUMBER']]    
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
        if type(df1.ix[0,'L_LIFE_ID']) != int:
            df1.ix[:,'L_LIFE_ID'] = [as_integer(x) for x in df1.ix[:, 'L_LIFE_ID']]
    return(df1)


def read_slice_combine(run_name, filename,col_list1):
    df1_r = read_and_slice(run_name, filename[0], col_list1)
    df2_r = read_and_slice(run_name, filename[1], col_list1)
    if filename[2] > 0:
        df3_r = read_and_slice(run_name, filename[2], col_list1)
        df4_r = read_and_slice(run_name, filename[3], col_list1)
        if filename[4] > 0:
            df5_r = read_and_slice(run_name, filename[4], col_list1)
            df6_r = read_and_slice(run_name, filename[5], col_list1)
            df_ResultComb = pd.concat([df1_r, df2_r, df3_r, df4_r, df5_r, df6_r], ignore_index=True)
        else:    
            df_ResultComb = pd.concat([df1_r, df2_r, df3_r, df4_r], ignore_index=True)    
    else:   
        df_ResultComb = pd.concat([df1_r, df2_r], ignore_index=True)
    
    return df_ResultComb

def merge_by_id(df_a, df_b):
    return(pd.merge(df_a, df_b, how = 'outer', 
            on = ['POL_NUMBER', 'L_LIFE_ID','SUM_ASSURED','B_BEN_NO','AGE_AT_ENTRY']))

# You should delete things you are not using
# they are rubbish, and make programs harder to read
# def write(nfold, nfile):
#     path = get_file_path(nfold, nfile)
#     df = pd.to_csv(path)
#     return(df)
 
print ('start of program')
print os.getcwd()

col_list_res_ls = ['AGE_AT_ENTRY','ANNUAL_PREM_1','BE_RESERVE', 'L_LIFE_ID',
                'POL_NUMBER','PREMIUM_TYPE','SUM_ASSD_PP','OAP', 'POL_FEE_PP', 'ANNUAL_PREM_13']
col_list_ls_mpf = ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY',
                'DURATIONIF_M', 'SEX','SMOKER_IND', 'SUM_ASSURED', 'B_BEN_NO',
                'B_OFF_APREM','PREMIUM_TYPE', 'POLICY_FEE',
                'BROKER', 'DCS_REIN_SI', 'CHANNEL']

col_list_res_ip = ['AGE_AT_ENTRY', 'POL_NUMBER','SUM_INS', 'B_BEN_NO',
                'MOS_PV_PREM','ANNUAL_PREM_12', 'BE_RESERVE', 'L_LIFE_ID', 'ANNUAL_PREM_01'] 
col_list_ip_mpf = ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY', 'PREMIUM_TYPE',
                'DURATIONIF_M', 'SEX','SMOKER_IND',
                'ANN_PHI_BEN', 'B_BEN_NO', 'ANNUAL_PREM',
                'BENEFIT_CODE', 'DEFER_PER_MP', 'TOTAL_SI', 'BEN_PERIOD', 'OCC_CLASS', 
                'DII_TYPE', 'BROKER', 'OTR_ANNPHIBEN', 'CHANNEL']


death_name = ['COCDL1.csv','COCDS1.csv', 'COCDS2.csv','COCDS3.csv']                
tra_name = ['COCTL1.csv','COCTS1.csv']
tpd_name = ['COCPL1.csv','COCPS1.csv']
ip_name = ['PAOCL1.csv', 'PAOCL2.csv', 'PAOCL3.csv', 'PAOCL4.csv',
            'PAOCS1.csv','PAOCS2.csv', 'PAOCS3.csv', 'PAOCS4.csv','PAOCS5.csv',
            'PAOCS6.csv', 'PAOCS7.csv' ,'PAOCS8.csv', 'PAOCS9.csv','PAOCS10.csv']

death_name_mpf = ['COCDL1.PRO.csv','COCDS1.PRO.csv', 'COCDS2.PRO.csv','COCDS3.PRO.csv']
tpd_name_mpf = ['COCPL1.PRO.csv','COCPS1.PRO.csv']
tra_name_mpf = ['COCTL1.PRO.csv','COCTS1.PRO.csv']
ip_name_mpf = ['PAOCL1.PRO.csv', 'PAOCL2.PRO.csv', 'PAOCL3.PRO.csv', 'PAOCL4.PRO.csv',
            'PAOCS1.PRO.csv','PAOCS2.PRO.csv', 'PAOCS3.PRO.csv', 'PAOCS4.PRO.csv','PAOCS5.PRO.csv',
            'PAOCS6.PRO.csv', 'PAOCS7.PRO.csv' ,'PAOCS8.PRO.csv', 'PAOCS9.PRO.csv','PAOCS10.PRO.csv']
print os.path.join(DATA, death_name[0])


df_death1_res = read_and_check(run_name, death_name[0], col_list_res_ls)
df_death2_res = read_and_check(run_name, death_name[1], col_list_res_ls)
df_death3_res = read_and_check(run_name, death_name[2], col_list_res_ls)
df_death4_res = read_and_check(run_name, death_name[3], col_list_res_ls)
df_death = pd.concat([df_death1_res, df_death2_res, df_death3_res, df_death4_res],
        ignore_index=True)
df_death = df_death.rename(columns = {'SUM_ASSD_PP':'SUM_ASSURED','OAP':'B_OFF_APREM',
                                    'POL_FEE_PP':'POLICY_FEE'})
print 'df_death'
print df_death.info()

size_death_count = df_death1_res.shape[0] + df_death2_res.shape[0] +df_death3_res.shape[0] +df_death4_res.shape[0]
size_death_ann_prem = df_death1_res['ANNUAL_PREM_1'] + df_death2_res['ANNUAL_PREM_1'] +df_death3_res['ANNUAL_PREM_1'] +df_death4_res['ANNUAL_PREM_1']
size_death_sum_ass = df_death1_res['SUM_ASSD_PP'] + df_death2_res['SUM_ASSD_PP'] +df_death3_res['SUM_ASSD_PP'] +df_death4_res['SUM_ASSD_PP']
# print size_death_count, size_death_ann_prem +size_death_sum_ass

if os.path.exists(os.path.join(DATA, tra_name[0])) == True:
    df_tra1_res = read_and_check(run_name, tra_name[0], col_list_res_ls)
    df_tra2_res = read_and_check(run_name, tra_name[1], col_list_res_ls)
    df_tra = pd.concat([df_tra1_res, df_tra2_res], ignore_index=True)
    df_tra = df_tra.rename(columns = {'SUM_ASSD_PP':'SUM_ASSURED','OAP':'B_OFF_APREM',
                                        'POL_FEE_PP':'POLICY_FEE'})

df_tpd1_res = read_and_check(run_name, tpd_name[0] , col_list_res_ls)
df_tpd2_res = read_and_check(run_name, tpd_name[1] , col_list_res_ls)
df_tpd = pd.concat([df_tpd1_res, df_tpd2_res], ignore_index=True)
df_tpd = df_tpd.rename(columns = {'SUM_ASSD_PP':'SUM_ASSURED','OAP':'B_OFF_APREM',
                                    'POL_FEE_PP':'POLICY_FEE'})
df_ip =pd.DataFrame()
for i in range(0,len(ip_name)):
    df_ip1 = read_and_check(run_name, ip_name[i], col_list_res_ip)
    df_ip = pd.concat([df_ip,df_ip1], ignore_index=True)
df_ip = df_ip.rename(columns = {'SUM_INS':'SUM_ASSURED','ANNUAL_PREM':'B_OFF_APREM',
                                'ANNUAL_PREM_12':'ANNUAL_PREM_13','ANNUAL_PREM_01':'ANNUAL_PREM_1'})

# stitch together MPF file


df_death_mpf = pd.DataFrame()
for i in range(0, len(death_name_mpf)):
    df_death_mpf1 = read_and_check('0.Mpf',death_name_mpf[i], col_list_ls_mpf)
    df_death_mpf = pd.concat([df_death_mpf, df_death_mpf1], ignore_index=True)
    print df_death_mpf1.info()
    print df_death_mpf.info()
  


print df_death.info()
print df_death_mpf.info()



df_tpd_mpf1 = read_and_check('0.Mpf',tpd_name_mpf[0], col_list_ls_mpf)
df_tpd_mpf2 = read_and_check('0.Mpf',tpd_name_mpf[1], col_list_ls_mpf)
df_tpd_mpf = pd.concat([df_tpd_mpf1, df_tpd_mpf2], ignore_index=True)

if os.path.exists(os.path.join(DATA, tra_name[0])) == True:
    df_tra_mpf1 = read_and_check('0.Mpf', tra_name_mpf[0], col_list_ls_mpf)
    df_tra_mpf2 = read_and_check('0.Mpf', tra_name_mpf[1], col_list_ls_mpf)  
    df_tra_mpf = pd.concat([df_tra_mpf1,df_tra_mpf2], ignore_index=True)

df_ip_mpf =pd.DataFrame()
for i in range(0,len(ip_name_mpf)):
    df_ip_mpf1 = read_and_check('0.Mpf', ip_name_mpf[i], col_list_ip_mpf)
    df_ip_mpf = pd.concat([df_ip_mpf,df_ip_mpf1], ignore_index=True)
df_ip_mpf = df_ip_mpf.rename(columns = {'ANN_PHI_BEN':'SUM_ASSURED',
                                        'ANNUAL_PREM':'B_OFF_APREM'})







# df_death.to_csv(fpath + '\\' + 'Death.csv', index=False)
# df_tpd.to_csv(fpath + '\\' + 'TPD.csv', index=False)
# df_tra.to_csv(fpath + '\\' + 'Trauma.csv', index=False)
# df_ip.to_csv(fpath + '\\' + 'Income secure.csv', index=False)


# df_death_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'Death mpf.csv', index=False)
# df_tpd_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'TPD mpf.csv', index=False)
# df_tra_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'Trauma mpf.csv', index=False)
# df_ip_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'Income secure mpf.csv', index=False)


   
# print 'death mpf', df_death_mpf.head(2)
# print 'tra mpf',df_tra_mpf.head(2)
# print 'tpd mpf',df_tpd_mpf.head(2)
# print 'ip mpf',df_ip_mpf.head(2)

# print df_death.head(2)
# print df_tra.head(2)
# print df_tpd.head(2)
# print df_ip.head(2)

print df_death_mpf.info()
print df_death.info()

death_cmb =pd.concat([df_death_mpf,df_death], axis = 1)
tpd_cmb = pd.concat([df_tpd_mpf,df_tpd], axis = 1)
if os.path.exists(os.path.join(DATA, tra_name[0])) == True:
    tra_cmb = pd.concat([df_tra_mpf,df_tra], axis = 1)
ip_cmb = merge_by_id(df_ip_mpf, df_ip)


death_cmb = death_cmb.loc[:,~death_cmb.columns.duplicated()]
if os.path.exists(os.path.join(DATA, tra_name[0])) == True:
    tra_cmb = tra_cmb.loc[:,~tra_cmb.columns.duplicated()]
tpd_cmb = tpd_cmb.loc[:,~tpd_cmb.columns.duplicated()]
ip_cmb = ip_cmb.drop_duplicates(['POL_NUMBER', 'L_LIFE_ID','SUM_ASSURED', 'AGE_AT_ENTRY', 'B_BEN_NO'])


# Check that merged correctly:
if df_death.shape[0] != death_cmb.shape[0]:
    print ("CHECK MERGING of death_cmb. Length before and after is different")
    print ('before:'), df_death.shape[0]
    print ('after:'), death_cmb.shape[0]
if df_death['SUM_ASSURED'].sum() != death_cmb['SUM_ASSURED'].sum():
    print ("CHECK MERGING of death_cmb. Total Sum Assured before and after is different")
if os.path.exists(os.path.join(DATA, tra_name[0])) == True:
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



f_run_name = run_name[-5:]#last 5 letters
print f_run_name

fpath_res = os.path.join(RESULT, 'intermediate-' + f_run_name)
print fpath_res

if os.path.exists(fpath_res) == False:
     os.makedirs(fpath_res)

 
death_cmb.to_csv(fpath_res + '\\' + 'Death' + run_name[-2:] + '.csv', index=False)
tpd_cmb.to_csv(fpath_res + '\\' + 'TPD' + run_name[-2:] + '.csv', index=False)
if os.path.exists(os.path.join(DATA, tra_name[0])) == True:
    tra_cmb.to_csv(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv', index=False)
ip_cmb.to_csv(fpath_res + '\\' + 'Income secure' + run_name[-2:] + '.csv', index=False)
