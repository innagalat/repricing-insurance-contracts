import pandas as pd
import rpt
import numpy as np
import os
from sets import Set
import sys
from datetime import datetime 


run_name = '0.Run_02'   
# run_name = '0.Run_06'


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

def read(nfold, nfile):
    path = get_file_path(nfold, nfile)
    df = read_if_exists(path)
    return(df)

# You should delete things you are not using
# they are rubbish, and make programs harder to read
# def write(nfold, nfile):
#     path = get_file_path(nfold, nfile)
#     df = pd.to_csv(path)
#     return(df)

def read_and_slice(nfold, nfile, col_list):
    path = get_file_path(nfold, nfile)
    df = read_if_exists(path)
    df = df.ix[:, col_list]
    return(df)

def as_integer(value):
    try:
        int(value)
        return int(value)
    except:
        return np.nan

# sets cwd to location of the script
os.chdir(os.path.dirname(__file__))

# _________________________
#old_f = sys.stdout
#class F:
#    def write(self, x):
#        old_f.write(x.replace("\n", " [%s]\n" % str(datetime.now())))
#sys.stdout = F()
# ___________________________

print ('start of program')
print os.getcwd()

# dff2 = pd.DataFrame({'A' :1., 'E' : [u'"1111"',2222, 333]})
# print dff2
# print type(dff2.ix[0,'A'])
# if type(dff2.ix[0,'E']) == str:
#         dff2.ix[:,'E'] = [as_integer(x) for x in dff2.ix[:, 'E']]
# print dff2
# print type(dff2.ix[0,'A'])

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
 


col_list_res_ls = ['AGE_AT_ENTRY','ANNUAL_PREM_1','BE_RESERVE', 'L_LIFE_ID',
                'POL_NUMBER','PREMIUM_TYPE','SUM_ASSD_PP','OAP', 'PV_PREM', 'POL_FEE_PP', 'ANNUAL_PREM_13']
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



df_death1_res = read_and_check(run_name, death_name[0], col_list_res_ls)
df_death2_res = read_and_check(run_name, death_name[1], col_list_res_ls)
df_death3_res = read_and_check(run_name, death_name[2], col_list_res_ls)
df_death4_res = read_and_check(run_name, death_name[3], col_list_res_ls)
df_death = pd.concat([df_death1_res, df_death2_res, df_death3_res, df_death4_res],
        ignore_index=True)
df_death = df_death.rename(columns = {'SUM_ASSD_PP':'SUM_ASSURED','OAP':'B_OFF_APREM',
                                    'POL_FEE_PP':'POLICY_FEE'})

size_death_count = df_death1_res.shape[0] + df_death2_res.shape[0] +df_death3_res.shape[0] +df_death4_res.shape[0]
size_death_ann_prem = df_death1_res['ANNUAL_PREM_1'] + df_death2_res['ANNUAL_PREM_1'] +df_death3_res['ANNUAL_PREM_1'] +df_death4_res['ANNUAL_PREM_1']
size_death_sum_ass = df_death1_res['SUM_ASSD_PP'] + df_death2_res['SUM_ASSD_PP'] +df_death3_res['SUM_ASSD_PP'] +df_death4_res['SUM_ASSD_PP']
print size_death_count, size_death_ann_prem +size_death_sum_ass

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
df_ip = df_ip.rename(columns = {'SUM_INS':'SUM_ASSURED',
                                        'ANNUAL_PREM':'B_OFF_APREM'})

# stitch together MPF file

df_death_mpf = pd.DataFrame()
for i in range(0, len(death_name_mpf)):
    df_death_mpf1 = read_and_check('0.Mpf',death_name_mpf[i], col_list_ls_mpf)
    df_death_mpf = pd.concat([df_death_mpf, df_death_mpf1], ignore_index=True)

df_tpd_mpf1 = read_and_check('0.Mpf',tpd_name_mpf[0], col_list_ls_mpf)
df_tpd_mpf2 = read_and_check('0.Mpf',tpd_name_mpf[1], col_list_ls_mpf)
df_tpd_mpf = pd.concat([df_tpd_mpf1, df_tpd_mpf2], ignore_index=True)

df_tra_mpf1 = read_and_check('0.Mpf', tra_name_mpf[0], col_list_ls_mpf)
df_tra_mpf2 = read_and_check('0.Mpf', tra_name_mpf[1], col_list_ls_mpf)  
df_tra_mpf = pd.concat([df_tra_mpf1,df_tra_mpf2], ignore_index=True)

df_ip_mpf =pd.DataFrame()
for i in range(0,len(ip_name_mpf)):
    df_ip_mpf1 = read_and_check('0.Mpf', ip_name_mpf[i], col_list_ip_mpf)
    df_ip_mpf = pd.concat([df_ip_mpf,df_ip_mpf1], ignore_index=True)
df_ip_mpf = df_ip_mpf.rename(columns = {'ANN_PHI_BEN':'SUM_ASSURED',
                                        'ANNUAL_PREM':'B_OFF_APREM'})




f_run_name = run_name[-6:]#last 4 letters
fpath = r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\' + 'Results Files_'+ f_run_name 
if os.path.exists(fpath) == False:
     os.makedirs(fpath)

  
print fpath
df_death.to_csv(fpath + '\\' + 'Death.csv', index=False)
df_tpd.to_csv(fpath + '\\' + 'TPD.csv', index=False)
df_tra.to_csv(fpath + '\\' + 'Trauma.csv', index=False)
df_ip.to_csv(fpath + '\\' + 'Income secure.csv', index=False)


df_death_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'Death mpf.csv', index=False)
df_tpd_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'TPD mpf.csv', index=False)
df_tra_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'Trauma mpf.csv', index=False)
df_ip_mpf.to_csv(r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + 'Income secure mpf.csv', index=False)


   
print 'death mpf', df_death_mpf.head(2)
print 'tra mpf',df_tra_mpf.head(2)
print 'tpd mpf',df_tpd_mpf.head(2)
print 'ip mpf',df_ip_mpf.head(2)

print df_death.head(2)
print df_tra.head(2)
print df_tpd.head(2)
print df_ip.head(2)
