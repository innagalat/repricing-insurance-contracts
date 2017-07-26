# stitching 4 benefits together, finding corrresponding BEl and summary statistics per unique life id.
import pandas as pd
import numpy as np
import os
from sets import Set
import sys
from datetime import datetime 

# foldername_slice_2 ='Results Files_Run_02'
foldername_slice_2 ='Results Files_Run_06'
# for run 6: insert Annual_Prem_13, see other file

f_run_name = foldername_slice_2[-2:] #last 2 letters
print f_run_name

fpath = r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\' + f_run_name 
if os.path.exists(fpath) == False:
     os.makedirs(fpath)


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

# def merge_by_id_expanded(df_a, df_b):
#     return(pd.merge(df_a, df_b, how = 'outer', 
#             on = ['POL_NUMBER','SUM_ASSURED', 'AGE_AT_ENTRY', 'B_OFF_APREM', 'PREMIUM_TYPE','POLICY_FEE'], 
#             indicator =True))
# def merge_by_id_expanded(df_b, df_a):
#     return(pd.merge(df_b, df_a, how = 'left', 
#             on = ['POL_NUMBER','SUM_ASSURED', 'AGE_AT_ENTRY', 'B_OFF_APREM', 'PREMIUM_TYPE','POLICY_FEE'], 
#             indicator =True))

def merge_by_id_expanded(df_a, df_b):
    return(pd.merge(df_a, df_b, how = 'outer', 
            on = ['POL_NUMBER','L_LIFE_ID','SUM_ASSURED','B_OFF_APREM','PV_PREM'], 
            indicator =True))

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


col_list_ls_res = ['AGE_AT_ENTRY','ANNUAL_PREM_1','BE_RESERVE', 'L_LIFE_ID',
                'POL_NUMBER','PREMIUM_TYPE','SUM_ASSURED','B_OFF_APREM', 'PV_PREM', 'POLICY_FEE', 'ANNUAL_PREM_13']
col_list_ip_res = ['AGE_AT_ENTRY', 'POL_NUMBER','SUM_ASSURED', 'B_BEN_NO',
                'MOS_PV_PREM','ANNUAL_PREM_12', 'BE_RESERVE', 'L_LIFE_ID', 'ANNUAL_PREM_01'] 
col_list_ls_mpf = ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY',
                'DURATIONIF_M', 'SEX','SMOKER_IND', 'SUM_ASSURED', 'B_BEN_NO',
                'B_OFF_APREM','PREMIUM_TYPE', 'POLICY_FEE']
col_list_ip_mpf = ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY', 'PREMIUM_TYPE',
                'DURATIONIF_M', 'SEX','SMOKER_IND',
                'ANN_PHI_BEN', 'B_BEN_NO', 'ANNUAL_PREM']


# filename_mpf_test =['Death mpf_test.csv', 'TPD mpf_test.csv', 'Trauma mpf_test.csv','Income secure mpf_test.csv']
# filename_test =['Death_test.csv', 'TPD_test.csv','Trauma_test.csv','Income secure_test.csv'] 
filename_test =['Death.csv', 'TPD.csv','Trauma.csv','Income secure.csv'] 
filename_mpf_test =['Death mpf.csv', 'TPD mpf.csv', 'Trauma mpf.csv','Income secure mpf.csv']

# slice1 is MPF
# column_slice_1 = ['POL_NUMBER', 'L_LIFE_ID', 'AGE_AT_ENTRY', 'DURATIONIF_M', 'SEX', 'SMOKER_IND',
#                 'SUM_ASSURED', 'B_BEN_NO', 'B_OFF_APREM', 'PREMIUM_TYPE', 'POLICY_FEE'] 
# column_slice_2 = ['POL_NUMBER','AGE_AT_ENTRY', 'B_OFF_APREM', 'BE_RESERVE', 'PREMIUM_TYPE',
#             'SUM_ASSURED', 'ANNUAL_PREM_1','PV_PREM','POLICY_FEE']
                

#create subsets of data for life, Tra, Ip, Tpd
# df_slice_1 = read_and_slice('Mpf', filename_mpf_test[0], column_slice_1)
# df_slice_2 = read_and_slice(foldername_slice_2, filename_test[0], column_slice_2)
df_slice_1 = pd.read_csv(
        r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' +\
        filename_mpf_test[0], 
        dtype = {'AGE_AT_ENTRY':np.int32, 'POL_NUMBER': np.int32, 'L_LIFE_ID':np.int32, 'SUM_ASSURED':np.int32,
        'ANNUAL_PREM_1':np.float64, 'BE_RESERVE':np.float64,'B_OFF_APREM':np.float64,
        'PV_PREM':np.float64,'POLICY_FEE':np.float64,'PREMIUM_TYPE':str})

df_slice_2 = pd.read_csv(
        r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\' + foldername_slice_2 +\
        '\\' + filename_test[0],
        dtype = {'AGE_AT_ENTRY':np.int32, 'POL_NUMBER': np.int32, 'L_LIFE_ID':np.int32,'SUM_ASSURED':np.int32,
        'B_BEN_NO':np.int32, 'L_LIFE_ID':np.int32,'DURATIONIF_M':np.int32, 'SEX':np.int32, 'SMOKER_IND':np.int32, 
        'B_OFF_APREM':np.float64,'POLICY_FEE':np.float64,'PREMIUM_TYPE':str, 'ANNUAL_PREM_13':np.float64})

df_slice_1.drop(['BROKER','DCS_REIN_SI', 'CHANNEL'], axis = 1, inplace = True)
# df_slice_2 = df_slice_2.ix[:, col_list_ls_res]

# df_slice_1 = df_slice_1.sort_index(by=['POL_NUMBER','AGE_AT_ENTRY' ,'B_OFF_APREM'], ascending=[True,True ,True])
# df_slice_2 = df_slice_2.sort_index(by=['POL_NUMBER','AGE_AT_ENTRY' ,'B_OFF_APREM'], ascending=[True,True ,True])

# max_count = max(df_slice_2['POL_NUMBER'].value_counts())
# print max_count
# print df_slice_2['POL_NUMBER'].value_counts() == max_count
# ___________________________
# df_slice_1_TPD = read_and_slice('Mpf', filename_mpf_test[1], column_slice_1)
# df_slice_2_TPD = read_and_slice(foldername_slice_2, filename_test[1],column_slice_2)

df_slice_1_TPD = pd.read_csv(
        r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' +\
        filename_mpf_test[1])

df_slice_2_TPD = pd.read_csv(
        r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\' + foldername_slice_2 +\
        '\\' + filename_test[1])
# df_slice_1_TPD.drop(['BROKER','DCS_REIN_SI', 'CHANNEL'], axis = 1, inplace = True)

# df_slice_1_TPD = df_slice_1_TPD.ix[:, col_list_ls_mpf]
# df_slice_2_TPD = df_slice_2_TPD.ix[:, col_list_ls_res]
# df_slice_1_TPD = df_slice_1_TPD.sort_index(by=['POL_NUMBER','AGE_AT_ENTRY' ,'B_OFF_APREM'], ascending=[True,True ,True])
# df_slice_2_TPD = df_slice_2_TPD.sort_index(by=['POL_NUMBER','AGE_AT_ENTRY' ,'B_OFF_APREM'], ascending=[True,True ,True])

#______________________________

# df_slice_1_TRA = read_and_slice('Mpf', filename_mpf_test[2] , column_slice_1)
# df_slice_2_TRA = read_and_slice(foldername_slice_2, filename_test[2], column_slice_2)
df_slice_1_TRA = pd.read_csv(
        r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' +\
        filename_mpf_test[2])
df_slice_2_TRA = pd.read_csv(
        r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\' + foldername_slice_2 +\
        '\\' + filename_test[2])
df_slice_1_TRA.drop(['BROKER','DCS_REIN_SI', 'CHANNEL'], axis = 1, inplace = True)

# df_slice_1_TRA = df_slice_1_TRA.ix[:, col_list_ls_mpf]
# df_slice_2_TRA = df_slice_2_TRA.ix[:, col_list_ls_res]
# df_slice_1_TRA = df_slice_1_TRA.sort_index(by=['POL_NUMBER','AGE_AT_ENTRY' ,'B_OFF_APREM'], ascending=[True,True ,True])
# df_slice_2_TRA = df_slice_2_TRA.sort_index(by=['POL_NUMBER','AGE_AT_ENTRY' ,'B_OFF_APREM'], ascending=[True,True ,True])


if type(df_slice_2_TRA.ix[0,'POL_NUMBER']) == str:
    df_slice_2_TRA.ix[:,'POL_NUMBER'] = [x.replace('"', '') for x in df_slice_2_TRA.ix[:,'POL_NUMBER']]
    df_slice_2_TRA.ix[:,'POL_NUMBER'] = [as_integer(x) for x in df_slice_2_TRA.ix[:, 'POL_NUMBER']]
# if type(df_slice_2_TRA.ix[0,'PREMIUM_TYPE']) == str:
#     df_slice_2_TRA.ix[:,'PREMIUM_TYPE'] = [as_integer(x) for x in df_slice_2_TRA.ix[:, 'PREMIUM_TYPE']]

#________________________

# df_slice_1_IP = read_and_slice('Mpf', filename_mpf_test[3], 
#                 ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY','PREMIUM_TYPE',
#                 'DURATIONIF_M','SEX','SMOKER_IND', 'SUM_ASSURED', 'B_BEN_NO', 'B_OFF_APREM'])

# df_slice_2_IP = read_and_slice(foldername_slice_2, filename_test[3],
#             ['AGE_AT_ENTRY','POL_NUMBER','SUM_ASSURED','B_BEN_NO','B_OFF_APREM',
#             'MOS_PV_PREM','ANNUAL_PREM_12','BE_RESERVE'])

df_slice_1_IP = pd.read_csv(
        r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\Mpf\\' + filename_mpf_test[3])
df_slice_2_IP = pd.read_csv(
        r'C:\\galati_files\\pyscripts\\Callo_Repricing\\Compare_runs\\' +\
        foldername_slice_2 +'\\' + filename_test[3])
df_slice_2_IP.drop('L_LIFE_ID', axis=1, inplace=True)
df_slice_1_IP.drop(['BENEFIT_CODE', 'DEFER_PER_MP', 'TOTAL_SI', 'BEN_PERIOD', 'OCC_CLASS', 
                'DII_TYPE'], axis=1, inplace=True)
df_slice_1_IP.drop(['BROKER', 'OTR_ANNPHIBEN', 'CHANNEL'], axis=1, inplace=True)



# ___________________________________________________
# if df_slice_1.shape[0] == df_slice_2.shape[0]:
#     for i in range(0, df_slice_1.shape[0]):
#         df_slice_1.ix[i,'KEY'] = i+1
#         df_slice_2.ix[i,'KEY'] = i+1

# if df_slice_1_TRA.shape[0] == df_slice_2_TRA.shape[0]:
#     for i in range(0, df_slice_1_TRA.shape[0]):
#         df_slice_1_TRA.ix[i,'KEY'] = i+1
#         df_slice_2_TRA.ix[i,'KEY'] = i+1

# if df_slice_1_TPD.shape[0] == df_slice_2_TPD.shape[0]:
#     for i in range(0, df_slice_1_TRA.shape[0]):
#         df_slice_1_TPD.ix[i,'KEY'] = i+1
#         df_slice_2_TPD.ix[i,'KEY'] = i+1


# # cleaning data_______________________
# df_slice_2_IP.ix[:,'POL_NUMBER'] = [x.replace('"', '') for x in df_slice_2_IP.ix[:,'POL_NUMBER']]
# df_slice_2_IP.ix[:,'B_BEN_NO']  = [x.replace(' ', '') for x in df_slice_2_IP.ix[:,'B_BEN_NO']]
# df_slice_2_IP.ix[:,'B_BEN_NO']  = [x.replace('"', '') for x in df_slice_2_IP.ix[:,'B_BEN_NO']]
# # df_slice_2_IP.ix[:,'B_BEN_NO'] = [int(x) for x in df_slice_2_IP.ix[:,'B_BEN_NO']]
# # df_slice_1_IP = df_slice_1_IP.drop_duplicates(['POL_NUMBER','L_LIFE_ID','SUM_ASSURED', 'B_BEN_NO'])

# #________________________

# combining MPF and Results data for each benefit type 
# _________________________
# df_slice_2_sub =df_slice_2.drop(
#     ['POL_NUMBER','SUM_ASSURED', 'AGE_AT_ENTRY', 'B_OFF_APREM', 'PREMIUM_TYPE','POLICY_FEE'],
#     axis = 1)

# df_slice_2_TRA_sub =df_slice_2_TRA.drop(
#     ['POL_NUMBER','SUM_ASSURED', 'AGE_AT_ENTRY', 'B_OFF_APREM', 'PREMIUM_TYPE','POLICY_FEE'],
#     axis = 1)

# df_slice_2_TPD_sub =df_slice_2_TPD.drop(
#     ['POL_NUMBER','SUM_ASSURED', 'AGE_AT_ENTRY', 'B_OFF_APREM', 'PREMIUM_TYPE','POLICY_FEE'],
#     axis = 1)
# death_cmb = df_slice_1.merge(df_slice_2_sub, how = 'left')
# tra_cmb = df_slice_1_TRA.merge(df_slice_2_TRA_sub, how = 'left')
# tpd_cmb = df_slice_1_TPD.merge(df_slice_2_TPD_sub, how = 'left')
# ip_cmb = merge_by_id(df_slice_1_IP, df_slice_2_IP)
# __________________________


# death_cmb = merge_by_id_expanded(df_slice_1, df_slice_2)
# tpd_cmb = merge_by_id_expanded(df_slice_1_TPD, df_slice_2_TPD)
# tra_cmb = merge_by_id_expanded(df_slice_1_TRA, df_slice_2_TRA)
# ip_cmb = merge_by_id(df_slice_1_IP, df_slice_2_IP)

print df_slice_1_IP.info()
print df_slice_2_IP.info()

# df_slice_1 = df_slice_1.drop(['POL_NUMBER', 'SUM_ASSURED','B_OFF_APREM', 'PREMIUM_TYPE', 'POLICY_FEE'], axis=1)
death_cmb =pd.concat([df_slice_1,df_slice_2], axis = 1)
tpd_cmb = pd.concat([df_slice_1_TPD,df_slice_2_TPD], axis = 1)
tra_cmb = pd.concat([df_slice_1_TRA,df_slice_2_TRA], axis = 1)
ip_cmb = merge_by_id(df_slice_1_IP, df_slice_2_IP)
print ip_cmb.info()

death_cmb = death_cmb.loc[:,~death_cmb.columns.duplicated()]
tra_cmb = tra_cmb.loc[:,~tra_cmb.columns.duplicated()]
tpd_cmb = tpd_cmb.loc[:,~tpd_cmb.columns.duplicated()]
ip_cmb = ip_cmb.drop_duplicates(['POL_NUMBER', 'L_LIFE_ID','SUM_ASSURED', 'AGE_AT_ENTRY', 'B_BEN_NO'])


if df_slice_1.shape[0] != death_cmb.shape[0]:
    print ("CHECK MERGING of death_cmb. Length before and after is different")
    print ('before:'), df_slice_2.shape[0]
    print ('after:'), death_cmb.shape[0]
if df_slice_1['SUM_ASSURED'].sum() != death_cmb['SUM_ASSURED'].sum():
    print ("CHECK MERGING of death_cmb. Total Sum Assured before and after is different")
if df_slice_1_TRA.shape[0] != tra_cmb.shape[0]:
    print ("CHECK MERGING of tra_cmb. Length before and after is different")
    print ('before:'), df_slice_1_TRA.shape[0]
    print ('after:'), tra_cmb.shape[0]
if df_slice_1_TRA['SUM_ASSURED'].sum() != tra_cmb['SUM_ASSURED'].sum():
    print ("CHECK MERGING of tra_cmb. Total Sum Assured before and after is different")
if df_slice_1_TPD.shape[0] != tpd_cmb.shape[0]:
    print ("CHECK MERGING of tpd_cmb. Length before and after is different")
    print ('before:'), df_slice_1_TPD.shape[0]
    print ('after:'), tpd_cmb.shape[0]
if df_slice_1_TPD['SUM_ASSURED'].sum() != tpd_cmb['SUM_ASSURED'].sum():
    print ("CHECK MERGING of tpd_cmb. Total Sum Assured before and after is different")
if df_slice_1_IP.shape[0] != ip_cmb.shape[0]:
    print ("CHECK MERGING of ip_cmb. Length before and after is different")
    print ('before:'), df_slice_1_IP.shape[0]
    print ('after:'), ip_cmb.shape[0]
if df_slice_1_IP['SUM_ASSURED'].sum() != ip_cmb['SUM_ASSURED'].sum():
    print ("CHECK MERGING of ip_cmb. Total Sum Assured before and after is different")


#list-comprehensions
# tra_cmb.ix[:,'BE_RESERVE'] = [x.replace(',', '') for x in tra_cmb.ix[:, 'BE_RESERVE']]
# tra_cmb.ix[:,'BE_RESERVE'] = [x.replace(' ', '') for x in tra_cmb.ix[:, 'BE_RESERVE']]
# tra_cmb.ix[:,'BE_RESERVE'] = [as_float(x) for x in tra_cmb.ix[:, 'BE_RESERVE']]
print('finished merging')

# finding total sum insured per life id for each benefit. 
# So, if one life_id had 2 life benefits under one policy, we will find total sum insured
death_cmb['SA'] = death_cmb.groupby('L_LIFE_ID')['SUM_ASSURED'].transform('sum')
death_cmb['ANNUM_PREM'] = death_cmb.groupby('L_LIFE_ID')['B_OFF_APREM'].transform('sum')
death_cmb['ANNUM_PREM_New'] = death_cmb.groupby('L_LIFE_ID')['ANNUAL_PREM_1'].transform('sum')
death_cmb['ANNUM_PREM_13'] = death_cmb.groupby('L_LIFE_ID')['ANNUAL_PREM_13'].transform('sum')
death_cmb['ANNUM_PV_PREM_New'] = death_cmb.groupby('L_LIFE_ID')['PV_PREM'].transform('sum')
death_cmb['BEL'] = death_cmb.groupby('L_LIFE_ID')['BE_RESERVE'].transform('sum')
death_cmb = death_cmb.drop_duplicates(['L_LIFE_ID'])
death_cmb['Age'] = (death_cmb.AGE_AT_ENTRY + death_cmb.DURATIONIF_M/12).round()
death_cmb['Death'] = 'LIFE'
death_cmb.drop(['B_OFF_APREM', 'BE_RESERVE','SUM_ASSURED',
            'POL_NUMBER','AGE_AT_ENTRY','DURATIONIF_M', 'ANNUAL_PREM_1', 'ANNUAL_PREM_13','PV_PREM'],
                                                axis = 1, inplace = True)

#CHECK:
# df_cmb = pd.DataFrame()
# df_cmb['SUM_ASSURED'] = death_cmb.groupby('L_LIFE_ID', sort = True)['SUM_ASSURED'].sum()
# df_cmb['ANNUM_PREM'] = death_cmb.groupby('L_LIFE_ID', sort = True)['B_OFF_APREM'].sum()
# df_cmb['BEL'] = death_cmb.groupby('L_LIFE_ID', sort = True)['BE_RESERVE'].sum()
#END CHECK

print tpd_cmb.info()
print df_slice_2_TPD.info()

tpd_cmb['SA'] = tpd_cmb.groupby('L_LIFE_ID')['SUM_ASSURED'].transform('sum')
tpd_cmb['ANNUM_PREM'] = tpd_cmb.groupby('L_LIFE_ID')['B_OFF_APREM'].transform('sum')
tpd_cmb['ANNUM_PREM_New'] = tpd_cmb.groupby('L_LIFE_ID')['ANNUAL_PREM_1'].transform('sum')
tpd_cmb['ANNUM_PREM_13'] = tpd_cmb.groupby('L_LIFE_ID')['ANNUAL_PREM_13'].transform('sum')
tpd_cmb['ANNUM_PV_PREM_New'] = tpd_cmb.groupby('L_LIFE_ID')['PV_PREM'].transform('sum')
tpd_cmb['BEL'] = tpd_cmb.groupby('L_LIFE_ID')['BE_RESERVE'].transform('sum')
tpd_cmb = tpd_cmb.drop_duplicates(['L_LIFE_ID'])
tpd_cmb['Age'] = (tpd_cmb.AGE_AT_ENTRY + tpd_cmb.DURATIONIF_M/12).round()
tpd_cmb['TPD'] = 'TPD'
tpd_cmb.drop(['B_OFF_APREM', 'BE_RESERVE','SUM_ASSURED',
            'POL_NUMBER','AGE_AT_ENTRY','DURATIONIF_M', 'ANNUAL_PREM_1', 'ANNUAL_PREM_13', 'PV_PREM'],
                                            axis = 1, inplace = True)

print ('TRA_______')


tra_cmb['SA'] = tra_cmb.groupby('L_LIFE_ID')['SUM_ASSURED'].transform('sum')
tra_cmb['ANNUM_PREM'] = tra_cmb.groupby('L_LIFE_ID')['B_OFF_APREM'].transform('sum')
tra_cmb['ANNUM_PREM_New'] = tra_cmb.groupby('L_LIFE_ID')['ANNUAL_PREM_1'].transform('sum')
tra_cmb['ANNUM_PREM_13'] = tra_cmb.groupby('L_LIFE_ID')['ANNUAL_PREM_13'].transform('sum')
tra_cmb['ANNUM_PV_PREM_New'] = tra_cmb.groupby('L_LIFE_ID')['PV_PREM'].transform('sum')
tra_cmb['BEL'] = tra_cmb.groupby('L_LIFE_ID')['BE_RESERVE'].transform('sum')
tra_cmb = tra_cmb.drop_duplicates(['L_LIFE_ID'])
tra_cmb['Age'] = (tra_cmb.AGE_AT_ENTRY + tra_cmb.DURATIONIF_M/12).round()
tra_cmb['TRA'] = 'TRA'
tra_cmb.drop(['B_OFF_APREM', 'BE_RESERVE','SUM_ASSURED',
                    'POL_NUMBER','AGE_AT_ENTRY','DURATIONIF_M', 'ANNUAL_PREM_1', 'ANNUAL_PREM_13', 'PV_PREM'],
                                                axis = 1, inplace = True)

print ("IP___________________")

ip_cmb['SA'] = ip_cmb.groupby('L_LIFE_ID')['SUM_ASSURED'].transform('sum')
ip_cmb['ANNUM_PREM'] = ip_cmb.groupby('L_LIFE_ID')['B_OFF_APREM'].transform('sum')
ip_cmb['ANNUM_PREM_New'] = ip_cmb.groupby('L_LIFE_ID')['ANNUAL_PREM_01'].transform('sum')
ip_cmb['ANNUM_PREM_13'] = ip_cmb.groupby('L_LIFE_ID')['ANNUAL_PREM_12'].transform('sum')
ip_cmb['ANNUM_PV_PREM_New'] = ip_cmb.groupby('L_LIFE_ID')['MOS_PV_PREM'].transform('sum')
ip_cmb['BEL'] = ip_cmb.groupby('L_LIFE_ID')['BE_RESERVE'].transform('sum')
ip_cmb = ip_cmb.drop_duplicates(['L_LIFE_ID'])
ip_cmb['Age'] = (ip_cmb.AGE_AT_ENTRY + ip_cmb.DURATIONIF_M/12).round()
ip_cmb['IP'] = 'IP'
ip_cmb.drop(['B_OFF_APREM', 'BE_RESERVE','SUM_ASSURED', 'POL_NUMBER', 'B_BEN_NO',
                'AGE_AT_ENTRY','DURATIONIF_M', 'ANNUAL_PREM_12','ANNUAL_PREM_01','MOS_PV_PREM'],
                axis = 1, inplace = True)
# print ip_cmb.info()


print ('finished modifications')

# creating lists of unique life_id
id_1 = death_cmb.L_LIFE_ID.unique() #DON'T REALLY USE id_1, because use a whole death_cmb
id_2 = ip_cmb.L_LIFE_ID.unique()
id_3 = tpd_cmb.L_LIFE_ID.unique()
id_4 = tra_cmb.L_LIFE_ID.unique()

# id = np.append([id_1,id_2],[id_3, id_4])
# print np.count_nonzero(np.unique(id))


# creating df with unique life_id
id_frame_2 = pd.DataFrame(columns = ['L_LIFE_ID']) # 'col1', 'col2', 'col3','col4','col5','col6','col7'
id_frame_2.L_LIFE_ID = id_2
id_frame_3 =pd.DataFrame(id_3, columns= ['L_LIFE_ID'])
id_frame_4 =pd.DataFrame(id_4, columns= ['L_LIFE_ID'])


result_frame = pd.concat([death_cmb, id_frame_2, id_frame_3, id_frame_4])
result_frame = result_frame.drop_duplicates(['L_LIFE_ID'])

print('Att to check:')
print result_frame.info()
print ip_cmb.info()
print tpd_cmb.info()
print tra_cmb.info()

result_frame = pd.merge(result_frame, ip_cmb, on = 'L_LIFE_ID', how ='left', suffixes=('_LIFE', '_IP'))
result_frame = pd.merge(result_frame, tpd_cmb, on = 'L_LIFE_ID', how ='left')
result_frame = pd.merge(result_frame, tra_cmb, on = 'L_LIFE_ID', how ='left', suffixes=('_TPD', '_TRA'))
result_frame.reset_index(drop = True)

print ('merged into one frame')
print result_frame.info()

# BASE PREM- OAP, same for run 02 and run 06
# result_frame.ix[:,'Total_Premium'] = result_frame.ix[:,'ANNUM_PREM_LIFE'].fillna(0) +\
#                                 result_frame.ix[:,'ANNUM_PREM_IP'].fillna(0) +\
#                                 result_frame.ix[:,'ANNUM_PREM_TPD'].fillna(0) +\
#                                 result_frame.ix[:,'ANNUM_PREM_TRA'].fillna(0)

result_frame.ix[:,'Total_BEL'] = result_frame.ix[:,'BEL_LIFE'].fillna(0) +\
                                result_frame.ix[:,'BEL_IP'].fillna(0) +\
                                result_frame.ix[:,'BEL_TPD'].fillna(0) +\
                                result_frame.ix[:,'BEL_TRA'].fillna(0)
result_frame.ix[:,'Total_ANN_Prem'] = result_frame.ix[:,'ANNUM_PREM_New_LIFE'].fillna(0) +\
                                result_frame.ix[:,'ANNUM_PREM_New_IP'].fillna(0) +\
                                result_frame.ix[:,'ANNUM_PREM_New_TPD'].fillna(0) +\
                                result_frame.ix[:,'ANNUM_PREM_New_TRA'].fillna(0)
result_frame.ix[:,'Total_Prem_13'] = result_frame.ix[:,'ANNUM_PREM_13_LIFE'].fillna(0) +\
                                result_frame.ix[:,'ANNUM_PREM_13_IP'].fillna(0) +\
                                result_frame.ix[:,'ANNUM_PREM_13_TPD'].fillna(0) +\
                                result_frame.ix[:,'ANNUM_PREM_13_TRA'].fillna(0)
# result_frame.ix[:,'Total_PV_Prem'] = result_frame.ix[:,'ANNUM_PV_PREM_New_LIFE'].fillna(0) +\
#                                 result_frame.ix[:,'ANNUM_PV_PREM_New_IP'].fillna(0) +\
#                                 result_frame.ix[:,'ANNUM_PV_PREM_New_TPD'].fillna(0) +\
#                                 result_frame.ix[:,'ANNUM_PV_PREM_New_TRA'].fillna(0)

# result_frame['Package']=result_frame.apply(lambda x:'%s %s' % (x['Death'].fillna(''), x['TPD'].fillna('')),axis=1)
result_frame['Death'] = result_frame['Death'].fillna('')
result_frame['TRA'] = result_frame['TRA'].fillna('')
result_frame['TPD'] = result_frame['TPD'].fillna('')
result_frame['IP'] = result_frame['IP'].fillna('')

result_frame['Package'] = result_frame['Death'].astype(str) + ' ' + result_frame['TRA'].astype(str) + ' ' + result_frame['TPD'].astype(str) +\
                        ' ' + result_frame['IP'].astype(str)
result_frame['Package'] = result_frame['Package'].fillna(0)


# /remove PV_Prem: 'Total_PV_Prem', 'ANNUM_PV_PREM_New_LIFE', 'ANNUM_PV_PREM_New_TRA', ,'ANNUM_PV_PREM_New_TPD', 'ANNUM_PV_PREM_New_IP'
# 'Total_Premium', 'ANNUM_PREM_LIFE', ''ANNUM_PREM_TRA', 'ANNUM_PREM_TPD', 'ANNUM_PREM_IP'

cols = result_frame.columns.tolist()
cols = ['L_LIFE_ID', 'Total_BEL','Total_ANN_Prem', 'Total_Prem_13',
        'SEX_LIFE', 'SMOKER_IND_LIFE','Age_LIFE','SA_LIFE', 'ANNUM_PREM_New_LIFE', 'ANNUM_PREM_13_LIFE', 'BEL_LIFE',
        'SEX_TRA', 'SMOKER_IND_TRA','Age_TRA', 'SA_TRA', 'ANNUM_PREM_New_TRA', 'ANNUM_PREM_13_TRA','BEL_TRA',
        'SEX_TPD', 'SMOKER_IND_TPD','Age_TPD','SA_TPD', 'ANNUM_PREM_New_TPD', 'ANNUM_PREM_13_TPD' , 'BEL_TPD',
            'SEX_IP', 'SMOKER_IND_IP','Age_IP','SA_IP', 'ANNUM_PREM_New_IP','ANNUM_PREM_13_IP', 'BEL_IP', 'Package']
result_frame = result_frame[cols]

print ('before adding AGE')
define_par_per_life1('Age', ['Age_LIFE', 'Age_IP', 'Age_TPD','Age_TRA'])
print ('before adding SEX')
define_par_per_life1('Sex', ['SEX_LIFE','SEX_IP', 'SEX_TPD','SEX_TRA'])
print ('before adding Smoking Indic')

define_par_per_life1('Smoker_IND', ['SMOKER_IND_LIFE','SMOKER_IND_IP',
                    'SMOKER_IND_TPD','SMOKER_IND_TRA'])

print ('before adding age bins')
bins_age = [0, 31, 36, 41,  46, 51, 56, 61, 130]
group_names_age = ['30 & below', '31 - 35', '36 - 40','41 - 45','46 - 50','51 - 55', '56 - 60', '61 & above']


bins_ls = [0, 99999, 249999, 499999, 749999, 999999, 1499999, 1999999, 4999999,1000000000]
group_names_ls = ['Below $100K', '$100K - $250K', '$250K - $500K', '$500K - $750K', '$750K-$1M',
                    '$1M - $1.5M','$1.5M - $2M', '$2M - $5M', '$5M & above']

bins_ip = [0, 999, 2499, 4999, 7499, 99999, 5000000]
group_names_ip = ['Below $1K', '$1K - $2.5K', '$2.5K - $5K', '$5K - $7.5K', '$7.5K - $10K',
                    '$10K & above']

result_frame['Age_Group'] = pd.cut(result_frame['Age'], bins_age, labels = group_names_age)
result_frame['TERM_SI_BAND'] = pd.cut(result_frame['SA_LIFE'], bins_ls, labels = group_names_ls)
result_frame['TRA_SI_BAND'] = pd.cut(result_frame['SA_TRA'], bins_ls, labels = group_names_ls)
result_frame['TPD_SI_BAND'] = pd.cut(result_frame['SA_TPD'], bins_ls, labels = group_names_ls)
result_frame['IP_SI_BAND'] = pd.cut(result_frame['SA_IP'], bins_ip, labels = group_names_ip)




#Save output to dataframes
death_cmb.to_csv(fpath + '\\' + '1.Death_dataframe_byID.csv')
ip_cmb.to_csv(fpath + '\\' +'1.IP_dataframe_byID.csv')
tpd_cmb.to_csv(fpath + '\\' +'1.TPD_dataframe_byID.csv')
tra_cmb.to_csv(fpath + '\\' +'1.TRA_dataframe_byID.csv')
result_frame.to_csv(fpath + '\\' +'Result_run'+f_run_name+'_byID.csv')
print fpath

# id_list = Set(id_1)
# res_id = np.concatenate((id_1,id_2), axis = 0)
# res_frame['L_LIFE_ID'] = np.unique(res_id)

# res_frame = pd.merge(res_frame, death_cmb, on = 'L_LIFE_ID', how ='inner')
# res_frame.to_csv('result.csv')

# for i in range(0, results.shape[0]):
#     if result.ix[i,'res_frameL_LIFE_ID'] == df_cmb.ix['L_LIFE_ID']:
#         result.ix[i, ]






# print len(id_list)

# df[is.nan(df)] = 0




# for i in id_list:
#     for  in death_cmb[]:
#         result[death_cmb.ix[i]]



# fp1 = get_file_path('Results Files', 'Death_test.csv')
# df = read_if_exists(fp1)

# df_slice_1 = df.ix[:,['POL_NUMBER','L_LIFE_ID','B_BEN_NO','AGE_AT_ENTRY',
#                     'B_OFF_APREM', 'DURATIONIF_M', 'POLICY_COUNT', 'SEX',
#                     'SMOKER_IND', 'SUM_ASSURED','B_OFF_APREM','PREMIUM_TYPE']]
# print "read 1"
# fp2 = get_file_path('Mpf', 'Death mpf_test.csv')
# df_R = pd.read_csv(fp2)
# df_slice_2 = df_R.ix[:,['POL_NUMBER','L_LIFE_ID','B_BEN_NO','BE_RESERVE']]
# print "read 2"

#id_1 = gen_id(df_slice_1)
#print id_1


# df_IP = pd.read_csv('Income secure mpf.csv')
# df_TPD = pd.read_csv('TPD mpf.csv')
# df_TRA = pd.read_csv('Trauma mpf.csv')

# df1 = pd.DataFrame()
# df1 = df.ix[:,['POL_NUMBER','L_LIFE_ID','B_BEN_NO','AGE_AT_ENTRY','B_OFF_APREM', 'DURATIONIF_M', 'POLICY_COUNT', 'SEX',
#             'SMOKER_IND', 'SUM_ASSURED','B_OFF_APREM']]

# df2 = pd.DataFrame()
# df2 = df_R.ix[:,['POL_NUMBER','PREMIUM_TYPE','BE_RESERVE']]

# df_comb = pd.merge(df1,df2, on = 'POL_NUMBER', how='inner')
# df_comb2 = pd.merge(df1,df2, on = 'POL_NUMBER', how='outer')


# # print len(df_comb.index)
# print len(df_comb2.index)

# print df1.info()
# print df_comb.info()
# print df_comb.head(5)

# df_comb['Check_Duplicate_By_Pol'] = df_comb.duplicated(subset='POL_NUMBER', keep=False)


























# import pandas as pd
# import numpy as np
# import os
# from sets import Set

# def get_file_path(foldname, filename):
#     cwd = os.getcwd()
#     filepath = os.path.join(cwd, foldname, filename)
#     return(filepath)

# def read_if_exists(fpath):
#     if os.path.exists(fpath) == True:
#         df = pd.read_csv(fpath)
#         return(df)
#     else:
#         print 'File ' + fpath + ' not found'
#         exit(1)

# def read_and_slice(nfold, nfile, col_list):
#     path = get_file_path(nfold, nfile)
#     df = read_if_exists(path)
#     df = df.ix[:, col_list]
#     return(df)

# def merge_by_id(df_a, df_b):
#     return(pd.merge(df_a, df_b, how = 'outer', 
#             on = ['POL_NUMBER','L_LIFE_ID','SUM_ASSURED']))

# # sets cwd to location of the script
# os.chdir(os.path.dirname(__file__))

# df_slice_1 = read_and_slice('Mpf', 'Death mpf.csv', 
#     ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY',
#     'DURATIONIF_M', 'SEX',
#     'SMOKER_IND', 'SUM_ASSURED','B_OFF_APREM','PREMIUM_TYPE'])

# df_slice_2 = read_and_slice('Results Files', 'Death.csv',
#     ['POL_NUMBER','L_LIFE_ID','SUM_ASSURED','BE_RESERVE'])
# #________________________

# df_slice_1_IP = read_and_slice('Mpf', 'Income secure mpf.csv', 
#     ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY',
#     'DURATIONIF_M','SEX','SMOKER_IND', 'ANN_PHI_BEN','ANNUAL_PREM','PREMIUM_TYPE'])
# df_slice_1_IP = df_slice_1_IP.rename(columns = {'ANN_PHI_BEN':'SUM_ASSURED','ANNUAL_PREM':'B_OFF_APREM'})
# # df_slice_1_IP = df_slice_1_IP.dropna()
# df_slice_2_IP = read_and_slice('Results Files', 'Income secure.csv',
#     ['POL_NUMBER','L_LIFE_ID','SUM_INS', 'BE_RESERVE'])
# df_slice_2_IP = df_slice_2_IP.rename(columns = {'SUM_INS':'SUM_ASSURED'})
# #________________________

# df_slice_1_TPD = read_and_slice('Mpf', 'TPD mpf.csv', 
#     ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY',
#     'DURATIONIF_M','SEX','SMOKER_IND', 'SUM_ASSURED','B_OFF_APREM','PREMIUM_TYPE'])
# # df_slice_1_TPD = df_slice_1_TPD.dropna()
# df_slice_2_TPD = read_and_slice('Results Files', 'TPD.csv',
#     ['POL_NUMBER','L_LIFE_ID','SUM_ASSURED', 'BE_RESERVE'])


# # print df_slice_1_TPD.info()
# # print df_slice_2_TPD.info()
# #________________________

# df_slice_1_TRA = read_and_slice('Mpf', 'Trauma mpf.csv', 
#     ['POL_NUMBER','L_LIFE_ID','AGE_AT_ENTRY',
#     'DURATIONIF_M','SEX','SMOKER_IND', 'SUM_ASSURED','B_OFF_APREM','PREMIUM_TYPE'])
# # df_slice_1_TPD = df_slice_1_TPD.dropna()
# df_slice_2_TRA = read_and_slice('Results Files', 'Trauma.csv',
#     ['POL_NUMBER','L_LIFE_ID','SUM_ASSURED', 'BE_RESERVE'])


# # print df_slice_1_TRA.info()
# # print df_slice_2_TRA.info()
# # #________________________

# death_cmb = merge_by_id(df_slice_1, df_slice_2)
# death_cmb = death_cmb.drop_duplicates(['L_LIFE_ID','SUM_ASSURED'])

# # print ('duplicates removed:')
# # print death_cmb.head()
# # death_cmb.to_csv('2.csv')

# ip_cmb = merge_by_id(df_slice_1_IP, df_slice_2_IP)

# df_slice_1_IP.to_csv('output_slice1.csv')
# df_slice_2_IP.to_csv('output_slice2.csv')
# ip_cmb.to_csv('merged slices.csv')

# ip_cmb = ip_cmb.drop_duplicates(['L_LIFE_ID','SUM_ASSURED'])

# tpd_cmb = merge_by_id(df_slice_1_TPD, df_slice_2_TPD)
# tpd_cmb = tpd_cmb.drop_duplicates(['L_LIFE_ID','SUM_ASSURED'])

# tra_cmb = merge_by_id(df_slice_1_TRA, df_slice_2_TRA)
# tra_cmb = tra_cmb.drop_duplicates(['L_LIFE_ID','SUM_ASSURED'])

# death_cmb.to_csv('Death_dataframe.csv')
# ip_cmb.to_csv('IP_dataframe.csv')
# tpd_cmb.to_csv('TPD_dataframe.csv')
# tra_cmb.to_csv('TRA_dataframe.csv')

# #check duplic
# # print ('tra duplic:')
# # print tra_cmb.duplicated(['L_LIFE_ID','SUM_ASSURED']).unique()

# #Save by slices and merged 
# #df_slice_1_TPD.to_csv('output_slice1.csv')
# # df_slice_2_TPD.to_csv('output_slice2.csv')
# # tpd_cmb.to_csv('merged slices.csv')
# # print ('original:')
# # print ip_cmb.head()



# # print len(df_slice_1.L_LIFE_ID.unique())
# # for i in df_slice_1.L_LIFE_ID.unique()


# # print df_slice_1_IP.info()
# # print df_slice_1_IP.shape[0]
# # print df_slice_2_IP.shape[0]



# #_______________________


# death_cmb['SA'] = death_cmb.groupby('L_LIFE_ID')['SUM_ASSURED'].transform('sum')
# death_cmb['ANNUM_PREM'] = death_cmb.groupby('L_LIFE_ID')['B_OFF_APREM'].transform('sum')
# death_cmb['BEL'] = death_cmb.groupby('L_LIFE_ID')['BE_RESERVE'].transform('sum')
# death_cmb = death_cmb.drop_duplicates(['L_LIFE_ID'])
# del death_cmb['B_OFF_APREM']
# del death_cmb['BE_RESERVE']
# del death_cmb['SUM_ASSURED']
# del death_cmb['POL_NUMBER']
# death_cmb['Age'] = (death_cmb.AGE_AT_ENTRY + death_cmb.DURATIONIF_M/12)
# del death_cmb['AGE_AT_ENTRY']
# del death_cmb['DURATIONIF_M']



# # print ('death_cmb.head:')
# # print death_cmb.head()

# #CHECK:
# # df_cmb = pd.DataFrame()
# # df_cmb['SUM_ASSURED'] = death_cmb.groupby('L_LIFE_ID', sort = True)['SUM_ASSURED'].sum()
# # df_cmb['ANNUM_PREM'] = death_cmb.groupby('L_LIFE_ID', sort = True)['B_OFF_APREM'].sum()
# # df_cmb['BEL'] = death_cmb.groupby('L_LIFE_ID', sort = True)['BE_RESERVE'].sum()
# #END CHECK

# # print death_cmb.shape[1]
# # print death_cmb.shape[0]

# ip_cmb['SA'] = ip_cmb.groupby('L_LIFE_ID')['SUM_ASSURED'].transform('sum')
# ip_cmb['ANNUM_PREM'] = ip_cmb.groupby('L_LIFE_ID')['B_OFF_APREM'].transform('sum')
# ip_cmb['BEL'] = ip_cmb.groupby('L_LIFE_ID')['BE_RESERVE'].transform('sum')
# ip_cmb = ip_cmb.drop_duplicates(['L_LIFE_ID'])
# del ip_cmb['B_OFF_APREM']
# del ip_cmb['BE_RESERVE']
# del ip_cmb['SUM_ASSURED']
# del ip_cmb['POL_NUMBER']
# ip_cmb['Age'] = (ip_cmb.AGE_AT_ENTRY + ip_cmb.DURATIONIF_M/12)
# del ip_cmb['AGE_AT_ENTRY']
# del ip_cmb['DURATIONIF_M']



# tpd_cmb['SA'] = tpd_cmb.groupby('L_LIFE_ID')['SUM_ASSURED'].transform('sum')
# tpd_cmb['ANNUM_PREM'] = tpd_cmb.groupby('L_LIFE_ID')['B_OFF_APREM'].transform('sum')
# tpd_cmb['BEL'] = tpd_cmb.groupby('L_LIFE_ID')['BE_RESERVE'].transform('sum')
# tpd_cmb = tpd_cmb.drop_duplicates(['L_LIFE_ID'])
# del tpd_cmb['B_OFF_APREM']
# del tpd_cmb['BE_RESERVE']
# del tpd_cmb['SUM_ASSURED']
# del tpd_cmb['POL_NUMBER']
# tpd_cmb['Age'] = (tpd_cmb.AGE_AT_ENTRY + tpd_cmb.DURATIONIF_M/12)
# del tpd_cmb['AGE_AT_ENTRY']
# del tpd_cmb['DURATIONIF_M']

# tra_cmb['SA'] = tra_cmb.groupby('L_LIFE_ID')['SUM_ASSURED'].transform('sum')
# tra_cmb['ANNUM_PREM'] = tra_cmb.groupby('L_LIFE_ID')['B_OFF_APREM'].transform('sum')
# tra_cmb['BEL'] = tra_cmb.groupby('L_LIFE_ID')['BE_RESERVE'].transform('sum')
# tra_cmb = tra_cmb.drop_duplicates(['L_LIFE_ID'])
# del tra_cmb['B_OFF_APREM']
# del tra_cmb['BE_RESERVE']
# del tra_cmb['SUM_ASSURED']
# del tra_cmb['POL_NUMBER']
# tra_cmb['Age'] = (tra_cmb.AGE_AT_ENTRY + tra_cmb.DURATIONIF_M/12)
# del tra_cmb['AGE_AT_ENTRY']
# del tra_cmb['DURATIONIF_M']

# death_cmb.to_csv('res_death.csv')
# ip_cmb.to_csv('res_ip.csv')
# tpd_cmb.to_csv('res_tpd.csv')
# tra_cmb.to_csv('res_tra.csv')




# id_1 = death_cmb.L_LIFE_ID.unique()
# id_2 = ip_cmb.L_LIFE_ID.unique()
# id_3 = tpd_cmb.L_LIFE_ID.unique()
# id_4 = tra_cmb.L_LIFE_ID.unique()


# id_frame_2 = pd.DataFrame(columns = ['L_LIFE_ID']) # 'col1', 'col2', 'col3','col4','col5','col6','col7'
# id_frame_2.L_LIFE_ID = id_2

# id_frame_3 =pd.DataFrame(id_3, columns= ['L_LIFE_ID'])
# id_frame_4 =pd.DataFrame(id_4, columns= ['L_LIFE_ID'])


# result_frame = pd.concat([death_cmb, id_frame_2, id_frame_3, id_frame_4])
# result_frame = result_frame.drop_duplicates(['L_LIFE_ID'])

# result_frame = pd.merge(result_frame, ip_cmb, on = 'L_LIFE_ID', how ='left', suffixes=('_LIFE', '_IP'))
# result_frame = pd.merge(result_frame, tpd_cmb, on = 'L_LIFE_ID', how ='left')
# result_frame = pd.merge(result_frame, tra_cmb, on = 'L_LIFE_ID', how ='left', suffixes=('_TPD', '_TRA'))
# result_frame.reset_index(drop = True)
# result_frame.ix[:,'Total_Premium'] = result_frame.ix[:,'ANNUM_PREM_LIFE'].fillna(0) +\
#                                 result_frame.ix[:,'ANNUM_PREM_IP'].fillna(0) +\
#                                 result_frame.ix[:,'ANNUM_PREM_TPD'].fillna(0) +\
#                                 result_frame.ix[:,'ANNUM_PREM_TRA'].fillna(0)

# cols = result_frame.columns.tolist()

# cols = ['L_LIFE_ID','Total_Premium','ANNUM_PREM_LIFE', 'Age_LIFE', 'BEL_LIFE', 'PREMIUM_TYPE_LIFE', 'SA_LIFE',
#     'SEX_LIFE', 'SMOKER_IND_LIFE', 'SEX_IP', 'SMOKER_IND_IP', 'PREMIUM_TYPE_IP', 'SA_IP',
#     'ANNUM_PREM_IP', 'BEL_IP', 'Age_IP', 'SEX_TPD', 'SMOKER_IND_TPD', 'PREMIUM_TYPE_TPD',
#     'SA_TPD', 'ANNUM_PREM_TPD', 'BEL_TPD', 'Age_TPD', 'SEX_TRA', 'SMOKER_IND_TRA',
#     'PREMIUM_TYPE_TRA', 'SA_TRA', 'ANNUM_PREM_TRA', 'BEL_TRA', 'Age_TRA']

# result_frame = result_frame[cols]




# result_frame.to_csv('RESULT_all2.csv')

