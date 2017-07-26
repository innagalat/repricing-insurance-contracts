# stitching 4 benefits together, finding corrresponding BEl and summary statistics per unique life id.
import pandas as pd
import numpy as np
import os
from sets import Set
import sys
from datetime import datetime 

run_name = '0.repricing-run02'   
# run_name = '0.repricing-run06'


f_run_name = run_name[-5:]#last 5 letters
print f_run_name



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


PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data')
RESULT = os.path.join(PROJ, 'results')


fpath_res = os.path.join(RESULT, 'intermediate-' + f_run_name)
print fpath_res

death_cmb = pd.read_csv(fpath_res + '\\' + 'Death' + run_name[-2:] + '.csv')
tpd_cmb = pd.read_csv(fpath_res + '\\' + 'TPD' + run_name[-2:] + '.csv')
tra_cmb = pd.read_csv(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv')
ip_cmb = pd.read_csv(fpath_res + '\\' + 'Income secure' + run_name[-2:] + '.csv')


# death_cmb.drop(['BROKER','DCS_REIN_SI', 'CHANNEL'], axis = 1, inplace = True)

# tpd_cmb.drop(['BROKER','DCS_REIN_SI', 'CHANNEL'], axis = 1, inplace = True)
# tra_cmb.drop(['BROKER','DCS_REIN_SI', 'CHANNEL'], axis = 1, inplace = True)
# ip_cmb.drop(['BENEFIT_CODE', 'DEFER_PER_MP', 'TOTAL_SI', 'BEN_PERIOD', 'OCC_CLASS', 
#                 'DII_TYPE'], axis=1, inplace=True)
# ip_cmb.drop(['BROKER', 'OTR_ANNPHIBEN', 'CHANNEL'], axis=1, inplace=True)



# # cleaning data_______________________
if type(tra_cmb.ix[0,'POL_NUMBER']) == str:
    tra_cmb.ix[:,'POL_NUMBER'] = [x.replace('"', '') for x in tra_cmb.ix[:,'POL_NUMBER']]
    tra_cmb.ix[:,'POL_NUMBER'] = [as_integer(x) for x in tra_cmb.ix[:, 'POL_NUMBER']]
# df_slice_2_IP.ix[:,'POL_NUMBER'] = [x.replace('"', '') for x in df_slice_2_IP.ix[:,'POL_NUMBER']]
# df_slice_2_IP.ix[:,'B_BEN_NO']  = [x.replace(' ', '') for x in df_slice_2_IP.ix[:,'B_BEN_NO']]
# df_slice_2_IP.ix[:,'B_BEN_NO']  = [x.replace('"', '') for x in df_slice_2_IP.ix[:,'B_BEN_NO']]
# # df_slice_2_IP.ix[:,'B_BEN_NO'] = [int(x) for x in df_slice_2_IP.ix[:,'B_BEN_NO']]
# # df_slice_1_IP = df_slice_1_IP.drop_duplicates(['POL_NUMBER','L_LIFE_ID','SUM_ASSURED', 'B_BEN_NO'])

# #________________________


#list-comprehensions
# tra_cmb.ix[:,'BE_RESERVE'] = [x.replace(',', '') for x in tra_cmb.ix[:, 'BE_RESERVE']]
# tra_cmb.ix[:,'BE_RESERVE'] = [x.replace(' ', '') for x in tra_cmb.ix[:, 'BE_RESERVE']]
# tra_cmb.ix[:,'BE_RESERVE'] = [as_float(x) for x in tra_cmb.ix[:, 'BE_RESERVE']]




def return_result(data):
    data['SA'] = data.groupby('L_LIFE_ID')['SUM_ASSURED'].transform('sum')
    data['BASE_PREM'] = data.groupby('L_LIFE_ID')['B_OFF_APREM'].transform('sum')
    data['A_PREM'] = data.groupby('L_LIFE_ID')['ANNUAL_PREM_1'].transform('sum')
    data['A_PREM_13'] = data.groupby('L_LIFE_ID')['ANNUAL_PREM_13'].transform('sum')
    data['BEL'] = data.groupby('L_LIFE_ID')['BE_RESERVE'].transform('sum')
    data = data.drop_duplicates(['L_LIFE_ID'])
    data = data.drop(['B_OFF_APREM', 'BE_RESERVE','SUM_ASSURED',
            'POL_NUMBER', 'ANNUAL_PREM_1', 'ANNUAL_PREM_13'],
                                                axis = 1)
    return(data)

death_cmb = return_result(death_cmb)
tra_cmb = return_result(tra_cmb)
tpd_cmb = return_result(tpd_cmb)
ip_cmb = return_result(ip_cmb)

death_cmb['Death'] = 'LIFE'
tpd_cmb['TPD'] = 'TPD'
tra_cmb['TRA'] = 'TRA'
ip_cmb['IP'] = 'IP'

death_cmb['AGE'] = (death_cmb.AGE_AT_ENTRY + death_cmb.DURATIONIF_M/12).round()
tpd_cmb['AGE'] = (tpd_cmb.AGE_AT_ENTRY + tpd_cmb.DURATIONIF_M/12).round()
tra_cmb['AGE'] = (tra_cmb.AGE_AT_ENTRY + tra_cmb.DURATIONIF_M/12).round()
ip_cmb['AGE'] = (ip_cmb.AGE_AT_ENTRY + ip_cmb.DURATIONIF_M/12).round()

death_cmb = death_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)
tpd_cmb = tpd_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)
tra_cmb = tra_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)
ip_cmb = ip_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M'], axis = 1)
ip_cmb.drop(['B_BEN_NO','MOS_PV_PREM'], axis = 1, inplace = True)
print ('finished modifications')

# creating lists of unique life_id
id_1 = death_cmb.L_LIFE_ID.unique() #DON'T REALLY USE id_1, because use a whole death_cmb
id_2 = ip_cmb.L_LIFE_ID.unique()
id_3 = tpd_cmb.L_LIFE_ID.unique()
id_4 = tra_cmb.L_LIFE_ID.unique()

# id = np.append([id_1,id_2],[id_3, id_4])
# print np.count_nonzero(np.unique(id))


# creating df with unique life_id
id_frame_2 = pd.DataFrame(columns = ['L_LIFE_ID'])
id_frame_2.L_LIFE_ID = id_2
id_frame_3 =pd.DataFrame(id_3, columns= ['L_LIFE_ID'])
id_frame_4 =pd.DataFrame(id_4, columns= ['L_LIFE_ID'])

result_frame = pd.concat([death_cmb, id_frame_2, id_frame_3, id_frame_4])
result_frame = result_frame.drop_duplicates(['L_LIFE_ID'])

print('Att to check:')
print result_frame.info()
# print ip_cmb.info()
# print tpd_cmb.info()
# print tra_cmb.info()


result_frame = pd.merge(result_frame, ip_cmb, on = 'L_LIFE_ID', how ='left', suffixes=('_LIFE', '_IP'))
result_frame = pd.merge(result_frame, tpd_cmb, on = 'L_LIFE_ID', how ='left')
result_frame = pd.merge(result_frame, tra_cmb, on = 'L_LIFE_ID', how ='left', suffixes=('_TPD', '_TRA'))
result_frame.reset_index(drop = True)

print ('merged into one frame')

# BASE PREM- OAP, same for run 02 and run 06
# result_frame.ix[:,'Total_Base_Prem'] = result_frame.ix[:,'BASE_PREM_LIFE'].fillna(0) +\
#                                 result_frame.ix[:,'BASE_PREM_IP'].fillna(0) +\
#                                 result_frame.ix[:,'BASE_PREM_TPD'].fillna(0) +\
#                                 result_frame.ix[:,'BASE_PREM_TRA'].fillna(0)

result_frame.ix[:,'TOT_BEL'] = result_frame.ix[:,'BEL_LIFE'].fillna(0) +\
                                result_frame.ix[:,'BEL_IP'].fillna(0) +\
                                result_frame.ix[:,'BEL_TPD'].fillna(0) +\
                                result_frame.ix[:,'BEL_TRA'].fillna(0)
result_frame.ix[:,'TOT_ANN_PREM'] = result_frame.ix[:,'A_PREM_LIFE'].fillna(0) +\
                                result_frame.ix[:,'A_PREM_IP'].fillna(0) +\
                                result_frame.ix[:,'A_PREM_TPD'].fillna(0) +\
                                result_frame.ix[:,'A_PREM_TRA'].fillna(0)
result_frame.ix[:,'TOT_PREM_13'] = result_frame.ix[:,'A_PREM_13_LIFE'].fillna(0) +\
                                result_frame.ix[:,'A_PREM_13_IP'].fillna(0) +\
                                result_frame.ix[:,'A_PREM_13_TPD'].fillna(0) +\
                                result_frame.ix[:,'A_PREM_13_TRA'].fillna(0)


# result_frame['Package']=result_frame.apply(lambda x:'%s %s' % (x['Death'].fillna(''), x['TPD'].fillna('')),axis=1)
result_frame['Death'] = result_frame['Death'].fillna('')
result_frame['TRA'] = result_frame['TRA'].fillna('')
result_frame['TPD'] = result_frame['TPD'].fillna('')
result_frame['IP'] = result_frame['IP'].fillna('')

result_frame['PACKAGE'] = result_frame['Death'].astype(str) + ' ' + result_frame['TRA'].astype(str) + ' ' + result_frame['TPD'].astype(str) +\
                        ' ' + result_frame['IP'].astype(str)
result_frame['PACKAGE'] = result_frame['PACKAGE'].fillna(0)

result_frame = result_frame.drop(['Death', 'TRA', 'TPD', 'IP'], axis =1)

# /remove PV_Prem: 'Total_PV_Prem', 'ANNUM_PV_PREM_New_LIFE', 'ANNUM_PV_PREM_New_TRA', ,'ANNUM_PV_PREM_New_TPD', 'ANNUM_PV_PREM_New_IP'
# 'Total_Premium', 'ANNUM_PREM_LIFE', ''ANNUM_PREM_TRA', 'ANNUM_PREM_TPD', 'ANNUM_PREM_IP'

print result_frame.info()

print ('before adding AGE')
define_par_per_life1('AGE', ['AGE_LIFE', 'AGE_IP', 'AGE_TPD','AGE_TRA'])
print ('before adding SEX')
define_par_per_life1('SEX', ['SEX_LIFE','SEX_IP', 'SEX_TPD','SEX_TRA'])
# print ('before adding CHANNEL')
# define_par_per_life1('CHANNEL', ['CHANNEL_LIFE','CHANNEL_IP', 'CHANNEL_TPD','CHANNEL_TRA'])

print ('before adding Smoking Indic')
define_par_per_life1('SMOKER_IND', ['SMOKER_IND_LIFE','SMOKER_IND_IP',
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

result_frame['AGE-GROUP'] = pd.cut(result_frame['AGE'], bins_age, labels = group_names_age)
result_frame['TERM_SI_BAND'] = pd.cut(result_frame['SA_LIFE'], bins_ls, labels = group_names_ls)
result_frame['TRA_SI_BAND'] = pd.cut(result_frame['SA_TRA'], bins_ls, labels = group_names_ls)
result_frame['TPD_SI_BAND'] = pd.cut(result_frame['SA_TPD'], bins_ls, labels = group_names_ls)
result_frame['IP_SI_BAND'] = pd.cut(result_frame['SA_IP'], bins_ip, labels = group_names_ip)


cols = result_frame.columns.tolist()
print result_frame.columns.values
# result_frame.to_csv(r'C:\\galati_files\\pyscripts\\callo-repricing\\compare-runs\\Resv0.csv')

# cols = ['L_LIFE_ID', 'PACKAGE', 'AGE', 'AGE-GROUP', 'SEX', 'SMOKER_IND',
#         'TOTAL_SI', 'TOT_BEL','TOT_ANN_PREM', 'TOT_PREM_13', 
        
#         'SA_LIFE', 'A_PREM_LIFE', 'A_PREM_13_LIFE', 'BEL_LIFE',
#         'BROKER_LIFE','CHANNEL_LIFE', 'DCS_REIN_SI_x', 'PREMIUM_TYPE_LIFE', 
        
#         'SA_IP', 'A_PREM_IP', 'A_PREM_13_IP','BEL_IP',
#         'PREMIUM_TYPE_IP', 'BENEFIT_CODE', 'DEFER_PER_MP', 'BEN_PERIOD', 'OCC_CLASS', 'DII_TYPE', 
#         'BROKER_IP', 'CHANNEL_IP', 'OTR_ANNPHIBEN',

#         'SA_TPD', 'A_PREM_TPD', 'A_PREM_13_TPD', 'BEL_TPD',
#         'BROKER_TPD', 'CHANNEL_TPD', 'DCS_REIN_SI_y', 'PREMIUM_TYPE_TPD', 

#         'SA_TRA', 'A_PREM_TRA', 'A_PREM_13_TRA', 'BEL_TRA',
#         'BROKER_TRA','CHANNEL_TRA','DCS_REIN_SI','PREMIUM_TYPE_TRA',

#         'TERM_SI_BAND', 'TRA_SI_BAND', 'TPD_SI_BAND', 'IP_SI_BAND']

cols = ['L_LIFE_ID', 'PACKAGE', 'AGE', 'AGE-GROUP', 'SEX', 'SMOKER_IND',
        'TOTAL_SI', 'TOT_BEL','TOT_ANN_PREM', 'TOT_PREM_13', 
        
        'SA_LIFE', 'A_PREM_LIFE', 'A_PREM_13_LIFE', 'BEL_LIFE',
        'BROKER_LIFE','CHANNEL_LIFE', 'DCS_REIN_SI_x',
        
        'SA_IP', 'A_PREM_IP', 'A_PREM_13_IP','BEL_IP',
        'BROKER_IP', 'CHANNEL_IP', 'OTR_ANNPHIBEN',

        'SA_TPD', 'A_PREM_TPD', 'A_PREM_13_TPD', 'BEL_TPD',
        'BROKER_TPD', 'CHANNEL_TPD', 'DCS_REIN_SI_y',

        'SA_TRA', 'A_PREM_TRA', 'A_PREM_13_TRA', 'BEL_TRA',
        'BROKER_TRA','CHANNEL_TRA','DCS_REIN_SI',

        'TERM_SI_BAND', 'TRA_SI_BAND', 'TPD_SI_BAND', 'IP_SI_BAND']

result_frame = result_frame[cols]

#Save output to dataframe
fpath_res = os.path.join(RESULT, 'final')

if os.path.exists(fpath_res) == False:
     os.makedirs(fpath_res)

result_frame.to_csv(fpath_res + '\\' +'Result_'+f_run_name+'_byID.csv')
exit('0')



