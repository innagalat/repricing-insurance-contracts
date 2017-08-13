# stitching 4 benefits together, finding corrresponding BEl and summary statistics per unique life id.
import pandas as pd
import numpy as np
import os
from sets import Set
import sys
from datetime import datetime 

run_name = '0.repricing-run02'   
# run_name = '0.repricing-run06'


policy_subset = 'ordinary-cover'

PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
DATA = os.path.join(PROJ, 'data', policy_subset)
RESULT = os.path.join(PROJ, 'results', policy_subset)


f_run_name = run_name[-5:]#last 5 letters
print f_run_name
fpath_res = os.path.join(RESULT, 'intermediate-' + f_run_name)
print fpath_res


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

# rename cols and keep structure, no aggreg
def return_result_2(data):
    data['SA'] = data['SUM_ASSURED']
    data['BASE_PREM'] = data['B_OFF_APREM']
    data['A_PREM'] = data['ANNUAL_PREM_1']
    data['A_PREM_13'] = data['ANNUAL_PREM_13']
    data['BEL'] = data['BE_RESERVE']
    # data = data.drop_duplicates(['L_LIFE_ID'])
    data = data.drop(['B_OFF_APREM', 'BE_RESERVE',
                'ANNUAL_PREM_1', 'ANNUAL_PREM_13'],
                                                axis = 1)
    return(data)

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


death_cmb = pd.read_csv(fpath_res + '\\' + 'Death' + run_name[-2:] + '.csv')
tpd_cmb = pd.read_csv(fpath_res + '\\' + 'TPD' + run_name[-2:] + '.csv')
ip_cmb = pd.read_csv(fpath_res + '\\' + 'Income secure' + run_name[-2:] + '.csv')




tra_cmb = pd.read_csv(fpath_res + '\\' + 'Trauma' + run_name[-2:] + '.csv')
# # cleaning data_______________________
if type(tra_cmb.ix[0,'POL_NUMBER']) == str:
    tra_cmb.ix[:,'POL_NUMBER'] = [x.replace('"', '') for x in tra_cmb.ix[:,'POL_NUMBER']]
    tra_cmb.ix[:,'POL_NUMBER'] = [as_integer(x) for x in tra_cmb.ix[:, 'POL_NUMBER']]

# #________________________

print ip_cmb['ANNUAL_PREM_1'].sum()

death_cmb = return_result_2(death_cmb)
tra_cmb = return_result_2(tra_cmb)
tpd_cmb = return_result_2(tpd_cmb)
ip_cmb = return_result_2(ip_cmb)

print ip_cmb['A_PREM'].sum()


death_cmb['Death'] = 'LIFE'
tpd_cmb['TPD'] = 'TPD'
tra_cmb['TRA'] = 'TRA'
ip_cmb['IP'] = 'IP'
ip_cmb['SA_IP'] = ip_cmb['SUM_ASSURED']

death_cmb['AGE'] = (death_cmb.AGE_AT_ENTRY + death_cmb.DURATIONIF_M/12).round()
tpd_cmb['AGE'] = (tpd_cmb.AGE_AT_ENTRY + tpd_cmb.DURATIONIF_M/12).round()
tra_cmb['AGE'] = (tra_cmb.AGE_AT_ENTRY + tra_cmb.DURATIONIF_M/12).round()
ip_cmb['AGE'] = (ip_cmb.AGE_AT_ENTRY + ip_cmb.DURATIONIF_M/12).round()

death_cmb = death_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M', 'SUM_ASSURED'], axis = 1)
tpd_cmb = tpd_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M', 'SUM_ASSURED'], axis = 1)
tra_cmb = tra_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M', 'SUM_ASSURED'], axis = 1)
ip_cmb = ip_cmb.drop(['AGE_AT_ENTRY','DURATIONIF_M', 'SUM_ASSURED'], axis = 1)
ip_cmb.drop(['B_BEN_NO','MOS_PV_PREM'], axis = 1, inplace = True)
print ('finished modifications')

result_frame = pd.concat([death_cmb, tra_cmb, tpd_cmb, ip_cmb])
result_frame.reset_index(drop = True)
print ('merged into one frame')
print result_frame.info()

# print('Att to check:')
# # print ip_cmb.info()
# # print tpd_cmb.info()
# # print tra_cmb.info()


print ('before adding age bins')
bins_age = [0, 31, 36, 41,  46, 51, 56, 61, 130]
group_names_age = ['30 & below', '31 - 35', '36 - 40','41 - 45','46 - 50','51 - 55', '56 - 60', '61 & above']


bins_ls = [0, 99999, 249999, 499999, 749999, 999999, 1499999, 1999999, 4999999,1000000000]
group_names_ls = ['Below $100K', '$100K - $250K', '$250K - $500K', '$500K - $750K', '$750K-$1M',
                    '$1M - $1.5M','$1.5M - $2M', '$2M - $5M', '$5M & above']

bins_ip = [0, 999, 2499, 4999, 7499, 9999, 5000000]
group_names_ip = ['Below $1K', '$1K - $2.5K', '$2.5K - $5K', '$5K - $7.5K', '$7.5K - $10K',
                    '$10K & above']

result_frame['AGE-GROUP'] = pd.cut(result_frame['AGE'], bins_age, labels = group_names_age)
result_frame['SI_LS_BAND'] = pd.cut(result_frame['SA'], bins_ls, labels = group_names_ls)
result_frame['SI_IP_BAND'] = pd.cut(result_frame['SA_IP'], bins_ip, labels = group_names_ip)

cols = result_frame.columns.tolist()
print result_frame.columns.values
# result_frame.to_csv(r'C:\\galati_files\\pyscripts\\callo-repricing\\compare-runs\\Resv0.csv')
cols = ['L_LIFE_ID','POL_NUMBER', 'SEX', 'SMOKER_IND', 'AGE', 'AGE-GROUP', 'TOTAL_SI', 
        'Death', 'TRA', 'TPD','IP',
        'SA', 'A_PREM', 'A_PREM_13', 'BEL',
        'BENEFIT_CODE', 'BEN_PERIOD', 'B_BEN_NO', 'PREMIUM_TYPE',  
        'DEFER_PER_MP', 'DII_TYPE', 'OCC_CLASS',
        'TOTAL_SI', 'OTR_ANNPHIBEN', 'DCS_REIN_SI','CHANNEL', 'BROKER', 
         'SI_LS_BAND', 'SI_IP_BAND']
result_frame = result_frame[cols]

#Save output to dataframe
fpath_res = os.path.join(RESULT, 'final')

if os.path.exists(fpath_res) == False:
     os.makedirs(fpath_res)

result_frame.to_csv(fpath_res + '\\' +'Result_'+f_run_name+'_byLine.csv')
print 'file saved to: ', fpath_res
print ('end of script')
