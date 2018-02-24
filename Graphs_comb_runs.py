#for ordinary and super part

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.ticker as ticker
import seaborn as sns
sns.set(style="whitegrid", color_codes=True)
# Matplotlib for additional customization
from matplotlib import pyplot as plt


# policy_subset = 'ordinary-cover'
policy_subset = 'super-cover'

PROJ = 'C:\galati_files\pyscripts\callo-repricing\compare-runs'
RESULT = os.path.join(PROJ, 'results', policy_subset, 'final')


flag_test = False


fpath_res = os.path.join(RESULT, 'graphs')
print fpath_res

if os.path.exists(fpath_res) == False:
     os.makedirs(fpath_res)
# # ___________________________

pkmn_type_colors = ['#78C850',  # Grass
                    '#F08030',  # Fire
                    '#6890F0',  # Water
                    '#A8B820',  # Bug
                    '#A8A878',  # Normal
                    '#A040A0',  # Poison
                    '#F8D030',  # Electric
                    '#E0C068',  # Ground
                    '#EE99AC',  # Fairy
                    '#C03028',  # Fighting
                    '#F85888',  # Psychic
                    '#B8A038',  # Rock
                    '#705898',  # Ghost
                    '#98D8D8',  # Ice
                    '#7038F8',  # Dragon
                   ]


print ('start of program')
if policy_subset == 'ordinary-cover':
    df = pd.read_csv(os.path.join(RESULT,'Combined(R02&R06)_byID.csv'), index_col = False)
elif policy_subset == 'super-cover':
    df = pd.read_csv(os.path.join(RESULT,'Combined(R02&R06)_byPol_wBROKER-sup.csv'), index_col = False) 


print df['%Ch_Tot_Prem1_toPrem13'].describe()
print df['%Ch_Prem13_to_Prem25'].describe()

# print ('largest:')
# sub_lrg = df.nlargest(3, '%Ch_Tot_Prem1_toPrem13')
# sub_lrd = sub_lrg.reset_index()
# print sub_lrg


# '%Ch_Total_Prem(R02vsR06)'
# '%Ch_Tot_Prem1_toPrem13'
# '%bin_Total_Prem(R02vsR06)'
# '%bin_Tot_Prem_toPrem13'

df = df.sort_values('%Ch_Tot_Prem1_toPrem13')
# df_over30 = df.loc[df[%bin_Tot_Prem_toPrem13].isin(['[30%; 40%)', '[40%; 50%)', '[50%; 60%)', '[> 60%)'])]


plt.figure(figsize=(15,9))
plot4 = sns.countplot(x = '%bin_Tot_Prem_toPrem13', hue ='PACKAGE', data = df, palette=pkmn_type_colors)
plt.xticks(rotation = 90)
plt.legend(bbox_to_anchor = (1,1), loc = 1)
plt.title('Count dsn of % Change, Prem1 to Prem13')
plt.show()    

plt.figure(figsize=(15,9))
plot4 = sns.countplot(x = '%bin_Prem13_to_Prem25', hue ='PACKAGE', data = df, palette=pkmn_type_colors)
plt.xticks(rotation = 90)
plt.legend(bbox_to_anchor = (1,1), loc = 1)
plt.title('Count dsn of % Change, Prem13 to Prem25')
plt.show()  
# sns.violinplot(x="Package", y = '%Ch_Total_New_Prem', hue='Sex', data = df, split=True)
# plt.xticks(rotation= -45)
# plt.show()

def count_freq(df_used, col_used, benemix_name, scale_factor):
    ncount = len(df_used)
    plt.figure(figsize=(12,8))
    ax = sns.countplot(x= col_used, data=df_used, order=['(> -30%)','[-30%;-20%)', '[-20% ;-10%)', '[-10%; 0%)',
                     '[0%; 10%)', '[10%; 20%)', '[20%; 30%)','[30%; 40%)', '[40%; 50%)', '[50%; 60%)'])
    plt.xticks(rotation=0)
    plt.title('Distribution Of Changes '+benemix_name)
    plt.xlabel('Premium Change Bands ')

    # Make twin axis
    ax2=ax.twinx()
    # Switch so count axis is on right, frequency on left
    ax2.yaxis.tick_left()
    ax.yaxis.tick_right()
    # Also switch the labels over
    ax.yaxis.set_label_position('right')
    ax2.yaxis.set_label_position('left')
    ax2.set_ylabel('frequency [%]')

    for p in ax.patches:
        x=p.get_bbox().get_points()[:,0]
        y=p.get_bbox().get_points()[1,1]
        if flag_test == True:
            print x.mean()
            print y
        ax.annotate('{:.0f}%'.format(100.*y/ncount), (x.mean(), y), 
                ha='center', va='bottom', color= 'blue') # set the alignment of the text
        # ax.annotate('(~{:.0f})'.format(1.*y), (x.mean(), y), 
        #         ha='left', va='bottom', size=10) # set the alignment of the text

    # Use a LinearLocator to ensure the correct number of ticks
    ax.yaxis.set_major_locator(ticker.LinearLocator(11))
    # Fix the frequency range to 0-50
    ax2.set_ylim(0,100*scale_factor) #freq
    ax.set_ylim(0,ncount*scale_factor) #count
    # And use a MultipleLocator to ensure a tick spacing of 10
    ax2.yaxis.set_major_locator(ticker.MultipleLocator(10))
    # Need to turn the grid on ax2 off, otherwise the gridlines end up on top of the bars
    ax2.grid(None)
    
    if col_used == '%bin_Tot_Prem_toPrem13':
        plt.savefig(fpath_res + '\\' + 'Figure_Prem1_toPrem13)' + benemix_name +'.png') 
    
    elif col_used == '%bin_Prem13_to_Prem25':
        plt.savefig(fpath_res + '\\' + 'Figure_Prem13_toPrem25' + benemix_name +'.png') 

    return(0) #plt.show()



if policy_subset == 'ordinary-cover':
    print policy_subset, ' used'
    col_used = '%bin_Tot_Prem_toPrem13'
    count_freq(df, col_used, '(all Mix benefits)', 0.5)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE   '], col_used, '(Life benefit)', 1)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TRA  '], col_used, '(Life and Trauma benefit)', 0.7)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TRA TPD IP'], col_used, '(Life & Trauma & TPD & IP benefit)', 0.7)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE   IP'], col_used, '(Life & IP benefit)', 0.5)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TRA TPD '], col_used, '(Life & Trauma & TPD benefit)', 0.7)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TRA  IP'], col_used, '(Life & Trauma & IP benefit)', 0.5)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE  TPD IP'], col_used, '(Life & TPD & IP benefit)', 0.5)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE  TPD '], col_used, '(Life & TPD benefit)', 1)
    count_freq(df.loc[df['PACKAGE'] == u'   IP'], col_used, '(IP benefit)', 0.5)
    count_freq(df.loc[df['PACKAGE'] == u' TRA  IP'], col_used, '(Trauma & IP benefit)', 0.5)
    count_freq(df.loc[df['PACKAGE'] == u' TRA TPD IP'], col_used, '(Trauma & TPD & IP benefit)', 0.5)
    count_freq(df.loc[df['PACKAGE'] == u'  TPD IP'], col_used, '(TPD & IP benefit)', 0.5)
    count_freq(df.loc[df['PACKAGE'] == u' TRA TPD '], col_used, '(Trauma & TPD benefit)', 0.5)
    count_freq(df.loc[df['PACKAGE'] == u'  TPD '], col_used, '(TPD benefit)', 1)
    count_freq(df.loc[df['PACKAGE'] == u' TRA  '], col_used, '(Trauma benefit)', 0.7)


    col_used = '%bin_Prem13_to_Prem25'
    count_freq(df, col_used, '(all Mix benefits)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE   '], col_used, '(Life benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TRA  '], col_used, '(Life and Trauma benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TRA TPD IP'], col_used, '(Life & Trauma & TPD & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE   IP'], col_used, '(Life & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TRA TPD '], col_used, '(Life & Trauma & TPD benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TRA  IP'], col_used, '(Life & Trauma & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE  TPD IP'], col_used, '(Life & TPD & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE  TPD '], col_used, '(Life & TPD benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'   IP'], col_used, '(IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u' TRA  IP'], col_used, '(Trauma & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u' TRA TPD IP'], col_used, '(Trauma & TPD & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'  TPD IP'], col_used, '(TPD & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u' TRA TPD '], col_used, '(Trauma & TPD benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'  TPD '], col_used, '(TPD benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u' TRA  '], col_used, '(Trauma benefit)', 0.6)



elif policy_subset == 'super-cover':

    col_used = '%bin_Tot_Prem_toPrem13'
    count_freq(df, col_used, '(all Mix benefits)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE  '], col_used, '(Life benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TPD '], col_used, '(Life and TPD benefit)', 0.65)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TPD IP'], col_used, '(Life & TPD & IP benefit)', 0.65)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE  IP'], col_used, '(Life & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'  IP'], col_used, '(IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u' TPD IP'], col_used, '(TPD & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u' TPD '], col_used, '(TPD benefit)', 0.6)

    col_used = '%bin_Prem13_to_Prem25'
    count_freq(df, col_used, '(all Mix benefits)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE  '], col_used,    '(Life benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TPD '], col_used, '(Life and TPD benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE TPD IP'], col_used,'(Life & TPD & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'LIFE  IP'], col_used,  '(Life & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u'  IP'], col_used,      '(IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u' TPD IP'], col_used,   '(TPD & IP benefit)', 0.6)
    count_freq(df.loc[df['PACKAGE'] == u' TPD '], col_used,     '(TPD benefit)', 0.6)

print df.loc[:,'PACKAGE'].unique()
