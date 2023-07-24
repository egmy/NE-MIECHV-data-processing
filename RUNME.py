
### Purpose: 

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.

#%%##################################################
### PREPARATION ###
#####################################################

### In SETTINGS below:
    ### Change Quarter variable.

### Create 4 folders with name of nehv_quarter in these locations:
    ### U:\Working\nebraska_miechv_coded_data_source\data\01_input 
    ### U:\Working\nebraska_miechv_coded_data_source\previous\previous_output 
    ### U:\Working\nebraska_miechv_coded_data_source\previous\previous_tableau 
    ### U:\Working\nebraska_miechv_coded_data_source\data\03_output 
### ... and then copy over the associated files for the first 3 locations (no this code's output).
    ### In old folders see "source....txt" files to see where to copy from.

#%%##################################################
### PACKAGES ###
#####################################################

import pandas as pd
from pathlib import Path
import numpy as np
import sys
import runpy
import IPython
import collections
import re

print('Version Of Python: ' + sys.version)
print('Version Of Pandas: ' + pd.__version__)
print('Version Of Numpy: ' + np.version.version)

#%%##################################################
### SETTINGS ###
#####################################################

# nehv_quarter = 'Y12Q1 (Oct 2022 - Dec 2023)'
nehv_quarter = 'Y12Q2 (Oct 2022 - Mar 2023)'

### From #3:
    ### Current Federal Poverty something.....
    ### appropriate_var = ''


#%%##################################################
### PATHS ###
#####################################################

path_dir_input = Path('U:\\Working\\nebraska_miechv_coded_data_source\\data\\01_input', nehv_quarter)
### path_dir_mid = Path('U:\\Working\\nebraska_miechv_coded_data_source\\data\\02_mid_process', nehv_quarter)
path_dir_output = Path('U:\\Working\\nebraska_miechv_coded_data_source\\data\\03_output', nehv_quarter)

###########################
### Data Source for 2nd Tableau file:
### path_2_data_source_file = 'U:\\Working\\Tableau\\Y12 (Oct 2022 - Sept 2023)\\Child Activity Master File.xlsx' ### old.
### local:
path_2_data_source_file = Path(path_dir_input, 'Child Activity Master File.xlsx')

path_2_data_source_sheets = [
    'Project ID' # 1.
    ,'ER Injury' # 2.
    ,'Family Wise' # 3.
    ,'LLCHD' # 4.
    ,'Well Child' # 5.
]

### Output for 2nd Tableau file:
path_2_output = Path(path_dir_output, 'Child Activity Master File from Excel on NE Server.csv')

###########################
### Data Source for 3rd Tableau file, 1st Data Source (for Form 2):
### DS: "Adult Activity Master File from Excel on NE Server".
### path_3_data_source = 'U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Adult Activity Master File Y12.xlsx'
### local:
path_3_data_source_file = Path(path_dir_input, 'Adult Activity Master File.xlsx')

path_3_data_source_sheets = [
    'Project ID' # 1.
    ,'Caregiver Insurance' # 2.
    ,'Family Wise' # 3.
    ,'LLCHD' # 4.
]

### Output for 3rd Tableau file:
path_3_output = Path(path_dir_output, 'Adult Activity Master File from Excel on NE Server.csv')

###########################
### Data Source for 4th Tableau file, 2nd Data Source (for Form 1):
### DS: "Adult Activity Master File for Form 1 from Excel on NE Server".
### path_4_data_source = 'U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Adult Activity Master File Y12.xlsx'
### Actually used for Q1:
### path_4_data_source = 'U:\\Working\\Tableau\\Y12 (Oct 2022 - Sept 2023)\\Y12Q1 (Oct 2022 - Dec 2023)\\Tableau Queries\\TESTING\WORKS\\Master Files\\Adult Activity Master File - Deleting Rows.xlsx'
### local:
path_4_data_source_file = Path(path_dir_input, 'Adult Activity Master File.xlsx')

path_4_data_source_sheets = [
    'Project ID' # 1.
    ,'Caregiver Insurance' # 2.
    ,'Family Wise' # 3.
    ,'LLCHD' # 4.
    ,'MOB or FOB' # 5.
]

### Output for 4th Tableau file:
path_4_output = Path(path_dir_output, 'Adult Activity Master File for Form 1 from Excel on NE Server.csv')


###########################
### Comparison Files ###
### Files created for selected quarter by the old data sourcing process with Tableau.

path_dir_comparison_csv = Path('U:\\Working\\nebraska_miechv_coded_data_source\\previous\\previous_output', nehv_quarter)

path_2_comparison_csv = Path(path_dir_comparison_csv, 'Child Activity Master File from Excel on NE Server.csv')
path_3_comparison_csv = Path(path_dir_comparison_csv, 'Adult Activity Master File from Excel on NE Server.csv')
path_4_comparison_csv = Path(path_dir_comparison_csv, 'Adult Activity Master File for Form 1 from Excel on NE Server.csv')


#%%##################################################
### Utility Functions ###
#####################################################

def inspect_df (df):
    print(df.describe(include='all'))
    print('\n')
    print(df.dtypes.to_string())
    print('\n')
    print(df.info(verbose=True, show_counts=True))
    print('\n')
    print(f'Rows: {len(df)}')
    print(f'Columns: {len(df.columns)}')
    print('\n')
    IPython.display.display(df)

### fSeries = df column or Series: e.g., df['colname'].
def inspect_col(fSeries):
    print(fSeries.info())
    print('\n')
    print('value_counts:')
    print(fSeries.value_counts(dropna=False).to_string())
    print('\n')
    print(fSeries)

def compare_col(fdf_1, fdf_2, fcol, info_or_value_counts='info'): ### or 'value_counts'.
    if info_or_value_counts=='info':
        print(f'DataFrame 1:\n')
        print(fdf_1[fcol].info())
        print('\n')
        print(f'DataFrame 2:\n')
        print(fdf_2[fcol].info())
    elif info_or_value_counts=='value_counts':
        print(f'DataFrame 1:\n')
        print(fdf_1[fcol].value_counts(dropna=False))
        print('\n')
        print(f'DataFrame 2:\n')
        print(fdf_2[fcol].value_counts(dropna=False))

def fn_all_value_counts(fdf):
    for column in fdf.columns:
        print('Column: ', column)
        print(fdf[column].value_counts(dropna=False).to_string(), '\n')


#%%##################################################
### END of Setup ###
#####################################################

print('end setup')


#%%##################################################
### RUN CODE FILES ###
#####################################################

# runpy.run_path('ne_replacing_tableau_1_CPS_ID_File.py')
# runpy.run_path('ne_replacing_tableau_2_Child_Activities.py')
# runpy.run_path('ne_replacing_tableau_3_Adult_Activities_Form2DS.py')
# runpy.run_path('ne_replacing_tableau_4_Adult_Activities_Form1DS.py')
# runpy.run_path('ne_replacing_tableau_5_Child_CPS_Agg_Data.py')

# exec(open('ne_replacing_tableau_1_CPS_ID_File.py').read())
# exec(open('ne_replacing_tableau_2_Child_Activities.py').read())
# exec(open('ne_replacing_tableau_3_Adult_Activities_Form2DS.py').read())
# exec(open('ne_replacing_tableau_4_Adult_Activities_Form1DS.py').read())
# exec(open('ne_replacing_tableau_5_Child_CPS_Agg_Data.py').read())


#%%##################################################
### END ###
#####################################################

print('end file :)')



# %%

### TODO:
    ### Function to stop & show/flag Unrecognized Values.
    ### Read in all with desired dtypes.
    ### After testing, make all ASQ3 scores floats (document that should be).

