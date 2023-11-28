
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
### ... and then copy over the associated files for the first 3 locations (not this code's output).
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
# nehv_quarter = 'Y12Q2 (Oct 2022 - Mar 2023)'
# nehv_quarter = 'Y12Q3 (Oct 2022 - Jun 2023)'
nehv_quarter = 'Y12Q4 (Oct 2022 - Sep 2023)'

###########################
### Federal Poverty Guidelines
    ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines 
    ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines/prior-hhs-poverty-guidelines-federal-register-references 

### 2022: https://www.federalregister.gov/documents/2022/01/21/2022-01166/annual-update-of-the-hhs-poverty-guidelines 
if (nehv_quarter == 'Y12Q1 (Oct 2022 - Dec 2023)'):
    ### 8870 + (4720 * [household size])
    Fpg_Base = 8870 
    Fpg_Increment = 4720 

### 2023: https://www.federalregister.gov/documents/2023/01/19/2023-00885/annual-update-of-the-hhs-poverty-guidelines 
elif (nehv_quarter in ('Y12Q2 (Oct 2022 - Mar 2023)', 'Y12Q3 (Oct 2022 - Jun 2023)', 'Y12Q4 (Oct 2022 - Sep 2023)')):
    ### 9440 + (5140 * [household size])
    Fpg_Base = 9440 
    Fpg_Increment = 5140 

#%%##################################################
### PATHS ###
#####################################################

path_code_base = Path('U:\\Working\\nehv_ds_code_repository\\code\\1main\\1.4tableau')

path_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.4tableau')

path_dir_input = Path(path_files_base, '0in', nehv_quarter)
### path_dir_mid = Path(path_files_base, '02_mid_process', nehv_quarter)
path_dir_output = Path(path_files_base, '9out', nehv_quarter)

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

path_dir_comparison_csv = Path(path_files_base, 'previous/output', nehv_quarter)

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
        print(fdf_1[fcol].value_counts(dropna=False).to_string())
        print('\n')
        print(f'DataFrame 2:\n')
        print(fdf_2[fcol].value_counts(dropna=False).to_string())

def fn_all_value_counts(fdf):
    for column in fdf.columns:
        print('Column: ', column)
        print(fdf[column].value_counts(dropna=False).to_string(), '\n')

def fn_find_unrecognized_value(fdf):
    fn_list = []
    for col_index, col in enumerate(fdf.columns):
        if (fdf[col].isin(['Unrecognized Value']).any()):
            fn_list.append({
                'col': col
                ,'col_index': col_index
                ,'col_row_indices': fdf[['Project Id','Year','Quarter', col]].query(f'`{col}` == "Unrecognized Value"').index.tolist()
                ,'col_row_ids': fdf.query(f'`{col}` == "Unrecognized Value"')['Project Id'].tolist()
                ,'col_df': fdf[['Project Id','Year','Quarter', col]].query(f'`{col}` == "Unrecognized Value"')
            })
    print(fn_list)
    return fn_list 

### Function to keep rows where comparison column values are different. 
    ### For use on ".compare" pandas DataFrames with multi-index column names broken into "self" & "other".
def fn_keep_row_differences(fdf, variable2compare):
    if pd.isna(fdf[(variable2compare, 'self')]) and pd.isna(fdf[(variable2compare, 'other')]):
        return False 
    elif pd.isna(fdf[(variable2compare, 'self')] != fdf[(variable2compare, 'other')]):
        return True 
    else:
        return fdf[(variable2compare, 'self')] != fdf[(variable2compare, 'other')] 

#%%##################################################
### END of Setup ###
#####################################################

print('end setup')


#%%##################################################
### RUN CODE FILES ###
#####################################################

# runpy.run_path(Path(path_code_base, 'ne_replacing_tableau_2_Child_Activities.py'))
# runpy.run_path(Path(path_code_base, 'ne_replacing_tableau_3_Adult_Activities_Form2DS.py'))
# runpy.run_path(Path(path_code_base, 'ne_replacing_tableau_4_Adult_Activities_Form1DS.py'))

### THIS seems to work best:

# exec(open(Path(path_code_base, 'ne_replacing_tableau_2_Child_Activities.py')).read())
# exec(open(Path(path_code_base, 'ne_replacing_tableau_3_Adult_Activities_Form2DS.py')).read())
# exec(open(Path(path_code_base, 'ne_replacing_tableau_4_Adult_Activities_Form1DS.py')).read())


#%%##################################################
### END ###
#####################################################

print('end file :)')



# %%

### TODO:
    ### Function to stop & show/flag Unrecognized Values.
    ### Read in all with desired dtypes.
    ### After testing, make all ASQ3 scores floats (document that should be).

