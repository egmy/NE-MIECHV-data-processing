
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

### Create 4 folders with name of str_nehv_quarter in these locations:
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\0in
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\2mid
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\9out
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\previous\...
### ... and then copy over the associated files for the ___ locations (not this code's output).
    ### In old folders see "source....txt" files to see where to copy from.


#%%##################################################
### PACKAGES & FUNCTIONS ###
#####################################################

import sys
sys.path.append('U:\\Working\\nehv_ds_code_repository')
from packages_and_functions import * 

### from functions import *


#%%##################################################
### SETTINGS ###
#####################################################

# str_nehv_quarter = 'Y12Q1 (Oct 2022 - Dec 2023)'
# str_nehv_quarter = 'Y12Q2 (Oct 2022 - Mar 2023)'
# str_nehv_quarter = 'Y12Q3 (Oct 2022 - Jun 2023)'
str_nehv_quarter = 'Y12Q4 (Oct 2022 - Sep 2023)'

date_fy_start = pd.Timestamp("2022-10-01")

date_fy_end = pd.Timestamp("2023-09-30")

#%%########################
### Federal Poverty Guidelines
    ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines 
    ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines/prior-hhs-poverty-guidelines-federal-register-references 

### 2022: https://www.federalregister.gov/documents/2022/01/21/2022-01166/annual-update-of-the-hhs-poverty-guidelines 
if (str_nehv_quarter == 'Y12Q1 (Oct 2022 - Dec 2023)'):
    ### 8870 + (4720 * [household size])
    int_fpg_base = 8870 
    int_fpg_increment = 4720 

### 2023: https://www.federalregister.gov/documents/2023/01/19/2023-00885/annual-update-of-the-hhs-poverty-guidelines 
elif (str_nehv_quarter in ('Y12Q2 (Oct 2022 - Mar 2023)', 'Y12Q3 (Oct 2022 - Jun 2023)', 'Y12Q4 (Oct 2022 - Sep 2023)')):
    ### 9440 + (5140 * [household size])
    int_fpg_base = 9440 
    int_fpg_increment = 5140 

#%%##################################################
### PATHS ###
#####################################################

path_12LL_code_base = Path('U:\\Working\\nehv_ds_code_repository\\code\\1main\\1.2LL')

path_12LL_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.2LL')

path_12LL_dir_input = Path(path_12LL_files_base, '0in', str_nehv_quarter)
path_12LL_dir_mid = Path(path_12LL_files_base, '2mid', str_nehv_quarter)
path_12LL_dir_output = Path(path_12LL_files_base, '9out', str_nehv_quarter)

###########################

### Input:
### U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Y12Q4 (Oct 2022 - Sep 2023)\LLCHD ### oldest file.
### U:\SFTP ### Should see same file here.
path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_231001.xlsx')

list_path_12LL_input_raw_sheets = [
    'KU_BASETABLE' # 1.
    ,'KU_CHILDERINJ' # 2.
    ,'KU_MATERNALINS' # 3.
    ,'KU_WELLCHILDVISITS' # 4.
]

### Output:
path_12LL_output = Path(path_12LL_dir_output, 'file_before_combining_with_FW.csv')

###########################
### Comparison Files ###
### Files created for selected quarter by the old data sourcing process.


#%%##################################################
### END of Setup ###
#####################################################

print('end setup')


#%%##################################################
### RUN CODE FILES ###
#####################################################

if __name__ == "__main__":
    import _1_2LL_raw_to_master
    print('Imported "_1_2LL_raw_to_master"')


#%%##################################################
### END ###
#####################################################

print('end file :)')



# %%

