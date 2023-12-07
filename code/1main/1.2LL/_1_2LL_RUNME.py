
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
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\0in
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\2mid
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\9out
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\previous\...
### ... and then copy over the associated files for the ___ locations (not this code's output).
    ### In old folders see "source....txt" files to see where to copy from.


#%%##################################################
### PACKAGES & FUNCTIONS ###
#####################################################

from functions import *


#%%##################################################
### SETTINGS ###
#####################################################

# nehv_quarter = 'Y12Q1 (Oct 2022 - Dec 2023)'
# nehv_quarter = 'Y12Q2 (Oct 2022 - Mar 2023)'
# nehv_quarter = 'Y12Q3 (Oct 2022 - Jun 2023)'
nehv_quarter = 'Y12Q4 (Oct 2022 - Sep 2023)'

fy_start_date = pd.Timestamp("2022-10-01")

fy_end_date = pd.Timestamp("2023-09-30")

#%%########################
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

path_1_2LL_code_base = Path('U:\\Working\\nehv_ds_code_repository\\code\\1main\\1.2LL')

path_1_2LL_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.2LL')

path_1_2LL_dir_input = Path(path_1_2LL_files_base, '0in', nehv_quarter)
path_1_2LL_dir_mid = Path(path_1_2LL_files_base, '2mid', nehv_quarter)
path_1_2LL_dir_output = Path(path_1_2LL_files_base, '9out', nehv_quarter)

###########################

### Input:
### U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Y12Q4 (Oct 2022 - Sep 2023)\LLCHD ### oldest file.
### U:\SFTP ### Should see same file here.
path_1_2LL_input_raw = Path(path_1_2LL_dir_input, 'Flatfile_CHSR_231001.xlsx')

path_1_2LL_input_raw_sheets = [
    'KU_BASETABLE' # 1.
    ,'KU_CHILDERINJ' # 2.
    ,'KU_MATERNALINS' # 3.
    ,'KU_WELLCHILDVISITS' # 4.
]

### Output:
path_1_2LL_output = Path(path_1_2LL_dir_output, 'file_before_combining_with_FW.csv')

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

