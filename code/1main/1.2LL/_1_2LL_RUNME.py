
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

from pathlib import Path
print('Local Code Repository: ', str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))

#%%
import sys
sys.path.append(str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))
from RUNME import * 


#%%##################################################
### PATHS ###
#####################################################

path_12LL_code_base = Path.cwd()

path_12LL_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.2LL')

path_12LL_dir_input = Path(path_12LL_files_base, '0in', str_nehv_quarter)
path_12LL_dir_mid = Path(path_12LL_files_base, '2mid', str_nehv_quarter)
path_12LL_dir_output = Path(path_12LL_files_base, '9out', str_nehv_quarter)

###########################

### Input:
### U:\Working\Tableau\Y## (<date_range>)\Y##Q# (<date_range>)\LLCHD ### oldest file.
### U:\SFTP ### Should see same file here.
# path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_231001.xlsx')
path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_240102.xlsx')

list_path_12LL_input_raw_sheets = [
    'KU_BASETABLE' # 1.
    ,'KU_CHILDERINJ' # 2.
    ,'KU_MATERNALINS' # 3.
    ,'KU_WELLCHILDVISITS' # 4.
]

### Output:
path_12LL_output = Path(path_12LL_dir_output, 'LL_before_combining_with_FW.csv')

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

### The following is run if running this file by itself interactively (& ignored when run from one of the code files):
    ### Using exec() instead of import so that code files can "see" packages, functions, & any objects created in RUNME.
if __name__ == "__main__":
    exec(open(Path(path_12LL_code_base, '_1_2LL_raw_to_master.py')).read())
    print('Executed code files')


#%%##################################################
### END ###
#####################################################

print('end file :)')



# %%

