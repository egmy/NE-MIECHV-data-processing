
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
import os
if (os.path.basename(__file__) == '_1_1_2FW_RUNME.py'):
    import sys
    sys.path.append(str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))
    from RUNME import * 


#%%##################################################
### PATHS ###
#####################################################

path_112FW_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other')

path_112FW_dir_input = Path(path_112FW_files_base, '0in', str_nehv_quarter)
path_112FW_dir_mid = Path(path_112FW_files_base, '2mid', str_nehv_quarter)
path_112FW_dir_output = Path(path_112FW_files_base, '9out', str_nehv_quarter)

###########################

### TODO Y13Q2: ASKJOE: Are there 2 new files for input? (so total 10?)

### Input:
### U:\Working\Tableau\Y## (<date_range>)\Y##Q# (<date_range>)\LLCHD ### oldest file.
### U:\SFTP ### Should see same file here.
# path_112FW_input_raw = Path(path_112FW_dir_input, 'Flatfile_CHSR_231001.xlsx')
path_112FW_input_well_child = Path(path_112FW_dir_input, '04 Well Child v2 no MAX - use this one.xlsx')
path_112FW_input_child_injury = Path(path_112FW_dir_input, '08 Child ER Injury.xlsx')
path_112FW_input_cg_ins = Path(path_112FW_dir_input, '16 - Caregiver Insurance v2 - USE THIS ONE.xlsx')
path_112FW_input_adult_act = Path(path_112FW_dir_input, 'Adult Activities Query.xlsx')
path_112FW_input_adult_uncope = Path(path_112FW_dir_input, 'Adult UNCOPE Query.xlsx')
path_112FW_input_child_act = Path(path_112FW_dir_input, 'Child Activities Query.xlsx')
path_112FW_input_home_visit = Path(path_112FW_dir_input, 'F1 - Home Visit Type Query.xlsx')
path_112FW_input_ref_excl = Path(path_112FW_dir_input, 'Referral Exclusions 1 thru 6.xlsx')
path_112FW_input_frog = Path(path_112FW_dir_input, '14 IPV Screen FROG.xlsx')


### Output:
path_112FW_output = Path(path_112FW_dir_output, 'LL_before_combining_with_FW.csv')


#%%##################################################
### END of Setup ###
#####################################################

print('end setup')


#%%##################################################
### RUN CODE FILES ###
#####################################################

### The following is run if running this file by itself interactively (& ignored when run from one of the code files):
    ### Using exec() instead of import so that code files can "see" packages, functions, & any objects created in RUNME.

if (os.path.basename(__file__) in ('_1_1_2FW_RUNME.py', 'RUNME.py') and __name__ == "__main__"):
    exec(open(Path(path_112FW_code_base, '_1_1_2FW_access_to_master.py')).read())
    print('Executed code files')


#%%##################################################
### END ###
#####################################################

print('end file :)')



# %%

### TODO: Update 1.1.2 for Q2, AND make sure <Q1 still works.

