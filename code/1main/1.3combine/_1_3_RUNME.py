
### Purpose: 

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.


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

### TODO: Bring up all 1.3 paths to here.
### TODO: Also, keep 1.3 siloed: Only read in & out of 1.3 folders. Manually copy files as needed between silos. (We'll use other code to copy between silos.)

path_13_code_base = Path.cwd()
#U:\Working\nehv_ds_data_files\2mid\1main\1.3combine\0in\Y13Q1 (Oct 2023 - Dec 2023)
path_13_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.3combine')

path_13_dir_input = Path(path_13_files_base, '0in', str_nehv_quarter)
path_13_dir_mid = Path(path_13_files_base, '2mid', str_nehv_quarter)
path_13_dir_output = Path(path_13_files_base, '9out', str_nehv_quarter)

###########################

### Input:
### U:\Working\Tableau\Y## (<date_range>)\Y##Q# (<date_range>)\LLCHD ### oldest file.
### U:\SFTP ### Should see same file here.
# path_13_input_raw = Path(path_13_dir_input, 'Flatfile_CHSR_231001.xlsx')
path_13_input_well_child_FW = Path(path_13_dir_input, 'df_11FW_pivoted_well_child.csv')
path_13_input_well_child_LL = Path(path_13_dir_input, 'df_12LL_pivoted_WellChildVisits_4.csv')
path_13_input_child_injury_FW = Path(path_13_dir_input, 'df_11FW_pivoted_child_injury.csv')
path_13_input_child_injury_LL = Path(path_13_dir_input, 'df_12LL_pivoted_ChildERInj_2.csv')
path_13_input_cg_ins_FW = Path(path_13_dir_input, 'df_11FW_pivoted_cg_ins.csv')
path_13_input_cg_ins_LL = Path(path_13_dir_input, 'df_12LL_pivoted_MaternalIns_3.csv')
path_13_input_adult_act= Path(path_13_dir_input, 'df_11FW_adult_act.csv')
path_13_input_child_act = Path(path_13_dir_input, 'df_11FW_child_act.csv')
path_13_input_base_table = Path(path_13_dir_input, 'df_12LL_BaseTable.csv')


### Output:
path_13_output = Path(path_13_dir_output, 'LL and FW combined.csv')



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
    exec(open(Path(path_13_code_base, '_1_3_combine.py')).read())
    print('Executed code files')


#%%##################################################
### END ###
#####################################################

print('end file :)')



# %%

### TODO: Test that 1.4 works with 1.3 output.
