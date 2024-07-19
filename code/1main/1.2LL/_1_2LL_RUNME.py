
### Purpose: Read in raw Lincoln-Lancaster (LL) Excel file. Clean & prepare LL data before it is combined.

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### Every Quarter:

### 1. Create new folders for the quarter:
    ### Find the new quarter's name in the folder for the current fiscal year:
        ### Example: In year folder "U:\Working\Tableau\Y13 (Oct 2023 - Sept 2024)" there is a quarter subfolder "Y13Q2 (Oct 2023 - Mar 2024)".
    ### Copy that quarter's folder's name (for example, "Y13Q2 (Oct 2023 - Mar 2024)"), and create new folders in these locations:
        ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\0in  
        ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\9out  
        ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\LL_ID_File_base    

### 2. Update the main RUNME file's key values. See that file's instructions.

### 3. When files are sent from partners, copy input files into the input folder. See text file there for location.

### 4. Edit BELOW the input FILENAME in the line that creates "path_12LL_input_raw".

### 5. Run this RUNME.py file.


#%%##################################################
### PACKAGES & FUNCTIONS ###
#####################################################

from pathlib import Path
print('Local Code Repository: ', str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))

#%%
import os
if (os.path.basename(__file__) == '_1_2LL_RUNME.py'):
    import sys
    sys.path.append(str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))
    from RUNME import * 


#%%##################################################
### PATHS ###
#####################################################

path_12LL_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.2LL')
path_1_3_files_input = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.3combine')

path_12LL_dir_input = Path(path_12LL_files_base, '0in', str_nehv_quarter)
path_12LL_dir_mid = Path(path_12LL_files_base, '2mid', str_nehv_quarter)
path_12LL_dir_output = Path(path_12LL_files_base, '9out', str_nehv_quarter)
path_1_3_dir_input = Path(path_1_3_files_input, '0in', str_nehv_quarter)

###########################

### Input:
### U:\Working\Tableau\Y## (<date_range>)\Y##Q# (<date_range>)\LLCHD ### oldest file.
### U:\SFTP ### Should see same file here.
# path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_231001.xlsx')
# path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_240102.xlsx')
# path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_240403.xlsx')
path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_240702.xlsx')


list_path_12LL_input_raw_sheets = [
    'KU_BASETABLE' # 1.
    ,'KU_CHILDERINJ' # 2.
    ,'KU_MATERNALINS' # 3.
    ,'KU_WELLCHILDVISITS' # 4.
]

###########################

### Output:
path_12LL_output = Path(path_12LL_dir_output, 'LL_before_combining_with_FW.csv')

path_12LL_output_ID_file = Path(path_12LL_files_base, 'LL_ID_File_base', str_nehv_quarter, 'LL_ID_File_base.csv')
### Will copy to: "U:\Working\nehv_ds_data_files\2mid\2CFS\2.1forCFS\2.1.2LL\Y13Q2 (Oct 2023 - Mar 2024)"


#%%##################################################
### END of Setup ###
#####################################################

print('end setup')


#%%##################################################
### RUN CODE FILES ###
#####################################################

### The following is run if running this file by itself interactively (& ignored when run from one of the code files):
    ### Using exec() instead of import so that code files can "see" packages, functions, & any objects created in RUNME.

if (os.path.basename(__file__) in ('_1_2LL_RUNME.py', 'RUNME.py') and __name__ == "__main__"):
    exec(open(Path(path_12LL_code_base, '_1_2LL_raw_to_master.py')).read())
    print('Executed LL code files')


#%%##################################################
### END ###
#####################################################

print('end file :)')



# %%

