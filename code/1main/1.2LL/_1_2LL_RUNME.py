



### Process for receiving data from Lincoln Lancaster County Health Department (LL):
    # 1. Original files transferred from external vendor (LL) to NE State, who then puts the files in folder "U:\SFTP".
    # 2. Then we organize the files in folder "Master (Files Used For Quarterly Reports)".
    # 3. Then we copy the files into: "U:\Working" >> "Tableau" >> year >> quarter >> "LL" folder (for example: "U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Y12Q4 (Oct 2022 - Sep 2023)\LLCHD").


### Copying over file from: "U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Y12Q4 (Oct 2022 - Sep 2023)\LLCHD":

### To here: "U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\0in\Y12Q4 (Oct 2022 - Sep 2023)"



### Purpose: In the Nebraska MIECHV data sourcing process, ...

#%%##############################################!>>>
### >>>  INSTRUCTIONS 
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.

### TODO: Add code that stops the code & flags when partners send us new columns that need to be added to our lists/dictionaries.

#%%##############################################!>>> 
### >>>  SETUP 
#####################################################

import os 

#%%
#print('File that is running: ', os.path.basename(__file__))

from pathlib import Path
import sys
import shutil
print('Local Code Repository: ', str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))
sys.path.append(str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))
from RUNME import *

#%%
### The following is run if running this file by itself interactively (& ignored when run from RUNME):
if (os.path.basename(__file__) == '_1_2LL_RUNME.py'):
    print('running 1.2LL')
    import sys
    sys.path.append(str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))
    from RUNME import *
else:
    print("Did NOT run RUNME again... because it's already running!")


path_12LL_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.2LL')
path_1_3_files_input = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.3combine')

path_12LL_dir_input = Path(path_12LL_files_base, '0in', str_nehv_quarter)
path_12LL_dir_mid = Path(path_12LL_files_base, '2mid', str_nehv_quarter)
path_12LL_dir_output = Path(path_12LL_files_base, '9out', str_nehv_quarter)
path_1_3_dir_input = Path(path_1_3_files_input, '0in', str_nehv_quarter)

for path in [path_12LL_dir_input, path_12LL_dir_mid, path_12LL_dir_output, path_1_3_dir_input]:
    path.mkdir(parents=True, exist_ok=True)

###########################

### Input:
### U:\Working\Tableau\Y## (<date_range>)\Y##Q# (<date_range>)\LLCHD ### oldest file.
### U:\SFTP ### Should see same file here.
# path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_231001.xlsx')
# path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_240102.xlsx')
# path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_240403.xlsx')
# path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_240702.xlsx')
# path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_241003.xlsx')
path_12LL_input_raw = Path(path_12LL_dir_input, 'Flatfile_CHSR_2509.xlsx')

path_12LL_input_original = Path(f'U:\Working\Tableau\{str_nehv_year}\{str_nehv_quarter}\LLCHD')

# Destination directory
destination_dir_ll = path_12LL_dir_input

# Find the only .xlsx file in that folder (likely Flatfile_CHSR_*.xlsx)
flatfiles = list(path_12LL_input_original.glob('Flatfile_CHSR_*.xlsx'))

if len(flatfiles) == 1:
    src_file = flatfiles[0]
    dst_file = destination_dir_ll / 'Flatfile_CHSR_2509.xlsx'  # You can rename if needed
    shutil.copy2(src_file, dst_file)
    print(f"Copied: {src_file.name} → {dst_file.name}")
elif len(flatfiles) == 0:
    print(f"⚠️ No matching flatfile found in: {path_12LL_input_original}")
else:
    print(f"⚠️ Multiple matching flatfiles found in: {path_12LL_input_original}")
    for f in flatfiles:
        print(f" - {f.name}")

list_path_12LL_input_raw_sheets = [
    'KU_BASETABLE' # 1.
    ,'KU_CHILDERINJ' # 2.
    ,'KU_MATERNALINS' # 3.
    ,'KU_WELLCHILDVISITS' # 4.
]

###########################

### Output:

path_12LL_output_ID_file = Path(path_12LL_files_base, 'LL_ID_File_base', str_nehv_quarter, 'LL_ID_File_base.xlsx')
path_12LL_output_ID_file.parent.mkdir(parents=True, exist_ok=True)

#%%##############################################!>>>
### >>> COLUMN DEFINITIONS 
#####################################################

#%%### df_12LL_1: 'KU_BASETABLE'.
#%%### df_12LL_2: 'KU_CHILDERINJ'.
#%%### df_12LL_3: 'KU_MATERNALINS'.
#%%### df_12LL_4: 'KU_WELLCHILDVISITS'.

#######################

### List = [name, dtype]

#######################
#%%### df_12LL_1: 'KU_BASETABLE'.
list_12LL_col_detail_1 = [
    ['site_id', 'string']
    ,['worker_id', 'string']
    ,['family_id', 'string']
    ,['CaseProgramID', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['case_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['tgt_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['tgt_identifier', 'string']
    ,['tgt_dob', 'datetime64[ns]']
    ,['tgt_first_name', 'string']
    ,['tgt_last_name', 'string']
    ,['tgt_ssn', 'string']
    ,['tgt_gender', 'string']
    ,['tgt_ethnicity', 'string']
    ,['tgt_race_indian', 'string']
    ,['tgt_race_asian', 'string']
    ,['tgt_race_black', 'string']
    ,['tgt_race_hawaiian', 'string']
    ,['tgt_race_white', 'string']
    ,['tgt_race_other', 'string']
    ,['tgt_GestationalAge', 'Int64']
    ,['tgt_medical_home', 'Int64']
    ,['tgt_medical_home_dt', 'datetime64[ns]']
    ,['tgt_dental_home', 'Int64']
    ,['tgt_dental_home_dt', 'datetime64[ns]']
    ,['dt_edc', 'datetime64[ns]']
    ,['enroll_dt', 'datetime64[ns]']
    ,['enroll_preg_status', 'datetime64[ns]']
    ,['current_pregnancy', 'string']
    ,['discharge_reason', 'string']
    ,['discharge_dt', 'datetime64[ns]']
    ,['last_home_visit', 'datetime64[ns]']
    ,['home_visits_num', 'Int64']
    ,['home_visits_pre', 'Int64']
    ,['home_visits_post', 'Int64']
    ,['home_visits_person', 'Int64']
    ,['home_visits_virtual', 'Int64']
    ,['funding', 'string']
    ,['c_fundingdate', 'datetime64[ns]']
    ,['p_funding', 'string']
    ,['p_fundingdate', 'datetime64[ns]']
    ,['primary_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['primary_relation', 'string']
    ,['mob_id', 'string']
    ,['mob_first_name', 'string']
    ,['mob_last_name', 'string']
    ,['mob_ssn', 'string']
    ,['mob_dob', 'datetime64[ns]']
    ,['mob_gender', 'string']
    ,['mob_ethnicity', 'string']
    ,['mob_race', 'string']
    ,['mob_race_indian', 'string']
    ,['mob_race_asian', 'string']
    ,['mob_race_black', 'string']
    ,['mob_race_hawaiian', 'string']
    ,['mob_race_white', 'string']
    ,['mob_race_other', 'string']
    ,['mob_marital_status', 'string']
    ,['mob_living_arrangement', 'Int64']
    ,['mob_living_arrangement_dt', 'datetime64[ns]']
    ,['fob_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['fob_first_name', 'string']
    ,['fob_last_name', 'string']
    ,['fob_dob', 'datetime64[ns]']
    ,['fob_ssn', 'string']
    ,['fob_gender', 'string']
    ,['fob_ethnicity', 'string']
    ,['fob_race', 'string']
    ,['fob_race_indian', 'string']
    ,['fob_race_asian', 'string']
    ,['fob_race_black', 'string']
    ,['fob_race_hawaiian', 'string']
    ,['fob_race_white', 'string']
    ,['fob_race_other', 'string']
    ,['fob_marital_status', 'string']
    ,['fob_living_arrangement', 'Int64']
    ,['fob_living_arrangement_dt', 'datetime64[ns]']
    ,['fob_edu_dt', 'datetime64[ns]']
    ,['fob_edu', 'string']
    ,['fob_employ_dt', 'datetime64[ns]']
    ,['fob_employ', 'Int64']
    ,['fob_involved', 'string']
    ,['fob_visits', 'Int64']
    ,['address', 'string']
    ,['city', 'string']
    ,['zip', 'string']
    ,['household_income', 'Int64']
    ,['household_size', 'Int64']
    ,['mcafss_income_dt', 'datetime64[ns]']
    ,['mcafss_income', 'Int64']
    ,['mcafss_edu_dt1', 'datetime64[ns]']
    ,['mcafss_edu1', 'Int64']
    ,['mcafss_edu1_enroll', 'string']
    ,['mcafss_edu1_enroll_dt', 'datetime64[ns]']
    ,['mcafss_edu1_prog', 'Int64']
    ,['mcafss_edu_dt2', 'datetime64[ns]']
    ,['mcafss_edu2', 'Int64']
    ,['mcafss_edu2_enroll', 'string']
    ,['mcafss_edu2_enroll_dt', 'datetime64[ns]']
    ,['mcafss_edu2_prog', 'Int64']
    ,['mcafss_employ_dt', 'datetime64[ns]']
    ,['mcafss_employ', 'Int64']
    ,['language_primary', 'string']
    ,['priority_child_welfare', 'string']
    ,['priority_substance_abuse', 'string']
    ,['priority_tobacco_use', 'string']
    ,['priority_low_student', 'string']
    ,['priority_develop_delays', 'string']
    ,['priority_military', 'string']
    ,['uncope_dt', 'datetime64[ns]']
    ,['uncope_score', 'Int64']
    ,['substance_abuse_ref_dt', 'datetime64[ns]']
    ,['tobacco_use', 'string']
    ,['tobacco_use_dt', 'datetime64[ns]']
    ,['tobacco_ref_dt', 'datetime64[ns]']
    ,['safe_sleep_yes', 'string']
    ,['safe_sleep_yes_dt', 'datetime64[ns]']
    ,['cci_dt', 'datetime64[ns]']
    ,['early_language', 'string']
    ,['early_language_dt', 'datetime64[ns]']
    ,['asq3_dt_9mm', 'datetime64[ns]']
    ,['asq3_timing_9mm', 'Int64'] ### Maybe string.
    ,['asq3_comm_9mm', 'Float64']
    ,['asq3_gross_9mm', 'Float64']
    ,['asq3_fine_9mm', 'Float64']
    ,['asq3_problem_9mm', 'Float64']
    ,['asq3_social_9mm', 'Float64']
    ,['asq3_feedback_9mm', 'string']
    ,['asq3_referral_9mm', 'datetime64[ns]']
    ,['asq3_dt_18mm', 'datetime64[ns]']
    ,['asq3_timing_18mm', 'Int64'] ### Maybe string.
    ,['asq3_comm_18mm', 'Float64']
    ,['asq3_gross_18mm', 'Float64']
    ,['asq3_fine_18mm', 'Float64']
    ,['asq3_problem_18mm', 'Float64']
    ,['asq3_social_18mm', 'Float64']
    ,['asq3_feedback_18mm', 'string']
    ,['asq3_referral_18mm', 'datetime64[ns]']
    ,['asq3_dt_24mm', 'datetime64[ns]']
    ,['asq3_timing_24mm', 'Int64'] ### Maybe string.
    ,['asq3_comm_24mm', 'Float64']
    ,['asq3_gross_24mm', 'Float64']
    ,['asq3_fine_24mm', 'Float64']
    ,['asq3_problem_24mm', 'Float64']
    ,['asq3_social_24mm', 'Float64']
    ,['asq3_feedback_24mm', 'string']
    ,['asq3_referral_24mm', 'datetime64[ns]']
    ,['asq3_dt_30mm', 'datetime64[ns]']
    ,['asq3_timing_30mm', 'Int64'] ### Maybe string.
    ,['asq3_comm_30mm', 'Float64']
    ,['asq3_gross_30mm', 'Float64']
    ,['asq3_fine_30mm', 'Float64']
    ,['asq3_problem_30mm', 'Float64']
    ,['asq3_social_30mm', 'Float64']
    ,['asq3_feedback_30mm', 'string']
    ,['asq3_referral_30mm', 'datetime64[ns]']
    ,['behavioral_concerns', 'Int64']
    ,['ipv_screen', 'string']
    ,['ipv_screen_dt', 'datetime64[ns]']
    ,['ipv_referral_dt', 'datetime64[ns]']
    ,['prim_care_dt', 'datetime64[ns]']
    ,['cesd_dt', 'datetime64[ns]']
    ,['cesd_score', 'Int64']
    ,['ment_hlth_ref_dt', 'datetime64[ns]']
    ,['lsp_bf_initiation_dt', 'datetime64[ns]']
    ,['lsp_bf_discon_dt', 'datetime64[ns]']
    ,['hlth_insure_mob', 'Int64']
    ,['hlth_insure_mob_dt', 'datetime64[ns]']
    ,['hlth_insure_tgt', 'Int64']
    ,['hlth_insure_tgt_dt', 'datetime64[ns]']
    ,['last_well_child_visit', 'datetime64[ns]']
    ,['hlth_insure_fob', 'Int64'] ### All blank, but following pattern of mob/tgt.
    ,['hlth_insure_fob_dt', 'datetime64[ns]']
    ,['need_exclusion1', 'string']
    ,['need_exclusion2', 'string']
    ,['need_exclusion3', 'string']
    ,['need_exclusion4', 'string']
    ,['need_exclusion5', 'string']
    ,['need_exclusion6', 'string']
    ,['Has_ChildWelfareAdaptation', 'string']
]
#%%### df_12LL_1: 'KU_BASETABLE'.
dict_12LL_col_dtypes_1 = {x[0]:x[1] for x in list_12LL_col_detail_1}
# print(dict_12LL_col_dtypes_1)
# print(collections.Counter(list(dict_12LL_col_dtypes_1.values())))
### List of date columns:
list_12LL_date_cols_1 = [key for key, value in dict_12LL_col_dtypes_1.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_12LL_2: 'KU_CHILDERINJ'.
list_12LL_col_detail_2 = [
    ['family_id', 'string']
    ,['case_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['tgt_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['funding', 'string']
    ,['reason', 'string']
    ,['date', 'datetime64[ns]']
]
#%%### df_12LL_2: 'KU_CHILDERINJ'.
dict_12LL_col_dtypes_2 = {x[0]:x[1] for x in list_12LL_col_detail_2}
# print(dict_12LL_col_dtypes_2)
# print(collections.Counter(list(dict_12LL_col_dtypes_2.values())))
### List of date columns:
list_12LL_date_cols_2 = [key for key, value in dict_12LL_col_dtypes_2.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_12LL_3: 'KU_MATERNALINS'.
list_12LL_col_detail_3 = [
    ['family_id', 'string']
    ,['case_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['tgt_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['funding', 'string']
    ,['insurance', 'string'] ### Could be 'Int64'; however, left as string because will union with FW string vars. Will be UN-coded in 1.4 code.
    ,['date', 'datetime64[ns]']
]
#%%### df_12LL_3: 'KU_MATERNALINS'.
dict_12LL_col_dtypes_3 = {x[0]:x[1] for x in list_12LL_col_detail_3}
# print(dict_12LL_col_dtypes_3)
# print(collections.Counter(list(dict_12LL_col_dtypes_3.values())))
### List of date columns:
list_12LL_date_cols_3 = [key for key, value in dict_12LL_col_dtypes_3.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_12LL_4: 'KU_WELLCHILDVISITS'.
list_12LL_col_detail_4 = [
    ['family_id', 'string']
    ,['case_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['tgt_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['funding', 'string']
    ,['date', 'datetime64[ns]']
]
#%%### df_12LL_4: 'KU_WELLCHILDVISITS'.
dict_12LL_col_dtypes_4 = {x[0]:x[1] for x in list_12LL_col_detail_4}
# print(dict_12LL_col_dtypes_4)
# print(collections.Counter(list(dict_12LL_col_dtypes_4.values())))
### List of date columns:
list_12LL_date_cols_4 = [key for key, value in dict_12LL_col_dtypes_4.items() if value == 'datetime64[ns]'] 



#%%##############################################!>>>
### >>> READ 
#####################################################

#%%
### Performance benefit for reading in file to memory only once by creating an ExcelFile class object.
xlsx_12LL = pd.ExcelFile(path_12LL_input_raw)

#%% 
### CHECK that all list_path_12LL_input_raw_sheets same as xlsx.sheet_names:
# print(sorted(list_path_12LL_input_raw_sheets))
# print(sorted(xlsx_12LL.sheet_names))
if (sorted(list_path_12LL_input_raw_sheets) == sorted(xlsx_12LL.sheet_names)):
    print('Passed Check that all Excel sheet names as expected.')
else:
    raise Exception('**Check Failed: Unexpected Excel sheet names.')

print('____________\n')

#%%###################################
### READ in all sheets as strings.

### Read in EVERYTHING as a string WITH pd.NA for empty cells:
df_12LL_allstring_1 = pd.read_excel(xlsx_12LL, sheet_name=list_path_12LL_input_raw_sheets[0], keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_12LL_col_dtypes_1)
df_12LL_allstring_2 = pd.read_excel(xlsx_12LL, sheet_name=list_path_12LL_input_raw_sheets[1], keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_12LL_col_dtypes_2)
df_12LL_allstring_3 = pd.read_excel(xlsx_12LL, sheet_name=list_path_12LL_input_raw_sheets[2], keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_12LL_col_dtypes_3)
df_12LL_allstring_4 = pd.read_excel(xlsx_12LL, sheet_name=list_path_12LL_input_raw_sheets[3], keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_12LL_col_dtypes_4)

# ### Backup:
# df_12LL_1 = df_12LL_allstring_1.copy()
# df_12LL_2 = df_12LL_allstring_2.copy()
# df_12LL_3 = df_12LL_allstring_3.copy()
# df_12LL_4 = df_12LL_allstring_4.copy()

# #%% 
# ### Test if all read in as strings:
# print(df_12LL_allstring_1.dtypes.to_string())
# print(df_12LL_allstring_2.dtypes.to_string())
# print(df_12LL_allstring_3.dtypes.to_string())
# print(df_12LL_allstring_4.dtypes.to_string())



#%%##############################################!>>>
### >>> CLEAN 
#####################################################



#%%###################################
### >>> df_12LL_1: 'KU_BASETABLE'.

#%%###################################
### <> Before & After 
### df_12LL_1: 'KU_BASETABLE'.
#%%###################################
### <> Initial copy
df_12LL_BaseTable = df_12LL_allstring_1.copy()

#%%###################################
### <> 1. Strip surrounding whitespace
print('Step 1: Strip surrounding whitespace')
df_12LL_BaseTable = df_12LL_BaseTable.map(lambda cell: cell.strip() if isinstance(cell, str) else cell).astype('string')

#%%###################################
### <> 2. Find & replace "null" values

#%%
### 
print('Step 2: Find & replace "null" values')
list_12LL_values_to_find_and_replace = ['null'] 
df_12LL_BaseTable = (
    df_12LL_BaseTable
    .pipe(fn_find_and_replace_value_in_df, one_id_var='family_id', list_of_values_to_find=list_12LL_values_to_find_and_replace, replacement_value=pd.NA)
)
### Note: ### TODO: At the moment, searching is case-insensitive. Could make option for case sensitive.
### Note: ### TODO: At the moment, the entire cell must match. Could make an option for matching with substrings.


######################################
#%%###################################
### <> 3. Add nanoseconds to datetime strings (missing them)
print('Step 3: Add nanoseconds to datetime strings')
regex_12LL_dates_to_fix = r'(^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$)' 
regex_12LL_dates_replacement = r'\1.000000000'
df_12LL_BaseTable = df_12LL_BaseTable.replace(
    {col: regex_12LL_dates_to_fix for col in list_12LL_date_cols_1},
    regex_12LL_dates_replacement,
    regex=True
)

#%%###################################
### <> 4. Set data types
print('Step 4: Set data types')
df_12LL_BaseTable = df_12LL_BaseTable.pipe(fn_apply_dtypes, dict_12LL_col_dtypes_1)

#%%###################################
### <> 5. Set site_id to "ll"
print('Step 5: Set site_id to "ll"')
df_12LL_BaseTable['site_id'] = 'll'
df_12LL_BaseTable['site_id'] = df_12LL_BaseTable['site_id'].astype('string')


######################################
#%%###################################
### <> 6. Column tgt_id fill NA with "0" 
#%%
print('Step 6: Fill NA in tgt_id with "0"')
df_12LL_BaseTable['tgt_id'] = df_12LL_BaseTable['tgt_id'].fillna('0').astype('string')
print('Filled NA in tgt_id with "0".\n')

######################################
#%%###################################
### <> 7. Create project_id column 
#%%
### 
print('Step 7: Create project_id column') 
df_12LL_BaseTable = (
    df_12LL_BaseTable
    .assign(project_id = lambda df: (df['site_id'] + df['family_id'] + '-' + df['tgt_id']).astype('string'))
)

######################################
#%%###################################
### <> 8. Filter out families that discharged before the current reporting year (using "discharge_dt"). 

### Note: Filtering rows (parent-child combinations), not really families.
#%%
### 2. Make change:
print(f'Rows: {len(df_12LL_BaseTable)}')
print('Step 8: Filter "discharge_dt" to remove families discharged before current reporting year') 
df_12LL_BaseTable = (
    df_12LL_BaseTable
    ### Keep both later dates AND where NO discharge date:
    .query('discharge_dt >= @date_fy_start or discharge_dt.isna()')
)

######################################
#%%###################################
### <> 9. Filter out families without a home visit in the current fiscal year (using "last_home_visit"). 

### Note: Filtering rows (parent-child combinations), not really families. (Joe approves!)
### TODO: Check later (in 1.4 or Report) (maybe "active child") where DOB are checked to filter out "subsequent children".

print('Step 9: Filter out families without a home visit in fiscal year')
df_12LL_BaseTable = df_12LL_BaseTable.query('last_home_visit >= @date_fy_start and last_home_visit < @date_fy_end_day_after')
print('Filtered rows by last_home_visit.\n')

### Note: Keeping the code below as an example of the before and after concept created by Nathan.
# #%%
# ### 1. Test that DFs identical:
# df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

# # #%%
# # inspect_col(df_12LL_after_BaseTable['last_home_visit'])

# #%%
# ### 2. Make change:
# print(f'Rows: {len(df_12LL_after_BaseTable)}')
# print('Filter "last_home_visit" to remove families without a home visit in the current fiscal year') 
# df_12LL_after_BaseTable = (
#     df_12LL_after_BaseTable
#     .query('last_home_visit >= @date_fy_start and last_home_visit < @date_fy_end_day_after')
# )
# print(f'Rows: {len(df_12LL_after_BaseTable)}')

# #%%
# ### 3. Manual/Visual checks:
# print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

# #%%
# ### 4. Programmatically test change:
# print('For change "Filter "last_home_visit" to remove families without a home visit in the current fiscal year"...') 
# ### ________________________________

# if (len(df_12LL_before_BaseTable) >= len(df_12LL_after_BaseTable)): 
#     print('Passed Test 2: Rows have been removed (unless no change).')
# else:
#     raise Exception('**Test 2 Failed: Greater number of rows after.')
# ### TODO: More specific test of row numbers?
# ### ________________________________

# if ((len(df_12LL_before_BaseTable.columns) == len(df_12LL_after_BaseTable.columns))
#     and ([*df_12LL_before_BaseTable] == [*df_12LL_after_BaseTable])): 
#     print('Passed Test 3: Number and names of columns unchanged.')
# else:
#     raise Exception('**Test 3 Failed: Number or names of columns has changed.')
# ### ________________________________

# if (all((df_12LL_after_BaseTable['last_home_visit'] >= date_fy_start) & (df_12LL_after_BaseTable['last_home_visit'] < date_fy_end_day_after))
#     and all(~((df_12LL_after_BaseTable['last_home_visit'] < date_fy_start) | (df_12LL_after_BaseTable['last_home_visit'] >= date_fy_end_day_after)))): 
#     print('Passed Test 4: After change, all "last_home_visit" dates within the Fiscal Year.')
# else:
#     raise Exception('**Test 4 Failed: After change, at least one "last_home_visit" date outside the Fiscal Year.')
# ### ________________________________

# if (all((df_12LL_before_BaseTable[~df_12LL_before_BaseTable.index.isin(df_12LL_after_BaseTable.index)]['last_home_visit'] < date_fy_start)
#     | (df_12LL_before_BaseTable[~df_12LL_before_BaseTable.index.isin(df_12LL_after_BaseTable.index)]['last_home_visit'] >= date_fy_end_day_after))): 
#     print('Passed Test 5: All rows filtered out had "last_home_visit" dates outside the Fiscal Year.')
# else:
#     raise Exception('**Test 5 Failed: At least one row filtered out had a "last_home_visit" within the Fiscal Year.')
# ### ________________________________

# print('All tests passed!')
# print(f'Rows: {len(df_12LL_after_BaseTable)}')
# print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

# #%%
# ### 5. Make DFs identical:
# df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 

# print('____________\n')


######################################
#%%###################################
### <> 10. Filter out families with 0 home visits (using "home_visits_num"). 

### Note: Filtering rows (parent-child combinations), not really families.

#%%
### Make change:
print('Step 10: Filter "home_visits_num" to remove families without a home visit') 
df_12LL_BaseTable = (
    df_12LL_BaseTable
    .query('home_visits_num > 0')
)
### Note: By this filter, all families to remove should have been removed above.


######################################
#%%###################################
### <> CREATE CFS ID File here.

df_12LL_ID_file = df_12LL_BaseTable.copy() 

#%%
### Write out Excel:

df_12LL_ID_file.to_excel(Path(path_12LL_output_ID_file), index = False)



### TODO: put in documentation:
### tgt = child
### mob = primary caregiver
### fob = secondary caregiver
### Expectation of target children: only first child (unless multiples); secondary children not tracked.

print('____________\n')


######################################
#%%###################################
### <> 11. Remove identifying variables 

# Step 1: Define columns to remove
cols_to_remove = [
    'worker_id',
    'tgt_first_name', 'tgt_last_name', 'tgt_ssn',
    'mob_first_name', 'mob_last_name', 'mob_ssn',
    'fob_first_name', 'fob_last_name', 'fob_ssn',
    'address', 'city'  # Note: keep ZIP
]

# Step 2: Drop columns and confirm change
print(f'Columns before: {len(df_12LL_BaseTable.columns)}')
print('Removing identifying variables...')

df_12LL_BaseTable = df_12LL_BaseTable.drop(columns=cols_to_remove)
print(f'Columns after: {len(df_12LL_BaseTable.columns)}')

# Step 3: Run tests to confirm success
print('Testing removal...')

# Test 1: Sensitive columns not present
sensitive_col_pattern = re.compile(r'(?i)(name|ssn|address|worker|((?<!ethni)city))')
remaining_sensitive_cols = [col for col in df_12LL_BaseTable.columns if sensitive_col_pattern.search(col)]

if len(remaining_sensitive_cols) == 0:
    print('✔ Passed: No identifying columns remain.')
else:
    raise Exception(f'✘ Failed: These sensitive columns still exist: {remaining_sensitive_cols}')

print('All tests passed.')
print(f'Rows: {len(df_12LL_BaseTable)}')
print(f'Columns: {len(df_12LL_BaseTable.columns)}')
print('____________\n')



######################################
#%%###################################
### <> 12. Create year & quarter columns (after all filtering)
### Make change:
print('Step 12: Create year & quarter columns') 
df_12LL_BaseTable = (
    df_12LL_BaseTable
    ### Add year & quarter columns AFTER all filters:
    .assign(year = int_nehv_year, quarter = int_nehv_quarter).astype({'year': 'Int64', 'quarter': 'Int64'})
)


######################################
#%%###################################
### <> 13. Reorder columns 

df_12LL_BaseTable = df_12LL_BaseTable[['project_id', 'year', 'quarter'] + [c for c in df_12LL_BaseTable.columns if c not in ['project_id', 'year', 'quarter']]]

### TODO: check number of columns.



#%%###################################
### <> df_12LL_BaseTable
df_12LL_BaseTable = df_12LL_BaseTable.copy()
df_12LL_BaseTablE=df_12LL_BaseTable.drop_duplicates()


#%%

print(df_12LL_allstring_2)
### !>>> 
#%%###################################
### <> df_12LL_2: 'KU_CHILDERINJ'.
df_12LL_ChildERInj_2 = (
    df_12LL_allstring_2
    ### 1. Strip surrounding whitespace:
    .map(lambda cell: cell.strip(), na_action='ignore').astype('string')
    ### 2. Find & replace "null" values:
    .pipe(fn_find_and_replace_value_in_df, 'family_id', list_12LL_values_to_find_and_replace, pd.NA)
    ### 3. Add nanoseconds to datetimes missing them:
    .replace({col:regex_12LL_dates_to_fix for col in list_12LL_date_cols_2}, regex_12LL_dates_replacement, regex=True) 
    ### 4. Set data types:
    .pipe(fn_apply_dtypes, dict_12LL_col_dtypes_2)
    ### 5v2. Column agency set to "ll":
    .assign(agency = 'll').astype({'agency': 'string'})
    ### 6. Column tgt_id fill NA with "0":
    .assign(tgt_id = lambda df: (df['tgt_id'].fillna('0')).astype('string')) 
    ### 7v2. Create project_id column:
    .assign(project_id = lambda df: (df['agency'] + df['family_id'] + '-' + df['tgt_id']).astype('string'))
    ### new8. Filter dates: Only want current FY for Form 2 Construct 8.
    .query('date >= @date_fy_start')
    ### TODO: rename section?: filter column reason to only accept "ER Visit".
    .query('reason == "ER Visit"')
    ### new9. Add year & quarter columns AFTER filter:
    .assign(year = int_nehv_year, quarter = int_nehv_quarter).astype({'year': 'Int64', 'quarter': 'Int64'})
    ### new10. Sort rows:
    .sort_values(by=['project_id', 'date'], na_position='first', ignore_index=True)
    ### new11. Reorder columns:
    [['project_id', 'year', 'quarter', 'agency', 'family_id', 'tgt_id', 'funding', 'reason', 'date']]
    ### new12. Rename columns:
    .rename(columns={'project_id': 'ProjectID', 'family_id': 'FAMILYNUMBER', 'tgt_id': 'ChildNumber', 'reason': 'ERVisitReason', 'date': 'IncidentDate'})
)

print(df_12LL_ChildERInj_2)



#%%###################################
### <> df_12LL_3: 'KU_MATERNALINS'.
df_12LL_MaternalIns_3 = (
    df_12LL_allstring_3
    ### 1. Strip surrounding whitespace:
    .map(lambda cell: cell.strip(), na_action='ignore').astype('string')
    ### 2. Find & replace "null" values:
    .pipe(fn_find_and_replace_value_in_df, 'family_id', list_12LL_values_to_find_and_replace, pd.NA)
    ### 3. Add nanoseconds to datetimes missing them:
    .replace({col:regex_12LL_dates_to_fix for col in list_12LL_date_cols_3}, regex_12LL_dates_replacement, regex=True) 
    ### 4. Set data types:
    .pipe(fn_apply_dtypes, dict_12LL_col_dtypes_3)
    ### 5v2. Column agency set to "ll":
    .assign(agency = 'll').astype({'agency': 'string'})
    ### 6. Column tgt_id fill NA with "0":
    .assign(tgt_id = lambda df: (df['tgt_id'].fillna('0')).astype('string')) 
    ### 7v2. Create project_id column:
    .assign(project_id = lambda df: (df['agency'] + df['family_id'] + '-' + df['tgt_id']).astype('string'))
    ### Note: Do NOT filter dates. Need insurance change dates before current FY for Form 2 Construct 16.
    ### new9. Add year & quarter columns AFTER filter:
    .assign(year = int_nehv_year, quarter = int_nehv_quarter).astype({'year': 'Int64', 'quarter': 'Int64'})
    ### new10. Sort rows:
    .sort_values(by=['project_id', 'date'], na_position='first', ignore_index=True)
    ### new11. Reorder columns:
    [['project_id', 'year', 'quarter', 'agency', 'family_id', 'tgt_id', 'funding', 'insurance', 'date']]
    ### new12. Rename columns:
    .rename(columns={'project_id': 'ProjectID', 'family_id': 'FAMILYNUMBER', 'tgt_id': 'ChildNumber', 'insurance': 'AD1PrimaryIns', 'date': 'AD1InsChangeDate'})
)
### from instructions "Insert a column B and enter this formula =COUNTIF($A$2:A2,A2) and move to column 4". 
### Answer: probably not needed. Was a count of rows per person.
### TODO: check later in code & see if a count column like this is needed.



#%%###################################
### <> df_12LL_4: 'KU_WELLCHILDVISITS'.
df_12LL_WellChildVisits_4 = (
    df_12LL_allstring_4
    ### 1. Strip surrounding whitespace:
    .map(lambda cell: cell.strip(), na_action='ignore').astype('string')
    ### 2. Find & replace "null" values:
    .pipe(fn_find_and_replace_value_in_df, 'family_id', list_12LL_values_to_find_and_replace, pd.NA)
    ### 3. Add nanoseconds to datetimes missing them:
    .replace({col:regex_12LL_dates_to_fix for col in list_12LL_date_cols_4}, regex_12LL_dates_replacement, regex=True) 
    ### 4. Set data types:
    .pipe(fn_apply_dtypes, dict_12LL_col_dtypes_4)
    ### 5v2. Column agency set to "ll":
    .assign(agency = 'll').astype({'agency': 'string'})
    ### 6. Column tgt_id fill NA with "0":
    .assign(tgt_id = lambda df: (df['tgt_id'].fillna('0')).astype('string')) 
    ### 7v2. Create project_id column:
    .assign(project_id = lambda df: (df['agency'] + df['family_id'] + '-' + df['tgt_id']).astype('string'))
    ### new8. Filter dates: Filter out bad data earlier than "2017-10-01". Need WCVisits before current FY for Form 2 Construct 4: ### TODO: Check C04.
        ### TODO: Add to wiki NE doc notably what is being filtered out & what is being kept. Just overview.
    .query('date >= "2017-10-01"') 
    ### new9. Add year & quarter columns AFTER filter:
    .assign(year = int_nehv_year, quarter = int_nehv_quarter).astype({'year': 'Int64', 'quarter': 'Int64'})
    ### new10. Sort rows:
    .sort_values(by=['project_id', 'date'], na_position='first', ignore_index=True)
    ### new11. Reorder columns:
    [['project_id', 'year', 'quarter', 'agency', 'family_id', 'tgt_id', 'funding', 'date']]
    ### new12. Rename columns:
    .rename(columns={'project_id': 'ProjectID', 'family_id': 'FAMILYNUMBER', 'tgt_id': 'ChildNumber', 'date': 'WellVisitDate'})
)



#%%##############################################!>>>
### >>> RESTRUCTURING  
#####################################################

### Compare to files here: 
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\previous\before restructure\Y13Q1 (Oct 2023 - Dec 2023) 
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.3combine\previous\after restructuring\Y13Q1 (Oct 2023 - Dec 2023) 


#%%###################################
### <> ChildERInj 

### Pivot the DataFrame:
df_12LL_pivoted_ChildERInj_2 = df_12LL_ChildERInj_2.pivot_table(
    index=['ProjectID', 'year', 'quarter', 'agency', 'FAMILYNUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted). ### NOTE: 'reason' not included because filtered above to be all the same.
    ,columns=df_12LL_ChildERInj_2.groupby(['ProjectID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['ERVisitReason', 'IncidentDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
df_12LL_pivoted_ChildERInj_2
### TODO ASKJOE: Do we need col 'ERVisitReason'? Joe does not keep.

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_12LL_pivoted_ChildERInj_2 = df_12LL_pivoted_ChildERInj_2.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
df_12LL_pivoted_ChildERInj_2

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_12LL_pivoted_ChildERInj_2.columns = [f'{col[0]}.{col[1]}' for col in df_12LL_pivoted_ChildERInj_2.columns]
df_12LL_pivoted_ChildERInj_2

#%%
### Reset row & column indices:
df_12LL_pivoted_ChildERInj_2 = df_12LL_pivoted_ChildERInj_2.reset_index()
df_12LL_pivoted_ChildERInj_2



#%%###################################
### <> MaternalIns 

### Pivot the DataFrame:
df_12LL_pivoted_MaternalIns_3 = df_12LL_MaternalIns_3.pivot_table(
    index=['ProjectID', 'year', 'quarter', 'agency', 'FAMILYNUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_12LL_MaternalIns_3.groupby(['ProjectID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['AD1PrimaryIns', 'AD1InsChangeDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
df_12LL_pivoted_MaternalIns_3

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_12LL_pivoted_MaternalIns_3 = df_12LL_pivoted_MaternalIns_3.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
df_12LL_pivoted_MaternalIns_3

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_12LL_pivoted_MaternalIns_3.columns = [f'{col[0]}.{col[1]}' for col in df_12LL_pivoted_MaternalIns_3.columns]
df_12LL_pivoted_MaternalIns_3

#%%
### Reset row & column indices:
df_12LL_pivoted_MaternalIns_3 = df_12LL_pivoted_MaternalIns_3.reset_index()
df_12LL_pivoted_MaternalIns_3



#%%###################################
### <> WellChildVisits 

### Pivot the DataFrame:
df_12LL_pivoted_WellChildVisits_4 = df_12LL_WellChildVisits_4.pivot_table(
    index=['ProjectID', 'year', 'quarter', 'agency', 'FAMILYNUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_12LL_WellChildVisits_4.groupby(['ProjectID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['WellVisitDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
# df_12LL_pivoted_WellChildVisits_4

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_12LL_pivoted_WellChildVisits_4 = df_12LL_pivoted_WellChildVisits_4.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
# df_12LL_pivoted_WellChildVisits_4

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_12LL_pivoted_WellChildVisits_4.columns = [f'{col[0]}.{col[1]}' for col in df_12LL_pivoted_WellChildVisits_4.columns]
# df_12LL_pivoted_WellChildVisits_4

#%%
### Reset row & column indices:
df_12LL_pivoted_WellChildVisits_4 = df_12LL_pivoted_WellChildVisits_4.reset_index()
# df_12LL_pivoted_WellChildVisits_4



#%%##############################################!>>>
### >>> WRITE OUT FILES   
#####################################################

### Note: Date columns written out without timestamps.

df_12LL_BaseTable.to_csv(Path(path_12LL_dir_output, 'df_12LL_BaseTable.csv'), index = False, date_format="%m/%d/%Y")
df_12LL_pivoted_ChildERInj_2.to_csv(Path(path_12LL_dir_output, 'df_12LL_pivoted_ChildERInj_2.csv'), index = False, date_format="%m/%d/%Y")
df_12LL_pivoted_MaternalIns_3.to_csv(Path(path_12LL_dir_output, 'df_12LL_pivoted_MaternalIns_3.csv'), index = False, date_format="%m/%d/%Y")
df_12LL_pivoted_WellChildVisits_4.to_csv(Path(path_12LL_dir_output, 'df_12LL_pivoted_WellChildVisits_4.csv'), index = False, date_format="%m/%d/%Y")



df_12LL_BaseTable.to_csv(Path(path_1_3_dir_input, 'df_12LL_BaseTable.csv'), index = False, date_format="%m/%d/%Y")
df_12LL_pivoted_ChildERInj_2.to_csv(Path(path_1_3_dir_input, 'df_12LL_pivoted_ChildERInj_2.csv'), index = False, date_format="%m/%d/%Y")
df_12LL_pivoted_MaternalIns_3.to_csv(Path(path_1_3_dir_input, 'df_12LL_pivoted_MaternalIns_3.csv'), index = False, date_format="%m/%d/%Y")
df_12LL_pivoted_WellChildVisits_4.to_csv(Path(path_1_3_dir_input, 'df_12LL_pivoted_WellChildVisits_4.csv'), index = False, date_format="%m/%d/%Y")


#%%##############################################!>>>
### >>> Remove old objects  
#####################################################

#%%
[o for o in list(globals().keys()) if o.startswith(('date', 'int', 'path', 'str'))]
### Keep.

#%%
# [o for o in list(globals().keys()) if o.startswith('df')]
#%%
del df_12LL_allstring_1, df_12LL_allstring_2, df_12LL_allstring_3, df_12LL_allstring_4, df_12LL_ChildERInj_2, df_12LL_MaternalIns_3, df_12LL_WellChildVisits_4 

#%%
# [o for o in list(globals().keys()) if o.startswith('dict')]
#%%
del dict_12LL_col_dtypes_1, dict_12LL_col_dtypes_2, dict_12LL_col_dtypes_3, dict_12LL_col_dtypes_4 

#%%
# [o for o in list(globals().keys()) if o.startswith('list')]
#%%
del list_path_12LL_input_raw_sheets, list_12LL_col_detail_1, list_12LL_date_cols_1, list_12LL_col_detail_2, list_12LL_date_cols_2, list_12LL_col_detail_3, list_12LL_date_cols_3, list_12LL_col_detail_4, list_12LL_date_cols_4, list_12LL_values_to_find_and_replace 

#%%
# [o for o in list(globals().keys()) if o.startswith(('regex', 'xlsx'))]
#%%
del xlsx_12LL, regex_12LL_dates_to_fix, regex_12LL_dates_replacement 

#%%
### Is what's left over what is wanted?:
[o for o in list(globals().keys()) if o.startswith(('df', 'dict', 'list', 'regex', 'xlsx'))]
### Should only be:
# ['df_12LL_BaseTable',
#  'df_12LL_pivoted_ChildERInj_2',
#  'df_12LL_pivoted_MaternalIns_3',
#  'df_12LL_pivoted_WellChildVisits_4']



#%%##############################################!>>>
### >>> END 
#################################################!>>>

print('Congrats! You ran 1.2LL!')





