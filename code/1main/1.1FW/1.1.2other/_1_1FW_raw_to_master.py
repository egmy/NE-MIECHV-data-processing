



### Process for receiving data from Lincoln Lancaster County Health Department (LL):
    # 1. Original files transferred from external vendor (LL) to NE State, who then puts the files in folder "U:\SFTP".
    # 2. Then we organize the files in folder "Master (Files Used For Quarterly Reports)".
    # 3. Then we copy the files into: "U:\Working" >> "Tableau" >> year >> quarter >> "LL" folder (for example: "U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Y12Q4 (Oct 2022 - Sep 2023)\LLCHD").


### Copying over file from: "U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Y12Q4 (Oct 2022 - Sep 2023)\LLCHD":

### To here: "U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\0in\Y12Q4 (Oct 2022 - Sep 2023)"






### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%##############################################!>>>
### >>>  INSTRUCTIONS 
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.



#%%##############################################!>>> 
### >>>  SETUP 
#####################################################

import os 

#%%
print('File that is running: ', os.path.basename(__file__))

#%%
### The following is run if running this file by itself interactively (& ignored when run from RUNME):
if (os.path.basename(__file__) == '_1_2LL_raw_to_master.py'):
    from _1_1_FW_RUNME import * 
    print('Imported "_1_1_FW_RUNME"')
else:
    print("Did NOT run RUNME again... because it's already running!")


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
print(dict_12LL_col_dtypes_1)
print(collections.Counter(list(dict_12LL_col_dtypes_1.values())))
### List of date columns:
list_12LL_date_cols_1 = [key for key, value in dict_12LL_col_dtypes_1.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_12LL_2: 'KU_CHILDERINJ'.
list_12LL_col_detail_2 = [
    ['family_id', 'string']
    ,['tgt_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['funding', 'string']
    ,['reason', 'string']
    ,['date', 'datetime64[ns]']
]
#%%### df_12LL_2: 'KU_CHILDERINJ'.
dict_12LL_col_dtypes_2 = {x[0]:x[1] for x in list_12LL_col_detail_2}
print(dict_12LL_col_dtypes_2)
print(collections.Counter(list(dict_12LL_col_dtypes_2.values())))
### List of date columns:
list_12LL_date_cols_2 = [key for key, value in dict_12LL_col_dtypes_2.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_12LL_3: 'KU_MATERNALINS'.
list_12LL_col_detail_3 = [
    ['family_id', 'string']
    ,['tgt_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['funding', 'string']
    ,['insurance', 'string'] ### Could be 'Int64'; however, left as string because will union with FW string vars. Will be UN-coded in 1.4 code.
    ,['date', 'datetime64[ns]']
]
#%%### df_12LL_3: 'KU_MATERNALINS'.
dict_12LL_col_dtypes_3 = {x[0]:x[1] for x in list_12LL_col_detail_3}
print(dict_12LL_col_dtypes_3)
print(collections.Counter(list(dict_12LL_col_dtypes_3.values())))
### List of date columns:
list_12LL_date_cols_3 = [key for key, value in dict_12LL_col_dtypes_3.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_12LL_4: 'KU_WELLCHILDVISITS'.
list_12LL_col_detail_4 = [
    ['family_id', 'string']
    ,['tgt_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['funding', 'string']
    ,['date', 'datetime64[ns]']
]
#%%### df_12LL_4: 'KU_WELLCHILDVISITS'.
dict_12LL_col_dtypes_4 = {x[0]:x[1] for x in list_12LL_col_detail_4}
print(dict_12LL_col_dtypes_4)
print(collections.Counter(list(dict_12LL_col_dtypes_4.values())))
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
print(sorted(list_path_12LL_input_raw_sheets))
print(sorted(xlsx_12LL.sheet_names))
sorted(list_path_12LL_input_raw_sheets) == sorted(xlsx_12LL.sheet_names)

#%%###################################
### READ in all sheets as strings.

### Read in EVERYTHING as a string WITH pd.NA for empty cells:
df_12LL_allstring_1 = pd.read_excel(xlsx_12LL, sheet_name=list_path_12LL_input_raw_sheets[0], keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_12LL_col_dtypes_1)
df_12LL_allstring_2 = pd.read_excel(xlsx_12LL, sheet_name=list_path_12LL_input_raw_sheets[1], keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_12LL_col_dtypes_2)
df_12LL_allstring_3 = pd.read_excel(xlsx_12LL, sheet_name=list_path_12LL_input_raw_sheets[2], keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_12LL_col_dtypes_3)
df_12LL_allstring_4 = pd.read_excel(xlsx_12LL, sheet_name=list_path_12LL_input_raw_sheets[3], keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_12LL_col_dtypes_4)

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

df_12LL_before_BaseTable = df_12LL_allstring_1.copy()
df_12LL_after_BaseTable = df_12LL_allstring_1.copy() 

#%% ### If needed, fo rtesting:
# df_12LL_after_BaseTable = df_12LL_before_BaseTable.copy() 



######################################
#%%###################################
### <> 1. Strip surrounding whitespace

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print('Strip surrounding whitespace')
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

#%%
### See differences:
df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)

#%%
### 4. Programmatically test change:
print('For change "Strip surrounding whitespace"...') 
### ________________________________

if (df_12LL_before_BaseTable.isna().sum().sum() == df_12LL_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_12LL_before_BaseTable) == len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) == len(df_12LL_after_BaseTable.columns))
    and ([*df_12LL_before_BaseTable] == [*df_12LL_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> 2. Find & replace "null" values

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print('Find & replace "null" values')
list_12LL_values_to_find_and_replace = ['null'] 
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .pipe(fn_find_and_replace_value_in_df, one_id_var='family_id', list_of_values_to_find=list_12LL_values_to_find_and_replace, replacement_value=pd.NA)
)
### Note: ### TODO: At the moment, searching is case-insensitive. Could make option for case sensitive.
### Note: ### TODO: At the moment, the entire cell must match. Could make an option for matching with substrings.

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

# print(f'{len(df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)) == 0}')
# print(f'{df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')
# print(f'{assert_frame_equal(df_12LL_before_BaseTable, df_12LL_after_BaseTable) is None}')

#%%
### See differences:
df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)

#%%
### 4. Programmatically test change:
print('For change "Find & replace "null" values"...') 
### ________________________________

if (df_12LL_before_BaseTable.isna().sum().sum() <= df_12LL_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: There are more NA after (unless no change).')
else:
    raise Exception('**Test 1 Failed: Fewer NA after.')
### ________________________________

if (len(df_12LL_before_BaseTable) == len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) == len(df_12LL_after_BaseTable.columns))
    and ([*df_12LL_before_BaseTable] == [*df_12LL_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if ((fn_find_and_count_value_in_df(df_12LL_before_BaseTable, list_12LL_values_to_find_and_replace) >= 0)
    and (fn_find_and_count_value_in_df(df_12LL_after_BaseTable, list_12LL_values_to_find_and_replace) == 0)): 
    print('Passed Test 4: Values to find NOT found after, but maybe found before.')
else:
    raise Exception('**Test 4 Failed: Vales to find found after.')
### ________________________________

### TODO: Test 5: Find where before is different & is "null" & see if after turned that NA.
if (True): #TODO
    print('Passed Test 4:')
else:
    raise Exception('**Test 4 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> 3. Add nanoseconds to strings of datetimes missing them   

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print('Add nanoseconds to datetimes strings missing them') 

### Columns that later have problems with "fn_apply_dtypes":
### list_date_cols_to_edit = ['c_fundingdate', 'mob_living_arrangement_dt', 'fob_edu_dt', 'mcafss_edu_dt1', 'mcafss_edu_dt2', 'hlth_insure_tgt_dt'] ### Specific columns causing errors Y13Q1.

regex_12LL_dates_to_fix = r'(^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$)' 
regex_12LL_dates_replacement = r'\1.000000000' ### 9 zeros for nanoseconds! 

df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .replace({col:regex_12LL_dates_to_fix for col in list_12LL_date_cols_1}, regex_12LL_dates_replacement, regex=True) ### Checking all date columns.
)
### Format causing errors: "2019-12-04 17:48:04" (missing nanoseconds).
### Want this format: "2019-12-10 14:02:37.223000000"

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

#%%
### See differences:
df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)

#%%
print((
    df_12LL_before_BaseTable
    ### Compare: Keep shape so ID column not dropped when the same. Keep equal so can see ID values.
    # .compare(df_12LL_after_BaseTable, keep_shape=True, keep_equal=True) 
    ### Select desired columns:
    .loc[:, ['c_fundingdate', 'mob_living_arrangement_dt', 'fob_edu_dt', 'mcafss_edu_dt1', 'mcafss_edu_dt2', 'hlth_insure_tgt_dt']]
    ### Keep rows where columns different:
    # .loc[lambda df: df.apply(fn_keep_row_differences, variable2compare=str_col_to_compare, axis=1), :] 
).to_string())

#%%
### 4. Programmatically test change:
print('For change "Add nanoseconds to datetimes strings missing them"...') 
### ________________________________

if (df_12LL_before_BaseTable.isna().sum().sum() == df_12LL_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_12LL_before_BaseTable) == len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) == len(df_12LL_after_BaseTable.columns))
    and ([*df_12LL_before_BaseTable] == [*df_12LL_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

### TODO: Add a test on the regex values & see if changed.
if (True): #TODO
    print('Passed Test 4:')
else:
    raise Exception('**Test 4 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> 4. Set data types 

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print('Set data types') 
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .pipe(fn_apply_dtypes, dict_12LL_col_dtypes_1)
)
### Note: Needed to edit date strings above before applying dtypes.

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

#%%
### See differences:
df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)

#%%
### Check if data types changed:
print(df_12LL_before_BaseTable.dtypes.to_string())
print('-------------------------------------')
print(df_12LL_after_BaseTable.dtypes.to_string())
# print('-------------------------------------')
# print(df_12LL_allstring_1.dtypes.to_string())

#%%
df_12LL_before_BaseTable.dtypes.to_string() == df_12LL_after_BaseTable.dtypes.to_string()

#%%
### 4. Programmatically test change:
print('For change "Set data types"...') 
### ________________________________

if (df_12LL_before_BaseTable.isna().sum().sum() == df_12LL_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_12LL_before_BaseTable) == len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) == len(df_12LL_after_BaseTable.columns))
    and ([*df_12LL_before_BaseTable] == [*df_12LL_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

### TODO: Test 4: Not every column is string (Not always true for every dataset!):
if (True): #TODO
    print('Passed Test 4:')
else:
    raise Exception('**Test 4 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> 5. Column site_id set to "ll" 

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print('Column site_id should now be all "ll".') 
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .assign(site_id = 'll').astype({'site_id': 'string'})
)

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

#%%
### See differences:
df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)

#%%
compare_col(df_12LL_before_BaseTable, df_12LL_after_BaseTable, 'site_id', 'value_counts')

#%%
### 4. Programmatically test change:
print('For change "Column site_id set to "ll""...') 
### ________________________________

if (df_12LL_before_BaseTable.isna().sum().sum() == df_12LL_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_12LL_before_BaseTable) == len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) == len(df_12LL_after_BaseTable.columns))
    and ([*df_12LL_before_BaseTable] == [*df_12LL_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if ((all(df_12LL_before_BaseTable['site_id']=='01')) and (all(df_12LL_after_BaseTable['site_id']=='ll'))): 
    print('Passed Test 4: site_id is "01" before and "ll" after.')
else:
    raise Exception('**Test 4 Failed: site_id is either not "01" before or not "ll" after.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> 6. Column tgt_id fill NA with "0" 


#     ####
#     .pipe(fn_print_fstring_and_return_df, '-----\nColumn tgt_id before: Rows with NA that will be changed to "0":')
#     .pipe(fn_print_expression_and_return_df, (lambda df: df.loc[(lambda df: pd.isna(df['tgt_id'])), 'tgt_id'].index.tolist()))
#     
#     .pipe(fn_print_fstring_and_return_df, 'Column tgt_id after: Rows with "0":')
#     .pipe(fn_print_expression_and_return_df, (lambda df: df.loc[(df['tgt_id'] == "0"), 'tgt_id'].index.tolist()))


#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print('All NAs in column tgt_id should be replaced with "0".') 
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .assign(tgt_id = lambda df: (df['tgt_id'].fillna('0')).astype('string')) 
)

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

#%%
### See differences:
df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)

#%%
### 4. Programmatically test change:
print('For change "Column tgt_id fill NA with "0""...') 
### ________________________________

if (df_12LL_before_BaseTable.isna().sum().sum() > df_12LL_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Now less NA.')
else:
    raise Exception('**Test 1 Failed: Number of NA not less.')
### ________________________________

if (len(df_12LL_before_BaseTable) == len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) == len(df_12LL_after_BaseTable.columns))
    and ([*df_12LL_before_BaseTable] == [*df_12LL_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if (df_12LL_after_BaseTable['tgt_id'].isna().sum() == 0): 
    print('Passed Test 4: Column tgt_id has no NA after change.')
else:
    raise Exception('**Test 4 Failed: Column tgt_id does have NA after change.')
### ________________________________

# if (df_12LL_before_BaseTable['tgt_id'].isna().sum() == df_12LL_after_BaseTable['tgt_id']): #TODO
#     print('Passed Test 5: Number of NA before equals number of 0 after... (maybe below better)')
# else:
#     raise Exception('**Test 5 Failed:')
### ________________________________

### Test 6: show that all before 0's are now NA -- filtering on those indices?
if (True): #TODO
    print('Passed Test 4:')
else:
    raise Exception('**Test 4 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> 7. Create project_id column 

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')
print('Create project_id column') 
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .assign(project_id = lambda df: (df['site_id'] + df['family_id'] + '-' + df['tgt_id']).astype('string'))
)
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

### #%%
### ### See differences:
### df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable) ### Can't because columns different.

#%% 
inspect_col(df_12LL_after_BaseTable['project_id'])

#%%
### 4. Programmatically test change:
print('For change "Create project_id column"...') 
### ________________________________

### Note: Should have no new NA because new column should be entirely filled.
if (df_12LL_before_BaseTable.isna().sum().sum() == df_12LL_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_12LL_before_BaseTable) == len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) + 1 == len(df_12LL_after_BaseTable.columns))
    and (sorted([*df_12LL_before_BaseTable] + ['project_id']) == sorted([*df_12LL_after_BaseTable]))): 
    print('Passed Test 3: Exactly one more column with name "project_id".')
else:
    raise Exception('**Test 3 Failed: Not exactly one more column named "project_id".')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%
### Review columns that will be used to filter rows:
print((
    df_12LL_before_BaseTable
    .loc[:, ['project_id', 'discharge_dt', 'last_home_visit', 'home_visits_num']]
    .dtypes
).to_string())



######################################
#%%###################################
### <> 8. Filter out families that discharged before the current reporting year (using "discharge_dt"). 

### Note: Filtering rows (parent-child combinations), not really families.

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

# #%%
# df_12LL_after_BaseTable['discharge_dt'].dtypes

# #%%
# print(df_12LL_before_BaseTable.dtypes.to_string())
# print('-------------------------------------')
# print(df_12LL_after_BaseTable.dtypes.to_string())

# discharge_dt                  datetime64[ns]

#%%
### 2. Make change:
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print('Filter "discharge_dt" to remove families discharged before current reporting year') 
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    ### Keep both later dates AND where NO discharge date:
    .query('discharge_dt >= @date_fy_start or discharge_dt.isna()')
)
print(f'Rows: {len(df_12LL_after_BaseTable)}')

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

# #%%
# ### See differences:
# df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable) ### Cannot .compare when different rows.

#%%
### 4. Programmatically test change:
print('For change "Filter "discharge_dt" to remove families discharged before current reporting year"...') 
### ________________________________

# ### Don't use Test 1: Because removing rows, NA might be very different.
# if (df_12LL_before_BaseTable.isna().sum().sum() == df_12LL_after_BaseTable.isna().sum().sum()):
#     print('Passed Test 1: Number of NA unchanged.')
# else:
#     raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_12LL_before_BaseTable) >= len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Rows have been removed (unless no change).')
else:
    raise Exception('**Test 2 Failed: Greater number of rows after.')
### TODO: More specific test of row numbers?
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) == len(df_12LL_after_BaseTable.columns))
    and ([*df_12LL_before_BaseTable] == [*df_12LL_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if (all((df_12LL_after_BaseTable['discharge_dt'] >= date_fy_start) | pd.isna(df_12LL_after_BaseTable['discharge_dt']))
    and all(~(df_12LL_after_BaseTable['discharge_dt'] < date_fy_start))): 
    print('Passed Test 4: After change, all "discharge_dt" dates on or after the Fiscal Year start date (or are NA).')
else:
    raise Exception('**Test 4 Failed: After change, at least one "discharge_dt" date before the Fiscal Year start date.')
### ________________________________

if (all(df_12LL_before_BaseTable[~df_12LL_before_BaseTable.index.isin(df_12LL_after_BaseTable.index)]['discharge_dt'] < date_fy_start)): 
    print('Passed Test 5: All rows filtered out had "discharge_dt" dates before the Fiscal Year start date.')
else:
    raise Exception('**Test 5 Failed: At least one row filtered out had a "discharge_dt" date not before the Fiscal Year start date.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> 9. Filter out families without a home visit in the current fiscal year (using "last_home_visit"). 

### Note: Filtering rows (parent-child combinations), not really families. (Joe approves!)
### TODO: Check later (in 1.4 or Report) (maybe "active child") where DOB are checked to filter out "subsequent children".

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

# #%%
# inspect_col(df_12LL_after_BaseTable['last_home_visit'])

#%%
### 2. Make change:
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print('Filter "last_home_visit" to remove families without a home visit in the current fiscal year') 
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .query('last_home_visit >= @date_fy_start and last_home_visit < @date_fy_end_day_after')
)
print(f'Rows: {len(df_12LL_after_BaseTable)}')

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

#%%
### 4. Programmatically test change:
print('For change "Filter "last_home_visit" to remove families without a home visit in the current fiscal year"...') 
### ________________________________

if (len(df_12LL_before_BaseTable) >= len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Rows have been removed (unless no change).')
else:
    raise Exception('**Test 2 Failed: Greater number of rows after.')
### TODO: More specific test of row numbers?
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) == len(df_12LL_after_BaseTable.columns))
    and ([*df_12LL_before_BaseTable] == [*df_12LL_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if (all((df_12LL_after_BaseTable['last_home_visit'] >= date_fy_start) & (df_12LL_after_BaseTable['last_home_visit'] < date_fy_end_day_after))
    and all(~((df_12LL_after_BaseTable['last_home_visit'] < date_fy_start) | (df_12LL_after_BaseTable['last_home_visit'] >= date_fy_end_day_after)))): 
    print('Passed Test 4: After change, all "last_home_visit" dates within the Fiscal Year.')
else:
    raise Exception('**Test 4 Failed: After change, at least one "last_home_visit" date outside the Fiscal Year.')
### ________________________________

if (all((df_12LL_before_BaseTable[~df_12LL_before_BaseTable.index.isin(df_12LL_after_BaseTable.index)]['last_home_visit'] < date_fy_start)
    | (df_12LL_before_BaseTable[~df_12LL_before_BaseTable.index.isin(df_12LL_after_BaseTable.index)]['last_home_visit'] >= date_fy_end_day_after))): 
    print('Passed Test 5: All rows filtered out had "last_home_visit" dates outside the Fiscal Year.')
else:
    raise Exception('**Test 5 Failed: At least one row filtered out had a "last_home_visit" within the Fiscal Year.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> 10. Filter out families with 0 home visits (using "home_visits_num"). 

### Note: Filtering rows (parent-child combinations), not really families.

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

# #%%
# inspect_col(df_12LL_after_BaseTable['home_visits_num']) ### Int64.

#%%
### 2. Make change:
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print('Filter "home_visits_num" to remove families without a home visit') 
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .query('home_visits_num > 0')
)
print(f'Rows: {len(df_12LL_after_BaseTable)}')
### Note: By this filter, all families to remove should have been removed above.

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

#%%
### 4. Programmatically test change:
print('For change "Filter "home_visits_num" to remove families without a home visit"...') 
### ________________________________

if (len(df_12LL_before_BaseTable) >= len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Rows have been removed (unless no change).')
else:
    raise Exception('**Test 2 Failed: Greater number of rows after.')
### TODO: More specific test of row numbers?
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) == len(df_12LL_after_BaseTable.columns))
    and ([*df_12LL_before_BaseTable] == [*df_12LL_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if (all(df_12LL_after_BaseTable['home_visits_num'] > 0)
    and all(~(df_12LL_after_BaseTable['home_visits_num'] <= 0))): 
    print('Passed Test 4: After change, all "home_visits_num" numbers greater than 0.')
else:
    raise Exception('**Test 4 Failed: After change, at least one "home_visits_num" number less than or equal to 0.')
### ________________________________

if (all(df_12LL_before_BaseTable[~df_12LL_before_BaseTable.index.isin(df_12LL_after_BaseTable.index)]['home_visits_num'] <= 0)): 
    print('Passed Test 5: All rows filtered out had "home_visits_num" numbers less than or equal to 0.')
else:
    raise Exception('**Test 5 Failed: At least one row filtered out had a "home_visits_num" number greater than 0.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



### <> NOTE: CREATE CFS ID File here.


### TODO: put in documentation:
### tgt = child
### mob = primary caregiver
### fob = secondary caregiver
### Expectation of target children: only first child (unless multiples); secondary children not tracked.


######################################
#%%###################################
### <> 11. Remove identifying variables 

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### Search for specific columns:
### Want to remove: first and last name of tgt, mob, and fob; SSNs of tgt, mob, and fob; address; city; and worker_id. (Leave ZIP).
list(filter(lambda col: re.search(r'(?i)(name|ssn|address|worker|((?<!ethni)city))', col), [*df_12LL_after_BaseTable]))

#%%
### 2. Make change:
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')
print('Remove identifying variables') 
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .drop(columns=['worker_id', 'tgt_first_name', 'tgt_last_name', 'tgt_ssn', 'mob_first_name', 'mob_last_name', 'mob_ssn', 'fob_first_name', 'fob_last_name', 'fob_ssn', 'address', 'city']) 
)
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')
### Note: LEAVE ZIP Code!

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

#%%
### 4. Programmatically test change:
print('For change "Remove identifying variables"...') 
### ________________________________

if (len(df_12LL_before_BaseTable) == len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if (len(df_12LL_before_BaseTable.columns) >= len(df_12LL_after_BaseTable.columns)):
    print('Passed Test 3: Columns have been removed (unless no change).')
else:
    raise Exception('**Test 3 Failed: Greater number of columns after.')
### ________________________________

if ((len(list(filter(lambda col: re.search(r'(?i)(name|ssn|address|worker|((?<!ethni)city))', col), [*df_12LL_before_BaseTable]))) >= 0)
    and (len(list(filter(lambda col: re.search(r'(?i)(name|ssn|address|worker|((?<!ethni)city))', col), [*df_12LL_after_BaseTable]))) == 0)): 
    print('Passed Test 4: Variables to delete possibly present before but definitely not after.')
else:
    raise Exception('**Test 4 Failed: Variables to delete not present before or present after.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> 12. Create year & quarter columns (after all filtering)

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')
print('Create year & quarter columns') 
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    ### Add year & quarter columns AFTER all filters:
    .assign(year = int_nehv_year, quarter = int_nehv_quarter).astype({'year': 'Int64', 'quarter': 'Int64'})
)
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

### #%%
### ### See differences:
### df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable) ### Can't because columns different.

#%% 
inspect_col(df_12LL_after_BaseTable['year'])
#%%
inspect_col(df_12LL_after_BaseTable['quarter'])

#%%
### 4. Programmatically test change:
print('For change "Create year & quarter columns"...') 
### ________________________________

### Note: Should have no new NA because new column should be entirely filled.
if (df_12LL_before_BaseTable.isna().sum().sum() == df_12LL_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_12LL_before_BaseTable) == len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) + 2 == len(df_12LL_after_BaseTable.columns))
    and (sorted([*df_12LL_before_BaseTable] + ['year', 'quarter']) == sorted([*df_12LL_after_BaseTable]))): 
    print('Passed Test 3: Exactly 2 more columns named "year" & "quarter".')
else:
    raise Exception('**Test 3 Failed: Not exactly 2 more columns named "year" & "quarter".')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> 13. Reorder columns 

df_12LL_after_BaseTable = df_12LL_after_BaseTable[['project_id', 'year', 'quarter'] + [c for c in df_12LL_after_BaseTable.columns if c not in ['project_id', 'year', 'quarter']]]

### TODO: check number of columns.



#%%###################################
### <> df_12LL_BaseTable
df_12LL_BaseTable = df_12LL_after_BaseTable.copy()



#%%
### <> NOTE: Previously, FW & LL combined before the following restructuring and joining.


### TODO: add to 1.3:
### NOTE for 1.3 step: intention is to have all quarters represented in DS, but NO data from previous FYs. Purpose: allow local users to check & clean their data throughout the year.


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



#%%###################################
### <> Inspect DFs

#%%
# inspect_df(df_12LL_BaseTable)
# ### Counts of dtypes:
# print(collections.Counter(df_12LL_BaseTable.dtypes))

#%%
inspect_df(df_12LL_ChildERInj_2)

#%%
inspect_df(df_12LL_MaternalIns_3)

#%%
inspect_df(df_12LL_WellChildVisits_4)



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
    index=['ProjectID', 'year', 'quarter', 'agency', 'FAMILYNUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_12LL_ChildERInj_2.groupby(['ProjectID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['ERVisitReason', 'IncidentDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
df_12LL_pivoted_ChildERInj_2

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



#%%##############################################!>>>
### >>> Remove old objects  
#####################################################

#%%
[o for o in list(globals().keys()) if o.startswith(('date', 'int', 'path', 'str'))]
### Keep.

#%%
# [o for o in list(globals().keys()) if o.startswith('df')]
#%%
del df_12LL_allstring_1, df_12LL_allstring_2, df_12LL_allstring_3, df_12LL_allstring_4, df_12LL_before_BaseTable, df_12LL_after_BaseTable, df_12LL_ChildERInj_2, df_12LL_MaternalIns_3, df_12LL_WellChildVisits_4 

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




# %%
### TODO:
    ### Remove duplciate rows (like how tried in 1.4 code).
    ### see if "logger" package would be useful.




