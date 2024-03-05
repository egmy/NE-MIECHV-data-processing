



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
    from _1_2LL_RUNME import * 
    print('Imported "_1_2LL_RUNME"')
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

#######################
#%%### df_12LL_3: 'KU_MATERNALINS'.
list_12LL_col_detail_3 = [
    ['family_id', 'string']
    ,['tgt_id', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['funding', 'string']
    ,['insurance', 'Int64']
    ,['date', 'datetime64[ns]']
]
#%%### df_12LL_3: 'KU_MATERNALINS'.
dict_12LL_col_dtypes_3 = {x[0]:x[1] for x in list_12LL_col_detail_3}
print(dict_12LL_col_dtypes_3)
print(collections.Counter(list(dict_12LL_col_dtypes_3.values())))

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

### Backup:
df_12LL_1 = df_12LL_allstring_1.copy()
df_12LL_2 = df_12LL_allstring_2.copy()
df_12LL_3 = df_12LL_allstring_3.copy()
df_12LL_4 = df_12LL_allstring_4.copy()

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
### <> df_12LL_2: 'KU_CHILDERINJ'.
df_12LL_ChildERInj = (
    df_12LL_allstring_2
    .applymap(lambda cell: cell.strip(), na_action='ignore').astype('string')
    .pipe(fn_find_and_replace_value_in_df, 'family_id', ['null'], pd.NA)
    .pipe(fn_apply_dtypes, dict_12LL_col_dtypes_2)
)

#%%###################################
### <> df_12LL_3: 'KU_MATERNALINS'.
df_12LL_MaternalIns = (
    df_12LL_allstring_3
    .applymap(lambda cell: cell.strip(), na_action='ignore').astype('string')
    .pipe(fn_find_and_replace_value_in_df, 'family_id', ['null'], pd.NA)
    .pipe(fn_apply_dtypes, dict_12LL_col_dtypes_3)
)

#%%###################################
### <> df_12LL_4: 'KU_WELLCHILDVISITS'.
df_12LL_WellChildVisits = (
    df_12LL_allstring_4
    .applymap(lambda cell: cell.strip(), na_action='ignore').astype('string')
    .pipe(fn_find_and_replace_value_in_df, 'family_id', ['null'], pd.NA)
    .pipe(fn_apply_dtypes, dict_12LL_col_dtypes_4)
)









































#%%###################################
### >>> df_12LL_1: 'KU_BASETABLE'.


### <> old
# df_12LL_BaseTable = (
#     ### Raw table all read in as strings:
#     df_12LL_allstring_1


# )

    # .pipe(fn_print_expression_and_return_df, (lambda df: df), '')


# #%%
# # df_12LL_BaseTable.project_id


# # #%%
# # print(df_12LL_allstring_1['tgt_id'].value_counts(dropna=False).to_string())
# # #%%
# # print(df_12LL_BaseTable['tgt_id'].value_counts(dropna=False).to_string())
# #%%
# col_to_review = 'tgt_id'
# compare_col(
#     (df_12LL_allstring_1
#         .loc[lambda df: pd.isna(df[col_to_review])]
#     )
#     ,(df_12LL_BaseTable
#         .query(f'`{col_to_review}` == "0"')
#     )
#     ,col_to_review ,'value_counts'
# )



#%%###################################
### <> Before & After 
### df_12LL_1: 'KU_BASETABLE'.

df_12LL_before_BaseTable = df_12LL_allstring_1.copy()
df_12LL_after_BaseTable = df_12LL_allstring_1.copy() 

#%% ### If needed, fo rtesting:
# df_12LL_after_BaseTable = df_12LL_before_BaseTable.copy() 

######################################
#%%###################################
### <> Strip surrounding whitespace

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print('Strip surrounding whitespace')
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .applymap(lambda cell: cell.strip(), na_action='ignore').astype('string')
)

### TODO: "FutureWarning: DataFrame.applymap has been deprecated. Use DataFrame.map instead."

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
### <> Find & replace "null" values

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print('Find & replace "null" values')
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .pipe(fn_find_and_replace_value_in_df, one_id_var='family_id', list_of_values_to_find=['null'], replacement_value=pd.NA)
)
### Note: ### TODO: At the moment, searching is case-insensitive. Could make option for case sensitive.

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

### Test 4: Find where before is different & is "null" & see if after turned that NA.
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
### <> Add nanoseconds to datetimes missing them   

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print('Add nanoseconds to datetimes missing them') 

### Columns that later have problems with "fn_apply_dtypes":
list_date_cols_to_edit = ['c_fundingdate', 'mob_living_arrangement_dt', 'fob_edu_dt', 'mcafss_edu_dt1', 'mcafss_edu_dt2', 'hlth_insure_tgt_dt']

df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    # .replace({col:r'(^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$)' for col in list_date_cols_to_edit}, r'(\1)(\.)0{9}', regex=True) 
    .replace({col:r'(^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$)' for col in list_date_cols_to_edit}, r'\1.000000000', regex=True) 
)
### TODO: apply on all date vars.

# dict_12LL_col_dtypes_1 = {x[0]:x[1] for x in list_12LL_col_detail_1}

### Want: "2019-12-10 14:02:37.223000000"
### Causing errors: "2019-12-04 17:48:04"

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
print('For change "Add nanoseconds to datetimes missing them"...') 
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
### <> Set data types 

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

#%%
### TODO: Date issues.

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

### Test 4: Not every column is string (Not always true for every dataset!):
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
### <> Column site_id set to "ll" 

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print('Column site_id should now be all "ll".') 
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .assign(site_id = 'll')
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

if (True): #TODO
    print('Passed Test 5:')
else:
    raise Exception('**Test 5 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> Column tgt_id fill NA with "0" 


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
    .assign(tgt_id = lambda df: df['tgt_id'].fillna('0')) 
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

### Test 6: show that all before 0's are now NA -- filtering on those idicies?
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
### <> Create project_id column 

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
#%%
### Review columns that will be used to filter rows:
print((
    df_12LL_before_BaseTable
    .loc[:, ['project_id', 'discharge_dt', 'last_home_visit', 'home_visits_num']]
    .dtypes
).to_string())



######################################
#%%###################################
### <> Filter out families that discharged before the current reporting year (using "discharge_dt"). 

### TODO ASKJOE: Filtering rows (parent-child combinations), not really families.

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

if (True): #TODO
    print('Passed Test 6:')
else:
    raise Exception('**Test 6 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> Filter out families without a home visit in the current fiscal year (using "last_home_visit"). 

### TODO ASKJOE: Filtering rows (parent-child combinations), not really families.

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

if (True): #TODO
    print('Passed Test 6:')
else:
    raise Exception('**Test 6 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



######################################
#%%###################################
### <> Filter out families with 0 home visits (using "home_visits_num"). 

### TODO ASKJOE: Filtering rows (parent-child combinations), not really families.

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

if (True): #TODO
    print('Passed Test 6:')
else:
    raise Exception('**Test 6 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



### <> NOTE: CREATE CFS ID File here.



######################################
#%%###################################
### <> Remove identifying variables 

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

if (True): #TODO
    print('Passed Test 5:')
else:
    raise Exception('**Test 5 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 



#%%
### <> NOTE: Previously, FW & LL combined before the following restructuring and joining.





### !>>> 
#%%##############################################!>>>
### >>> OLD  
#####################################################



#%%###################################

#%%
# inspect_df(df_12LL_BaseTable)

#%%
# inspect_df(df_12LL_ChildERInj)

#%%
# inspect_df(df_12LL_MaternalIns)

#%%
# inspect_df(df_12LL_WellChildVisits)






#%%##############################################!>>>
### >>> END 
#################################################!>>>




# %%
### TODO:
    ### - Remove duplciate rows.
    ### look up "logger"




