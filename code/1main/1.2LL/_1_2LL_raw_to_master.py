



### Process for receiving data from Lincoln Lancaster County Health Department (LL):
    # 1. Original files transferred from external vendor (LL) to NE State, who then puts the files in folder "U:\SFTP".
    # 2. Then we organize the files in folder "Master (Files Used For Quarterly Reports)".
    # 3. Then we copy the files into: "U:\Working" >> "Tableau" >> year >> quarter >> "LL" folder (for example: "U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Y12Q4 (Oct 2022 - Sep 2023)\LLCHD").


### Copying over file from: "U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Y12Q4 (Oct 2022 - Sep 2023)\LLCHD":

### To here: "U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\0in\Y12Q4 (Oct 2022 - Sep 2023)"






### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.


#%%##################################################
### SETUP ###
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


#%%##################################################
### COLUMN DEFINITIONS ###
#####################################################

#%%### df_12LL_1: 'KU_BASETABLE'.
#%%### df_12LL_2: 'KU_CHILDERINJ'.
#%%### df_12LL_3: 'KU_MATERNALINS'.
#%%### df_12LL_4: 'KU_WELLCHILDVISITS'.

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


#%%##################################################
### READ ###
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

#%%##################################################
### CLEAN ###
#####################################################

#%%###################################
### df_12LL_2: 'KU_CHILDERINJ'.
df_12LL_ChildERInj = (
    df_12LL_allstring_2
    .applymap(lambda cell: cell.strip(), na_action='ignore').astype('string')
    .pipe(fn_find_and_replace_value_in_df, 'family_id', ['null'], pd.NA)
    .pipe(fn_apply_dtypes, dict_12LL_col_dtypes_2)
)

#%%###################################
### df_12LL_3: 'KU_MATERNALINS'.
df_12LL_MaternalIns = (
    df_12LL_allstring_3
    .applymap(lambda cell: cell.strip(), na_action='ignore').astype('string')
    .pipe(fn_find_and_replace_value_in_df, 'family_id', ['null'], pd.NA)
    .pipe(fn_apply_dtypes, dict_12LL_col_dtypes_3)
)

#%%###################################
### df_12LL_4: 'KU_WELLCHILDVISITS'.
df_12LL_WellChildVisits = (
    df_12LL_allstring_4
    .applymap(lambda cell: cell.strip(), na_action='ignore').astype('string')
    .pipe(fn_find_and_replace_value_in_df, 'family_id', ['null'], pd.NA)
    .pipe(fn_apply_dtypes, dict_12LL_col_dtypes_4)
)

#%%###################################
### df_12LL_1: 'KU_BASETABLE'.
# df_12LL_BaseTable = (
#     ### Raw table all read in as strings:
#     df_12LL_allstring_1
#     ### 
#     .pipe(fn_print_fstring_and_return_df, '-----\nStrip surrounding whitespace')
#     .applymap(lambda cell: cell.strip(), na_action='ignore').astype('string')
#     ###
#     .pipe(fn_print_fstring_and_return_df, '-----\nFind & replace "null" values:')
#     .pipe(fn_find_and_replace_value_in_df, 'family_id', ['null'], pd.NA)
#     ### 
#     .pipe(fn_print_fstring_and_return_df, '-----\nSet data types:')
#     .pipe(fn_apply_dtypes, dict_12LL_col_dtypes_1)
#     ####
#     .assign(site_id = 'll')
#     .pipe(fn_print_col_and_return_df, 'site_id', '-----\nColumn site_id should now be all "ll":')
#     ####
#     .pipe(fn_print_fstring_and_return_df, '-----\nColumn tgt_id before: Rows with NA that will be changed to "0":')
#     .pipe(fn_print_expression_and_return_df, (lambda df: df.loc[(lambda df: pd.isna(df['tgt_id'])), 'tgt_id'].index.tolist()))
#     .assign(tgt_id = lambda df: df['tgt_id'].fillna('0'))
#     .pipe(fn_print_fstring_and_return_df, 'Column tgt_id after: Rows with "0":')
#     .pipe(fn_print_expression_and_return_df, (lambda df: df.loc[(df['tgt_id'] == "0"), 'tgt_id'].index.tolist()))
#     ####
#     .assign(project_id = lambda df: df['site_id'] + df['family_id'] + '-' + df['tgt_id'])
#     .pipe(fn_print_col_and_return_df, 'project_id', '-----\nNew column:\n')
#     ####
#     .pipe(fn_print_expression_and_return_df, (lambda df: len(df)), '-----\nRemoving families discharged before current reporting year:\nDF Rows Before: ')
#     .pipe(fn_print_expression_and_return_df, (lambda df: df['discharge_dt'].min()), 'discharge_dt min Before: ')
#     .pipe(fn_print_expression_and_return_df, (lambda df: df['discharge_dt'].max()), 'discharge_dt max Before: ')
#     .query(f'discharge_dt >= @date_fy_start')
#     .pipe(fn_print_expression_and_return_df, (lambda df: len(df)), 'DF Rows After:  ')
#     .pipe(fn_print_expression_and_return_df, (lambda df: df['discharge_dt'].min()), 'discharge_dt min After:  ')
#     .pipe(fn_print_expression_and_return_df, (lambda df: df['discharge_dt'].max()), 'discharge_dt max After:  ')
#     ###
# )

    # .pipe(fn_print_expression_and_return_df, (lambda df: df), '')

#%%###################################
### df_12LL_1: 'KU_BASETABLE'.

df_12LL_before_BaseTable = df_12LL_allstring_1.copy()
df_12LL_after_BaseTable = df_12LL_allstring_1.copy()


######################################
#%%###################################
### Test that DFs the same:
len(df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)) == 0

#%%
### Make change:
print('Strip surrounding whitespace')
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .applymap(lambda cell: cell.strip(), na_action='ignore').astype('string')
)

#%%
### See differences:
df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)

#%%
### Test change:
if (
    ### Test 1: Number of NA has not increased.
    (df_12LL_before_BaseTable.isna().sum().sum() == df_12LL_after_BaseTable.isna().sum().sum())
):
    print('tests passed!')
    print('Test 1: Number of NA has not increased.')
else:
    raise Exception

#%%
### Make DFs the same:
df_12LL_after_BaseTable = df_12LL_before_BaseTable


######################################
#%%###################################
### Test that DFs the same:
len(df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)) == 0

#%%
### Make change:
print('Strip surrounding whitespace')
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    .applymap(lambda cell: cell.strip(), na_action='ignore').astype('string')
)

#%%
### See differences:
df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)

#%%
### Test change:
if (
    ### Test 1: Number of NA has not increased.
    (df_12LL_before_BaseTable.isna().sum().sum() == df_12LL_after_BaseTable.isna().sum().sum())
):
    print('tests passed!')
    print('Test 1: Number of NA has not increased.')
else:
    raise Exception

#%%
### Make DFs the same:
df_12LL_after_BaseTable = df_12LL_before_BaseTable


#%%###################################


#%%
# df_12LL_BaseTable.project_id


# #%%
# print(df_12LL_allstring_1['tgt_id'].value_counts(dropna=False).to_string())
# #%%
# print(df_12LL_BaseTable['tgt_id'].value_counts(dropna=False).to_string())
#%%
col_to_review = 'tgt_id'
compare_col(
    (df_12LL_allstring_1
        .loc[lambda df: pd.isna(df[col_to_review])]
    )
    ,(df_12LL_BaseTable
        .query(f'`{col_to_review}` == "0"')
    )
    ,col_to_review ,'value_counts'
)

#%%###################################

#%%
# inspect_df(df_12LL_BaseTable)

#%%
# inspect_df(df_12LL_ChildERInj)

#%%
# inspect_df(df_12LL_MaternalIns)

#%%
# inspect_df(df_12LL_WellChildVisits)






# %%
### TODO:
### - Remove duplciate rows.
### look up "logger"



