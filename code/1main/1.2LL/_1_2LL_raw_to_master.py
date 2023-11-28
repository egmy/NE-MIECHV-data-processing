



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

if __name__ == "__main__":
    from _1_2LL_RUNME import * 
    print('Imported "_1_2LL_RUNME"')

#%%##################################################
### COLUMN DEFINITIONS ###
#####################################################

#%%### df12LL_1: 'KU_BASETABLE'.
#%%### df12LL_2: 'KU_CHILDERINJ'.
#%%### df12LL_3: 'KU_MATERNALINS'.
#%%### df12LL_4: 'KU_WELLCHILDVISITS'.

#######################
#%%### df12LL_1: 'KU_BASETABLE'.
df12LL_1_col_detail = [
    ['site_id', 'string']
    ,['worker_id', 'string']
    ,['family_id', 'string']
    ,['tgt_id', 'string'] ### Maybe 'Int64'.
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
    ,['primary_id', 'string'] ### Maybe 'Int64'.
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
    ,['fob_id', 'string'] ### Maybe 'Int64'.
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
#%%### df12LL_1: 'KU_BASETABLE'.
df12LL_1_col_dtypes = {x[0]:x[1] for x in df12LL_1_col_detail}
print(df12LL_1_col_dtypes)
print(collections.Counter(list(df12LL_1_col_dtypes.values())))

#######################
#%%### df12LL_2: 'KU_CHILDERINJ'.
df12LL_2_col_detail = [
    ['family_id', 'string']
    ,['tgt_id', 'string'] ### Maybe 'Int64'.
    ,['funding', 'string']
    ,['reason', 'string']
    ,['date', 'datetime64[ns]']
]
#%%### df12LL_2: 'KU_CHILDERINJ'.
df12LL_2_col_dtypes = {x[0]:x[1] for x in df12LL_2_col_detail}
print(df12LL_2_col_dtypes)
print(collections.Counter(list(df12LL_2_col_dtypes.values())))

#######################
#%%### df12LL_3: 'KU_MATERNALINS'.
df12LL_3_col_detail = [
    ['family_id', 'string']
    ,['tgt_id', 'string'] ### Maybe 'Int64'.
    ,['funding', 'string']
    ,['insurance', 'Int64']
    ,['date', 'datetime64[ns]']
]
#%%### df12LL_3: 'KU_MATERNALINS'.
df12LL_3_col_dtypes = {x[0]:x[1] for x in df12LL_3_col_detail}
print(df12LL_3_col_dtypes)
print(collections.Counter(list(df12LL_3_col_dtypes.values())))

#######################
#%%### df12LL_4: 'KU_WELLCHILDVISITS'.
df12LL_4_col_detail = [
    ['family_id', 'string']
    ,['tgt_id', 'string'] ### Maybe 'Int64'.
    ,['funding', 'string']
    ,['date', 'datetime64[ns]']
]
#%%### df12LL_4: 'KU_WELLCHILDVISITS'.
df12LL_4_col_dtypes = {x[0]:x[1] for x in df12LL_4_col_detail}
print(df12LL_4_col_dtypes)
print(collections.Counter(list(df12LL_4_col_dtypes.values())))


#%%##################################################
### READ ###
#####################################################

#%%
### Performance benefit for reading in file to memory only once by creating an ExcelFile class object.
xlsx_df12LL = pd.ExcelFile(path_1_2LL_input_raw)

#%% 
### CHECK that all path_1_2LL_input_raw_sheets same as xlsx.sheet_names:
print(sorted(path_1_2LL_input_raw_sheets))
print(sorted(xlsx_df12LL.sheet_names))
sorted(path_1_2LL_input_raw_sheets) == sorted(xlsx_df12LL.sheet_names)

#%%###################################
### READ in all sheets as strings.

### Read in EVERYTHING as a string WITH pd.NA for empty cells:
df12LL_1_allstring = pd.read_excel(xlsx_df12LL, sheet_name=path_1_2LL_input_raw_sheets[0], keep_default_na=False, na_values=[''], dtype='string')# dtype=df12LL_1_col_dtypes)
df12LL_2_allstring = pd.read_excel(xlsx_df12LL, sheet_name=path_1_2LL_input_raw_sheets[1], keep_default_na=False, na_values=[''], dtype='string')# dtype=df12LL_2_col_dtypes)
df12LL_3_allstring = pd.read_excel(xlsx_df12LL, sheet_name=path_1_2LL_input_raw_sheets[2], keep_default_na=False, na_values=[''], dtype='string')# dtype=df12LL_3_col_dtypes)
df12LL_4_allstring = pd.read_excel(xlsx_df12LL, sheet_name=path_1_2LL_input_raw_sheets[3], keep_default_na=False, na_values=[''], dtype='string')# dtype=df12LL_4_col_dtypes)

### Backup:
df12LL_1 = df12LL_1_allstring.copy()
df12LL_2 = df12LL_2_allstring.copy()
df12LL_3 = df12LL_3_allstring.copy()
df12LL_4 = df12LL_4_allstring.copy()

#%%##################################################
### CLEAN ###
#####################################################

#%%###################################
### df12LL_1: 'KU_BASETABLE'.
df12LL_1 = (
    df12LL_1
    ###.astype(df12LL_1_col_dtypes)
    .pipe(fn_apply_dtypes, df12LL_1_col_dtypes)
)
inspect_df(df12LL_1)

#%%###################################
### df12LL_2: 'KU_CHILDERINJ'.
df12LL_2 = (
    df12LL_2
    ###.astype(df12LL_2_col_dtypes)
    .pipe(fn_apply_dtypes, df12LL_2_col_dtypes)
)
inspect_df(df12LL_2)

#%%###################################
### df12LL_3: 'KU_MATERNALINS'.
df12LL_3 = (
    df12LL_3
    ###.astype(df12LL_3_col_dtypes)
    .pipe(fn_apply_dtypes, df12LL_3_col_dtypes)
)
inspect_df(df12LL_3)

#%%###################################
### df12LL_4: 'KU_WELLCHILDVISITS'.
df12LL_4 = (
    df12LL_4
    ###.astype(df12LL_4_col_dtypes)
    .pipe(fn_apply_dtypes, df12LL_4_col_dtypes)
)
inspect_df(df12LL_4)









# %%
