
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### Instructions for how to get into environment & how to edit/run code files.

#%%##################################################
### PACKAGES ###
#####################################################

### pip list ### See packages installed in this virtual environment that can be imported.

import pandas as pd
from pathlib import Path
import numpy as np

# import matplotlib.pyplot as plt

### Test that pandas imported:
print('pandas verion: ' + pd.__version__)

#%%##################################################
### Section to Adjust ###
#####################################################

### Data Source for 2nd Tableau file:
### path_2_data_source_file = 'U:\\Working\\Tableau\\Y12 (Oct 2022 - Sept 2023)\\Child Activity Master File.xlsx' ### old.
### local:
path_2_data_source_file = Path('U:\\Working\\nebraska_miechv_coded_data_source\\data\\01 original\Y12Q1 (Oct 2022 - Dec 2023)\\Child Activity Master File.xlsx')

path_2_data_source_sheets = [
    'Project ID',  # 1
    'ER Injury',  # 3
    'Family Wise',  # 4
    'LLCHD',  # 5
    'Well Child'  # 6
]

### Output for 2nd Tableau file:
path_2_output_dir = Path('U:\\Working\\nebraska_miechv_coded_data_source\\data\\04 output')
path_2_output = Path(path_2_output_dir, 'Child Activity Master File from Excel on NE Server.csv')

#%% test write
# with open(Path(path_2_output_dir, 'test.txt'), 'w') as f:
#     f.write('testtest test  test')


#%%### df2_1: 'Project ID'.
#%%### df2_2: 'ER Injury'.
#%%### df2_3: 'Family Wise'.
#%%### df2_4: 'LLCHD'.
#%%### df2_5: 'Well Child'. 

#%%##################################################
### Utility Functions ###
#####################################################

def inspect_df(df):
    print(f'Rows: {len(df)}')
    print(f'Columns: {len(df.columns)}')
    print('\n')
    print(df.describe)
    print('\n')
    print(df.info())
    print('\n')
    display(df)

#%%##################################################
### READ ###
#####################################################

### https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html
### https://pandas.pydata.org/docs/user_guide/io.html#reading-excel-files 

#%%
### Performance benefit for reading in file to memory only once by creating an ExcelFile class object.
xlsx = pd.ExcelFile(path_2_data_source_file)

#%% 
### CHECK that all path_2_data_source_sheets same as xlsx.sheet_names (different order ok):
print(sorted(path_2_data_source_sheets))
print(sorted(xlsx.sheet_names))
sorted(path_2_data_source_sheets) == sorted(xlsx.sheet_names)

#%%
### READ all sheets:
df2_1 = pd.read_excel(xlsx, sheet_name=path_2_data_source_sheets[0])
df2_2 = pd.read_excel(xlsx, sheet_name=path_2_data_source_sheets[1])
df2_3 = pd.read_excel(xlsx, sheet_name=path_2_data_source_sheets[2])
df2_4 = pd.read_excel(xlsx, sheet_name=path_2_data_source_sheets[3])
df2_5 = pd.read_excel(xlsx, sheet_name=path_2_data_source_sheets[4])

# df2_1 = pd.read_excel(path_2_data_source_file, sheet_name=path_2_data_source_sheets[0])
# df2_2 = pd.read_excel(path_2_data_source_file, sheet_name=path_2_data_source_sheets[1])
# df2_3 = pd.read_excel(path_2_data_source_file, sheet_name=path_2_data_source_sheets[2])
# df2_4 = pd.read_excel(path_2_data_source_file, sheet_name=path_2_data_source_sheets[3])
# df2_5 = pd.read_excel(path_2_data_source_file, sheet_name=path_2_data_source_sheets[4])

#%%
### Function to add row to DF if no rows. Than map over list/dictionary of df's.

### TODO

#%% 
### CHECK that there's data in each df (that are not empty):

### TODO

#%%
# inspect_df(df2_1)
#%%
# inspect_df(df2_2)
#%%
# inspect_df(df2_3)
#%%
# inspect_df(df2_4)
#%%
# inspect_df(df2_5)

#%%##################################################
### Rename Columns ###
#####################################################

#%%
# df2_1 = df2_1.add_suffix(' (Project ID)')
# df2_2 = df2_2.add_suffix(' (ER Injury)')
# df2_3 = df2_3.add_suffix(' (Family Wise)')
# df2_4 = df2_4.add_suffix(' (LLCHD)')
# df2_5 = df2_5.add_suffix(' (Well Child)')

#%%### df2_1: 'Project ID'.
#%%### df2_2: 'ER Injury'.
#%%### df2_3: 'Family Wise'.
#%%### df2_4: 'LLCHD'.
#%%### df2_5: 'Well Child'.

#######################
#%%### df2_1: 'Project ID'.
[*df2_1]
#%%### df2_1: 'Project ID'.
df2_1_colnames = {
    'project_id': 'Project Id', 
    'year': 'Year', 
    'quarter': 'Quarter'
}
#%%### df2_1: 'Project ID'.
df2_1 = df2_1.rename(columns=df2_1_colnames)
[*df2_1]

#######################
#%%### df2_2: 'ER Injury'.
[*df2_2]
#%%### df2_2: 'ER Injury'.
df2_2_colnames = {
    'Project ID': 'Project ID (ER Injury)',
    'year': 'year (ER Injury)',
    'quarter': 'quarter (ER Injury)',
    'agency': 'agency (ER Injury)',
    'FAMILY NUMBER': 'FAMILY NUMBER (ER Injury)',
    'ChildNumber': 'ChildNumber (ER Injury)',
    'funding': 'funding (ER Injury)',
    'IncidentDate': 'Incident Date'
    ###'IncidentDate2': 'IncidentDate2'
}
#%%### df2_2: 'ER Injury'.
df2_2 = df2_2.rename(columns=df2_2_colnames)
[*df2_2]

#######################
#%%### df2_3: 'Family Wise'.
[*df2_3]
#%%### df2_3: 'Family Wise'.
df2_3_colnames = {
    ### 'Project ID': 'Project ID',
    'year': 'year (Family Wise)',
    'quarter': 'quarter (Family Wise)',
    'agency': 'Agency',
    'FAMILY NUMBER': 'Family Number',
    'ChildNumber': 'Child Number',
    'MinOfHVDate': 'Min Of HV Date',
    'TERMINATION DATE': 'Termination Date',
    'TERMINATION STATUS': 'Termination Status',
    'MaxofHVDate': 'Maxof HV Date',
    'TGT DOB-CR': 'Tgt Dob-Cr',
    ### 'EDC Date': 'EDC Date',
    'MinHVDateBFYes': 'Min HV Date BF Yes',
    'BreastFeeding': 'Breast Feeding', ### 'Breast Feeding (Count)'
    'MinOfDateDiscontinueBF': 'Min Of Date Discontinue BF',
    'SleepOnBack': 'Sleep On Back',
    'CoSleeping': 'Co Sleeping',
    'SoftBedding': 'Soft Bedding',
    'SafeSleepDate': 'Safe Sleep Date',
    'SafeSleepPartialDate': 'Safe Sleep Partial Date',
    ### 'ASQ9MoDate': 'ASQ9MoDate',
    ### 'ASQ9MoTiming': 'ASQ9MoTiming',
    ### 'ASQ9MoCom': 'ASQ9MoCom',
    ### 'ASQ9MoGross': 'ASQ9MoGross',
    ### 'ASQ9MoFine': 'ASQ9MoFine',
    ### 'ASQ9MoProblem': 'ASQ9MoProblem',
    ### 'ASQ9MoPersonal': 'ASQ9MoPersonal',
    ### 'ASQ18MoDate': 'ASQ18MoDate',
    ### 'ASQ18MoTiming': 'ASQ18MoTiming',
    ### 'ASQ18MoCom': 'ASQ18MoCom',
    ### 'ASQ18MoGross': 'ASQ18MoGross',
    ### 'ASQ18MoFine': 'ASQ18MoFine',
    ### 'ASQ18MoProblem': 'ASQ18MoProblem',
    ### 'ASQ18MoPersonal': 'ASQ18MoPersonal',
    ### 'ASQ24MoDate': 'ASQ24MoDate',
    ### 'ASQ24MoTiming': 'ASQ24MoTiming',
    ### 'ASQ24MoCom': 'ASQ24MoCom',
    ### 'ASQ24MoGross': 'ASQ24MoGross',
    ### 'ASQ24MoFine': 'ASQ24MoFine',
    ### 'ASQ24MoProblem': 'ASQ24MoProblem',
    ### 'ASQ24MoPersonal': 'ASQ24MoPersonal',
    ### 'ASQ30MoDate': 'ASQ30MoDate',
    ### 'ASQ30MoTiming': 'ASQ30MoTiming',
    ### 'ASQ30MoCom': 'ASQ30MoCom',
    ### 'ASQ30MoGross': 'ASQ30MoGross',
    ### 'ASQ30MoFine': 'ASQ30MoFine',
    ### 'ASQ30MoProblem': 'ASQ30MoProblem',
    ### 'ASQ30MoPersonal': 'ASQ30MoPersonal',
    'MaxEarlyLiteracyDate': 'Max Early Literacy Date',
    'ReadTellStorySing': 'Read Tell Story Sing',
    'BehaviorDenom': 'Behavior Denom',
    'BehaviorNumer': 'Behavior Numer',
    'HomeVisitsPrental': 'Home Visits Prental',
    'HomeVisitsTotal': 'Home Visits Total',
    'TGTInsureChangeDate': 'TGT Insure Change Date',
    'CHINSPrimaryIns': 'CHINS Primary Ins',
    'MOB LANGUAGE': 'Mob Language',
    'ChildMedCareSource': 'Child Med Care Source',
    'ChildDentalCareSource': 'Child Dental Care Source',
    'NTChildDevDelay': 'NT Child Dev Delay',
    'NTChildLowAchievement': 'NT Child Low Achievement',
    'HistoryInterWelfareChild': 'History Inter Welfare Child',
    ### 'ASQ9MoRefDate': 'ASQ9MoRefDate',
    ### 'ASQ9MoRefLocation': 'ASQ9MoRefLocation',
    ### 'ASQ9MoRefEIDate': 'ASQ9MoRefEIDate',
    ### 'ASQ9MoRefCSDate': 'ASQ9MoRefCSDate',
    ### 'ASQ18MoRefDate': 'ASQ18MoRefDate',
    ### 'ASQ18MoRefLocation': 'ASQ18MoRefLocation',
    ### 'ASQ18MoEIDate': 'ASQ18MoEIDate',
    ### 'ASQ18MoCSDate': 'ASQ18MoCSDate',
    ### 'ASQ24MoRefDate': 'ASQ24MoRefDate',
    ### 'ASQ24MoRefLocation': 'ASQ24MoRefLocation',
    ### 'ASQ24MoEIDate': 'ASQ24MoEIDate',
    ### 'ASQ24MoCSDate': 'ASQ24MoCSDate',
    ### 'ASQ30MoRefDate': 'ASQ30MoRefDate',
    ### 'ASQ30MoRefLocation': 'ASQ30MoRefLocation',
    ### 'ASQ30MoEIDate': 'ASQ30MoEIDate',
    ### 'ASQ30MoCSDate': 'ASQ30MoCSDate',
    ### 'TGT Gender': 'TGT Gender',
    'TGT ETHNICITY': 'Tgt Ethnicity',
    'TGTRaceWhite': 'TGT Race White',
    'TGTRaceBlack': 'TGT Race Black',
    'TGTRaceIndianAlaskan': 'TGT Race Indian Alaskan',
    'TGTRaceAsian': 'TGT Race Asian',
    'TGTRaceHawaiianPacific': 'TGT Race Hawaiian Pacific',
    'TGTRaceOther': 'TGT Race Other',
    ### 'Adaptation': 'Adaptation',
    '12 - 09 ASQ3_WhyNotDone': '12 - 09 ASQ3 WhyNotDone',
    '12 - 18 ASQ3_WhyNotDone': '12 - 18 ASQ3 WhyNotDone',
    '12 - 24 ASQ3_WhyNotDone': '12 - 24 ASQ3 WhyNotDone',
    '12 - 30 ASQ3_WhyNotDone': '12 - 30 ASQ3 WhyNotDone',
    'GESTATIONAL AGE': 'Gestational Age',
    'need_exclusion4': 'Need Exclusion4'
    ### 'ZIP Code': 'ZIP Code'
}
#%%### df2_3: 'Family Wise'.
df2_3 = df2_3.rename(columns=df2_3_colnames)
[*df2_3]

#######################
#%%### df2_4: 'LLCHD'.
[*df2_4]
#%%### df2_4: 'LLCHD'.
df2_4_colnames = {
    'project_id': 'project id (LLCHD)',
    'year': 'year (LLCHD)',
    'quarter': 'quarter (LLCHD)',
    'site_id': 'Site Id',
    'family_id': 'Family Id',
    'tgt_id': 'Tgt Id',
    'tgt_dob': 'Tgt Dob',
    'tgt_gender': 'Tgt Gender',
    'tgt_ethnicity': 'Tgt Ethnicity1',
    'tgt_race_indian': 'Tgt Race Indian',
    'tgt_race_asian': 'Tgt Race Asian',
    'tgt_race_black': 'Tgt Race Black',
    'tgt_race_hawaiian': 'Tgt Race Hawaiian',
    'tgt_race_white': 'Tgt Race White',
    'tgt_race_other': 'Tgt Race Other',
    'tgt_GestationalAge': 'tgt GestationalAge',
    'tgt_medical_home': 'Tgt Medical Home',
    'tgt_medical_home_dt': 'Tgt Medical Home Dt',
    'tgt_dental_home': 'Tgt Dental Home',
    'tgt_dental_home_dt': 'Tgt Dental Home Dt',
    'dt_edc': 'Dt Edc',
    'enroll_dt': 'Enroll Dt',
    'enroll_preg_status': 'Enroll Preg Status',
    'current_pregnancy': 'Current Pregnancy',
    'discharge_reason': 'Discharge Reason',
    'discharge_dt': 'Discharge Dt',
    'last_home_visit': '',
    'home_visits_num': '',
    'home_visits_pre': '',
    'home_visits_post': '',
    'home_visits_person': '',
    'home_visits_virtual': '',
    'funding': '',
    'c_fundingdate': '',
    'p_funding': '',
    'p_fundingdate': '',
    'primary_id': '',
    'primary_relation': '',
    'mob_id': '',
    'mob_dob': '',
    'mob_gender': '',
    'mob_ethnicity': '',
    'mob_race': '',
    'mob_race_indian': '',
    'mob_race_asian': '',
    'mob_race_black': '',
    'mob_race_hawaiian': '',
    'mob_race_white': '',
    'mob_race_other': '',
    'mob_marital_status': '',
    'mob_living_arrangement': '',
    'mob_living_arrangement_dt': '',
    'fob_id': '',
    'fob_dob': '',
    'fob_gender': '',
    'fob_ethnicity': '',
    'fob_race': '',
    'fob_race_indian': '',
    'fob_race_asian': '',
    'fob_race_black': '',
    'fob_race_hawaiian': '',
    'fob_race_white': '',
    'fob_race_other': '',
    'fob_marital_status': '',
    'fob_living_arrangement': '',
    'fob_living_arrangement_dt': '',
    'fob_edu_dt': '',
    'fob_edu': '',
    'fob_employ_dt': '',
    'fob_employ': '',
    'fob_involved': '',
    'zip': '',
    'fob_visits': '',
    'household_income': '',
    'household_size': '',
    'mcafss_income_dt': '',
    'mcafss_income': '',
    'mcafss_edu_dt1': '',
    'mcafss_edu1': '',
    'mcafss_edu1_enroll': '',
    'mcafss_edu1_enroll_dt': '',
    'mcafss_edu1_prog': '',
    'mcafss_edu_dt2': '',
    'mcafss_edu2': '',
    'mcafss_edu2_enroll': '',
    'mcafss_edu2_enroll_dt': '',
    'mcafss_edu2_prog': '',
    'mcafss_employ_dt': '',
    'mcafss_employ': '',
    'language_primary': '',
    'priority_child_welfare': '',
    'priority_substance_abuse': '',
    'priority_tobacco_use': '',
    'priority_low_student': '',
    'priority_develop_delays': '',
    'priority_military': '',
    'uncope_dt': '',
    'uncope_score': '',
    'substance_abuse_ref_dt': '',
    'tobacco_use': '',
    'tobacco_use_dt': '',
    'tobacco_ref_dt': '',
    'safe_sleep_yes': '',
    'safe_sleep_yes_dt': '',
    'cheeers_date': '',
    'early_language': '',
    'early_language_dt': '',
    'asq3_dt_9mm': '',
    'asq3_timing_9mm': '',
    'asq3_comm_9mm': '',
    'asq3_gross_9mm': '',
    'asq3_fine_9mm': '',
    'asq3_problem_9mm': '',
    'asq3_social_9mm': '',
    'asq3_feedback_9mm': '',
    'asq3_referral_9mm': '',
    'asq3_dt_18mm': '',
    'asq3_timing_18mm': '',
    'asq3_comm_18mm': '',
    'asq3_gross_18mm': '',
    'asq3_fine_18mm': '',
    'asq3_problem_18mm': '',
    'asq3_social_18mm': '',
    'asq3_feedback_18mm': '',
    'asq3_referral_18mm': '',
    'asq3_dt_24mm': '',
    'asq3_timing_24mm': '',
    'asq3_comm_24mm': '',
    'asq3_gross_24mm': '',
    'asq3_fine_24mm': '',
    'asq3_problem_24mm': '',
    'asq3_social_24mm': '',
    'asq3_feedback_24mm': '',
    'asq3_referral_24mm': '',
    'asq3_dt_30mm': '',
    'asq3_timing_30mm': '',
    'asq3_comm_30mm': '',
    'asq3_gross_30mm': '',
    'asq3_fine_30mm': '',
    'asq3_problem_30mm': '',
    'asq3_social_30mm': '',
    'asq3_feedback_30mm': '',
    'asq3_referral_30mm': '',
    'behavioral_concerns': '',
    'ipv_screen': '',
    'ipv_screen_dt': '',
    'ipv_referral_dt': '',
    'prim_care_dt': '',
    'cesd_dt': '',
    'cesd_score': '',
    'ment_hlth_ref_dt': '',
    'lsp_bf_initiation_dt': '',
    'lsp_bf_discon_dt': '',
    'hlth_insure_mob': '',
    'hlth_insure_mob_dt': '',
    'hlth_insure_tgt': '',
    'hlth_insure_tgt_dt': '',
    'last_well_child_visit': '',
    'hlth_insure_fob': '',
    'hlth_insure_fob_dt': '',
    'need_exclusion1': '',
    'need_exclusion2': '',
    'need_exclusion3': '',
    'need_exclusion4': '',
    'need_exclusion5': '',
    'need_exclusion6': '',
    'Has_ChildWelfareAdaptation': ''
}
#%%### df2_4: 'LLCHD'.
df2_4 = df2_4.rename(columns=df2_4_colnames)
[*df2_4]

# #######################
# #%%### df2_5: 'Well Child'.
# [*df2_5]
# #%%### df2_5: 'Well Child'.
# df2_5_colnames = {
# }
# #%%### df2_5: 'Well Child'.
# df2_5 = df2_5.rename(columns=df2_5_colnames)
# [*df2_5]






#%%##################################################
### JOIN ###
#####################################################

### https://pandas.pydata.org/docs/reference/api/pandas.merge.html

### TO DO: add validation.

#%%
df2 = (
    pd.merge(
        df2_1, ### 'Project ID'.
        df2_2, ### 'ER Injury'.
        how='left', 
        left_on=['Project Id','Year','Quarter'], 
        right_on=['Project ID (ER Injury)','year (ER Injury)','quarter (ER Injury)'], 
        # suffixes=(' (Project ID)', ' (ER Injury)'),
        indicator='LJ_df2_2ER'
    ).merge(
        df2_3, ### 'Family Wise'.
        how='left', 
        left_on=['Project Id','Year','Quarter'], 
        right_on=['Project ID (Family Wise)','year (Family Wise)','quarter (Family Wise)'], 
        # suffixes=(' (Project ID)', ' (Family Wise)'),
        indicator='LJ_df2_3FW'
    ).merge(
        df2_4, ### 'LLCHD'.
        how='left', 
        left_on=['Project Id','Year','Quarter'], 
        right_on=['project_id (LLCHD)','year (LLCHD)','quarter (LLCHD)'], 
        # suffixes=(' (Project ID)', ' (LLCHD)'),
        indicator='LJ_df2_4LL'
    ).merge(
        df2_5, ### 'Well Child'.
        how='left', 
        left_on=['Project Id','Year','Quarter'], 
        right_on=['ProjectID (Well Child)','year (Well Child)','quarter (Well Child)'], 
        # suffixes=(' (Project ID)', ' (Well Child)'),
        indicator='LJ_df2_5WC'
    ) 
)

#%%##################################################
### Set Data Types ###
#####################################################

### Tableau does this automatically; will need to here too.

#%%##################################################
### RECODE / Creating Columns ###
#####################################################

df2_edits1 = df2.copy()  ### Make a deep-ish copy of the DF's Data. Does NOT copy embedded mutable objects.

### Not needed?
    ### [Number of Records]

#%%##################################################
### DUPLICATING

df2_edits1['_C18 ASQ 18 Mo Ref Location'] = df2_edits1['ASQ18MoRefLocation']
    ### [ASQ18MoRefLocation]

df2_edits1['_C18 ASQ 24 Mo Ref Location'] = df2_edits1['ASQ24MoRefLocation']
    ### [ASQ24MoRefLocation]

df2_edits1['_C18 ASQ 30 Mo Ref Location'] = df2_edits1['ASQ30MoRefLocation']
    ### [ASQ30MoRefLocation]

df2_edits1['_C18 ASQ 9 Mo Ref Location'] = df2_edits1['ASQ9MoRefLocation']
    ### [ASQ9MoRefLocation]

### TO DO: LLCHD needs to provide a safe sleep partial date.
df2_edits1['_C7 Safe Sleep Partial Date'] = df2_edits1['SafeSleepPartialDate']
    ### ### IFNULL(
    ### [SafeSleepPartialDate]  ### FW
    ### ### ,[Safe Sleep Yes Dt]) ### LLCHD needs to provide a safe sleep partial date
    ### ### END

#%%##################################################
### COALESCING

df2_edits1['_Agency'] = df2_edits1['Agency'].combine_first(df2_edits1['Site Id'])
    ### IFNULL([Agency],[Site Id])

df2_edits1['_C11 Literacy'] = df2_edits1['Max Early Literacy Date'].combine_first(df2_edits1['Early Language Dt'])
    ### IFNULL([Max Early Literacy Date],[Early Language Dt])

df2_edits1['_C12 ASQ 18 Mo Date'] = df2_edits1['ASQ18MoDate'].combine_first(df2_edits1['Asq3 Dt 18Mm'])
    ### IFNULL([ASQ18MoDate],[Asq3 Dt 18Mm])

df2_edits1['_C12 ASQ 24 Mo Date'] = df2_edits1['ASQ24MoDate'].combine_first(df2_edits1['Asq3 Dt 24Mm'])
    ### IFNULL([ASQ24MoDate],[Asq3 Dt 24Mm])

df2_edits1['_C12 ASQ 30 Mo Date'] = df2_edits1['ASQ30MoDate'].combine_first(df2_edits1['Asq3 Dt 30Mm'])
    ### IFNULL([ASQ30MoDate],[Asq3 Dt 30Mm])

df2_edits1['_C12 ASQ 9 Mo Date'] = df2_edits1['Asq3 Dt 9Mm'].combine_first(df2_edits1['ASQ9MoDate'])
    ### IFNULL([Asq3 Dt 9Mm],[ASQ9MoDate])

df2_edits1['_C18 ASQ 18 Mo Referral Date'] = df2_edits1['asq3_referral_18mm'].combine_first(df2_edits1['ASQ18MoRefDate'])
    ### IFNULL([asq3_referral_18mm],[ASQ18MoRefDate])

df2_edits1['_C18 ASQ 24 Mo Referral Date'] = df2_edits1['ASQ24MoRefDate'].combine_first(df2_edits1['asq3_referral_24mm'])
    ### IFNULL([ASQ24MoRefDate],[asq3_referral_24mm])

df2_edits1['_C18 ASQ 30 Mo Referral Date'] = df2_edits1['ASQ30MoRefDate'].combine_first(df2_edits1['asq3_referral_30mm'])
    ### IFNULL([ASQ30MoRefDate],[asq3_referral_30mm])

df2_edits1['_C18 ASQ 9 Mo Referral Date'] = df2_edits1['asq3_referral_9mm'].combine_first(df2_edits1['ASQ9MoRefDate'])
    ### IFNULL([asq3_referral_9mm],[ASQ9MoRefDate])

df2_edits1['_C2 BF Discontinuation Date'] = df2_edits1['Min Of Date Discontinue BF'].combine_first(df2_edits1['Lsp Bf Discon Dt'])
    ### IFNULL([Min Of Date Discontinue BF],[Lsp Bf Discon Dt])

df2_edits1['_C2 BF Initiation Date'] = df2_edits1['Min HV Date BF Yes'].combine_first(df2_edits1['Lsp Bf Initiation Dt'])
    ### IFNULL([Min HV Date BF Yes],[Lsp Bf Initiation Dt])

df2_edits1['_C7 Safe Sleep Date'] = df2_edits1['Safe Sleep Date'].combine_first(df2_edits1['Safe Sleep Yes Dt'])
    ### IFNULL([Safe Sleep Date],[Safe Sleep Yes Dt])

df2_edits1['_Discharge Date'] = df2_edits1['Discharge Dt'].combine_first(df2_edits1['Termination Date'])
    ### IFNULL([Discharge Dt],[Termination Date])

df2_edits1['_Enroll'] = df2_edits1['Enroll Dt'].combine_first(df2_edits1['Min Of HV Date'])
    ### IFNULL([Enroll Dt],[Min Of HV Date])

df2_edits1['_Family Number'] = df2_edits1['Family Id'].combine_first(df2_edits1['Family Number'])
    ### IFNULL([Family Id],[Family Number])

df2_edits1['_Max HV Date'] = df2_edits1['MaxofHVDate'].combine_first(df2_edits1['last_home_visit'])
    ### IFNULL([MaxofHVDate],[last_home_visit])

df2_edits1['_TGT Gestational Age'] = df2_edits1['tgt_GestationalAge'].combine_first(df2_edits1['_FW Gestation Age Recode'])
    ### IFNULL([tgt_GestationalAge],[_FW Gestation Age Recode])

df2_edits1['_C12 ASQ 18 Mo Communication'] = df2_edits1['Asq3 Comm 18Mm'].combine_first(df2_edits1['ASQ18MoCom'])
    ### IFNULL([Asq3 Comm 18Mm],[ASQ18MoCom])

df2_edits1['_C12 ASQ 18 Mo Fine Motor'] = df2_edits1['ASQ18MoFine'].combine_first(df2_edits1['Asq3 Fine 18Mm'])
    ### IFNULL([ASQ18MoFine],[Asq3 Fine 18Mm])

df2_edits1['_C12 ASQ 18 Mo Gross Motor'] = df2_edits1['ASQ18MoGross'].combine_first(df2_edits1['Asq3 Gross 18Mm'])
    ### IFNULL([ASQ18MoGross],[Asq3 Gross 18Mm])

df2_edits1['_C12 ASQ 18 Mo Personal Social'] = df2_edits1['ASQ18MoPersonal'].combine_first(df2_edits1['Asq3 Social 18Mm'])
    ### IFNULL([ASQ18MoPersonal],[Asq3 Social 18Mm])

df2_edits1['_C12 ASQ 18 Mo Problem Solving'] = df2_edits1['ASQ18MoProblem'].combine_first(df2_edits1['Asq3 Problem 18Mm'])
    ### IFNULL([ASQ18MoProblem],[Asq3 Problem 18Mm])

df2_edits1['_C12 ASQ 24 Mo Communication'] = df2_edits1['Asq3 Comm 24Mm'].combine_first(df2_edits1['ASQ24MoCom'])
    ### IFNULL([Asq3 Comm 24Mm],[ASQ24MoCom])

df2_edits1['_C12 ASQ 24 Mo Fine Motor'] = df2_edits1['ASQ24MoFine'].combine_first(df2_edits1['Asq3 Fine 24Mm'])
    ### IFNULL([ASQ24MoFine],[Asq3 Fine 24Mm])

df2_edits1['_C12 ASQ 24 Mo Gross Motor'] = df2_edits1['ASQ24MoGross'].combine_first(df2_edits1['Asq3 Gross 24Mm'])
    ### IFNULL([ASQ24MoGross],[Asq3 Gross 24Mm])

df2_edits1['_C12 ASQ 24 Mo Personal Social'] = df2_edits1['ASQ24MoPersonal'].combine_first(df2_edits1['Asq3 Social 24Mm'])
    ### IFNULL([ASQ24MoPersonal],[Asq3 Social 24Mm])

df2_edits1['_C12 ASQ 24 Mo Problem Solving'] = df2_edits1['ASQ24MoProblem'].combine_first(df2_edits1['Asq3 Problem 24Mm'])
    ### IFNULL([ASQ24MoProblem],[Asq3 Problem 24Mm])

df2_edits1['_C12 ASQ 30 Mo Communication'] = df2_edits1['ASQ30MoCom'].combine_first(df2_edits1['Asq3 Comm 30Mm'])
    ### IFNULL([ASQ30MoCom],[Asq3 Comm 30Mm])

df2_edits1['_C12 ASQ 30 Mo Fine Motor'] = df2_edits1['ASQ30MoFine'].combine_first(df2_edits1['Asq3 Fine 30Mm'])
    ### IFNULL([ASQ30MoFine],[Asq3 Fine 30Mm])

df2_edits1['_C12 ASQ 30 Mo Gross Motor'] = df2_edits1['ASQ30MoGross'].combine_first(df2_edits1['Asq3 Gross 30Mm'])
    ### IFNULL([ASQ30MoGross],[Asq3 Gross 30Mm])

df2_edits1['_C12 ASQ 30 Mo Personal Social'] = df2_edits1['ASQ30MoPersonal'].combine_first(df2_edits1['Asq3 Social 30Mm'])
    ### IFNULL([ASQ30MoPersonal],[Asq3 Social 30Mm])

df2_edits1['_C12 ASQ 30 Mo Problem Solving'] = df2_edits1['ASQ30MoProblem'].combine_first(df2_edits1['Asq3 Problem 30Mm'])
    ### IFNULL([ASQ30MoProblem],[Asq3 Problem 30Mm])

df2_edits1['_C12 ASQ 9 Mo Communication'] = df2_edits1['ASQ9MoCom'].combine_first(df2_edits1['Asq3 Comm 9Mm'])
    ### IFNULL([ASQ9MoCom],[Asq3 Comm 9Mm])

df2_edits1['_C12 ASQ 9 Mo Fine Motor'] = df2_edits1['ASQ9MoFine'].combine_first(df2_edits1['Asq3 Fine 9Mm'])
    ### IFNULL([ASQ9MoFine],[Asq3 Fine 9Mm])

df2_edits1['_C12 ASQ 9 Mo Gross Motor'] = df2_edits1['ASQ9MoGross'].combine_first(df2_edits1['Asq3 Gross 9Mm'])
    ### IFNULL([ASQ9MoGross],[Asq3 Gross 9Mm])

df2_edits1['_C12 ASQ 9 Mo Personal Social'] = df2_edits1['ASQ9MoPersonal'].combine_first(df2_edits1['Asq3 Social 9Mm'])
    ### IFNULL([ASQ9MoPersonal],[Asq3 Social 9Mm])

df2_edits1['_C12 ASQ 9 Mo Problem Solving'] = df2_edits1['ASQ9MoProblem'].combine_first(df2_edits1['Asq3 Problem 9Mm'])
    ### IFNULL([ASQ9MoProblem],[Asq3 Problem 9Mm])

df2_edits1['_C13 Behavioral Concerns Asked'] = df2_edits1['Behavior Numer'].combine_first(df2_edits1['Behavioral Concerns'])
    ### IFNULL([Behavior Numer],[Behavioral Concerns])

df2_edits1['_C13 Behavioral Concerns Visits'] = df2_edits1['Behavior Denom'].combine_first(df2_edits1['home_visits_post'])
    ### IFNULL([Behavior Denom],[home_visits_post])

df2_edits1['_T16 Total Home Visits'] = df2_edits1['HomeVisitsTotal'].combine_first(df2_edits1['Home Visits Num'])
    ### IFNULL([HomeVisitsTotal],[Home Visits Num])

df2_edits1['_TGT Number'] = df2_edits1['Tgt Id'].combine_first(df2_edits1['Child Number'])
    ### IFNULL([Tgt Id],[Child Number])
    ### In tableau on left name shows as "_TGT Number (Count)" -- but not when you try to edit. Not sure how exports.

### If variables are already dtypes "datetime64", then this should be a date too:
df2_edits1['_T20 TGT Insurance Date'] = df2_edits1['TGT Insure Change Date'].combine_first(df2_edits1['Hlth Insure Tgt Dt'])
    ### DATE(IFNULL([TGT Insure Change Date],[Hlth Insure Tgt Dt]))

#%%##################################################
### DATE CALCULATIONS

### These calculations assume all date variables are dtype "datetime64".

df2_edits1['_C18 ASQ 18 Mo 30 Day Date'] = df2_edits1['_C12 ASQ 18 Mo Date'] + pd.Timedelta(days=30) 
    ### DATE(DATEADD('day',30,[_C12 ASQ 18 Mo Date])) 

df2_edits1['_C18 ASQ 18 Mo 45 Day Date'] = df2_edits1['_C12 ASQ 18 Mo Date'] + pd.Timedelta(days=45) 
    ### DATE(DATEADD('day',45,[_C12 ASQ 18 Mo Date])) 

df2_edits1['_C18 ASQ 24 Mo 30 Day Date'] = df2_edits1['_C12 ASQ 24 Mo Date'] + pd.Timedelta(days=30) 
    ### DATE(DATEADD('day',30,[_C12 ASQ 24 Mo Date])) 

df2_edits1['_C18 ASQ 24 Mo 45 Day Date'] = df2_edits1['_C12 ASQ 24 Mo Date'] + pd.Timedelta(days=45) 
    ### DATE(DATEADD('day',45,[_C12 ASQ 24 Mo Date])) 

df2_edits1['_C18 ASQ 30 Mo 30 Day Date'] = df2_edits1['_C12 ASQ 30 Mo Date'] + pd.Timedelta(days=30) 
    ### DATE(DATEADD('day',30,[_C12 ASQ 30 Mo Date])) 

df2_edits1['_C18 ASQ 30 Mo 45 Day Date'] = df2_edits1['_C12 ASQ 30 Mo Date'] + pd.Timedelta(days=45) 
    ### DATE(DATEADD('day',45,[_C12 ASQ 30 Mo Date])) 

df2_edits1['_C18 ASQ 9 Mo 30 Day Date'] = df2_edits1['_C12 ASQ 9 Mo Date'] + pd.Timedelta(days=30) 
    ### DATE(DATEADD('day',30,[_C12 ASQ 9 Mo Date])) 

df2_edits1['_C18 ASQ 9 Mo 45 Day Date'] = df2_edits1['_C12 ASQ 9 Mo Date'] + pd.Timedelta(days=45) 
    ### DATE(DATEADD('day',45,[_C12 ASQ 9 Mo Date])) 

df2_edits1['_Enroll 3 Month Date'] = df2_edits1['_Enroll'] + pd.Timedelta(months=3) 
    ### DATE(DATEADD('month',3,[_Enroll])) 

df2_edits1['_Enroll 6 Month Date'] = df2_edits1['_Enroll'] + pd.Timedelta(months=6) 
    ### DATE(DATEADD('month',6,[_Enroll])) 

### TO DO: Fix Space in variable name! (but not yet.)
df2_edits1['_TGT 10 Month Date '] = df2_edits1['_TGT DOB'] + pd.Timedelta(months=10) 
    ### DATE(DATEADD('month',10,[_TGT DOB])) 

df2_edits1['_TGT 11 Month Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(months=11) 
    ### DATE(DATEADD('month',11,[_TGT DOB])) 

df2_edits1['_TGT 2 Month Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(months=2) 
    ### DATE(DATEADD('month',2,[_TGT DOB])) 

df2_edits1['_TGT 2 Week Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(weeks=2) 
    ### DATE(DATEADD('week',2,[_TGT DOB])) 

df2_edits1['_TGT 3 Day Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(days=3) 
    ### DATE(DATEADD('day',3,[_TGT DOB])) 

df2_edits1['_TGT 3 Month Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(months=3) 
    ### DATE(DATEADD('month',3,[_TGT DOB])) 

df2_edits1['_TGT 30 Day Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(days=30) 
    ### DATE(DATEADD('day',30,[_TGT DOB])) 

df2_edits1['_TGT 4 Month Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(months=4) 
    ### DATE(DATEADD('month',4,[_TGT DOB])) 

df2_edits1['_TGT 4 Week Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(weeks=4) 
    ### DATE(DATEADD('week',4,[_TGT DOB])) 

df2_edits1['_TGT 5 Month Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(months=5) 
    ### DATE(DATEADD('month',5,[_TGT DOB])) 

df2_edits1['_TGT 5 Week Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(weeks=5) 
    ### DATE(DATEADD('week',5,[_TGT DOB])) 

df2_edits1['_TGT 56 Day Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(days=56) 
    ### DATE(DATEADD('day',56,[_TGT DOB])) 

### TO DO: Fix Space in variable name! (but not yet.)
df2_edits1['_TGT 6 Month Date '] = df2_edits1['_TGT DOB'] + pd.Timedelta(months=6) 
    ### DATE(DATEADD('month',6,[_TGT DOB])) 

df2_edits1['_TGT 7 Day Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(days=7) 
    ### DATE(DATEADD('day',7,[_TGT DOB])) 

df2_edits1['_TGT 7 Month Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(months=7) 
    ### DATE(DATEADD('month',7,[_TGT DOB])) 

df2_edits1['_TGT 8 Day Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(days=8) 
    ### DATE(DATEADD('day',8,[_TGT DOB])) 

df2_edits1['_TGT 8 Month Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(months=8) 
    ### DATE(DATEADD('month',8,[_TGT DOB])) 

df2_edits1['_TGT 9 Month Date'] = df2_edits1['_TGT DOB'] + pd.Timedelta(months=9) 
    ### DATE(DATEADD('month',9,[_TGT DOB])) 

#%%##################################################
### IF/ELSE, CASE/WHEN

### fdf == "function DataFrame "

def function1(fdf):
    if (fdf['_Agency'] != "ll"):
        match fdf['Breast Feeding']:  ### FW
            case "YES":
                return 1
            case "1":
                return 1
            case "0":
                return 0
            case "-1":
                return -1
            case _:
                return None 
    elif (fdf['_Agency'] == "ll"):
        return None  ### add CASE for LLCHD values when they add them to their dataset
df2_edits1['_C2 BF Status'] = df2_edits1.apply(func=function1, axis=1, result_type='broadcast')

    ### IF [_Agency] <> "ll" THEN CASE [Breast Feeding]  // FW
    ###     WHEN "YES" THEN 1
    ###     WHEN "1" THEN 1
    ###     WHEN "0" THEN 0
    ###     WHEN "-1" THEN -1
    ###     ELSE NULL   
    ###     END
    ### ELSEIF [_Agency] = "ll" THEN NULL  // add CASE for LLCHD values when they add them to their dataset
    ### END

df2_edits1['_C7 Safe Sleep Yes Date'] = 
IF [Sleep On Back] = "Yes" ###FW
AND [Co Sleeping] = "No"
AND [Soft Bedding] = "No"
THEN [Safe Sleep Date]
ELSE [Safe Sleep Yes Dt] ###LLCHD
END

df2_edits1['_Discharge Reason'] = 
IF NOT ISNULL([Discharge Dt]) THEN CASE [Discharge Reason] ###LLCHD, see full reasons below
    WHEN 1 THEN "Completed Services" 
    ELSE "Stopped Services Before Completion"
    END
ELSEIF NOT ISNULL([Termination Date]) THEN CASE [Termination Status] ###FW
    WHEN "Family graduated/met all program goals" THEN "Completed Services"
    ELSE "Stopped Services Before Completion"
    ###need to check values for FW reasons
    END
ELSE "Currently Receiving Services"
END
###LLCHD discharge reasons
###1Family graduated/met all program goals
###2Family moved out of service area
###3Parent/guardian returned to school
###4Parent/guardian returned to work
###5Parent/guardian refused service
###6Death of participant
###7Unable to locate family
###8Target child adopted
###9Target child entered foster care
###10Target child living with another care giverx
###11Target child entered school/child care
###12Family never engaged
###13Unknown & a text box

df2_edits1['_Funding'] = 
IF [_Agency] <> "ll" THEN CASE [Agency]
    WHEN "hs" THEN "F"
    WHEN "ph" THEN "F"
    WHEN "nc" THEN "S"
    WHEN "ps" THEN "S"
    WHEN "vn" THEN "S"
    WHEN "se" THEN "TANF"
    ELSE "Unrecognized Value"
    END
ELSEIF [_Agency] = "ll" THEN [Funding]
END

df2_edits1['_FW Gestation Age Recode'] = 
CASE [GESTATIONAL AGE]
    WHEN '29 weeks' THEN 29
    WHEN '31 weeks' THEN 31
    WHEN '33 weeks' THEN 33
    WHEN '34 weeks' THEN 34
    WHEN '35 weeks' THEN 35
    WHEN '36 weeks' THEN 36
    WHEN '37 weeks' THEN 37
    WHEN '38 weeks' THEN 38
    WHEN '39 weeks' THEN 39
    WHEN '40 weeks' THEN 40
    WHEN '41 weeks' THEN 41
    WHEN '42 weeks' THEN 42
    WHEN 'Unknown' THEN NULL
ELSE NULL
END

df2_edits1['_Need Exclusion 4 - Dev Delay'] = 
IF [need_exclusion4 (Family Wise)] = "Developmental Delay" THEN "Developmental Delay" ###FW
ELSEIF [Need Exclusion4] = "Y" THEN "Developmental Delay" ###LLCHD
END

df2_edits1['_T05 Age Categories'] = 
IF [_T05 TGT Age in Months] < 12 THEN "< 1 year"
ELSEIF [_T05 TGT Age in Months] < 36 THEN "1-2 years" ###there is no group for 2-3 years old on F1 so they are lumped in here
ELSEIF [_T05 TGT Age in Months] < 48 THEN "3-4 years"
ELSEIF [_T05 TGT Age in Months] <= 60 THEN "5-6 years"
ELSEIF [_T05 TGT Age in Months] > 60 THEN "6+ years"
ELSE "Unknown/Did Not Report"
END

df2_edits1['_T06 TGT Ethnicity'] = 
###FW
IF NOT ISNULL([TGT ETHNICITY]) THEN CASE [TGT ETHNICITY]
    WHEN "Non Hispanic/Latino" THEN "Not Hispanic or Latino"
    WHEN "Hispanic/Latino" THEN "Hispanic or Latino"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
###LLCDH
ELSEIF NOT ISNULL([tgt_ethnicity]) THEN CASE [tgt_ethnicity] 
    WHEN "HISPANIC/LATINO" THEN "Hispanic or Latino" 
    WHEN "HISPANIC" THEN "Hispanic or Latino"
    WHEN "NOT HISPANIC/LATINO" THEN "Not Hispanic or Latino"
    WHEN "NON-Hispanic" THEN "Not Hispanic or Latino"
    WHEN "UNREPORTED/REFUSED TO REPORT" THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSE "Unknown/Did Not Report"
END

df2_edits1['_T1 Tgt Gender'] = 
###FW
IF NOT ISNULL([TGT Gender]) THEN CASE [TGT Gender]
    WHEN "Female" THEN "Female"
    WHEN "Male" THEN "Male"
    WHEN "Non-Binary" THEN "Non-Binary"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Null" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
###LLCHD
ELSEIF NOT ISNULL([Tgt Gender]) THEN CASE [Tgt Gender]
    WHEN "F" THEN "Female"
    WHEN "M" THEN "Male"
    ### WHEN "N" THEN "Non-Binary" ### Don't have this value yet - confirm
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSE "Unknown/Did Not Report"
END

df2_edits1['_T13 TGT Language'] = 
IF NOT ISNULL([Mob Language]) THEN CASE [Mob Language]
    WHEN "English" THEN "English"
    WHEN "Spanish" THEN "Spanish"
    ELSE "Other"
    END
ELSEIF NOT ISNULL([Language Primary]) THEN CASE [Language Primary]
    WHEN "ENGLISH" THEN "English"
    WHEN "SPANISH" THEN "Spanish"
    ELSE "Other"
    END
ELSE "Unknown/Did Not Report"
END

df2_edits1['_T15-7 Household Developmental Delay'] = 
IF [NT Child Dev Delay] = "Yes" THEN 1 ###FW
ELSEIF [NT Child Dev Delay] = "No" THEN 0
ELSEIF [Priority Develop Delays] = "Y" THEN 1 ###LLCHD
ELSEIF [Priority Develop Delays] = "N" THEN 0
END
###To determine priority population, positive ASQ results also need to be considered

df2_edits1['_T20 TGT Insurance Status'] = 
IF NOT ISNULL([CHINS Primary Ins]) THEN CASE [CHINS Primary Ins] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSEIF NOT ISNULL([Hlth Insure Tgt]) THEN CASE [Hlth Insure Tgt] ###LLCHD
    WHEN 0 THEN "No Insurance Coverage"
    WHEN 1 THEN "Medicaid or CHIP" ###1=Medicaid
    WHEN 2 THEN "Tri-Care" ###2=Tricare
    WHEN 3 THEN "Private or Other" ###3=Private/Other
    WHEN 4 THEN "FamilyChildHealthPlus" ###4=Unknown/Did Not Report
    WHEN 5 THEN "No Insurance Coverage" ###5=None
    WHEN 6 THEN "Unknown/Did Not Report"
    WHEN 99 THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSE "Unknown/Did Not Report"
END

df2_edits1['_T21 TGT Usual Source of Medical Care'] = 
IF NOT ISNULL([Child Med Care Source]) THEN CASE [Child Med Care Source] ###FW
    WHEN "Doctor/Nurse Practitioner" THEN "Doctor's/Nurse Practitioner's Office"
    WHEN "Federally Qualified Health Center" THEN "Federally Qualified Health Center"
    WHEN "Hospital ER" THEN "Hospital Emergency Room"
    WHEN "Hospital Outpatient" THEN "Hospital Outpatient"
    WHEN "None" THEN "None"
    WHEN "Other" THEN "Other"
    WHEN "Retail or Minute Clinic" THEN "Retail Store or Minute Clinic"
    WHEN "Prenatal Client" THEN "Prenatal Client"
    ELSE "Unrecognized Value"
    END
ELSEIF NOT ISNULL([Tgt Medical Home]) THEN CASE [Tgt Medical Home] ###LLCHD, coded values are = to form 1 categories
    WHEN 0 THEN "None"
    WHEN 1 THEN "Doctor's/Nurse Practitioner's Office"
    WHEN 2 THEN "Hospital Emergency Room"
    WHEN 3 THEN "Hospital Outpatient"
    WHEN 4 THEN "Federally Qualified Health Center"
    WHEN 5 THEN "Retail Store or Minute Clinic"
    WHEN 6 THEN "Other"
    WHEN 7 THEN "None"
    WHEN 8 THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSE "Unknown/Did Not Report"
END

df2_edits1['_T22 TGT Usual Souce of Dental Care'] = 
IF NOT ISNULL([Child Dental Care Source]) THEN CASE [Child Dental Care Source] ###FW
    WHEN "Do not have a usual source of dental care" THEN "Do not have a usual source of dental care"
    WHEN "Does not have a usual source of dental care" THEN "Do not have a usual source of dental care"
    WHEN "Has a usual source of dental care" THEN "Have a usual source of dental care"
    WHEN "Have a usual source of dental care" THEN "Have a usual source of dental care"
    WHEN "Prenatal Client" THEN "Prenatal Client"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSEIF NOT ISNULL([Tgt Dental Home]) THEN CASE [Tgt Dental Home] ###LLCHD, coded values are = to form 1 categories
    WHEN 1 THEN "Have a usual source of dental care" 
    WHEN 2 THEN "Do not have a usual source of dental care"
    WHEN 3 THEN "Unknown/Did Not Report"
    WHEN 6 THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSE "Unknown/Did Not Report"
END

df2_edits1['_TGT DOB'] = 
IF [Tgt Dob] = DATE(1/1/1900) THEN NULL ###LLCHD
ELSEIF [Tgt Dob-Cr] = DATE(1/1/1900) THEN NULL ###FW
ELSE df2_edits1['Tgt Dob'].combine_first(df2_edits1['Tgt Dob-Cr'])
END
    ### IF [Tgt Dob] = DATE(1/1/1900) THEN NULL ###LLCHD
    ### ELSEIF [Tgt Dob-Cr] = DATE(1/1/1900) THEN NULL ###FW
    ### ELSE IFNULL([Tgt Dob],[Tgt Dob-Cr])
    ### END

df2_edits1['_TGT EDC Date'] = 
IF [Dt Edc] = DATE(1/1/1900) THEN NULL ###LLCHD
ELSEIF [EDC Date] = DATE(1/1/1900) THEN NULL ###FW
ELSE df2_edits1['Dt Edc'].combine_first(df2_edits1['EDC Date'])
END
    ### IF [Dt Edc] = DATE(1/1/1900) THEN NULL ###LLCHD
    ### ELSEIF [EDC Date] = DATE(1/1/1900) THEN NULL ###FW
    ### ELSE IFNULL([Dt Edc],[EDC Date])
    ### END

df2_edits1['_TGT Race'] = 
###LLCHD
###multiracial
IF IIF([Tgt Race Asian]="Y",1,0,0)+IIF([Tgt Race Black]="Y",1,0,0)+IIF([Tgt Race Hawaiian]="Y",1,0,0)+IIF([Tgt Race Indian]="Y",1,0,0)
+IIF([Tgt Race Other]="Y",1,0,0)+IIF([Tgt Race White]="Y",1,0,0) > 1 THEN "More than one race"
###single race
ELSEIF [Tgt Race Asian] = "Y" THEN "Asian"
ELSEIF [Tgt Race Black] = "Y" THEN "Black or African American"
ELSEIF [Tgt Race Hawaiian] = "Y" THEN "Native Hawaiian or Other Pacific Islander"
ELSEIF [Tgt Race Indian] = "Y" THEN "American Indian or Alaska Native"
ELSEIF [Tgt Race White] = "Y" THEN "White"
ELSEIF [Tgt Race Other] = "Y" THEN "Other"
###FW
###multiracial, = "True" is not required in IIF statement because race is boolean
ELSEIF IIF([TGTRaceAsian],1,0,0)+IIF([TGTRaceBlack],1,0,0)+IIF([TGTRaceHawaiianPacific],1,0,0)
+IIF([TGTRaceIndianAlaskan],1,0,0)+IIF([TGTRaceWhite],1,0,0)+IIF([TGTRaceOther],1,0,0) > 1 
THEN "More than one race"
###single race
ELSEIF [TGTRaceAsian] = True THEN "Asian"
ELSEIF [TGTRaceBlack] = True THEN "Black or African American"
ELSEIF [TGTRaceHawaiianPacific] = True THEN "Native Hawaiian or Other Pacific Islander"
ELSEIF [TGTRaceIndianAlaskan] = True THEN "American Indian or Alaska Native"
ELSEIF [TGTRaceWhite] = True THEN "White"
ELSEIF [TGTRaceOther] = True THEN "Other"
ELSE "Unknown/Did Not Report"
END

df2_edits1['_C11 Literacy Read Sing'] = 
IF [_Agency] <> "ll" THEN CASE [Read Tell Story Sing]  ### FW
    WHEN "0" THEN 0
    WHEN "1" THEN 1
    WHEN "2" THEN 2
    WHEN "3" THEN 3
    WHEN "4" THEN 4
    WHEN "5" THEN 5
    WHEN "6" THEN 6
    WHEN "7" THEN 7
    WHEN "YES" THEN 7
    ELSE NULL   
    END
ELSEIF [_Agency] = "ll" THEN CASE [Early Language]  ### LLCHD
    WHEN "N" THEN 0
    WHEN "Y" THEN 7
### Y = “Every day of the week / 
    ### Most days of the week / 
    ### Several days of the week”
    ELSE NULL
    END
END

df2_edits1['_Child Welfare Interaction'] = 
IF [History Inter Welfare Child] = True THEN 1 ###FW
ELSEIF [History Inter Welfare Child] = False THEN 0
ELSEIF [Priority Child Welfare] = "Y" THEN 1 ###LLCHD
ELSEIF [Priority Child Welfare] = "N" THEN 0
END
###For priority population, current maltreatment reports also need to be considered

df2_edits1['_T05 TGT Age in Months'] = 
IF [_TGT DOB]> DATEADD('month',-DATEDIFF('month',[_TGT DOB],TODAY()),TODAY())
THEN DATEDIFF('month',[_TGT DOB],TODAY()-1)
ELSE DATEDIFF('month',[_TGT DOB],TODAY())
END

df2_edits1['_T15-6 Low Student Achievement'] = 
IF [NT Child Low Achievement] = "No" THEN 0 ###FW
ELSEIF [NT Child Low Achievement] = "Yes" THEN 1
ELSEIF [Priority Low Student] = "N" THEN 0 ###LLCHD
ELSEIF [Priority Low Student] = "Y" THEN 1
END





#%%##################################################
### Identify/FLAG "Unrecognized Value" ###
#####################################################

### FLAG any "Unrecognized Value" --- new value & needs to be edited earlier in the Data Source process.
### Across many variables.

def nameoffunction (df.columns):
    if pd.dtypes == string:
        look for U.R.
    if not sting
        


df.loc[df['column']]



#%%##################################################
### Data Types ###
#####################################################

### REMEMBER to check/set the data type of each column like it should be in output.





#%%##################################################
### Prepare CSV ###
#####################################################




#%%##################################################
### WRITE ###
#####################################################

#%%
df2.to_csv(path_2_output, index=False)





#%%##################################################
### COMPARE CSVs ###
#####################################################

#%%
path_comparison_csv = Path('U:\\Working\\nebraska_miechv_coded_data_source\\previous\\previous output\\Y12Q1 (Oct 2022 - Dec 2023)\\Child Activity Master File from Excel on NE Server.csv')

#%%
comparison_csv = pd.read_csv(path_comparison_csv)

#%%
[*df2] == [*comparison_csv]

#%%
[*df2]
#%%
[*comparison_csv]

#%%### Overlap / Similarities
set([*comparison_csv]).intersection([*df2])

#%%### Differences
set([*comparison_csv]).symmetric_difference([*df2])


####### Compare values





#%%##################################################
### Questions ###
#####################################################

# Why no "Birth File" tab columns? (never existed in Tableau wb). Answer: Last Quarter was removed from Join.
# Why Office not working?




