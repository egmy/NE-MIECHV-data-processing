
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TO DO: Instructions for how to get into environment & how to edit/run code files.

#%%##################################################
### PACKAGES ###
#####################################################

import pandas as pd
from pathlib import Path
import numpy as np
import sys
import IPython

print('Version Of Python: ' + sys.version)
print('Version Of Pandas: ' + pd.__version__)
print('Version Of Numpy: ' + np.version.version)

#%%##################################################
### SETTINGS ###
#####################################################

### None for now.

#%%##################################################
### PATHS ###
#####################################################

### Data Source for 3rd Tableau file, 1st Data Source (for Form 2):
### DS: "Adult Activity Master File from Excel on NE Server".
### path_3_data_source = 'U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Adult Activity Master File Y12.xlsx'
### local:
path_3_data_source_file = Path('U:\\Working\\nebraska_miechv_coded_data_source\\data\\01 original\Y12Q1 (Oct 2022 - Dec 2023)\\Adult Activity Master File.xlsx')

path_3_data_source_sheets = [
    'Project ID' # 1
    ,'Caregiver Insurance' # 2
    ,'Family Wise' # 3
    ,'LLCHD' # 4
]

### Output for 2nd Tableau file:
path_3_output_dir = Path('U:\\Working\\nebraska_miechv_coded_data_source\\data\\04 output')
path_3_output = Path(path_3_output_dir, 'Adult Activity Master File from Excel on NE Server.csv')

#%%##################################################
### Comparison File ###
#####################################################

### File created for Y12Q1 by the old data sourcing process with Tableau.
path_df_comparison_csv = Path('U:\\Working\\nebraska_miechv_coded_data_source\\previous\\previous output\\Y12Q1 (Oct 2022 - Dec 2023)\\Adult Activity Master File from Excel on NE Server.csv')
df_comparison_csv = pd.read_csv(path_df_comparison_csv, dtype=object, keep_default_na=False, na_values=[''])
df_comparison_csv = df_comparison_csv.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)

#%%##################################################
### Utility Functions ###
#####################################################

def inspect_df (df):
    print(df.describe(include='all'))
    print('\n')
    print(df.dtypes.to_string())
    print('\n')
    print(df.info(verbose=True, show_counts=True))
    print('\n')
    print(f'Rows: {len(df)}')
    print(f'Columns: {len(df.columns)}')
    print('\n')
    IPython.display.display(df)

### fSeries = df column or Series: e.g., df['colname'].
def inspect_col(fSeries):
    print(fSeries.info())
    print('\n')
    print('value_counts:')
    print(fSeries.value_counts(dropna=False))
    print('\n')
    print(fSeries)

def compare_col(fdf_2, fcol, info_or_value_counts='info', fdf_1=df_comparison_csv): ### or 'value_counts'.
    if info_or_value_counts=='info':
        print(f'DataFrame 1 (df_comparison_csv):\n')
        print(fdf_1[fcol].info())
        print('\n')
        print(f'DataFrame 2:\n')
        print(fdf_2[fcol].info())
    elif info_or_value_counts=='value_counts':
        print(f'DataFrame 1 (df_comparison_csv):\n')
        print(fdf_1[fcol].value_counts(dropna=False))
        print('\n')
        print(f'DataFrame 2:\n')
        print(fdf_2[fcol].value_counts(dropna=False))

#%%##################################################
### COLUMN DEFINITIONS ###
#####################################################

#%%### df3_1: 'Project ID'.
#%%### df3_2: 'Caregiver Insurance'.
#%%### df3_3: 'Family Wise'.
#%%### df3_4: 'LLCHD'.

#######################
#%%### df3_1: 'Project ID'.
df3_1_col_detail = [
    ['join_id', 'Join Id', '', 'Int64'],
    ['project_id', 'Project Id', '', 'string'], 
    ['year', 'Year', '', 'Int64'], 
    ['quarter', 'Quarter', '', 'Int64']
]
#%%### df3_1: 'Project ID'.
### For Renaming, we only need a dictionary of the columns with names changing.
### If x[2] == 'same' or x[0] == x[1] then that column is not included in df_colnames.
df3_1_colnames = {x[0]:x[1] for x in df3_1_col_detail if x[2] != 'same' and x[0] != x[1]}
df3_1_colnames
#%%### df3_1: 'Project ID'.
df3_1_col_dtypes = {x[0]:x[3] for x in df3_1_col_detail}
df3_1_col_dtypes

#######################
#%%### df3_2: 'Caregiver Insurance'.
df3_2_col_detail = [
    ['ProjectID', '', '', ''],
    ['year', '', '', ''],
    ['quarter', '', '', ''],
    ['agency', '', '', ''],
    ['FAMILYNUMBER', '', '', ''],
    ['ChildNumber', '', '', ''],
    ['funding', '', '', ''],
    ['AD1PrimaryIns.1', '', '', ''],
    ['AD1InsChangeDate.1', '', '', ''],
    ['AD1PrimaryIns.2', '', '', ''],
    ['AD1InsChangeDate.2', '', '', ''],
    ['AD1PrimaryIns.3', '', '', ''],
    ['AD1InsChangeDate.3', '', '', ''],
    ['AD1PrimaryIns.4', '', '', ''],
    ['AD1InsChangeDate.4', '', '', ''],
    ['AD1PrimaryIns.5', '', '', ''],
    ['AD1InsChangeDate.5', '', '', ''],
    ['AD1PrimaryIns.6', '', '', ''],
    ['AD1InsChangeDate.6', '', '', ''],
    ['AD1PrimaryIns.7', '', '', ''],
    ['AD1InsChangeDate.7', '', '', ''],
    ['AD1PrimaryIns.8', '', '', ''],
    ['AD1InsChangeDate.8', '', '', ''],
    ['AD1PrimaryIns.9', '', '', ''],
    ['AD1InsChangeDate.9', '', '', ''],
    ['AD1PrimaryIns.10', '', '', ''],
    ['AD1InsChangeDate.10', '', '', ''],
    ['AD1PrimaryIns.11', '', '', ''],
    ['AD1InsChangeDate.11', '', '', ''],
    ['AD1PrimaryIns.12', '', '', ''],
    ['AD1InsChangeDate.12', '', '', ''],
    ['AD1PrimaryIns.13', '', '', ''],
    ['AD1InsChangeDate.13', '', '', ''],
    ['AD1PrimaryIns.14', '', '', ''],
    ['AD1InsChangeDate.14', '', '', ''],
    ['AD1PrimaryIns.15', '', '', ''],
    ['AD1InsChangeDate.15', '', '', ''],
    ['AD1PrimaryIns.16', '', '', ''],
    ['AD1InsChangeDate.16', '', '', '']
]
#%%### df3_2: 'Caregiver Insurance'.
df3_2_colnames = {x[0]:x[1] for x in df3_2_col_detail if x[2] != 'same' and x[0] != x[1]}
df3_2_colnames
#%%### df3_2: 'Caregiver Insurance'.
df3_2_col_dtypes = {x[0]:x[3] for x in df3_2_col_detail}
df3_2_col_dtypes

#######################
#%%### df3_3: 'Family Wise'.
df3_3_col_detail = [
    ['Project ID', '', '', ''],
    ['year', '', '', ''],
    ['quarter', '', '', ''],
    ['agency', '', '', ''],
    ['FamilyNumber', '', '', ''],
    ['ChildNumber', '', '', ''],
    ['TGT DOB-CR', '', '', ''],
    ['EDC Date', '', '', ''],
    ['MinOfHVDate', '', '', ''],
    ['TERMINATION STATUS', '', '', ''],
    ['TERMINATION DATE', '', '', ''],
    ['Pregnancystatus', '', '', ''],
    ['MaxOfHVDate', '', '', ''],
    ['MaxOfVISIT NUMBER', '', '', ''],
    ['BehaviorNumer', '', '', ''],
    ['BehaviorDenom', '', '', ''],
    ['HomeVisitsPrenatal', '', '', ''],
    ['HomeVisitsTotal', '', '', ''],
    ['MOBDOB', '', '', ''],
    ['FOBDOB', '', '', ''],
    ['MinEducationDate', '', '', ''],
    ['AD1MinEdu', '', '', ''],
    ['MinEduEnroll', '', '', ''],
    ['MaxEduDate', '', '', ''],
    ['AD1MaxEdu', '', '', ''],
    ['MaxEduEnroll', '', '', ''],
    ['DATEUNCOPE', '', '', ''],
    ['U', '', '', ''],
    ['N', '', '', ''],
    ['C', '', '', ''],
    ['O', '', '', ''],
    ['P', '', '', ''],
    ['E', '', '', ''],
    ['UNCOPERefDate', '', '', ''],
    ['UNCOPERefCategory', '', '', ''],
    ['TobaccoUseDate', '', '', ''],
    ['TobaccoRefDate', '', '', ''],
    ['AssessAfraid', '', '', ''],
    ['AssessPolice', '', '', ''],
    ['AssessIPV', '', '', ''],
    ['IPV Assess Date', '', '', ''],
    ['IPVRefDate', '', '', ''],
    ['PostpartumCheckupDate', '', '', ''],
    ['MinOfCESDDATE', '', '', ''],
    ['CESDTotal', '', '', ''],
    ['MinOfMHRefDate', '', '', ''],
    ['MaxCHEEERSDate', '', '', ''],
    ['AD2EduDateMax', '', '', ''],
    ['AD2EDLevel', '', '', ''],
    ['AD2InSchool', '', '', ''],
    ['MaxOfMaxOfAD2InsChangeDate', '', '', ''],
    ['AD2InsPrimary', '', '', ''],
    ['MaxOfAD1EmpChangeDate', '', '', ''],
    ['AD1EmpStatus', '', '', ''],
    ['MaxOfAD2EmplChangeDate', '', '', ''],
    ['AD2EmployStatus', '', '', ''],
    ['MaxOfMaxOfAD1MSChangeDate', '', '', ''],
    ['Adult1MaritalStatus', '', '', ''],
    ['MaxOfAD2MSChangeDate', '', '', ''],
    ['Adult2MaritalStatus', '', '', ''],
    ['ANNUAL INCOME', '', '', ''],
    ['POVERTY LEVEL', '', '', ''],
    ['TYPE HOUSING', '', '', ''],
    ['HomelessStatus', '', '', ''],
    ['HousingStatus', '', '', ''],
    ['HistoryInterWelfareAdult', '', '', ''],
    ['MOB SUBSTANCE ABUSE', '', '', ''],
    ['FOB SUBSTANCE ABUSE', '', '', ''],
    ['TobaccoUseInHome', '', '', ''],
    ['LowAchievement', '', '', ''],
    ['NTChildLowAchievement', '', '', ''],
    ['NTChildDevDelay', '', '', ''],
    ['Military', '', '', ''],
    ['Adult1Gender', '', '', ''],
    ['Adult1TGTRelation', '', '', ''],
    ['MOB ETHNIC', '', '', ''],
    ['MOBRaceWhite', '', '', ''],
    ['MOBRaceBlack', '', '', ''],
    ['MOBRaceIndianAlaskan', '', '', ''],
    ['MOBRaceAsian', '', '', ''],
    ['MOBRaceHawaiianPacific', '', '', ''],
    ['MOBRaceOther', '', '', ''],
    ['FOB INVOLVED', '', '', ''],
    ['Adult2Gender', '', '', ''],
    ['Adult2TGTRelation', '', '', ''],
    ['FOB ETHNICITY', '', '', ''],
    ['FOBRaceWhite', '', '', ''],
    ['FOBRaceBlack', '', '', ''],
    ['FOBRaceIndianAlaskan', '', '', ''],
    ['FOBRaceAsian', '', '', ''],
    ['FOBRaceHawaiianPacific', '', '', ''],
    ['FOBRaceOther', '', '', ''],
    ['MOB ZIP', '', '', ''],
    ['Adaptation', '', '', ''],
    ['need_exclusion1', '', '', ''],
    ['need_exclusion2', '', '', ''],
    ['need_exclusion3', '', '', ''],
    ['need_exclusion5', '', '', ''],
    ['need_exclusion6', '', '', '']
]
#%%### df3_3: 'Family Wise'.
df3_3_colnames = {x[0]:x[1] for x in df3_3_col_detail if x[2] != 'same' and x[0] != x[1]}
df3_3_colnames
#%%### df3_3: 'Family Wise'.
df3_3_col_dtypes = {x[0]:x[3] for x in df3_3_col_detail}
df3_3_col_dtypes

#######################
#%%### df3_4: 'LLCHD'.
df3_4_col_detail = [
    ['project_id', '', '', ''],
    ['year', '', '', ''],
    ['quarter', '', '', ''],
    ['site_id', '', '', ''],
    ['family_id', '', '', ''],
    ['tgt_id', '', '', ''],
    ['tgt_dob', '', '', ''],
    ['tgt_gender', '', '', ''],
    ['tgt_ethnicity', '', '', ''],
    ['tgt_race_indian', '', '', ''],
    ['tgt_race_asian', '', '', ''],
    ['tgt_race_black', '', '', ''],
    ['tgt_race_hawaiian', '', '', ''],
    ['tgt_race_white', '', '', ''],
    ['tgt_race_other', '', '', ''],
    ['tgt_GestationalAge', '', '', ''],
    ['tgt_medical_home', '', '', ''],
    ['tgt_medical_home_dt', '', '', ''],
    ['tgt_dental_home', '', '', ''],
    ['tgt_dental_home_dt', '', '', ''],
    ['dt_edc', '', '', ''],
    ['enroll_dt', '', '', ''],
    ['enroll_preg_status', '', '', ''],
    ['current_pregnancy', '', '', ''],
    ['discharge_reason', '', '', ''],
    ['discharge_dt', '', '', ''],
    ['last_home_visit', '', '', ''],
    ['home_visits_num', '', '', ''],
    ['home_visits_pre', '', '', ''],
    ['home_visits_post', '', '', ''],
    ['home_visits_person', '', '', ''],
    ['home_visits_virtual', '', '', ''],
    ['funding', '', '', ''],
    ['c_fundingdate', '', '', ''],
    ['p_funding', '', '', ''],
    ['p_fundingdate', '', '', ''],
    ['primary_id', '', '', ''],
    ['primary_relation', '', '', ''],
    ['mob_id', '', '', ''],
    ['mob_dob', '', '', ''],
    ['mob_gender', '', '', ''],
    ['mob_ethnicity', '', '', ''],
    ['mob_race', '', '', ''],
    ['mob_race_indian', '', '', ''],
    ['mob_race_asian', '', '', ''],
    ['mob_race_black', '', '', ''],
    ['mob_race_hawaiian', '', '', ''],
    ['mob_race_white', '', '', ''],
    ['mob_race_other', '', '', ''],
    ['mob_marital_status', '', '', ''],
    ['mob_living_arrangement', '', '', ''],
    ['mob_living_arrangement_dt', '', '', ''],
    ['fob_id', '', '', ''],
    ['fob_dob', '', '', ''],
    ['fob_gender', '', '', ''],
    ['fob_ethnicity', '', '', ''],
    ['fob_race', '', '', ''],
    ['fob_race_indian', '', '', ''],
    ['fob_race_asian', '', '', ''],
    ['fob_race_black', '', '', ''],
    ['fob_race_hawaiian', '', '', ''],
    ['fob_race_white', '', '', ''],
    ['fob_race_other', '', '', ''],
    ['fob_marital_status', '', '', ''],
    ['fob_living_arrangement', '', '', ''],
    ['fob_living_arrangement_dt', '', '', ''],
    ['fob_edu_dt', '', '', ''],
    ['fob_edu', '', '', ''],
    ['fob_employ_dt', '', '', ''],
    ['fob_employ', '', '', ''],
    ['fob_involved', '', '', ''],
    ['fob_visits', '', '', ''],
    ['zip', '', '', ''],
    ['household_income', '', '', ''],
    ['household_size', '', '', ''],
    ['mcafss_income_dt', '', '', ''],
    ['mcafss_income', '', '', ''],
    ['mcafss_edu_dt1', '', '', ''],
    ['mcafss_edu1', '', '', ''],
    ['mcafss_edu1_enroll', '', '', ''],
    ['mcafss_edu1_enroll_dt', '', '', ''],
    ['mcafss_edu1_prog', '', '', ''],
    ['mcafss_edu_dt2', '', '', ''],
    ['mcafss_edu2', '', '', ''],
    ['mcafss_edu2_enroll', '', '', ''],
    ['mcafss_edu2_enroll_dt', '', '', ''],
    ['mcafss_edu2_prog', '', '', ''],
    ['mcafss_employ_dt', '', '', ''],
    ['mcafss_employ', '', '', ''],
    ['language_primary', '', '', ''],
    ['priority_child_welfare', '', '', ''],
    ['priority_substance_abuse', '', '', ''],
    ['priority_tobacco_use', '', '', ''],
    ['priority_low_student', '', '', ''],
    ['priority_develop_delays', '', '', ''],
    ['priority_military', '', '', ''],
    ['uncope_dt', '', '', ''],
    ['uncope_score', '', '', ''],
    ['substance_abuse_ref_dt', '', '', ''],
    ['tobacco_use', '', '', ''],
    ['tobacco_use_dt', '', '', ''],
    ['tobacco_ref_dt', '', '', ''],
    ['safe_sleep_yes', '', '', ''],
    ['safe_sleep_yes_dt', '', '', ''],
    ['cheeers_date', '', '', ''],
    ['early_language', '', '', ''],
    ['early_language_dt', '', '', ''],
    ['asq3_dt_9mm', '', '', ''],
    ['asq3_timing_9mm', '', '', ''],
    ['asq3_comm_9mm', '', '', ''],
    ['asq3_gross_9mm', '', '', ''],
    ['asq3_fine_9mm', '', '', ''],
    ['asq3_problem_9mm', '', '', ''],
    ['asq3_social_9mm', '', '', ''],
    ['asq3_feedback_9mm', '', '', ''],
    ['asq3_referral_9mm', '', '', ''],
    ['asq3_dt_18mm', '', '', ''],
    ['asq3_timing_18mm', '', '', ''],
    ['asq3_comm_18mm', '', '', ''],
    ['asq3_gross_18mm', '', '', ''],
    ['asq3_fine_18mm', '', '', ''],
    ['asq3_problem_18mm', '', '', ''],
    ['asq3_social_18mm', '', '', ''],
    ['asq3_feedback_18mm', '', '', ''],
    ['asq3_referral_18mm', '', '', ''],
    ['asq3_dt_24mm', '', '', ''],
    ['asq3_timing_24mm', '', '', ''],
    ['asq3_comm_24mm', '', '', ''],
    ['asq3_gross_24mm', '', '', ''],
    ['asq3_fine_24mm', '', '', ''],
    ['asq3_problem_24mm', '', '', ''],
    ['asq3_social_24mm', '', '', ''],
    ['asq3_feedback_24mm', '', '', ''],
    ['asq3_referral_24mm', '', '', ''],
    ['asq3_dt_30mm', '', '', ''],
    ['asq3_timing_30mm', '', '', ''],
    ['asq3_comm_30mm', '', '', ''],
    ['asq3_gross_30mm', '', '', ''],
    ['asq3_fine_30mm', '', '', ''],
    ['asq3_problem_30mm', '', '', ''],
    ['asq3_social_30mm', '', '', ''],
    ['asq3_feedback_30mm', '', '', ''],
    ['asq3_referral_30mm', '', '', ''],
    ['behavioral_concerns', '', '', ''],
    ['ipv_screen', '', '', ''],
    ['ipv_screen_dt', '', '', ''],
    ['ipv_referral_dt', '', '', ''],
    ['prim_care_dt', '', '', ''],
    ['cesd_dt', '', '', ''],
    ['cesd_score', '', '', ''],
    ['ment_hlth_ref_dt', '', '', ''],
    ['lsp_bf_initiation_dt', '', '', ''],
    ['lsp_bf_discon_dt', '', '', ''],
    ['hlth_insure_mob', '', '', ''],
    ['hlth_insure_mob_dt', '', '', ''],
    ['hlth_insure_tgt', '', '', ''],
    ['hlth_insure_tgt_dt', '', '', ''],
    ['last_well_child_visit', '', '', ''],
    ['hlth_insure_fob', '', '', ''],
    ['hlth_insure_fob_dt', '', '', ''],
    ['need_exclusion1', '', '', ''],
    ['need_exclusion2', '', '', ''],
    ['need_exclusion3', '', '', ''],
    ['need_exclusion4', '', '', ''],
    ['need_exclusion5', '', '', ''],
    ['need_exclusion6', '', '', ''],
    ['Has_ChildWelfareAdaptation', '', '', '']
]
#%%### df3_4: 'LLCHD'.
df3_4_colnames = {x[0]:x[1] for x in df3_4_col_detail if x[2] != 'same' and x[0] != x[1]}
df3_4_colnames
#%%### df3_4: 'LLCHD'.
df3_4_col_dtypes = {x[0]:x[3] for x in df3_4_col_detail}
df3_4_col_dtypes

#%%##################################################
### READ ###
#####################################################

#%%
### Performance benefit for reading in file to memory only once by creating an ExcelFile class object.
xlsx = pd.ExcelFile(path_3_data_source_file)

#%% 
### CHECK that all path_3_data_source_sheets same as xlsx.sheet_names (different order ok):
print(sorted(path_3_data_source_sheets))
print([x for x in sorted(xlsx.sheet_names) if x != 'MOB or FOB'])
sorted(path_3_data_source_sheets) == [x for x in sorted(xlsx.sheet_names) if x != 'MOB or FOB']

#%%
### READ all sheets:
df3_1 = pd.read_excel(xlsx, sheet_name=path_3_data_source_sheets[0], keep_default_na=False, na_values=[''])#, dtype=df3_1_col_dtypes)
df3_2 = pd.read_excel(xlsx, sheet_name=path_3_data_source_sheets[1], keep_default_na=False, na_values=[''])#, dtype=df3_2_col_dtypes)
df3_3 = pd.read_excel(xlsx, sheet_name=path_3_data_source_sheets[2], keep_default_na=False, na_values=[''])#, dtype=df3_3_col_dtypes)
df3_4 = pd.read_excel(xlsx, sheet_name=path_3_data_source_sheets[3], keep_default_na=False, na_values=[''])#, dtype=df3_4_col_dtypes)

### Review each sheet:
### Note: Even empty DFs merge fine below.

#%%
inspect_df(df3_1)
#%%
inspect_df(df3_2)
#%%
inspect_df(df3_3)
#%%
inspect_df(df3_4)

#%%##################################################
### JOIN ###
#####################################################

### join with pandas.merge() or pandas.df.join()
### https://pandas.pydata.org/docs/reference/api/pandas.merge.html

### DS: "Adult Activity Master File from Excel on NE Server".
df3 = pd.merge(df3_1, df3_2, how='left', left_on=['Project Id','Year','Quarter'], right_on=['Project ID','year (Caregiver Insurance)','quarter (Caregiver Insurance)'], indicator='LJ_df3_2').merge(df3_3, how='left', left_on=['Project Id','Year','Quarter'], right_on=['Project ID 1','year (Family Wise)','quarter (Family Wise)'], indicator='LJ_df3_3').merge(df3_4, how='left', left_on=['Project Id','Year','Quarter'], right_on=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)'], indicator='LJ_df3_4') 

#%%##################################################
### RECODE ###
#####################################################

### numpy.where() --- Sami: use to treat NULL values weird at times; may be quicker than apply.
### pandas

### Not needed?
    ### Number of Records


df3['_90 Day UNCOPE Date'] = DATE(DATEADD('day',90,[_UNCOPE Date]))

df3['_Agency'] = IFNULL([agency (Family Wise)],[Site Id])

df3['_C03 CES-D Date'] = IFNULL([Cesd Dt],[Min Of CESDDATE])

df3['_C05 Postpartum Checkup Date'] = IFNULL([Postpartum Checkup Date],[Prim Care Dt])

df3['_C05 TGT 30 Day Date'] = DATE(DATEADD('day',30,[_TGT DOB]))

df3['_C05 TGT 56 Day Date'] = DATE(DATEADD('day',56,[_TGT DOB]))

df3['_C06 Tobacco Referral Date'] = IFNULL([Tobacco Ref Date],[_C06 Tobacco Use Date])

df3['_C06 Tobacco Use Date'] = IFNULL([Tobacco Use Date],[Tobacco Use Dt]) ###a date indicates tobacco use

df3['_C10 CHEEERS'] = IFNULL([Cheeers Date],[Max CHEEERS Date])

df3['_C14 IPV Date'] = IFNULL([IPV Assess Date],[Ipv Screen Dt])

df3['_C15 Max Education Date'] = DATE(IFNULL([Mcafss Edu Dt2],[Max Edu Date]))

df3['_C15 Max Educational Enrollment'] = 
IF [Max Edu Enroll] = "College 2 Year" THEN "Student/trainee" ###FW
ELSEIF [Max Edu Enroll] = "College 4 Year" THEN "Student/trainee"
ELSEIF [Max Edu Enroll] = "ESL" THEN "Student/trainee"
ELSEIF [Max Edu Enroll] = "GED Program" THEN "Student/trainee HS/GED"
ELSEIF [Max Edu Enroll] = "Graduate School" THEN "Student/trainee"
ELSEIF [Max Edu Enroll] = "High/Middle School" THEN "Student/trainee HS/GED"
ELSEIF [Max Edu Enroll] = "Not Enrolled in School" THEN "Not a student/trainee"
ELSEIF [Max Edu Enroll] = "Unknown" THEN "Unknown/Did not Report"
ELSEIF [Max Edu Enroll] = "Vocational College" THEN "Student/trainee"
ELSEIF ([mcafss_edu2_enroll] = "YES" ### Enrolled
        AND
        ([mcafss_edu2_prog] = 1 ### Enrolled in Middle School
        OR
        [mcafss_edu2_prog] = 2 ### Enrolled in High School
        OR
        [mcafss_edu2_prog] = 3 ### Enrolled in GED
        )) THEN "Student/trainee HS/GED" ###LLCHD
ELSEIF [mcafss_edu2_enroll] = "NO" THEN "Not a student/trainee"
ELSE "Unknown/Did not Report"
END
###Student/trainee indicates enrollment in a program other than a high school diploma or GED
###LLCHD - Kodi sent this coding for mcafss_edu2_prog on 12/7/2021
###01 = Middle School
###02 = High School
###03 = GED
###04 = ESL
###05 = Adult education in basic reading or math
###06 = College
###07 = Vocational training, technical or trade school (excluding training received during HS)
###08 = Job search or job placement
###09 = Work experience
###10 = Other (Specify)

df3['_C15 Max Educational Status'] = 
###LLCHD
IF [Mcafss Edu2] = 1 THEN "Less than HS diploma" ### Less than 8th Grade
ELSEIF [Mcafss Edu2] = 2 THEN "Less than HS diploma" ### 8-11th Grade
ELSEIF [Mcafss Edu2] = 3 THEN "HS diploma/GED" ### High School Grad
ELSEIF [Mcafss Edu2] = 4 THEN "HS diploma/GED" ###Completed a GED
ELSEIF [Mcafss Edu2] = 5 THEN "Technical Training or Associates Degree" ### Vocational School after High School
ELSEIF [Mcafss Edu2] = 6 THEN "Some college/training" ### Some College
ELSEIF [Mcafss Edu2] = 7 THEN "Technical Training or Associates Degree" ### Associates Degree
ELSEIF [Mcafss Edu2] = 8 THEN "Bachelor's Degree or Higher"  ### Bachelors Degree or Higher
### ELSEIF [Mcafss Edu2] = 9 THEN "HS diploma/GED" ###currently enrolled in college - vocational training or trade apprenticeship
### ELSEIF [Mcafss Edu2] = 10 THEN "HS diploma/GED"  ###currently not enrolled in college - vocational training or trade apprenticeship
### ELSEIF [Mcafss Edu2] = 11 THEN "Other" ###other education
### ELSEIF [Mcafss Edu2] = 12 THEN "Unknown/Did Not Report" ###unknown/did not report
ELSEIF [Mcafss Edu2] = 0 THEN "Unknown/Did Not Report" ###unknown/did not report (missing data)
ELSEIF [Mcafss Edu2] >= 9 THEN "Unknown/Did Not Report"
###FW
ELSEIF [AD1MaxEdu] = "8th Grade or less" THEN "Less than HS diploma"
ELSEIF [AD1MaxEdu] = "Some High School" THEN "Less than HS diploma"
ELSEIF [AD1MaxEdu] = "GED" THEN "HS diploma/GED"
ELSEIF [AD1MaxEdu] = "High School Graduate" THEN "HS diploma/GED"
ELSEIF [AD1MaxEdu] = "Achievement Certificate" THEN "Technical Training or Certification"
ELSEIF [AD1MaxEdu] = "Certificate Program" THEN "Technical Training or Certification"
ELSEIF [AD1MaxEdu] = "Some College" THEN "Some college/training"
ELSEIF [AD1MaxEdu] = "Associates or Two Year Technical Degree" THEN "Technical Training or Associates Degree" ###these are two serparate categories on F1
ELSEIF [AD1MaxEdu] = "Two Year Degree" THEN "Associate's Degree"
ELSEIF [AD1MaxEdu] = "Four Year College Degree" THEN "Bachelor's Degree or Higher"
ELSEIF [AD1MaxEdu] = "Graduate School" THEN "Bachelor's Degree or Higher"
ELSEIF [AD1MaxEdu] = "Unknown" THEN "Unknown/Did Not Report"
ELSEIF [AD1MaxEdu] = "null" THEN "Unknown/Did Not Report"
ELSEIF ISNULL([Mcafss Edu2]) AND ISNULL([AD1MaxEdu]) THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END
###LLCHD Code from Kodi on 11/30/2021
###1 – Less than 8th Grade
###2 – 8-11th Grade
###3 – High School Grad
###4 - Completed a GED
###5 - Vocational School after High School
###6 – Some College
###7 – Associates Degree 
###8 - Bachelors Degree or Higher
###Confirmed 9-12 are old and no longer needed - new LLCHD variables are sent to confirm enrollment

df3['_C15 Min Education Date'] = DATE(IFNULL([Mcafss Edu Dt1],[Min Education Date]))

df3['_C15 Min Educational Enrollment'] = 
IF [Min Edu Enroll] = "College 2 Year" THEN "Student/trainee" ###FW
ELSEIF [Min Edu Enroll] = "College 4 Year" THEN "Student/trainee"
ELSEIF [Min Edu Enroll] = "ESL" THEN "Student/trainee"
ELSEIF [Min Edu Enroll] = "GED Program" THEN "Student/trainee HS/GED"
ELSEIF [Min Edu Enroll] = "Graduate School" THEN "Student/trainee"
ELSEIF [Min Edu Enroll] = "High/Middle School" THEN "Student/trainee HS/GED"
ELSEIF [Min Edu Enroll] = "Not Enrolled in School" THEN "Not a student/trainee"
ELSEIF [Min Edu Enroll] = "Unknown" THEN "Unknown/Did not Report"
ELSEIF [Min Edu Enroll] = "Vocational College" THEN "Student/trainee"
ELSEIF ([mcafss_edu1_enroll] = "YES" ### Enrolled
        AND
        ([mcafss_edu1_prog] = 1 ### Enrolled in Middle School
        OR
        [mcafss_edu1_prog] = 2 ### Enrolled in High School
        OR
        [mcafss_edu1_prog] = 3 ### Enrolled in GED
        )) THEN "Student/trainee HS/GED" ###LLCHD
ELSEIF [mcafss_edu1_enroll] = "NO" THEN "Student/trainee"
ELSE "Unknown/Did not Report"
END
###Student/trainee indicates enrollment in a program other than a high school diploma or GED
###LLCHD - Kodi sent this coding for mcafss_edu1_prog on 12/7/2021
###01 = Middle School
###02 = High School
###03 = GED
###04 = ESL
###05 = Adult education in basic reading or math
###06 = College
###07 = Vocational training, technical or trade school (excluding training received during HS)
###08 = Job search or job placement
###09 = Work experience
###10 = Other (Specify)

df3['_C15 Min Educational Status'] = 
IF [Mcafss Edu1] = 1 THEN "Less than HS diploma" ###LLCHD ### Less than 8th Grade
ELSEIF [Mcafss Edu1] = 2 THEN "Less than HS diploma" ### 8-11th Grade
ELSEIF [Mcafss Edu1] = 3 THEN "HS diploma/GED" ### High School Grad
ELSEIF [Mcafss Edu1] = 4 THEN "HS diploma/GED" ###Completed a GED
ELSEIF [Mcafss Edu1] = 5 THEN "Technical Training or Associates Degree" ### Vocational School after High School
ELSEIF [Mcafss Edu1] = 6 THEN "Some college/training" ### Some College
ELSEIF [Mcafss Edu1] = 7 THEN "Technical Training or Associates Degree" ### Associates Degree
ELSEIF [Mcafss Edu1] = 8 THEN "Bachelor's Degree or Higher"  ### Bachelors Degree or Higher
### ELSEIF [Mcafss Edu1] = 9 THEN "HS diploma/GED"
### ELSEIF [Mcafss Edu1] = 10 THEN "HS diploma/GED" 
### ELSEIF [Mcafss Edu1] = 11 THEN "Other"
### ELSEIF [Mcafss Edu1] = 12 THEN "Unknown/Did Not Report"
ELSEIF [Mcafss Edu1] = 0 THEN "Unknown/Did Not Report"
ELSEIF [Mcafss Edu1] >= 9 THEN "Unknown/Did Not Report"
ELSEIF [AD1MinEdu] = "8th Grade or less" THEN "Less than HS diploma"
ELSEIF [AD1MinEdu] = "Some High School" THEN "Less than HS diploma"
ELSEIF [AD1MinEdu] = "GED" THEN "HS diploma/GED"
ELSEIF [AD1MinEdu] = "High School Graduate" THEN "HS diploma/GED"
ELSEIF [AD1MinEdu] = "Achievement Certificate" THEN "Technical Training or Certification"
ELSEIF [AD1MinEdu] = "Certificate Program" THEN "Technical Training or Certification"
ELSEIF [AD1MinEdu] = "Some College" THEN "Some college/training"
ELSEIF [AD1MinEdu] = "Associates or Two Year Technical Degree" THEN "Technical Training or Associates Degree" ###these are two serparate categories on F1
ELSEIF [AD1MinEdu] = "Two Year Degree" THEN "Associate's Degree"
ELSEIF [AD1MinEdu] = "Four Year College Degree" THEN "Bachelor's Degree or Higher"
ELSEIF [AD1MinEdu] = "Graduate School" THEN "Bachelor's Degree or Higher"
ELSEIF [AD1MinEdu] = "Unknown" THEN "Unknown/Did Not Report"
ELSEIF [AD1MinEdu] = "null"  THEN  "Unknown/Did Not Report"
ELSEIF ISNULL([Mcafss Edu1])AND ISNULL([AD1MinEdu]) THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END
###LLCHD Code from Kodi on 11/30/2021
###1 – Less than 8th Grade
###2 – 8-11th Grade
###3 – High School Grad
###4 - Completed a GED
###5 - Vocational School after High School
###6 – Some College
###7 – Associates Degree 
###8 - Bachelors Degree or Higher
###Confirmed 9-12 are old and no longer needed - new LLCHD variables are sent to confirm enrollment

df3['_C16 CG Insurance 1 Status'] = 
CASE [AD1PrimaryIns.1] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 10 Status'] = 
CASE [AD1PrimaryIns.10] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 11 Status'] = 
CASE [AD1PrimaryIns.11] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 12 Status'] = 
CASE [AD1PrimaryIns.12] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 13 Status'] = 
CASE [AD1PrimaryIns.13] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 14 Status'] = 
CASE [AD1PrimaryIns.14] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 15 Status'] = 
CASE [AD1PrimaryIns.15] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 16 Status'] = 
CASE [AD1PrimaryIns.16] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 2 Status'] = 
CASE [AD1PrimaryIns.2] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 3 Status'] = 
CASE [AD1PrimaryIns.3] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 4 Status'] = 
CASE [AD1PrimaryIns.4] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 5 Status'] = 
CASE [AD1PrimaryIns.5] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 6 Status'] = 
CASE [AD1PrimaryIns.6] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 7 Status'] = 
CASE [AD1PrimaryIns.7] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 8 Status'] = 
CASE [AD1PrimaryIns.8] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C16 CG Insurance 9 Status'] = 
CASE [AD1PrimaryIns.9] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "SCHIP" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
###LLCHD
    WHEN "1" THEN "Medicaid or CHIP"
    WHEN "2" THEN "Tri-Care"
    WHEN "3" THEN "Private or Other"
    WHEN "4" THEN "Unknown/Did Not Report"
    WHEN "5" THEN "No Insurance Coverage"
    WHEN "6" THEN "Unknown/Did Not Report"
    WHEN "99" THEN "Unknown/Did Not Report"
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Private" THEN "Private or Other"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Uninsure" THEN "No Insurance Coverage"
    WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    WHEN NULL THEN "Unknown/Did Not Report"
ELSE "Unrecognized Value"
END

df3['_C17 90 Day CES-D Date'] = DATE(DATEADD('day',90, [_C03 CES-D Date]))

df3['_C17 MH Referral Date'] = IFNULL([Ment Hlth Ref Dt],[Min Of MH Ref Date])

df3['_C19 90 Day IPV Date'] = DATE(DATEADD('day',90, [_C14 IPV Date]))

df3['_C19 IPV Referral Date'] = IFNULL([Ipv Referral Dt],[IPV Ref Date])

df3['_C19 IPV Screen Result'] = IFNULL([_IPV Score FW],[Ipv Screen])

df3['_Discharge Date'] = IFNULL([Termination Date],[Discharge Dt])

df3['_Enroll 3 Month Date'] = DATE(DATEADD('month',3,[_Enrollment Date]))

df3['_Enroll Preg Status'] = 
IF [Pregnancystatus] = 0 THEN "Pregnant" ###FW
ELSEIF [Pregnancystatus] = 1 THEN "Not pregnant"
ELSEIF [Enroll Preg Status] = "Postpartum" THEN "Not pregnant" ###LLCHD
ELSEIF [Enroll Preg Status] = "Pregnant" THEN "Pregnant"
ELSE NULL
END

df3['_Enrollment Date'] = IFNULL([Min Of HV Date],[Enroll Dt])

df3['_Family ID'] = IFNULL([Family Id], [Family Number])

df3['_FOB DOB'] = 
IF [Fob Dob] = DATE(1/1/1900) THEN NULL ###LLCHD
ELSEIF [Fobdob] = DATE(1/1/1900) THEN NULL ###FW
ELSE IFNULL([Fob Dob],[Fobdob])
END

df3['_FOB Gender'] = 
###should we incorporate involved status into the fob variables?
IF [Fob Involved1] = "Y" THEN CASE[Fob Gender]
    WHEN "M" THEN "Male" ###LLCHD
    WHEN "F" THEN "Female"
    ### WHEN "N" THEN "Non-Binary" ### No values yet - confirm
    END
ELSEIF [Fob Involved] = True THEN [Adult2Gender] ###FW
ELSE NULL
END

df3['_FOB Relation'] = 
IF [Fob Involved1] = "Y" THEN "FOB"
ELSEIF [Fob Involved] = True
THEN CASE [Adult2TGTRelation]
    WHEN "Biological father" THEN "FOB"
    WHEN "Biological mother" THEN "MOB"
    WHEN "FOB" THEN "FOB"
    WHEN "Foster father" THEN "FOB"
    WHEN "Guardian" THEN "Guardian"
    WHEN "Grandmother" THEN "Grandmother"
    WHEN "MOB" THEN "MOB"
    WHEN "Other" THEN "Other"
    END
ELSE NULL
END

df3['_IPV Score FW'] = 
IF [Agency] <> "ll" THEN
    (IF [Assess Afraid] = TRUE 
    OR [Assess IPV] = TRUE 
    OR [Assess Police] = TRUE
    THEN "P" ELSE "N" END)
END

df3['_Max HV Date'] = IFNULL([Max Of HV Date],[Last Home Visit])

df3['_MOB DOB'] = 
IF [Mob Dob] = DATE(1/1/1900) THEN NULL ###LLCHD
ELSEIF [Mobdob] = DATE(1/1/1900) THEN NULL ###FW
ELSE IFNULL([Mob Dob],[Mobdob])
END

df3['_MOB Gender'] = 
IF [Adult1Gender] = "Female" THEN "Female" ###FW
ELSEIF [Adult1Gender] = "Male" THEN "Male"
ELSEIF [Adult1Gender] = "Non-Binary" THEN "Non-Binary"
ELSEIF [Mob Gender]= "F" THEN "Female" ###LLCHD
ELSEIF [Mob Gender] = "M" THEN "Male"
### ELSEIF [Mob Gender] = "N" THEN "Non-Binary" ### Don't have this value yet - Confirm
END

df3['_MOB TGT Relation'] = 
IF [Adult1TGTRelation] = "Biological mother" THEN "MOB" ###FW
ELSEIF  [Adult1TGTRelation] = "Biological father" THEN "FOB"
ELSEIF  [Adult1TGTRelation] = "FOB" THEN "FOB"
ELSEIF  [Adult1TGTRelation] = "MOB" THEN "MOB"
ELSEIF  [Adult1TGTRelation] = "Adoptive father" THEN "FOB"
ELSEIF  [Adult1TGTRelation] = "Adoptive mother" THEN "MOB"
ELSEIF  [Adult1TGTRelation] = "Foster mother" THEN "MOB"
ELSEIF  [Adult1TGTRelation] = "Grandmother" THEN "MOB"
ELSEIF  [Adult1TGTRelation] = "Guardian" THEN "MOB"
ELSEIF NOT ISNULL([Adult1TGTRelation]) THEN "Unrecognized Value"
ELSEIF [Primary Relation] = "FATHER OF CHILD" THEN "FOB" ###LLCHD
ELSEIF [Primary Relation] = "MOTHER OF CHILD" THEN "MOB"
ELSEIF [Primary Relation] = "PRIMARY CAREGIVER" AND [Mob Gender] = "F" THEN "MOB"
ELSEIF [Primary Relation] = "PRIMARY CAREGIVER" AND [Mob Gender] = "M" THEN "FOB"
ELSEIF NOT ISNULL([Primary Relation]) THEN "Unrecognized Value"
END

df3['_Need Exclusion 1 - Sub Abuse'] = 
IF [Need Exclusion1] = "Substance Abuse" THEN "Alcohol/Drug Abuse" ###FW
    ELSEIF [Need Exclusion1] = "Drug Abuse" THEN "Alcohol/Drug Abuse"
    ELSEIF [Need Exclusion1] = "Alcohol Abuse" THEN "Alcohol/Drug Abuse"
ELSEIF [need exclusion1 (LLCHD)] = "Y" THEN "Alcohol/Drug Abuse" ###LLCHD
END

df3['_Need Exclusion 2 - Fam Plan'] = 
IF [Need Exclusion2] = "Family Planning" THEN "Family Planning" ###FW
ELSEIF [need exclusion2 (LLCHD)] = "Y" THEN "Family Planning" ###LLCHD
END

df3['_Need Exclusion 3 - Mental Health'] = 
IF [Need Exclusion3] = "Mental Health" THEN "Mental Health" ###FW
ELSEIF [need exclusion3 (LLCHD)] = "Y" THEN "Mental Health" ###LLCHD
END

df3['_Need Exclusion 5 - IPV'] = 
IF [Need Exclusion5] = "IPV Services" THEN "IPV Services" ###FW
ELSEIF [need exclusion5 (LLCHD)] = "Y" THEN "IPV Services" ###LLCHD
END

df3['_Need Exclusion 6 - Tobacco'] = 
IF [Need Exclusion6] = "Tobacco Cessation" THEN "Tobacco Cessation" ###FW
ELSEIF [need_exclusion6] = "Y" THEN "Tobacco Cessation" ###LLCHD
END

df3['_T06 FOB Ethnicity'] = 
IF [Fob Involved] = True ###FW
THEN CASE [Fob Ethnicity]
    WHEN "Non Hispanic/Latino" THEN "Not Hispanic or Latino" 
    WHEN "Hispanic/Latino" THEN "Hispanic or Latino"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSEIF [Fob Involved1] = "Y" ###LLCHD
THEN CASE [Fob Ethnicity1]
    WHEN "HISPANIC/LATINO" THEN "Hispanic or Latino" 
    WHEN "NOT HISPANIC/LATINO" THEN "Not Hispanic or Latino"
    WHEN "NON-HISPANIC" THEN "Not Hispanic or Latino"
    WHEN "UNREPORTED/REFUSED TO REPORT" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSE NULL
END

### Duplicate? Used?
df3['_T06 FOB Ethnicity (1)'] = 
IF [Fob Involved] = True ###FW
THEN CASE [Fob Ethnicity]
    WHEN "Non Hispanic/Latino" THEN "Not Hispanic or Latino" 
    WHEN "Hispanic/Latino" THEN "Hispanic or Latino"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSEIF [Fob Involved1] = "Y" ###LLCHD
THEN CASE [Fob Ethnicity1]
    WHEN "HISPANIC/LATINO" THEN "Hispanic or Latino" 
    WHEN "NOT HISPANIC/LATINO" THEN "Not Hispanic or Latino"
    WHEN "NON-Hispanic" THEN "Not Hispanic or Latino"
    WHEN "UNREPORTED/REFUSED TO REPORT" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSE NULL
END

df3['_T06 MOB Ethnicity'] = 
IF NOT ISNULL([Mob Ethnic]) THEN CASE [Mob Ethnic]
    WHEN "Non Hispanic/Latino" THEN "Not Hispanic or Latino"
    WHEN "Hispanic/Latino" THEN "Hispanic or Latino"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSEIF NOT ISNULL([Mob Ethnicity]) THEN CASE [Mob Ethnicity]
    WHEN "HISPANIC/LATINO" THEN "Hispanic or Latino" ###LLCHD
    WHEN "Hispanic" THEN "Hispanic or Latino"
    WHEN "NOT HISPANIC/LATINO" THEN "Not Hispanic or Latino"
    WHEN "NON-Hispanic" THEN "Not Hispanic or Latino"
    ELSE "Unrecognized Value"
    END
ELSE "Unknown/Did Not Report"
END

df3['_T07 FOB Race'] = 
###LLCHD
###multiracial
IF [Fob Involved1]= "Y" THEN 
    (IF(IIF([Fob Race Asian] ="Y",1,0,0)+IIF([Fob Race Black] ="Y",1,0,0)+IIF([Fob Race Hawaiian]="Y",1,0,0)
    +IIF([Fob Race Indian]="Y",1,0,0)+IIF([Fob Race Other]="Y",1,0,0)+IIF([Fob Race White]="Y",1,0,0) > 1) THEN "More than one race"
    ###single race
    ELSEIF [Fob Race Asian] = "Y" THEN "Asian"
    ELSEIF [Fob Race Black] = "Y" THEN "Black or African American"
    ELSEIF [Fob Race Hawaiian] = "Y" THEN "Native Hawaiian or Other Pacific Islander"
    ELSEIF [Fob Race Indian] = "Y" THEN "American Indian or Alaska Native"
    ELSEIF [Fob Race White] = "Y" THEN "White"
    ELSEIF [Fob Race Other] = "Y" THEN "Other"
    ELSE "Unknown/Did Not Report"
    END)
###FW
###multiracial, = "True" is not required in IIF statement because race is boolean
ELSEIF [Fob Involved]= True THEN 
    (IF(IIF([FOB Race Asian],1,0,0)+IIF([FOB Race Black],1,0,0)+IIF([FOB Race Hawaiian Pacific],1,0,0)
    +IIF([FOB Race Indian Alaskan],1,0,0)+IIF([FOB Race White],1,0,0) +IIF([FOB Race Other],1,0,0) > 1) THEN "More than one race"
    ###single race
    ELSEIF [FOB Race Asian] = True THEN "Asian"
    ELSEIF [FOB Race Black] = True THEN "Black or African American"
    ELSEIF [FOB Race Hawaiian Pacific] = True THEN "Native Hawaiian or Other Pacific Islander"
    ELSEIF [FOB Race Indian Alaskan] = True THEN "American Indian or Alaska Native"
    ELSEIF [FOB Race White] = True THEN "White"
    ELSEIF [FOB Race Other] = True THEN "Other"
    ELSE "Unknown/Did Not Report"
    END)
ELSE NULL
END

df3['_T07 MOB Race'] = 
###LLCHD
###multiracial
IF IIF([Mob Race Asian]="Y",1,0,0)+IIF([Mob Race Black]="Y",1,0,0)+IIF([Mob Race Hawaiian]="Y",1,0,0)+IIF([Mob Race Indian]="Y",1,0,0)
+IIF([Mob Race Other]="Y",1,0,0)+IIF([Mob Race White]="Y",1,0,0) > 1 THEN "More than one race"
###single race
ELSEIF [Mob Race Asian] = "Y" THEN "Asian"
ELSEIF [Mob Race Black] = "Y" THEN "Black or African American"
ELSEIF [Mob Race Hawaiian] = "Y" THEN "Native Hawaiian or Other Pacific Islander"
ELSEIF [Mob Race Indian] = "Y" THEN "American Indian or Alaska Native"
ELSEIF [Mob Race White] = "Y" THEN "White"
ELSEIF [Mob Race Other] = "Y" THEN "Other"
###FW
###multiracial, = "True" is not required in IIF statement because race is boolean
ELSEIF IIF([MOB Race Asian],1,0,0)+IIF([MOB Race Black],1,0,0)+IIF([MOB Race Hawaiian Pacific],1,0,0)
+IIF([MOB Race Indian Alaskan],1,0,0)+IIF([MOB Race White],1,0,0) +IIF([MOB Race Other],1,0,0) > 1 
THEN "More than one race"
###single race
ELSEIF [MOB Race Asian] = True THEN "Asian"
ELSEIF [MOB Race Black] = True THEN "Black or African American"
ELSEIF [MOB Race Hawaiian Pacific] = True THEN "Native Hawaiian or Other Pacific Islander"
ELSEIF [MOB Race Indian Alaskan] = True THEN "American Indian or Alaska Native"
ELSEIF [MOB Race White] = True THEN "White"
ELSEIF [MOB Race Other] = True THEN "Other"
ELSE "Unknown/Did Not Report"
END

df3['_T08 FOB Marital Status'] = 
###FW
IF [Fob Involved] = True THEN CASE [Adult2MaritalStatus]
    WHEN "Divorced" THEN "Separated, Divorced, or Widowed"
    WHEN "Living with Partner" THEN "Not Married but Living Together with Partner"
    WHEN "Married" THEN "Married"
    WHEN "Separated" THEN "Separated, Divorced, or Widowed"
    WHEN "Single" THEN "Never Married"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
###LLCHD
ELSEIF [Fob Involved1] = "Y" THEN CASE [Mob Marital Status] 
    WHEN "DIVORCED" THEN "Separated, Divorced, or Widowed"
    WHEN "LEGALLY SEPARATED" THEN "Separated, Divorced, or Widowed"
    WHEN "LIFE PARTNER" THEN "Not Married but Living Together with Partner"
    WHEN "MARRIED" THEN "Married"
    WHEN "SEPARATED" THEN "Separated, Divorced, or Widowed"
    WHEN "SINGLE" THEN "Never Married"
    WHEN "Not Married" THEN "Never Married"
    WHEN "UNKNOWN" THEN "Unknown/Did Not Report"
    WHEN "WIDOWED" THEN "Separated, Divorced, or Widowed"
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSE NULL
END

df3['_T08 MOB Marital Status'] = 
###FW
IF NOT ISNULL([Adult1MaritalStatus]) THEN CASE [Adult1MaritalStatus]
    WHEN "Divorced" THEN "Separated, Divorced, or Widowed"
    WHEN "Living with Partner" THEN "Not Married but Living Together with Partner"
    WHEN "Married" THEN "Married"
    WHEN "Separated" THEN "Separated, Divorced, or Widowed"
    WHEN "Single" THEN "Never Married"
    WHEN "Widowed" THEN "Separated, Divorced, or Widowed"
    WHEN "Legally Separated" THEN "Separated, Divorced, or Widowed"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
###LLCHD
ELSEIF NOT ISNULL([Mob Marital Status]) THEN CASE [Mob Marital Status] 
    WHEN "DIVORCED" THEN "Separated, Divorced, or Widowed"
    WHEN "Divorced" THEN "Separated, Divorced, or Widowed"
    WHEN "LEGALLY SEPARATED" THEN "Separated, Divorced, or Widowed"
    WHEN "LIFE PARTNER" THEN "Not Married but Living Together with Partner"
    WHEN "MARRIED" THEN "Married"
    WHEN "Married" THEN "Married"
    WHEN "SEPARATED" THEN "Separated, Divorced, or Widowed"
    WHEN "Separated" THEN "Separated, Divorced, or Widowed"
    WHEN "SINGLE" THEN "Never Married"
    WHEN "UNKNOWN" THEN "Unknown/Did Not Report"
    WHEN "WIDOWED" THEN "Separated, Divorced, or Widowed"
    WHEN "Not Married" THEN "Never Married"
    ELSE "Unrecognized Value"
    END
ELSE "Unknown/Did Not Report"
END

df3['_T09 FOB Education Status'] = 
IF [Fob Involved]= True THEN CASE[AD2EDLevel] ###FW
    WHEN "8th Grade or less" THEN "Less than HS diploma"
    WHEN "Some High School" THEN "Less than HS diploma"
    WHEN "GED" THEN "HS diploma/GED"
    WHEN "High School Graduate" THEN "HS diploma/GED"
    WHEN "Achievement Certificate" THEN "Some college/training" ###is this the right category?
    WHEN "Some College" THEN "Some college/training"
    WHEN "Associates or Two Year Technical Degree" THEN "Technical Training or Associates Degree" ###these are two serparate categories on F1
    WHEN "Four Year College Degree" THEN "Bachelor's Degree or Higher"
    WHEN "Graduate School" THEN "Bachelor's Degree or Higher"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
    END 
ELSEIF [Fob Involved1]= "Y" THEN CASE [Fob Edu] ###LLCHD
    WHEN 01 THEN "Less than HS diploma"
    WHEN 02 THEN "Less than HS diploma"
    WHEN 03 THEN "HS diploma/GED"
    WHEN 04 THEN "HS diploma/GED"
    WHEN 05 THEN "Vocational School after High School"
    WHEN 06 THEN "Some college/training"
    WHEN 07 THEN "Associates Degree" ###these are two serparate categories on F1
    WHEN 08 THEN "Bachelor's Degree or Higher"
    WHEN 00 THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
    END
ELSE NULL
END

df3['_T10 FOB Educational Enrollment'] = 
###max
IF [Fob Involved] = True THEN CASE[AD2InSchool] ###FW
    WHEN "College 2 Year" THEN "Student/trainee" 
    WHEN "College 4 Year" THEN "Student/trainee"
    WHEN "ESL" THEN "Student/trainee"
    WHEN "GED Program" THEN "Student/trainee" 
    WHEN "Graduate School" THEN "Student/trainee"
    WHEN "High/Middle School" THEN "Student/trainee" 
    WHEN "Not Enrolled in School" THEN "Not a student/trainee"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "Vocational College" THEN "Student/trainee"
    WHEN NULL THEN "Unknown/Did Not Report"
    END
ELSEIF [Fob Involved1] = "Y" THEN CASE[Fob Edu] ###LLCHD
    WHEN 1 THEN "Student/trainee"
    WHEN 2 THEN "Not a student/trainee"
    WHEN 3 THEN "Not a student/trainee"
    WHEN 4 THEN "Not a student/trainee"
    WHEN 5 THEN "Not a student/trainee"
    WHEN 6 THEN "Not a student/trainee"
    WHEN 7 THEN "Not a student/trainee"
    WHEN 8 THEN "Not a student/trainee"
    WHEN 9 THEN "Student/trainee"
    WHEN 10 THEN "Not a student/trainee"
    WHEN 11 THEN "Not a student/trainee"
    WHEN 12 THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
    END
ELSE NULL
END

df3['_T11 FOB Employment'] = 
IF [Fob Involved] = True THEN CASE [AD2EmployStatus] ###FW
    WHEN "Employed Full Time" THEN "Employed Full Time"
    WHEN "Employed Part Time" THEN "Employed Part Time"
    WHEN "Maternal leave, paid, full time" THEN "Employed Full Time"
    WHEN "Maternal leave, unpaid, full time" THEN "Employed Full Time"
    WHEN "Maternal leave, unpaid, part time" THEN "Employed Part Time"
    WHEN "Permanent Disability" THEN "Not Employed"
    WHEN "Self-Employed" THEN "Employed Part Time"
    WHEN "Unemployed - Unspecified" THEN "Not Employed"
    WHEN "Unemployed Not Seeking Work-Barriers" THEN "Not Employed"
    WHEN "Unemployed Not Seeking Work-Preference" THEN "Not Employed"
    WHEN "Unemployed Not Seeking Work-Teen Caregiver" THEN "Not Employed"
    WHEN "Unemployed Seeking Work" THEN "Not Employed"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN "null" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSEIF [Fob Involved1] = "Y" THEN CASE [Fob Employ] ###LLCHD
    WHEN 1 THEN "Not Employed"
    WHEN 2 THEN "Not Employed"
    WHEN 3 THEN "Employed Part Time"
    WHEN 4 THEN "Employed Full Time"
    WHEN 5 THEN "Employed Full Time"
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSE NULL
END

df3['_T11 MOB Employment'] = 
IF NOT ISNULL([AD1EmpStatus]) THEN CASE [AD1EmpStatus] ###FW
    WHEN "Employed Full Time" THEN "Employed Full Time"
    WHEN "Employed Part Time" THEN "Employed Part Time"
    WHEN "Maternal leave, paid, full time" THEN "Employed Full Time"
    WHEN "Maternal leave, unpaid, full time" THEN "Employed Full Time"
    WHEN "Maternal leave, unpaid, part time" THEN "Employed Part Time"
    WHEN "Permanent Disability" THEN "Not Employed"
    WHEN "Self-Employed" THEN "Employed Part Time"
    WHEN "Temporary Disability" THEN "Not Employed"
    WHEN "Unemployed - Unspecified" THEN "Not Employed"
    WHEN "Unemployed Not Seeking Work-Barriers" THEN "Not Employed"
    WHEN "Unemployed Not Seeking Work-Preference" THEN "Not Employed"
    WHEN "Unemployed Not Seeking Work-Teen Caregiver" THEN "Not Employed"
    WHEN "Unemployed Seeking Work" THEN "Not Employed"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSEIF NOT ISNULL([Mcafss Employ])THEN CASE [Mcafss Employ] ###LLCHD
    WHEN 1 THEN "Not Employed"
    WHEN 2 THEN "Not Employed"
    WHEN 3 THEN "Employed Part Time"
    WHEN 4 THEN "Employed Full Time"
    WHEN 5 THEN "Employed Full Time"
    ELSE "Unrecognized Value"
    END
ELSE "Unknown/Did Not Report"
END

df3['_T12 MOB Housing Status'] = 
IF [_Agency]<> "ll" THEN CASE [Housing Status] ###FW
    WHEN "Homeless and living in an emergency or transitional shelter" THEN "Homeless and living in an emergency or transition shelter" ###Homeless and living in emergency or transitional shelter
    WHEN "Homeless and sharing housing" THEN "Homeless and sharing housing"
    WHEN "Live in public housing" THEN "Lives in public housing"
    WHEN "Lives with parent or family member" THEN "Lives with parent or family member"
    WHEN "Other" THEN  "Some other arrangement" ###Not sure this is the right category
    WHEN "Owns or shares own home, condominium, or apartment" THEN "Owns or shares own home, condominium, or apartment"
    WHEN "Rents of shares own home or apartment" THEN "Rents or shares own home or apartment"
    WHEN "Rents or shares own home or apartment" THEN "Rents or shares own home or apartment"
    WHEN "Some other arrangement" THEN "Some other arrangement"
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value" ###will have to add new FW values as they come in, they aren't all here
    END
ELSEIF [_Agency] = "ll" THEN CASE[Mob Living Arrangement] ###LLCHD
    WHEN 1 THEN "Owns or shares own home, condominium, or apartment" ###Owns or shared own home, condo, or apartment
    WHEN 2 THEN "Rents or shares own home or apartment" ###Rents or shared own home or apartment
    WHEN 3 THEN "Lives in public housing" ###Lives in public housing
    WHEN 4 THEN "Lives with parent or family member" ###Lives with parent or family member
    WHEN 5 THEN "Not homeless, some other arrangement" ###Some other arrangement
    WHEN 6 THEN "Homeless and sharing housing" ###Homeless and sharing housing
    WHEN 7 THEN "Homeless and living in an emergency or transition shelter" ###Homeless and living in emergency or transitional shelter
    WHEN 8 THEN "Homeless, some other arrangement" ###Homeless with some other arrangement
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
END

df3['_T14 Federal Poverty Categories'] = 
IF [_T14 Poverty Percent] <= .50 THEN "50% and Under"
ELSEIF [_T14 Poverty Percent] <= 1.00 THEN "51-100%"
ELSEIF [_T14 Poverty Percent] <= 1.33 THEN "101-133%"
ELSEIF [_T14 Poverty Percent] <= 2.00 THEN "134-200%"
ELSEIF [_T14 Poverty Percent] <= 3.00 THEN "201-300%"
ELSEIF [_T14 Poverty Percent] > 3.00  THEN ">300%"
ELSEIF NULL THEN "Unknown/Did Not Report"
END

df3['_T14 Poverty Percent'] = 
IF [_Agency] = "ll" AND ISNULL([Household Income]) THEN NULL ###LLCHD
ELSEIF [_Agency] = "ll" AND ISNULL([Household Size]) THEN NULL
ELSEIF [_Agency] = "ll" THEN [Household Income]/[_T14 Federal Poverty Level update]
ELSEIF [_Agency] <> "ll" THEN [Poverty Level] ###FW
END

df3['_T17 Discharge Reason'] = 
IF NOT ISNULL([Discharge Dt]) THEN CASE [Discharge Reason] ###LLCHD, see full reasons below
    WHEN "1" THEN "Completed Services" 
    WHEN "Family Has Met Program Goals" THEN "Completed Services"
    ELSE "Stopped Services Before Completion"
    END
ELSEIF NOT ISNULL([Termination Date]) THEN CASE [Termination Status] ###FW
    WHEN "Family graduated/met all program goals" THEN "Completed Services"
    ELSE "Stopped Services Before Completion"
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

df3['_T20 FOB Insurance'] = 
IF [Fob Involved] = True THEN CASE [AD2InsPrimary] ###FW
    WHEN "Medicaid" THEN "Medicaid or CHIP"
    WHEN "Medicare" THEN "Other" ###this is what our previous syntax indicated
    WHEN "None" THEN "No Insurance Coverage"
    WHEN "Other" THEN "Private or Other"
    WHEN "Private" THEN "Private or Other"
    WHEN "Tri-Care" THEN "Tri-Care"
    WHEN "Unknown" THEN "Unknown/Did Not Report"
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSEIF [Fob Involved1] = "Y" THEN CASE [Hlth Insure Fob] ###LLCHD
    WHEN 1 THEN "Medicaid or CHIP"
    WHEN 2 THEN "Tri-Care"
    WHEN 3 THEN "Private or Other"
    WHEN 4 THEN "Unknown/Did Not Report"
    WHEN 5 THEN "No Insurance Coverage"
    WHEN NULL THEN "Unknown/Did Not Report"
    ELSE "Unrecognized Value"
    END
ELSE NULL
END

df3['_TGT 3 Month Date'] = DATE(DATEADD('month',3,[_TGT DOB]))

df3['_TGT DOB'] = 
IF [Tgt Dob] = DATE(1/1/1900) THEN NULL ###LLCHD
ELSEIF [Tgt Dob-Cr] = DATE(1/1/1900) THEN NULL ###FW
ELSE IFNULL([Tgt Dob],[Tgt Dob-Cr])
END

df3['_TGT EDC Date'] = 
IF [Dt Edc] = DATE(1/1/1900) THEN NULL ###LLCHD
ELSEIF [EDC Date] = DATE(1/1/1900) THEN NULL ###FW
ELSE IFNULL([Dt Edc],[EDC Date])
END

df3['_TGT ID'] = IFNULL([Tgt Id],[Child Number])

df3['_UNCOPE C Recode'] = 
IF [C] = "Yes" THEN INT(1)
ELSEIF [C] = "No" THEN INT(0)
END

df3['_UNCOPE Date'] = IFNULL([Uncope Dt],[Dateuncope])

df3['_UNCOPE Referral'] = IFNULL([Substance Abuse Ref Dt], [UNCOPE Ref Date])

df3['_UNCOPE E Recode'] = 
IF [E] = "Yes" THEN INT(1)
ELSEIF [E] = "No" THEN INT(0)
END

df3['_UNCOPE N Recode'] = 
IF [N] = "Yes" THEN INT(1)
ELSEIF [N] = "No" THEN INT(0)
END

df3['_UNCOPE O Recode'] = 
IF [O] = "Yes" THEN INT(1)
ELSEIF [O] = "No" THEN INT(0)
END

df3['_UNCOPE P Recode'] = 
IF [P] = "Yes" THEN INT(1)
ELSEIF [P] = "No" THEN INT(0)
END

df3['_UNCOPE U Recode'] = 
IF [U] = "Yes" THEN INT(1)
ELSEIF [U] = "No" THEN INT(0)
END

df3['_UNCOPE Score FW'] = 
[_UNCOPE U Recode]+[_UNCOPE N Recode]+[_UNCOPE C Recode]+[_UNCOPE O Recode]+[_UNCOPE P Recode]+[_UNCOPE E Recode]
###sum of UNCOPE scores in the FW dataset

df3['_Zip'] = IFNULL([Zip],[Mob Zip])

########################################

df3['_C13 Behavioral Concerns Asked'] = IFNULL([BehaviorNumer],[Behavioral Concerns])

df3['_C13 Behavioral Concerns Visits'] = IFNULL([BehaviorDenom],[home_visits_post])

df3['_C17 CESD Score'] = IFNULL([Cesd Score],[CESD Total])

df3['_FOB Involved'] = 
IF [Fob Involved] = True THEN 1 ###FW
ELSEIF [Fob Involved] = False THEN 0
ELSEIF [Fob Involved1] = "Y" THEN 1 ###LLCHD
ELSEIF [Fob Involved1] = "N" THEN 0
ELSE 0
END

df3['_T04 FOB Age'] = 
IF [_FOB DOB]> DATEADD('year',-DATEDIFF('year',[_FOB DOB],TODAY()),TODAY())
THEN DATEDIFF('year',[_FOB DOB],TODAY()-1)
ELSE DATEDIFF('year',[_FOB DOB],TODAY())
END

df3['_T04 MOB Age'] = 
IF [_MOB DOB]> DATEADD('year',-DATEDIFF('year',[_MOB DOB],TODAY()),TODAY())
THEN DATEDIFF('year',[_MOB DOB],TODAY()-1)
ELSE DATEDIFF('year',[_MOB DOB],TODAY())
END

df3['_T14 Federal Poverty Level update'] = 
###uses 2022 federal guidelines, will need to update to 2023 guidelines when they become available
8870 + (4720 * [Household Size])

df3['_T15-3 History Welfare Interaction'] = 
IF [History Inter Welfare Adult] = True THEN 1 ###FW
ELSEIF  [History Inter Welfare Adult] = False THEN 0
ELSEIF[Priority Child Welfare] = "Y" THEN 1 ###LLCHD
ELSEIF [Priority Child Welfare] = "N" THEN 0
ELSE 0
END

df3['_T15-5 Tobacco Use in Home'] = 
IF[Tobacco Use In Home] = "Yes" THEN 1 ###FW
ELSEIF [Tobacco Use In Home] = "No" THEN 0
ELSEIF [Priority Tobacco Use] = "Y" THEN 1 ###LLCHD
ELSEIF [Priority Tobacco Use] = "N" THEN 0
ELSE 0
END

df3['_T15-6 Low Achievement'] = 
IF[Low Achievement] = "Yes" THEN 1 ###FW
ELSEIF [Low Achievement] = "No" THEN 0
ELSEIF [Priority Low Student] = "Y" THEN 1 ###LLCHD
ELSEIF [Priority Low Student] = "N" THEN 0
ELSE 0
END

df3['_T15-8 Military'] = 
IF [Military]= "Y" THEN 1 ###FW
ELSEIF [Military] = "N" THEN 0
ELSEIF [Priority Military] = "Y" THEN 1 ###LLCHD
ELSEIF [Priority Military] = "N" THEN 0
ELSE 0
END

df3['_T16 Number of Home Visits'] = IFNULL([HomeVisitsTotal],[Home Visits Num])

df3['_UNCOPE Score'] = IFNULL([Uncope Score],[_UNCOPE Score FW])


#%%##################################################
### Identify/FLAG "Unrecognized Value" ###
#####################################################

### FLAG any "Unrecognized Value" --- new value & needs to be edited earlier in the Data Source process.
### Across many variables.

#%%##################################################
### Data Types ###
#####################################################

### REMEMBER to check/set the data type of each column like it should be in output.

#%%##################################################
### WRITE ###
#####################################################

### Output for 3rd Tableau file, 1st Data Source (for Form 2):
path_3_output = ''

### move to top.






