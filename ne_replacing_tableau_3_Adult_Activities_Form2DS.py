
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
path_3_comparison_csv = Path('U:\\Working\\nebraska_miechv_coded_data_source\\previous\\previous output\\Y12Q1 (Oct 2022 - Dec 2023)\\Adult Activity Master File from Excel on NE Server.csv')
df3_comparison_csv = pd.read_csv(path_3_comparison_csv, dtype=object, keep_default_na=False, na_values=[''])
df3_comparison_csv = df3_comparison_csv.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)

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

def compare_col(fdf_2, fcol, info_or_value_counts='info', fdf_1=df3_comparison_csv): ### or 'value_counts'.
    if info_or_value_counts=='info':
        print(f'DataFrame 1 (df3_comparison_csv):\n')
        print(fdf_1[fcol].info())
        print('\n')
        print(f'DataFrame 2:\n')
        print(fdf_2[fcol].info())
    elif info_or_value_counts=='value_counts':
        print(f'DataFrame 1 (df3_comparison_csv):\n')
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
    ['ProjectID', 'Project ID', '', 'string'],
    ['year', 'year (Caregiver Insurance)', '', 'Int64'],
    ['quarter', 'quarter (Caregiver Insurance)', '', 'Int64'],
    ['agency', 'Agency', '', 'string'],
    ['FAMILYNUMBER', 'Familynumber', '', 'string'],
    ['ChildNumber', 'Child Number', '', 'string'],
    ['funding', 'Funding', '', 'string'],
    ['AD1PrimaryIns.1', 'AD1PrimaryIns.1', 'same', 'string'],
    ['AD1InsChangeDate.1', 'AD1InsChangeDate.1', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.2', 'AD1PrimaryIns.2', 'same', 'string'],
    ['AD1InsChangeDate.2', 'AD1InsChangeDate.2', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.3', 'AD1PrimaryIns.3', 'same', 'string'],
    ['AD1InsChangeDate.3', 'AD1InsChangeDate.3', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.4', 'AD1PrimaryIns.4', 'same', 'string'],
    ['AD1InsChangeDate.4', 'AD1InsChangeDate.4', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.5', 'AD1PrimaryIns.5', 'same', 'string'],
    ['AD1InsChangeDate.5', 'AD1InsChangeDate.5', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.6', 'AD1PrimaryIns.6', 'same', 'string'],
    ['AD1InsChangeDate.6', 'AD1InsChangeDate.6', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.7', 'AD1PrimaryIns.7', 'same', 'string'],
    ['AD1InsChangeDate.7', 'AD1InsChangeDate.7', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.8', 'AD1PrimaryIns.8', 'same', 'string'],
    ['AD1InsChangeDate.8', 'AD1InsChangeDate.8', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.9', 'AD1PrimaryIns.9', 'same', 'string'],
    ['AD1InsChangeDate.9', 'AD1InsChangeDate.9', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.10', 'AD1PrimaryIns.10', 'same', 'string'],
    ['AD1InsChangeDate.10', 'AD1InsChangeDate.10', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.11', 'AD1PrimaryIns.11', 'same', 'string'],
    ['AD1InsChangeDate.11', 'AD1InsChangeDate.11', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.12', 'AD1PrimaryIns.12', 'same', 'string'],
    ['AD1InsChangeDate.12', 'AD1InsChangeDate.12', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.13', 'AD1PrimaryIns.13', 'same', 'string'],
    ['AD1InsChangeDate.13', 'AD1InsChangeDate.13', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.14', 'AD1PrimaryIns.14', 'same', 'string'],
    ['AD1InsChangeDate.14', 'AD1InsChangeDate.14', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.15', 'AD1PrimaryIns.15', 'same', 'string'],
    ['AD1InsChangeDate.15', 'AD1InsChangeDate.15', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.16', 'AD1PrimaryIns.16', 'same', 'string'],
    ['AD1InsChangeDate.16', 'AD1InsChangeDate.16', 'same', 'datetime64[ns]']
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
    ['Project ID', 'Project ID1', '', 'string'],
    ['year', 'year (Family Wise)', '', 'Int64'],
    ['quarter', 'quarter (Family Wise)', '', 'Int64'],
    ['agency', 'agency (Family Wise)', '', 'string'],
    ['FamilyNumber', 'Family Number', '', 'string'],
    ['ChildNumber', 'ChildNumber (Family Wise)', '', 'string'],
    ['TGT DOB-CR', 'Tgt Dob-Cr', '', 'datetime64[ns]'],
    ['EDC Date', 'EDC Date', 'same', 'datetime64[ns]'],
    ['MinOfHVDate', 'Min Of HV Date', '', 'datetime64[ns]'],
    ['TERMINATION STATUS', 'Termination Status', '', 'string'],
    ['TERMINATION DATE', 'Termination Date', '', 'datetime64[ns]'],
    ['Pregnancystatus', 'Pregnancystatus', 'same', 'Int64'],
    ['MaxOfHVDate', 'Max Of HV Date', '', 'datetime64[ns]'],
    ['MaxOfVISIT NUMBER', 'MaxOfVISIT NUMBER', 'same', 'Int64'],
    ['BehaviorNumer', 'BehaviorNumer', 'same', 'Int64'],
    ['BehaviorDenom', 'BehaviorDenom', 'same', 'Int64'],
    ['HomeVisitsPrenatal', 'HomeVisitsPrenatal', 'same', 'Int64'],
    ['HomeVisitsTotal', 'HomeVisitsTotal', 'same', 'Int64'],
    ['MOBDOB', 'Mobdob', '', 'datetime64[ns]'],
    ['FOBDOB', 'Fobdob', '', 'datetime64[ns]'],
    ['MinEducationDate', 'Min Education Date', '', 'datetime64[ns]'],
    ['AD1MinEdu', 'AD1MinEdu', 'same', 'string'],
    ['MinEduEnroll', 'Min Edu Enroll', '', 'string'],
    ['MaxEduDate', 'Max Edu Date', '', 'datetime64[ns]'],
    ['AD1MaxEdu', 'AD1MaxEdu', 'same', 'string'],
    ['MaxEduEnroll', 'Max Edu Enroll', '', 'string'],
    ['DATEUNCOPE', 'Dateuncope', '', 'datetime64[ns]'],
    ['U', 'U', 'same', 'string'],
    ['N', 'N', 'same', 'string'],
    ['C', 'C', 'same', 'string'],
    ['O', 'O', 'same', 'string'],
    ['P', 'P', 'same', 'string'],
    ['E', 'E', 'same', 'string'],
    ['UNCOPERefDate', 'UNCOPE Ref Date', '', 'datetime64[ns]'],
    ['UNCOPERefCategory', 'UNCOPE Ref Category', '', 'string'],
    ['TobaccoUseDate', 'Tobacco Use Date', '', 'datetime64[ns]'],
    ['TobaccoRefDate', 'Tobacco Ref Date', '', 'datetime64[ns]'],
    ['AssessAfraid', 'Assess Afraid', '', 'boolean'],
    ['AssessPolice', 'Assess Police', '', 'boolean'],
    ['AssessIPV', 'Assess IPV', '', 'boolean'],
    ['IPV Assess Date', 'IPV Assess Date', 'same', 'datetime64[ns]'],
    ['IPVRefDate', 'IPV Ref Date', '', 'datetime64[ns]'],
    ['PostpartumCheckupDate', 'Postpartum Checkup Date', '', 'datetime64[ns]'],
    ['MinOfCESDDATE', 'Min Of CESDDATE', '', 'datetime64[ns]'],
    ['CESDTotal', 'CESD Total', '', 'Int64'],
    ['MinOfMHRefDate', 'Min Of MH Ref Date', '', 'datetime64[ns]'],
    ['MaxCHEEERSDate', 'Max CHEEERS Date', '', 'datetime64[ns]'],
    ['AD2EduDateMax', 'AD2EduDateMax', 'same', 'datetime64[ns]'],
    ['AD2EDLevel', 'AD2EDLevel', 'same', 'string'],
    ['AD2InSchool', 'AD2InSchool', 'same', 'string'],
    ['MaxOfMaxOfAD2InsChangeDate', 'MaxOfMaxOfAD2InsChangeDate', 'same', 'datetime64[ns]'],
    ['AD2InsPrimary', 'AD2InsPrimary', 'same', 'string'],
    ['MaxOfAD1EmpChangeDate', 'MaxOfAD1EmpChangeDate', 'same', 'datetime64[ns]'],
    ['AD1EmpStatus', 'AD1EmpStatus', 'same', 'string'],
    ['MaxOfAD2EmplChangeDate', 'MaxOfAD2EmplChangeDate', 'same', 'datetime64[ns]'],
    ['AD2EmployStatus', 'AD2EmployStatus', 'same', 'string'],
    ['MaxOfMaxOfAD1MSChangeDate', 'MaxOfMaxOfAD1MSChangeDate', 'same', 'datetime64[ns]'],
    ['Adult1MaritalStatus', 'Adult1MaritalStatus', 'same', 'string'],
    ['MaxOfAD2MSChangeDate', 'MaxOfAD2MSChangeDate', 'same', 'datetime64[ns]'],
    ['Adult2MaritalStatus', 'Adult2MaritalStatus', 'same', 'string'],
    ['ANNUAL INCOME', 'Annual Income', '', 'Float64'],
    ['POVERTY LEVEL', 'Poverty Level', '', 'Float64'],
    ['TYPE HOUSING', 'Type Housing', '', 'string'],
    ['HomelessStatus', 'Homeless Status', '', 'string'],
    ['HousingStatus', 'Housing Status', '', 'string'],
    ['HistoryInterWelfareAdult', 'History Inter Welfare Adult', '', 'boolean'],
    ['MOB SUBSTANCE ABUSE', 'Mob Substance Abuse', '', 'boolean'],
    ['FOB SUBSTANCE ABUSE', 'Fob Substance Abuse', '', 'boolean'],
    ['TobaccoUseInHome', 'Tobacco Use In Home', '', 'string'],
    ['LowAchievement', 'Low Achievement', '', 'string'],
    ['NTChildLowAchievement', 'NT Child Low Achievement', '', 'string'],
    ['NTChildDevDelay', 'NT Child Dev Delay', '', 'string'],
    ['Military', 'Military', 'same', 'string'],
    ['Adult1Gender', 'Adult1Gender', 'same', 'string'],
    ['Adult1TGTRelation', 'Adult1TGTRelation', 'same', 'string'],
    ['MOB ETHNIC', 'Mob Ethnic', '', 'string'],
    ['MOBRaceWhite', 'MOB Race White', '', 'boolean'],
    ['MOBRaceBlack', 'MOB Race Black', '', 'boolean'],
    ['MOBRaceIndianAlaskan', 'MOB Race Indian Alaskan', '', 'boolean'],
    ['MOBRaceAsian', 'MOB Race Asian', '', 'boolean'],
    ['MOBRaceHawaiianPacific', 'MOB Race Hawaiian Pacific', '', 'boolean'],
    ['MOBRaceOther', 'MOB Race Other', '', 'boolean'],
    ['FOB INVOLVED', 'Fob Involved', '', 'boolean'],
    ['Adult2Gender', 'Adult2Gender', 'same', 'string'],
    ['Adult2TGTRelation', 'Adult2TGTRelation', 'same', 'string'],
    ['FOB ETHNICITY', 'Fob Ethnicity', '', 'string'],
    ['FOBRaceWhite', 'FOB Race White', '', 'boolean'],
    ['FOBRaceBlack', 'FOB Race Black', '', 'boolean'],
    ['FOBRaceIndianAlaskan', 'FOB Race Indian Alaskan', '', 'boolean'],
    ['FOBRaceAsian', 'FOB Race Asian', '', 'boolean'],
    ['FOBRaceHawaiianPacific', 'FOB Race Hawaiian Pacific', '', 'boolean'],
    ['FOBRaceOther', 'FOB Race Other', '', 'boolean'],
    ['MOB ZIP', 'Mob Zip', '', 'string'],
    ['Adaptation', 'Adaptation', 'same', 'string'],
    ['need_exclusion1', 'Need Exclusion1', '', 'string'],
    ['need_exclusion2', 'Need Exclusion2', '', 'string'],
    ['need_exclusion3', 'Need Exclusion3', '', 'string'],
    ['need_exclusion5', 'Need Exclusion5', '', 'string'],
    ['need_exclusion6', 'Need Exclusion6', '', 'string']
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
    ['project_id', 'project id (LLCHD)', '', 'string'],
    ['year', 'year (LLCHD)', '', 'Int64'],
    ['quarter', 'quarter (LLCHD)', '', 'Int64'],
    ['site_id', 'Site Id', '', 'string'],
    ['family_id', 'Family Id', '', 'string'],
    ['tgt_id', 'Tgt Id', '', 'string'],
    ['tgt_dob', 'Tgt Dob', '', 'datetime64[ns]'],
    ['tgt_gender', 'Tgt Gender', '', 'string'],
    ['tgt_ethnicity', 'Tgt Ethnicity', '', 'string'],
    ['tgt_race_indian', 'Tgt Race Indian', '', 'string'],
    ['tgt_race_asian', 'Tgt Race Asian', '', 'string'],
    ['tgt_race_black', 'Tgt Race Black', '', 'string'],
    ['tgt_race_hawaiian', 'Tgt Race Hawaiian', '', 'string'],
    ['tgt_race_white', 'Tgt Race White', '', 'string'],
    ['tgt_race_other', 'Tgt Race Other', '', 'string'],
    ['tgt_GestationalAge', 'tgt_GestationalAge', '', 'string'],
    ['tgt_medical_home', 'Tgt Medical Home', '', 'Int64'],
    ['tgt_medical_home_dt', 'Tgt Medical Home Dt', '', 'datetime64[ns]'],
    ['tgt_dental_home', 'Tgt Dental Home', '', 'Int64'],
    ['tgt_dental_home_dt', 'Tgt Dental Home Dt', '', 'datetime64[ns]'],
    ['dt_edc', 'Dt Edc', '', 'datetime64[ns]'],
    ['enroll_dt', 'Enroll Dt', '', 'datetime64[ns]'],
    ['enroll_preg_status', 'Enroll Preg Status', '', 'string'],
    ['current_pregnancy', 'Current Pregnancy', '', 'string'],
    ['discharge_reason', 'Discharge Reason', '', 'string'],
    ['discharge_dt', 'Discharge Dt', '', 'datetime64[ns]'],
    ['last_home_visit', 'Last Home Visit', '', 'datetime64[ns]'],
    ['home_visits_num', 'Home Visits Num', '', 'Int64'],
    ['home_visits_pre', 'home_visits_pre', 'same', 'Int64'],
    ['home_visits_post', 'home_visits_post', 'same', 'Int64'],
    ['home_visits_person', 'home_visits_person', 'same', 'string'],
    ['home_visits_virtual', 'home_visits_virtual', 'same', 'string'],
    ['funding', 'funding (LLCHD)', '', 'string'],
    ['c_fundingdate', 'c_fundingdate', 'same', 'datetime64[ns]'],
    ['p_funding', 'p_funding', 'same', 'string'],
    ['p_fundingdate', 'p_fundingdate', 'same', 'string'], ### Probably should be datetime64[ns]'].
    ['primary_id', 'Primary Id', '', 'string'],
    ['primary_relation', 'Primary Relation', '', 'string'],
    ['mob_id', 'Mob Id', '', 'string'],
    ['mob_dob', 'Mob Dob', '', 'datetime64[ns]'],
    ['mob_gender', 'Mob Gender', '', 'string'],
    ['mob_ethnicity', 'Mob Ethnicity', '', 'string'],
    ['mob_race', 'Mob Race', '', 'string'],
    ['mob_race_indian', 'Mob Race Indian', '', 'string'],
    ['mob_race_asian', 'Mob Race Asian', '', 'string'],
    ['mob_race_black', 'Mob Race Black', '', 'string'],
    ['mob_race_hawaiian', 'Mob Race Hawaiian', '', 'string'],
    ['mob_race_white', 'Mob Race White', '', 'string'],
    ['mob_race_other', 'Mob Race Other', '', 'string'],
    ['mob_marital_status', 'Mob Marital Status', '', 'string'],
    ['mob_living_arrangement', 'Mob Living Arrangement', '', 'Int64'],
    ['mob_living_arrangement_dt', 'Mob Living Arrangement Dt', '', 'datetime64[ns]'],
    ['fob_id', 'Fob Id', '', 'string'],
    ['fob_dob', 'Fob Dob', '', 'datetime64[ns]'],
    ['fob_gender', 'Fob Gender', '', 'string'],
    ['fob_ethnicity', 'Fob Ethnicity1', '', 'string'],
    ['fob_race', 'Fob Race', '', 'string'],
    ['fob_race_indian', 'Fob Race Indian', '', 'string'],
    ['fob_race_asian', 'Fob Race Asian', '', 'string'],
    ['fob_race_black', 'Fob Race Black', '', 'string'],
    ['fob_race_hawaiian', 'Fob Race Hawaiian', '', 'string'],
    ['fob_race_white', 'Fob Race White', '', 'string'],
    ['fob_race_other', 'Fob Race Other', '', 'string'],
    ['fob_marital_status', 'Fob Marital Status', '', 'string'],
    ['fob_living_arrangement', 'Fob Living Arrangement', '', 'Int64'],
    ['fob_living_arrangement_dt', 'Fob Living Arrangement Dt', '', 'datetime64[ns]'],
    ['fob_edu_dt', 'Fob Edu Dt', '', 'datetime64[ns]'],
    ['fob_edu', 'Fob Edu', '', 'Int64'],
    ['fob_employ_dt', 'Fob Employ Dt', '', 'datetime64[ns]'],
    ['fob_employ', 'Fob Employ', '', 'Int64'],
    ['fob_involved', 'Fob Involved1', '', 'string'],
    ['fob_visits', 'Fob Visits', '', 'Int64'],
    ['zip', 'Zip', '', 'string'],
    ['household_income', 'Household Income', '', 'Int64'],
    ['household_size', 'Household Size', '', 'Int64'],
    ['mcafss_income_dt', 'Mcafss Income Dt', '', 'datetime64[ns]'],
    ['mcafss_income', 'Mcafss Income', '', 'Int64'],
    ['mcafss_edu_dt1', 'Mcafss Edu Dt1', '', 'datetime64[ns]'],
    ['mcafss_edu1', 'Mcafss Edu1', '', 'Int64'],
    ['mcafss_edu1_enroll', 'mcafss_edu1_enroll', 'same', 'string'],
    ['mcafss_edu1_enroll_dt', 'mcafss_edu1_enroll_dt', 'same', 'datetime64[ns]'],
    ['mcafss_edu1_prog', 'mcafss_edu1_prog', 'same', 'Int64'],
    ['mcafss_edu_dt2', 'Mcafss Edu Dt2', '', 'datetime64[ns]'],
    ['mcafss_edu2', 'Mcafss Edu2', '', 'Int64'],
    ['mcafss_edu2_enroll', 'mcafss_edu2_enroll', 'same', 'string'],
    ['mcafss_edu2_enroll_dt', 'mcafss_edu2_enroll_dt', 'same', 'datetime64[ns]'],
    ['mcafss_edu2_prog', 'mcafss_edu2_prog', 'same', 'Int64'],
    ['mcafss_employ_dt', 'Mcafss Employ Dt', '', 'datetime64[ns]'],
    ['mcafss_employ', 'Mcafss Employ', '', 'Int64'],
    ['language_primary', 'Language Primary', '', 'string'],
    ['priority_child_welfare', 'Priority Child Welfare', '', 'string'],
    ['priority_substance_abuse', 'Priority Substance Abuse', '', 'string'],
    ['priority_tobacco_use', 'Priority Tobacco Use', '', 'string'],
    ['priority_low_student', 'Priority Low Student', '', 'string'],
    ['priority_develop_delays', 'Priority Develop Delays', '', 'string'],
    ['priority_military', 'Priority Military', '', 'string'],
    ['uncope_dt', 'Uncope Dt', '', 'datetime64[ns]'],
    ['uncope_score', 'Uncope Score', '', 'Int64'],
    ['substance_abuse_ref_dt', 'Substance Abuse Ref Dt', '', 'datetime64[ns]'],
    ['tobacco_use', 'Tobacco Use', '', 'string'],
    ['tobacco_use_dt', 'Tobacco Use Dt', '', 'datetime64[ns]'],
    ['tobacco_ref_dt', 'Tobacco Ref Dt', '', 'datetime64[ns]'],
    ['safe_sleep_yes', 'Safe Sleep Yes', '', 'string'],
    ['safe_sleep_yes_dt', 'Safe Sleep Yes Dt', '', 'datetime64[ns]'],
    ['cheeers_date', 'Cheeers Date', '', 'datetime64[ns]'],
    ['early_language', 'Early Language', '', 'string'],
    ['early_language_dt', 'Early Language Dt', '', 'datetime64[ns]'],
    ['asq3_dt_9mm', 'Asq3 Dt 9Mm', '', 'datetime64[ns]'],
    ['asq3_timing_9mm', 'Asq3 Timing 9Mm', '', 'Int64'],
    ['asq3_comm_9mm', 'Asq3 Comm 9Mm', '', 'Int64'],
    ['asq3_gross_9mm', 'Asq3 Gross 9Mm', '', 'Int64'],
    ['asq3_fine_9mm', 'Asq3 Fine 9Mm', '', 'Int64'],
    ['asq3_problem_9mm', 'Asq3 Problem 9Mm', '', 'Int64'],
    ['asq3_social_9mm', 'Asq3 Social 9Mm', '', 'Int64'],
    ['asq3_feedback_9mm', 'Asq3 Feedback 9Mm', '', 'string'],
    ['asq3_referral_9mm', 'Asq3 Referral 9Mm', '', 'string'], ### 'string' in Tableau; but probably should be 'int'.
    ['asq3_dt_18mm', 'Asq3 Dt 18Mm', '', 'datetime64[ns]'],
    ['asq3_timing_18mm', 'Asq3 Timing 18Mm', '', 'Int64'],
    ['asq3_comm_18mm', 'Asq3 Comm 18Mm', '', 'Int64'],
    ['asq3_gross_18mm', 'Asq3 Gross 18Mm', '', 'Int64'],
    ['asq3_fine_18mm', 'Asq3 Fine 18Mm', '', 'Int64'],
    ['asq3_problem_18mm', 'Asq3 Problem 18Mm', '', 'Int64'],
    ['asq3_social_18mm', 'Asq3 Social 18Mm', '', 'Int64'],
    ['asq3_feedback_18mm', 'Asq3 Feedback 18Mm', '', 'string'],
    ['asq3_referral_18mm', 'Asq3 Referral 18Mm', '', 'Int64'],
    ['asq3_dt_24mm', 'Asq3 Dt 24Mm', '', 'datetime64[ns]'],
    ['asq3_timing_24mm', 'Asq3 Timing 24Mm', '', 'Int64'],
    ['asq3_comm_24mm', 'Asq3 Comm 24Mm', '', 'Int64'],
    ['asq3_gross_24mm', 'Asq3 Gross 24Mm', '', 'Int64'],
    ['asq3_fine_24mm', 'Asq3 Fine 24Mm', '', 'Int64'],
    ['asq3_problem_24mm', 'Asq3 Problem 24Mm', '', 'Int64'],
    ['asq3_social_24mm', 'Asq3 Social 24Mm', '', 'Int64'],
    ['asq3_feedback_24mm', 'Asq3 Feedback 24Mm', '', 'string'],
    ['asq3_referral_24mm', 'Asq3 Referral 24Mm', '', 'Int64'],
    ['asq3_dt_30mm', 'Asq3 Dt 30Mm', '', 'datetime64[ns]'],
    ['asq3_timing_30mm', 'Asq3 Timing 30Mm', '', 'Int64'],
    ['asq3_comm_30mm', 'Asq3 Comm 30Mm', '', 'Int64'],
    ['asq3_gross_30mm', 'Asq3 Gross 30Mm', '', 'Int64'],
    ['asq3_fine_30mm', 'Asq3 Fine 30Mm', '', 'Int64'],
    ['asq3_problem_30mm', 'Asq3 Problem 30Mm', '', 'Int64'],
    ['asq3_social_30mm', 'Asq3 Social 30Mm', '', 'Int64'],
    ['asq3_feedback_30mm', 'Asq3 Feedback 30Mm', '', 'string'],
    ['asq3_referral_30mm', 'Asq3 Referral 30Mm', '', 'Int64'],
    ['behavioral_concerns', 'Behavioral Concerns', '', 'Int64'],
    ['ipv_screen', 'Ipv Screen', '', 'string'],
    ['ipv_screen_dt', 'Ipv Screen Dt', '', 'datetime64[ns]'],
    ['ipv_referral_dt', 'Ipv Referral Dt', '', 'datetime64[ns]'],
    ['prim_care_dt', 'Prim Care Dt', '', 'datetime64[ns]'],
    ['cesd_dt', 'Cesd Dt', '', 'datetime64[ns]'],
    ['cesd_score', 'Cesd Score', '', 'Int64'],
    ['ment_hlth_ref_dt', 'Ment Hlth Ref Dt', '', 'datetime64[ns]'],
    ['lsp_bf_initiation_dt', 'Lsp Bf Initiation Dt', '', 'datetime64[ns]'],
    ['lsp_bf_discon_dt', 'Lsp Bf Discon Dt', '', 'datetime64[ns]'],
    ['hlth_insure_mob', 'Hlth Insure Mob', '', 'Int64'],
    ['hlth_insure_mob_dt', 'Hlth Insure Mob Dt', '', 'datetime64[ns]'],
    ['hlth_insure_tgt', 'Hlth Insure Tgt', '', 'Int64'],
    ['hlth_insure_tgt_dt', 'Hlth Insure Tgt Dt', '', 'datetime64[ns]'],
    ['last_well_child_visit', 'Last Well Child Visit', '', 'datetime64[ns]'],
    ['hlth_insure_fob', 'Hlth Insure Fob', '', 'Int64'],
    ['hlth_insure_fob_dt', 'Hlth Insure Fob Dt', '', 'datetime64[ns]'],
    ['need_exclusion1', 'need exclusion1 (LLCHD)', '', 'string'],
    ['need_exclusion2', 'need exclusion2 (LLCHD)', '', 'string'],
    ['need_exclusion3', 'need exclusion3 (LLCHD)', '', 'string'],
    ['need_exclusion4', 'Need Exclusion4', '', 'string'],
    ['need_exclusion5', 'need exclusion5 (LLCHD)', '', 'string'],
    ['need_exclusion6', 'need_exclusion6', 'same', 'string'],
    ['Has_ChildWelfareAdaptation', 'Has_ChildWelfareAdaptation', 'same', 'string']
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
### Rename Columns ###
#####################################################

#######################
#%%### df3_1: 'Project ID'.
[*df3_1]
#%%### df3_1: 'Project ID'.
df3_1_colnames
#%%### df3_1: 'Project ID'.
df3_1 = df3_1.rename(columns=df3_1_colnames)
[*df3_1]

#######################
#%%### df3_2: 'Caregiver Insurance'.
[*df3_2]
#%%### df3_2: 'Caregiver Insurance'.
df3_2_colnames
#%%### df3_2: 'Caregiver Insurance'.
df3_2 = df3_2.rename(columns=df3_2_colnames)
[*df3_2]

#######################
#%%### df3_3: 'Family Wise'.
[*df3_3]
#%%### df3_3: 'Family Wise'.
df3_3_colnames
#%%### df3_3: 'Family Wise'.
df3_3 = df3_3.rename(columns=df3_3_colnames)
[*df3_3]

#######################
#%%### df3_4: 'LLCHD'.
[*df3_4]
#%%### df3_4: 'LLCHD'.
df3_4_colnames
#%%### df3_4: 'LLCHD'.
df3_4 = df3_4.rename(columns=df3_4_colnames)
[*df3_4]

#%%##################################################
### Prep for JOIN ###
#####################################################

# ### Each row SHOULD be unique on these sheets, especially the 'Project ID' sheet.
# ### TO DO: Actually run section to deduplicate.

# #%%### Restart deduplication
# ### df3_1 = df3_1_bf_ddup.copy()
# ### df3_2 = df3_2_bf_ddup.copy()
# ### df3_3 = df3_3_bf_ddup.copy()
# ### df3_4 = df3_4_bf_ddup.copy()
# ### df3_5 = df3_5_bf_ddup.copy()

# #######################
# ### NOTE: 24 duplicate rows. TO DO: Fix in Master File creation.
# #%%### df3_1: 'Project ID'. 
# ### Backup:
# df3_1_bf_ddup = df3_1.copy()
# #%%### df3_1: 'Project ID'. 
# ### Duplicate rows:
# df3_1[df3_1.duplicated()]
# #%%### df3_1: 'Project ID'. 
# ### Dropping duplicate rows:
# df3_1 = df3_1.drop_duplicates(ignore_index=True)
# df3_1
# #%%### df3_1: 'Project ID'. 
# ### Test
# len(df3_1_bf_ddup) - len(df3_1) == len(df3_1_bf_ddup[df3_1_bf_ddup.duplicated()])
# #%%### df3_1: 'Project ID'. 
# if (len(df3_1_bf_ddup) != len(df3_1)):
#     print(f'{len(df3_1_bf_ddup) - len(df3_1)} duplicate rows dropped.')
# elif (len(df3_1_bf_ddup) == len(df3_1)):
#     print('No duplicate rows.')
# else:
#     print("Don't know what's going on here!")

# #######################
# ### NOTE: NO duplicate rows.
# #%%### df3_2: 'Caregiver Insurance'.
# df3_2_bf_ddup = df3_2.copy()
# #%%### df3_2: 'Caregiver Insurance'.
# df3_2[df3_2.duplicated()]
# # df3_2[df3_2.duplicated(keep=False, subset=['Project ID (ER Injury)','year (ER Injury)','quarter (ER Injury)'])]
# #%%### df3_2: 'Caregiver Insurance'.
# df3_2 = df3_2.drop_duplicates(ignore_index=True)
# df3_2
# #%%### df3_2: 'Caregiver Insurance'.
# len(df3_2_bf_ddup) - len(df3_2) == len(df3_2_bf_ddup[df3_2_bf_ddup.duplicated()])
# #%%### df3_2: 'Caregiver Insurance'.
# if (len(df3_2_bf_ddup) != len(df3_2)):
#     print(f'{len(df3_2_bf_ddup) - len(df3_2)} duplicate rows dropped.')
# elif (len(df3_2_bf_ddup) == len(df3_2)):
#     print('No duplicate rows.')
# else:
#     print("Don't know what's going on here!")

# #######################
# ### NOTE: 3 duplicate rows. TO DO: Fix in Master File creation.
# #%%### df3_3: 'Family Wise'.
# df3_3_bf_ddup = df3_3.copy()
# #%%### df3_3: 'Family Wise'.
# df3_3[df3_3.duplicated()]
# # df3_3[df3_3.duplicated(keep=False, subset=['Project ID','year (Family Wise)','quarter (Family Wise)'])]
# #%%### df3_3: 'Family Wise'.
# df3_3 = df3_3.drop_duplicates(ignore_index=True)
# df3_3
# #%%### df3_3: 'Family Wise'.
# len(df3_3_bf_ddup) - len(df3_3) == len(df3_3_bf_ddup[df3_3_bf_ddup.duplicated()])
# #%%### df3_3: 'Family Wise'.
# if (len(df3_3_bf_ddup) != len(df3_3)):
#     print(f'{len(df3_3_bf_ddup) - len(df3_3)} duplicate rows dropped.')
# elif (len(df3_3_bf_ddup) == len(df3_3)):
#     print('No duplicate rows.')
# else:
#     print("Don't know what's going on here!")

# #######################
# ### NOTE: NO duplicate rows.
# #%%### df3_4: 'LLCHD'.
# df3_4_bf_ddup = df3_4.copy()
# #%%### df3_4: 'LLCHD'.
# df3_4[df3_4.duplicated()]
# # df3_4[df3_4.duplicated(keep=False, subset=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)'])]
# #%%### df3_4: 'LLCHD'.
# df3_4 = df3_4.drop_duplicates(ignore_index=True)
# df3_4
# #%%### df3_4: 'LLCHD'.
# len(df3_4_bf_ddup) - len(df3_4) == len(df3_4_bf_ddup[df3_4_bf_ddup.duplicated()])
# #%%### df3_4: 'LLCHD'.
# if (len(df3_4_bf_ddup) != len(df3_4)):
#     print(f'{len(df3_4_bf_ddup) - len(df3_4)} duplicate rows dropped.')
# elif (len(df3_4_bf_ddup) == len(df3_4)):
#     print('No duplicate rows.')
# else:
#     print("Don't know what's going on here!")

#%%##################################################
### JOIN ###
#####################################################

### TO DO: Turn on validation once deduplication turned on.
### TO DO: Address "PerformanceWarning: DataFrame is highly fragmented." from running merge.

#%%
df3 = (
    pd.merge(
        df3_1 ### 'Project ID'.
        ,df3_2 ### 'Caregiver Insurance'.
        ,how='left'
        ,left_on=['Project Id','Year','Quarter']
        ,right_on=['Project ID','year (Caregiver Insurance)','quarter (Caregiver Insurance)']
        ,indicator='LJ_df3_2CI'
        # ,validate='one_to_one'
    ).merge(
        df3_3 ### 'Family Wise'.
        ,how='left'
        ,left_on=['Project Id','Year','Quarter']
        ,right_on=['Project ID1','year (Family Wise)','quarter (Family Wise)']
        ,indicator='LJ_df3_3FW'
        # ,validate='one_to_one'
    ).merge(
        df3_4 ### 'LLCHD'.
        ,how='left'
        ,left_on=['Project Id','Year','Quarter']
        ,right_on=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)']
        ,indicator='LJ_df3_4LL')
        # ,validate='one_to_one'
) 

#%%##################################################
### Set Data Types ###
#####################################################

### Tableau does this automatically. So does Python on Read in. Made needed adjustments above.

#%%##################################################
### RECODE / Creating Columns ###
#####################################################

#%%
df3_edits1 = df3.copy()  ### Make a deep-ish copy of the DF's Data. Does NOT copy embedded mutable objects.

#%%##################################################
### DUPLICATING

#%%##################################################
### COALESCING 1 

### In Child2 & Adult3.
df3_edits1['_Agency'] = df3_edits1['agency (Family Wise)'].combine_first(df3_edits1['Site Id'])
    ### IFNULL([agency (Family Wise)],[Site Id]) 
    ### Data Type in Tableau: string.

df3_edits1['_C03 CES-D Date'] = df3_edits1['Cesd Dt'].combine_first(df3_edits1['Min Of CESDDATE'])
    ### IFNULL([Cesd Dt],[Min Of CESDDATE]) 
    ### Data Type in Tableau: date.

df3_edits1['_C05 Postpartum Checkup Date'] = df3_edits1['Postpartum Checkup Date'].combine_first(df3_edits1['Prim Care Dt'])
    ### IFNULL([Postpartum Checkup Date],[Prim Care Dt]) 
    ### Data Type in Tableau: date.

### Need before: '_C06 Tobacco Referral Date'.
### A date indicates tobacco use.
df3_edits1['_C06 Tobacco Use Date'] = df3_edits1['Tobacco Use Date'].combine_first(df3_edits1['Tobacco Use Dt'])
    ### IFNULL([Tobacco Use Date],[Tobacco Use Dt]) //a date indicates tobacco use 
    ### Data Type in Tableau: date.

df3_edits1['_C06 Tobacco Referral Date'] = df3_edits1['Tobacco Ref Date'].combine_first(df3_edits1['_C06 Tobacco Use Date'])
    ### IFNULL([Tobacco Ref Date],[_C06 Tobacco Use Date])
    ### Data Type in Tableau: date.

df3_edits1['_C10 CHEEERS'] = df3_edits1['Cheeers Date'].combine_first(df3_edits1['Max CHEEERS Date'])
    ### IFNULL([Cheeers Date],[Max CHEEERS Date]) 
    ### Data Type in Tableau: date.

df3_edits1['_C14 IPV Date'] = df3_edits1['IPV Assess Date'].combine_first(df3_edits1['Ipv Screen Dt'])
    ### IFNULL([IPV Assess Date],[Ipv Screen Dt]) 
    ### Data Type in Tableau: date.

df3_edits1['_C17 MH Referral Date'] = df3_edits1['Ment Hlth Ref Dt'].combine_first(df3_edits1['Min Of MH Ref Date'])
    ### IFNULL([Ment Hlth Ref Dt],[Min Of MH Ref Date]) 
    ### Data Type in Tableau: date.

df3_edits1['_C19 IPV Referral Date'] = df3_edits1['Ipv Referral Dt'].combine_first(df3_edits1['IPV Ref Date'])
    ### IFNULL([Ipv Referral Dt],[IPV Ref Date]) 
    ### Data Type in Tableau: date.

### In Child2 & Adult3.
df3_edits1['_Discharge Date'] = df3_edits1['Termination Date'].combine_first(df3_edits1['Discharge Dt'])
    ### IFNULL([Termination Date],[Discharge Dt]) 
    ### Data Type in Tableau: date.

df3_edits1['_Enrollment Date'] = df3_edits1['Min Of HV Date'].combine_first(df3_edits1['Enroll Dt'])
    ### IFNULL([Min Of HV Date],[Enroll Dt]) 
    ### Data Type in Tableau: date.

df3_edits1['_Family ID'] = df3_edits1['Family Id'].combine_first(df3_edits1['Family Number'])
    ### IFNULL([Family Id], [Family Number]) 
    ### Data Type in Tableau: string.

### In Child2 & Adult3.
df3_edits1['_Max HV Date'] = df3_edits1['Max Of HV Date'].combine_first(df3_edits1['Last Home Visit'])
    ### IFNULL([Max Of HV Date],[Last Home Visit]) 
    ### Data Type in Tableau: date.

df3_edits1['_TGT ID'] = df3_edits1['Tgt Id'].combine_first(df3_edits1['Child Number'])
    ### IFNULL([Tgt Id],[Child Number]) 
    ### Data Type in Tableau: string.

df3_edits1['_UNCOPE Date'] = df3_edits1['Uncope Dt'].combine_first(df3_edits1['Dateuncope'])
    ### IFNULL([Uncope Dt],[Dateuncope]) 
    ### Data Type in Tableau: date.

df3_edits1['_UNCOPE Referral'] = df3_edits1['Substance Abuse Ref Dt'].combine_first(df3_edits1['UNCOPE Ref Date'])
    ### IFNULL([Substance Abuse Ref Dt], [UNCOPE Ref Date]) 
    ### Data Type in Tableau: date.

### In Child2 & Adult3.
df3_edits1['_Zip'] = df3_edits1['Zip'].combine_first(df3_edits1['Mob Zip'])
    ### IFNULL([Zip],[Mob Zip]) 
    ### Data Type in Tableau: string.

### In Child2 & Adult3.
df3_edits1['_C13 Behavioral Concerns Asked'] = df3_edits1['BehaviorNumer'].combine_first(df3_edits1['Behavioral Concerns'])
    ### IFNULL([BehaviorNumer],[Behavioral Concerns]) 
    ### Data Type in Tableau: int.

### In Child2 & Adult3.
df3_edits1['_C13 Behavioral Concerns Visits'] = df3_edits1['BehaviorDenom'].combine_first(df3_edits1['home_visits_post'])
    ### IFNULL([BehaviorDenom],[home_visits_post]) 
    ### Data Type in Tableau: int.

df3_edits1['_C17 CESD Score'] = df3_edits1['Cesd Score'].combine_first(df3_edits1['CESD Total'])
    ### IFNULL([Cesd Score],[CESD Total]) 
    ### Data Type in Tableau: int.

df3_edits1['_T16 Number of Home Visits'] = df3_edits1['HomeVisitsTotal'].combine_first(df3_edits1['Home Visits Num'])
    ### IFNULL([HomeVisitsTotal],[Home Visits Num]) 
    ### Data Type in Tableau: int.

#%%###################################

df3_edits1['_C15 Max Education Date'] = (df3_edits1['Mcafss Edu Dt2'].combine_first(df3_edits1['Max Edu Date']))
    ### DATE(IFNULL([Mcafss Edu Dt2],[Max Edu Date])) 
    ### Data Type in Tableau: date.
inspect_col(df3_edits1['_C15 Max Education Date'])
    ### Is still a date.

#%%###################################

df3_edits1['_C15 Min Education Date'] = (df3_edits1['Mcafss Edu Dt1'].combine_first(df3_edits1['Min Education Date']))
    ### DATE(IFNULL([Mcafss Edu Dt1],[Min Education Date])) 
    ### Data Type in Tableau: date.
inspect_col(df3_edits1['_C15 Min Education Date'])
    ### Is still a date.

#%%##################################################
### IF/ELSE, CASE/WHEN

### fdf == "function DataFrame"

#%%###################################

### Other vars depend on this.
### In Child2 & Adult3. Copied exactly.
def fn_TGT_DOB(fdf):
    ### LLCHD.
    if (fdf['Tgt Dob'].date() == pd.Timestamp("1900-01-01").date()):
        return pd.NaT 
    ### FW.
    elif (fdf['Tgt Dob-Cr'].date() == pd.Timestamp("1900-01-01").date()):
        return pd.NaT 
    else:
        if (fdf['Tgt Dob'] is not pd.NaT):
            return fdf['Tgt Dob']
        else:
            return fdf['Tgt Dob-Cr']
    ### IF [Tgt Dob] = DATE(1/1/1900) THEN NULL //LLCHD
    ### ELSEIF [Tgt Dob-Cr] = DATE(1/1/1900) THEN NULL //FW
    ### ELSE IFNULL([Tgt Dob],[Tgt Dob-Cr])
    ### END
df3_edits1['_TGT DOB'] = df3_edits1.apply(func=fn_TGT_DOB, axis=1)
    ### Data Type in Tableau: 'date'.
inspect_col(df3_edits1['_TGT DOB'])
# #%%
# inspect_col(df3_edits1['Tgt Dob'])
# #%%
# inspect_col(df3_edits1['Tgt Dob-Cr'])

#%%###################################

def fn_MOB_DOB(fdf):
    ### LLCHD.
    if (fdf['Mob Dob'].date() == pd.Timestamp("1900-01-01").date()):
        return pd.NaT 
    ### FW.
    elif (fdf['Mobdob'].date() == pd.Timestamp("1900-01-01").date()):
        return pd.NaT 
    else:
        if (fdf['Mob Dob'] is not pd.NaT):
            return fdf['Mob Dob']
        else:
            return fdf['Mobdob']
    # IF [Mob Dob] = DATE(1/1/1900) THEN NULL //LLCHD
    # ELSEIF [Mobdob] = DATE(1/1/1900) THEN NULL //FW
    # ELSE IFNULL([Mob Dob],[Mobdob])
    # END
df3_edits1['_MOB DOB'] = df3_edits1.apply(func=fn_MOB_DOB, axis=1) 
    ### Data Type in Tableau: 'date'.
inspect_col(df3_edits1['_MOB DOB']) 

#%%###################################

def fn_FOB_DOB(fdf):
    ### LLCHD.
    if (fdf['Fob Dob'].date() == pd.Timestamp("1900-01-01").date()):
        return pd.NaT 
    ### FW.
    elif (fdf['Fobdob'].date() == pd.Timestamp("1900-01-01").date()):
        return pd.NaT 
    else:
        if (fdf['Fob Dob'] is not pd.NaT):
            return fdf['Fob Dob']
        else:
            return fdf['Fobdob']
    # IF [Fob Dob] = DATE(1/1/1900) THEN NULL //LLCHD
    # ELSEIF [Fobdob] = DATE(1/1/1900) THEN NULL //FW
    # ELSE IFNULL([Fob Dob],[Fobdob])
    # END
df3_edits1['_FOB DOB'] = df3_edits1.apply(func=fn_FOB_DOB, axis=1) 
    ### Data Type in Tableau: 'date'.
inspect_col(df3_edits1['_FOB DOB']) 

#%%###################################

### TO DO: Move age calculations to Tableau Report Workbook.
def fn_T04_MOB_Age(fdf):

    ### FIX:
    1

    # IF [_MOB DOB]> DATEADD('year',-DATEDIFF('year',[_MOB DOB],TODAY()),TODAY())
    # THEN DATEDIFF('year',[_MOB DOB],TODAY()-1)
    # ELSE DATEDIFF('year',[_MOB DOB],TODAY())
    # END

    # IF [_MOB DOB]> DATEADD('year',-DATEDIFF('year',[_MOB DOB],TODAY()),TODAY())
    # THEN DATEDIFF('year',[_MOB DOB],TODAY()-1)
    # ELSE DATEDIFF('year',[_MOB DOB],TODAY())
    # END
df3_edits1['_T04 MOB Age'] = df3_edits1.apply(func=fn_T04_MOB_Age, axis=1) 
    ### Data Type in Tableau: 'int'.
inspect_col(df3_edits1['_T04 MOB Age']) 

#%%###################################

### TO DO: Move age calculations to Tableau Report Workbook.
def fn_T04_FOB_Age(fdf):

    ### FIX:
    1

    # IF [_FOB DOB]> DATEADD('year',-DATEDIFF('year',[_FOB DOB],TODAY()),TODAY())
    # THEN DATEDIFF('year',[_FOB DOB],TODAY()-1)
    # ELSE DATEDIFF('year',[_FOB DOB],TODAY())
    # END

    # IF [_FOB DOB]> DATEADD('year',-DATEDIFF('year',[_FOB DOB],TODAY()),TODAY())
    # THEN DATEDIFF('year',[_FOB DOB],TODAY()-1)
    # ELSE DATEDIFF('year',[_FOB DOB],TODAY())
    # END
df3_edits1['_T04 FOB Age'] = df3_edits1.apply(func=fn_T04_FOB_Age, axis=1) 
    ### Data Type in Tableau: 'int'.
inspect_col(df3_edits1['_T04 FOB Age']) 

#%%###################################

### TO DO: Confirm that LLCHD not using "Non-Binary" yet.
def fn_MOB_Gender(fdf):
    ### FW.
    if (fdf['Adult1Gender'] == "Female"):
        return "Female" 
    elif (fdf['Adult1Gender'] == "Male"):
        return "Male"
    elif (fdf['Adult1Gender'] == "Non-Binary"):
        return "Non-Binary"
    ### LLCHD.
    elif (fdf['Mob Gender'] == "F"):
        return "Female" 
    elif (fdf['Mob Gender'] == "M"):
        return "Male"
    ### elif (fdf['Mob Gender'] == "N"):
    ###     return "Non-Binary" ### Don't have this value yet - Confirm.
    ###
    # IF [Adult1Gender] = "Female" THEN "Female" //FW
    # ELSEIF [Adult1Gender] = "Male" THEN "Male"
    # ELSEIF [Adult1Gender] = "Non-Binary" THEN "Non-Binary"
    # ELSEIF [Mob Gender]= "F" THEN "Female" //LLCHD
    # ELSEIF [Mob Gender] = "M" THEN "Male"
    # // ELSEIF [Mob Gender] = "N" THEN "Non-Binary" // Don't have this value yet - Confirm
    # END
df3_edits1['_MOB Gender'] = df3_edits1.apply(func=fn_MOB_Gender, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_MOB Gender']) 

#%%###################################

### TO DO: Question: Should we incorporate involved status into the fob variables?
### TO DO: Confirm that LLCHD not using "Non-Binary" yet.
def fn_FOB_Gender(fdf):
    ### LLCHD.
    if (fdf['Fob Involved1'] == "Y"):
        match fdf['Fob Gender']:
            case "M":
                return "Male" 
            case "F":
                return "Female"
            ### case "N":
            ###     return "Non-Binary" ### No values yet - confirm.
    ### FW.
    elif (fdf['Fob Involved'] == True):
        return fdf['Adult2Gender'] 
    ###
    else:
        return np.nan
    # //should we incorporate involved status into the fob variables?
    # IF [Fob Involved1] = "Y" THEN CASE[Fob Gender]
    #     WHEN "M" THEN "Male" //LLCHD
    #     WHEN "F" THEN "Female"
    #     // WHEN "N" THEN "Non-Binary" // No values yet - confirm
    #     END
    # ELSEIF [Fob Involved] = True THEN [Adult2Gender] //FW
    # ELSE NULL
    # END
df3_edits1['_FOB Gender'] = df3_edits1.apply(func=fn_FOB_Gender, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_FOB Gender']) 
# #%%
# inspect_col(df3_edits1['Fob Involved1']) 
# inspect_col(df3_edits1['Fob Gender']) 
# inspect_col(df3_edits1['Fob Involved']) 
# inspect_col(df3_edits1['Adult2Gender']) 


#%%###################################

def fn_T06_MOB_Ethnicity(fdf):
    ### FW?
    if pd.notna(fdf['Mob Ethnic']):
        match fdf['Mob Ethnic'].lower():
            case "non hispanic/latino":
                return "Not Hispanic or Latino"
            case "hispanic/latino":
                return "Hispanic or Latino"
            case "unknown":
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ### LLCHD?
    elif pd.notna(fdf['Mob Ethnicity']):
        match fdf['Mob Ethnicity'].lower():
            case "hispanic/latino", "hispanic":
                return "Hispanic or Latino"
            case "not hispanic/latino", "non-hispanic":
                return "Not Hispanic or Latino"
            case _:
                return "Unrecognized Value"
    ###
    else:
        return "Unknown/Did Not Report"
    # IF NOT ISNULL([Mob Ethnic]) THEN CASE [Mob Ethnic]
    #     WHEN "Non Hispanic/Latino" THEN "Not Hispanic or Latino"
    #     WHEN "Hispanic/Latino" THEN "Hispanic or Latino"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSEIF NOT ISNULL([Mob Ethnicity]) THEN CASE [Mob Ethnicity]
    #     WHEN "HISPANIC/LATINO" THEN "Hispanic or Latino" //LLCHD
    #     WHEN "Hispanic" THEN "Hispanic or Latino"
    #     WHEN "NOT HISPANIC/LATINO" THEN "Not Hispanic or Latino"
    #     WHEN "NON-Hispanic" THEN "Not Hispanic or Latino"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSE "Unknown/Did Not Report"
    # END
df3_edits1['_T06 MOB Ethnicity'] = df3_edits1.apply(func=fn_T06_MOB_Ethnicity, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_T06 MOB Ethnicity']) 

#%%###################################

### Slight difference between vars in original Tableau: "NON-HISPANIC" here instead of "NON-Hispanic" in (1).
def fn_T06_FOB_Ethnicity(fdf):
    ### FW.
    if (fdf['Fob Involved'] == True):
        if pd.isna(fdf['Fob Ethnicity']):
            return "Unknown/Did Not Report"
        else: 
            match fdf['Fob Ethnicity'].lower():
                case "non hispanic/latino":
                    return "Not Hispanic or Latino" 
                case "hispanic/latino":
                    return "Hispanic or Latino"
                case "unknown":
                    return "Unknown/Did Not Report"
                ### case np.nan:
                ###     return "Unknown/Did Not Report" ### Pulled out above.
                case _:
                    return "Unrecognized Value"
    ### LLCHD.
    elif (fdf['Fob Involved1'] == "Y"):
        if pd.isna(fdf['Fob Ethnicity1']):
            return "Unknown/Did Not Report"
        else: 
            match fdf['Fob Ethnicity1'].lower():
                case "hispanic/latino":
                    return "Hispanic or Latino" 
                case "not hispanic/latino", "non-hispanic":
                    return "Not Hispanic or Latino"
                case "unreported/refused to report":
                    return "Unknown/Did Not Report"
                ### case np.nan:
                ###     return "Unknown/Did Not Report" ### Pulled out above.
                case _:
                    return "Unrecognized Value"
    ###
    else:
        return np.nan 
    # IF [Fob Involved] = True //FW
    # THEN CASE [Fob Ethnicity]
    #     WHEN "Non Hispanic/Latino" THEN "Not Hispanic or Latino" 
    #     WHEN "Hispanic/Latino" THEN "Hispanic or Latino"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSEIF [Fob Involved1] = "Y" //LLCHD
    # THEN CASE [Fob Ethnicity1]
    #     WHEN "HISPANIC/LATINO" THEN "Hispanic or Latino" 
    #     WHEN "NOT HISPANIC/LATINO" THEN "Not Hispanic or Latino"
    #     WHEN "NON-HISPANIC" THEN "Not Hispanic or Latino" 
    #     WHEN "UNREPORTED/REFUSED TO REPORT" THEN "Unknown/Did Not Report"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSE NULL
    # END
df3_edits1['_T06 FOB Ethnicity'] = df3_edits1.apply(func=fn_T06_FOB_Ethnicity, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_T06 FOB Ethnicity']) 
# #%%
# inspect_col(df3_edits1['Fob Involved']) 
# #%%
# inspect_col(df3_edits1['Fob Ethnicity']) 
# #%%
# inspect_col(df3_edits1['Fob Involved1']) 
# #%%
# inspect_col(df3_edits1['Fob Ethnicity1']) 

#%%###################################

### TO DO: Check: Is this a Duplicate? Is it Used?
    ### Slight difference between vars in original Tableau: "NON-Hispanic" in (1) vs "NON-HISPANIC" in original.
    ### However, duplicate Python code because of the ".lower()".
def fn_T06_FOB_Ethnicity_1(fdf):
    ### FW.
    if (fdf['Fob Involved'] == True):
        if pd.isna(fdf['Fob Ethnicity']):
            return "Unknown/Did Not Report"
        else: 
            match fdf['Fob Ethnicity'].lower():
                case "non hispanic/latino":
                    return "Not Hispanic or Latino" 
                case "hispanic/latino":
                    return "Hispanic or Latino"
                case "unknown":
                    return "Unknown/Did Not Report"
                ### case np.nan:
                ###     return "Unknown/Did Not Report" ### Pulled out above.
                case _:
                    return "Unrecognized Value"
    ### LLCHD.
    elif (fdf['Fob Involved1'] == "Y"):
        if pd.isna(fdf['Fob Ethnicity1']):
            return "Unknown/Did Not Report"
        else: 
            match fdf['Fob Ethnicity1'].lower():
                case "hispanic/latino":
                    return "Hispanic or Latino" 
                case "not hispanic/latino", "non-hispanic":
                    return "Not Hispanic or Latino"
                case "unreported/refused to report":
                    return "Unknown/Did Not Report"
                ### case np.nan:
                ###     return "Unknown/Did Not Report" ### Pulled out above.
                case _:
                    return "Unrecognized Value"
    ###
    else:
        return np.nan 
    # IF [Fob Involved] = True //FW
    # THEN CASE [Fob Ethnicity]
    #     WHEN "Non Hispanic/Latino" THEN "Not Hispanic or Latino" 
    #     WHEN "Hispanic/Latino" THEN "Hispanic or Latino"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSEIF [Fob Involved1] = "Y" //LLCHD
    # THEN CASE [Fob Ethnicity1]
    #     WHEN "HISPANIC/LATINO" THEN "Hispanic or Latino" 
    #     WHEN "NOT HISPANIC/LATINO" THEN "Not Hispanic or Latino"
    #     WHEN "NON-Hispanic" THEN "Not Hispanic or Latino"
    #     WHEN "UNREPORTED/REFUSED TO REPORT" THEN "Unknown/Did Not Report"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSE NULL
    # END
df3_edits1['_T06 FOB Ethnicity (1)'] = df3_edits1.apply(func=fn_T06_FOB_Ethnicity_1, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_T06 FOB Ethnicity (1)']) 

#%%###################################

def fn_T07_MOB_Race(fdf):
    ###########
    ### LLCHD.
    ### multiracial.
    if (
        (
            (0 if pd.isna(fdf['Mob Race Asian']) else (1 if fdf['Mob Race Asian']=="Y" else 0)) + 
            (0 if pd.isna(fdf['Mob Race Black']) else (1 if fdf['Mob Race Black']=="Y" else 0)) + 
            (0 if pd.isna(fdf['Mob Race Hawaiian']) else (1 if fdf['Mob Race Hawaiian']=="Y" else 0)) + 
            (0 if pd.isna(fdf['Mob Race Indian']) else (1 if fdf['Mob Race Indian']=="Y" else 0)) + 
            (0 if pd.isna(fdf['Mob Race White']) else (1 if fdf['Mob Race White']=="Y" else 0)) + 
            (0 if pd.isna(fdf['Mob Race Other']) else (1 if fdf['Mob Race Other']=="Y" else 0)) 
        ) > 1 
    ):
        return "More than one race"
    ### single race.
    elif (fdf['Mob Race Asian'] == "Y"):
        return "Asian"
    elif (fdf['Mob Race Black'] == "Y"):
        return "Black or African American"
    elif (fdf['Mob Race Hawaiian'] == "Y"):
        return "Native Hawaiian or Other Pacific Islander"
    elif (fdf['Mob Race Indian'] == "Y"):
        return "American Indian or Alaska Native"
    elif (fdf['Mob Race White'] == "Y"):
        return "White"
    elif (fdf['Mob Race Other'] == "Y"):
        return "Other"
    ###########
    ### FW.
    ### multiracial, == "True" is not required in IIF statement because race is boolean.
    elif (
        (
            (0 if pd.isna(fdf['MOB Race Asian']) else (1 if fdf['MOB Race Asian'] else 0)) + 
            (0 if pd.isna(fdf['MOB Race Black']) else (1 if fdf['MOB Race Black'] else 0)) + 
            (0 if pd.isna(fdf['MOB Race Hawaiian Pacific']) else (1 if fdf['MOB Race Hawaiian Pacific'] else 0)) + 
            (0 if pd.isna(fdf['MOB Race Indian Alaskan']) else (1 if fdf['MOB Race Indian Alaskan'] else 0)) + 
            (0 if pd.isna(fdf['MOB Race White']) else (1 if fdf['MOB Race White'] else 0)) + 
            (0 if pd.isna(fdf['MOB Race Other']) else (1 if fdf['MOB Race Other'] else 0)) 
        ) > 1 
    ):
        return "More than one race"
    ### single race.
    elif (fdf['MOB Race Asian'] == True):
        return "Asian"
    elif (fdf['MOB Race Black'] == True):
        return "Black or African American"
    elif (fdf['MOB Race Hawaiian Pacific'] == True):
        return "Native Hawaiian or Other Pacific Islander"
    elif (fdf['MOB Race Indian Alaskan'] == True):
        return "American Indian or Alaska Native"
    elif (fdf['MOB Race White'] == True):
        return "White"
    elif (fdf['MOB Race Other'] == True):
        return "Other"
    ###########
    else:
        return "Unknown/Did Not Report"
    # //LLCHD
    # //multiracial
    # IF IIF([Mob Race Asian]="Y",1,0,0)+IIF([Mob Race Black]="Y",1,0,0)+IIF([Mob Race Hawaiian]="Y",1,0,0)+IIF([Mob Race Indian]="Y",1,0,0)
    # +IIF([Mob Race Other]="Y",1,0,0)+IIF([Mob Race White]="Y",1,0,0) > 1 THEN "More than one race"
    # //single race
    # ELSEIF [Mob Race Asian] = "Y" THEN "Asian"
    # ELSEIF [Mob Race Black] = "Y" THEN "Black or African American"
    # ELSEIF [Mob Race Hawaiian] = "Y" THEN "Native Hawaiian or Other Pacific Islander"
    # ELSEIF [Mob Race Indian] = "Y" THEN "American Indian or Alaska Native"
    # ELSEIF [Mob Race White] = "Y" THEN "White"
    # ELSEIF [Mob Race Other] = "Y" THEN "Other"
    # //FW
    # //multiracial, = "True" is not required in IIF statement because race is boolean
    # ELSEIF IIF([MOB Race Asian],1,0,0)+IIF([MOB Race Black],1,0,0)+IIF([MOB Race Hawaiian Pacific],1,0,0)
    # +IIF([MOB Race Indian Alaskan],1,0,0)+IIF([MOB Race White],1,0,0) +IIF([MOB Race Other],1,0,0) > 1 
    # THEN "More than one race"
    # //single race
    # ELSEIF [MOB Race Asian] = True THEN "Asian"
    # ELSEIF [MOB Race Black] = True THEN "Black or African American"
    # ELSEIF [MOB Race Hawaiian Pacific] = True THEN "Native Hawaiian or Other Pacific Islander"
    # ELSEIF [MOB Race Indian Alaskan] = True THEN "American Indian or Alaska Native"
    # ELSEIF [MOB Race White] = True THEN "White"
    # ELSEIF [MOB Race Other] = True THEN "Other"
    # ELSE "Unknown/Did Not Report"
    # END
df3_edits1['_T07 MOB Race'] = df3_edits1.apply(func=fn_T07_MOB_Race , axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_T07 MOB Race']) 

#%%###################################

def fn_T07_FOB_Race(fdf):
    ###########
    ### LLCHD.
    ### multiracial.
    if (fdf['Fob Involved1'] == "Y"):
        if (
            (
                (0 if pd.isna(fdf['Fob Race Asian']) else (1 if fdf['Fob Race Asian']=="Y" else 0)) + 
                (0 if pd.isna(fdf['Fob Race Black']) else (1 if fdf['Fob Race Black']=="Y" else 0)) + 
                (0 if pd.isna(fdf['Fob Race Hawaiian']) else (1 if fdf['Fob Race Hawaiian']=="Y" else 0)) + 
                (0 if pd.isna(fdf['Fob Race Indian']) else (1 if fdf['Fob Race Indian']=="Y" else 0)) + 
                (0 if pd.isna(fdf['Fob Race White']) else (1 if fdf['Fob Race White']=="Y" else 0)) + 
                (0 if pd.isna(fdf['Fob Race Other']) else (1 if fdf['Fob Race Other']=="Y" else 0)) 
            ) > 1
        ):
            return "More than one race"
        ### single race.
        elif (fdf['Fob Race Asian'] == "Y"):
            return "Asian"
        elif (fdf['Fob Race Black'] == "Y"):
            return "Black or African American"
        elif (fdf['Fob Race Hawaiian'] == "Y"):
            return "Native Hawaiian or Other Pacific Islander"
        elif (fdf['Fob Race Indian'] == "Y"):
            return "American Indian or Alaska Native"
        elif (fdf['Fob Race White'] == "Y"):
            return "White"
        elif (fdf['Fob Race Other'] == "Y"):
            return "Other"
        else:
            return "Unknown/Did Not Report"
    ###########
    ### FW.
    ### multiracial, == "True" is not required in IIF statement because race is boolean.
    elif (fdf['Fob Involved'] == True):
        if (
            (
                (0 if pd.isna(fdf['FOB Race Asian']) else (1 if fdf['FOB Race Asian'] else 0)) + 
                (0 if pd.isna(fdf['FOB Race Black']) else (1 if fdf['FOB Race Black'] else 0)) + 
                (0 if pd.isna(fdf['FOB Race Hawaiian Pacific']) else (1 if fdf['FOB Race Hawaiian Pacific'] else 0)) + 
                (0 if pd.isna(fdf['FOB Race Indian Alaskan']) else (1 if fdf['FOB Race Indian Alaskan'] else 0)) + 
                (0 if pd.isna(fdf['FOB Race White']) else (1 if fdf['FOB Race White'] else 0)) + 
                (0 if pd.isna(fdf['FOB Race Other']) else (1 if fdf['FOB Race Other'] else 0)) 
            ) > 1
        ):
            return "More than one race"
        ### single race.
        elif (fdf['FOB Race Asian'] == True):
            return "Asian"
        elif (fdf['FOB Race Black'] == True):
            return "Black or African American"
        elif (fdf['FOB Race Hawaiian Pacific'] == True):
            return "Native Hawaiian or Other Pacific Islander"
        elif (fdf['FOB Race Indian Alaskan'] == True):
            return "American Indian or Alaska Native"
        elif (fdf['FOB Race White'] == True):
            return "White"
        elif (fdf['FOB Race Other'] == True):
            return "Other"
        else:
            return "Unknown/Did Not Report"
    ###########
    else:
        return np.nan 
    # //LLCHD
    # //multiracial
    # IF [Fob Involved1]= "Y" THEN 
    #     (IF(IIF([Fob Race Asian] ="Y",1,0,0)+IIF([Fob Race Black] ="Y",1,0,0)+IIF([Fob Race Hawaiian]="Y",1,0,0)
    #     +IIF([Fob Race Indian]="Y",1,0,0)+IIF([Fob Race Other]="Y",1,0,0)+IIF([Fob Race White]="Y",1,0,0) > 1) THEN "More than one race"
    #     //single race
    #     ELSEIF [Fob Race Asian] = "Y" THEN "Asian"
    #     ELSEIF [Fob Race Black] = "Y" THEN "Black or African American"
    #     ELSEIF [Fob Race Hawaiian] = "Y" THEN "Native Hawaiian or Other Pacific Islander"
    #     ELSEIF [Fob Race Indian] = "Y" THEN "American Indian or Alaska Native"
    #     ELSEIF [Fob Race White] = "Y" THEN "White"
    #     ELSEIF [Fob Race Other] = "Y" THEN "Other"
    #     ELSE "Unknown/Did Not Report"
    #     END)
    # //FW
    # //multiracial, = "True" is not required in IIF statement because race is boolean
    # ELSEIF [Fob Involved]= True THEN 
    #     (IF(IIF([FOB Race Asian],1,0,0)+IIF([FOB Race Black],1,0,0)+IIF([FOB Race Hawaiian Pacific],1,0,0)
    #     +IIF([FOB Race Indian Alaskan],1,0,0)+IIF([FOB Race White],1,0,0) +IIF([FOB Race Other],1,0,0) > 1) THEN "More than one race"
    #     //single race
    #     ELSEIF [FOB Race Asian] = True THEN "Asian"
    #     ELSEIF [FOB Race Black] = True THEN "Black or African American"
    #     ELSEIF [FOB Race Hawaiian Pacific] = True THEN "Native Hawaiian or Other Pacific Islander"
    #     ELSEIF [FOB Race Indian Alaskan] = True THEN "American Indian or Alaska Native"
    #     ELSEIF [FOB Race White] = True THEN "White"
    #     ELSEIF [FOB Race Other] = True THEN "Other"
    #     ELSE "Unknown/Did Not Report"
    #     END)
    # ELSE NULL
    # END
df3_edits1['_T07 FOB Race'] = df3_edits1.apply(func=fn_T07_FOB_Race, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_T07 FOB Race']) 

#%%###################################

def fn_T08_MOB_Marital_Status(fdf):
    ###########
    ### FW.
    if (pd.notna(fdf['Adult1MaritalStatus'])):
        match fdf['Adult1MaritalStatus'].lower():
            case "married":
                return "Married"
            case "living with partner":
                return "Not Married but Living Together with Partner"
            case "separated" | "legally separated" | "divorced" | "widowed":
                return "Separated, Divorced, or Widowed"
            case "single":
                return "Never Married"
            case "unknown" | "null":
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ###########
    ### LLCHD.
    elif (pd.notna(fdf['Mob Marital Status'])):
        match fdf['Mob Marital Status'].lower():
            case "married":
                return "Married"
            case "life partner":
                return "Not Married but Living Together with Partner"
            case "separated" | "legally separated" | "divorced" | "widowed":
                return "Separated, Divorced, or Widowed"
            case "single" | "not married":
                return "Never Married"
            case "unknown":
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ###########
    else:
        return "Unknown/Did Not Report"
    # //FW
    # IF NOT ISNULL([Adult1MaritalStatus]) THEN CASE [Adult1MaritalStatus]
    #     WHEN "Divorced" THEN "Separated, Divorced, or Widowed"
    #     WHEN "Living with Partner" THEN "Not Married but Living Together with Partner"
    #     WHEN "Married" THEN "Married"
    #     WHEN "Separated" THEN "Separated, Divorced, or Widowed"
    #     WHEN "Single" THEN "Never Married"
    #     WHEN "Widowed" THEN "Separated, Divorced, or Widowed"
    #     WHEN "Legally Separated" THEN "Separated, Divorced, or Widowed"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     WHEN "null" THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # //LLCHD
    # ELSEIF NOT ISNULL([Mob Marital Status]) THEN CASE [Mob Marital Status] 
    #     WHEN "DIVORCED" THEN "Separated, Divorced, or Widowed"
    #     WHEN "Divorced" THEN "Separated, Divorced, or Widowed"
    #     WHEN "LEGALLY SEPARATED" THEN "Separated, Divorced, or Widowed"
    #     WHEN "LIFE PARTNER" THEN "Not Married but Living Together with Partner"
    #     WHEN "MARRIED" THEN "Married"
    #     WHEN "Married" THEN "Married"
    #     WHEN "SEPARATED" THEN "Separated, Divorced, or Widowed"
    #     WHEN "Separated" THEN "Separated, Divorced, or Widowed"
    #     WHEN "SINGLE" THEN "Never Married"
    #     WHEN "UNKNOWN" THEN "Unknown/Did Not Report"
    #     WHEN "WIDOWED" THEN "Separated, Divorced, or Widowed"
    #     WHEN "Not Married" THEN "Never Married"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSE "Unknown/Did Not Report"
    # END
df3_edits1['_T08 MOB Marital Status'] = df3_edits1.apply(func=fn_T08_MOB_Marital_Status, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_T08 MOB Marital Status']) 

#%%###################################

### TO DO: Fix ERROR: using ['Mob Marital Status'] when should be using ['Fob Marital Status'].
def fn_T08_FOB_Marital_Status(fdf):
    ###########
    ### FW.
    if (fdf['Fob Involved'] == True):
        if pd.isna(fdf['Adult2MaritalStatus']):
            return "Unknown/Did Not Report"
        else:
            match fdf['Adult2MaritalStatus'].lower():
                case "married":
                    return "Married"
                case "living with partner":
                    return "Not Married but Living Together with Partner"
                case "separated" | "legally separated" | "divorced" | "widowed":
                    return "Separated, Divorced, or Widowed"
                case "single":
                    return "Never Married"
                case "unknown" | "null":
                    return "Unknown/Did Not Report"
                ### case np.nan:
                ###     return "Unknown/Did Not Report" ### Pulled out above.
                case _:
                    return "Unrecognized Value"
    ###########
    ### LLCHD.
    elif (fdf['Fob Involved1'] == "Y"):
        if pd.isna(fdf['Mob Marital Status']):
            return "Unknown/Did Not Report"
        else:
            match fdf['Mob Marital Status'].lower():
                case "married":
                    return "Married"
                case "life partner":
                    return "Not Married but Living Together with Partner"
                case "separated" | "legally separated" | "divorced" | "widowed":
                    return "Separated, Divorced, or Widowed"
                case "single" | "not married":
                    return "Never Married"
                case "unknown":
                    return "Unknown/Did Not Report"
                ### case np.nan:
                ###     return "Unknown/Did Not Report" ### Pulled out above.
                case _:
                    return "Unrecognized Value"
    ###########
    else:
        return np.nan 
    # //FW
    # IF [Fob Involved] = True THEN CASE [Adult2MaritalStatus]
    #     WHEN "Divorced" THEN "Separated, Divorced, or Widowed"
    #     WHEN "Living with Partner" THEN "Not Married but Living Together with Partner"
    #     WHEN "Married" THEN "Married"
    #     WHEN "Separated" THEN "Separated, Divorced, or Widowed"
    #     WHEN "Single" THEN "Never Married"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     WHEN "Null" THEN "Unknown/Did Not Report"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # //LLCHD
    # ELSEIF [Fob Involved1] = "Y" THEN CASE [Mob Marital Status] 
    #     WHEN "DIVORCED" THEN "Separated, Divorced, or Widowed"
    #     WHEN "LEGALLY SEPARATED" THEN "Separated, Divorced, or Widowed"
    #     WHEN "LIFE PARTNER" THEN "Not Married but Living Together with Partner"
    #     WHEN "MARRIED" THEN "Married"
    #     WHEN "SEPARATED" THEN "Separated, Divorced, or Widowed"
    #     WHEN "SINGLE" THEN "Never Married"
    #     WHEN "Not Married" THEN "Never Married"
    #     WHEN "UNKNOWN" THEN "Unknown/Did Not Report"
    #     WHEN "WIDOWED" THEN "Separated, Divorced, or Widowed"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSE NULL
    # END
df3_edits1['_T08 FOB Marital Status'] = df3_edits1.apply(func=fn_T08_FOB_Marital_Status, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_T08 FOB Marital Status']) 

#%%###################################

def fn_T11_MOB_Employment(fdf):
    ###########
    ### FW.
    if (pd.notna(fdf['AD1EmpStatus'])):
        match fdf['AD1EmpStatus'].lower():
            case "employed full time" | "maternal leave, paid, full time" | "maternal leave, unpaid, full time":
                return "Employed Full Time"
            case "employed part time" | "maternal leave, unpaid, part time" | "self-employed":
                return "Employed Part Time"
            case (
                "temporary disability" |
                "permanent disability" |
                "unemployed - unspecified" |
                "unemployed not seeking work-barriers" |
                "unemployed not seeking work-preference" |
                "unemployed not seeking work-teen caregiver" |
                "unemployed seeking work"
            ):
                return "Not Employed"
            case "unknown":
                return "Unknown/Did Not Report"
            case "null":
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ###########
    ### LLCHD.
    elif (pd.notna(fdf['Mcafss Employ'])):
        match fdf['Mcafss Employ']:
            case 1 | 2:
                return "Not Employed"
            case 3 | 4 | 5:
                return "Employed Full Time"
            case _:
                return "Unrecognized Value"
    ###########
    else:
        return "Unknown/Did Not Report"
    # IF NOT ISNULL([AD1EmpStatus]) THEN CASE [AD1EmpStatus] //FW
    #     WHEN "Employed Full Time" THEN "Employed Full Time"
    #     WHEN "Employed Part Time" THEN "Employed Part Time"
    #     WHEN "Maternal leave, paid, full time" THEN "Employed Full Time"
    #     WHEN "Maternal leave, unpaid, full time" THEN "Employed Full Time"
    #     WHEN "Maternal leave, unpaid, part time" THEN "Employed Part Time"
    #     WHEN "Permanent Disability" THEN "Not Employed"
    #     WHEN "Self-Employed" THEN "Employed Part Time"
    #     WHEN "Temporary Disability" THEN "Not Employed"
    #     WHEN "Unemployed - Unspecified" THEN "Not Employed"
    #     WHEN "Unemployed Not Seeking Work-Barriers" THEN "Not Employed"
    #     WHEN "Unemployed Not Seeking Work-Preference" THEN "Not Employed"
    #     WHEN "Unemployed Not Seeking Work-Teen Caregiver" THEN "Not Employed"
    #     WHEN "Unemployed Seeking Work" THEN "Not Employed"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSEIF NOT ISNULL([Mcafss Employ])THEN CASE [Mcafss Employ] //LLCHD
    #     WHEN 1 THEN "Not Employed"
    #     WHEN 2 THEN "Not Employed"
    #     WHEN 3 THEN "Employed Part Time"
    #     WHEN 4 THEN "Employed Full Time"
    #     WHEN 5 THEN "Employed Full Time"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSE "Unknown/Did Not Report"
    # END
df3_edits1['_T11 MOB Employment'] = df3_edits1.apply(func=fn_T11_MOB_Employment, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_T11 MOB Employment']) 

#%%###################################

def fn_T11_FOB_Employment(fdf):
    ###########
    ### FW.
    if (fdf['Fob Involved'] == True):
        if pd.isna(fdf['AD2EmployStatus']):
            return "Unknown/Did Not Report"
        else:
            match fdf['AD2EmployStatus'].lower():
                case "employed full time" | "maternal leave, paid, full time" | "maternal leave, unpaid, full time":
                    return "Employed Full Time"
                case "employed part time" | "maternal leave, unpaid, part time" | "self-employed":
                    return "Employed Part Time"
                case (
                    "temporary disability" |
                    "permanent disability" |
                    "unemployed - unspecified" |
                    "unemployed not seeking work-barriers" |
                    "unemployed not seeking work-preference" |
                    "unemployed not seeking work-teen caregiver" |
                    "unemployed seeking work"
                ):
                    return "Not Employed"
                case "unknown":
                    return "Unknown/Did Not Report"
                case "null":
                    return "Unknown/Did Not Report"
                ### case np.nan:
                ###     return "Unknown/Did Not Report" ### Pulled out above.
                case _:
                    return "Unrecognized Value"
    ###########
    ### LLCHD.
    elif (fdf['Fob Involved1'] == "Y"):
        if pd.isna(fdf['Fob Employ']):
            return "Unknown/Did Not Report"
        else:
            match fdf['Fob Employ']:
                case 1 | 2:
                    return "Not Employed"
                case 3 | 4 | 5:
                    return "Employed Full Time"
                ### case np.nan:
                ###     return "Unknown/Did Not Report" ### Pulled out above.
                case _:
                    return "Unrecognized Value"
    ###########
    else:
        return np.nan 
    # IF [Fob Involved] = True THEN CASE [AD2EmployStatus] //FW
    #     WHEN "Employed Full Time" THEN "Employed Full Time"
    #     WHEN "Employed Part Time" THEN "Employed Part Time"
    #     WHEN "Maternal leave, paid, full time" THEN "Employed Full Time"
    #     WHEN "Maternal leave, unpaid, full time" THEN "Employed Full Time"
    #     WHEN "Maternal leave, unpaid, part time" THEN "Employed Part Time"
    #     WHEN "Permanent Disability" THEN "Not Employed"
    #     WHEN "Self-Employed" THEN "Employed Part Time"
    #     WHEN "Unemployed - Unspecified" THEN "Not Employed"
    #     WHEN "Unemployed Not Seeking Work-Barriers" THEN "Not Employed"
    #     WHEN "Unemployed Not Seeking Work-Preference" THEN "Not Employed"
    #     WHEN "Unemployed Not Seeking Work-Teen Caregiver" THEN "Not Employed"
    #     WHEN "Unemployed Seeking Work" THEN "Not Employed"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     WHEN "null" THEN "Unknown/Did Not Report"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSEIF [Fob Involved1] = "Y" THEN CASE [Fob Employ] //LLCHD
    #     WHEN 1 THEN "Not Employed"
    #     WHEN 2 THEN "Not Employed"
    #     WHEN 3 THEN "Employed Part Time"
    #     WHEN 4 THEN "Employed Full Time"
    #     WHEN 5 THEN "Employed Full Time"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSE NULL
    # END
df3_edits1['_T11 FOB Employment'] = df3_edits1.apply(func=fn_T11_FOB_Employment, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_T11 FOB Employment']) 

#%%###################################

### TO DO: Ask about significant difference from '_C15 Max Educational Enrollment'.
def fn_C15_Min_Educational_Enrollment(fdf):
    ###########
    ### FW.
    if (fdf['Min Edu Enroll'] == "College 2 Year"):
        return "Student/trainee" 
    elif (fdf['Min Edu Enroll'] == "College 4 Year"):
        return "Student/trainee"
    elif (fdf['Min Edu Enroll'] == "ESL"):
        return "Student/trainee"
    elif (fdf['Min Edu Enroll'] == "GED Program"):
        return "Student/trainee HS/GED"
    elif (fdf['Min Edu Enroll'] == "Graduate School"):
        return "Student/trainee"
    elif (fdf['Min Edu Enroll'] == "High/Middle School"):
        return "Student/trainee HS/GED"
    elif (fdf['Min Edu Enroll'] == "Not Enrolled in School"):
        return "Not a student/trainee"
    elif (fdf['Min Edu Enroll'] == "Unknown"):
        return "Unknown/Did not Report"
    elif (fdf['Min Edu Enroll'] == "Vocational College"):
        return "Student/trainee"
    ###########
    ### LLCHD.
    elif (
        fdf['mcafss_edu1_enroll'] == "YES" ### Enrolled.
        and
        (
            fdf['mcafss_edu1_prog'] == 1 ### Enrolled in Middle School.
            or
            fdf['mcafss_edu1_prog'] == 2 ### Enrolled in High School.
            or
            fdf['mcafss_edu1_prog'] == 3 ### Enrolled in GED.
        )
    ):
        return "Student/trainee HS/GED" 
    elif (fdf['mcafss_edu1_enroll'] == "NO"):
        return "Student/trainee" ### Only significant difference from '_C15 Max Educational Enrollment'.
    ###########
    else:
        return "Unknown/Did not Report"
    # IF [Min Edu Enroll] = "College 2 Year" THEN "Student/trainee" //FW
    # ELSEIF [Min Edu Enroll] = "College 4 Year" THEN "Student/trainee"
    # ELSEIF [Min Edu Enroll] = "ESL" THEN "Student/trainee"
    # ELSEIF [Min Edu Enroll] = "GED Program" THEN "Student/trainee HS/GED"
    # ELSEIF [Min Edu Enroll] = "Graduate School" THEN "Student/trainee"
    # ELSEIF [Min Edu Enroll] = "High/Middle School" THEN "Student/trainee HS/GED"
    # ELSEIF [Min Edu Enroll] = "Not Enrolled in School" THEN "Not a student/trainee"
    # ELSEIF [Min Edu Enroll] = "Unknown" THEN "Unknown/Did not Report"
    # ELSEIF [Min Edu Enroll] = "Vocational College" THEN "Student/trainee"
    # ELSEIF ([mcafss_edu1_enroll] = "YES" // Enrolled
    #         AND
    #         ([mcafss_edu1_prog] = 1 // Enrolled in Middle School
    #         OR
    #         [mcafss_edu1_prog] = 2 // Enrolled in High School
    #         OR
    #         [mcafss_edu1_prog] = 3 // Enrolled in GED
    #         )) THEN "Student/trainee HS/GED" //LLCHD
    # ELSEIF [mcafss_edu1_enroll] = "NO" THEN "Student/trainee"
    # ELSE "Unknown/Did not Report"
    # END
    # //Student/trainee indicates enrollment in a program other than a high school diploma or GED
    # //LLCHD - Kodi sent this coding for mcafss_edu1_prog on 12/7/2021
    # //01 = Middle School
    # //02 = High School
    # //03 = GED
    # //04 = ESL
    # //05 = Adult education in basic reading or math
    # //06 = College
    # //07 = Vocational training, technical or trade school (excluding training received during HS)
    # //08 = Job search or job placement
    # //09 = Work experience
    # //10 = Other (Specify)
df3_edits1['_C15 Min Educational Enrollment'] = df3_edits1.apply(func=fn_C15_Min_Educational_Enrollment, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C15 Min Educational Enrollment']) 

#%%###################################

def fn_C15_Max_Educational_Enrollment(fdf):
    ###########
    ### FW.
    if (fdf['Max Edu Enroll'] == "College 2 Year"):
        return "Student/trainee" 
    elif (fdf['Max Edu Enroll'] == "College 4 Year"):
        return "Student/trainee"
    elif (fdf['Max Edu Enroll'] == "ESL"):
        return "Student/trainee"
    elif (fdf['Max Edu Enroll'] == "GED Program"):
        return "Student/trainee HS/GED"
    elif (fdf['Max Edu Enroll'] == "Graduate School"):
        return "Student/trainee"
    elif (fdf['Max Edu Enroll'] == "High/Middle School"):
        return "Student/trainee HS/GED"
    elif (fdf['Max Edu Enroll'] == "Not Enrolled in School"):
        return "Not a student/trainee"
    elif (fdf['Max Edu Enroll'] == "Unknown"):
        return "Unknown/Did not Report"
    elif (fdf['Max Edu Enroll'] == "Vocational College"):
        return "Student/trainee"
    ###########
    ### LLCHD.
    elif (
        fdf['mcafss_edu2_enroll'] == "YES" ### Enrolled.
        and
        (
            fdf['mcafss_edu2_prog'] == 1 ### Enrolled in Middle School.
            or
            fdf['mcafss_edu2_prog'] == 2 ### Enrolled in High School.
            or
            fdf['mcafss_edu2_prog'] == 3 ### Enrolled in GED.
        )
    ):
        return "Student/trainee HS/GED" 
    elif (fdf['mcafss_edu2_enroll'] == "NO"):
        return "Not a student/trainee" ### Only significant difference from '_C15 Min Educational Enrollment'.
    ###########
    else:
        return "Unknown/Did not Report"
    # IF [Max Edu Enroll] = "College 2 Year" THEN "Student/trainee" //FW
    # ELSEIF [Max Edu Enroll] = "College 4 Year" THEN "Student/trainee"
    # ELSEIF [Max Edu Enroll] = "ESL" THEN "Student/trainee"
    # ELSEIF [Max Edu Enroll] = "GED Program" THEN "Student/trainee HS/GED"
    # ELSEIF [Max Edu Enroll] = "Graduate School" THEN "Student/trainee"
    # ELSEIF [Max Edu Enroll] = "High/Middle School" THEN "Student/trainee HS/GED"
    # ELSEIF [Max Edu Enroll] = "Not Enrolled in School" THEN "Not a student/trainee"
    # ELSEIF [Max Edu Enroll] = "Unknown" THEN "Unknown/Did not Report"
    # ELSEIF [Max Edu Enroll] = "Vocational College" THEN "Student/trainee"
    # ELSEIF ([mcafss_edu2_enroll] = "YES" // Enrolled
    #         AND
    #         ([mcafss_edu2_prog] = 1 // Enrolled in Middle School
    #         OR
    #         [mcafss_edu2_prog] = 2 // Enrolled in High School
    #         OR
    #         [mcafss_edu2_prog] = 3 // Enrolled in GED
    #         )) THEN "Student/trainee HS/GED" //LLCHD
    # ELSEIF [mcafss_edu2_enroll] = "NO" THEN "Not a student/trainee"
    # ELSE "Unknown/Did not Report"
    # END
    # //Student/trainee indicates enrollment in a program other than a high school diploma or GED
    # //LLCHD - Kodi sent this coding for mcafss_edu2_prog on 12/7/2021
    # //01 = Middle School
    # //02 = High School
    # //03 = GED
    # //04 = ESL
    # //05 = Adult education in basic reading or math
    # //06 = College
    # //07 = Vocational training, technical or trade school (excluding training received during HS)
    # //08 = Job search or job placement
    # //09 = Work experience
    # //10 = Other (Specify)
df3_edits1['_C15 Max Educational Enrollment'] = df3_edits1.apply(func=fn_C15_Max_Educational_Enrollment, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C15 Max Educational Enrollment']) 

#%%###################################

def fn_T10_FOB_Educational_Enrollment(fdf):
    ### max.
    ###########
    ### FW.
    if (fdf['Fob Involved'] == True):
        if pd.isna(fdf['AD2InSchool']):
            return "Unknown/Did Not Report"
        else:
            match fdf['AD2InSchool'].lower():
                case (
                    "esl" |
                    "ged program" |
                    "high/middle school" |
                    "vocational college" |
                    "college 2 year" |
                    "college 4 year" |
                    "graduate school"
                ):
                    return "Student/trainee" 
                case "not enrolled in school":
                    return "Not a student/trainee"
                case "unknown":
                    return "Unknown/Did Not Report"
                ### case np.nan:
                ###     return "Unknown/Did Not Report"
    ###########
    ### LLCHD.
    elif (fdf['Fob Involved1'] == "Y"):
        if pd.isna(fdf['Fob Edu']):
            return "Unknown/Did Not Report"
        else:
            match fdf['Fob Edu']:
                case 1 | 9:
                    return "Student/trainee"
                case 2 | 3 | 4 | 5 | 6 | 7 | 8 | 10 | 11:
                    return "Not a student/trainee"
                case 12:
                    return "Unknown/Did Not Report"
                ### case np.nan:
                ###     return "Unknown/Did Not Report"
    ###########
    else:
        return np.nan 
    # //max
    # IF [Fob Involved] = True THEN CASE[AD2InSchool] //FW
    #     WHEN "College 2 Year" THEN "Student/trainee" 
    #     WHEN "College 4 Year" THEN "Student/trainee"
    #     WHEN "ESL" THEN "Student/trainee"
    #     WHEN "GED Program" THEN "Student/trainee" 
    #     WHEN "Graduate School" THEN "Student/trainee"
    #     WHEN "High/Middle School" THEN "Student/trainee" 
    #     WHEN "Not Enrolled in School" THEN "Not a student/trainee"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     WHEN "Vocational College" THEN "Student/trainee"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     END
    # ELSEIF [Fob Involved1] = "Y" THEN CASE[Fob Edu] //LLCHD
    #     WHEN 1 THEN "Student/trainee"
    #     WHEN 2 THEN "Not a student/trainee"
    #     WHEN 3 THEN "Not a student/trainee"
    #     WHEN 4 THEN "Not a student/trainee"
    #     WHEN 5 THEN "Not a student/trainee"
    #     WHEN 6 THEN "Not a student/trainee"
    #     WHEN 7 THEN "Not a student/trainee"
    #     WHEN 8 THEN "Not a student/trainee"
    #     WHEN 9 THEN "Student/trainee"
    #     WHEN 10 THEN "Not a student/trainee"
    #     WHEN 11 THEN "Not a student/trainee"
    #     WHEN 12 THEN "Unknown/Did Not Report"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     END
    # ELSE NULL
    # END
df3_edits1['_T10 FOB Educational Enrollment'] = df3_edits1.apply(func=fn_T10_FOB_Educational_Enrollment, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_T10 FOB Educational Enrollment']) 

#%%###################################

def fn_C15_Min_Educational_Status(fdf):
    ###########
    ### LLCHD.
    if (fdf['Mcafss Edu1'] == 1):
        return "Less than HS diploma" ### Less than 8th Grade.
    elif (fdf['Mcafss Edu1'] == 2):
        return "Less than HS diploma" ### 8-11th Grade.
    elif (fdf['Mcafss Edu1'] == 3):
        return "HS diploma/GED" ### High School Grad.
    elif (fdf['Mcafss Edu1'] == 4):
        return "HS diploma/GED" ### Completed a GED.
    elif (fdf['Mcafss Edu1'] == 5):
        return "Technical Training or Associates Degree" ### Vocational School after High School.
    elif (fdf['Mcafss Edu1'] == 6):
        return "Some college/training" ### Some College.
    elif (fdf['Mcafss Edu1'] == 7):
        return "Technical Training or Associates Degree" ### Associates Degree.
    elif (fdf['Mcafss Edu1'] == 8):
        return "Bachelor's Degree or Higher" ### Bachelors Degree or Higher.
    ### elif (fdf['Mcafss Edu1'] == 9):
    ###     return "HS diploma/GED" ### currently enrolled in college - vocational training or trade apprenticeship.
    ### elif (fdf['Mcafss Edu1'] == 10):
    ###     return "HS diploma/GED" ### currently not enrolled in college - vocational training or trade apprenticeship.
    ### elif (fdf['Mcafss Edu1'] == 11):
    ###     return "Other" ### other education.
    ### elif (fdf['Mcafss Edu1'] == 12):
    ###     return "Unknown/Did Not Report" ### unknown/did not report.
    elif (fdf['Mcafss Edu1'] == 0):
        return "Unknown/Did Not Report" ### unknown/did not report (missing data).
    elif (fdf['Mcafss Edu1'] >= 9):
        return "Unknown/Did Not Report"
    ###########
    ### FW.
    elif (fdf['AD1MinEdu'] == "8th Grade or less"):
        return "Less than HS diploma"
    elif (fdf['AD1MinEdu'] == "Some High School"):
        return "Less than HS diploma"
    elif (fdf['AD1MinEdu'] == "GED"):
        return "HS diploma/GED"
    elif (fdf['AD1MinEdu'] == "High School Graduate"):
        return "HS diploma/GED"
    elif (fdf['AD1MinEdu'] == "Achievement Certificate"):
        return "Technical Training or Certification"
    elif (fdf['AD1MinEdu'] == "Certificate Program"):
        return "Technical Training or Certification"
    elif (fdf['AD1MinEdu'] == "Some College"):
        return "Some college/training"
    elif (fdf['AD1MinEdu'] == "Associates or Two Year Technical Degree"):
        return "Technical Training or Associates Degree" ### these are two serparate categories on F1.
    elif (fdf['AD1MinEdu'] == "Two Year Degree"):
        return "Associate's Degree"
    elif (fdf['AD1MinEdu'] == "Four Year College Degree"):
        return "Bachelor's Degree or Higher"
    elif (fdf['AD1MinEdu'] == "Graduate School"):
        return "Bachelor's Degree or Higher"
    elif (fdf['AD1MinEdu'] == "Unknown"):
        return "Unknown/Did Not Report"
    elif (fdf['AD1MinEdu'] == "null"):
        return "Unknown/Did Not Report"
    ###########
    elif (pd.isna(fdf['Mcafss Edu1']) and pd.isna(fdf['AD1MinEdu'])):
        return "Unknown/Did Not Report"
    else:
        return "Unrecognized Value"
    # IF [Mcafss Edu1] = 1 THEN "Less than HS diploma" //LLCHD // Less than 8th Grade
    # ELSEIF [Mcafss Edu1] = 2 THEN "Less than HS diploma" // 8-11th Grade
    # ELSEIF [Mcafss Edu1] = 3 THEN "HS diploma/GED" // High School Grad
    # ELSEIF [Mcafss Edu1] = 4 THEN "HS diploma/GED" //Completed a GED
    # ELSEIF [Mcafss Edu1] = 5 THEN "Technical Training or Associates Degree" // Vocational School after High School
    # ELSEIF [Mcafss Edu1] = 6 THEN "Some college/training" // Some College
    # ELSEIF [Mcafss Edu1] = 7 THEN "Technical Training or Associates Degree" // Associates Degree
    # ELSEIF [Mcafss Edu1] = 8 THEN "Bachelor's Degree or Higher"  // Bachelors Degree or Higher
    # // ELSEIF [Mcafss Edu1] = 9 THEN "HS diploma/GED"
    # // ELSEIF [Mcafss Edu1] = 10 THEN "HS diploma/GED" 
    # // ELSEIF [Mcafss Edu1] = 11 THEN "Other"
    # // ELSEIF [Mcafss Edu1] = 12 THEN "Unknown/Did Not Report"
    # ELSEIF [Mcafss Edu1] = 0 THEN "Unknown/Did Not Report"
    # ELSEIF [Mcafss Edu1] >= 9 THEN "Unknown/Did Not Report"
    # ELSEIF [AD1MinEdu] = "8th Grade or less" THEN "Less than HS diploma"
    # ELSEIF [AD1MinEdu] = "Some High School" THEN "Less than HS diploma"
    # ELSEIF [AD1MinEdu] = "GED" THEN "HS diploma/GED"
    # ELSEIF [AD1MinEdu] = "High School Graduate" THEN "HS diploma/GED"
    # ELSEIF [AD1MinEdu] = "Achievement Certificate" THEN "Technical Training or Certification"
    # ELSEIF [AD1MinEdu] = "Certificate Program" THEN "Technical Training or Certification"
    # ELSEIF [AD1MinEdu] = "Some College" THEN "Some college/training"
    # ELSEIF [AD1MinEdu] = "Associates or Two Year Technical Degree" THEN "Technical Training or Associates Degree" //these are two serparate categories on F1
    # ELSEIF [AD1MinEdu] = "Two Year Degree" THEN "Associate's Degree"
    # ELSEIF [AD1MinEdu] = "Four Year College Degree" THEN "Bachelor's Degree or Higher"
    # ELSEIF [AD1MinEdu] = "Graduate School" THEN "Bachelor's Degree or Higher"
    # ELSEIF [AD1MinEdu] = "Unknown" THEN "Unknown/Did Not Report"
    # ELSEIF [AD1MinEdu] = "null"  THEN  "Unknown/Did Not Report"
    # ELSEIF ISNULL([Mcafss Edu1])AND ISNULL([AD1MinEdu]) THEN "Unknown/Did Not Report"
    # ELSE "Unrecognized Value"
    # END
    # //LLCHD Code from Kodi on 11/30/2021
    # //1 – Less than 8th Grade
    # //2 – 8-11th Grade
    # //3 – High School Grad
    # //4 - Completed a GED
    # //5 - Vocational School after High School
    # //6 – Some College
    # //7 – Associates Degree 
    # //8 - Bachelors Degree or Higher
    # //Confirmed 9-12 are old and no longer needed - new LLCHD variables are sent to confirm enrollment
df3_edits1['_C15 Min Educational Status'] = df3_edits1.apply(func=fn_C15_Min_Educational_Status, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C15 Min Educational Status']) 

#%%###################################

def fn_C15_Max_Educational_Status(fdf):
    ###########
    ### LLCHD.
    if (fdf['Mcafss Edu2'] == 1):
        return "Less than HS diploma" ### Less than 8th Grade.
    elif (fdf['Mcafss Edu2'] == 2):
        return "Less than HS diploma" ### 8-11th Grade.
    elif (fdf['Mcafss Edu2'] == 3):
        return "HS diploma/GED" ### High School Grad.
    elif (fdf['Mcafss Edu2'] == 4):
        return "HS diploma/GED" ### Completed a GED.
    elif (fdf['Mcafss Edu2'] == 5):
        return "Technical Training or Associates Degree" ### Vocational School after High School.
    elif (fdf['Mcafss Edu2'] == 6):
        return "Some college/training" ### Some College.
    elif (fdf['Mcafss Edu2'] == 7):
        return "Technical Training or Associates Degree" ### Associates Degree.
    elif (fdf['Mcafss Edu2'] == 8):
        return "Bachelor's Degree or Higher" ### Bachelors Degree or Higher.
    ### elif (fdf['Mcafss Edu2'] == 9):
    ###     return "HS diploma/GED" ### currently enrolled in college - vocational training or trade apprenticeship.
    ### elif (fdf['Mcafss Edu2'] == 10):
    ###     return "HS diploma/GED" ### currently not enrolled in college - vocational training or trade apprenticeship.
    ### elif (fdf['Mcafss Edu2'] == 11):
    ###     return "Other" ### other education.
    ### elif (fdf['Mcafss Edu2'] == 12):
    ###     return "Unknown/Did Not Report" ### unknown/did not report.
    elif (fdf['Mcafss Edu2'] == 0):
        return "Unknown/Did Not Report" ### unknown/did not report (missing data).
    elif (fdf['Mcafss Edu2'] >= 9):
        return "Unknown/Did Not Report"
    ###########
    ### FW.
    elif (fdf['AD1MaxEdu'] == "8th Grade or less"):
        return "Less than HS diploma"
    elif (fdf['AD1MaxEdu'] == "Some High School"):
        return "Less than HS diploma"
    elif (fdf['AD1MaxEdu'] == "GED"):
        return "HS diploma/GED"
    elif (fdf['AD1MaxEdu'] == "High School Graduate"):
        return "HS diploma/GED"
    elif (fdf['AD1MaxEdu'] == "Achievement Certificate"):
        return "Technical Training or Certification"
    elif (fdf['AD1MaxEdu'] == "Certificate Program"):
        return "Technical Training or Certification"
    elif (fdf['AD1MaxEdu'] == "Some College"):
        return "Some college/training"
    elif (fdf['AD1MaxEdu'] == "Associates or Two Year Technical Degree"):
        return "Technical Training or Associates Degree" ### these are two serparate categories on F1.
    elif (fdf['AD1MaxEdu'] == "Two Year Degree"):
        return "Associate's Degree"
    elif (fdf['AD1MaxEdu'] == "Four Year College Degree"):
        return "Bachelor's Degree or Higher"
    elif (fdf['AD1MaxEdu'] == "Graduate School"):
        return "Bachelor's Degree or Higher"
    elif (fdf['AD1MaxEdu'] == "Unknown"):
        return "Unknown/Did Not Report"
    elif (fdf['AD1MaxEdu'] == "null"):
        return "Unknown/Did Not Report"
    ###########
    elif (pd.isna(fdf['Mcafss Edu2']) and pd.isna(fdf['AD1MaxEdu'])):
        return "Unknown/Did Not Report"
    else:
        return "Unrecognized Value"
    # //LLCHD
    # IF [Mcafss Edu2] = 1 THEN "Less than HS diploma" // Less than 8th Grade
    # ELSEIF [Mcafss Edu2] = 2 THEN "Less than HS diploma" // 8-11th Grade
    # ELSEIF [Mcafss Edu2] = 3 THEN "HS diploma/GED" // High School Grad
    # ELSEIF [Mcafss Edu2] = 4 THEN "HS diploma/GED" //Completed a GED
    # ELSEIF [Mcafss Edu2] = 5 THEN "Technical Training or Associates Degree" // Vocational School after High School
    # ELSEIF [Mcafss Edu2] = 6 THEN "Some college/training" // Some College
    # ELSEIF [Mcafss Edu2] = 7 THEN "Technical Training or Associates Degree" // Associates Degree
    # ELSEIF [Mcafss Edu2] = 8 THEN "Bachelor's Degree or Higher"  // Bachelors Degree or Higher
    # // ELSEIF [Mcafss Edu2] = 9 THEN "HS diploma/GED" //currently enrolled in college - vocational training or trade apprenticeship
    # // ELSEIF [Mcafss Edu2] = 10 THEN "HS diploma/GED"  //currently not enrolled in college - vocational training or trade apprenticeship
    # // ELSEIF [Mcafss Edu2] = 11 THEN "Other" //other education
    # // ELSEIF [Mcafss Edu2] = 12 THEN "Unknown/Did Not Report" //unknown/did not report
    # ELSEIF [Mcafss Edu2] = 0 THEN "Unknown/Did Not Report" //unknown/did not report (missing data)
    # ELSEIF [Mcafss Edu2] >= 9 THEN "Unknown/Did Not Report"
    # //FW
    # ELSEIF [AD1MaxEdu] = "8th Grade or less" THEN "Less than HS diploma"
    # ELSEIF [AD1MaxEdu] = "Some High School" THEN "Less than HS diploma"
    # ELSEIF [AD1MaxEdu] = "GED" THEN "HS diploma/GED"
    # ELSEIF [AD1MaxEdu] = "High School Graduate" THEN "HS diploma/GED"
    # ELSEIF [AD1MaxEdu] = "Achievement Certificate" THEN "Technical Training or Certification"
    # ELSEIF [AD1MaxEdu] = "Certificate Program" THEN "Technical Training or Certification"
    # ELSEIF [AD1MaxEdu] = "Some College" THEN "Some college/training"
    # ELSEIF [AD1MaxEdu] = "Associates or Two Year Technical Degree" THEN "Technical Training or Associates Degree" //these are two serparate categories on F1
    # ELSEIF [AD1MaxEdu] = "Two Year Degree" THEN "Associate's Degree"
    # ELSEIF [AD1MaxEdu] = "Four Year College Degree" THEN "Bachelor's Degree or Higher"
    # ELSEIF [AD1MaxEdu] = "Graduate School" THEN "Bachelor's Degree or Higher"
    # ELSEIF [AD1MaxEdu] = "Unknown" THEN "Unknown/Did Not Report"
    # ELSEIF [AD1MaxEdu] = "null" THEN "Unknown/Did Not Report"
    # ELSEIF ISNULL([Mcafss Edu2]) AND ISNULL([AD1MaxEdu]) THEN "Unknown/Did Not Report"
    # ELSE "Unrecognized Value"
    # END
    # //LLCHD Code from Kodi on 11/30/2021
    # //1 – Less than 8th Grade
    # //2 – 8-11th Grade
    # //3 – High School Grad
    # //4 - Completed a GED
    # //5 - Vocational School after High School
    # //6 – Some College
    # //7 – Associates Degree 
    # //8 - Bachelors Degree or Higher
    # //Confirmed 9-12 are old and no longer needed - new LLCHD variables are sent to confirm enrollment
df3_edits1['_C15 Max Educational Status'] = df3_edits1.apply(func=fn_C15_Max_Educational_Status, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C15 Max Educational Status']) 

#%%###################################

def fn_T09_FOB_Education_Status(fdf):
    ###########
    ### FW.
    if (fdf['Fob Involved'] == True):
        if pd.isna(fdf['AD2EDLevel']):
            return "Unknown/Did Not Report"
        else:
            match fdf['AD2EDLevel'].lower():
                case "8th grade or less" | "some high school":
                    return "Less than HS diploma"
                case "ged" | "high school graduate":
                    return "HS diploma/GED"
                case "achievement certificate":
                    return "Some college/training" ### is this the right category?.
                case "some college":
                    return "Some college/training"
                case "associates or two year technical degree":
                    return "Technical Training or Associates Degree" ### these are two serparate categories on F1.
                case "four year college degree" | "graduate school":
                    return "Bachelor's Degree or Higher"
                case "unknown":
                    return "Unknown/Did Not Report"
                ### case np.nan:
                ###     return "Unknown/Did Not Report"
    ###########
    ### LLCHD.
    elif (fdf['Fob Involved1'] == "Y"):
        if pd.isna(fdf['Fob Edu']):
            return "Unknown/Did Not Report"
        else:
            match fdf['Fob Edu']:
                case 1 | 2:
                    return "Less than HS diploma"
                case 3 | 4:
                    return "HS diploma/GED"
                case 5:
                    return "Vocational School after High School"
                case 6:
                    return "Some college/training"
                case 7:
                    return "Associates Degree" ### these are two serparate categories on F1.
                case 8:
                    return "Bachelor's Degree or Higher"
                case 0:
                    return "Unknown/Did Not Report"
                ### case np.nan:
                ###     return "Unknown/Did Not Report"
    ###########
    else:
        return np.nan 
    # IF [Fob Involved]= True THEN CASE[AD2EDLevel] //FW
    #     WHEN "8th Grade or less" THEN "Less than HS diploma"
    #     WHEN "Some High School" THEN "Less than HS diploma"
    #     WHEN "GED" THEN "HS diploma/GED"
    #     WHEN "High School Graduate" THEN "HS diploma/GED"
    #     WHEN "Achievement Certificate" THEN "Some college/training" //is this the right category?
    #     WHEN "Some College" THEN "Some college/training"
    #     WHEN "Associates or Two Year Technical Degree" THEN "Technical Training or Associates Degree" //these are two serparate categories on F1
    #     WHEN "Four Year College Degree" THEN "Bachelor's Degree or Higher"
    #     WHEN "Graduate School" THEN "Bachelor's Degree or Higher"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     END 
    # ELSEIF [Fob Involved1]= "Y" THEN CASE [Fob Edu] //LLCHD
    #     WHEN 01 THEN "Less than HS diploma"
    #     WHEN 02 THEN "Less than HS diploma"
    #     WHEN 03 THEN "HS diploma/GED"
    #     WHEN 04 THEN "HS diploma/GED"
    #     WHEN 05 THEN "Vocational School after High School"
    #     WHEN 06 THEN "Some college/training"
    #     WHEN 07 THEN "Associates Degree" //these are two serparate categories on F1
    #     WHEN 08 THEN "Bachelor's Degree or Higher"
    #     WHEN 00 THEN "Unknown/Did Not Report"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     END
    # ELSE NULL
    # END
df3_edits1['_T09 FOB Education Status'] = df3_edits1.apply(func=fn_T09_FOB_Education_Status, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_T09 FOB Education Status']) 


#%%###################################

def fn_C16_CG_Insurance_Status(fdf_column):
    match fdf_column:
        ###########
        ### FW.
        case "Medicaid":
            return "Medicaid or CHIP"
        case "SCHIP":
            return "Medicaid or CHIP"
        case "Medicare":
            return "Private or Other"
        case "Tri-Care":
            return "Tri-Care"
        case "None":
            return "No Insurance Coverage"
        case "Other":
            return "Private or Other"
        case "Private":
            return "Private or Other"
        case "Unknown":
            return "Unknown/Did Not Report"
        case "null":
            return "Unknown/Did Not Report"
        case np.nan:
            return "Unknown/Did Not Report"
        ###########
        ### LLCHD.
        case "1":
            return "Medicaid or CHIP"
        case "2":
            return "Tri-Care"
        case "3":
            return "Private or Other"
        case "4":
            return "Unknown/Did Not Report"
        case "5":
            return "No Insurance Coverage"
        case "6":
            return "Unknown/Did Not Report"
        case "99":
            return "Unknown/Did Not Report"
        case "Medicaid":
            return "Medicaid or CHIP"
        case "Private":
            return "Private or Other"
        case "Unknown":
            return "Unknown/Did Not Report"
        case "Uninsure":
            return "No Insurance Coverage"
        case "FamilyCh":
            return "FamilyChildHealthPlus"
        case np.nan:
            return "Unknown/Did Not Report"
        ###########
        case _:
            return "Unrecognized Value"
    # CASE [AD1PrimaryIns.1] //FW
    #     WHEN "Medicaid" THEN "Medicaid or CHIP"
    #     WHEN "SCHIP" THEN "Medicaid or CHIP"
    #     WHEN "Medicare" THEN "Private or Other"
    #     WHEN "Tri-Care" THEN "Tri-Care"
    #     WHEN "None" THEN "No Insurance Coverage"
    #     WHEN "Other" THEN "Private or Other"
    #     WHEN "Private" THEN "Private or Other"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     WHEN "null" THEN "Unknown/Did Not Report"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    # //LLCHD
    #     WHEN "1" THEN "Medicaid or CHIP"
    #     WHEN "2" THEN "Tri-Care"
    #     WHEN "3" THEN "Private or Other"
    #     WHEN "4" THEN "Unknown/Did Not Report"
    #     WHEN "5" THEN "No Insurance Coverage"
    #     WHEN "6" THEN "Unknown/Did Not Report"
    #     WHEN "99" THEN "Unknown/Did Not Report"
    #     WHEN "Medicaid" THEN "Medicaid or CHIP"
    #     WHEN "Private" THEN "Private or Other"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     WHEN "Uninsure" THEN "No Insurance Coverage"
    #     WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    # ELSE "Unrecognized Value"
    # END

#%%###################################

df3_edits1['_C16 CG Insurance 1 Status'] = df3_edits1['AD1PrimaryIns.1'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 1 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 2 Status'] = df3_edits1['AD1PrimaryIns.2'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 2 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 3 Status'] = df3_edits1['AD1PrimaryIns.3'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 3 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 4 Status'] = df3_edits1['AD1PrimaryIns.4'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 4 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 5 Status'] = df3_edits1['AD1PrimaryIns.5'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 5 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 6 Status'] = df3_edits1['AD1PrimaryIns.6'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 6 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 7 Status'] = df3_edits1['AD1PrimaryIns.7'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 7 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 8 Status'] = df3_edits1['AD1PrimaryIns.8'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 8 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 9 Status'] = df3_edits1['AD1PrimaryIns.9'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 9 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 10 Status'] = df3_edits1['AD1PrimaryIns.10'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 10 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 11 Status'] = df3_edits1['AD1PrimaryIns.11'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 11 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 12 Status'] = df3_edits1['AD1PrimaryIns.12'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 12 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 13 Status'] = df3_edits1['AD1PrimaryIns.13'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 13 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 14 Status'] = df3_edits1['AD1PrimaryIns.14'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 14 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 15 Status'] = df3_edits1['AD1PrimaryIns.15'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 15 Status']) 

#%%###################################

df3_edits1['_C16 CG Insurance 16 Status'] = df3_edits1['AD1PrimaryIns.16'].apply(func=fn_C16_CG_Insurance_Status) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_C16 CG Insurance 16 Status']) 

#%%###################################

df3_edits1['_T20 FOB Insurance'] = 






df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 


#%%###################################

def fn_Enroll_Preg_Status(fdf):
    ### FW.
    if (fdf['Pregnancystatus'] == 0):
        return "Pregnant" 
    elif (fdf['Pregnancystatus'] == 1):
        return "Not pregnant"
    ### LLCHD.
    elif (fdf['Enroll Preg Status'] == "Postpartum"):
        return "Not pregnant" 
    elif (fdf['Enroll Preg Status'] == "Pregnant"):
        return "Pregnant"
    ###
    else:
        return np.nan 
    # IF [Pregnancystatus] = 0 THEN "Pregnant" //FW
    # ELSEIF [Pregnancystatus] = 1 THEN "Not pregnant"
    # ELSEIF [Enroll Preg Status] = "Postpartum" THEN "Not pregnant" //LLCHD
    # ELSEIF [Enroll Preg Status] = "Pregnant" THEN "Pregnant"
    # ELSE NULL
    # END
df3_edits1['_Enroll Preg Status'] = df3_edits1.apply(func=fn_Enroll_Preg_Status, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_Enroll Preg Status']) 

#%%###################################

def fn_FOB_Involved(fdf):
    ### FW.
    if (fdf['Fob Involved'] == True):
        return 1 
    elif (fdf['Fob Involved'] == False):
        return 0 
    ### LLCHD.
    elif (fdf['Fob Involved1'] == "Y"):
        return 1 
    elif (fdf['Fob Involved1'] == "N"):
        return 0 
    else:
        return 0 
    # IF [Fob Involved] = True THEN 1 //FW
    # ELSEIF [Fob Involved] = False THEN 0
    # ELSEIF [Fob Involved1] = "Y" THEN 1 //LLCHD
    # ELSEIF [Fob Involved1] = "N" THEN 0
    # ELSE 0
    # END
df3_edits1['_FOB Involved'] = df3_edits1.apply(func=fn_FOB_Involved, axis=1) 
    ### Data Type in Tableau: 'int'.
inspect_col(df3_edits1['_FOB Involved']) 

#%%###################################

def fn_MOB_TGT_Relation(fdf):
    ### FW.
    if (fdf['Adult1TGTRelation'] == "Biological mother"):
        return "MOB" 
    elif (fdf['Adult1TGTRelation'] == "Biological father"):
        return "FOB"
    elif (fdf['Adult1TGTRelation'] == "FOB"):
        return "FOB"
    elif (fdf['Adult1TGTRelation'] == "MOB"):
        return "MOB"
    elif (fdf['Adult1TGTRelation'] == "Adoptive father"):
        return "FOB"
    elif (fdf['Adult1TGTRelation'] == "Adoptive mother"):
        return "MOB"
    elif (fdf['Adult1TGTRelation'] == "Foster mother"):
        return "MOB"
    elif (fdf['Adult1TGTRelation'] == "Grandmother"):
        return "MOB"
    elif (fdf['Adult1TGTRelation'] == "Guardian"):
        return "MOB"
    elif (pd.notna(fdf['Adult1TGTRelation'])):
        return "Unrecognized Value"
    ### LLCHD.
    elif (fdf['Primary Relation'] == "FATHER OF CHILD"):
        return "FOB" 
    elif (fdf['Primary Relation'] == "MOTHER OF CHILD"):
        return "MOB"
    elif (fdf['Primary Relation'] == "PRIMARY CAREGIVER" and fdf['Mob Gender'] == "F"):
        return "MOB"
    elif (fdf['Primary Relation'] == "PRIMARY CAREGIVER" and fdf['Mob Gender'] == "M"):
        return "FOB"
    elif (pd.notna(fdf['Primary Relation'])):
        return "Unrecognized Value"
    # IF [Adult1TGTRelation] = "Biological mother" THEN "MOB" //FW
    # ELSEIF  [Adult1TGTRelation] = "Biological father" THEN "FOB"
    # ELSEIF  [Adult1TGTRelation] = "FOB" THEN "FOB"
    # ELSEIF  [Adult1TGTRelation] = "MOB" THEN "MOB"
    # ELSEIF  [Adult1TGTRelation] = "Adoptive father" THEN "FOB"
    # ELSEIF  [Adult1TGTRelation] = "Adoptive mother" THEN "MOB"
    # ELSEIF  [Adult1TGTRelation] = "Foster mother" THEN "MOB"
    # ELSEIF  [Adult1TGTRelation] = "Grandmother" THEN "MOB"
    # ELSEIF  [Adult1TGTRelation] = "Guardian" THEN "MOB"
    # ELSEIF NOT ISNULL([Adult1TGTRelation]) THEN "Unrecognized Value"
    # ELSEIF [Primary Relation] = "FATHER OF CHILD" THEN "FOB" //LLCHD
    # ELSEIF [Primary Relation] = "MOTHER OF CHILD" THEN "MOB"
    # ELSEIF [Primary Relation] = "PRIMARY CAREGIVER" AND [Mob Gender] = "F" THEN "MOB"
    # ELSEIF [Primary Relation] = "PRIMARY CAREGIVER" AND [Mob Gender] = "M" THEN "FOB"
    # ELSEIF NOT ISNULL([Primary Relation]) THEN "Unrecognized Value"
    # END
df3_edits1['_MOB TGT Relation'] = df3_edits1.apply(func=fn_MOB_TGT_Relation, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_MOB TGT Relation']) 

#%%###################################

def fn_FOB_Relation(fdf):
    if (fdf['Fob Involved1'] == "Y"):
        return "FOB"
    elif (fdf['Fob Involved'] == True):
        match fdf['Adult2TGTRelation']:
            case "Biological father":
                return "FOB"
            case "Biological mother":
                return "MOB"
            case "FOB":
                return "FOB"
            case "Foster father":
                return "FOB"
            case "Guardian":
                return "Guardian"
            case "Grandmother":
                return "Grandmother"
            case "MOB":
                return "MOB"
            case "Other":
                return "Other"
    else:
        return pd.nan
    # IF [Fob Involved1] = "Y" THEN "FOB"
    # ELSEIF [Fob Involved] = True
    # THEN CASE [Adult2TGTRelation]
    #     WHEN "Biological father" THEN "FOB"
    #     WHEN "Biological mother" THEN "MOB"
    #     WHEN "FOB" THEN "FOB"
    #     WHEN "Foster father" THEN "FOB"
    #     WHEN "Guardian" THEN "Guardian"
    #     WHEN "Grandmother" THEN "Grandmother"
    #     WHEN "MOB" THEN "MOB"
    #     WHEN "Other" THEN "Other"
    #     END
    # ELSE NULL
    # END
df3_edits1['_FOB Relation'] = df3_edits1.apply(func=fn_FOB_Relation, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_FOB Relation']) 

#%%###################################

df3_edits1['_T12 MOB Housing Status'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_T14 Federal Poverty Categories'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_T14 Poverty Percent'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_T14 Federal Poverty Level update'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_T17 Discharge Reason'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

def fn_Need_Exclusion_1_Sub_Abuse(fdf):
    ### FW.
    if (fdf['Need Exclusion1'] == "Substance Abuse"):
        return "Alcohol/Drug Abuse" 
    elif (fdf['Need Exclusion1'] == "Drug Abuse"):
        return "Alcohol/Drug Abuse"
    elif (fdf['Need Exclusion1'] == "Alcohol Abuse"):
        return "Alcohol/Drug Abuse"
    ### LLCHD.
    elif (fdf['need exclusion1 (LLCHD)'] == "Y"):
        return "Alcohol/Drug Abuse" 
    # IF [Need Exclusion1] = "Substance Abuse" THEN "Alcohol/Drug Abuse" //FW
    #     ELSEIF [Need Exclusion1] = "Drug Abuse" THEN "Alcohol/Drug Abuse"
    #     ELSEIF [Need Exclusion1] = "Alcohol Abuse" THEN "Alcohol/Drug Abuse"
    # ELSEIF [need exclusion1 (LLCHD)] = "Y" THEN "Alcohol/Drug Abuse" //LLCHD
    # END
df3_edits1['_Need Exclusion 1 - Sub Abuse'] = df3_edits1.apply(func=fn_Need_Exclusion_1_Sub_Abuse, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_Need Exclusion 1 - Sub Abuse']) 

#%%###################################

def fn_Need_Exclusion_2_Fam_Plan(fdf):
    ### FW.
    if (fdf['Need Exclusion2'] == "Family Planning"):
        return "Family Planning" 
    ### LLCHD.
    elif (fdf['need exclusion2 (LLCHD)'] == "Y"):
        return "Family Planning" 
    # IF [Need Exclusion2] = "Family Planning" THEN "Family Planning" //FW
    # ELSEIF [need exclusion2 (LLCHD)] = "Y" THEN "Family Planning" //LLCHD
    # END
df3_edits1['_Need Exclusion 2 - Fam Plan'] = df3_edits1.apply(func=fn_Need_Exclusion_2_Fam_Plan, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_Need Exclusion 2 - Fam Plan']) 

#%%###################################

def fn_Need_Exclusion_3_Mental_Health(fdf):
    ### FW.
    if (fdf['Need Exclusion3'] == "Mental Health"):
        return "Mental Health" 
    ### LLCHD.
    elif (fdf['need exclusion3 (LLCHD)'] == "Y"):
        return "Mental Health" 
    # IF [Need Exclusion3] = "Mental Health" THEN "Mental Health" //FW
    # ELSEIF [need exclusion3 (LLCHD)] = "Y" THEN "Mental Health" //LLCHD
    # END
df3_edits1['_Need Exclusion 3 - Mental Health'] = df3_edits1.apply(func=fn_Need_Exclusion_3_Mental_Health, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_Need Exclusion 3 - Mental Health']) 

#%%###################################

def fn_Need_Exclusion_5_IPV(fdf):
    ## FW.
    if (fdf['Need Exclusion5'] == "IPV Services"):
        return "IPV Services" 
    ## LLCHD.
    elif (fdf['need exclusion5 (LLCHD)'] == "Y"):
        return "IPV Services" 
    # IF [Need Exclusion5] = "IPV Services" THEN "IPV Services" //FW
    # ELSEIF [need exclusion5 (LLCHD)] = "Y" THEN "IPV Services" //LLCHD
    # END
df3_edits1['_Need Exclusion 5 - IPV'] = df3_edits1.apply(func=fn_Need_Exclusion_5_IPV, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_Need Exclusion 5 - IPV']) 

#%%###################################

def fn_Need_Exclusion_6_Tobacco(fdf):
    ### FW.
    if (fdf['Need Exclusion6'] == "Tobacco Cessation"):
        return "Tobacco Cessation" 
    ### LLCHD.
    elif (fdf['need_exclusion6'] == "Y"):
        return "Tobacco Cessation" 
    # IF [Need Exclusion6] = "Tobacco Cessation" THEN "Tobacco Cessation" //FW
    # ELSEIF [need_exclusion6] = "Y" THEN "Tobacco Cessation" //LLCHD
    # END
df3_edits1['_Need Exclusion 6 - Tobacco'] = df3_edits1.apply(func=fn_Need_Exclusion_6_Tobacco, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_Need Exclusion 6 - Tobacco']) 

#%%###################################

df3_edits1['_T15-5 Tobacco Use in Home'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

### Required for '_C19 IPV Screen Result' below.
def fn_IPV_Score_FW(fdf):
    if (fdf['Agency'] != "ll"):
        if (
            fdf['Assess Afraid'] == True 
            or fdf['Assess IPV'] == True 
            or fdf['Assess Police'] == True
        ):
            return "P"
        else:
            return "N" 
    # IF [Agency] <> "ll" THEN
    #     (IF [Assess Afraid] = TRUE 
    #     OR [Assess IPV] = TRUE 
    #     OR [Assess Police] = TRUE
    #     THEN "P" ELSE "N" END)
    # END
df3_edits1['_IPV Score FW'] = df3_edits1.apply(func=fn_IPV_Score_FW, axis=1) 
    ### Data Type in Tableau: 'string'.
inspect_col(df3_edits1['_IPV Score FW']) 

#%%###################################

### In Child2 & Adult3.
df3_edits1['_TGT EDC Date'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_UNCOPE U Recode'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_UNCOPE N Recode'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_UNCOPE C Recode'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_UNCOPE O Recode'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_UNCOPE P Recode'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_UNCOPE E Recode'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_UNCOPE Score FW'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_T15-3 History Welfare Interaction'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_T15-6 Low Achievement'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%###################################

df3_edits1['_T15-8 Military'] = 

df3_edits1['wwwwww'] = df3_edits1.apply(func=fn_wwwwww, axis=1) 
    ### Data Type in Tableau: 'wwww'.
inspect_col(df3_edits1['wwwwwww']) 

#%%##################################################
### COALESCING 2 

### Dependent on '_IPV Score FW'.
df3_edits1['_C19 IPV Screen Result'] = df3_edits1['_IPV Score FW'].combine_first(df3_edits1['Ipv Screen'])
    ### IFNULL([_IPV Score FW],[Ipv Screen])
    ### Data Type in Tableau: string.

df3_edits1['_UNCOPE Score'] = df3_edits1['Uncope Score'].combine_first(df3_edits1['_UNCOPE Score FW'])
    ### IFNULL([Uncope Score],[_UNCOPE Score FW]) 
    ### Data Type in Tableau: int.



#%%##################################################
### DATE CALCULATIONS

### These calculations assume all date variables are dtype "datetime64".

df3_edits1['_90 Day UNCOPE Date'] = DATE(DATEADD('day',90,[_UNCOPE Date]))

df3_edits1['_C05 TGT 30 Day Date'] = DATE(DATEADD('day',30,[_TGT DOB]))

df3_edits1['_C05 TGT 56 Day Date'] = DATE(DATEADD('day',56,[_TGT DOB]))

df3_edits1['_C17 90 Day CES-D Date'] = DATE(DATEADD('day',90, [_C03 CES-D Date]))

df3_edits1['_C19 90 Day IPV Date'] = DATE(DATEADD('day',90, [_C14 IPV Date]))

### In Child2 & Adult3.
df3_edits1['_Enroll 3 Month Date'] = DATE(DATEADD('month',3,[_Enrollment Date]))

### In Child2 & Adult3.
df3_edits1['_TGT 3 Month Date'] = DATE(DATEADD('month',3,[_TGT DOB]))




##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################





#%%##################################################
### Identify/FLAG "Unrecognized Value" ###
#####################################################

### FLAG any "Unrecognized Value" --- new value & needs to be edited earlier in the Data Source process.
### Across many variables.



#%%##################################################
### Prepare CSV ###
#####################################################

#%%################################
### REMOVE extra COLUMNS

### Remove columns created in merge.
df3_edits2 = df3_edits1.drop(columns=['LJ_df3_2ER', 'LJ_df3_3FW', 'LJ_df3_4LL', 'LJ_df3_5WC'])

#%%################################
### ORDER COLUMNS

### Final order for columns:
[*df3_comparison_csv]

#%%
### Reorder Columns.
df3_edits2 = df3_edits2[[*df3_comparison_csv]]

#%%################################
### SORT ROWS

df3_edits2 = df3_edits2.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)

###################################
### SET DATA TYPES
### Set the data type of each column like it should be in output.

#%%
### Identify columns that should be Integers:
int_cols_df3 = df3_edits2.select_dtypes(include=['float']).fillna(-9999).applymap(float.is_integer).all().loc[lambda x: x==True].index.to_series()
print(int_cols_df3.to_string())
#%%
print(df3_edits2.dtypes.to_string())

#%%
### Turn all columns that should be into Integers:
df3_edits2[int_cols_df3] = df3_edits2[int_cols_df3].astype('Int64')
#%%
print(df3_edits2.dtypes.to_string())

#%%##################################################
### WRITE ###
#####################################################

#%%
### Created Final DF.
df3__final = df3_edits2.copy()

#%%
### Write out df.
df3__final.to_csv(path_3_output, index=False, date_format="%#m/%#d/%Y")

#%%
### Read back in df for comparison.
df3__final_from_csv = pd.read_csv(path_3_output, dtype=object, keep_default_na=False, na_values=[''])

#%%##################################################
### COMPARE CSVs ###
#####################################################

#%%###################################

#%%
### Column names:
[*df3__final_from_csv]
#%%
### Column names:
[*df3_comparison_csv]

#%%
### Overlap / Similarities: Columns in both.
set([*df3_comparison_csv]).intersection([*df3__final_from_csv])

#%%###################################
### COLUMNS:

#%%
### Check if all Column names identical & in same order.
[*df3__final_from_csv] == [*df3_comparison_csv]

#%%
### Differences: Columns only in one.
set([*df3_comparison_csv]).symmetric_difference([*df3__final_from_csv])

#%%###################################

####### Compare values
### including row count, distinct ids, 

#%%
# Check rows & cols:
print(f'df3__final_from_csv Rows: {len(df3__final_from_csv)}')
print(f'df3_comparison_csv Rows: {len(df3_comparison_csv)}')

print(f'df3__final_from_csv Columns: {len(df3__final_from_csv.columns)}')
print(f'df3_comparison_csv Columns: {len(df3_comparison_csv.columns)}')

#%%
df3__final_from_csv == df3_comparison_csv

#%%
### Checking ID columns used in Join >> DF should be empty (meaning all the same).
df3_comp_compare = df3_comparison_csv[['Project Id','Year','Quarter']].compare(df3__final_from_csv[['Project Id','Year','Quarter']])
df3_comp_compare

###################################
###################################
###################################

#%%
### Now comparing ALL columns. DF created shows all differences:
df3_comp_compare = df3_comparison_csv.compare(df3__final_from_csv)
df3_comp_compare

#%%
### Number of columns with different values/types:
len([*df3_comp_compare]) / 2 


#%%
### Columns:
[*df3_comp_compare]




