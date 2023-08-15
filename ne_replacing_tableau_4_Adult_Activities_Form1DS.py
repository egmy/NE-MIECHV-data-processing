
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.

#%%##################################################
### SETUP ###
#####################################################

### import RUNME ### This does not run the code.

# exec(open('RUNME.py').read())

#%%##################################################
### PACKAGES ###
#####################################################

### Only importing here so that VSC doesn't show lots of warnings for things not defined. Can comment out in production.

# import pandas as pd
# import numpy as np
# import sys
# import collections
# import re

# print('Version Of Python: ' + sys.version)
# print('Version Of Pandas: ' + pd.__version__)
# print('Version Of Numpy: ' + np.version.version)

# from RUNME import inspect_df
# from RUNME import inspect_col
# from RUNME import compare_col
# from RUNME import fn_all_value_counts

#%%##################################################
### Comparison File ###
#####################################################

# df4_comparison_csv = pd.read_csv(path_4_comparison_csv, dtype='string', keep_default_na=False, na_values=[''])
df4_comparison_csv = pd.read_csv(path_4_comparison_csv, dtype='object', keep_default_na=False, na_values=[''])
df4_comparison_csv = df4_comparison_csv.sort_values(by=['Project Id','Year','Quarter','__F1 Caregiver ID for MOB or FOB'], ignore_index=True)

#%%##################################################
### COLUMN DEFINITIONS ###
#####################################################

#%%### df4_1: 'Project ID'.
#%%### df4_2: 'Caregiver Insurance'.
#%%### df4_3: 'Family Wise'.
#%%### df4_4: 'LLCHD'.
#%%### df4_5: 'MOB or FOB'.

#######################
#%%### df4_1: 'Project ID'.
df4_1_col_detail = [
    ['join_id', 'Join Id', '', 'Int64'],
    ['project_id', 'Project Id', '', 'string'], 
    ['year', 'Year', '', 'Int64'], 
    ['quarter', 'Quarter', '', 'Int64']
]
#%%### df4_1: 'Project ID'.
### For Renaming, we only need a dictionary of the columns with names changing.
### If x[2] == 'same' or x[0] == x[1] then that column is not included in df_colnames.
df4_1_colnames = {x[0]:x[1] for x in df4_1_col_detail if x[2] != 'same' and x[0] != x[1]}
df4_1_colnames
#%%### df4_1: 'Project ID'.
df4_1_col_dtypes = {x[0]:x[3] for x in df4_1_col_detail}
print(df4_1_col_dtypes)
print(collections.Counter(list(df4_1_col_dtypes.values())))

#######################
#%%### df4_2: 'Caregiver Insurance'.
df4_2_col_detail = [
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
#%%### df4_2: 'Caregiver Insurance'.
df4_2_colnames = {x[0]:x[1] for x in df4_2_col_detail if x[2] != 'same' and x[0] != x[1]}
df4_2_colnames
#%%### df4_2: 'Caregiver Insurance'.
df4_2_col_dtypes = {x[0]:x[3] for x in df4_2_col_detail}
print(df4_2_col_dtypes)
print(collections.Counter(list(df4_2_col_dtypes.values())))

#######################
#%%### df4_3: 'Family Wise'.
df4_3_col_detail = [
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
    ['ANNUAL INCOME', 'Annual Income', '', 'Int64'], ### In Tableau set to decimal (& then drops all .0 in output), when really all are integers. Changed from Float to Int.
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
    ['need_exclusion6', 'need_exclusion6 (Family Wise)', '', 'string'] ### Different from Adult3 Form2: 'Need Exclusion6'.
]
#%%### df4_3: 'Family Wise'.
df4_3_colnames = {x[0]:x[1] for x in df4_3_col_detail if x[2] != 'same' and x[0] != x[1]}
df4_3_colnames
#%%### df4_3: 'Family Wise'.
df4_3_col_dtypes = {x[0]:x[3] for x in df4_3_col_detail}
print(df4_3_col_dtypes)
print(collections.Counter(list(df4_3_col_dtypes.values())))

#######################
#%%### df4_4: 'LLCHD'.
df4_4_col_detail = [
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
    ['mob_id', 'Mob Id', '', 'string'], ### In Tableau Y12Q2, is integer, but should be string.
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
    ['fob_edu', 'Fob Edu', '', 'Int64'], ### In Tableau Y12Q2 is integer, but should be string.
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
    ['asq3_referral_9mm', 'Asq3 Referral 9Mm', '', 'datetime64[ns]'], ### 'string' in Tableau in Q1 but should be date.
    ['asq3_dt_18mm', 'Asq3 Dt 18Mm', '', 'datetime64[ns]'],
    ['asq3_timing_18mm', 'Asq3 Timing 18Mm', '', 'Int64'],
    ['asq3_comm_18mm', 'Asq3 Comm 18Mm', '', 'Int64'],
    ['asq3_gross_18mm', 'Asq3 Gross 18Mm', '', 'Int64'],
    ['asq3_fine_18mm', 'Asq3 Fine 18Mm', '', 'Int64'],
    ['asq3_problem_18mm', 'Asq3 Problem 18Mm', '', 'Int64'],
    ['asq3_social_18mm', 'Asq3 Social 18Mm', '', 'Int64'],
    ['asq3_feedback_18mm', 'Asq3 Feedback 18Mm', '', 'string'],
    ['asq3_referral_18mm', 'Asq3 Referral 18Mm', '', 'datetime64[ns]'], ### 'int' in Tableau in Q2 but should be date.
    ['asq3_dt_24mm', 'Asq3 Dt 24Mm', '', 'datetime64[ns]'],
    ['asq3_timing_24mm', 'Asq3 Timing 24Mm', '', 'Int64'],
    ['asq3_comm_24mm', 'Asq3 Comm 24Mm', '', 'Int64'],
    ['asq3_gross_24mm', 'Asq3 Gross 24Mm', '', 'Int64'],
    ['asq3_fine_24mm', 'Asq3 Fine 24Mm', '', 'Int64'],
    ['asq3_problem_24mm', 'Asq3 Problem 24Mm', '', 'Int64'],
    ['asq3_social_24mm', 'Asq3 Social 24Mm', '', 'Int64'],
    ['asq3_feedback_24mm', 'Asq3 Feedback 24Mm', '', 'string'],
    ['asq3_referral_24mm', 'Asq3 Referral 24Mm', '', 'datetime64[ns]'], ### 'int' in Tableau in Q2 but should be date.
    ['asq3_dt_30mm', 'Asq3 Dt 30Mm', '', 'datetime64[ns]'],
    ['asq3_timing_30mm', 'Asq3 Timing 30Mm', '', 'Int64'],
    ['asq3_comm_30mm', 'Asq3 Comm 30Mm', '', 'Int64'],
    ['asq3_gross_30mm', 'Asq3 Gross 30Mm', '', 'Int64'],
    ['asq3_fine_30mm', 'Asq3 Fine 30Mm', '', 'Int64'],
    ['asq3_problem_30mm', 'Asq3 Problem 30Mm', '', 'Int64'],
    ['asq3_social_30mm', 'Asq3 Social 30Mm', '', 'Int64'],
    ['asq3_feedback_30mm', 'Asq3 Feedback 30Mm', '', 'string'],
    ['asq3_referral_30mm', 'Asq3 Referral 30Mm', '', 'datetime64[ns]'], ### 'int' in Tableau in Q2 but should be date.
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
#%%### df4_4: 'LLCHD'.
df4_4_colnames = {x[0]:x[1] for x in df4_4_col_detail if x[2] != 'same' and x[0] != x[1]}
df4_4_colnames
#%%### df4_4: 'LLCHD'.
df4_4_col_dtypes = {x[0]:x[3] for x in df4_4_col_detail}
print(df4_4_col_dtypes)
print(collections.Counter(list(df4_4_col_dtypes.values())))

#######################
#%%### df4_5: 'MOB or FOB'.
df4_5_col_detail = [
    ['join_id', 'join id (MOB or FOB)', '', 'Int64'],
    ['MOB_or_FOB', 'MOB or FOB', '', 'string'] 
]
#%%### df4_5: 'MOB or FOB'.
df4_5_colnames = {x[0]:x[1] for x in df4_5_col_detail if x[2] != 'same' and x[0] != x[1]}
df4_5_colnames
#%%### df4_5: 'MOB or FOB'.
df4_5_col_dtypes = {x[0]:x[3] for x in df4_5_col_detail}
print(df4_5_col_dtypes)
print(collections.Counter(list(df4_5_col_dtypes.values())))


#%%##################################################
### READ ###
#####################################################

#%%
### Performance benefit for reading in file to memory only once by creating an ExcelFile class object.
xlsx_df4 = pd.ExcelFile(path_4_data_source_file)

#%% 
### CHECK that all path_4_data_source_sheets same as xlsx.sheet_names (different order ok):
print(sorted(path_4_data_source_sheets))
print(sorted(xlsx.sheet_names))
sorted(path_4_data_source_sheets) == sorted(xlsx.sheet_names)

#%%###################################
### READ all sheets:

# df4_1 = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[0], keep_default_na=False, na_values=[''], dtype=df4_1_col_dtypes)

# #%%
# df4_2 = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[1], keep_default_na=False, na_values=[''], dtype=df4_2_col_dtypes)
# ### ValueError: Unable to convert column AD1InsChangeDate.9 to type datetime64[ns].

# #%%
# df4_3 = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[2], keep_default_na=False, na_values=[''], dtype=df4_3_col_dtypes)
# ### ValueError: False cannot be cast to bool.

# #%%
# df4_4 = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[3], keep_default_na=False, na_values=[''], dtype=df4_4_col_dtypes)
# ### ValueError: Unable to parse string "HS Grad" at position 0.

# #%%
# df4_5 = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[4], keep_default_na=False, na_values=[''], dtype=df4_5_col_dtypes)

#%%###################################
### READ in all sheets as strings.

### To read in EVERYTHING as a string with NO NA:
### df4_1_allstring = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[0], na_filter=False, keep_default_na=False, dtype='string')# dtype=df4_1_col_dtypes)
### df4_2_allstring = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[1], na_filter=False, keep_default_na=False, dtype='string')# dtype=df4_2_col_dtypes)
### df4_3_allstring = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[2], na_filter=False, keep_default_na=False, dtype='string')# dtype=df4_3_col_dtypes)
### df4_4_allstring = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[3], na_filter=False, keep_default_na=False, dtype='string')# dtype=df4_4_col_dtypes)
### df4_5_allstring = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[4], na_filter=False, keep_default_na=False, dtype='string')# dtype=df4_5_col_dtypes)

### To read in EVERYTHING as a string WITH NA:
df4_1_allstring = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[0], keep_default_na=False, na_values=[''], dtype='string')# dtype=df4_1_col_dtypes)
df4_2_allstring = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[1], keep_default_na=False, na_values=[''], dtype='string')# dtype=df4_2_col_dtypes)
df4_3_allstring = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[2], keep_default_na=False, na_values=[''], dtype='string')# dtype=df4_3_col_dtypes)
df4_4_allstring = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[3], keep_default_na=False, na_values=[''], dtype='string')# dtype=df4_4_col_dtypes)
df4_5_allstring = pd.read_excel(xlsx, sheet_name=path_4_data_source_sheets[4], keep_default_na=False, na_values=[''], dtype='string')# dtype=df4_5_col_dtypes)

df4_1 = df4_1_allstring.copy()
df4_2 = df4_2_allstring.copy()
df4_3 = df4_3_allstring.copy()
df4_4 = df4_4_allstring.copy()
df4_5 = df4_5_allstring.copy()

#%%##################################################
### CLEAN ###
#####################################################

# def fn_try_astype(fdf, dict_col_dtypes):
#     for column in fdf.columns:
#         try:
#             fdf[column] = fdf[column].astype(dict_col_dtypes[column])
#         except Exception as e:
#             print('Error for column: ', column)
#             print('Attempted dtype: ', dict_col_dtypes[column])
#             print(e, '\n')

### Function designed to turn a Pandas DataFrame whose columns are all dtype 'string' into dtypes specified in a dictionary.
def fn_apply_dtypes(fdf, dict_col_dtypes):
    for column in fdf.columns:
        if (dict_col_dtypes[column] == 'datetime64[ns]'):
            try:
                fdf[column] = pd.to_datetime(fdf[column])
                ### Because .astype() cannot handle a 'string' dtype with multiple date formats, but .to_datetime() can!
            except Exception as e:
                print('Error for column: ', column)
                print('Attempted dtype: ', dict_col_dtypes[column])
                print(e, '\n')
        elif (dict_col_dtypes[column] == 'Int64'):
            try:
                fdf[column] = fdf[column].astype('Float64').astype('Int64')
                ### Because .astype() cannot turn a string of a float (e.g., "3.0") into Int64. This solution might not be needed by every column to be turned into Int64.
            except Exception as e:
                print('Error for column: ', column)
                print('Attempted dtype: ', dict_col_dtypes[column])
                print(e, '\n')
        elif (dict_col_dtypes[column] == 'boolean' and all([(x in set(['True', 'False', '1', '0', '1.0', '0.0', pd.NA])) for x in set(fdf[column])])):
            try:
                fdf[column] = fdf[column].map({'True': True, 'False': False, '1': True, '0': False, '1.0': True, '0.0': False}).astype('boolean')
                ### Because .astype() cannot turn strings ['True', 'False'] into dtype 'boolean' (or correctly/at all for 'bool', depending if has NA).
                ### Also: .astype() can turn integer & float 0/1 into boolean, but not strings of ints/floats.
                ### Solution only for cases where column has only the values listed -- because other values will be forcibly coerced to NA (which we don't want).
            except Exception as e:
                print('Error for column: ', column)
                print('Attempted dtype: ', dict_col_dtypes[column])
                print(e, '\n')
        else:
            try:
                fdf[column] = fdf[column].astype(dict_col_dtypes[column])
                ### Default. I wish this worked with all dtypes.
            except Exception as e:
                print('Error for column: ', column)
                print('Attempted dtype: ', dict_col_dtypes[column])
                print(e, '\n')
    return fdf 

#%%###################################
### Testing

# inspect_col(df4_2['AD1InsChangeDate.1'])
### In df4_2 tab "Caregiver Insurance", dates columns have 2 conflicting formats.

#%%
# print(df4_2.dtypes.to_string())
#%%
# fn_try_astype(df4_2, df4_2_col_dtypes)
# fn_apply_dtypes(df4_2, df4_2_col_dtypes)
#%%
# print(df4_2.dtypes.to_string())

#%%
# fn_try_astype(df4_3, df4_3_col_dtypes)
# fn_apply_dtypes(df4_3, df4_3_col_dtypes)
#%%
# print(df4_3.dtypes.to_string())
#%%
# inspect_df(df4_3)

#%%
# fn_try_astype(df4_4, df4_4_col_dtypes)
# fn_apply_dtypes(df4_4, df4_4_col_dtypes)
#%%
# print(df4_4.dtypes.to_string())

#%%###################################
### df4_1: 'Project ID'.
df4_1 = (
    df4_1
    ###.astype(df4_1_col_dtypes)
    .pipe(fn_apply_dtypes, df4_1_col_dtypes)
)
inspect_df(df4_1)

#%%###################################
### df4_2: 'Caregiver Insurance'.
df4_2 = (
    df4_2
    ###.astype(df4_2_col_dtypes)
    .pipe(fn_apply_dtypes, df4_2_col_dtypes)
)
inspect_df(df4_2)

# Error for column:  AD1InsChangeDate.9
# Attempted dtype:  datetime64[ns]
# Unknown string format: Unknown present at position 1 

# Error for column:  AD1InsChangeDate.10
# Attempted dtype:  datetime64[ns]
# Unknown string format: Medicaid present at position 1 

# Error for column:  AD1InsChangeDate.11
# Attempted dtype:  datetime64[ns]
# Unknown string format: None present at position 1 

# Error for column:  AD1InsChangeDate.12
# Attempted dtype:  datetime64[ns]
# Unknown string format: Medicaid present at position 1 

# Error for column:  AD1InsChangeDate.13
# Attempted dtype:  datetime64[ns]
# Unknown string format: None present at position 1 

# Error for column:  AD1InsChangeDate.14
# Attempted dtype:  datetime64[ns]
# Unknown string format: Unknown present at position 1 

# Error for column:  AD1InsChangeDate.15
# Attempted dtype:  datetime64[ns]
# Unknown string format: Medicaid present at position 1 

#%%###################################
### df4_3: 'Family Wise'.
df4_3 = (
    df4_3
    ###.astype(df4_3_col_dtypes)
    .pipe(fn_apply_dtypes, df4_3_col_dtypes)
) 
inspect_df(df4_3)
#%%
### Test:
# (
#     df4_3
#     ###.astype(df4_3_col_dtypes)
#     .pipe(fn_apply_dtypes, df4_3_col_dtypes)
#     .loc[:, [k for k, v in df4_3_col_dtypes.items() if v == 'boolean']]
#     .pipe(fn_all_value_counts)
# ) 

#%%###################################
### df4_4: 'LLCHD'.
df4_4 = (
    df4_4
    ###.astype(df4_4_col_dtypes)
    .pipe(fn_apply_dtypes, df4_4_col_dtypes)
)
inspect_df(df4_4)

# Error for column:  fob_edu
# Attempted dtype:  Int64
# could not convert string to float: 'HS Grad' 

#%%###################################
### df4_5: 'MOB or FOB'.
df4_5 = (
    df4_5
    ###.astype(df4_5_col_dtypes)
    .pipe(fn_apply_dtypes, df4_5_col_dtypes)
)
inspect_df(df4_5)


#%%###################################

### TODO:
    ### Review conversions & see if anything lost.
    ### Compare old to new cols & see if any new NA. Write function to compare.








#%%###################################
### Review each sheet:
### Note: Even empty DFs merge fine below.

#%%### df4_1: 'Project ID'.
inspect_df(df4_1)
#%%### df4_2: 'Caregiver Insurance'.
inspect_df(df4_2)
#%%### df4_3: 'Family Wise'.
inspect_df(df4_3)
#%%### df4_4: 'LLCHD'.
inspect_df(df4_4)
#%%### df4_5: 'MOB or FOB'.
inspect_df(df4_5)

### TODO: Ask how cross-joining (exploding) ALL rows with "MOB" & "FOB" helps anything.

#%%##################################################
### Rename Columns ###
#####################################################

#######################
#%%### df4_1: 'Project ID'.
[*df4_1]
#%%### df4_1: 'Project ID'.
df4_1_colnames
#%%### df4_1: 'Project ID'.
df4_1 = df4_1.rename(columns=df4_1_colnames)
[*df4_1]

#######################
#%%### df4_2: 'Caregiver Insurance'.
[*df4_2]
#%%### df4_2: 'Caregiver Insurance'.
df4_2_colnames
#%%### df4_2: 'Caregiver Insurance'.
df4_2 = df4_2.rename(columns=df4_2_colnames)
[*df4_2]

#######################
#%%### df4_3: 'Family Wise'.
[*df4_3]
#%%### df4_3: 'Family Wise'.
df4_3_colnames
#%%### df4_3: 'Family Wise'.
df4_3 = df4_3.rename(columns=df4_3_colnames)
[*df4_3]

#######################
#%%### df4_4: 'LLCHD'.
[*df4_4]
#%%### df4_4: 'LLCHD'.
df4_4_colnames
#%%### df4_4: 'LLCHD'.
df4_4 = df4_4.rename(columns=df4_4_colnames)
[*df4_4]

#######################
#%%### df4_5: 'MOB or FOB'.
[*df4_5]
#%%### df4_5: 'MOB or FOB'.
df4_5_colnames
#%%### df4_5: 'MOB or FOB'.
df4_5 = df4_5.rename(columns=df4_5_colnames)
[*df4_5]

#%%##################################################
### Prep for JOIN ###
#####################################################

# ### Each row SHOULD be unique on these sheets, especially the 'Project ID' sheet.
# ### TO DO: Actually run section to deduplicate.

# #%%### Restart deduplication
# ### df4_1 = df4_1_bf_ddup.copy()
# ### df4_2 = df4_2_bf_ddup.copy()
# ### df4_3 = df4_3_bf_ddup.copy()
# ### df4_4 = df4_4_bf_ddup.copy()
# ### df4_5 = df4_5_bf_ddup.copy()

# #######################
# ### NOTE: 24 duplicate rows. TO DO: Fix in Master File creation.
# #%%### df4_1: 'Project ID'. 
# ### Backup:
# df4_1_bf_ddup = df4_1.copy()
# #%%### df4_1: 'Project ID'. 
# ### Duplicate rows:
# df4_1[df4_1.duplicated()]
# #%%### df4_1: 'Project ID'. 
# ### Dropping duplicate rows:
# df4_1 = df4_1.drop_duplicates(ignore_index=True)
# df4_1
# #%%### df4_1: 'Project ID'. 
# ### Test
# len(df4_1_bf_ddup) - len(df4_1) == len(df4_1_bf_ddup[df4_1_bf_ddup.duplicated()])
# #%%### df4_1: 'Project ID'. 
# if (len(df4_1_bf_ddup) != len(df4_1)):
#     print(f'{len(df4_1_bf_ddup) - len(df4_1)} duplicate rows dropped.')
# elif (len(df4_1_bf_ddup) == len(df4_1)):
#     print('No duplicate rows.')
# else:
#     print("Don't know what's going on here!")

# #######################
# ### NOTE: NO duplicate rows.
# #%%### df4_2: 'Caregiver Insurance'.
# df4_2_bf_ddup = df4_2.copy()
# #%%### df4_2: 'Caregiver Insurance'.
# df4_2[df4_2.duplicated()]
# # df4_2[df4_2.duplicated(keep=False, subset=['Project ID (ER Injury)','year (ER Injury)','quarter (ER Injury)'])]
# #%%### df4_2: 'Caregiver Insurance'.
# df4_2 = df4_2.drop_duplicates(ignore_index=True)
# df4_2
# #%%### df4_2: 'Caregiver Insurance'.
# len(df4_2_bf_ddup) - len(df4_2) == len(df4_2_bf_ddup[df4_2_bf_ddup.duplicated()])
# #%%### df4_2: 'Caregiver Insurance'.
# if (len(df4_2_bf_ddup) != len(df4_2)):
#     print(f'{len(df4_2_bf_ddup) - len(df4_2)} duplicate rows dropped.')
# elif (len(df4_2_bf_ddup) == len(df4_2)):
#     print('No duplicate rows.')
# else:
#     print("Don't know what's going on here!")

# #######################
# ### NOTE: 3 duplicate rows. TO DO: Fix in Master File creation.
# #%%### df4_3: 'Family Wise'.
# df4_3_bf_ddup = df4_3.copy()
# #%%### df4_3: 'Family Wise'.
# df4_3[df4_3.duplicated()]
# # df4_3[df4_3.duplicated(keep=False, subset=['Project ID','year (Family Wise)','quarter (Family Wise)'])]
# #%%### df4_3: 'Family Wise'.
# df4_3 = df4_3.drop_duplicates(ignore_index=True)
# df4_3
# #%%### df4_3: 'Family Wise'.
# len(df4_3_bf_ddup) - len(df4_3) == len(df4_3_bf_ddup[df4_3_bf_ddup.duplicated()])
# #%%### df4_3: 'Family Wise'.
# if (len(df4_3_bf_ddup) != len(df4_3)):
#     print(f'{len(df4_3_bf_ddup) - len(df4_3)} duplicate rows dropped.')
# elif (len(df4_3_bf_ddup) == len(df4_3)):
#     print('No duplicate rows.')
# else:
#     print("Don't know what's going on here!")

# #######################
# ### NOTE: NO duplicate rows.
# #%%### df4_4: 'LLCHD'.
# df4_4_bf_ddup = df4_4.copy()
# #%%### df4_4: 'LLCHD'.
# df4_4[df4_4.duplicated()]
# # df4_4[df4_4.duplicated(keep=False, subset=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)'])]
# #%%### df4_4: 'LLCHD'.
# df4_4 = df4_4.drop_duplicates(ignore_index=True)
# df4_4
# #%%### df4_4: 'LLCHD'.
# len(df4_4_bf_ddup) - len(df4_4) == len(df4_4_bf_ddup[df4_4_bf_ddup.duplicated()])
# #%%### df4_4: 'LLCHD'.
# if (len(df4_4_bf_ddup) != len(df4_4)):
#     print(f'{len(df4_4_bf_ddup) - len(df4_4)} duplicate rows dropped.')
# elif (len(df4_4_bf_ddup) == len(df4_4)):
#     print('No duplicate rows.')
# else:
#     print("Don't know what's going on here!")

# #######################
# ### NOTE: NO duplicate rows.
# #%%### df4_5: 'MOB or FOB'.
# df4_5_bf_ddup = df4_5.copy()
# #%%### df4_5: 'MOB or FOB'.
# df4_5[df4_5.duplicated()]
# # df4_5[df4_5.duplicated(keep=False, subset=[...])]
# #%%### df4_5: 'MOB or FOB'.
# df4_5 = df4_5.drop_duplicates(ignore_index=True)
# df4_5
# #%%### df4_5: 'MOB or FOB'.
# len(df4_5_bf_ddup) - len(df4_5) == len(df4_5_bf_ddup[df4_5_bf_ddup.duplicated()])
# #%%### df4_5: 'MOB or FOB'.
# if (len(df4_5_bf_ddup) != len(df4_5)):
#     print(f'{len(df4_5_bf_ddup) - len(df4_5)} duplicate rows dropped.')
# elif (len(df4_5_bf_ddup) == len(df4_5)):
#     print('No duplicate rows.')
# else:
#     print("Don't know what's going on here!")

#%%##################################################
### JOIN ###
#####################################################

### TO DO: Turn on validation once deduplication turned on.
### TO DO: Address "PerformanceWarning: DataFrame is highly fragmented." from running merge.

#%%
df4 = (
    pd.merge(
        df4_1 ### 'Project ID'.
        ,df4_2 ### 'Caregiver Insurance'.
        ,how='left'
        ,left_on=['Project Id','Year','Quarter']
        ,right_on=['Project ID','year (Caregiver Insurance)','quarter (Caregiver Insurance)']
        ,indicator='LJ_df4_2CI'
        # ,validate='one_to_one'
    ).merge(
        df4_3 ### 'Family Wise'.
        ,how='left'
        ,left_on=['Project Id','Year','Quarter']
        ,right_on=['Project ID1','year (Family Wise)','quarter (Family Wise)']
        ,indicator='LJ_df4_3FW'
        # ,validate='one_to_one'
    ).merge(
        df4_4 ### 'LLCHD'.
        ,how='left'
        ,left_on=['Project Id','Year','Quarter']
        ,right_on=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)']
        ,indicator='LJ_df4_4LL'
        # ,validate='one_to_one'
    ).merge(
        df4_5 ### 'MOB or FOB'.
        ,how='left'
        ,left_on=['Join Id']
        ,right_on=['join id (MOB or FOB)']
        ,indicator='LJ_df4_5MoF'
        # ,validate='one_to_one'
    )
) 


##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################


#%%##################################################
### RECREATE every Tableau Calculation ###
#####################################################

#%%
df4_edits1 = df4.copy()  ### Make a deep-ish copy of the DF's Data. Does NOT copy embedded mutable objects.


#####################################################
#####################################################
#####################################################
#%%##################################################

df4_edits1['Number of Records'] = 1 
inspect_col(df4_edits1['Number of Records'])

#%%
df4_edits1['source'] = (
    df4_edits1
    .apply(func=(
        lambda df: 'FW' if pd.notna(df['Project ID1']) else ('LL' if pd.notna('project id (LLCHD)') else 'um... problem')
    ), axis=1)
    .astype('string') 
)
inspect_col(df4_edits1['source'])


#####################################################
#####################################################
#####################################################
#%%##################################################
### DUPLICATING


#####################################################
#####################################################
#####################################################
#%%##################################################
### COALESCING 

df4_edits1['_Agency'] = df4_edits1['agency (Family Wise)'].combine_first(df4_edits1['Site Id']).astype('string') 
    ### IFNULL([agency (Family Wise)],[Site Id])
    ### Data Type in Tableau: string.
inspect_col(df4_edits1['_Agency'])

df4_edits1['_Family ID'] = df4_edits1['Family Id'].combine_first(df4_edits1['Family Number']).astype('string') 
    ### IFNULL([Family Id], [Family Number])
    ### Data Type in Tableau: string.
inspect_col(df4_edits1['_Family ID'])

df4_edits1['_TGT ID'] = df4_edits1['Tgt Id'].combine_first(df4_edits1['Child Number']).astype('string') 
    ### STR(IFNULL([Tgt Id],[Child Number]))
    ### Data Type in Tableau: string.
inspect_col(df4_edits1['_TGT ID'])

#%%###################################

### 'Mob Zip' has the string value "null" that needs to be recoded.
### TODO: limit ZIP codes to first five -- have some with the extra 4.
df4_edits1['_Zip'] = (
    df4_edits1['Zip'].combine_first(df4_edits1['Mob Zip'])
    .replace('null', pd.NA)
    .astype('Int64') 
)
    ### IFNULL([Zip], INT([Mob Zip]))
    ### Data Type in Tableau: integer.
inspect_col(df4_edits1['_Zip'])
# #%%
# print(df4_edits1['_Zip'].value_counts(dropna=False).to_string())
# #%%
# # inspect_col(df4_edits1['Zip'])
# print(df4_edits1['Zip'].value_counts(dropna=False).to_string())
# #%%
# # inspect_col(df4_edits1['Mob Zip'])
# print(df4_edits1['Mob Zip'].value_counts(dropna=False).to_string()) ### Actually has text "null".
# #%%
# # print(df4_edits1[['_Zip', 'Mob Zip', 'Zip']].query('`Mob Zip` == "null"').to_string())
# print(df4_edits1[['_Zip', 'Mob Zip', 'Zip']].to_string())

#%%

df4_edits1['_T16 Number of Home Visits'] = df4_edits1['HomeVisitsTotal'].combine_first(df4_edits1['Home Visits Num']).astype('Float64').astype('Int64') 
    ### IFNULL([HomeVisitsTotal],[Home Visits Num])
    ### Data Type in Tableau: integer.
inspect_col(df4_edits1['_T16 Number of Home Visits'])

#%%###################################

df4_edits1['_C15 Max Education Date'] = df4_edits1['Mcafss Edu Dt2'].combine_first(df4_edits1['Max Edu Date']).astype('datetime64[ns]') 
    ### IFNULL([Mcafss Edu Dt2],[Max Edu Date])
    ### Data Type in Tableau: date.
inspect_col(df4_edits1['_C15 Max Education Date'])

df4_edits1['_C15 Min Education Date'] = df4_edits1['Mcafss Edu Dt1'].combine_first(df4_edits1['Min Education Date']).astype('datetime64[ns]') 
    ### IFNULL([Mcafss Edu Dt1],[Min Education Date])
    ### Data Type in Tableau: date.
inspect_col(df4_edits1['_C15 Min Education Date'])

df4_edits1['_Discharge Date'] = df4_edits1['Termination Date'].combine_first(df4_edits1['Discharge Dt']).astype('datetime64[ns]') 
    ### IFNULL([Termination Date],[Discharge Dt])
    ### Data Type in Tableau: date.
inspect_col(df4_edits1['_Discharge Date'])

df4_edits1['_Enrollment Date'] = df4_edits1['Min Of HV Date'].combine_first(df4_edits1['Enroll Dt']).astype('datetime64[ns]') 
    ### IFNULL([Min Of HV Date],[Enroll Dt])
    ### Data Type in Tableau: date.
inspect_col(df4_edits1['_Enrollment Date'])

df4_edits1['_Max HV Date'] = df4_edits1['Max Of HV Date'].combine_first(df4_edits1['Last Home Visit']).astype('datetime64[ns]') 
    ### IFNULL([Max Of HV Date],[Last Home Visit])
    ### Data Type in Tableau: date.
inspect_col(df4_edits1['_Max HV Date'])


#####################################################
#####################################################
#####################################################
#%%##################################################

### Other vars depend on this ('_TGT 3 Month Date').
### In Child2 & Adult3 & Adult4. Copied exactly. (except added astype()).
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
df4_edits1['_TGT DOB'] = df4_edits1.apply(func=fn_TGT_DOB, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: 'date'.
inspect_col(df4_edits1['_TGT DOB'])
# #%%
# inspect_col(df4_edits1['Tgt Dob'])
# #%%
# inspect_col(df4_edits1['Tgt Dob-Cr'])


#####################################################
#####################################################
#####################################################
#%%##################################################
### DATE CALCULATIONS

### These calculations assume all date variables are dtype "datetime64".

df4_edits1['_Enroll 3 Month Date'] = df4_edits1['_Enrollment Date'].astype('datetime64[ns]') + pd.DateOffset(months=3) 
    ### DATE(DATEADD('month',3,[_Enrollment Date]))
    ### Data Type in Tableau: date.
inspect_col(df4_edits1['_Enroll 3 Month Date'])

df4_edits1['_TGT 3 Month Date'] = df4_edits1['_TGT DOB'].astype('datetime64[ns]') + pd.DateOffset(months=3) 
    ### DATE(DATEADD('month',3,[_TGT DOB]))
    ### Data Type in Tableau: date.
inspect_col(df4_edits1['_TGT 3 Month Date'])


#####################################################
#####################################################
#####################################################
#%%##################################################
### IF/ELSE, CASE/WHEN

### fdf == "function DataFrame"

#%%###################################

### Required for '__F1 Caregiver ID for MOB or FOB'.
### Purpose: Remove "-" & numbers after.
def fn__Primary_Caregiver_ID(fdf):
    return re.findall(r'^.*(?=-)', fdf['Project Id'])[0]
    ### /// Tableau Calculation:
    ### MID([Project Id], 1, FIND([Project Id], '-') - 1) 
df4_edits1['__Primary Caregiver ID'] = df4_edits1.apply(func=fn__Primary_Caregiver_ID, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['__Primary Caregiver ID']) 
# #%%
# re.findall(r'(^.*)(?=-)', df4_edits1['Project Id'])[0]
# re.findall(r'.*', df4_edits1['Project Id'])#[0]
# re.findall(r'^.*(?=-)', 'llSF77010001416-562')[0]

#%%###################################

### Note: 'MOB or FOB' cannot be NA because of join.
def fn__F1_Caregiver_ID_for_MOB_or_FOB(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['__Primary Caregiver ID'] + "MOB"
        case "FOB":
            return fdf['__Primary Caregiver ID'] + "FOB"
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN STR([__Primary Caregiver ID]) + "MOB"
    ###     WHEN "FOB" THEN STR([__Primary Caregiver ID]) + "FOB"
    ### END
df4_edits1['__F1 Caregiver ID for MOB or FOB'] = df4_edits1.apply(func=fn__F1_Caregiver_ID_for_MOB_or_FOB, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['__F1 Caregiver ID for MOB or FOB']) 

#%%###################################

### Required for 'Caregiver Involved'.
def fn_FOB_Involved(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Fob Involved']:
            case _ if pd.isna(fdf['Fob Involved']):
                return 0 
            case True:
                return 1 
            case False:
                return 0 
            case _:
                return 0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Fob Involved1']:
            case _ if pd.isna(fdf['Fob Involved1']):
                return 0 
            case "Y":
                return 1 
            case "N":
                return 0 
            case _:
                return 0 
    ###########
    else:
        return 0 
    ###########
    ### /// Tableau Calculation:
    ### IF [Fob Involved] = True THEN 1 //FW
    ### ELSEIF [Fob Involved] = False THEN 0
    ### ELSEIF [Fob Involved1] = "Y" THEN 1 //LLCHD
    ### ELSEIF [Fob Involved1] = "N" THEN 0
    ### ELSE 0
    ### END
df4_edits1['_FOB Involved'] = df4_edits1.apply(func=fn_FOB_Involved, axis=1).astype('Int64') 
    ### Data Type in Tableau: 'int'.
inspect_col(df4_edits1['_FOB Involved']) 
# #%%
# print(df4_edits1[['source', '_FOB Involved', 'Fob Involved', 'Fob Involved1']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string()) 

#%%###################################

### Note: 'MOB or FOB' cannot be NA because of join.
def fn_Caregiver_Involved(fdf):
    return (
    fdf['MOB or FOB'] == "MOB"
    or
        (fdf['MOB or FOB'] == "FOB"
        and
        fdf['_FOB Involved'] == 1)
    )
    ### /// Tableau Calculation:
    ### [MOB or FOB] = "MOB"
    ### OR
    ###     ([MOB or FOB] = "FOB"
    ###     AND
    ###     [_FOB Involved] = 1)
df4_edits1['Caregiver Involved'] = df4_edits1.apply(func=fn_Caregiver_Involved, axis=1).astype('boolean') 
    ### Data Type in Tableau: 'boolean'.
inspect_col(df4_edits1['Caregiver Involved']) 
# #%%
# print(df4_edits1[['MOB or FOB', '_FOB Involved', 'Caregiver Involved', 'source']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string()) 


#%%###################################

def fn_TGT_EDC_Date(fdf):
    ### LLCHD.
    if (fdf['Dt Edc'].date() == pd.Timestamp("1900-01-01").date()):
        return pd.NaT 
    ### FW.
    elif (fdf['EDC Date'].date() == pd.Timestamp("1900-01-01").date()):
        return pd.NaT 
    else:
        if (fdf['Dt Edc'] is not pd.NaT):
            return fdf['Dt Edc']
        else:
            return fdf['EDC Date']
    ### /// Tableau Calculation:
    ### IF [Dt Edc] = DATE(1/1/1900) THEN NULL //LLCHD
    ### ELSEIF [EDC Date] = DATE(1/1/1900) THEN NULL //FW
    ### ELSE IFNULL([Dt Edc],[EDC Date])
    ### END
df4_edits1['_TGT EDC Date'] = df4_edits1.apply(func=fn_TGT_EDC_Date, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: 'date'.
inspect_col(df4_edits1['_TGT EDC Date']) 

#%%###################################

def fn_MOB_Gender(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Adult1Gender']:
            case _ if pd.isna(fdf['Adult1Gender']):
                return pd.NA 
            case "Female":
                return "Female"
            case "Male":
                return "Male" 
            case "Non-Binary":
                return "Non-Binary" 
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Mob Gender']:
            case _ if pd.isna(fdf['Mob Gender']):
                return pd.NA 
            case "F":
                return "Female"
            case "M":
                return "Male" 
            ### case "Non-Binary":
            ###     return "Non-Binary" ### Don't have this value yet - Confirm.
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### IF [Adult1Gender] = "Female" THEN "Female" //FW
    ### ELSEIF [Adult1Gender] = "Male" THEN "Male"
    ### ELSEIF [Adult1Gender] = "Non-Binary" THEN "Non-Binary"
    ### ELSEIF [Mob Gender]= "F" THEN "Female" //LLCHD
    ### ELSEIF [Mob Gender] = "M" THEN "Male"
    ### // ELSEIF [Mob Gender] = "N" THEN "Non-Binary" // Don't have this value yet - Confirm
    ### END
df4_edits1['_MOB Gender'] = df4_edits1.apply(func=fn_MOB_Gender, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_MOB Gender']) 
### TODO:Confirm that we don't have/are not expecting "N" from LL.

#%%###################################

def fn_FOB_Gender(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            return fdf['Adult2Gender'] 
        else:
            return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == "Y"):
            match fdf['Fob Gender']:
                case _ if pd.isna(fdf['Fob Gender']):
                    return pd.NA 
                case "F":
                    return "Female"
                case "M":
                    return "Male" 
                case "Non-Binary":
                    return "Non-Binary" ### Don't have this value yet - Confirm.
                case _:
                    return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
        else:
            return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########    ### /// Tableau Calculation:
    ### //should we incorporate involved status into the fob variables?
    ### IF [Fob Involved1] = "Y" THEN CASE[Fob Gender]
    ###     WHEN "M" THEN "Male" //LLCHD
    ###     WHEN "F" THEN "Female"
    ###     WHEN "N" THEN "Non-Binary" // Don't have this value yet - confirm
    ###     END
    ### ELSEIF [Fob Involved] = True THEN [Adult2Gender] //FW
    ### ELSE NULL
    ### END
df4_edits1['_FOB Gender'] = df4_edits1.apply(func=fn_FOB_Gender, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_FOB Gender']) 
### TODO:Confirm that we don't have/are not expecting "N" from LL.
### TODO: Ask old question: //should we incorporate involved status into the fob variables?

#%%###################################

def fn_F1_Caregiver_Gender(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_MOB Gender']
        case "FOB":
            return fdf['_FOB Gender']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_MOB Gender]
    ###     WHEN "FOB" THEN [_FOB Gender]
    ### END
df4_edits1['_F1 Caregiver Gender'] = df4_edits1.apply(func=fn_F1_Caregiver_Gender, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_F1 Caregiver Gender']) 

#%%###################################

def fn_MOB_TGT_Relation(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Adult1TGTRelation']:
            case _ if pd.isna(fdf['Adult1TGTRelation']):
                return pd.NA 
            case "MOB" | "Biological mother" | "Foster mother" | "Grandmother":
                return "MOB"
            case "FOB" | "Biological father" | "Adoptive father":
                return "FOB"
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?0 
            ### TODO: Maybe add options from 'Adult2TGTRelation': "Foster father", "Guardian", "Other".
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Primary Relation']:
            case _ if pd.isna(fdf['Primary Relation']):
                return pd.NA 
            case "MOTHER OF CHILD":
                return "MOB"
            case "FATHER OF CHILD":
                return "FOB" 
            case "PRIMARY CAREGIVER":
                match fdf['Mob Gender']:
                    case _ if pd.isna(fdf['Mob Gender']):
                        return pd.NA 
                    case "F":
                        return "MOB"
                    case "M":
                        return "FOB"
                    case _:
                        return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### //should we call this primary relation??
    ### //FW
    ### IF [Adult1TGTRelation] = "Biological mother" THEN "MOB" 
    ### ELSEIF  [Adult1TGTRelation] = "Biological father" THEN "FOB"
    ### ELSEIF  [Adult1TGTRelation] = "FOB" THEN "FOB"
    ### ELSEIF  [Adult1TGTRelation] = "MOB" THEN "MOB"
    ### ELSEIF  [Adult1TGTRelation] = "Foster mother" THEN "MOB"
    ### ELSEIF  [Adult1TGTRelation] = "Adoptive father" THEN "FOB"
    ### ELSEIF  [Adult1TGTRelation] = "Grandmother" THEN "MOB"
    ### //LLCHD
    ### ELSEIF [Primary Relation] = "FATHER OF CHILD" THEN "FOB" 
    ### ELSEIF [Primary Relation] = "MOTHER OF CHILD" THEN "MOB"
    ### ELSEIF [Primary Relation] = "PRIMARY CAREGIVER" AND [Mob Gender] = "F" THEN "MOB"
    ### ELSEIF [Primary Relation] = "PRIMARY CAREGIVER" AND [Mob Gender] = "M" THEN "FOB"
    ### //alternatively we could just leave PCs as that
    ### END
df4_edits1['_MOB TGT Relation'] = df4_edits1.apply(func=fn_MOB_TGT_Relation, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_MOB TGT Relation']) 
### TODO: Ask old question: //should we call this primary relation??
### TODO: Ask: What is this referring to?: //alternatively we could just leave PCs as that.

#%%###################################

def fn_FOB_Relation(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            match fdf['Adult2TGTRelation']:
                case _ if pd.isna(fdf['Adult2TGTRelation']):
                    return pd.NA 
                case "MOB" | "Biological mother":
                    return "MOB"
                case "FOB" | "Biological father" | "Foster father":
                    return "FOB"
                case "Grandmother": 
                    return "Grandmother" ### TODO: Review whether should match MOB version where "Grandmother" is "MOB".
                case "Guardian":
                    return "Guardian"
                case "Other":
                    return "Other"
                case _:
                    return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?0 
                ### TODO: Maybe add options from 'Adult1TGTRelation': "Foster mother", "Adoptive father".
        else:
            return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == "Y"):
            return "FOB"
        else:
            return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### IF [Fob Involved1] = "Y" THEN "FOB"
    ### ELSEIF [Fob Involved] = True
    ### THEN CASE [Adult2TGTRelation]
    ###     WHEN "Biological father" THEN "FOB"
    ###     WHEN "Biological mother" THEN "MOB"
    ###     WHEN "FOB" THEN "FOB"
    ###     WHEN "Foster father" THEN "FOB"
    ###     WHEN "Guardian" THEN "Guardian"
    ###     WHEN "Grandmother" THEN "Grandmother"
    ###     WHEN "MOB" THEN "MOB"
    ###     WHEN "Other" THEN "Other"
    ###     END
    ### ELSE NULL
    ### END
df4_edits1['_FOB Relation'] = df4_edits1.apply(func=fn_FOB_Relation, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_FOB Relation']) 

#%%###################################

def fn_Enroll_Preg_Status(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Pregnancystatus']:
            case _ if pd.isna(fdf['Pregnancystatus']):
                return pd.NA 
            case 0:
                return 'Pregnant'
            case 1:
                return 'Not pregnant'
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Enroll Preg Status']:
            case _ if pd.isna(fdf['Enroll Preg Status']):
                return pd.NA 
            case 'Pregnant':
                return 'Pregnant'
            case 'Postpartum':
                return 'Not pregnant'
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### IF [Pregnancystatus] = 0 THEN "Pregnant" //FW
    ### ELSEIF [Pregnancystatus] = 1 THEN "Not pregnant"
    ### ELSEIF [Enroll Preg Status] = "Postpartum" THEN "Not pregnant" //LLCHD
    ### ELSEIF [Enroll Preg Status] = "Pregnant" THEN "Pregnant"
    ### ELSE NULL
    ### END
df4_edits1['_Enroll Preg Status'] = df4_edits1.apply(func=fn_Enroll_Preg_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_Enroll Preg Status']) 

#%%###################################

### ### Test whether code works with NAs:
### df4_edits1['Assess Afraid'] = df4_edits1['Assess Afraid'].sample(frac=0.8)
### df4_edits1['Assess IPV'] = df4_edits1['Assess IPV'].sample(frac=0.8)
### df4_edits1['Assess Police'] = df4_edits1['Assess Police'].sample(frac=0.8)

### TODO: Is 'Agency' from df4_2 ('Caregiver Insurance') wanted? Or 'agency (Family Wise)'? Or '_Agency'?
### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_IPV_Score_FW(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Agency']):
            return pd.NA
        elif (fdf['Agency'] != '11'):
            if ((
                (0 if pd.isna(fdf['Assess Afraid']) else (1 if fdf['Assess Afraid'] else 0)) + 
                (0 if pd.isna(fdf['Assess IPV']) else (1 if fdf['Assess IPV'] else 0)) +
                (0 if pd.isna(fdf['Assess Police']) else (1 if fdf['Assess Police'] else 0))
                ) >= 1 
            ):
                return 'P'
            else:
                return 'N'
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
        ### including LLCHD.
    ###########
    ### /// Tableau Calculation:
    ### IF [Agency] <> "ll" THEN
    ###     (IF [Assess Afraid] = TRUE 
    ###     OR [Assess IPV] = TRUE 
    ###     OR [Assess Police] = TRUE
    ###     THEN "P" ELSE "N" END)
    ### END
df4_edits1['_IPV Score FW'] = df4_edits1.apply(func=fn_IPV_Score_FW, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_IPV Score FW']) 
#%%
# print(df4_edits1[['source', '_IPV Score FW', 'Agency', 'Assess Afraid', 'Assess IPV', 'Assess Police']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string()) 
# print(df4_edits1[['source', '_IPV Score FW', 'Assess Afraid', 'Assess IPV', 'Assess Police']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string()) 

#%%###################################

def fn_Need_Exclusion_1_Sub_Abuse(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Need Exclusion1']:
            case _ if pd.isna(fdf['Need Exclusion1']):
                return pd.NA 
            case "Substance Abuse" | "Drug Abuse" | "Alcohol Abuse":
                return "Alcohol/Drug Abuse"
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['need exclusion1 (LLCHD)']:
            case _ if pd.isna(fdf['need exclusion1 (LLCHD)']):
                return pd.NA 
            case "Y":
                return "Alcohol/Drug Abuse"
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### IF [Need Exclusion1] = "Substance Abuse" THEN "Alcohol/Drug Abuse" //FW
    ###     ELSEIF [Need Exclusion1] = "Drug Abuse" THEN "Alcohol/Drug Abuse"
    ###     ELSEIF [Need Exclusion1] = "Alcohol Abuse" THEN "Alcohol/Drug Abuse"
    ### ELSEIF [need exclusion1 (LLCHD)] = "Y" THEN "Alcohol/Drug Abuse" //LLCHD
    ### END
df4_edits1['_Need Exclusion 1 - Sub Abuse'] = df4_edits1.apply(func=fn_Need_Exclusion_1_Sub_Abuse, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_Need Exclusion 1 - Sub Abuse']) 

#%%###################################

def fn_Need_Exclusion_2_Fam_Plan(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Need Exclusion2']:
            case _ if pd.isna(fdf['Need Exclusion2']):
                return pd.NA 
            case "Family Planning":
                return "Family Planning"
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['need exclusion2 (LLCHD)']:
            case _ if pd.isna(fdf['need exclusion2 (LLCHD)']):
                return pd.NA 
            case "Y":
                return "Family Planning"
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### IF [Need Exclusion2] = "Family Planning" THEN "Family Planning" //FW
    ### ELSEIF [need exclusion2 (LLCHD)] = "Y" THEN "Family Planning" //LLCHD
    ### END
df4_edits1['_Need Exclusion 2 - Fam Plan'] = df4_edits1.apply(func=fn_Need_Exclusion_2_Fam_Plan, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_Need Exclusion 2 - Fam Plan']) 

#%%###################################

def fn_Need_Exclusion_3_Mental_Health(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Need Exclusion3']:
            case _ if pd.isna(fdf['Need Exclusion3']):
                return pd.NA 
            case "Mental Health":
                return "Mental Health"
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['need exclusion3 (LLCHD)']:
            case _ if pd.isna(fdf['need exclusion3 (LLCHD)']):
                return pd.NA 
            case "Y":
                return "Mental Health"
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### IF [Need Exclusion3] = "Mental Health" THEN "Mental Health" //FW
    ### ELSEIF [need exclusion3 (LLCHD)] = "Y" THEN "Mental Health" //LLCHD
    ### END
df4_edits1['_Need Exclusion 3 - Mental Health'] = df4_edits1.apply(func=fn_Need_Exclusion_3_Mental_Health, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_Need Exclusion 3 - Mental Health']) 

#%%###################################

def fn_Need_Exclusion_5_IPV(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Need Exclusion5']:
            case _ if pd.isna(fdf['Need Exclusion5']):
                return pd.NA 
            case "IPV Services":
                return "IPV Services"
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['need exclusion5 (LLCHD)']:
            case _ if pd.isna(fdf['need exclusion5 (LLCHD)']):
                return pd.NA 
            case "Y":
                return "IPV Services"
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### IF [Need Exclusion5] = "IPV Services" THEN "IPV Services" //FW
    ### ELSEIF [need exclusion5 (LLCHD)] = "Y" THEN "IPV Services" //LLCHD
    ### END
df4_edits1['_Need Exclusion 5 - IPV'] = df4_edits1.apply(func=fn_Need_Exclusion_5_IPV, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_Need Exclusion 5 - IPV']) 

#%%###################################

def fn_Need_Exclusion_6_Tobacco(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['need_exclusion6 (Family Wise)']:
            case _ if pd.isna(fdf['need_exclusion6 (Family Wise)']):
                return pd.NA 
            case "Tobacco Cessation":
                return "Tobacco Cessation"
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['need_exclusion6']:
            case _ if pd.isna(fdf['need_exclusion6']):
                return pd.NA 
            case "Y":
                return "Tobacco Cessation"
            case _:
                return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return pd.NA ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### IF [need_exclusion6 (Family Wise)] = "Tobacco Cessation" THEN "Tobacco Cessation" //FW
    ### ELSEIF [need_exclusion6] = "Y" THEN "Tobacco Cessation" //LLCHD
    ### END
df4_edits1['_Need Exclusion 6 - Tobacco'] = df4_edits1.apply(func=fn_Need_Exclusion_6_Tobacco, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_Need Exclusion 6 - Tobacco']) 

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
    ### /// Tableau Calculation:
    ### IF [Mob Dob] = DATE(1/1/1900) THEN NULL //LLCHD
    ### ELSEIF [Mobdob] = DATE(1/1/1900) THEN NULL //FW
    ### ELSE IFNULL([Mob Dob],[Mobdob])
    ### END
df4_edits1['_MOB DOB'] = df4_edits1.apply(func=fn_MOB_DOB, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: 'date'.
inspect_col(df4_edits1['_MOB DOB']) 

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
    ### /// Tableau Calculation:
    ### IF [Fob Dob] = DATE(1/1/1900) THEN NULL //LLCHD
    ### ELSEIF [Fobdob] = DATE(1/1/1900) THEN NULL //FW
    ### ELSE IFNULL([Fob Dob],[Fobdob])
    ### END
df4_edits1['_FOB DOB'] = df4_edits1.apply(func=fn_FOB_DOB, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: 'date'.
inspect_col(df4_edits1['_FOB DOB']) 

#%%###################################

def fn_T04_Caregiver_DOB(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_MOB DOB']
        case "FOB":
            return fdf['_FOB DOB']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_MOB DOB]
    ###     WHEN "FOB" THEN [_FOB DOB]
    ### END
df4_edits1['_T04 Caregiver DOB'] = df4_edits1.apply(func=fn_T04_Caregiver_DOB, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: 'date'.
inspect_col(df4_edits1['_T04 Caregiver DOB']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T06_MOB_Ethnicity(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.notna(fdf['Mob Ethnic']):
            match fdf['Mob Ethnic'].lower():
                case "hispanic/latino":
                    return "Hispanic or Latino"
                case "non hispanic/latino":
                    return "Not Hispanic or Latino"
                case "unknown":
                    return "Unknown/Did Not Report"
                case _:
                    return "Unrecognized Value"
        else: 
            return "Unknown/Did Not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.notna(fdf['Mob Ethnicity']):
            match fdf['Mob Ethnicity'].lower():
                case "hispanic/latino" | "hispanic":
                    return "Hispanic or Latino"
                case "not hispanic/latino" | "non-hispanic":
                    return "Not Hispanic or Latino"
                case _:
                    return "Unrecognized Value"
        else: 
            return "Unknown/Did Not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return "Unknown/Did Not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### IF NOT ISNULL([Mob Ethnic]) THEN CASE [Mob Ethnic]
    ###     WHEN "Non Hispanic/Latino" THEN "Not Hispanic or Latino"
    ###     WHEN "Hispanic/Latino" THEN "Hispanic or Latino"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### //LLCHD
    ### ELSEIF NOT ISNULL([Mob Ethnicity]) THEN CASE [Mob Ethnicity]
    ###     WHEN "HISPANIC/LATINO" THEN "Hispanic or Latino" 
    ###     WHEN "Hispanic" THEN "Hispanic or Latino"
    ###     WHEN "NON-Hispanic" THEN "Not Hispanic or Latino"
    ###     WHEN "NOT HISPANIC/LATINO" THEN "Not Hispanic or Latino"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE "Unknown/Did Not Report"
    ### END
df4_edits1['_T06 MOB Ethnicity'] = df4_edits1.apply(func=fn_T06_MOB_Ethnicity, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T06 MOB Ethnicity']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T06_FOB_Ethnicity(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            if pd.isna(fdf['Fob Ethnicity']):
                return "Unknown/Did Not Report"
            else: 
                match fdf['Fob Ethnicity'].lower():
                    case "hispanic/latino":
                        return "Hispanic or Latino"
                    case "non hispanic/latino":
                        return "Not Hispanic or Latino" 
                    case "unknown":
                        return "Unknown/Did Not Report"
                    case _:
                        return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == "Y"):
            if pd.isna(fdf['Fob Ethnicity1']):
                return "Unknown/Did Not Report"
            else: 
                match fdf['Fob Ethnicity1'].lower():
                    case "hispanic/latino":
                        return "Hispanic or Latino" 
                    case "not hispanic/latino" | "non-hispanic":
                        return "Not Hispanic or Latino"
                    case "unreported/refused to report":
                        return "Unknown/Did Not Report"
                    case _:
                        return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA  
    ###########
    ### /// Tableau Calculation:
    ### IF [Fob Involved] = True //FW
    ### THEN CASE[Fob Ethnicity]
    ###     WHEN "Non Hispanic/Latino" THEN "Not Hispanic or Latino" 
    ###     WHEN "Hispanic/Latino" THEN "Hispanic or Latino"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSEIF [Fob Involved1] = "Y" //LLCHD
    ### THEN CASE[Fob Ethnicity1]
    ###     WHEN "HISPANIC/LATINO" THEN "Hispanic or Latino" 
    ###     WHEN "NOT HISPANIC/LATINO" THEN "Not Hispanic or Latino"
    ###     WHEN  "NON-Hispanic" THEN "Not Hispanic or Latino"
    ###     WHEN "UNREPORTED/REFUSED TO REPORT" THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE NULL
    ### END
df4_edits1['_T06 FOB Ethnicity'] = df4_edits1.apply(func=fn_T06_FOB_Ethnicity, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T06 FOB Ethnicity']) 

#%%###################################

def fn_T06_Ethnicity(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_T06 MOB Ethnicity']
        case "FOB":
            return fdf['_T06 FOB Ethnicity']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_T06 MOB Ethnicity]
    ###     WHEN "FOB" THEN [_T06 FOB Ethnicity]
    ### END
df4_edits1['_T06 Ethnicity'] = df4_edits1.apply(func=fn_T06_Ethnicity, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T06 Ethnicity']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T07_MOB_Race(fdf):
    ###########
    ### FW (FW race variables are boolean).
    ### multiracial.
    if (fdf['source'] == 'FW'):
        if (
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
        elif (False if pd.isna(fdf['MOB Race Asian']) else (True if fdf['MOB Race Asian'] else False)):
            return "Asian"
        elif (False if pd.isna(fdf['MOB Race Black']) else (True if fdf['MOB Race Black'] else False)):
            return "Black or African American"
        elif (False if pd.isna(fdf['MOB Race Hawaiian Pacific']) else (True if fdf['MOB Race Hawaiian Pacific'] else False)):
            return "Native Hawaiian or Other Pacific Islander"
        elif (False if pd.isna(fdf['MOB Race Indian Alaskan']) else (True if fdf['MOB Race Indian Alaskan'] else False)):
            return "American Indian or Alaska Native"
        elif (False if pd.isna(fdf['MOB Race White']) else (True if fdf['MOB Race White'] else False)):
            return "White"
        elif (False if pd.isna(fdf['MOB Race Other']) else (True if fdf['MOB Race Other'] else False)):
            return "Other"
        else:
            return "Unknown/Did Not Report"
    ###########
    ### LLCHD (LL race variables are strings).
    ### multiracial.
    elif (fdf['source'] == 'LL'):
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
        elif (False if pd.isna(fdf['Mob Race Asian']) else (True if fdf['Mob Race Asian']=="Y" else False)):
            return "Asian"
        elif (False if pd.isna(fdf['Mob Race Black']) else (True if fdf['Mob Race Black']=="Y" else False)):
            return "Black or African American"
        elif (False if pd.isna(fdf['Mob Race Hawaiian']) else (True if fdf['Mob Race Hawaiian']=="Y" else False)):
            return "Native Hawaiian or Other Pacific Islander"
        elif (False if pd.isna(fdf['Mob Race Indian']) else (True if fdf['Mob Race Indian']=="Y" else False)):
            return "American Indian or Alaska Native"
        elif (False if pd.isna(fdf['Mob Race White']) else (True if fdf['Mob Race White']=="Y" else False)):
            return "White"
        elif (False if pd.isna(fdf['Mob Race Other']) else (True if fdf['Mob Race Other']=="Y" else False)):
            return "Other"
        else:
            return "Unknown/Did Not Report"
    ###########
    else:
        return "Unknown/Did Not Report"
    ###########
    ### /// Tableau Calculation:
    ### //LLCHD
    ### //multiracial
    ### IF IIF([Mob Race Asian]="Y",1,0,0)+IIF([Mob Race Black]="Y",1,0,0)+IIF([Mob Race Hawaiian]="Y",1,0,0)+IIF([Mob Race Indian]="Y",1,0,0)
    ### +IIF([Mob Race Other]="Y",1,0,0)+IIF([Mob Race White]="Y",1,0,0) > 1 THEN "More than one race"
    ### //single race
    ### ELSEIF [Mob Race Asian] = "Y" THEN "Asian"
    ### ELSEIF [Mob Race Black] = "Y" THEN "Black or African American"
    ### ELSEIF [Mob Race Hawaiian] = "Y" THEN "Native Hawaiian or Other Pacific Islander"
    ### ELSEIF [Mob Race Indian] = "Y" THEN "American Indian or Alaska Native"
    ### ELSEIF [Mob Race White] = "Y" THEN "White"
    ### ELSEIF [Mob Race Other] = "Y" THEN "Other"
    ### ////
    ### //FW
    ### //multiracial, = "True" is not required in IIF statement because race is boolean
    ### ELSEIF IIF([MOB Race Asian],1,0,0)+IIF([MOB Race Black],1,0,0)+IIF([MOB Race Hawaiian Pacific],1,0,0)
    ### +IIF([MOB Race Indian Alaskan],1,0,0)+IIF([MOB Race White],1,0,0) +IIF([MOB Race Other],1,0,0) > 1 
    ### THEN "More than one race"
    ### //single race
    ### ELSEIF [MOB Race Asian] = True THEN "Asian"
    ### ELSEIF [MOB Race Black] = True THEN "Black or African American"
    ### ELSEIF [MOB Race Hawaiian Pacific] = True THEN "Native Hawaiian or Other Pacific Islander"
    ### ELSEIF [MOB Race Indian Alaskan] = True THEN "American Indian or Alaska Native"
    ### ELSEIF [MOB Race White] = True THEN "White"
    ### ELSEIF [MOB Race Other] = True THEN "Other"
    ### ////
    ### ELSE "Unknown/Did Not Report"
    ### END
df4_edits1['_T07 MOB Race'] = df4_edits1.apply(func=fn_T07_MOB_Race, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T07 MOB Race']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T07_FOB_Race(fdf):
    ###########
    ### FW (FW race variables are boolean).
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            ### multiracial.
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
            elif (False if pd.isna(fdf['FOB Race Asian']) else (True if fdf['FOB Race Asian'] else False)):
                return "Asian"
            elif (False if pd.isna(fdf['FOB Race Black']) else (True if fdf['FOB Race Black'] else False)):
                return "Black or African American"
            elif (False if pd.isna(fdf['FOB Race Hawaiian Pacific']) else (True if fdf['FOB Race Hawaiian Pacific'] else False)):
                return "Native Hawaiian or Other Pacific Islander"
            elif (False if pd.isna(fdf['FOB Race Indian Alaskan']) else (True if fdf['FOB Race Indian Alaskan'] else False)):
                return "American Indian or Alaska Native"
            elif (False if pd.isna(fdf['FOB Race White']) else (True if fdf['FOB Race White'] else False)):
                return "White"
            elif (False if pd.isna(fdf['FOB Race Other']) else (True if fdf['FOB Race Other'] else False)):
                return "Other"
            else:
                return "Unknown/Did Not Report"
        else:
            return pd.NA 
    ###########
    ### LLCHD (LL race variables are strings).
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == "Y"):
            ### multiracial.
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
            elif (False if pd.isna(fdf['Fob Race Asian']) else (True if fdf['Fob Race Asian']=="Y" else False)):
                return "Asian"
            elif (False if pd.isna(fdf['Fob Race Black']) else (True if fdf['Fob Race Black']=="Y" else False)):
                return "Black or African American"
            elif (False if pd.isna(fdf['Fob Race Hawaiian']) else (True if fdf['Fob Race Hawaiian']=="Y" else False)):
                return "Native Hawaiian or Other Pacific Islander"
            elif (False if pd.isna(fdf['Fob Race Indian']) else (True if fdf['Fob Race Indian']=="Y" else False)):
                return "American Indian or Alaska Native"
            elif (False if pd.isna(fdf['Fob Race White']) else (True if fdf['Fob Race White']=="Y" else False)):
                return "White"
            elif (False if pd.isna(fdf['Fob Race Other']) else (True if fdf['Fob Race Other']=="Y" else False)):
                return "Other"
            else:
                return "Unknown/Did Not Report"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA ### Different from MOB Race's "Unknown/Did Not Report".
    ###########
    ### /// Tableau Calculation:
    ### //LLCHD
    ### //multiracial
    ### IF [Fob Involved1]= "Y" THEN 
    ###     (IF(IIF([Fob Race Asian] ="Y",1,0,0)+IIF([Fob Race Black] ="Y",1,0,0)+IIF([Fob Race Hawaiian]="Y",1,0,0)
    ###     +IIF([Fob Race Indian]="Y",1,0,0)+IIF([Fob Race Other]="Y",1,0,0)+IIF([Fob Race White]="Y",1,0,0) > 1) THEN "More than one race"
    ###     //single race
    ###     ELSEIF [Fob Race Asian] = "Y" THEN "Asian"
    ###     ELSEIF [Fob Race Black] = "Y" THEN "Black or African American"
    ###     ELSEIF [Fob Race Hawaiian] = "Y" THEN "Native Hawaiian or Other Pacific Islander"
    ###     ELSEIF [Fob Race Indian] = "Y" THEN "American Indian or Alaska Native"
    ###     ELSEIF [Fob Race White] = "Y" THEN "White"
    ###     ELSEIF [Fob Race Other] = "Y" THEN "Other"
    ###     ELSE "Unknown/Did Not Report"
    ###     END)
    ### //FW
    ### //multiracial, = "True" is not required in IIF statement because race is boolean
    ### ELSEIF [Fob Involved]= True THEN 
    ###     (IF(IIF([FOB Race Asian],1,0,0)+IIF([FOB Race Black],1,0,0)+IIF([FOB Race Hawaiian Pacific],1,0,0)
    ###     +IIF([FOB Race Indian Alaskan],1,0,0)+IIF([FOB Race White],1,0,0) +IIF([FOB Race Other],1,0,0) > 1) THEN "More than one race"
    ###     //single race
    ###     ELSEIF [FOB Race Asian] = True THEN "Asian"
    ###     ELSEIF [FOB Race Black] = True THEN "Black or African American"
    ###     ELSEIF [FOB Race Hawaiian Pacific] = True THEN "Native Hawaiian or Other Pacific Islander"
    ###     ELSEIF [FOB Race Indian Alaskan] = True THEN "American Indian or Alaska Native"
    ###     ELSEIF [FOB Race White] = True THEN "White"
    ###     ELSEIF [FOB Race Other] = True THEN "Other"
    ###     ELSE "Unknown/Did Not Report"
    ###     END)
    ### ELSE NULL
    ### END
df4_edits1['_T07 FOB Race'] = df4_edits1.apply(func=fn_T07_FOB_Race, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T07 FOB Race']) 

#%%###################################

def fn_T07_Race(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_T07 MOB Race']
        case "FOB":
            return fdf['_T07 FOB Race']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_T07 MOB Race]
    ###     WHEN "FOB" THEN [_T07 FOB Race]
    ### END
df4_edits1['_T07 Race'] = df4_edits1.apply(func=fn_T07_Race, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T07 Race']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. SIMILAR Tableau Calculations: some different options, could be combined. Python modified.
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
            case "living with partner" | "life partner":
                return "Not Married but Living Together with Partner"
            case "separated" | "legally separated" | "divorced" | "widowed":
                return "Separated, Divorced, or Widowed"
            case "single" | "not married" | "not":
                return "Never Married"
            case "unknown" | "null":
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ###########
    else:
        return "Unknown/Did Not Report"
    ###########
    ### /// Tableau Calculation:
    ### //FW
    ### IF NOT ISNULL([Adult1MaritalStatus]) THEN CASE [Adult1MaritalStatus]
    ###     WHEN "Divorced" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "Living with Partner" THEN "Not Married but Living Together with Partner"
    ###     WHEN "Married" THEN "Married"
    ###     WHEN "Separated" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "Single" THEN "Never Married"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "Widowed" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "Null" THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### //LLCHD
    ### ELSEIF NOT ISNULL([Mob Marital Status]) THEN CASE [Mob Marital Status] 
    ###     WHEN "DIVORCED" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "divorced" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "LEGALLY SEPARATED" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "LIFE PARTNER" THEN "Not Married but Living Together with Partner"
    ###     WHEN "LIVING WITH PARTNER" THEN "Not Married but Living Together with Partner"
    ###     WHEN "MARRIED" THEN "Married"
    ###     WHEN "married" THEN "Married"
    ###     WHEN "NOT MARRIED" THEN "Never Married"
    ###     WHEN "Not" THEN "Never Married"
    ###     WHEN "Not married" THEN "Never Married"
    ###     WHEN "null" THEN "Unknown/Did Not Report"
    ###     WHEN "SEPARATED" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "Separated" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "SINGLE" THEN "Never Married"
    ###     WHEN "UNKNOWN" THEN "Unknown/Did Not Report"
    ###     WHEN "WIDOWED" THEN "Separated, Divorced, or Widowed"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE "Unknown/Did Not Report"
    ### END
df4_edits1['_T08 MOB Marital Status'] = df4_edits1.apply(func=fn_T08_MOB_Marital_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T08 MOB Marital Status']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. SIMILAR Tableau Calculations: some different options, could be combined. Python modified.
def fn_T08_FOB_Marital_Status(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
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
                    case _:
                        return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == "Y"):
            if pd.isna(fdf['Fob Marital Status']):
                return "Unknown/Did Not Report"
            else:
                match fdf['Fob Marital Status'].lower():
                    case "married":
                        return "Married"
                    case "living with partner" | "life partner":
                        return "Not Married but Living Together with Partner"
                    case "separated" | "legally separated" | "divorced" | "widowed":
                        return "Separated, Divorced, or Widowed"
                    case "single" | "not married":
                        return "Never Married"
                    case "unknown" | "null":
                        return "Unknown/Did Not Report"
                    case _:
                        return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA
    ###########
    ### /// Tableau Calculation:
    ### //FW
    ### IF [Fob Involved] = True THEN CASE [Adult2MaritalStatus]
    ###     WHEN "Divorced" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "Living with Partner" THEN "Not Married but Living Together with Partner"
    ###     WHEN "Married" THEN "Married"
    ###     WHEN "Separated" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "Single" THEN "Never Married"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "Widowed" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "Null" THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### //LLCHD
    ### ELSEIF [Fob Involved1] = "Y" THEN CASE [Fob Marital Status] 
    ###     WHEN "DIVORCED" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "LEGALLY SEPARATED" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "LIFE PARTNER" THEN "Not Married but Living Together with Partner"
    ###     WHEN "LIVING WITH PARTNER" THEN " Not Married but Living Toghther with Partner"
    ###     WHEN "MARRIED" THEN "Married"
    ###     WHEN "Married" THEN "Married"
    ###     WHEN "SEPARATED" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "SINGLE" THEN "Never Married"
    ###     WHEN "NOT MARRIED" THEN "Never Married"
    ###     WHEN "Not married" THEN "Never Married"
    ###     WHEN "UNKNOWN" THEN "Unknown/Did Not Report"
    ###     WHEN "WIDOWED" THEN "Separated, Divorced, or Widowed"
    ###     WHEN "NULL" THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE NULL
    ### END
df4_edits1['_T08 FOB Marital Status'] = df4_edits1.apply(func=fn_T08_FOB_Marital_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T08 FOB Marital Status']) 

#%%###################################

def fn_T08_Caregiver_Marital_Status(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_T08 MOB Marital Status']
        case "FOB":
            return fdf['_T08 FOB Marital Status']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_T08 MOB Marital Status]
    ###     WHEN "FOB" THEN [_T08 FOB Marital Status]
    ### END
df4_edits1['_T08 Caregiver Marital Status'] = df4_edits1.apply(func=fn_T08_Caregiver_Marital_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T08 Caregiver Marital Status']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_C15_Min_Educational_Status(fdf):
    if (pd.isna(fdf['Mcafss Edu1']) and pd.isna(fdf['AD1MinEdu'])):
        return "Unknown/Did Not Report"
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Mcafss Edu1']:
            case _ if pd.isna(fdf['Mcafss Edu1']): 
                return "Unrecognized Value" 
            case 1 | 2:
                return "Less than HS diploma" ### "Less than 8th Grade" | "8-11th Grade".
            case 3 | 4:
                return "HS diploma/GED" ### "High School Grad" | "Completed a GED".
            case 5 | 7:
                return "Technical Training or Associates Degree" ### "Vocational School after High School" | "Associates Degree".
            case 6:
                return "Some college/training" ### Some College.
            case 8:
                return "Bachelor's Degree or Higher" ### Bachelors Degree or Higher.
            ### case 9:
            ###     return "HS diploma/GED" ### currently enrolled in college - vocational training or trade apprenticeship.
            ### case 10:
            ###     return "HS diploma/GED" ### currently not enrolled in college - vocational training or trade apprenticeship.
            ### case 11:
            ###     return "Other" ### other education.
            ### case 12:
            ###     return "Unknown/Did Not Report" ### unknown/did not report.
            case 0:
                return "Unknown/Did Not Report" ### unknown/did not report (missing data).
            case _ if int(fdf['Mcafss Edu1']) >= 9:
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ###########
    ### FW.
    elif (fdf['source'] == 'FW'):
        match fdf['AD1MinEdu']:
            case _ if pd.isna(fdf['AD1MinEdu']): 
                return "Unrecognized Value" 
            case "8th Grade or less" | "Some High School":
                return "Less than HS diploma"
            case "GED" | "High School Graduate":
                return "HS diploma/GED"
            case "Achievement Certificate" | "Certificate Program":
                return "Technical Training or Certification"
            case "Some College":
                return "Some college/training"
            case "Associates or Two Year Technical Degree":
                return "Technical Training or Associates Degree" ### these are two separate categories on F1.
            case "Two Year Degree":
                return "Associate's Degree"
            case "Four Year College Degree" | "Graduate School":
                return "Bachelor's Degree or Higher"
            case "Unknown" | "null":
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ###########
    else:
        return "Unrecognized Value"
    ###########
    ### /// Tableau Calculation:
    ### //LLCHD
    ### IF [Mcafss Edu1] = 1 THEN "Less than HS diploma" // Less than 8th Grade
    ### ELSEIF [Mcafss Edu1] = 2 THEN "Less than HS diploma" // 8-11th Grade
    ### ELSEIF [Mcafss Edu1] = 3 THEN "HS diploma/GED" // High School Grad
    ### ELSEIF [Mcafss Edu1] = 4 THEN "HS diploma/GED" //Completed a GED
    ### ELSEIF [Mcafss Edu1] = 5 THEN "Technical Training or Associates Degree" // Vocational School after High School
    ### ELSEIF [Mcafss Edu1] = 6 THEN "Some college/training" // Some College
    ### ELSEIF [Mcafss Edu1] = 7 THEN "Technical Training or Associates Degree" // Associates Degree
    ### ELSEIF [Mcafss Edu1] = 8 THEN "Bachelor's Degree or Higher"  // Bachelors Degree or Higher
    ### // ELSEIF [Mcafss Edu1] = 9 THEN "HS diploma/GED"
    ### // ELSEIF [Mcafss Edu1] = 10 THEN "HS diploma/GED" 
    ### // ELSEIF [Mcafss Edu1] = 11 THEN "Other"
    ### // ELSEIF [Mcafss Edu1] = 12 THEN "Unknown/Did Not Report"
    ### ELSEIF [Mcafss Edu1] = 0 THEN "Unknown/Did Not Report"
    ### ELSEIF [Mcafss Edu1] >= 9 THEN "Unknown/Did Not Report"
    ### ////
    ### ELSEIF [AD1MinEdu] = "8th Grade or less" THEN "Less than HS diploma"
    ### ELSEIF [AD1MinEdu] = "Some High School" THEN "Less than HS diploma"
    ### ELSEIF [AD1MinEdu] = "GED" THEN "HS diploma/GED"
    ### ELSEIF [AD1MinEdu] = "High School Graduate" THEN "HS diploma/GED"
    ### ELSEIF [AD1MinEdu] = "Achievement Certificate" THEN "Technical Training or Certification"
    ### ELSEIF [AD1MinEdu] = "Certificate Program" THEN "Technical Training or Certification"
    ### ELSEIF [AD1MinEdu] = "Some College" THEN "Some college/training"
    ### ELSEIF [AD1MinEdu] = "Associates or Two Year Technical Degree" THEN "Technical Training or Associates Degree" //these are two serparate categories on F1
    ### ELSEIF [AD1MinEdu] = "Two Year Degree" THEN "Associate's Degree"
    ### ELSEIF [AD1MinEdu] = "Four Year College Degree" THEN "Bachelor's Degree or Higher"
    ### ELSEIF [AD1MinEdu] = "Graduate School" THEN "Bachelor's Degree or Higher"
    ### ELSEIF [AD1MinEdu] = "Unknown" THEN "Unknown/Did Not Report"
    ### ELSEIF [AD1MinEdu] = "null"  THEN  "Unknown/Did Not Report"
    ### ////
    ### ELSEIF ISNULL([Mcafss Edu1])AND ISNULL([AD1MinEdu]) THEN "Unknown/Did Not Report"
    ### ELSE "Unrecognized Value"
    ### END
    ### //LLCHD Code from Kodi on 11/30/2021
    ### //1 – Less than 8th Grade
    ### //2 – 8-11th Grade
    ### //3 – High School Grad
    ### //4 - Completed a GED
    ### //5 - Vocational School after High School
    ### //6 – Some College
    ### //7 – Associates Degree 
    ### //8 - Bachelors Degree or Higher
    ### //Confirmed 9-12 are old and no longer needed - new LLCHD variables are sent to confirm enrollment
df4_edits1['_C15 Min Educational Status'] = df4_edits1.apply(func=fn_C15_Min_Educational_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C15 Min Educational Status']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_C15_Max_Educational_Status(fdf):
    if (pd.isna(fdf['Mcafss Edu2']) and pd.isna(fdf['AD1MaxEdu'])):
        return "Unknown/Did Not Report"
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Mcafss Edu2']:
            case _ if pd.isna(fdf['Mcafss Edu2']): 
                return "Unrecognized Value" 
            case 1 | 2:
                return "Less than HS diploma" ### "Less than 8th Grade" | "8-11th Grade".
            case 3 | 4:
                return "HS diploma/GED" ### "High School Grad" | "Completed a GED".
            case 5 | 7:
                return "Technical Training or Associates Degree" ### "Vocational School after High School" | "Associates Degree".
            case 6:
                return "Some college/training" ### Some College.
            case 8:
                return "Bachelor's Degree or Higher" ### Bachelors Degree or Higher.
            ### case 9:
            ###     return "HS diploma/GED" ### currently enrolled in college - vocational training or trade apprenticeship.
            ### case 10:
            ###     return "HS diploma/GED" ### currently not enrolled in college - vocational training or trade apprenticeship.
            ### case 11:
            ###     return "Other" ### other education.
            ### case 12:
            ###     return "Unknown/Did Not Report" ### unknown/did not report.
            case 0:
                return "Unknown/Did Not Report" ### unknown/did not report (missing data).
            case _ if int(fdf['Mcafss Edu2']) >= 9:
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ###########
    ### FW.
    elif (fdf['source'] == 'FW'):
        match fdf['AD1MaxEdu']:
            case _ if pd.isna(fdf['AD1MaxEdu']): 
                return "Unrecognized Value" 
            case "8th Grade or less" | "Some High School":
                return "Less than HS diploma"
            case "GED" | "High School Graduate":
                return "HS diploma/GED"
            case "Achievement Certificate" | "Certificate Program":
                return "Technical Training or Certification"
            case "Some College":
                return "Some college/training"
            case "Associates or Two Year Technical Degree":
                return "Technical Training or Associates Degree" ### these are two separate categories on F1.
            case "Two Year Degree":
                return "Associate's Degree"
            case "Four Year College Degree" | "Graduate School":
                return "Bachelor's Degree or Higher"
            case "Unknown" | "null":
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ###########
    else:
        return "Unrecognized Value"
    ###########
    ### /// Tableau Calculation:
    ### //LLCHD
    ### IF [Mcafss Edu2] = 1 THEN "Less than HS diploma" // Less than 8th Grade
    ### ELSEIF [Mcafss Edu2] = 2 THEN "Less than HS diploma" // 8-11th Grade
    ### ELSEIF [Mcafss Edu2] = 3 THEN "HS diploma/GED" // High School Grad
    ### ELSEIF [Mcafss Edu2] = 4 THEN "HS diploma/GED" //Completed a GED
    ### ELSEIF [Mcafss Edu2] = 5 THEN "Technical Training or Associates Degree" // Vocational School after High School
    ### ELSEIF [Mcafss Edu2] = 6 THEN "Some college/training" // Some College
    ### ELSEIF [Mcafss Edu2] = 7 THEN "Technical Training or Associates Degree" // Associates Degree
    ### ELSEIF [Mcafss Edu2] = 8 THEN "Bachelor's Degree or Higher"  // Bachelors Degree or Higher
    ### // ELSEIF [Mcafss Edu2] = 9 THEN "HS diploma/GED" //currently enrolled in college - vocational training or trade apprenticeship
    ### // ELSEIF [Mcafss Edu2] = 10 THEN "HS diploma/GED"  //currently not enrolled in college - vocational training or trade apprenticeship
    ### // ELSEIF [Mcafss Edu2] = 11 THEN "Other" //other education
    ### // ELSEIF [Mcafss Edu2] = 12 THEN "Unknown/Did Not Report" //unknown/did not report
    ### ELSEIF [Mcafss Edu2] = 0 THEN "Unknown/Did Not Report" //unknown/did not report (missing data)
    ### ELSEIF [Mcafss Edu2] >= 9 THEN "Unknown/Did Not Report" 
    ### //FW
    ### ELSEIF [AD1MaxEdu] = "8th Grade or less" THEN "Less than HS diploma"
    ### ELSEIF [AD1MaxEdu] = "Some High School" THEN "Less than HS diploma"
    ### ELSEIF [AD1MaxEdu] = "GED" THEN "HS diploma/GED"
    ### ELSEIF [AD1MaxEdu] = "High School Graduate" THEN "HS diploma/GED"
    ### ELSEIF [AD1MaxEdu] = "Achievement Certificate" THEN "Technical Training or Certification"
    ### ELSEIF [AD1MaxEdu] = "Certificate Program" THEN "Technical Training or Certification"
    ### ELSEIF [AD1MaxEdu] = "Some College" THEN "Some college/training"
    ### ELSEIF [AD1MaxEdu] = "Associates or Two Year Technical Degree" THEN "Technical Training or Associates Degree" //these are two serparate categories on F1
    ### ELSEIF [AD1MaxEdu] = "Two Year Degree" THEN "Associate's Degree"
    ### ELSEIF [AD1MaxEdu] = "Four Year College Degree" THEN "Bachelor's Degree or Higher"
    ### ELSEIF [AD1MaxEdu] = "Graduate School" THEN "Bachelor's Degree or Higher"
    ### ELSEIF [AD1MaxEdu] = "Unknown" THEN "Unknown/Did Not Report"
    ### ELSEIF [AD1MaxEdu] = "null" THEN "Unknown/Did Not Report"
    ### ELSEIF ISNULL([Mcafss Edu2]) AND ISNULL([AD1MaxEdu]) THEN "Unknown/Did Not Report"
    ### ELSE "Unrecognized Value"
    ### END
    ### //LLCHD Code from Kodi on 11/30/2021
    ### //1 – Less than 8th Grade
    ### //2 – 8-11th Grade
    ### //3 – High School Grad
    ### //4 - Completed a GED
    ### //5 - Vocational School after High School
    ### //6 – Some College
    ### //7 – Associates Degree 
    ### //8 - Bachelors Degree or Higher
    ### //Confirmed 9-12 are old and no longer needed - new LLCHD variables are sent to confirm enrollment
df4_edits1['_C15 Max Educational Status'] = df4_edits1.apply(func=fn_C15_Max_Educational_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C15 Max Educational Status']) 

#%%###################################

### TODO: Fix code because 'Fob Edu' is string & not int as expected.
def fn_T09_FOB_Education_Status(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            match fdf['AD2EDLevel']:
                case _ if pd.isna(fdf['AD2EDLevel']):
                    return "Unknown/Did Not Report"
                case "8th Grade or less" | "Some High School":
                    return "Less than HS diploma"
                case "GED" | "High School Graduate":
                    return "HS diploma/GED"
                case "Achievement Certificate":
                    return "Some college/training" ### is this the right category?
                case "Certificate Program":
                    return "Some college/training" ### is this the right category?
                case "Some College":
                    return "Some college/training"
                case "Associates or Two Year Technical Degree" | "Two Year Degree":
                    return "Technical Training or Associates Degree" ### these are two serparate categories on F1.
                case "Four Year College Degree" | "Graduate School":
                    return "Bachelor's Degree or Higher"
                case "Unknown":
                    return "Unknown/Did Not Report"
                case _:
                    return pd.NA 
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == "Y"):
            match fdf['Fob Edu']:
                case _ if pd.isna(fdf['Fob Edu']):
                    return "Unknown/Did Not Report"
                case 1 | 2:
                    return "Less than HS diploma"
                case 3 | 4:
                    return "HS diploma/GED"
                case 5:
                    return "Vocational School after High School"
                case 6:
                    return "Some college/training"
                case 7:
                    return "Associates Degree" ### these are two separate categories on F1.
                case 8:
                    return "Bachelor's Degree or Higher"
                case 0:
                    return "Unknow/Did Not Report" ### TODO: After compare, fix spelling "Unknown".
                case _:
                    return "Unknown/Did Not Report" ### TODO: Should be pd.NA per Tableau syntax (but that should probably be "Unrecognized Value"); set to "Unknown" because in Tableau 'Fob Edu' read in as all NA.
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### //FW
    ### IF [Fob Involved]= True THEN CASE [AD2EDLevel] 
    ###     WHEN "8th Grade or less" THEN "Less than HS diploma"
    ###     WHEN "Some High School" THEN "Less than HS diploma"
    ###     WHEN "GED" THEN "HS diploma/GED"
    ###     WHEN "High School Graduate" THEN "HS diploma/GED"
    ###     WHEN "Achievement Certificate" THEN "Some college/training" //is this the right category?
    ###     WHEN "Certificate Program" THEN "Some college/training" //is this the right category?
    ###     WHEN "Some College" THEN "Some college/training"
    ###     WHEN "Associates or Two Year Technical Degree" THEN "Technical Training or Associates Degree" //these are two serparate categories on F1
    ###     WHEN "Two Year Degree" THEN "Technical Training or Associates Degree" //these are two serparate categories on F1
    ###     WHEN "Four Year College Degree" THEN "Bachelor's Degree or Higher"
    ###     WHEN "Graduate School" THEN "Bachelor's Degree or Higher"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     END 
    ### //LLCHD
    ### ELSEIF [Fob Involved1]= "Y" THEN CASE [Fob Edu] 
    ###     WHEN 01 THEN "Less than HS diploma"
    ###     WHEN 02 THEN "Less than HS diploma"
    ###     WHEN 03 THEN "HS diploma/GED"
    ###     WHEN 04 THEN "HS diploma/GED"
    ###     WHEN 05 THEN "Vocational School after High School"
    ###     WHEN 06 THEN "Some college/training"
    ###     WHEN 07 THEN "Associates Degree" //these are two serparate categories on F1
    ###     WHEN 08 THEN "Bachelor's Degree or Higher"
    ###     WHEN 00 THEN "Unknow/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     END
    ### ////
    ### ELSE NULL
    ### END
    ### //LLCHD Code from Kodi on 11/30/2021
    ### //1 – Less than 8th Grade
    ### //2 – 8-11th Grade
    ### //3 – High School Grad
    ### //4 - Completed a GED
    ### //5 - Vocational School after High School
    ### //6 – Some College
    ### //7 – Associates Degree 
    ### //8 - Bachelors Degree or Higher
df4_edits1['_T09 FOB Education Status'] = df4_edits1.apply(func=fn_T09_FOB_Education_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T09 FOB Education Status']) 
# #%%
# inspect_col(df4_edits1['Fob Edu']) ### Is string NOT integer. TODO: Read in as string & fix this code.
#%%
print(df4_edits1[['source', '_T09 FOB Education Status', 'Fob Involved', 'AD2EDLevel', 'Fob Involved1', 'Fob Edu']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string()) 

#%%###################################

def fn_T09_Caregiver_Education_Status(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_C15 Max Educational Status']
        case "FOB":
            return fdf['_T09 FOB Education Status']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_C15 Max Educational Status]
    ###     WHEN "FOB" THEN [_T09 FOB Education Status]
    ### END
df4_edits1['_T09 Caregiver Education Status'] = df4_edits1.apply(func=fn_T09_Caregiver_Education_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T09 Caregiver Education Status']) 
### TODO: standardize "Educational Status" & "Education Status" variable names.

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_C15_Min_Educational_Enrollment(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Min Edu Enroll']:
            case _ if pd.isna(fdf['Min Edu Enroll']):
                return "Unknown/Did not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
            case "College 2 Year" | "College 4 Year" | "ESL" | "Graduate School" | "Vocational College":
                return "Student/trainee"
            case "GED Program" | "High/Middle School":
                return "Student/trainee HS/GED"
            case "Not Enrolled in School":
                return "Not a student/trainee"
            case "Unknown":
                return "Unknown/Did not Report"
            case _:
                return "Unknown/Did not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if (
            pd.isna(fdf['mcafss_edu1_enroll']) or
            (fdf['mcafss_edu1_enroll'] == "YES" and pd.isna(fdf['mcafss_edu1_prog'])) ### TODO: After comparison, change to 'Y'.
        ):
            return "Unknown/Did not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
        elif (
            fdf['mcafss_edu1_enroll'] == "YES" ### Enrolled. ### TODO: After comparison, change to 'Y'.
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
        elif (fdf['mcafss_edu1_enroll'] == "NO"): ### TODO: After comparison, change to 'N'.
            return "Student/trainee" ### Only difference from '_C15 Max Educational Enrollment' (other than diff vars).
        else:
            return "Unknown/Did not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return "Unknown/Did not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### //FW
    ### IF [Min Edu Enroll] = "College 2 Year" THEN "Student/trainee" 
    ### ELSEIF [Min Edu Enroll] = "College 4 Year" THEN "Student/trainee"
    ### ELSEIF [Min Edu Enroll] = "ESL" THEN "Student/trainee"
    ### ELSEIF [Min Edu Enroll] = "GED Program" THEN "Student/trainee HS/GED"
    ### ELSEIF [Min Edu Enroll] = "Graduate School" THEN "Student/trainee"
    ### ELSEIF [Min Edu Enroll] = "High/Middle School" THEN "Student/trainee HS/GED"
    ### ELSEIF [Min Edu Enroll] = "Not Enrolled in School" THEN "Not a student/trainee"
    ### ELSEIF [Min Edu Enroll] = "Unknown" THEN "Unknown/Did not Report"
    ### ELSEIF [Min Edu Enroll] = "Vocational College" THEN "Student/trainee"
    ### //LLCHD
    ### ELSEIF ([mcafss_edu1_enroll] = "YES" // Enrolled
    ###         AND
    ###         ([mcafss_edu1_prog] = 1 // Enrolled in Middle School
    ###         OR
    ###         [mcafss_edu1_prog] = 2 // Enrolled in High School
    ###         OR
    ###         [mcafss_edu1_prog] = 3 // Enrolled in GED
    ###         )) THEN "Student/trainee HS/GED" 
    ### ELSEIF [mcafss_edu1_enroll] = "NO" THEN "Student/trainee"
    ### ELSE "Unknown/Did not Report"
    ### END
    ### //Student/trainee indicates enrollment in a program other than a high school diploma or GED
    ### //LLCHD - Kodi sent this coding for mcafss_edu1_prog on 12/7/2021
    ### //01 = Middle School
    ### //02 = High School
    ### //03 = GED
    ### //04 = ESL
    ### //05 = Adult education in basic reading or math
    ### //06 = College
    ### //07 = Vocational training, technical or trade school (excluding training received during HS)
    ### //08 = Job search or job placement
    ### //09 = Work experience
    ### //10 = Other (Specify)
df4_edits1['_C15 Min Educational Enrollment'] = df4_edits1.apply(func=fn_C15_Min_Educational_Enrollment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C15 Min Educational Enrollment']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_C15_Max_Educational_Enrollment(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Max Edu Enroll']:
            case _ if pd.isna(fdf['Max Edu Enroll']):
                return "Unknown/Did not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
            case "College 2 Year" | "College 4 Year" | "ESL" | "Graduate School" | "Vocational College":
                return "Student/trainee"
            case "GED Program" | "High/Middle School":
                return "Student/trainee HS/GED"
            case "Not Enrolled in School":
                return "Not a student/trainee"
            case "Unknown":
                return "Unknown/Did not Report"
            case _:
                return "Unknown/Did not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if (
            pd.isna(fdf['mcafss_edu2_enroll']) or
            (fdf['mcafss_edu2_enroll'] == "YES" and pd.isna(fdf['mcafss_edu2_prog'])) ### TODO: After comparison, change to 'Y'.
        ):
            return "Unknown/Did not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
        elif (
            fdf['mcafss_edu2_enroll'] == "YES" ### Enrolled. ### TODO: After comparison, change to 'Y'.
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
        elif (fdf['mcafss_edu2_enroll'] == "NO"): ### TODO: After comparison, change to 'N'.
            return "Not a student/trainee" ### Only difference from '_C15 Min Educational Enrollment' (other than diff vars).
        else:
            return "Unknown/Did not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    else:
        return "Unknown/Did not Report" ### TODO: After comparison, Maybe change to "Unrecognized Value"?
    ###########
    ### /// Tableau Calculation:
    ### IF [Max Edu Enroll] = "College 2 Year" THEN "Student/trainee" //FW
    ### ELSEIF [Max Edu Enroll] = "College 4 Year" THEN "Student/trainee"
    ### ELSEIF [Max Edu Enroll] = "ESL" THEN "Student/trainee"
    ### ELSEIF [Max Edu Enroll] = "GED Program" THEN "Student/trainee HS/GED"
    ### ELSEIF [Max Edu Enroll] = "Graduate School" THEN "Student/trainee"
    ### ELSEIF [Max Edu Enroll] = "High/Middle School" THEN "Student/trainee HS/GED"
    ### ELSEIF [Max Edu Enroll] = "Not Enrolled in School" THEN "Not a student/trainee"
    ### ELSEIF [Max Edu Enroll] = "Unknown" THEN "Unknown/Did not Report"
    ### ELSEIF [Max Edu Enroll] = "Vocational College" THEN "Student/trainee"
    ### ////
    ### ELSEIF ([mcafss_edu2_enroll] = "YES" // LLCHD Enrolled
    ###         AND
    ###         ([mcafss_edu2_prog] = 1 // Enrolled in Middle School
    ###         OR
    ###         [mcafss_edu2_prog] = 2 // Enrolled in High School
    ###         OR
    ###         [mcafss_edu2_prog] = 3 // Enrolled in GED
    ###         )) THEN "Student/trainee HS/GED" //LLCHD
    ### ELSEIF [mcafss_edu2_enroll] = "NO" THEN "Not a student/trainee"
    ### ELSE "Unknown/Did not Report"
    ### END
    ### //Student/trainee indicates enrollment in a program other than a high school diploma or GED
    ### //LLCHD - Kodi sent this coding for mcafss_edu2_prog on 12/7/2021
    ### //01 = Middle School
    ### //02 = High School
    ### //03 = GED
    ### //04 = ESL
    ### //05 = Adult education in basic reading or math
    ### //06 = College
    ### //07 = Vocational training, technical or trade school (excluding training received during HS)
    ### //08 = Job search or job placement
    ### //09 = Work experience
    ### //10 = Other (Specify)
df4_edits1['_C15 Max Educational Enrollment'] = df4_edits1.apply(func=fn_C15_Max_Educational_Enrollment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C15 Max Educational Enrollment']) 
# #%%
# inspect_col(df4_edits1['Max Edu Enroll']) 
# #%%
# inspect_col(df4_edits1['mcafss_edu2_enroll']) 
# #%%
# inspect_col(df4_edits1['mcafss_edu2_prog']) 
#%%
df4_edits1[['source', '_C15 Max Educational Enrollment', 'Max Edu Enroll', 'mcafss_edu2_enroll', 'mcafss_edu2_prog']].drop_duplicates(ignore_index=True)

#%%###################################

def fn_T10_FOB_Educational_Enrollment(fdf):
    ### max.
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            match fdf['AD2InSchool']:
                case _ if pd.isna(fdf['AD2InSchool']):
                    return "Unknown/Did Not Report"
                case "College 2 Year" | "College 4 Year" | "ESL" | "GED Program" | "Graduate School" | "High/Middle School" | "Vocational College":
                    return "Student/trainee"
                case "Not Enrolled in School":
                    return "Not a student/trainee"
                case "Unknown":
                    return "Unknown/Did Not Report"
                case _:
                    return pd.NA 
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == "Y"):
            return "Unknown/Did Not Report" ### TODO: Fix after comparison. All "Unknown" bceause 'Fob Edu' reading in as ALL NULL in Tableau.
            # match fdf['Fob Edu']:
            #     case _ if pd.isna(fdf['Fob Edu']):
            #         return "Unknown/Did Not Report"
            #     ### case 1 | 9:
            #     ###     return "Student/trainee"
            #     ### case 2 | 3 | 4 | 5 | 6 | 7 | 8 | 10 | 11:
            #     ###     return "Not a student/trainee"
            #     ### case 12:
            #     ###     return "Unknown/Did Not Report"
            #     case _:
            #         return pd.NA 
        else:
            return pd.NA 
    ### TODO: Older note: Need an FOB enrollment prog from LLCHD.
    ### TODO: Ask why cases 1-12 for LL commented out?
    ### TODO: Fix logic because 'Fob Edu' is text not numbers. That's probably why.
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### //max
    ### //FW
    ### IF [Fob Involved] = True THEN CASE[AD2InSchool] 
    ###     WHEN "College 2 Year" THEN "Student/trainee" 
    ###     WHEN "College 4 Year" THEN "Student/trainee"
    ###     WHEN "ESL" THEN "Student/trainee"
    ###     WHEN "GED Program" THEN "Student/trainee" 
    ###     WHEN "Graduate School" THEN "Student/trainee"
    ###     WHEN "High/Middle School" THEN "Student/trainee" 
    ###     WHEN "Not Enrolled in School" THEN "Not a student/trainee"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "Vocational College" THEN "Student/trainee"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     END
    ### //LLCHD
    ### ELSEIF [Fob Involved1] = "Y" THEN CASE[Fob Edu] 
    ### //    WHEN 1 THEN "Student/trainee"
    ### //    WHEN 2 THEN "Not a student/trainee"
    ### //    WHEN 3 THEN "Not a student/trainee"
    ### //    WHEN 4 THEN "Not a student/trainee"
    ### //    WHEN 5 THEN "Not a student/trainee"
    ### //    WHEN 6 THEN "Not a student/trainee"
    ### //    WHEN 7 THEN "Not a student/trainee"
    ### //    WHEN 8 THEN "Not a student/trainee"
    ### //    WHEN 9 THEN "Student/trainee"
    ### //    WHEN 10 THEN "Not a student/trainee"
    ### //    WHEN 11 THEN "Not a student/trainee"
    ### //    WHEN 12 THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     END
    ### ////
    ### ELSE NULL
    ### END
    ### //Need an FOB enrollment prog from LLCHD
df4_edits1['_T10 FOB Educational Enrollment'] = df4_edits1.apply(func=fn_T10_FOB_Educational_Enrollment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T10 FOB Educational Enrollment']) 
#%%
print(df4_edits1[['source', '_T10 FOB Educational Enrollment', 'Fob Involved', 'AD2InSchool', 'Fob Involved1', 'Fob Edu']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string()) 

#%%###################################

def fn_T10_Caregiver_Educational_Enrollment(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_C15 Max Educational Enrollment']
        case "FOB":
            return fdf['_T10 FOB Educational Enrollment']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_C15 Max Educational Enrollment]
    ###     WHEN "FOB" THEN [_T10 FOB Educational Enrollment]
    ### END
df4_edits1['_T10 Caregiver Educational Enrollment'] = df4_edits1.apply(func=fn_T10_Caregiver_Educational_Enrollment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T10 Caregiver Educational Enrollment']) 
### TODO: Standardize "Unknown/Did not Report" vs "Unknown/Did Not Report".

#%%###################################

### In Adult3-Form2 & Adult4-Form1. SIMILAR Tableau Calculations: some different options, could be combined. Python modified.
def fn_T11_MOB_Employment(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if (pd.isna(fdf['AD1EmpStatus'])):
            return "Unknown/Did Not Report"
        else:
            match fdf['AD1EmpStatus'].lower():
                case "employed full time" | "maternal leave, paid, full time" | "maternal leave, unpaid, full time":
                    return "Employed Full Time"
                case "employed part time" | "maternal leave, unpaid, part time" | "self-employed":
                    return "Employed Part Time"
                case (
                    "permanent disability" |
                    "temporary disability" |
                    "unemployed - unspecified" |
                    "unemployed not seeking work-barriers" |
                    "unemployed not seeking work-preference" |
                    "unemployed not seeking work-teen caregiver" |
                    "unemployed seeking work"
                ):
                    return "Not Employed"
                case "unknown" | "null":
                    return "Unknown/Did Not Report"
                case _:
                    return "Unrecognized Value"
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if (pd.isna(fdf['Mcafss Employ'])):
            return "Unknown/Did Not Report"
        else:
            match fdf['Mcafss Employ']:
                case 0 | 1 | 2:
                    return "Not Employed"
                case 3 | 4 | 5:
                    return "Employed Full Time"
                case _:
                    return "Unrecognized Value"
    ###########
    else:
        return "Unknown/Did Not Report"
    ###########
    ### /// Tableau Calculation:
    ### IF NOT ISNULL([AD1EmpStatus]) THEN CASE [AD1EmpStatus] //FW
    ###     WHEN "Employed Full Time" THEN "Employed Full Time"
    ###     WHEN "Employed Part Time" THEN "Employed Part Time"
    ###     WHEN "Maternal leave, paid, full time" THEN "Employed Full Time"
    ###     WHEN "Maternal leave, unpaid, full time" THEN "Employed Full Time"
    ###     WHEN "Maternal leave, unpaid, part time" THEN "Employed Part Time"
    ###     WHEN "Null" THEN "Unknown/Did Not Report" /// Not in Adult3 Form2.
    ###     WHEN "null" THEN "Unknown/Did Not Report" /// Not in Adult3 Form2.
    ###     WHEN "Permanent Disability" THEN "Not Employed"
    ###     WHEN "Self-Employed" THEN "Employed Part Time"
    ###     WHEN "Temporary Disability" THEN "Not Employed"
    ###     WHEN "Unemployed - Unspecified" THEN "Not Employed"
    ###     WHEN "Unemployed Not Seeking Work-Barriers" THEN "Not Employed"
    ###     WHEN "Unemployed Not Seeking Work-Preference" THEN "Not Employed"
    ###     WHEN "Unemployed Not Seeking Work-Teen Caregiver" THEN "Not Employed"
    ###     WHEN "Unemployed Seeking Work" THEN "Not Employed"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSEIF NOT ISNULL([Mcafss Employ])THEN CASE [Mcafss Employ] //LLCHD
    ###     WHEN 0 THEN "Not Employed" /// Not in Adult3 Form2.
    ###     WHEN 1 THEN "Not Employed"
    ###     WHEN 2 THEN "Not Employed"
    ###     WHEN 3 THEN "Employed Part Time"
    ###     WHEN 4 THEN "Employed Full Time"
    ###     WHEN 5 THEN "Employed Full Time"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE "Unknown/Did Not Report"
    ### END
df4_edits1['_T11 MOB Employment'] = df4_edits1.apply(func=fn_T11_MOB_Employment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T11 MOB Employment']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T11_FOB_Employment(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            if pd.isna(fdf['AD2EmployStatus']):
                return "Unknown/Did Not Report"
            else:
                match fdf['AD2EmployStatus'].lower():
                    case "employed full time" | "maternal leave, paid, full time" | "maternal leave, unpaid, full time":
                        return "Employed Full Time"
                    case "employed part time" | "maternal leave, unpaid, part time" | "self-employed":
                        return "Employed Part Time"
                    case (
                        "permanent disability" |
                        ### "temporary disability" | ### TODO: Not in Tableau code, but should be. Add back in after comparison.
                        "unemployed - unspecified" |
                        "unemployed not seeking work-barriers" |
                        "unemployed not seeking work-preference" |
                        "unemployed not seeking work-teen caregiver" |
                        "unemployed seeking work"
                    ):
                        return "Not Employed"
                    case "unknown" | "null":
                        return "Unknown/Did Not Report"
                    case _:
                        return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == "Y"):
            if pd.isna(fdf['Fob Employ']):
                return "Unknown/Did Not Report"
            else:
                match fdf['Fob Employ']:
                    case 1 | 2:
                        return "Not Employed"
                    case 3:
                        return "Employed Part Time"
                    case 4 | 5:
                        return "Employed Full Time"
                    case _:
                        return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### IF [Fob Involved] = True THEN CASE [AD2EmployStatus] //FW
    ###     WHEN "Employed Full Time" THEN "Employed Full Time"
    ###     WHEN "Employed Part Time" THEN "Employed Part Time"
    ###     WHEN "Maternal leave, paid, full time" THEN "Employed Full Time"
    ###     WHEN "Maternal leave, unpaid, full time" THEN "Employed Full Time"
    ###     WHEN "Maternal leave, unpaid, part time" THEN "Employed Part Time"
    ###     WHEN "Permanent Disability" THEN "Not Employed"
    ###     WHEN "Self-Employed" THEN "Employed Part Time"
    ###     WHEN "Unemployed - Unspecified" THEN "Not Employed"
    ###     WHEN "Unemployed Not Seeking Work-Barriers" THEN "Not Employed"
    ###     WHEN "Unemployed Not Seeking Work-Preference" THEN "Not Employed"
    ###     WHEN "Unemployed Not Seeking Work-Teen Caregiver" THEN "Not Employed"
    ###     WHEN "Unemployed Seeking Work" THEN "Not Employed"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "null" THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSEIF [Fob Involved1] = "Y" THEN CASE [Fob Employ] //LLCHD
    ###     WHEN 1 THEN "Not Employed"
    ###     WHEN 2 THEN "Not Employed"
    ###     WHEN 3 THEN "Employed Part Time"
    ###     WHEN 4 THEN "Employed Full Time"
    ###     WHEN 5 THEN "Employed Full Time"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE NULL
    ### END
df4_edits1['_T11 FOB Employment'] = df4_edits1.apply(func=fn_T11_FOB_Employment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T11 FOB Employment']) 
#%%
print(df4_edits1[['source', '_T11 FOB Employment', 'Fob Involved', 'AD2EmployStatus', 'Fob Involved1', 'Fob Employ']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string()) 

#%%###################################

def fn_T11_Caregiver_Employment(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_T11 MOB Employment']
        case "FOB":
            return fdf['_T11 FOB Employment']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_T11 MOB Employment]
    ###     WHEN "FOB" THEN [_T11 FOB Employment]
    ### END
df4_edits1['_T11 Caregiver Employment'] = df4_edits1.apply(func=fn_T11_Caregiver_Employment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T11 Caregiver Employment']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T12_MOB_Housing_Status(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] != "ll"):
            if pd.isna(fdf['Housing Status']):
                return "Unknown/Did Not Report"
            else:
                match fdf['Housing Status'].lower():
                    case "owns or shares own home, condominium, or apartment":
                        return "Owns or shares own home, condominium, or apartment"
                    case (
                        "rents of shares own home or apartment" | 
                        "rents or shares own home or apartment"
                    ):
                        return "Rents or shares own home or apartment"
                    case "lives with parent or family member":
                        return "Lives with parent or family member"
                    case "live in public housing":
                        return "Lives in public housing"
                    case "homeless and sharing housing":
                        return "Homeless and sharing housing"
                    case "homeless and living in an emergency or transitional shelter":
                        return "Homeless and living in an emergency or transition shelter" ### Homeless and living in emergency or transitional shelter.
                    case "some other arrangement":
                        return "Some other arrangement"
                    case "other":
                        return "Some other arrangement" ### TODO: Check old comment: Not sure this is the right category.
                    case _:
                        return "Unrecognized Value" ### TODO: Check old comment: will have to add new FW values as they come in, they aren't all here.
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] == "ll"):
            match fdf['Mob Living Arrangement']:
                case _ if pd.isna(fdf['Mob Living Arrangement']):
                    return "Unknown/Did Not Report"
                case 1:
                    return "Owns or shares own home, condominium, or apartment" ### Owns or shared own home, condo, or apartment.
                case 2:
                    return "Rents or shares own home or apartment" ### Rents or shared own home or apartment.
                case 3:
                    return "Lives in public housing" ### Lives in public housing.
                case 4:
                    return "Lives with parent or family member" ### Lives with parent or family member.
                case 5:
                    return "Not homeless, some other arrangement" ### Some other arrangement.
                case 6:
                    return "Homeless and sharing housing" ### Homeless and sharing housing.
                case 7:
                    return "Homeless and living in an emergency or transition shelter" ### Homeless and living in emergency or transitional shelter.
                case 8:
                    return "Homeless, some other arrangement" ### Homeless with some other arrangement.
                case _:
                    return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### IF [_Agency]<> "ll" THEN CASE [Housing Status] //FW
    ###     WHEN "Homeless and living in an emergency or transitional shelter" THEN "Homeless and living in an emergency or transition shelter" //Homeless and living in emergency or transitional shelter
    ###     WHEN "Homeless and sharing housing" THEN "Homeless and sharing housing"
    ###     WHEN "Live in public housing" THEN "Lives in public housing"
    ###     WHEN "Lives with parent or family member" THEN "Lives with parent or family member"
    ###     WHEN "Other" THEN  "Some other arrangement" //Not sure this is the right category
    ###     WHEN "Owns or shares own home, condominium, or apartment" THEN "Owns or shares own home, condominium, or apartment"
    ###     WHEN "Rents of shares own home or apartment" THEN "Rents or shares own home or apartment"
    ###     WHEN "Rents or shares own home or apartment" THEN "Rents or shares own home or apartment"
    ###     WHEN "Some other arrangement" THEN "Some other arrangement"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value" //will have to add new FW values as they come in, they aren't all here
    ###     END
    ### ELSEIF [_Agency] = "ll" THEN CASE[Mob Living Arrangement] //LLCHD
    ###     WHEN 1 THEN "Owns or shares own home, condominium, or apartment" //Owns or shared own home, condo, or apartment
    ###     WHEN 2 THEN "Rents or shares own home or apartment" //Rents or shared own home or apartment
    ###     WHEN 3 THEN "Lives in public housing" //Lives in public housing
    ###     WHEN 4 THEN "Lives with parent or family member" //Lives with parent or family member
    ###     WHEN 5 THEN "Not homeless, some other arrangement" //Some other arrangement
    ###     WHEN 6 THEN "Homeless and sharing housing" //Homeless and sharing housing
    ###     WHEN 7 THEN "Homeless and living in an emergency or transition shelter" //Homeless and living in emergency or transitional shelter
    ###     WHEN 8 THEN "Homeless, some other arrangement" //Homeless with some other arrangement
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### END
df4_edits1['_T12 MOB Housing Status'] = df4_edits1.apply(func=fn_T12_MOB_Housing_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T12 MOB Housing Status']) 
# #%%
# inspect_col(df4_edits1['Mob Living Arrangement']) ### Integer.

#%%###################################

def fn_T12_FOB_Housing_Status(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] != "ll"):
            if pd.isna(fdf['Housing Status']):
                return "Unknown/Did Not Report"
            else:
                match fdf['Housing Status'].lower():
                    case "owns or shares own home, condominium, or apartment":
                        return "Owns or shares own home, condominium, or apartment"
                    case (
                        "rents of shares own home or apartment" | 
                        "rents or shares own home or apartment"
                    ):
                        return "Rents or shares own home or apartment"
                    case "lives with parent or family member":
                        return "Lives with parent or family member"
                    case "live in public housing":
                        return "Lives in public housing"
                    case "homeless and sharing housing":
                        return "Homeless and sharing housing"
                    case "homeless and living in an emergency or transitional shelter":
                        return "Homeless and living in an emergency or transition shelter" ### Homeless and living in emergency or transitional shelter.
                    case "some other arrangement":
                        return "Some other arrangement"
                    case "other":
                        return "Some other arrangement" ### TODO: Check old comment: Not sure this is the right category.
                    case _:
                        return "Unrecognized Value" ### TODO: Check old comment: will have to add new FW values as they come in, they aren't all here.
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] == "ll"):
            match fdf['Fob Living Arrangement']:
                case _ if pd.isna(fdf['Fob Living Arrangement']): ### One of 2 differences between FOB & MOB.
                    return "Unknown/Did Not Report"
                case 1:
                    return "Owns or shares own home, condominium, or apartment" ### Owns or shared own home, condo, or apartment.
                case 2:
                    return "Rents or shares own home or apartment" ### Rents or shared own home or apartment.
                case 3:
                    return "Lives in public housing" ### Lives in public housing.
                case 4:
                    return "Lives with parent or family member" ### Lives with parent or family member.
                case 5:
                    return "Not homeless, some other arrangement" ### Some other arrangement.
                case 6:
                    return "Homeless and sharing housing" ### Homeless and sharing housing.
                case 7:
                    return "Homeless and living in an emergency or transition shelter" ### Homeless and living in emergency or transitional shelter.
                case 8:
                    return "Homeless, some other arrangement" ### Homeless with some other arrangement.
                case 88: ### One of 2 differences between FOB & MOB.
                    return "Unknown/Did Not Report"
                case _:
                    return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### IF [_Agency]<> "ll" THEN CASE [Housing Status] //FW
    ###     WHEN "Homeless and living in an emergency or transitional shelter" THEN "Homeless and living in an emergency or transition shelter" //Homeless and living in emergency or transitional shelter
    ###     WHEN "Homeless and sharing housing" THEN "Homeless and sharing housing"
    ###     WHEN "Live in public housing" THEN "Lives in public housing"
    ###     WHEN "Lives with parent or family member" THEN "Lives with parent or family member"
    ###     WHEN "Other" THEN  "Some other arrangement" //Not sure this is the right category
    ###     WHEN "Owns or shares own home, condominium, or apartment" THEN "Owns or shares own home, condominium, or apartment"
    ###     WHEN "Rents of shares own home or apartment" THEN "Rents or shares own home or apartment"
    ###     WHEN "Rents or shares own home or apartment" THEN "Rents or shares own home or apartment"
    ###     WHEN "Some other arrangement" THEN "Some other arrangement"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value" //will have to add new FW values as they come in, they aren't all here
    ###     END
    ### ELSEIF [_Agency] = "ll" THEN CASE [Fob Living Arrangement] //LLCHD
    ###     WHEN 1 THEN "Owns or shares own home, condominium, or apartment" //Owns or shared own home, condo, or apartment
    ###     WHEN 2 THEN "Rents or shares own home or apartment" //Rents or shared own home or apartment
    ###     WHEN 3 THEN "Lives in public housing" //Lives in public housing
    ###     WHEN 4 THEN "Lives with parent or family member" //Lives with parent or family member
    ###     WHEN 5 THEN "Not homeless, some other arrangement" //Some other arrangement
    ###     WHEN 6 THEN "Homeless and sharing housing" //Homeless and sharing housing
    ###     WHEN 7 THEN "Homeless and living in an emergency or transition shelter" //Homeless and living in emergency or transitional shelter
    ###     WHEN 8 THEN "Homeless, some other arrangement" //Homeless with some other arrangement
    ###     WHEN 88 THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### END
df4_edits1['_T12 FOB Housing Status'] = df4_edits1.apply(func=fn_T12_FOB_Housing_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T12 FOB Housing Status']) 

#%%###################################

def fn_T12_Caregiver_Housing_Status(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_T12 MOB Housing Status']
        case "FOB":
            return fdf['_T12 FOB Housing Status']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_T12 MOB Housing Status]
    ###     WHEN "FOB" THEN [_T12 FOB Housing Status]
    ### END
df4_edits1['_T12 Caregiver Housing Status'] = df4_edits1.apply(func=fn_T12_Caregiver_Housing_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T12 Caregiver Housing Status']) 

#%%###################################

def fn_T12_MOB_Homeless_Status(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] != "ll"):
            match ('NA' if pd.isna(fdf['Homeless Status']) else fdf['Homeless Status']):
                case "Homeless":
                    return "Homeless"
                case "Not Homeless":
                        return "Not Homeless"
                case _:
                    if pd.isna(fdf['Housing Status']):
                        return "Unknown/Did Not Report"
                    else:
                        match fdf['Housing Status'].lower():
                            case "homeless and sharing housing" | "homeless and living in an emergency or transitional shelter":
                                return "Homeless"
                            case (
                                "owns or shares own home, condominium, or apartment" | 
                                "rents of shares own home or apartment" | 
                                "rents or shares own home or apartment" | 
                                "lives with parent or family member" | 
                                "live in public housing" 
                            ):
                                return "Not Homeless"
                            case "some other arrangement":
                                return "Unknown/Did Not Report"
                            case "other":
                                return "Unknown/Did Not Report" ### TODO: Check old comment: Not sure this is the right category.
                            case _:
                                return "Unrecognized Value" ### TODO: Check old comment: will have to add new FW values as they come in, they aren't all here.
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] == "ll"):
            match fdf['Mob Living Arrangement']:
                case _ if pd.isna(fdf['Mob Living Arrangement']):
                    return "Unknown/Did Not Report"
                case 1 | 2 | 3 | 4 | 5:
                    return "Not Homeless" 
                case 6 | 7 | 8:
                    return "Homeless" 
                case _:
                    return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### IF [_Agency]<> "ll" THEN CASE [Homeless Status] //FW
    ###     WHEN "Homeless" THEN "Homeless"
    ###     WHEN "Not Homeless" THEN "Not Homeless"
    ###     ELSE CASE [Housing Status] //FW
    ###         WHEN "Homeless and living in an emergency or transitional shelter" THEN "Homeless"
    ###         WHEN "Homeless and sharing housing" THEN "Homeless"
    ###         WHEN "Live in public housing" THEN "Not Homeless"
    ###         WHEN "Lives with parent or family member" THEN "Not Homeless"
    ###         WHEN "Other" THEN  "Unknown/Did Not Report" //Not sure this is the right category
    ###         WHEN "Owns or shares own home, condominium, or apartment" THEN "Not Homeless"
    ###         WHEN "Rents of shares own home or apartment" THEN "Not Homeless"
    ###         WHEN "Some other arrangement" THEN "Unknown/Did Not Report"
    ###         WHEN NULL THEN "Unknown/Did Not Report"
    ###         ELSE "Unrecognized Value" //will have to add new FW values as they come in, they aren't all here
    ###         END
    ###     END
    ### ELSEIF [_Agency] = "ll" THEN CASE [Mob Living Arrangement] //LLCHD
    ###     WHEN 1 THEN "Not Homeless" //Owns or shared own home, condo, or apartment
    ###     WHEN 2 THEN "Not Homeless" //Rents or shared own home or apartment
    ###     WHEN 3 THEN "Not Homeless" //Lives in public housing
    ###     WHEN 4 THEN "Not Homeless" //Lives with parent or family member
    ###     WHEN 5 THEN "Not Homeless" //Some other arrangement
    ###     WHEN 6 THEN "Homeless" //Homeless and sharing housing
    ###     WHEN 7 THEN "Homeless" //Homesless and living in emergency or transitional shelter
    ###     WHEN 8 THEN "Homeless" //Homeless with some other arrangement
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### END
df4_edits1['_T12 MOB Homeless Status'] = df4_edits1.apply(func=fn_T12_MOB_Homeless_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T12 MOB Homeless Status']) 

#%%###################################

def fn_T12_FOB_Homeless_Status(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] != "ll"):
            match ('NA' if pd.isna(fdf['Homeless Status']) else fdf['Homeless Status']):
                case "Homeless":
                    return "Homeless"
                case "Not Homeless":
                        return "Not Homeless"
                case _:
                    if pd.isna(fdf['Housing Status']):
                        return "Unknown/Did Not Report"
                    else:
                        match fdf['Housing Status'].lower():
                            case "homeless and sharing housing" | "homeless and living in an emergency or transitional shelter":
                                return "Homeless"
                            case (
                                "owns or shares own home, condominium, or apartment" | 
                                "rents of shares own home or apartment" | 
                                "rents or shares own home or apartment" | 
                                "lives with parent or family member" | 
                                "live in public housing" 
                            ):
                                return "Not Homeless"
                            case "some other arrangement":
                                return "Unknown/Did Not Report"
                            case "other":
                                return "Unknown/Did Not Report" ### TODO: Check old comment: Not sure this is the right category.
                            case _:
                                return "Unrecognized Value" ### TODO: Check old comment: will have to add new FW values as they come in, they aren't all here.
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] == "ll"):
            match fdf['Fob Living Arrangement']: ### One of 2 differences between FOB & MOB.
                case _ if pd.isna(fdf['Fob Living Arrangement']):
                    return "Unknown/Did Not Report"
                case 1 | 2 | 3 | 4 | 5:
                    return "Not Homeless" 
                case 6 | 7 | 8:
                    return "Homeless" 
                case 88: ### One of 2 differences between FOB & MOB.
                    return "Unknown/Did Not Report"
                case _:
                    return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### IF [_Agency]<> "ll" THEN CASE [Homeless Status] //FW
    ###     WHEN "Homeless" THEN "Homeless"
    ###     WHEN "Not Homeless" THEN "Not Homeless"
    ###     ELSE CASE [Housing Status] //FW
    ###         WHEN "Homeless and living in an emergency or transitional shelter" THEN "Homeless"
    ###         WHEN "Homeless and sharing housing" THEN "Homeless"
    ###         WHEN "Live in public housing" THEN "Not Homeless"
    ###         WHEN "Lives with parent or family member" THEN "Not Homeless"
    ###         WHEN "Other" THEN  "Unknown/Did Not Report" //Not sure this is the right category
    ###         WHEN "Owns or shares own home, condominium, or apartment" THEN "Not Homeless"
    ###         WHEN "Rents of shares own home or apartment" THEN "Not Homeless"
    ###         WHEN "Some other arrangement" THEN "Unknown/Did Not Report"
    ###         WHEN NULL THEN "Unknown/Did Not Report"
    ###         ELSE "Unrecognized Value" //will have to add new FW values as they come in, they aren't all here
    ###         END
    ###     END
    ### ELSEIF [_Agency] = "ll" THEN CASE [Fob Living Arrangement] //LLCHD
    ###     WHEN 1 THEN "Not Homeless" //Owns or shared own home, condo, or apartment
    ###     WHEN 2 THEN "Not Homeless" //Rents or shared own home or apartment
    ###     WHEN 3 THEN "Not Homeless" //Lives in public housing
    ###     WHEN 4 THEN "Not Homeless" //Lives with parent or family member
    ###     WHEN 5 THEN "Not Homeless" //Some other arrangement
    ###     WHEN 6 THEN "Homeless" //Homeless and sharing housing
    ###     WHEN 7 THEN "Homeless" //Homesless and living in emergency or transitional shelter
    ###     WHEN 8 THEN "Homeless" //Homeless with some other arrangement
    ###     WHEN 88 THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### END
df4_edits1['_T12 FOB Homeless Status'] = df4_edits1.apply(func=fn_T12_FOB_Homeless_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T12 FOB Homeless Status']) 

#%%###################################

def fn_T12_Caregiver_Homeless_Status(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_T12 MOB Homeless Status']
        case "FOB":
            return fdf['_T12 FOB Homeless Status']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_T12 MOB Homeless Status]
    ###     WHEN "FOB" THEN [_T12 FOB Homeless Status]
    ### END
df4_edits1['_T12 Caregiver Homeless Status'] = df4_edits1.apply(func=fn_T12_Caregiver_Homeless_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T12 Caregiver Homeless Status']) 

#%%##################################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
### REMINDER: Update to new year's federal poverty guidelines in RUNME.py.
df4_edits1['_T14 Federal Poverty Level update'] = Fpg_Base + (Fpg_Increment * df4_edits1['Household Size'].astype('Int64'))
    ### /// Tableau Calculation Q2:
    ### //uses 2023 federal guidelines, will need to update to 2024 guidelines when they become available
    ### 9440 + (5140 * [Household Size])
    ### Data Type in Tableau: integer.
inspect_col(df4_edits1['_T14 Federal Poverty Level update'])

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T14_Poverty_Percent(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] != "ll"):
            return fdf['Poverty Level'] 
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] == "ll"):
            if (pd.isna(fdf['Household Income']) or pd.isna(fdf['Household Size'])):
                return np.nan 
            else:
                return fdf['Household Income'] / fdf['_T14 Federal Poverty Level update']
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### IF [_Agency] = "ll" AND ISNULL([Household Income]) THEN NULL //LLCHD
    ### ELSEIF [_Agency] = "ll" AND ISNULL([Household Size]) THEN NULL
    ### ELSEIF [_Agency] = "ll" THEN [Household Income]/[_T14 Federal Poverty Level update]
    ### ELSEIF [_Agency] <> "ll" THEN [Poverty Level] //FW
    ### END
#### df4_edits1['_T14 Poverty Percent'] = df4_edits1.apply(func=fn_T14_Poverty_Percent, axis=1).round(2).astype('Float64')
df4_edits1['_T14 Poverty Percent'] = df4_edits1.apply(func=fn_T14_Poverty_Percent, axis=1).astype('Float64')
df4_edits1['_T14 Poverty Percent'] = df4_edits1['_T14 Poverty Percent'].round(2)
    ### Data Type in Tableau: 'float'.
inspect_col(df4_edits1['_T14 Poverty Percent']) 
# #%%
# inspect_col(df4_edits1['Poverty Level']) ### float.

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T14_Federal_Poverty_Categories(fdf):
    if (pd.isna(fdf['_T14 Poverty Percent'])):
        return pd.NA ### Should be "Unknown/Did Not Report" - Tableau code wrong. ### TODO: Fix after compare.
    elif (fdf['_T14 Poverty Percent'] <= .50):
        return "50% and Under"
    elif (fdf['_T14 Poverty Percent'] <= 1.00):
        return "51-100%"
    elif (fdf['_T14 Poverty Percent'] <= 1.33):
        return "101-133%"
    elif (fdf['_T14 Poverty Percent'] <= 2.00):
        return "134-200%"
    elif (fdf['_T14 Poverty Percent'] <= 3.00):
        return "201-300%"
    elif (fdf['_T14 Poverty Percent'] > 3.00):
        return ">300%"
    ###########
    ### /// Tableau Calculation:
    ### IF [_T14 Poverty Percent] <= .50 THEN "50% and Under"
    ### ELSEIF [_T14 Poverty Percent] <= 1.00 THEN "51-100%"
    ### ELSEIF [_T14 Poverty Percent] <= 1.33 THEN "101-133%"
    ### ELSEIF [_T14 Poverty Percent] <= 2.00 THEN "134-200%"
    ### ELSEIF [_T14 Poverty Percent] <= 3.00 THEN "201-300%"
    ### ELSEIF [_T14 Poverty Percent] > 3.00  THEN ">300%"
    ### ELSEIF NULL THEN "Unknown/Did Not Report" /// ERROR: Should be "ELSEIF ISNULL([_T14 Poverty Percent])..." OR just ELSE...
    ### END
df4_edits1['_T14 Federal Poverty Categories'] = df4_edits1.apply(func=fn_T14_Federal_Poverty_Categories, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T14 Federal Poverty Categories']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T15_3_History_Welfare_Interaction(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['History Inter Welfare Adult']:
            case _ if pd.isna(fdf['History Inter Welfare Adult']):
                return 0
            case True:
                return 1 
            case False:
                return 0 
            case _:
                return 0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Priority Child Welfare']:
            case _ if pd.isna(fdf['Priority Child Welfare']):
                return 0 
            case "Y":
                return 1 
            case "N":
                return 0 
            case _:
                return 0 
    ###########
    else:
        return 0 
    ###########
    ### /// Tableau Calculation:
    ### IF [History Inter Welfare Adult] = True THEN 1 //FW
    ### ELSEIF  [History Inter Welfare Adult] = False THEN 0
    ### ELSEIF[Priority Child Welfare] = "Y" THEN 1 //LLCHD
    ### ELSEIF [Priority Child Welfare] = "N" THEN 0
    ### ELSE 0
    ### END
df4_edits1['_T15-3 History Welfare Interaction'] = df4_edits1.apply(func=fn_T15_3_History_Welfare_Interaction, axis=1).astype('Int64') 
    ### Data Type in Tableau: 'integer'.
inspect_col(df4_edits1['_T15-3 History Welfare Interaction']) 

#%%###################################

def fn_T15_4_MOB_Substance_Abuse(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] != "ll"):
            match fdf['Mob Substance Abuse']:
                case _ if pd.isna(fdf['Mob Substance Abuse']):
                    return "Unknown/Did Not Report"
                case True:
                    return "Yes"
                case False:
                    return "No"
                case _:
                    return "Unknown/Did Not Report"
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] == "ll"):
            match fdf['Priority Substance Abuse']:
                case _ if pd.isna(fdf['Priority Substance Abuse']):
                    return "Unknown/Did Not Report"
                case "Y":
                    return "Yes"
                case "N":
                    return "No"
                case _:
                    return "Unknown/Did Not Report"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### IF [_Agency] <> "ll" THEN CASE[Mob Substance Abuse] //FW
    ###     WHEN TRUE THEN "Yes"
    ###     WHEN FALSE THEN "No"
    ###     ELSE "Unknown/Did Not Report"
    ###     END     
    ### ELSEIF [_Agency] = "ll" THEN CASE[Priority Substance Abuse]
    ###     WHEN "Y" THEN "Yes"
    ###     WHEN "N" THEN "No"
    ###     ELSE "Unknown/Did Not Report"
    ###     END
    ### END
df4_edits1['_T15-4 MOB Substance Abuse'] = df4_edits1.apply(func=fn_T15_4_MOB_Substance_Abuse, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T15-4 MOB Substance Abuse']) 

#%%###################################

def fn_T15_4_FOB_Substance_Abuse(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] != "ll"):
            match fdf['Fob Substance Abuse']:
                case _ if pd.isna(fdf['Fob Substance Abuse']):
                    return "Unknown/Did Not Report"
                case True:
                    return "Yes"
                case False:
                    return "No"
                case _:
                    return "Unknown/Did Not Report"
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['_Agency']):
            return pd.NA 
        elif (fdf['_Agency'] == "ll"):
            match fdf['Priority Substance Abuse']:
                case _ if pd.isna(fdf['Priority Substance Abuse']):
                    return "Unknown/Did Not Report"
                case "Y":
                    return "Yes"
                case "N":
                    return "No"
                case _:
                    return "Unknown/Did Not Report"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### IF [_Agency] <> "ll" THEN CASE[Fob Substance Abuse] //FW
    ###     WHEN TRUE THEN "Yes"
    ###     WHEN FALSE THEN "No"
    ###     ELSE "Unknown/Did Not Report"
    ###     END     
    ### ELSEIF [_Agency] = "ll" THEN CASE[Priority Substance Abuse]
    ###     WHEN "Y" THEN "Yes"
    ###     WHEN "N" THEN "No"
    ###     ELSE "Unknown/Did Not Report"
    ###     END
    ### END
df4_edits1['_T15-4 FOB Substance Abuse'] = df4_edits1.apply(func=fn_T15_4_FOB_Substance_Abuse, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T15-4 FOB Substance Abuse']) 
# #%%
# inspect_col(df4_edits1['_Agency']) 
# #%%
# print(df4_edits1[['source', '_T15-4 MOB Substance Abuse', 'Mob Substance Abuse', '_T15-4 FOB Substance Abuse', 'Fob Substance Abuse', '_Agency', 'Priority Substance Abuse']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string()) 

#%%###################################

def fn_T15_4_Caregiver_Substance_Abuse(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_T15-4 MOB Substance Abuse']
        case "FOB":
            return fdf['_T15-4 FOB Substance Abuse']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_T15-4 MOB Substance Abuse]
    ###     WHEN "FOB" THEN [_T15-4 FOB Substance Abuse]
    ### END
df4_edits1['_T15-4 Caregiver Substance Abuse'] = df4_edits1.apply(func=fn_T15_4_Caregiver_Substance_Abuse, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T15-4 Caregiver Substance Abuse']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T15_5_Tobacco_Use_in_Home(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Tobacco Use In Home']:
            case _ if pd.isna(fdf['Tobacco Use In Home']):
                return 0
            case "Yes":
                return 1 
            case "No":
                return 0 
            case _:
                return 0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Priority Tobacco Use']:
            case _ if pd.isna(fdf['Priority Tobacco Use']):
                return 0 
            case "Y":
                return 1 
            case "N":
                return 0 
            case _:
                return 0 
    ###########
    else:
        return 0 
    ###########
    ### /// Tableau Calculation:
    ### IF [Tobacco Use In Home] = "Yes" THEN 1 //FW
    ### ELSEIF [Tobacco Use In Home] = "No" THEN 0
    ### ELSEIF [Priority Tobacco Use] = "Y" THEN 1 //LLCHD
    ### ELSEIF [Priority Tobacco Use] = "N" THEN 0
    ### ELSE 0
    ### END
df4_edits1['_T15-5 Tobacco Use in Home'] = df4_edits1.apply(func=fn_T15_5_Tobacco_Use_in_Home, axis=1).astype('Int64') 
    ### Data Type in Tableau: 'integer'.
inspect_col(df4_edits1['_T15-5 Tobacco Use in Home']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T15_6_Low_Achievement(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Low Achievement']:
            case _ if pd.isna(fdf['Low Achievement']):
                return 0
            case "Yes":
                return 1 
            case "No":
                return 0 
            case _:
                return 0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Priority Low Student']:
            case _ if pd.isna(fdf['Priority Low Student']):
                return 0 
            case "Y":
                return 1 
            case "N":
                return 0 
            case _:
                return 0 
    ###########
    else:
        return 0 
    ###########
    ### /// Tableau Calculation:
    ### IF [Low Achievement] = "Yes" THEN 1 //FW
    ### ELSEIF [Low Achievement] = "No" THEN 0
    ### ELSEIF [Priority Low Student] = "Y" THEN 1 //LLCHD
    ### ELSEIF [Priority Low Student] = "N" THEN 0
    ### ELSE 0
    ### END
df4_edits1['_T15-6 Low Achievement'] = df4_edits1.apply(func=fn_T15_6_Low_Achievement, axis=1).astype('Int64') 
    ### Data Type in Tableau: 'integer'.
inspect_col(df4_edits1['_T15-6 Low Achievement']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T15_8_Military(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Military']:
            case _ if pd.isna(fdf['Military']):
                return 0
            case "Y":
                return 1 
            case "N":
                return 0 
            case _:
                return 0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Priority Military']:
            case _ if pd.isna(fdf['Priority Military']):
                return 0 
            case "Y":
                return 1 
            case "N":
                return 0 
            case _:
                return 0 
    ###########
    else:
        return 0 
    ###########
    ### /// Tableau Calculation:
    ### IF [Military]= "Y" THEN 1 //FW
    ### ELSEIF [Military] = "N" THEN 0
    ### ELSEIF [Priority Military] = "Y" THEN 1 //LLCHD
    ### ELSEIF [Priority Military] = "N" THEN 0
    ### ELSE 0
    ### END
df4_edits1['_T15-8 Military'] = df4_edits1.apply(func=fn_T15_8_Military, axis=1).astype('Int64') 
    ### Data Type in Tableau: 'integer'.
inspect_col(df4_edits1['_T15-8 Military']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T17_Discharge_Reason(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if (pd.notna(fdf['Termination Date'])):
            if pd.isna(fdf['Termination Status']):
                return "Stopped Services Before Completion" ### TODO: Check if all logic for this var desired.
            else: 
                match fdf['Termination Status'].lower():
                    case "family graduated/met all program goals":
                        return "Completed Services"
                    case _:
                        return "Stopped Services Before Completion"
        else:
            return "Currently Receiving Services"
    ###########
    ### LLCHD, see full reasons below.
    elif (fdf['source'] == 'LL'):
        if (pd.notna(fdf['Discharge Dt'])):
            if pd.isna(fdf['Discharge Reason']):
                return "Stopped Services Before Completion" ### TODO: Check if all logic for this var desired.
            else: 
                match fdf['Discharge Reason'].lower():
                    case "1" | "family has met program goals":
                        return "Completed Services"
                    case _:
                        return "Stopped Services Before Completion"
        else:
            return "Currently Receiving Services"
    ###########
    else:
        return "Currently Receiving Services"
    ###########
    ### /// Tableau Calculation:
    ### IF NOT ISNULL([Discharge Dt]) THEN CASE [Discharge Reason] //LLCHD, see full reasons below
    ###     WHEN "1" THEN "Completed Services" 
    ###     WHEN "Family Has Met Program Goals" THEN "Completed Services"
    ###     ELSE "Stopped Services Before Completion"
    ###     END
    ### ELSEIF NOT ISNULL([Termination Date]) THEN CASE [Termination Status] //FW
    ###     WHEN "Family graduated/met all program goals" THEN "Completed Services"
    ###     ELSE "Stopped Services Before Completion"
    ###     END
    ### ELSE "Currently Receiving Services"
    ### END
    ### //LLCHD discharge reasons
    ### //1Family graduated/met all program goals
    ### //2Family moved out of service area
    ### //3Parent/guardian returned to school
    ### //4Parent/guardian returned to work
    ### //5Parent/guardian refused service
    ### //6Death of participant
    ### //7Unable to locate family
    ### //8Target child adopted
    ### //9Target child entered foster care
    ### //10Target child living with another care giverx
    ### //11Target child entered school/child care
    ### //12Family never engaged
    ### //13Unknown & a text box
    ### //Family Has Met Program Goals
    ### //Miscarriage/Pregnancy Terminated
    ### //Other
    ### //Out of Geographical Target
    ### //Participant non-compliant,unresponsive
    ### //Participant refused
    ### //Participant Unavailable Due to School or Employment
    ### //Program Unable to Locate or Make Contact
    ### //Transferred/Referred/Involved in Other Program
df4_edits1['_T17 Discharge Reason'] = df4_edits1.apply(func=fn_T17_Discharge_Reason, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T17 Discharge Reason']) 

#%%###################################

def fn_C16_CG_Insurance_Status(fdf_column):
    match fdf_column:
        case _ if pd.isna(fdf_column):
            return "Unknown/Did Not Report" ### Difference from fn_T20_CG_Insurance_Status where it's pd.NA.
        ###########
        ### FW.
        case "Medicaid" | "SCHIP":
            return "Medicaid or CHIP"
        case "Private" | "Other" | "Medicare":
            return "Private or Other"
        case "Tri-Care":
            return "Tri-Care"
        case "None":
            return "No Insurance Coverage"
        case "Unknown" | "null":
            return "Unknown/Did Not Report"
        ###########
        ### LLCHD.
        case "1" | "Medicaid":
            return "Medicaid or CHIP"
        case "2":
            return "Tri-Care"
        case "3" | "Private":
            return "Private or Other"
        case "4":
            return "FamilyChildHealthPlus" ### Different from Form2's "Unknown/Did Not Report". ### TODO: standardize.
        case "5" | "Uninsure":
            return "No Insurance Coverage"
        case "6" | "99" | "Unknown":
            return "Unknown/Did Not Report"
        case "FamilyCh":
            return "FamilyChildHealthPlus"
        ###########
        case _:
            return "Unrecognized Value"
    ###########
    ### CASE [AD1PrimaryIns.1] //FW
    ###     WHEN "Medicaid" THEN "Medicaid or CHIP"
    ###     WHEN "SCHIP" THEN "Medicaid or CHIP"
    ###     WHEN "Medicare" THEN "Private or Other"
    ###     WHEN "Tri-Care" THEN "Tri-Care"
    ###     WHEN "None" THEN "No Insurance Coverage"
    ###     WHEN "Other" THEN "Private or Other"
    ###     WHEN "Private" THEN "Private or Other"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "null" THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ### //LLCHD
    ###     WHEN "1" THEN "Medicaid or CHIP"
    ###     WHEN "2" THEN "Tri-Care"
    ###     WHEN "3" THEN "Private or Other"
    ###     WHEN "4" THEN "FamilyChildHealthPlus"
    ###     WHEN "5" THEN "No Insurance Coverage"
    ###     WHEN "6" THEN "Unknown/Did Not Report"
    ###     WHEN "99" THEN "Unknown/Did Not Report"
    ###     WHEN "Medicaid" THEN "Medicaid or CHIP"
    ###     WHEN "Private" THEN "Private or Other"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "Uninsure" THEN "No Insurance Coverage"
    ###     WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ### ELSE "Unrecognized Value"
    ### END

### Only Form1 difference:
### ['_C16 CG Insurance 4 Status']: WHEN "4" THEN "Unknown/Did Not Report"
###     NOTE: this different version is the same as ALL of the Form2 versions.
### TODO: standardize.

#%%###################################

df4_edits1['_C16 CG Insurance 1 Status'] = df4_edits1['AD1PrimaryIns.1'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 1 Status']) 
# #%%
# compare_col(df4_comparison_csv, df4_edits1, '_C16 CG Insurance 1 Status')
# #%%
# compare_col(df4_comparison_csv, df4_edits1, '_C16 CG Insurance 1 Status', info_or_value_counts='value_counts')

#%%###################################

df4_edits1['_C16 CG Insurance 2 Status'] = df4_edits1['AD1PrimaryIns.2'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 2 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 3 Status'] = df4_edits1['AD1PrimaryIns.3'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 3 Status']) 

#%%###################################

### df4_edits1['_C16 CG Insurance 4 Status'] = df4_edits1['AD1PrimaryIns.4'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
###     ### Data Type in Tableau: 'string'.
### inspect_col(df4_edits1['_C16 CG Insurance 4 Status']) 

### Alternative because of one difference:
def fn_C16_CG_Insurance_4_Status(fdf_column):
    match fdf_column:
        case _ if pd.isna(fdf_column):
            return "Unknown/Did Not Report" ### Difference from fn_T20_CG_Insurance_Status where it's pd.NA.
        ###########
        ### FW.
        case "Medicaid" | "SCHIP":
            return "Medicaid or CHIP"
        case "Tri-Care":
            return "Tri-Care"
        case "Private" | "Other" | "Medicare":
            return "Private or Other"
        case "None":
            return "No Insurance Coverage"
        case "Unknown" | "null":
            return "Unknown/Did Not Report"
        ###########
        ### LLCHD.
        case "1" | "Medicaid":
            return "Medicaid or CHIP"
        case "2":
            return "Tri-Care"
        case "3" | "Private":
            return "Private or Other"
        case "4":
            return "Unknown/Did Not Report" ### #4 like Form2 but not other Form1's "FamilyChildHealthPlus". ### TODO: standardize.
        case "FamilyCh":
            return "FamilyChildHealthPlus"
        case "5" | "Uninsure":
            return "No Insurance Coverage"
        case "6" | "99" | "Unknown":
            return "Unknown/Did Not Report"
        ###########
        case _:
            return "Unrecognized Value"
    ###########
    ### /// Tableau Calculation:
    ### CASE [AD1PrimaryIns.4] //FW
    ###     WHEN "Medicaid" THEN "Medicaid or CHIP"
    ###     WHEN "SCHIP" THEN "Medicaid or CHIP"
    ###     WHEN "Medicare" THEN "Private or Other"
    ###     WHEN "Tri-Care" THEN "Tri-Care"
    ###     WHEN "None" THEN "No Insurance Coverage"
    ###     WHEN "Other" THEN "Private or Other"
    ###     WHEN "Private" THEN "Private or Other"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "null" THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ### //LLCHD
    ###     WHEN "1" THEN "Medicaid or CHIP"
    ###     WHEN "2" THEN "Tri-Care"
    ###     WHEN "3" THEN "Private or Other"
    ###     WHEN "4" THEN "Unknown/Did Not Report"
    ###     WHEN "5" THEN "No Insurance Coverage"
    ###     WHEN "6" THEN "Unknown/Did Not Report"
    ###     WHEN "99" THEN "Unknown/Did Not Report"
    ###     WHEN "Medicaid" THEN "Medicaid or CHIP"
    ###     WHEN "Private" THEN "Private or Other"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "Uninsure" THEN "No Insurance Coverage"
    ###     WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ### ELSE "Unrecognized Value"
    ### END
df4_edits1['_C16 CG Insurance 4 Status'] = df4_edits1['AD1PrimaryIns.4'].apply(func=fn_C16_CG_Insurance_4_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 4 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 5 Status'] = df4_edits1['AD1PrimaryIns.5'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 5 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 6 Status'] = df4_edits1['AD1PrimaryIns.6'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 6 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 7 Status'] = df4_edits1['AD1PrimaryIns.7'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 7 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 8 Status'] = df4_edits1['AD1PrimaryIns.8'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 8 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 9 Status'] = df4_edits1['AD1PrimaryIns.9'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 9 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 10 Status'] = df4_edits1['AD1PrimaryIns.10'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 10 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 11 Status'] = df4_edits1['AD1PrimaryIns.11'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 11 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 12 Status'] = df4_edits1['AD1PrimaryIns.12'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 12 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 13 Status'] = df4_edits1['AD1PrimaryIns.13'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 13 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 14 Status'] = df4_edits1['AD1PrimaryIns.14'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 14 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 15 Status'] = df4_edits1['AD1PrimaryIns.15'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 15 Status']) 

#%%###################################

df4_edits1['_C16 CG Insurance 16 Status'] = df4_edits1['AD1PrimaryIns.16'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_C16 CG Insurance 16 Status']) 

#%%###################################

def fn_T20_CG_Insurance_Status(fdf_column):
    match fdf_column:
        case _ if pd.isna(fdf_column):
            return pd.NA ### Difference from fn_C16_CG_Insurance_Status where it's "Unknown/Did Not Report". ### TODO: standardize? Or at least document why different.
        ###########
        ### FW.
        case "Medicaid" | "SCHIP":
            return "Medicaid or CHIP"
        case "Tri-Care":
            return "Tri-Care"
        case "Private" | "Other" | "Medicare":
            return "Private or Other"
        case "None":
            return "No Insurance Coverage"
        case "Unknown" | "null":
            return "Unknown/Did Not Report"
        ###########
        ### LLCHD.
        case "0":
            return "No Insurance Coverage" ### New option not in fn_C16_CG_Insurance_Status variables. ### TODO: Add to fn_C16_CG_Insurance_Status?
        case "1" | "Medicaid":
            return "Medicaid or CHIP"
        case "2":
            return "Tri-Care"
        case "3" | "Private":
            return "Private or Other"
        case "4":
            return "FamilyChildHealthPlus" ### Different from Form2's "Unknown/Did Not Report". ### TODO: standardize.
        case "FamilyCh":
            return "FamilyChildHealthPlus"
        case "5" | "Uninsure":
            return "No Insurance Coverage"
        case "6" | "99" | "Unknown":
            return "Unknown/Did Not Report"
        ###########
        case _:
            return "Unrecognized Value"
    ###########
    ### /// Tableau Calculation:
    ### IF NOT ISNULL([AD1PrimaryIns.1]) THEN
    ### CASE [AD1PrimaryIns.1] // FW
    ###     WHEN "Medicaid" THEN "Medicaid or CHIP"
    ###     WHEN "SCHIP" THEN "Medicaid or CHIP"
    ###     WHEN "Medicare" THEN "Private or Other"
    ###     WHEN "Tri-Care" THEN "Tri-Care"
    ###     WHEN "None" THEN "No Insurance Coverage"
    ###     WHEN "Other" THEN "Private or Other"
    ###     WHEN "Private" THEN "Private or Other"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "null" THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ### //LLCHD
    ###     WHEN "0" THEN "No Insurance Coverage"
    ###     WHEN "1" THEN "Medicaid or CHIP"
    ###     WHEN "2" THEN "Tri-Care"
    ###     WHEN "3" THEN "Private or Other"
    ###     WHEN "4" THEN "FamilyChildHealthPlus"
    ###     WHEN "5" THEN "No Insurance Coverage"
    ###     WHEN "6" THEN "Unknown/Did Not Report"
    ###     WHEN "99" THEN "Unknown/Did Not Report"
    ###     WHEN "Medicaid" THEN "Medicaid or CHIP"
    ###     WHEN "Private" THEN "Private or Other"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "Uninsure" THEN "No Insurance Coverage"
    ###     WHEN "FamilyCh" THEN "FamilyChildHealthPlus"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ### ELSE "Unrecognized Value"
    ### END
    ### END

### SAME as all other similar vars.

#%%###################################

df4_edits1['_T20 CG Insurance 1 Status'] = df4_edits1['AD1PrimaryIns.1'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 1 Status']) 
# #%%
# compare_col(df4_comparison_csv, df4_edits1, '_T20 CG Insurance 1 Status')
# #%%
# compare_col(df4_comparison_csv, df4_edits1, '_T20 CG Insurance 1 Status', info_or_value_counts='value_counts')

#%%###################################

df4_edits1['_T20 CG Insurance 2 Status'] = df4_edits1['AD1PrimaryIns.2'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 2 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 3 Status'] = df4_edits1['AD1PrimaryIns.3'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 3 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 4 Status'] = df4_edits1['AD1PrimaryIns.4'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 4 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 5 Status'] = df4_edits1['AD1PrimaryIns.5'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 5 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 6 Status'] = df4_edits1['AD1PrimaryIns.6'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 6 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 7 Status'] = df4_edits1['AD1PrimaryIns.7'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 7 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 8 Status'] = df4_edits1['AD1PrimaryIns.8'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 8 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 9 Status'] = df4_edits1['AD1PrimaryIns.9'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 9 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 10 Status'] = df4_edits1['AD1PrimaryIns.10'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 10 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 11 Status'] = df4_edits1['AD1PrimaryIns.11'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 11 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 12 Status'] = df4_edits1['AD1PrimaryIns.12'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 12 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 13 Status'] = df4_edits1['AD1PrimaryIns.13'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 13 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 14 Status'] = df4_edits1['AD1PrimaryIns.14'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 14 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 15 Status'] = df4_edits1['AD1PrimaryIns.15'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 15 Status']) 

#%%###################################

df4_edits1['_T20 CG Insurance 16 Status'] = df4_edits1['AD1PrimaryIns.16'].apply(func=fn_T20_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance 16 Status']) 

#%%###################################

def fn_T20_MOB_Insurance_Status(fdf):
    if pd.notna(fdf['_T20 CG Insurance 16 Status']):
        return fdf['_T20 CG Insurance 16 Status']
    elif pd.notna(fdf['_T20 CG Insurance 15 Status']):
        return fdf['_T20 CG Insurance 16 Status']
    elif pd.notna(fdf['_T20 CG Insurance 14 Status']):
        return fdf['_T20 CG Insurance 14 Status']
    elif pd.notna(fdf['_T20 CG Insurance 13 Status']):
        return fdf['_T20 CG Insurance 13 Status']
    elif pd.notna(fdf['_T20 CG Insurance 12 Status']):
        return fdf['_T20 CG Insurance 12 Status']
    elif pd.notna(fdf['_T20 CG Insurance 11 Status']):
        return fdf['_T20 CG Insurance 11 Status']
    elif pd.notna(fdf['_T20 CG Insurance 10 Status']):
        return fdf['_T20 CG Insurance 10 Status']
    elif pd.notna(fdf['_T20 CG Insurance 9 Status']):
        return fdf['_T20 CG Insurance 9 Status']
    elif pd.notna(fdf['_T20 CG Insurance 8 Status']):
        return fdf['_T20 CG Insurance 8 Status']
    elif pd.notna(fdf['_T20 CG Insurance 7 Status']):
        return fdf['_T20 CG Insurance 7 Status']
    elif pd.notna(fdf['_T20 CG Insurance 6 Status']):
        return fdf['_T20 CG Insurance 6 Status']
    elif pd.notna(fdf['_T20 CG Insurance 5 Status']):
        return fdf['_T20 CG Insurance 5 Status']
    elif pd.notna(fdf['_T20 CG Insurance 4 Status']):
        return fdf['_T20 CG Insurance 4 Status']
    elif pd.notna(fdf['_T20 CG Insurance 3 Status']):
        return fdf['_T20 CG Insurance 3 Status']
    elif pd.notna(fdf['_T20 CG Insurance 2 Status']):
        return fdf['_T20 CG Insurance 2 Status']
    elif pd.notna(fdf['_T20 CG Insurance 1 Status']):
        return fdf['_T20 CG Insurance 1 Status']
    ###########
    ### /// Tableau Calculation:
    ### IF NOT ISNULL([_T20 CG Insurance 16 Status]) THEN [_T20 CG Insurance 16 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 15 Status]) THEN [_T20 CG Insurance 16 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 14 Status]) THEN [_T20 CG Insurance 14 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 13 Status]) THEN [_T20 CG Insurance 13 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 12 Status]) THEN [_T20 CG Insurance 12 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 11 Status]) THEN [_T20 CG Insurance 11 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 10 Status]) THEN [_T20 CG Insurance 10 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 9 Status]) THEN [_T20 CG Insurance 9 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 8 Status]) THEN [_T20 CG Insurance 8 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 7 Status]) THEN [_T20 CG Insurance 7 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 6 Status]) THEN [_T20 CG Insurance 6 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 5 Status]) THEN [_T20 CG Insurance 5 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 4 Status]) THEN [_T20 CG Insurance 4 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 3 Status]) THEN [_T20 CG Insurance 3 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 2 Status]) THEN [_T20 CG Insurance 2 Status]
    ### ELSEIF NOT ISNULL([_T20 CG Insurance 1 Status]) THEN [_T20 CG Insurance 1 Status]
    ### END
df4_edits1['_T20 MOB Insurance Status'] = df4_edits1.apply(func=fn_T20_MOB_Insurance_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 MOB Insurance Status']) 

#%%###################################

### TODO: Compare options to / standardize with MOB insurance.
def fn_T20_FOB_Insurance_Status(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            if pd.isna(fdf['AD2InsPrimary']):
                return pd.NA ### "Unknown/Did Not Report" ### TODO: Why difference compared to older '_T20 FOB Insurance'?
            else:
                match fdf['AD2InsPrimary'].lower():
                    case "medicaid":
                        return "Medicaid or CHIP"
                    case "tri-care":
                        return "Tri-Care"
                    case "private" | "other" | "medicare":
                        return "Private or Other"
                    case "none":
                        return "No Insurance Coverage"
                    case "unknown":
                        return "Unknown/Did Not Report"
                    case _:
                        return pd.NA ### "Unrecognized Value" ### TODO: Why difference compared to older '_T20 FOB Insurance'?
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == "Y"):
            if pd.isna(fdf['Hlth Insure Fob']):
                return "Unknown/Did Not Report"
            else:
                match fdf['Hlth Insure Fob']:
                    case 1:
                        return "Medicaid or CHIP"
                    case 2:
                        return "Tri-Care"
                    case 3:
                        return "Private or Other"
                    case 4:
                        return "FamilyChildHealthPlus"
                    case 5:
                        return "No Insurance Coverage"
                    case 6 | 99:
                        return "Unknown/Did Not Report"
                    case _:
                        return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### IF [Fob Involved] = True THEN CASE [AD2InsPrimary] //FW
    ###     WHEN "Medicaid" THEN "Medicaid or CHIP"
    ###     WHEN "Medicare" THEN "Private or Other" //this is what our previous syntax indicated
    ###     WHEN "None" THEN "No Insurance Coverage"
    ###     WHEN "Other" THEN "Private or Other"
    ###     WHEN "Private" THEN "Private or Other"
    ###     WHEN "Tri-Care" THEN "Tri-Care"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     //WHEN NULL THEN "Unknown/Did Not Report"
    ###     //ELSE "Unrecognized Value"
    ###     END
    ### ELSEIF [Fob Involved1] = "Y" THEN CASE [Hlth Insure Fob] //LLCHD
    ###     WHEN 1 THEN "Medicaid or CHIP"
    ###     WHEN 2 THEN "Tri-Care"
    ###     WHEN 3 THEN "Private or Other"
    ###     WHEN 4 THEN "FamilyChildHealthPlus"
    ###     WHEN 5 THEN "No Insurance Coverage"
    ###     WHEN 6 THEN "Unknown/Did Not Report"
    ###     WHEN 99 THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE NULL
    ### END
df4_edits1['_T20 FOB Insurance Status'] = df4_edits1.apply(func=fn_T20_FOB_Insurance_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 FOB Insurance Status']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Almost Same Tableau Calculation. Python modified.
def fn_T20_FOB_Insurance(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            if pd.isna(fdf['AD2InsPrimary']):
                return "Unknown/Did Not Report"
            else:
                match fdf['AD2InsPrimary'].lower():
                    case "medicaid":
                        return "Medicaid or CHIP"
                    case "medicare":
                        return "Other" ### this is what our previous syntax indicated.
                    case "none":
                        return "No Insurance Coverage"
                    case "other" | "private":
                        return "Private or Other"
                    case "tri-care":
                        return "Tri-Care"
                    case "unknown":
                        return "Unknown/Did Not Report"
                    case _:
                        return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == "Y"):
            if pd.isna(fdf['Hlth Insure Fob']):
                return "Unknown/Did Not Report"
            else:
                match fdf['Hlth Insure Fob']:
                    case 1:
                        return "Medicaid or CHIP"
                    case 2:
                        return "Tri-Care"
                    case 3:
                        return "Private or Other"
                    case 4 | 99:
                        return "Unknown/Did Not Report"
                    case 5:
                        return "No Insurance Coverage"
                    case _:
                        return "Unrecognized Value"
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA 
    ###########
    ### /// Tableau Calculation:
    ### IF [Fob Involved] = True THEN CASE [AD2InsPrimary] //FW
    ###     WHEN "Medicaid" THEN "Medicaid or CHIP"
    ###     WHEN "Medicare" THEN "Other" //this is what our previous syntax indicated
    ###     WHEN "None" THEN "No Insurance Coverage"
    ###     WHEN "Other" THEN "Private or Other"
    ###     WHEN "Private" THEN "Private or Other"
    ###     WHEN "Tri-Care" THEN "Tri-Care"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSEIF [Fob Involved1] = "Y" THEN CASE [Hlth Insure Fob] //LLCHD
    ###     WHEN 1 THEN "Medicaid or CHIP"
    ###     WHEN 2 THEN "Tri-Care"
    ###     WHEN 3 THEN "Private or Other"
    ###     WHEN 4 THEN "Unknown/Did Not Report"
    ###     WHEN 5 THEN "No Insurance Coverage"
    ###     WHEN 99 THEN "Unknown/Did Not Report" ### Only difference between Form2 & Form1 (Adult4) is 99 added.
    ###     WHEN NULL THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE NULL
    ### END
df4_edits1['_T20 FOB Insurance'] = df4_edits1.apply(func=fn_T20_FOB_Insurance, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 FOB Insurance']) 
# #%%
# inspect_col(df4_edits1['Hlth Insure Fob']) ### integer... but Empty. All NA.
### TODO: Why is 'Hlth Insure Fob' empty?
### TODO: Is this var old & should be deleted? It looks like '_T20 FOB Insurance Status' is more updated (compare).

#%%###################################

def fn_T20_CG_Insurance_Status(fdf):
    match fdf['MOB or FOB']:
        case "MOB":
            return fdf['_T20 MOB Insurance Status']
        case "FOB":
            return fdf['_T20 FOB Insurance Status']
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN [_T20 MOB Insurance Status]
    ###     WHEN "FOB" THEN [_T20 FOB Insurance Status]
    ### END
df4_edits1['_T20 CG Insurance Status'] = df4_edits1.apply(func=fn_T20_CG_Insurance_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['_T20 CG Insurance Status']) 


##################################################################################################
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

# sorted([*df4_edits1])

#%%
### Remove columns created in merge.
df4_edits2 = df4_edits1.drop(columns=['LJ_df4_2CI', 'LJ_df4_3FW', 'LJ_df4_4LL', 'LJ_df4_5MoF'])

#%%################################
### ORDER COLUMNS

### Final order for columns:
[*df4_comparison_csv]

#%%
### Reorder Columns.
df4_edits2 = df4_edits2[[*df4_comparison_csv]]

#%%################################
### SORT ROWS

df4_edits2 = df4_edits2.sort_values(by=['Project Id','Year','Quarter','__F1 Caregiver ID for MOB or FOB'], ignore_index=True)



#%%##################################################
### WRITE ###
#####################################################

#%%
### Created Final DF.
df4__final = df4_edits2.copy()

#%%
### Write out df.
df4__final.to_csv(path_4_output, index=False, date_format="%#m/%#d/%Y")


##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################


#%%
### Read back in df for comparison.
# df4__final_from_csv = pd.read_csv(path_4_output, dtype='string', keep_default_na=False, na_values=[''])
df4__final_from_csv = pd.read_csv(path_4_output, dtype='object', keep_default_na=False, na_values=[''])


#%%##################################################
### COMPARE CSVs ###
#####################################################

#%%###################################

#%%
### Column names:
[*df4__final_from_csv]
#%%
### Column names:
[*df4_comparison_csv]

#%%
### Overlap / Similarities: Columns in both.
set([*df4_comparison_csv]).intersection([*df4__final_from_csv])

#%%###################################
### COLUMNS:

#%%
### Check if all Column names identical & in same order.
[*df4__final_from_csv] == [*df4_comparison_csv]

#%%
### Differences: Columns only in one.
set([*df4_comparison_csv]).symmetric_difference([*df4__final_from_csv])

#%%###################################

####### Compare values
### including row count, distinct ids, 

#%%
# Check rows & cols:
print(f'df4__final_from_csv Rows: {len(df4__final_from_csv)}')
print(f'df4_comparison_csv Rows: {len(df4_comparison_csv)}')

print(f'df4__final_from_csv Columns: {len(df4__final_from_csv.columns)}')
print(f'df4_comparison_csv Columns: {len(df4_comparison_csv.columns)}')

#%%
df4__final_from_csv == df4_comparison_csv

#%%
### Checking ID columns used in Join >> DF should be empty (meaning all the same).
df4_comp_compare = df4_comparison_csv[['Project Id','Year','Quarter']].compare(df4__final_from_csv[['Project Id','Year','Quarter']])
df4_comp_compare

###################################
###################################
###################################

#%%
### Now comparing ALL columns. DF created shows all differences:
df4_comp_compare = df4_comparison_csv.compare(df4__final_from_csv)
df4_comp_compare

#%%
### Number of columns with different values/types:
len([*df4_comp_compare]) / 2 
    ### Start: 50.

#%%
### Columns:
[*df4_comp_compare]




###################################
#### No change needed.
###################################

###########
### STILL show on list:

# var_to_compare = '_Zip' ### Var is Integer. Numbers the same. No functional difference. Python output missing initial 0 on a few ZIP Codes (that are probably errors). Tableau preserves first 0 because recognizes var as geographic.
# var_to_compare = 'Enroll Preg Status' ### Note: All dates functionally identical. TODO: Fix Mixture of date formats earlier in process. TODO: Figure out why python outputting mixture of dates & datetimes.
# var_to_compare = 'Poverty Level' ### Identical numbers. No functional difference. Tableau's output just removes any ".0" from floats.

###################################
#### Fixed above.
###################################

# var_to_compare = 'Annual Income' ### In Tableau set to decimal (& then drops all .0 in output), when really all are integers. Fixed by reading in as Integer.

# var_to_compare = '_FOB Involved' ### Code fixed above.

# var_to_compare = 'Caregiver Involved' ### Dependent on '_FOB Involved',  so now also fixed.
# var_to_compare = '_T11 Caregiver Employment' ### Dependent on '_T11 FOB Employment',  so now also fixed.
# var_to_compare = '_T10 Caregiver Educational Enrollment' ### Dependent on '_T10 FOB Educational Enrollment',  so now also fixed.
# var_to_compare = '_T07 Race' ### Dependent on '_T07 FOB Race',  so now also fixed.

# var_to_compare = '_T07 FOB Race' ### Fixed errors.

# var_to_compare = '_FOB Relation' ### Fixed above to match, but may need review (TODO).

###########
### STILL show on list:

# var_to_compare = '_T14 Poverty Percent' ### Tableau var rounded to 2 decimals. Fixed Python code to match. ### Even still, Python drops 0's at end of decimals. Functionally, numbers all the same.


###################################
#### TODO: Fix in Tableau / other process.
###################################

# var_to_compare = 'Fob Edu' ### Tableau only reading in NA (because incorrectly made integer). Python reading in true string values.
# var_to_compare = 'Mob Id' ### All NA in Tableau version (had been made integer); Python showing string IDs. Should be string.

###########
### STILL show on list:

# var_to_compare = 'Asq3 Referral 18Mm' ### Tableau reading/outputting as numeric when really is date.
# var_to_compare = 'Asq3 Referral 24Mm' ### Tableau reading/outputting as numeric when really is date.
# var_to_compare = 'Asq3 Referral 30Mm' ### Tableau reading/outputting as numeric when really is date.

###
### TODO: Fix. Seems to be an issue with these 2 Project IDs: 'hs123-1' & 'hs123-2' (each of which have 12 rows...)
    ### Python reads these in as strings because can't read in as dates. Whereas Tableau coercing to dates & turning strings to NA.
# var_to_compare = 'AD1InsChangeDate.9' ### Text mixed in with dates: 'hs123-1' & 'hs123-2'.
# var_to_compare = 'AD1InsChangeDate.10' ### Text mixed in with dates: 'hs123-1' & 'hs123-2'.
# var_to_compare = 'AD1InsChangeDate.11' ### Text mixed in with dates: 'hs123-1' & 'hs123-2'.
# var_to_compare = 'AD1InsChangeDate.12' ### Text mixed in with dates: 'hs123-1' & 'hs123-2'.
# var_to_compare = 'AD1InsChangeDate.13' ### Text mixed in with dates: 'hs123-1' & 'hs123-2'.
# var_to_compare = 'AD1InsChangeDate.14' ### Text mixed in with dates: 'hs123-1' & 'hs123-2'.
# var_to_compare = 'AD1InsChangeDate.15' ### Text mixed in with dates: 'hs123-1' & 'hs123-2'.
# var_list_for_comparison = [
#     'Year', 'Quarter', 
#     'AD1PrimaryIns.8', 'AD1InsChangeDate.8',
#     'AD1PrimaryIns.9', 'AD1InsChangeDate.9',
#     'AD1PrimaryIns.10', 'AD1InsChangeDate.10',
#     'AD1PrimaryIns.11', 'AD1InsChangeDate.11',
#     'AD1PrimaryIns.12', 'AD1InsChangeDate.12',
#     'AD1PrimaryIns.13', 'AD1InsChangeDate.13',
#     'AD1PrimaryIns.14', 'AD1InsChangeDate.14',
#     'AD1PrimaryIns.15', 'AD1InsChangeDate.15',
#     'AD1PrimaryIns.16', 'AD1InsChangeDate.16',
# ]

###################################
#### TODO: need work in Python.
###################################

# var_to_compare = '_T10 FOB Educational Enrollment' ### Edited to match comparison. TODO: FIX after comparisons.

# var_to_compare = '_T11 FOB Employment' ### Fixed one error in Python. BUT added error for comparison. ### TODO: fix after comparison (add back in value).

# var_to_compare = '_T14 Federal Poverty Categories' ### Python code actually correct. Reverted back to Tableau error for comparison. TODO: Fix after comparison.

###########
### STILL show on list:

# var_to_compare = 'p_fundingdate' ### Data actually datetimes with times, but Tableau truncated to only dates. TODO: Need to remove time data.


###################################
### investigation
###################################

#%%
### Columns still different:
[*df4_comp_compare]

# #%%
# dfTEST = df4_comparison_csv.compare(df4__final_from_csv, keep_equal=True, keep_shape=True)
# [*dfTEST]

#%%
### I do not understand why these are not showing up here [*df4_comp_compare] when they use to & should. 
    ### Is it because of reading in as string not object? Does that affect compare()?
    ### Yes. When read in as objects, with np.nan, compare finds these T09 vars when it didn't before.
    ### So, HOW to know these columns are an issue???
# var_to_compare = '_T09 Caregiver Education Status'
var_to_compare = '_T09 FOB Education Status'
### One side all NA.

# var_to_compare = 'www'

#######

var_list_for_comparison = [var_to_compare]

#%%

def fn_filter_with_na(df):
    if pd.isna(df[(var_to_compare, 'self')] != df[(var_to_compare, 'other')]):
        return True
    else:
        return df[(var_to_compare, 'self')] != df[(var_to_compare, 'other')]

print(
(
    df4_comparison_csv
    .compare(df4__final_from_csv, keep_equal=True, keep_shape=True)
    .loc[:, ['Project Id'] + var_list_for_comparison]
    # .loc[:, ['Project Id', 'Agency'] + var_list_for_comparison]
    # .loc[:, ['Project Id', 'Agency', 'Fob Involved', 'Fob Involved1'] + var_list_for_comparison]
    .dropna(how='all', subset=[(var_to_compare, 'self'), (var_to_compare, 'other')])
    .loc[lambda df: df.apply(fn_filter_with_na, axis=1), :]
    # .loc[(lambda df: df[(var_to_compare, 'self')] != df[(var_to_compare, 'other')]), :]
    # .loc[(lambda df: pd.isna(df[(var_to_compare, 'self')] != df[(var_to_compare, 'other')])), :]
    ##########
    ### Testing numeric vars:
    # .apply(lambda df: df[(var_to_compare, 'self')] == df[(var_to_compare, 'other')], axis=1) ### Outputs a Series.
    # .apply(lambda df: float(df[(var_to_compare, 'self')]) == float(df[(var_to_compare, 'other')]), axis=1)
    # .all()
    ##########
    ### Testing date vars:
    # .apply(lambda df: pd.to_datetime(df[(var_to_compare, 'self')]) == pd.to_datetime(df[(var_to_compare, 'other')]), axis=1)
    # .all()
# )) ### When using .all().
).to_string())


##########
#%%
# compare_col(df4_comparison_csv, df4__final_from_csv, var_to_compare, info_or_value_counts='info')
compare_col(df4_comparison_csv, df4__final_from_csv, var_to_compare, info_or_value_counts='value_counts')
#%%
inspect_col(df4__final_from_csv[var_to_compare]) 
#%%
inspect_col(df4_comparison_csv[var_to_compare]) 
#%%
inspect_col(df4_edits1[var_to_compare]) 
#%%
print(df4_comp_compare[[var_to_compare]].to_string())

###################################
### templates
###################################

# %%
# df4_comparison_csv[['Project Id', 'www']].compare(df4__final_from_csv[['Project Id', 'www']], keep_equal=True).loc[(lambda df: df[('www', 'self')] != df[('www', 'other')]), :]
# df4_comparison_csv[['www']].compare(df4__final_from_csv[['www']])

# #%%
# # df4_comparison_csv[['variable']].compare(df4__final_from_csv[['variable']])

# #%%
# (
#     df4_comparison_csv
#     .compare(df4__final_from_csv, keep_equal=True, keep_shape=True)
#     .loc[:, ['Project Id', 'Agency', 'variable']]
#     .dropna(how='all', subset=[('variable', 'self'), ('variable', 'other')])
#     .loc[(lambda df: df[('variable', 'self')] != df[('variable', 'other')]), :]
# )

# #%%
# (
#     df4_edits1
#     .sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)
#     .loc[(lambda df: pd.isna(df['Agency'])), ['Project Id', 'Agency', 'variable']]
# )


###################################
### investigation
###################################

#%%
### Columns still different:
[*df4_comp_compare]

# print(df4_comp_compare.to_string())

# #%%
# df4_comp_compare.columns.get_level_values(0)

#%%
df4__final_from_csv.drop(columns=list(df4_comp_compare.columns.get_level_values(0))) == df4_comparison_csv.drop(columns=list(df4_comp_compare.columns.get_level_values(0)))

#%%
def fn_check_if_same(df1value, df2value):
    if pd.isna(df1value) and pd.isna(df2value):
        return True 
    elif (pd.isna(df1value) and pd.notna(df2value)) or (pd.notna(df1value) and pd.isna(df2value)):
        return False
    else:
        df1value == df2value 

# df4__final_from_csv.applymap(fn_check_if_same(df4__final_from_csv, df4_comparison_csv))
# df4__final_from_csv.applymap(lambda x, y=df4_comparison_csv: fn_check_if_same(x, y))














### TODO: Compare all columns like above but considering NA's (maybe applymap).☺
### TODO: Check for "Unrecognized Value"s.


#%%
### Testing using output as Tableau Form 1 data source.
### These variables are broken:
# [_HomeVisitTypeAll]
# [_HomeVisitTypeIP]
# [_HomeVisitTypeV]
# [_T16 Number of In Person Home Visits by Primary Caregiver (unduplicated)]
# [_T16 Number of Virtual Home Visits by Primary Caregiver (unduplicated)]
# [_T16 Number of Visits by Primary Caregiver (unduplicated)]
### I believe the first 3 are renamed other variables & then turned from string to numeric.
    ### TODO: can I just create new versions with those names as Ints?




#%%
### END Adult4! SUCCESS!
