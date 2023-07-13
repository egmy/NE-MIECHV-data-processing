
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%
exec(open('RUNME.py').read())

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.

#%%##################################################
### PACKAGES ###
#####################################################

# import pandas as pd
# from pathlib import Path
# import numpy as np
# import sys
# import IPython

#%%##################################################
### Comparison File ###
#####################################################

df4_comparison_csv = pd.read_csv(path_4_comparison_csv, dtype=object, keep_default_na=False, na_values=[''])
df4_comparison_csv = df4_comparison_csv.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)

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
xlsx = pd.ExcelFile(path_4_data_source_file)

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

def fn_try_astype(fdf, dict_col_dtypes):
    for column in fdf.columns:
        try:
            fdf[column] = fdf[column].astype(dict_col_dtypes[column])
        except Exception as e:
            print('Error for column: ', column)
            print('Attempted dtype: ', dict_col_dtypes[column])
            print(e, '\n')

def fn_apply_dtypes(fdf, dict_col_dtypes):
    for column in fdf.columns:
        if (dict_col_dtypes[column] == 'datetime64[ns]'):
            fdf[column] = pd.to_datetime(fdf[column])
        else:
            fdf[column] = fdf[column].astype(dict_col_dtypes[column])
    return fdf 

#%%###################################
### Testing

# inspect_col(df4_2['AD1InsChangeDate.1'])
### In df4_2 tab "Caregiver Insurance", dates columns have 2 conflicting formats.

#%%
# fn_try_astype(df4_2, df4_2_col_dtypes)
# fn_apply_dtypes(df4_2, df4_2_col_dtypes)

#%%
# fn_try_astype(df4_3, df4_3_col_dtypes)
# fn_apply_dtypes(df4_3, df4_3_col_dtypes)

#%%
# fn_try_astype(df4_4, df4_4_col_dtypes)
# fn_apply_dtypes(df4_4, df4_4_col_dtypes)

#%%###################################
df4_1 = (
    df4_1
    ###.astype(df4_1_col_dtypes)
    .pipe(fn_apply_dtypes, df4_1_col_dtypes)
)
inspect_df(df4_1)

#%%###################################
df4_2 = (
    df4_2
    ###.astype(df4_2_col_dtypes)
    .pipe(fn_apply_dtypes, df4_2_col_dtypes)
)
inspect_df(df4_2)

#%%###################################
df4_3 = (
    df4_3
    ###.astype(df4_3_col_dtypes)
    .pipe(fn_apply_dtypes, df4_3_col_dtypes)
) 
inspect_df(df4_3)

#%%###################################
df4_4 = (
    df4_4
    ###.astype(df4_4_col_dtypes)
    .pipe(fn_apply_dtypes, df4_4_col_dtypes)
)
inspect_df(df4_4)

#%%###################################
df4_5 = (
    df4_5
    ###.astype(df4_5_col_dtypes)
    .pipe(fn_apply_dtypes, df4_5_col_dtypes)
)
inspect_df(df4_5)


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

#%%##################################################
### DUPLICATING


#%%##################################################
### COALESCING 


#%%##################################################
### DATE CALCULATIONS

### These calculations assume all date variables are dtype "datetime64".


#%%##################################################
### IF/ELSE, CASE/WHEN

### fdf == "function DataFrame"

#%%###################################

def fn__F1_Caregiver_ID_for_MOB_or_FOB(fdf):
    return True
    ### /// Tableau Calculation:
    ### CASE [MOB or FOB]
    ###     WHEN "MOB" THEN STR([__Primary Caregiver ID]) + "MOB"
    ###     WHEN "FOB" THEN STR([__Primary Caregiver ID]) + "FOB"
    ### END
df4_edits1['__F1 Caregiver ID for MOB or FOB'] = df4_edits1.apply(func=fn__F1_Caregiver_ID_for_MOB_or_FOB, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
inspect_col(df4_edits1['__F1 Caregiver ID for MOB or FOB']) 

#%%###################################

def fn_newvar(fdf):
    return True
    ### /// Tableau Calculation:

df4_edits1['newvar'] = df4_edits1.apply(func=fn_newvar, axis=1)###.astype('www') 
    ### Data Type in Tableau: 'www'.
inspect_col(df4_edits1['newvar']) 



##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################




