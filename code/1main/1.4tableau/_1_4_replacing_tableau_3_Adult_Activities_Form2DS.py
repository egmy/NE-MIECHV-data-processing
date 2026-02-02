
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.

#%%##################################################
### SETUP ###
#####################################################

import os 
from pathlib import Path

#%%
print('File that is running: ', os.path.basename(__file__))
import sys
path_1_3=Path(os.path.dirname(Path.cwd()))/'1.3combine/'
sys.path+=[str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']),str(path_1_3)] 

#%%
### The following is run if running this file by itself interactively (& ignored when run from RUNME):
if (os.path.basename(__file__) == '_1_4_replacing_tableau_3_Adult_Activities_Form2DS.py'):
    from _1_4tab_RUNME import * 
    print('Imported "_1_4tab_RUNME"')
else:
    print("Did NOT run RUNME again... because it's already running!")

print('____________\n')

#%%
bool_14t_deduplicate_tb3 = bool_14t_deduplicate  

#%%##################################################
### Comparison File ###
#####################################################

### As of Y13Q1, there is no comparison file because we are only using this python code!

### # df_14t_comparison_csv_tb3 = pd.read_csv(path_14t_comparison_csv_tb3, dtype='string', keep_default_na=False, na_values=list_na_values_to_read)
### df_14t_comparison_csv_tb3 = pd.read_csv(path_14t_comparison_csv_tb3, dtype='object', keep_default_na=False, na_values=list_na_values_to_read)
### print(f'df_14t_comparison_csv_tb3 Rows: {len(df_14t_comparison_csv_tb3)}')

### #%%
### ### Y12Q4 deduplicated rows to 3189 rows (but should be 3192?) vs. original comparison of 3581.
### if bool_14t_deduplicate_tb3:
###     df_14t_comparison_csv_tb3 = df_14t_comparison_csv_tb3.drop_duplicates(ignore_index=True) 
### print(f'df_14t_comparison_csv_tb3 Rows: {len(df_14t_comparison_csv_tb3)}')
### df_14t_comparison_csv_tb3 = df_14t_comparison_csv_tb3.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)

#%%##################################################
### COLUMN DEFINITIONS ###
#####################################################

#%%### df_14t_piece_tb3_1: 'Project ID'.
#%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
#%%### df_14t_piece_tb3_3: 'Family Wise'.
#%%### df_14t_piece_tb3_4: 'LLCHD'.

#######################
#%%### df_14t_piece_tb3_1: 'Project ID'.
list_14t_col_detail_tb3_1 = [
    ['join_id', 'Join Id', '', 'Int64'],
    ['project_id', 'Project Id', '', 'string'], 
    ['year', 'Year', '', 'Int64'], 
    ['quarter', 'Quarter', '', 'Int64']
]
#%%### df_14t_piece_tb3_1: 'Project ID'.
### For Renaming, we only need a dictionary of the columns with names changing.
### If x[2] == 'same' or x[0] == x[1] then that column is not included in df_colnames.
dict_14t_colnames_tb3_1 = {x[0]:x[1] for x in list_14t_col_detail_tb3_1 if x[2] != 'same' and x[0] != x[1]}
# dict_14t_colnames_tb3_1
#%%### df_14t_piece_tb3_1: 'Project ID'.
dict_14t_col_dtypes_tb3_1 = {x[0]:x[3] for x in list_14t_col_detail_tb3_1}
# print(dict_14t_col_dtypes_tb3_1)
# print(collections.Counter(list(dict_14t_col_dtypes_tb3_1.values())))

#######################
#%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
list_14t_col_detail_tb3_2 = [
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
    ['AD1InsChangeDate.16', 'AD1InsChangeDate.16', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.17', 'AD1PrimaryIns.16', 'same', 'string'],
    ['AD1InsChangeDate.17', 'AD1InsChangeDate.16', 'same', 'datetime64[ns]'],
    ['AD1PrimaryIns.18', 'AD1PrimaryIns.16', 'same', 'string'],
    ['AD1InsChangeDate.18', 'AD1InsChangeDate.16', 'same', 'datetime64[ns]']
]
#%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
dict_14t_colnames_tb3_2 = {x[0]:x[1] for x in list_14t_col_detail_tb3_2 if x[2] != 'same' and x[0] != x[1]}
# dict_14t_colnames_tb3_2
#%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
dict_14t_col_dtypes_tb3_2 = {x[0]:x[3] for x in list_14t_col_detail_tb3_2}
# print(dict_14t_col_dtypes_tb3_2)
# print(collections.Counter(list(dict_14t_col_dtypes_tb3_2.values())))

#######################
#%%### df_14t_piece_tb3_3: 'Family Wise'.
list_14t_col_detail_tb3_3 = [
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
    ['HomeVisitsPrenatal', 'Home Visits Prenatal', '', 'Int64'],
    ['HomeVisitsTotal', 'Home Visits Total', '', 'Int64'],
    ['HomeVisitTypeAll', 'Home Visit Type All', '', 'Int64'], ### New Y12Q4.
    ['HomeVisitTypeIP', 'Home Visit Type IP', '', 'Int64'], ### New Y12Q4.
    ['HomeVisitTypeV', 'Home Visit Type V', '', 'Int64'], ### New Y12Q4.
    ['MOBDOB', 'Mobdob', '', 'datetime64[ns]'],
    ['FOBDOB', 'Fobdob', '', 'datetime64[ns]'],
    ['MinEducationDate', 'Min Education Date', '', 'datetime64[ns]'],
    ['AD1MinEdu', 'AD1MinEdu', 'same', 'string'],
    ['MinEduEnroll', 'Min Edu Enroll', '', 'string'],
    ['MaxEduDate', 'Max Edu Date', '', 'datetime64[ns]'],
    ['AD1MaxEdu', 'AD1MaxEdu', 'same', 'string'],
    ['MaxEduEnroll', 'Max Edu Enroll', '', 'string'],
    ['DATEUNCOPE', 'Dateuncope', '', 'datetime64[ns]'], ### Y12Q3: Read in as object not datetime, probably because of 28 "00:00:00" values.
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
    ['need_exclusion6', 'Need Exclusion6', '', 'string'] ### Different from Adult4 Form1: 'need_exclusion6 (Family Wise)'.
]
#%%### df_14t_piece_tb3_3: 'Family Wise'.
dict_14t_colnames_tb3_3 = {x[0]:x[1] for x in list_14t_col_detail_tb3_3 if x[2] != 'same' and x[0] != x[1]}
# dict_14t_colnames_tb3_3
#%%### df_14t_piece_tb3_3: 'Family Wise'.
dict_14t_col_dtypes_tb3_3 = {x[0]:x[3] for x in list_14t_col_detail_tb3_3}
# print(dict_14t_col_dtypes_tb3_3)
# print(collections.Counter(list(dict_14t_col_dtypes_tb3_3.values())))

#######################
#%%### df_14t_piece_tb3_4: 'LLCHD'.
list_14t_col_detail_tb3_4 = [
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
    ['home_visits_pre', 'Home Visits Pre', '', 'Int64'],
    ['home_visits_post', 'Home Visits Post', '', 'Int64'],
    ['home_visits_person', 'Home Visits Person', '', 'Int64'],
    ['home_visits_virtual', 'Home Visits Virtual', '', 'Int64'],
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
    ['fob_edu', 'Fob Edu', '', 'string'], ### Tableau wb coerces to int -- but really is string data. Fixed Y12Q4.
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
    ['Has_ChildWelfareAdaptation', 'Has_ChildWelfareAdaptation', 'same', 'string'],
    ['CaseProgramID', 'Case Program ID', '', 'string'],
    ['case_id', 'Case ID', '', 'string'],
    ['tgt_identifier', 'TGT identifier', '', 'string'],
    ['cci_dt', 'CCI Dt', '', 'string']
]
#%%### df_14t_piece_tb3_4: 'LLCHD'.
dict_14t_colnames_tb3_4 = {x[0]:x[1] for x in list_14t_col_detail_tb3_4 if x[2] != 'same' and x[0] != x[1]}
# dict_14t_colnames_tb3_4
#%%### df_14t_piece_tb3_4: 'LLCHD'.
dict_14t_col_dtypes_tb3_4 = {x[0]:x[3] for x in list_14t_col_detail_tb3_4}
# print(dict_14t_col_dtypes_tb3_4)
# print(collections.Counter(list(dict_14t_col_dtypes_tb3_4.values())))


#%%##################################################
### READ ###
#####################################################

#%%
### Performance benefit for reading in file to memory only once by creating an ExcelFile class object.
if read_from_file==True:
    xlsx_14t_tb3 = pd.ExcelFile(path_14t_data_source_file_tb3)
    if (sorted(list_path_14t_data_source_sheets_tb3) == [x for x in sorted(xlsx_14t_tb3.sheet_names) if x != 'MOB or FOB']): 
        print('Passed Check that all Excel sheet names as expected.')
    else:
        raise Exception('**Check Failed: Unexpected Excel sheet names.')

    print('____________\n')


#%% 
### CHECK that all list_path_14t_data_source_sheets_tb3 same as xlsx.sheet_names (different order ok):
# print(sorted(list_path_14t_data_source_sheets_tb3))
# print([x for x in sorted(xlsx_14t_tb3.sheet_names) if x != 'MOB or FOB'])
#%%
### READ all sheets:
### df_14t_piece_tb3_1 = pd.read_excel(xlsx_14t_tb3, sheet_name=list_path_14t_data_source_sheets_tb3[0], keep_default_na=False, na_values=list_na_values_to_read)#, dtype=dict_14t_col_dtypes_tb3_1)
### df_14t_piece_tb3_2 = pd.read_excel(xlsx_14t_tb3, sheet_name=list_path_14t_data_source_sheets_tb3[1], keep_default_na=False, na_values=list_na_values_to_read)#, dtype=dict_14t_col_dtypes_tb3_2)
### df_14t_piece_tb3_3 = pd.read_excel(xlsx_14t_tb3, sheet_name=list_path_14t_data_source_sheets_tb3[2], keep_default_na=False, na_values=list_na_values_to_read)#, dtype={'FOBRaceAsian': 'boolean', 'FOBRaceBlack': 'boolean', 'FOBRaceHawaiianPacific': 'boolean', 'FOBRaceIndianAlaskan': 'boolean', 'FOBRaceOther': 'boolean', 'FOBRaceWhite': 'boolean'})#, dtype=dict_14t_col_dtypes_tb3_3)
### df_14t_piece_tb3_4 = pd.read_excel(xlsx_14t_tb3, sheet_name=list_path_14t_data_source_sheets_tb3[3], keep_default_na=False, na_values=list_na_values_to_read)#, dtype=dict_14t_col_dtypes_tb3_4)

#%%###################################
### READ in all sheets as strings.

### To read in EVERYTHING as a string WITH NA:
if read_from_file==True:

    df_14t_allstring_tb3_1 = pd.read_excel(xlsx_14t_tb3, sheet_name=list_path_14t_data_source_sheets_tb3[0], keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_14t_col_dtypes_tb3_1)
    df_14t_allstring_tb3_2 = pd.read_excel(xlsx_14t_tb3, sheet_name=list_path_14t_data_source_sheets_tb3[1], keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_14t_col_dtypes_tb3_2)
    df_14t_allstring_tb3_3 = pd.read_excel(xlsx_14t_tb3, sheet_name=list_path_14t_data_source_sheets_tb3[2], keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_14t_col_dtypes_tb3_3)
    df_14t_allstring_tb3_4 = pd.read_excel(xlsx_14t_tb3, sheet_name=list_path_14t_data_source_sheets_tb3[3], keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_14t_col_dtypes_tb3_4)

    df_14t_piece_tb3_1 = df_14t_allstring_tb3_1.copy()
    df_14t_piece_tb3_2 = df_14t_allstring_tb3_2.copy()
    df_14t_piece_tb3_3 = df_14t_allstring_tb3_3.copy()
    df_14t_piece_tb3_4 = df_14t_allstring_tb3_4.copy()

else:
    df_14t_allstring_tb3_1 = df_adult_project_id
    df_14t_allstring_tb3_2 = df_13_cg_ins
    df_14t_allstring_tb3_3 = df_13_adult_act
    df_14t_allstring_tb3_4 = df_13_base_table

    df_14t_piece_tb3_1 = df_14t_allstring_tb3_1.copy()
    df_14t_piece_tb3_2 = df_14t_allstring_tb3_2.copy()
    df_14t_piece_tb3_3 = df_14t_allstring_tb3_3.copy()
    df_14t_piece_tb3_4 = df_14t_allstring_tb3_4.copy()

#%%##################################################
### CLEAN ###
#####################################################

#%%###################################
### df_14t_piece_tb3_1: 'Project ID'.
print("Sheet 'Project ID':")
df_14t_piece_tb3_1 = (
    df_14t_piece_tb3_1
    .pipe(fn_apply_dtypes, dict_14t_col_dtypes_tb3_1)
)
# inspect_df(df_14t_piece_tb3_1)

#%%###################################
### df_14t_piece_tb3_2: 'Caregiver Insurance'.
print("Sheet 'Caregiver Insurance':")
df_14t_piece_tb3_2 = (
    df_14t_piece_tb3_2
    .pipe(fn_fix_mixed_date_dtypes, dict_14t_col_dtypes_tb3_2) ### Fixing date columns.
    .pipe(fn_apply_dtypes, dict_14t_col_dtypes_tb3_2)
)
# inspect_df(df_14t_piece_tb3_2)

#%%###################################
### df_14t_piece_tb3_3: 'Family Wise'.
print("Sheet 'Family Wise':")
df_14t_piece_tb3_3 = (
    df_14t_piece_tb3_3
    .pipe(fn_fix_mixed_date_dtypes, dict_14t_col_dtypes_tb3_3)
    .pipe(fn_apply_dtypes, dict_14t_col_dtypes_tb3_3)
) 
# inspect_df(df_14t_piece_tb3_3)

#%%###################################
### df_14t_piece_tb3_4: 'LLCHD'.
print("Sheet 'LLCHD':")
df_14t_piece_tb3_4 = (
    df_14t_piece_tb3_4
    .pipe(fn_fix_mixed_date_dtypes, dict_14t_col_dtypes_tb3_4)
    .pipe(fn_apply_dtypes, dict_14t_col_dtypes_tb3_4)
)
# inspect_df(df_14t_piece_tb3_4)

#%%###################################

print('____________\n')

#%%###################################
### Review each sheet:
### Note: Even empty DFs merge fine below.

# #%%### df_14t_piece_tb3_1: 'Project ID'.
# inspect_df(df_14t_piece_tb3_1)
# #%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
# inspect_df(df_14t_piece_tb3_2)
# #%%### df_14t_piece_tb3_3: 'Family Wise'.
# inspect_df(df_14t_piece_tb3_3)
# #%%### df_14t_piece_tb3_4: 'LLCHD'.
# inspect_df(df_14t_piece_tb3_4)

#%%##################################################
### Rename Columns ###
#####################################################

#######################
#%%### df_14t_piece_tb3_1: 'Project ID'.
### Rename df_14t_piece_tb3_1
# [*df_14t_piece_tb3_1]
# dict_14t_colnames_tb3_1
df_14t_piece_tb3_1 = df_14t_piece_tb3_1.rename(columns=dict_14t_colnames_tb3_1)
# [*df_14t_piece_tb3_1]

#######################
#%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
### Rename df_14t_piece_tb3_2
# [*df_14t_piece_tb3_2]
# dict_14t_colnames_tb3_2
df_14t_piece_tb3_2 = df_14t_piece_tb3_2.rename(columns=dict_14t_colnames_tb3_2)
# [*df_14t_piece_tb3_2]

#######################
#%%### df_14t_piece_tb3_3: 'Family Wise'.
### Rename df_14t_piece_tb3_3
# [*df_14t_piece_tb3_3]
# dict_14t_colnames_tb3_3
df_14t_piece_tb3_3 = df_14t_piece_tb3_3.rename(columns=dict_14t_colnames_tb3_3)
# [*df_14t_piece_tb3_3]

#######################
#%%### df_14t_piece_tb3_4: 'LLCHD'.
### Rename df_14t_piece_tb3_4
# [*df_14t_piece_tb3_4]
# dict_14t_colnames_tb3_4
df_14t_piece_tb3_4 = df_14t_piece_tb3_4.rename(columns=dict_14t_colnames_tb3_4)
# [*df_14t_piece_tb3_4]

#%%##################################################
### Prep for JOIN ###
#####################################################

### Each row SHOULD be unique on these sheets, especially the 'Project ID' sheet.

#%%### Restart deduplication
### df_14t_piece_tb3_1 = df_14t_bf_ddup_tb3_1.copy()
### df_14t_piece_tb3_2 = df_14t_bf_ddup_tb3_2.copy()
### df_14t_piece_tb3_3 = df_14t_bf_ddup_tb3_3.copy()
### df_14t_piece_tb3_4 = df_14t_bf_ddup_tb3_4.copy()

#######################
### NOTE: 24 duplicate rows. TODO: Fix in Master File creation.
#%%### df_14t_piece_tb3_1: 'Project ID'. 
### Backup:
df_14t_bf_ddup_tb3_1 = df_14t_piece_tb3_1.copy()
#%%### df_14t_piece_tb3_1: 'Project ID'. 
### Duplicate rows:
df_14t_piece_tb3_1[df_14t_piece_tb3_1.duplicated()]
#%%### df_14t_piece_tb3_1: 'Project ID'. 
### Dropping duplicate rows:
if bool_14t_deduplicate_tb3:
    df_14t_piece_tb3_1 = df_14t_piece_tb3_1.drop_duplicates(ignore_index=True)
df_14t_piece_tb3_1
#%%### df_14t_piece_tb3_1: 'Project ID'. 
### Test
len(df_14t_bf_ddup_tb3_1) - len(df_14t_piece_tb3_1) == len(df_14t_bf_ddup_tb3_1[df_14t_bf_ddup_tb3_1.duplicated()])
#%%### df_14t_piece_tb3_1: 'Project ID'. 
print('Project ID:')
if (len(df_14t_bf_ddup_tb3_1) != len(df_14t_piece_tb3_1)):
    print(f'{len(df_14t_bf_ddup_tb3_1) - len(df_14t_piece_tb3_1)} duplicate rows dropped.')
elif (len(df_14t_bf_ddup_tb3_1) == len(df_14t_piece_tb3_1)):
    print('No duplicate rows.')
else:
    print("Don't know what's going on here!")
#######################
#%%### df_14t_piece_tb3_1: 'Project ID'. 
### join columns: ['Project Id','Year','Quarter']
### Show rows where join columns are same BUT some other columns are not:
df_14t_piece_tb3_1[df_14t_piece_tb3_1[['Project Id','Year','Quarter']].duplicated(keep=False)]

#######################
### NOTE: NO duplicate rows.
#%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
df_14t_bf_ddup_tb3_2 = df_14t_piece_tb3_2.copy()
#%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
df_14t_piece_tb3_2[df_14t_piece_tb3_2.duplicated()]
# df_14t_piece_tb3_2[df_14t_piece_tb3_2.duplicated(keep=False, subset=['Project ID (ER Injury)','year (ER Injury)','quarter (ER Injury)'])]
#%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
if bool_14t_deduplicate_tb3:
    df_14t_piece_tb3_2 = df_14t_piece_tb3_2.drop_duplicates(ignore_index=True)
df_14t_piece_tb3_2
#%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
len(df_14t_bf_ddup_tb3_2) - len(df_14t_piece_tb3_2) == len(df_14t_bf_ddup_tb3_2[df_14t_bf_ddup_tb3_2.duplicated()])
#%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
print('Caregiver Insurance:')
if (len(df_14t_bf_ddup_tb3_2) != len(df_14t_piece_tb3_2)):
    print(f'{len(df_14t_bf_ddup_tb3_2) - len(df_14t_piece_tb3_2)} duplicate rows dropped.')
elif (len(df_14t_bf_ddup_tb3_2) == len(df_14t_piece_tb3_2)):
    print('No duplicate rows.')
else:
    print("Don't know what's going on here!")
#######################
#%%### df_14t_piece_tb3_2: 'Caregiver Insurance'.
### join columns: ['Project ID','year (Caregiver Insurance)','quarter (Caregiver Insurance)']
### Show rows where join columns are same BUT some other columns are not:
df_14t_piece_tb3_2[df_14t_piece_tb3_2[['Project ID','year (Caregiver Insurance)','quarter (Caregiver Insurance)']].duplicated(keep=False)]

#######################
### NOTE: 3 duplicate rows. TODO: Fix in Master File creation.
#%%### df_14t_piece_tb3_3: 'Family Wise'.
df_14t_bf_ddup_tb3_3 = df_14t_piece_tb3_3.copy()
#%%### df_14t_piece_tb3_3: 'Family Wise'.
df_14t_piece_tb3_3[df_14t_piece_tb3_3.duplicated()]
# df_14t_piece_tb3_3[df_14t_piece_tb3_3.duplicated(keep=False, subset=['Project ID','year (Family Wise)','quarter (Family Wise)'])]
#%%### df_14t_piece_tb3_3: 'Family Wise'.
if bool_14t_deduplicate_tb3:
    df_14t_piece_tb3_3 = df_14t_piece_tb3_3.drop_duplicates(ignore_index=True)
df_14t_piece_tb3_3
#%%### df_14t_piece_tb3_3: 'Family Wise'.
len(df_14t_bf_ddup_tb3_3) - len(df_14t_piece_tb3_3) == len(df_14t_bf_ddup_tb3_3[df_14t_bf_ddup_tb3_3.duplicated()])
#%%### df_14t_piece_tb3_3: 'Family Wise'.
print('Family Wise:')
if (len(df_14t_bf_ddup_tb3_3) != len(df_14t_piece_tb3_3)):
    print(f'{len(df_14t_bf_ddup_tb3_3) - len(df_14t_piece_tb3_3)} duplicate rows dropped.')
elif (len(df_14t_bf_ddup_tb3_3) == len(df_14t_piece_tb3_3)):
    print('No duplicate rows.')
else:
    print("Don't know what's going on here!")
#######################
#%%### df_14t_piece_tb3_3: 'Family Wise'.
### join columns: ['Project ID1','year (Family Wise)','quarter (Family Wise)']
### Show rows where join columns are same BUT some other columns are not:
cols_14t_forJoin_tb3_3 = ['Project ID1','year (Family Wise)','quarter (Family Wise)']

# print(df_14t_piece_tb3_3[df_14t_piece_tb3_3[cols_14t_forJoin_tb3_3].duplicated(keep=False)].to_string())
# print(df_14t_piece_tb3_3[df_14t_piece_tb3_3[cols_14t_forJoin_tb3_3].duplicated(keep=False)].sort_values(by=cols_14t_forJoin_tb3_3, ignore_index=True).to_string())
# print(df_14t_piece_tb3_3[df_14t_piece_tb3_3[cols_14t_forJoin_tb3_3].duplicated(keep=False)].query('`quarter (Family Wise)` == 4').sort_values(by=cols_14t_forJoin_tb3_3, ignore_index=True).to_string())

### Y12Q4: FULL: mostly groups of 4 rows. Some groups of 2 rows: ph535-1, ph548-1.
### Y12Q4: after filter to Y12Q4, only 28 rows.

#%%
# TESTdf_14t_piece_tb3_3 = df_14t_piece_tb3_3[df_14t_piece_tb3_3[cols_14t_forJoin_tb3_3].duplicated(keep=False)]
# TESTdf_14t_piece_tb3_3 = df_14t_piece_tb3_3[df_14t_piece_tb3_3[cols_14t_forJoin_tb3_3].duplicated(keep=False)].sort_values(by=cols_14t_forJoin_tb3_3, ignore_index=True)
TESTdf_14t_piece_tb3_3 = df_14t_piece_tb3_3[df_14t_piece_tb3_3[cols_14t_forJoin_tb3_3].duplicated(keep=False)].query('`quarter (Family Wise)` == 4').sort_values(by=cols_14t_forJoin_tb3_3, ignore_index=True)

# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[0:4], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[4:6], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[6:10], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[0:6], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[6:12], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[12:16], col].value_counts(dropna=False)) != 1)])

# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[0:2], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[2:4], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[4:8], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[8:12], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[12:16], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[16:20], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[20:24], col].value_counts(dropna=False)) != 1)])
# print([col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[24:28], col].value_counts(dropna=False)) != 1)])

# #%%### Change row indicies in 2 places:
# TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[0:4], cols_14t_forJoin_tb3_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[0:4], col].value_counts(dropna=False)) != 1)]]
# TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[4:6], cols_14t_forJoin_tb3_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[4:6], col].value_counts(dropna=False)) != 1)]]
# TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[0:6], cols_14t_forJoin_tb3_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[0:6], col].value_counts(dropna=False)) != 1)]]
#%%### Change row indicies in 2 places:
TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[0:2], cols_14t_forJoin_tb3_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[0:2], col].value_counts(dropna=False)) != 1)]]
#%%### Change row indicies in 2 places:
TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[2:4], cols_14t_forJoin_tb3_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[2:4], col].value_counts(dropna=False)) != 1)]]
#%%### Change row indicies in 2 places:
TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[4:8], cols_14t_forJoin_tb3_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[4:8], col].value_counts(dropna=False)) != 1)]]
#%%### Change row indicies in 2 places:
TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[8:12], cols_14t_forJoin_tb3_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[8:12], col].value_counts(dropna=False)) != 1)]]
#%%### Change row indicies in 2 places:
TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[12:16], cols_14t_forJoin_tb3_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[12:16], col].value_counts(dropna=False)) != 1)]]
#%%### Change row indicies in 2 places:
TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[16:20], cols_14t_forJoin_tb3_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[16:20], col].value_counts(dropna=False)) != 1)]]
#%%### Change row indicies in 2 places:
TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[20:24], cols_14t_forJoin_tb3_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[20:24], col].value_counts(dropna=False)) != 1)]]
#%%### Change row indicies in 2 places:
TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[24:28], cols_14t_forJoin_tb3_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb3_3] if col not in cols_14t_forJoin_tb3_3] if (len(TESTdf_14t_piece_tb3_3.loc[TESTdf_14t_piece_tb3_3.index[24:28], col].value_counts(dropna=False)) != 1)]]

### TODO: fix duplicates in Excel.

#######################
### NOTE: NO duplicate rows.
#%%### df_14t_piece_tb3_4: 'LLCHD'.
df_14t_bf_ddup_tb3_4 = df_14t_piece_tb3_4.copy()
#%%### df_14t_piece_tb3_4: 'LLCHD'.
df_14t_piece_tb3_4[df_14t_piece_tb3_4.duplicated()]
# df_14t_piece_tb3_4[df_14t_piece_tb3_4.duplicated(keep=False, subset=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)'])]
#%%### df_14t_piece_tb3_4: 'LLCHD'.
if bool_14t_deduplicate_tb3:
    df_14t_piece_tb3_4 = df_14t_piece_tb3_4.drop_duplicates(ignore_index=True)
df_14t_piece_tb3_4
#%%### df_14t_piece_tb3_4: 'LLCHD'.
len(df_14t_bf_ddup_tb3_4) - len(df_14t_piece_tb3_4) == len(df_14t_bf_ddup_tb3_4[df_14t_bf_ddup_tb3_4.duplicated()])
#%%### df_14t_piece_tb3_4: 'LLCHD'.
print('LLCHD:')
if (len(df_14t_bf_ddup_tb3_4) != len(df_14t_piece_tb3_4)):
    print(f'{len(df_14t_bf_ddup_tb3_4) - len(df_14t_piece_tb3_4)} duplicate rows dropped.')
elif (len(df_14t_bf_ddup_tb3_4) == len(df_14t_piece_tb3_4)):
    print('No duplicate rows.')
else:
    print("Don't know what's going on here!")
#######################
#%%### df_14t_piece_tb3_4: 'LLCHD'.
### join columns: ['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)']
### Show rows where join columns are same BUT some other columns are not:
df_14t_piece_tb3_4[df_14t_piece_tb3_4[['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)']].duplicated(keep=False)]

#######################
print('____________\n')


#%%##################################################
### JOIN ###
#####################################################

### TODO: Address "PerformanceWarning: DataFrame is highly fragmented." from running merge.

# #%%
# df_14t_base_tb3 = (
#     pd.merge(
#         df_14t_piece_tb3_1 ### 'Project ID'.
#         ,df_14t_piece_tb3_2 ### 'Caregiver Insurance'.
#         ,how='left'
#         ,left_on=['Project Id','Year','Quarter']
#         ,right_on=['Project ID','year (Caregiver Insurance)','quarter (Caregiver Insurance)']
#         ,indicator='LJ_tb3_2CI'
#         ,validate='one_to_one'
#     ).merge(
#         df_14t_piece_tb3_3 ### 'Family Wise'.
#         ,how='left'
#         ,left_on=['Project Id','Year','Quarter']
#         ,right_on=['Project ID1','year (Family Wise)','quarter (Family Wise)']
#         ,indicator='LJ_tb3_3FW'
#         # ,validate='one_to_one'
#     ).merge(
#         df_14t_piece_tb3_4 ### 'LLCHD'.
#         ,how='left'
#         ,left_on=['Project Id','Year','Quarter']
#         ,right_on=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)']
#         ,indicator='LJ_tb3_4LL'
#         # ,validate='one_to_one'
#     )
# ) 

#%%
### New join: issues: (1) df_14t_piece_tb3_3FW has has _ pairs of kind-of duplicate rows.
df_14t_base_tb3 = (
    pd.merge(
        df_14t_piece_tb3_1, ### 'Project ID'.
        df_14t_piece_tb3_4 ### 'LLCHD'.
        ,how='left'
        ,left_on=['Project Id','Year','Quarter']
        ,right_on=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)']
        ,indicator='LJ_tb3_4LL'
        # ,validate='one_to_one'
    ).merge(
        df_14t_piece_tb3_3 ### 'Family Wise'.
        ,how='left'
        ,left_on=['Project Id','Year','Quarter']
        ,right_on=['Project ID1','year (Family Wise)','quarter (Family Wise)']
        ,indicator='LJ_tb3_3FW'
        # ,validate='one_to_one' ### TODO: fix
    ).merge(
        df_14t_piece_tb3_2 ### 'Caregiver Insurance'.
        ,how='left'
        ,left_on=['Project Id','Year','Quarter']
        ,right_on=['Project ID','year (Caregiver Insurance)','quarter (Caregiver Insurance)']
        ,indicator='LJ_tb3_2CI'
        # ,validate='one_to_one' ### works for only LL... but does does it apply to LL?
    )
) 

print(f'After join: df_14t_base_tb3 Rows: {len(df_14t_base_tb3)}')
print('____________\n')

### Y12Q4: if NOT deduplicated = . If deduplicated = .


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
df_14t_edits1_tb3 = df_14t_base_tb3.copy()  ### Make a deep-ish copy of the DF's Data. Does NOT copy embedded mutable objects.


#####################################################
#####################################################
#####################################################
#%%##################################################

df_14t_edits1_tb3['Number of Records'] = 1 
# inspect_col(df_14t_edits1_tb3['Number of Records'])

#%%
df_14t_edits1_tb3['source'] = (
    df_14t_edits1_tb3
    .apply(func=(
        lambda df: 'FW' if pd.notna(df['Project ID1']) else ('LL' if pd.notna(df['project id (LLCHD)']) else 'um... problem')
    ), axis=1)
    .astype('string') 
)
# inspect_col(df_14t_edits1_tb3['source'])


#%%##################################################
### DUPLICATING

#%%##################################################
### COALESCING 1 

### Depended on by many variables below.
### In Child2 & Adult3.
df_14t_edits1_tb3['_Agency'] = df_14t_edits1_tb3['agency (Family Wise)'].combine_first(df_14t_edits1_tb3['Site Id']).astype('string') 
    ### IFNULL([agency (Family Wise)],[Site Id]) 
    ### Data Type in Tableau: 'string'.

df_14t_edits1_tb3['_Family ID'] = df_14t_edits1_tb3['Family Id'].combine_first(df_14t_edits1_tb3['Family Number']).astype('string') 
    ### IFNULL([Family Id], [Family Number]) 
    ### Data Type in Tableau: 'string'.

df_14t_edits1_tb3['_TGT ID'] = df_14t_edits1_tb3['Tgt Id'].combine_first(df_14t_edits1_tb3['Child Number']).astype('string') 
    ### IFNULL([Tgt Id],[Child Number]) 
    ### Data Type in Tableau: 'string'.

#%%###################################

### In Child2 & Adult3.
df_14t_edits1_tb3['_C13 Behavioral Concerns Asked'] = df_14t_edits1_tb3['BehaviorNumer'].combine_first(df_14t_edits1_tb3['Behavioral Concerns']).astype('Int64') 
    ### IFNULL([BehaviorNumer],[Behavioral Concerns]) 
    ### Data Type in Tableau: integer.

### TODO ASKJOE: Are the correct variables selected?
### In Child2 & Adult3.
df_14t_edits1_tb3['_C13 Behavioral Concerns Visits'] = df_14t_edits1_tb3['BehaviorDenom'].combine_first(df_14t_edits1_tb3['Home Visits Post']).astype('Int64') 
    ### IFNULL([BehaviorDenom],[Home Visits Post]) 
    ### Data Type in Tableau: integer.

df_14t_edits1_tb3['_C17 CESD Score'] = df_14t_edits1_tb3['Cesd Score'].combine_first(df_14t_edits1_tb3['CESD Total']).astype('Int64') 
    ### IFNULL([Cesd Score],[CESD Total]) 
    ### Data Type in Tableau: integer.

df_14t_edits1_tb3['_T16 Number of Home Visits'] = df_14t_edits1_tb3['Home Visits Total'].combine_first(df_14t_edits1_tb3['Home Visits Num']).astype('Int64') 
    ### IFNULL([Home Visits Total],[Home Visits Num]) 
    ### Data Type in Tableau: integer.

#%%###################################

df_14t_edits1_tb3['_C03 CES-D Date'] = df_14t_edits1_tb3['Cesd Dt'].combine_first(df_14t_edits1_tb3['Min Of CESDDATE']).astype('datetime64[ns]')
    ### IFNULL([Cesd Dt],[Min Of CESDDATE]) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb3['_C05 Postpartum Checkup Date'] = df_14t_edits1_tb3['Postpartum Checkup Date'].combine_first(df_14t_edits1_tb3['Prim Care Dt']).astype('datetime64[ns]')
    ### IFNULL([Postpartum Checkup Date],[Prim Care Dt]) 
    ### Data Type in Tableau: date.

### Need before: '_C06 Tobacco Referral Date'.
### A date indicates tobacco use.
df_14t_edits1_tb3['_C06 Tobacco Use Date'] = df_14t_edits1_tb3['Tobacco Use Date'].combine_first(df_14t_edits1_tb3['Tobacco Use Dt']).astype('datetime64[ns]')
    ### IFNULL([Tobacco Use Date],[Tobacco Use Dt]) //a date indicates tobacco use 
    ### Data Type in Tableau: date.

df_14t_edits1_tb3['_C06 Tobacco Referral Date'] = df_14t_edits1_tb3['Tobacco Ref Date'].combine_first(df_14t_edits1_tb3['_C06 Tobacco Use Date']).astype('datetime64[ns]')
    ### IFNULL([Tobacco Ref Date],[_C06 Tobacco Use Date])
    ### Data Type in Tableau: date.

df_14t_edits1_tb3['_C10 CHEEERS'] = df_14t_edits1_tb3['Max CHEEERS Date'].astype('datetime64[ns]')
    ### IFNULL([Cheeers Date],[Max CHEEERS Date]) 
    ### Data Type in Tableau: date. #NATHAN: there is not a Cheeers Date in tb3

df_14t_edits1_tb3['_C14 IPV Date'] = df_14t_edits1_tb3['IPV Assess Date'].combine_first(df_14t_edits1_tb3['Ipv Screen Dt']).astype('datetime64[ns]')
    ### IFNULL([IPV Assess Date],[Ipv Screen Dt]) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb3['_C17 MH Referral Date'] = df_14t_edits1_tb3['Ment Hlth Ref Dt'].combine_first(df_14t_edits1_tb3['Min Of MH Ref Date']).astype('datetime64[ns]')
    ### IFNULL([Ment Hlth Ref Dt],[Min Of MH Ref Date]) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb3['_C19 IPV Referral Date'] = df_14t_edits1_tb3['Ipv Referral Dt'].combine_first(df_14t_edits1_tb3['IPV Ref Date']).astype('datetime64[ns]')
    ### IFNULL([Ipv Referral Dt],[IPV Ref Date]) 
    ### Data Type in Tableau: date.

### In Child2 & Adult3.
df_14t_edits1_tb3['_Discharge Date'] = df_14t_edits1_tb3['Termination Date'].combine_first(df_14t_edits1_tb3['Discharge Dt']).astype('datetime64[ns]')
    ### IFNULL([Termination Date],[Discharge Dt]) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb3['_Enrollment Date'] = df_14t_edits1_tb3['Min Of HV Date'].combine_first(df_14t_edits1_tb3['Enroll Dt']).astype('datetime64[ns]')
    ### IFNULL([Min Of HV Date],[Enroll Dt]) 
    ### Data Type in Tableau: date.

### In Child2 & Adult3.
df_14t_edits1_tb3['_Max HV Date'] = df_14t_edits1_tb3['Max Of HV Date'].combine_first(df_14t_edits1_tb3['Last Home Visit']).astype('datetime64[ns]')
    ### IFNULL([Max Of HV Date],[Last Home Visit]) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb3['_UNCOPE Referral'] = df_14t_edits1_tb3['Substance Abuse Ref Dt'].combine_first(df_14t_edits1_tb3['UNCOPE Ref Date']).astype('datetime64[ns]')
    ### IFNULL([Substance Abuse Ref Dt], [UNCOPE Ref Date]) 
    ### Data Type in Tableau: date.

#%%###################################

### DONE: Fix 'Dateuncope' earlier in data sourcing process so does not have bad values. ### Resolved by setting data types at beginning.
df_14t_edits1_tb3['_UNCOPE Date'] = df_14t_edits1_tb3['Uncope Dt'].combine_first(df_14t_edits1_tb3['Dateuncope']).astype('datetime64[ns]')
    ### pd.to_datetime(df_14t_edits1_tb3['Dateuncope'].replace('00:00:00', np.nan).astype('string'))) ### OLD, from before setting dtypes.
    ###########
    ### /// Tableau Calculation:
    ### IFNULL([Uncope Dt],[Dateuncope]) 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb3['_UNCOPE Date'])
# #%%
# inspect_col(df_14t_edits1_tb3['Uncope Dt'])
# #%%
# inspect_col(df_14t_edits1_tb3['Dateuncope']) ### Y12Q3: Read in as object not datetime, probably because of 28 "00:00:00" values.
# #%%
# print(
# pd.to_datetime(
#     df_14t_edits1_tb3['Dateuncope']
#     .replace('00:00:00', np.nan)
#     .astype('string')
#     # .astype('datetime64[ns]')
# )
#     .to_string())

#%%###################################

### Similar variable In Child2. Basicaly dentical in Adult3 & Adult4 (except data type).
### 'Mob Zip' has the string value "null" that needs to be recoded. ### FY13Q1, other bad value seen.
df_14t_edits1_tb3['_Zip'] = (
    df_14t_edits1_tb3['Zip'].combine_first(df_14t_edits1_tb3['Mob Zip'])
    ### Remove all entries NOT matching the pattern:
    .replace(r'^(?!(\d{5})(-)?(\d{4})?).*$', pd.NA, regex=True)
    ### Only keep the first 5 digits:
    .replace(r'^(\d{5})(-)?(\d{4})?$', r'\1', regex=True)
    ###
    .astype('string') ### Difference.
)
    ### IFNULL([Zip],[Mob Zip]) 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_Zip'])
# #%%
# inspect_col(df_14t_edits1_tb3['Zip'])
# #%%
# inspect_col(df_14t_edits1_tb3['Mob Zip'])

ZIP_TO_COUNTY = {}

for county, zips in COUNTY_ZIPS.items():
    for z in zips:
        ZIP_TO_COUNTY[z] = county

df_14t_edits1_tb3['_Zip_int'] = (df_14t_edits1_tb3['_Zip']
    .astype('Int64')   # pandas nullable int
)

df_14t_edits1_tb3['_County'] = (df_14t_edits1_tb3['_Zip_int']
    .map(ZIP_TO_COUNTY)
    .astype('string')
)



#%%###################################

df_14t_edits1_tb3['_C15 Max Education Date'] = (df_14t_edits1_tb3['Mcafss Edu Dt2'].combine_first(df_14t_edits1_tb3['Max Edu Date'])).astype('datetime64[ns]')
    ### DATE(IFNULL([Mcafss Edu Dt2],[Max Edu Date])) 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb3['_C15 Max Education Date'])
    ### Is still a date.

#%%###################################

df_14t_edits1_tb3['_C15 Min Education Date'] = (df_14t_edits1_tb3['Mcafss Edu Dt1'].combine_first(df_14t_edits1_tb3['Min Education Date'])).astype('datetime64[ns]')
    ### DATE(IFNULL([Mcafss Edu Dt1],[Min Education Date])) 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb3['_C15 Min Education Date'])
    ### Is still a date.

#%%##################################################
### IF/ELSE, CASE/WHEN

### fdf == "function DataFrame"

#%%###################################

### Other vars depend on this.
### In Child2 & Adult3 & Adult4. Copied exactly.
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
    ###########
    ### /// Tableau Calculation:
    ### IF [Tgt Dob] = DATE(1/1/1900) THEN NULL //LLCHD
    ### ELSEIF [Tgt Dob-Cr] = DATE(1/1/1900) THEN NULL //FW
    ### ELSE IFNULL([Tgt Dob],[Tgt Dob-Cr])
    ### END
df_14t_edits1_tb3['_TGT DOB'] = df_14t_edits1_tb3.apply(func=fn_TGT_DOB, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb3['_TGT DOB'])
# #%%
# inspect_col(df_14t_edits1_tb3['Tgt Dob'])
# #%%
# inspect_col(df_14t_edits1_tb3['Tgt Dob-Cr'])

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
    ###########
    ### /// Tableau Calculation:
    # IF [Mob Dob] = DATE(1/1/1900) THEN NULL //LLCHD
    # ELSEIF [Mobdob] = DATE(1/1/1900) THEN NULL //FW
    # ELSE IFNULL([Mob Dob],[Mobdob])
    # END
df_14t_edits1_tb3['_MOB DOB'] = df_14t_edits1_tb3.apply(func=fn_MOB_DOB, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb3['_MOB DOB']) 

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
    ###########
    ### /// Tableau Calculation:
    # IF [Fob Dob] = DATE(1/1/1900) THEN NULL //LLCHD
    # ELSEIF [Fobdob] = DATE(1/1/1900) THEN NULL //FW
    # ELSE IFNULL([Fob Dob],[Fobdob])
    # END
df_14t_edits1_tb3['_FOB DOB'] = df_14t_edits1_tb3.apply(func=fn_FOB_DOB, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb3['_FOB DOB']) 

#%%###################################

### DONE: Move age calculations to Tableau Report Workbook. ### Joe ok!
### NOT Used in Form 2 (or Form 1). So REMOVING.
# def fn_T04_MOB_Age(fdf):
#     ### FIX because month calculations not calculation the same as in Tableau:
#     1
#     ###########
#     ### /// Tableau Calculation:
#     # IF [_MOB DOB]> DATEADD('year',-DATEDIFF('year',[_MOB DOB],TODAY()),TODAY())
#     # THEN DATEDIFF('year',[_MOB DOB],TODAY()-1)
#     # ELSE DATEDIFF('year',[_MOB DOB],TODAY())
#     # END
# df_14t_edits1_tb3['_T04 MOB Age'] = df_14t_edits1_tb3.apply(func=fn_T04_MOB_Age, axis=1).astype('Int64') 
#     ### Data Type in Tableau: integer.
# # inspect_col(df_14t_edits1_tb3['_T04 MOB Age']) 

#%%###################################

### DONE: Move age calculations to Tableau Report Workbook. ### Joe ok!
### NOT Used in Form 2 (or Form 1). So REMOVING.
# def fn_T04_FOB_Age(fdf):
#     ### FIX because month calculations not calculation the same as in Tableau:
#     1
#     ###########
#     ### /// Tableau Calculation:
#     # IF [_FOB DOB]> DATEADD('year',-DATEDIFF('year',[_FOB DOB],TODAY()),TODAY())
#     # THEN DATEDIFF('year',[_FOB DOB],TODAY()-1)
#     # ELSE DATEDIFF('year',[_FOB DOB],TODAY())
#     # END
# df_14t_edits1_tb3['_T04 FOB Age'] = df_14t_edits1_tb3.apply(func=fn_T04_FOB_Age, axis=1).astype('Int64') 
#     ### Data Type in Tableau: integer.
# # inspect_col(df_14t_edits1_tb3['_T04 FOB Age']) 

#%%###################################

### DONE: Confirm that LLCHD not using "Non-Binary" yet. If we get a new value, we can't assume what it means. Flag as "Unrecognized Value".
### DONE: Add "unrecognized value"
def fn_MOB_Gender(fdf):
    ### FW:
    if (fdf['source'] == 'FW'):
        match fdf['Adult1Gender']:
            case _ if pd.isna(fdf['Adult1Gender']):
                return pd.NA 
            case 'Female':
                return 'Female'
            case 'Male':
                return 'Male'
            case 'Non-Binary':
                return 'Non-Binary'
            case _:
                return "Unrecognized Value"
    ### LLCHD:
    elif (fdf['source'] == 'LL'):
        match fdf['Mob Gender']:
            case _ if pd.isna(fdf['Mob Gender']):
                return pd.NA 
            case 'F':
                return 'Female'
            case 'M':
                return 'Male'
            ### case 'N': ### Don't have this value yet - Confirm what means if comes through.
            ###     return 'Non-Binary' 
            case _:
                return f"Unrecognized Value: {fdf['Mob Gender']} "
    ###
    else:
        return "Unrecognized Value"
    ###########
    ### /// Tableau Calculation:
    # IF [Adult1Gender] = "Female" THEN "Female" //FW
    # ELSEIF [Adult1Gender] = "Male" THEN "Male"
    # ELSEIF [Adult1Gender] = "Non-Binary" THEN "Non-Binary"
    # ELSEIF [Mob Gender]= "F" THEN "Female" //LLCHD
    # ELSEIF [Mob Gender] = "M" THEN "Male"
    # // ELSEIF [Mob Gender] = "N" THEN "Non-Binary" // Don't have this value yet - Confirm
    # END
df_14t_edits1_tb3['_MOB Gender'] = df_14t_edits1_tb3.apply(func=fn_MOB_Gender, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_MOB Gender']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Mob Gender']) 


#%%###################################

### TODO: Question: Should we incorporate involved status into the fob variables? Answer: yes, should be checking Fob involvement first. 
    ### TODO: Check all FOB variables & make sure in logic.
### DONE: Confirm that LLCHD not using "Non-Binary" yet. If we get a new value, we can't assume what it means. Flag as "Unrecognized Value".
### DONE: Add "unrecognized value"
def fn_FOB_Gender(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            return fdf['Adult2Gender'] 
        else:
            return pd.NA ### probably MOB.
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == 'Y'):
            match fdf['Fob Gender']:
                case _ if pd.isna(fdf['Fob Gender']):
                    return pd.NA 
                case 'F':
                    return 'Female'
                case 'M':
                    return 'Male' 
                ### case 'N': ### Don't have this value yet - Confirm what means if comes through.
                ###     return 'Non-Binary' 
                case _:
                    return 'Unrecognized Value' 
        else:
            return pd.NA ### probably MOB.
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
    # //should we incorporate involved status into the fob variables?
    # IF [Fob Involved1] = "Y" THEN CASE[Fob Gender]
    #     WHEN "M" THEN "Male" //LLCHD
    #     WHEN "F" THEN "Female"
    #     // WHEN "N" THEN "Non-Binary" // No values yet - confirm
    #     END
    # ELSEIF [Fob Involved] = True THEN [Adult2Gender] //FW
    # ELSE NULL
    # END
df_14t_edits1_tb3['_FOB Gender'] = df_14t_edits1_tb3.apply(func=fn_FOB_Gender, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_FOB Gender']) 
#%%
# inspect_col(df_14t_edits1_tb3['Fob Involved1']) 
# inspect_col(df_14t_edits1_tb3['Fob Gender']) 
# inspect_col(df_14t_edits1_tb3['Fob Involved']) 
# inspect_col(df_14t_edits1_tb3['Adult2Gender']) 

def fn_Funding(fdf):
    if pd.notna(fdf['_Agency']):
        if (fdf['_Agency'] != "ll"):
            match fdf['Agency']:
                case _ if pd.isna(fdf['Agency']):
                    return pd.NA
                case "hs":
                    return "F"
                case "ph":
                    return "F"
                case "nc":
                    return "F"
                case "ps":
                    return "F"
                case "vn":
                    return "F"
                case "se":
                    return "F"
                case "lb":
                    return "F" ### Added Y12.
                case "tr":
                    return "F" ### Added Y13.
                case "sh":
                    return "F" ### Added Y13.
                case 'wc':
                    return 'TODO'
                case 'cd': ## Added Y13Q4
                    return "F" 
                case 'fc': ## Added Y13Q4
                    return "F"
                case "np":
                    return "F" ##Added Y13Q4
                case _:
                    return "Unrecognized Value"
    elif pd.notna(fdf['_Agency']):
        if (fdf['_Agency'] == "ll"):
            return fdf['Funding']
    ###########
    ### /// Tableau Calculation:
    ### IF [_Agency] <> "ll" THEN CASE [Agency]
    ###     WHEN "hs" THEN "F"
    ###     WHEN "ph" THEN "F"
    ###     WHEN "nc" THEN "S"
    ###     WHEN "ps" THEN "S"
    ###     WHEN "vn" THEN "S"
    ###     WHEN "se" THEN "TANF"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSEIF [_Agency] = "ll" THEN [Funding]
    ### END
df_14t_edits1_tb3['Funding'] = df_14t_edits1_tb3.apply(func=fn_Funding, axis=1).astype('string')

#%%###################################

def fn_T06_MOB_Ethnicity(fdf):
    ### FW.
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
    ### LLCHD.
    elif pd.notna(fdf['Mob Ethnicity']):
        match fdf['Mob Ethnicity'].lower():
            case "hispanic" | "hispanic/latino":
                return "Hispanic or Latino"
            case "non-hispanic" | "not hispanic/latino":
                return "Not Hispanic or Latino"
            case _:
                return "Unrecognized Value"
    ###
    else:
        return "Unknown/Did Not Report"
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_T06 MOB Ethnicity'] = df_14t_edits1_tb3.apply(func=fn_T06_MOB_Ethnicity, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T06 MOB Ethnicity'])
# #%% ### Run list_14t_unrecognized_values_tb3 code below first:
# ### DONE ### [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_T06 MOB Ethnicity'] 
# #%%
# inspect_col(df_14t_edits1_tb3['Mob Ethnic'])
# #%%
# inspect_col(df_14t_edits1_tb3['Mob Ethnicity'])

#%%###################################

### Slight difference between vars in original Tableau: "NON-HISPANIC" here instead of "NON-Hispanic" in (1).
def fn_T06_FOB_Ethnicity(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            if pd.isna(fdf['Fob Ethnicity']):
                return 'Unknown/Did Not Report'
            else: 
                match fdf['Fob Ethnicity'].lower():
                    case 'hispanic/latino':
                        return 'Hispanic or Latino'
                    case 'non hispanic/latino':
                        return 'Not Hispanic or Latino' 
                    case 'unknown':
                        return 'Unknown/Did Not Report'
                    case _:
                        return 'Unrecognized Value'
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == 'Y'):
            if pd.isna(fdf['Fob Ethnicity1']):
                return 'Unknown/Did Not Report'
            else: 
                match fdf['Fob Ethnicity1'].lower():
                    case 'hispanic/latino':
                        return 'Hispanic or Latino' 
                    case 'not hispanic/latino' | 'non-hispanic':
                        return 'Not Hispanic or Latino'
                    case 'unreported/refused to report':
                        return 'Unknown/Did Not Report'
                    case _:
                        return 'Unrecognized Value'
        else:
            return pd.NA 
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_T06 FOB Ethnicity'] = df_14t_edits1_tb3.apply(func=fn_T06_FOB_Ethnicity, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T06 FOB Ethnicity']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Fob Involved']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Fob Ethnicity']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Fob Involved1']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Fob Ethnicity1']) 
# #%% ### Run list_14t_unrecognized_values_tb3 code below first:
# ### DONE ### [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_T06 FOB Ethnicity']


#%%###################################

### TODO: Check: Is this a Duplicate? Is it Used? ### In Tableau - check & delete var not used.
    ### Slight difference between vars in original Tableau: "NON-Hispanic" in (1) vs "NON-HISPANIC" in original.
    ### However, duplicate Python code because of the ".lower()".
# def fn_T06_FOB_Ethnicity_1(fdf):
#     ### FW.
#     if (fdf['Fob Involved'] == True):
#         if pd.isna(fdf['Fob Ethnicity']):
#             return "Unknown/Did Not Report"
#         else: 
#             match fdf['Fob Ethnicity'].lower():
#                 case "non hispanic/latino":
#                     return "Not Hispanic or Latino" 
#                 case "hispanic/latino":
#                     return "Hispanic or Latino"
#                 case "unknown":
#                     return "Unknown/Did Not Report"
#                 ### case pd.NA:
#                 ###     return "Unknown/Did Not Report" ### Pulled out above.
#                 case _:
#                     return "Unrecognized Value"
#     ### LLCHD.
#     elif (fdf['Fob Involved1'] == "Y"):
#         if pd.isna(fdf['Fob Ethnicity1']):
#             return "Unknown/Did Not Report"
#         else: 
#             match fdf['Fob Ethnicity1'].lower():
#                 case "hispanic/latino":
#                     return "Hispanic or Latino" 
#                 case "not hispanic/latino" | "non-hispanic":
#                     return "Not Hispanic or Latino"
#                 case "unreported/refused to report":
#                     return "Unknown/Did Not Report"
#                 ### case pd.NA:
#                 ###     return "Unknown/Did Not Report" ### Pulled out above.
#                 case _:
#                     return "Unrecognized Value"
#     ###
#     else:
#         return pd.NA 
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_T06 FOB Ethnicity (1)'] = df_14t_edits1_tb3.apply(func=fn_T06_FOB_Ethnicity, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T06 FOB Ethnicity (1)']) 
### TODO: Check this is one used in Tableau Report, then delete other (changelog). Only one should be used.

#%%###################################

def fn_T07_MOB_Race(fdf):
    ###########
    ### FW (FW race variables are boolean).
    if (fdf['source'] == 'FW'):
        ### multiracial.
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
            return 'More than one race'
        ### single race.
        elif (False if pd.isna(fdf['MOB Race Asian']) else (True if fdf['MOB Race Asian'] else False)):
            return 'Asian'
        elif (False if pd.isna(fdf['MOB Race Black']) else (True if fdf['MOB Race Black'] else False)):
            return 'Black or African American'
        elif (False if pd.isna(fdf['MOB Race Hawaiian Pacific']) else (True if fdf['MOB Race Hawaiian Pacific'] else False)):
            return 'Native Hawaiian or Other Pacific Islander'
        elif (False if pd.isna(fdf['MOB Race Indian Alaskan']) else (True if fdf['MOB Race Indian Alaskan'] else False)):
            return 'American Indian or Alaska Native'
        elif (False if pd.isna(fdf['MOB Race White']) else (True if fdf['MOB Race White'] else False)):
            return 'White'
        elif (False if pd.isna(fdf['MOB Race Other']) else (True if fdf['MOB Race Other'] else False)):
            return 'Other'
        else:
            return 'Unknown/Did Not Report'
    ###########
    ### LLCHD (LL race variables are strings).
    elif (fdf['source'] == 'LL'):
        ### multiracial.
        if (
            (
                (0 if pd.isna(fdf['Mob Race Asian']) else (1 if fdf['Mob Race Asian']=='Y' else 0)) + 
                (0 if pd.isna(fdf['Mob Race Black']) else (1 if fdf['Mob Race Black']=='Y' else 0)) + 
                (0 if pd.isna(fdf['Mob Race Hawaiian']) else (1 if fdf['Mob Race Hawaiian']=='Y' else 0)) + 
                (0 if pd.isna(fdf['Mob Race Indian']) else (1 if fdf['Mob Race Indian']=='Y' else 0)) + 
                (0 if pd.isna(fdf['Mob Race White']) else (1 if fdf['Mob Race White']=='Y' else 0)) + 
                (0 if pd.isna(fdf['Mob Race Other']) else (1 if fdf['Mob Race Other']=='Y' else 0)) 
            ) > 1 
        ):
            return 'More than one race'
        ### single race.
        elif (False if pd.isna(fdf['Mob Race Asian']) else (True if fdf['Mob Race Asian']=='Y' else False)):
            return 'Asian'
        elif (False if pd.isna(fdf['Mob Race Black']) else (True if fdf['Mob Race Black']=='Y' else False)):
            return 'Black or African American'
        elif (False if pd.isna(fdf['Mob Race Hawaiian']) else (True if fdf['Mob Race Hawaiian']=='Y' else False)):
            return 'Native Hawaiian or Other Pacific Islander'
        elif (False if pd.isna(fdf['Mob Race Indian']) else (True if fdf['Mob Race Indian']=='Y' else False)):
            return 'American Indian or Alaska Native'
        elif (False if pd.isna(fdf['Mob Race White']) else (True if fdf['Mob Race White']=='Y' else False)):
            return 'White'
        elif (False if pd.isna(fdf['Mob Race Other']) else (True if fdf['Mob Race Other']=='Y' else False)):
            return 'Other'
        else:
            return 'Unknown/Did Not Report'
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_T07 MOB Race'] = df_14t_edits1_tb3.apply(func=fn_T07_MOB_Race , axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T07 MOB Race']) 

#%%###################################

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
                return 'More than one race'
            ### single race.
            elif (False if pd.isna(fdf['FOB Race Asian']) else (True if fdf['FOB Race Asian'] else False)):
                return 'Asian'
            elif (False if pd.isna(fdf['FOB Race Black']) else (True if fdf['FOB Race Black'] else False)):
                return 'Black or African American'
            elif (False if pd.isna(fdf['FOB Race Hawaiian Pacific']) else (True if fdf['FOB Race Hawaiian Pacific'] else False)):
                return 'Native Hawaiian or Other Pacific Islander'
            elif (False if pd.isna(fdf['FOB Race Indian Alaskan']) else (True if fdf['FOB Race Indian Alaskan'] else False)):
                return 'American Indian or Alaska Native'
            elif (False if pd.isna(fdf['FOB Race White']) else (True if fdf['FOB Race White'] else False)):
                return 'White'
            elif (False if pd.isna(fdf['FOB Race Other']) else (True if fdf['FOB Race Other'] else False)):
                return 'Other'
            else:
                return 'Unknown/Did Not Report'
        else:
            return pd.NA 
    ###########
    ### LLCHD (LL race variables are strings).
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == 'Y'):
            ### multiracial.
            if (
                (
                    (0 if pd.isna(fdf['Fob Race Asian']) else (1 if fdf['Fob Race Asian']=='Y' else 0)) + 
                    (0 if pd.isna(fdf['Fob Race Black']) else (1 if fdf['Fob Race Black']=='Y' else 0)) + 
                    (0 if pd.isna(fdf['Fob Race Hawaiian']) else (1 if fdf['Fob Race Hawaiian']=='Y' else 0)) + 
                    (0 if pd.isna(fdf['Fob Race Indian']) else (1 if fdf['Fob Race Indian']=='Y' else 0)) + 
                    (0 if pd.isna(fdf['Fob Race White']) else (1 if fdf['Fob Race White']=='Y' else 0)) + 
                    (0 if pd.isna(fdf['Fob Race Other']) else (1 if fdf['Fob Race Other']=='Y' else 0)) 
                ) > 1 
            ):
                return 'More than one race'
            ### single race.
            elif (False if pd.isna(fdf['Fob Race Asian']) else (True if fdf['Fob Race Asian']=='Y' else False)):
                return 'Asian'
            elif (False if pd.isna(fdf['Fob Race Black']) else (True if fdf['Fob Race Black']=='Y' else False)):
                return 'Black or African American'
            elif (False if pd.isna(fdf['Fob Race Hawaiian']) else (True if fdf['Fob Race Hawaiian']=='Y' else False)):
                return 'Native Hawaiian or Other Pacific Islander'
            elif (False if pd.isna(fdf['Fob Race Indian']) else (True if fdf['Fob Race Indian']=='Y' else False)):
                return 'American Indian or Alaska Native'
            elif (False if pd.isna(fdf['Fob Race White']) else (True if fdf['Fob Race White']=='Y' else False)):
                return 'White'
            elif (False if pd.isna(fdf['Fob Race Other']) else (True if fdf['Fob Race Other']=='Y' else False)):
                return 'Other'
            else:
                return 'Unknown/Did Not Report'
        else:
            return pd.NA 
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_T07 FOB Race'] = df_14t_edits1_tb3.apply(func=fn_T07_FOB_Race, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T07 FOB Race']) 

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
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_T08 MOB Marital Status'] = df_14t_edits1_tb3.apply(func=fn_T08_MOB_Marital_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T08 MOB Marital Status']) 

#%%###################################

### DONE: Fix ERROR: using ['Mob Marital Status'] when should be using ['Fob Marital Status']. ### Answer: Go ahead & fix.
def fn_T08_FOB_Marital_Status(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            if pd.isna(fdf['Adult2MaritalStatus']):
                return 'Unknown/Did Not Report'
            else:
                match fdf['Adult2MaritalStatus'].lower():
                    case 'married':
                        return 'Married'
                    case 'living with partner':
                        return 'Not Married but Living Together with Partner'
                    case 'separated' | 'legally separated' | 'divorced' | 'widowed':
                        return 'Separated, Divorced, or Widowed'
                    case 'single':
                        return 'Never Married'
                    case 'unknown' | 'null':
                        return 'Unknown/Did Not Report'
                    case _:
                        return 'Unrecognized Value'
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == 'Y'):
            if pd.isna(fdf['Fob Marital Status']):
                return 'Unknown/Did Not Report'
            else:
                match fdf['Fob Marital Status'].lower():
                    case 'married':
                        return 'Married'
                    case 'living with partner' | 'life partner':
                        return 'Not Married but Living Together with Partner'
                    case 'separated' | 'legally separated' | 'divorced' | 'widowed':
                        return 'Separated, Divorced, or Widowed'
                    case 'single' | 'not married':
                        return 'Never Married'
                    case 'unknown' | 'null':
                        return 'Unknown/Did Not Report'
                    case _:
                        return 'Unrecognized Value'
        else:
            return pd.NA 
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_T08 FOB Marital Status'] = df_14t_edits1_tb3.apply(func=fn_T08_FOB_Marital_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T08 FOB Marital Status']) 

#%%###################################

def fn_T11_MOB_Employment(fdf):
    ###########
    ### FW.
    if (pd.notna(fdf['AD1EmpStatus'])):
        match fdf['AD1EmpStatus'].lower():
            case "employed full time" | "maternal leave, paid, full time" | "maternal leave, unpaid, full time"|"full time": #Y13Q4: added 'full time'
                return "Employed Full Time"
            case "employed part time" | "maternal leave, unpaid, part time" | "self-employed"|"part time":#Y13Q4: added 'part time'
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
            case 3:
                return 'Employed Part Time' 
            case 4 | 5:
                return 'Employed Full Time'
            case _:
                return "Unrecognized Value"
    ###########
    else:
        return "Unknown/Did Not Report"
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_T11 MOB Employment'] = df_14t_edits1_tb3.apply(func=fn_T11_MOB_Employment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T11 MOB Employment']) 

#%%###################################

def fn_T11_FOB_Employment(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            if pd.isna(fdf['AD2EmployStatus']):
                return 'Unknown/Did Not Report'
            else:
                match fdf['AD2EmployStatus'].lower():
                    case 'employed full time' | 'maternal leave, paid, full time' | 'maternal leave, unpaid, full time'|"full time":
                        return 'Employed Full Time'
                    case 'employed part time' | 'maternal leave, unpaid, part time' | 'self-employed'|"part time": #Y13Q4: added full time and part time
                        return 'Employed Part Time'
                    case (
                        'temporary disability' | ### TODO ASKJOE: new value -- I believe it goes here.
                        'permanent disability' |
                        'unemployed - unspecified' |
                        'unemployed not seeking work-barriers' |
                        'unemployed not seeking work-preference' |
                        'unemployed not seeking work-teen caregiver' |
                        'unemployed seeking work'
                    ):
                        return 'Not Employed'
                    case 'unknown' | 'null':
                        return 'Unknown/Did Not Report'
                    case _:
                        return 'Unrecognized Value'
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == 'Y'):
            if pd.isna(fdf['Fob Employ']):
                return 'Unknown/Did Not Report'
            else:
                match fdf['Fob Employ']:
                    case 1 | 2:
                        return 'Not Employed'
                    case 3:
                        return 'Employed Part Time' 
                    case 4 | 5:
                        return 'Employed Full Time'
                    case _:
                        return 'Unrecognized Value'
        else:
            return pd.NA 
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_T11 FOB Employment'] = df_14t_edits1_tb3.apply(func=fn_T11_FOB_Employment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T11 FOB Employment']) 

#%%###################################

### TODO ASKJOE: Clarify with LL team -- on all education variables.
    ### TODO: mimic new logic.
### DONE: Ask about significant difference from '_C15 Max Educational Enrollment'.
    ### Answer: In Form 2, max-min is checked to see change over time.
def fn_C15_Min_Educational_Enrollment(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Min Edu Enroll']:
            case _ if pd.isna(fdf['Min Edu Enroll']):
                return 'Unknown/Did not Report' ### TODO ASKJOE: Maybe change to 'Unrecognized Value'?
            case 'College 2 Year' | 'College 4 Year' | 'ESL' | 'Graduate School' | 'Vocational College':
                return 'Student/trainee'
            case 'GED Program' | 'High/Middle School':
                return 'Student/trainee HS/GED'
            case 'Not Enrolled in School':
                return 'Not a student/trainee'
            case 'Unknown':
                return 'Unknown/Did not Report'
            case _:
                return 'Unrecognized Value'
    ###########
    ### LLCHD.
    elif fdf['source'] == 'LL':
        # Using parentheses to properly group conditions
        if (
            pd.isna(fdf['mcafss_edu1_enroll']) |
            (
                (fdf['mcafss_edu1_enroll'] == 'Y') | (fdf['mcafss_edu1_enroll'] == 'N') & 
                (
                    pd.isna(fdf['mcafss_edu1_prog']) |
                    (fdf['mcafss_edu1_prog'] == 8) |
                    (fdf['mcafss_edu1_prog'] == 9) |
                    (fdf['mcafss_edu1_prog'] == 10)
                )
            )
        ):
            return 'Unknown/Did not Report'
        elif (
            fdf['mcafss_edu1_enroll'] == 'Y' ### Enrolled. ### Y12Q4 changed from 'YES'.
            and
            (
                fdf['mcafss_edu1_prog'] == 1 ### Enrolled in Middle School.
                or
                fdf['mcafss_edu1_prog'] == 2 ### Enrolled in High School.
                or
                fdf['mcafss_edu1_prog'] == 3 ### Enrolled in GED.
            )
        ):
            return 'Student/trainee HS/GED' 
        ### Added Y12Q4.
        elif (
            fdf['mcafss_edu1_enroll'] == 'Y' ### Enrolled. ### Y12Q4 changed from 'YES'.
            and
            (
                fdf['mcafss_edu1_prog'] == 4 ### ESL.
                or
                fdf['mcafss_edu1_prog'] == 5 ### Adult education in basic reading or math.
                or
                fdf['mcafss_edu1_prog'] == 6 ### College.
                or
                fdf['mcafss_edu1_prog'] == 7 ### Vocational training, technical or trade school (excluding training received during HS).
            )
        ):
            return 'Student/trainee' 
        elif (fdf['mcafss_edu1_enroll'] == 'N'): ### Y12Q4 changed from 'NO'.
            return 'Not a student/trainee' ### Y12Q4 Changed to match '_C15 Max Educational Enrollment'.
        else:
            ### For example: (pd.notna(fdf['mcafss_edu1_prog']) and fdf['mcafss_edu1_prog'] not in [1,2,3,4,5,6,7]) 
            return F'Unrecognized: {fdf['mcafss_edu1_prog']}' ### TODO ASKJOE: see if this is wanted.
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_C15 Min Educational Enrollment'] = df_14t_edits1_tb3.apply(func=fn_C15_Min_Educational_Enrollment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C15 Min Educational Enrollment']) 
#%%
# inspect_col(df_14t_edits1_tb3['mcafss_edu1_prog']) 
# inspect_col(df_14t_edits1_tb3['mcafss_edu1_enroll']) ### Only "Y" & "N".
# print(df_14t_edits1_tb3[['_C15 Min Educational Enrollment', 'Min Edu Enroll', 'mcafss_edu1_enroll', 'mcafss_edu1_prog']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
# print(df_14t_edits1_tb3[['Min Edu Enroll', 'mcafss_edu1_enroll', 'mcafss_edu1_prog']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())

#%%###################################

def fn_C15_Max_Educational_Enrollment(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Max Edu Enroll']:
            case _ if pd.isna(fdf['Max Edu Enroll']):
                return 'Unknown/Did not Report' ### TODO ASKJOE: Maybe change to 'Unrecognized Value'?
            case 'College 2 Year' | 'College 4 Year' | 'ESL' | 'Graduate School' | 'Vocational College':
                return 'Student/trainee'
            case 'GED Program' | 'High/Middle School':
                return 'Student/trainee HS/GED'
            case 'Not Enrolled in School':
                return 'Not a student/trainee'
            case 'Unknown':
                return 'Unknown/Did not Report'
            case _:
                return 'Unrecognized Value'
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if (
            pd.isna(fdf['mcafss_edu1_enroll']) |
            (
                (fdf['mcafss_edu1_enroll'] == 'Y') | (fdf['mcafss_edu1_enroll'] == 'N') & 
                (
                    pd.isna(fdf['mcafss_edu1_prog']) |
                    (fdf['mcafss_edu1_prog'] == 8) |
                    (fdf['mcafss_edu1_prog'] == 9) |
                    (fdf['mcafss_edu1_prog'] == 10)
                )
            )
        ):
            return 'Unknown/Did not Report' ### TODO ASKJOE: see if this is wanted.
        elif (
            fdf['mcafss_edu2_enroll'] == 'Y' ### Enrolled. ### Y12Q4 changed from 'YES'.
            and
            (
                fdf['mcafss_edu2_prog'] == 1 ### Enrolled in Middle School.
                or
                fdf['mcafss_edu2_prog'] == 2 ### Enrolled in High School.
                or
                fdf['mcafss_edu2_prog'] == 3 ### Enrolled in GED.
            )
        ):
            return 'Student/trainee HS/GED' 
        ### Added Y12Q4.
        elif (
            fdf['mcafss_edu2_enroll'] == 'Y' ### Enrolled. ### Y12Q4 changed from 'YES'.
            and
            (
                fdf['mcafss_edu2_prog'] == 4 ### ESL.
                or
                fdf['mcafss_edu2_prog'] == 5 ### Adult education in basic reading or math.
                or
                fdf['mcafss_edu2_prog'] == 6 ### College.
                or
                fdf['mcafss_edu2_prog'] == 7 ### Vocational training, technical or trade school (excluding training received during HS).
            )
        ):
            return 'Student/trainee' 
        elif (fdf['mcafss_edu2_enroll'] == 'N'): ### Y12Q4 changed from 'NO'.
            return 'Not a student/trainee' ### Y12Q4 Changed to match '_C15 Max Educational Enrollment'.
        else:
            ### For example: (pd.notna(fdf['mcafss_edu2_prog']) and fdf['mcafss_edu2_prog'] not in [1,2,3,4,5,6,7]) 
            return f'Unrecognized: {fdf['mcafss_edu2_prog']}' ### TODO ASKJOE: see if this is wanted.
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_C15 Max Educational Enrollment'] = df_14t_edits1_tb3.apply(func=fn_C15_Max_Educational_Enrollment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C15 Max Educational Enrollment']) 
# #%%
# print(df_14t_edits1_tb3[['_C15 Max Educational Enrollment', 'Max Edu Enroll', 'mcafss_edu2_enroll', 'mcafss_edu2_prog']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())

#%%###################################

### TODO: Fix FOB variables.
def fn_T10_FOB_Educational_Enrollment(fdf):
    ### max.
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        if pd.isna(fdf['Fob Involved']):
            return pd.NA 
        elif (fdf['Fob Involved'] == True):
            if pd.isna(fdf['AD2InSchool']):
                return 'Unknown/Did Not Report'
            else:
                match fdf['AD2InSchool'].lower():
                    case (
                        'esl' |
                        'ged program' |
                        'high/middle school' |
                        'vocational college' |
                        'college 2 year' |
                        'college 4 year' |
                        'graduate school'
                    ):
                        return 'Student/trainee' 
                    case 'not enrolled in school':
                        return 'Not a student/trainee'
                    case 'unknown':
                        return 'Unknown/Did Not Report'
                    case _:
                        return pd.NA 
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == 'Y'):
            return 'Unknown/Did Not Report' ### Placeholder. ### TODO: need to fix all vars that use 'Fob Edu'.
            # if pd.isna(fdf['Fob Edu']):
            #     return 'Unknown/Did Not Report'
            # else:
            #     match fdf['Fob Edu']:
            #         case 1 | 9:
            #             return 'Student/trainee'
            #         case 2 | 3 | 4 | 5 | 6 | 7 | 8 | 10 | 11:
            #             return 'Not a student/trainee'
            #         case 12:
            #             return 'Unknown/Did Not Report'
            #         ### case pd.NA:
            #         ###     return 'Unknown/Did Not Report'
        ###########
                # ### TODO ASKJOE: this copied from var below. How to recode the above?
                # match fdf['Fob Edu']:
                #     # case 1 | 2:
                #     case 'Less than 8' | '8-11':
                #         return 'Less than HS diploma'
                #     # case 3 | 4:
                #     case 'HS Grad' | 'GED':
                #         return 'HS diploma/GED'
                #     # case 5:
                #     case 'Vocational school after HS':
                #         return 'Vocational School after High School'
                #     # case 6:
                #     case 'Some college':
                #         return 'Some college/training'
                #     # case 7:
                #     case 'Associates degree':
                #         return 'Associates Degree' 
                #     # case 8:
                #     case 'Bachelor’s degree or higher':
                #         return 'Bachelor's Degree or Higher'
                #     # case 0:
                #     case _:
                #         return 'Unrecognized Value'
        else:
            return pd.NA 
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_T10 FOB Educational Enrollment'] = df_14t_edits1_tb3.apply(func=fn_T10_FOB_Educational_Enrollment, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T10 FOB Educational Enrollment']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Fob Edu']) 
# #%%
# print(df_14t_edits1_tb3[['Fob Involved', 'Fob Involved1', '_T10 FOB Educational Enrollment', 'AD2InSchool', 'Fob Edu']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())


#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_C15_Min_Educational_Status(fdf):
    if (pd.isna(fdf['Mcafss Edu1']) and pd.isna(fdf['AD1MinEdu'])):
        return 'Unknown/Did Not Report'
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Mcafss Edu1']:
            case _ if pd.isna(fdf['Mcafss Edu1']): 
                return 'Unrecognized Value' 
            case 1 | 2:
                return 'Less than HS diploma' ### 'Less than 8th Grade' | '8-11th Grade'.
            case 3 | 4:
                return 'HS diploma/GED' ### 'High School Grad' | 'Completed a GED'.
            case 5 | 7:
                return 'Technical Training or Associates Degree' ### 'Vocational School after High School' | 'Associates Degree'.
            case 6:
                return 'Some college/training' ### Some College.
            case 8:
                return "Bachelor's Degree or Higher" ### Bachelors Degree or Higher.
            ### case 9:
            ###     return 'HS diploma/GED' ### currently enrolled in college - vocational training or trade apprenticeship.
            ### case 10:
            ###     return 'HS diploma/GED' ### currently not enrolled in college - vocational training or trade apprenticeship.
            ### case 11:
            ###     return 'Other' ### other education.
            ### case 12:
            ###     return 'Unknown/Did Not Report' ### unknown/did not report.
            case 0:
                return 'Unknown/Did Not Report' ### unknown/did not report (missing data).
            case _ if int(fdf['Mcafss Edu1']) >= 9:
                return 'Unknown/Did Not Report'
            case _:
                return 'Unrecognized Value'
    ###########
    ### FW.
    elif (fdf['source'] == 'FW'):
        match fdf['AD1MinEdu']:
            case _ if pd.isna(fdf['AD1MinEdu']): 
                return 'Unrecognized Value' 
            case '8th Grade or less' | 'Some High School':
                return 'Less than HS diploma'
            case 'GED' | 'High School Graduate':
                return 'HS diploma/GED'
            case 'Achievement Certificate' | 'Certificate Program':
                return 'Technical Training or Certification'
            case 'Some College':
                return 'Some college/training'
            case 'Associates or Two Year Technical Degree':
                return 'Technical Training or Associates Degree' ### these are two separate categories on F1.
            case 'Two Year Degree':
                return "Associate's Degree"
            case 'Four Year College Degree' | 'Graduate School':
                return "Bachelor's Degree or Higher"
            case 'Unknown' | 'null':
                return 'Unknown/Did Not Report'
            case _:
                return 'Unrecognized Value'
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_C15 Min Educational Status'] = df_14t_edits1_tb3.apply(func=fn_C15_Min_Educational_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C15 Min Educational Status']) 
# #%%
# print(df_14t_edits1_tb3[['_C15 Min Educational Status', 'Mcafss Edu1', 'AD1MinEdu']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_C15_Max_Educational_Status(fdf):
    if (pd.isna(fdf['Mcafss Edu2']) and pd.isna(fdf['AD1MaxEdu'])):
        return 'Unknown/Did Not Report'
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Mcafss Edu2']:
            case _ if pd.isna(fdf['Mcafss Edu2']): 
                return 'Unrecognized Value' 
            case 1 | 2:
                return 'Less than HS diploma' ### 'Less than 8th Grade' | '8-11th Grade'.
            case 3 | 4:
                return 'HS diploma/GED' ### 'High School Grad' | 'Completed a GED'.
            case 5 | 7:
                return 'Technical Training or Associates Degree' ### 'Vocational School after High School' | 'Associates Degree'.
            case 6:
                return 'Some college/training' ### Some College.
            case 8:
                return "Bachelor's Degree or Higher" ### Bachelors Degree or Higher.
            ### case 9:
            ###     return 'HS diploma/GED' ### currently enrolled in college - vocational training or trade apprenticeship.
            ### case 10:
            ###     return 'HS diploma/GED' ### currently not enrolled in college - vocational training or trade apprenticeship.
            ### case 11:
            ###     return 'Other' ### other education.
            ### case 12:
            ###     return 'Unknown/Did Not Report' ### unknown/did not report.
            case 0:
                return 'Unknown/Did Not Report' ### unknown/did not report (missing data).
            case _ if int(fdf['Mcafss Edu2']) >= 9:
                return 'Unknown/Did Not Report'
            case _:
                return 'Unrecognized Value'
    ###########
    ### FW.
    elif (fdf['source'] == 'FW'):
        match fdf['AD1MaxEdu']:
            case _ if pd.isna(fdf['AD1MaxEdu']): 
                return 'Unrecognized Value' 
            case '8th Grade or less' | 'Some High School':
                return 'Less than HS diploma'
            case 'GED' | 'High School Graduate':
                return 'HS diploma/GED'
            case 'Achievement Certificate' | 'Certificate Program':
                return 'Technical Training or Certification'
            case 'Some College':
                return 'Some college/training'
            case 'Associates or Two Year Technical Degree':
                return 'Technical Training or Associates Degree' ### these are two separate categories on F1.
            case 'Two Year Degree':
                return "Associate's Degree"
            case 'Four Year College Degree' | 'Graduate School':
                return "Bachelor's Degree or Higher"
            case 'Unknown' | 'null':
                return 'Unknown/Did Not Report'
            case _:
                return 'Unrecognized Value'
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_C15 Max Educational Status'] = df_14t_edits1_tb3.apply(func=fn_C15_Max_Educational_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C15 Max Educational Status']) 
# #%%
# print(df_14t_edits1_tb3[['_C15 Max Educational Status', 'Mcafss Edu2', 'AD1MaxEdu']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())

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
                    return 'Unknown/Did Not Report'
                case '8th Grade or less' | 'Some High School':
                    return 'Less than HS diploma'
                case 'GED' | 'High School Graduate':
                    return 'HS diploma/GED'
                case 'Achievement Certificate':
                    return 'Some college/training' ### is this the right category?
                case 'Certificate Program':
                    return 'Some college/training' ### is this the right category?
                case 'Some College':
                    return 'Some college/training'
                case 'Associates or Two Year Technical Degree' | 'Two Year Degree':
                    return 'Technical Training or Associates Degree' ### these are two serparate categories on F1.
                case 'Four Year College Degree' | 'Graduate School':
                    return "Bachelor's Degree or Higher"
                case 'Unknown':
                    return 'Unknown/Did Not Report'
                case _:
                    return pd.NA 
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == 'Y'):
            match fdf['Fob Edu']:
                case _ if pd.isna(fdf['Fob Edu']):
                    return 'Unknown/Did Not Report'
                # case 1 | 2:
                case 'Less than 8' | '8-11':
                    return 'Less than HS diploma'
                # case 3 | 4:
                case 'HS Grad' | 'GED':
                    return 'HS diploma/GED'
                # case 5:
                case 'Vocational school after HS':
                    return 'Vocational School after High School'
                # case 6:
                case 'Some college':
                    return 'Some college/training'
                # case 7:
                case 'Associates degree':
                    return "Associate's Degree"
                # case 8:
                case 'Bachelor’s degree or higher':
                    return "Bachelor's Degree or Higher"
                # case 0: ### Was 'Unknown/Did Not Report' ### TODO: check.
                case _:
                    return 'Unrecognized Value'
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA ### likely MOB.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_T09 FOB Education Status'] = df_14t_edits1_tb3.apply(func=fn_T09_FOB_Education_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T09 FOB Education Status']) 
#%%
# compare_col(df_14t_comparison_csv_tb3, df_14t_edits1_tb3, '_T09 FOB Education Status', info_or_value_counts='value_counts')
# compare_col(df_14t_comparison_csv_tb3, df_14t_edits1_tb3, 'Fob Edu', info_or_value_counts='value_counts')
# inspect_col(df_14t_edits1_tb3['Fob Edu']) 
### DONE: FIX this variable's logic: 'Fob Edu' should NOT be an integer; it is a string with multiple string values.
    ### Fix after comparison, because Tableau logic also broken.
    ### Go ahead & fix, but probably not used in Form 1 anyway.
# #%%
# print(df_14t_edits1_tb3[['Fob Involved', 'Fob Involved1', '_T09 FOB Education Status', 'AD2EDLevel', 'Fob Edu']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())

#%%###################################

def fn_C16_CG_Insurance_Status(fdf_column):
    match fdf_column:
        case _ if pd.isna(fdf_column):
            return "Unknown/Did Not Report"
        ###########
        ### FW.
        case "Medicaid" | "SCHIP"|"Medicare/Medicaid"|"Medica":
            return "Medicaid or CHIP"
        case "Private" | "Other" | "Medicare"|"Blue Cross Blue Shield"|"Meritain Health"|"Ambetter"|"NE Total Care"|"United Healthcare Community Plan": #Y13Q4: adding Medicare/Medicaid and Blue Cross Blue Shield and "Meritain Health"
            return "Private or Other"
        case "Tri-Care":
            return "Tri-Care"
        case "None":
            return "No Insurance Coverage"
        case "Unknown" | "null":
            return "Unknown/Did Not Report"
        ###########
        ### LLCHD.
        case "0":
            return "No Insurance Coverage" ### Added from fn_T20_CG_Insurance_Status.
        case "1" | "Medicaid":
            return "Medicaid or CHIP"
        case "2":
            return "Tri-Care"
        case "3" | "Private":
            return "Private or Other"
        case "4" | "FamilyCh":
            return "FamilyChildHealthPlus" ###  Was "Unknown/Did Not Report" for "4". Different from Form 1's "FamilyChildHealthPlus". ### DONE Y12Q4: standardize. Answer: match F1! 
        case "5" | "Uninsure":
            return "No Insurance Coverage"
        case "6" | "99" | "Unknown":
            return "Unknown/Did Not Report"
        ###########
        case _:
            return f"Unrecognized: {fdf_column}"
    ###########
    ### /// Tableau Calculation:
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

df_14t_edits1_tb3['_C16 CG Insurance 1 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.1'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 1 Status']) 
# #%%
# print(df_14t_edits1_tb3[['_C16 CG Insurance 1 Status', 'AD1PrimaryIns.1']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
#%% 
### TODO ASKJOE: address "Unrecognized Values": "Blue Cross Blue Shield" & "Medicare/Medicaid". How to code?
### See "list_14t_unrecognized_values_tb3". Rows with "Unrecognized Value":
# df_14t_edits1_tb3[['Project Id','Year','Quarter', '_C16 CG Insurance 1 Status', 'AD1PrimaryIns.1']].query(f'`_C16 CG Insurance 1 Status` == "Unrecognized Value"')
# #%% ### Run list_14t_unrecognized_values_tb3 code below first:
# ### [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_C16 CG Insurance 1 Status']
# #%%
# compare_col(df_14t_comparison_csv_tb3, df_14t_edits1_tb3, '_C16 CG Insurance 1 Status')
# #%%
# compare_col(df_14t_comparison_csv_tb3, df_14t_edits1_tb3, '_C16 CG Insurance 1 Status', info_or_value_counts='value_counts')

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 2 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.2'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 2 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 3 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.3'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 3 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 4 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.4'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 4 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 5 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.5'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 5 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 6 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.6'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 6 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 7 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.7'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 7 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 8 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.8'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 8 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 9 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.9'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 9 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 10 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.10'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 10 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 11 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.11'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 11 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 12 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.12'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 12 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 13 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.13'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 13 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 14 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.14'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 14 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 15 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.15'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 15 Status']) 

#%%###################################

df_14t_edits1_tb3['_C16 CG Insurance 16 Status'] = df_14t_edits1_tb3['AD1PrimaryIns.16'].apply(func=fn_C16_CG_Insurance_Status).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_C16 CG Insurance 16 Status']) 

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
                return 'Unknown/Did Not Report'
            else:
                match fdf['AD2InsPrimary'].lower():
                    case 'medicaid'|"medicare/medicaid"|"medica":
                        return 'Medicaid or CHIP'
                    case 'none':
                        return 'No Insurance Coverage'
                    case "private" | "other" | "medicare"|"blue cross blue shield"|"meritain health"|"ambetter"|"ne total care"|"united healthcare community plan": #Y13Q4: adding Medicare/Medicaid and Blue Cross Blue Shield and "Meritain Health"
                        return "Private or Other"
                    case 'tri-care':
                        return 'Tri-Care'
                    case 'unknown':
                        return 'Unknown/Did Not Report'
                    case _:
                        return 'Unrecognized Value'
        else:
            return pd.NA 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == 'Y'):
            if pd.isna(fdf['Hlth Insure Fob']):
                return 'Unknown/Did Not Report'
            else:
                match fdf['Hlth Insure Fob']:
                    case 1:
                        return 'Medicaid or CHIP'
                    case 2:
                        return 'Tri-Care'
                    case 3:
                        return 'Private or Other'
                    case 4 | 99:
                        return 'Unknown/Did Not Report'
                    case 5:
                        return 'No Insurance Coverage'
                    case _:
                        return 'Unrecognized Value'
        else:
            return pd.NA 
    ###########
    else:
        return pd.NA ### likely MOB.
    ###########
    ### /// Tableau Calculation:
    # IF [Fob Involved] = True THEN CASE [AD2InsPrimary] //FW
    #     WHEN "Medicaid" THEN "Medicaid or CHIP"
    #     WHEN "Medicare" THEN "Other" //this is what our previous syntax indicated
    #     WHEN "None" THEN "No Insurance Coverage"
    #     WHEN "Other" THEN "Private or Other"
    #     WHEN "Private" THEN "Private or Other"
    #     WHEN "Tri-Care" THEN "Tri-Care"
    #     WHEN "Unknown" THEN "Unknown/Did Not Report"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSEIF [Fob Involved1] = "Y" THEN CASE [Hlth Insure Fob] //LLCHD
    #     WHEN 1 THEN "Medicaid or CHIP"
    #     WHEN 2 THEN "Tri-Care"
    #     WHEN 3 THEN "Private or Other"
    #     WHEN 4 THEN "Unknown/Did Not Report"
    #     WHEN 5 THEN "No Insurance Coverage"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # ELSE NULL
    # END
df_14t_edits1_tb3['_T20 FOB Insurance'] = df_14t_edits1_tb3.apply(func=fn_T20_FOB_Insurance, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T20 FOB Insurance']) 

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
                return 'Unrecognized Value'
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
                return 'Unrecognized Value (coming in as date)'
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
    # IF [Pregnancystatus] = 0 THEN "Pregnant" //FW
    # ELSEIF [Pregnancystatus] = 1 THEN "Not pregnant"
    # ELSEIF [Enroll Preg Status] = "Postpartum" THEN "Not pregnant" //LLCHD
    # ELSEIF [Enroll Preg Status] = "Pregnant" THEN "Pregnant"
    # ELSE NULL
    # END
df_14t_edits1_tb3['_Enroll Preg Status'] = df_14t_edits1_tb3.apply(func=fn_Enroll_Preg_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_Enroll Preg Status']) 

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
    # IF [Fob Involved] = True THEN 1 //FW
    # ELSEIF [Fob Involved] = False THEN 0
    # ELSEIF [Fob Involved1] = "Y" THEN 1 //LLCHD
    # ELSEIF [Fob Involved1] = "N" THEN 0
    # ELSE 0
    # END
df_14t_edits1_tb3['_FOB Involved'] = df_14t_edits1_tb3.apply(func=fn_FOB_Involved, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_FOB Involved']) 

#%%###################################

def fn_MOB_TGT_Relation(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Adult1TGTRelation']:
            case _ if pd.isna(fdf['Adult1TGTRelation']):
                return pd.NA 
            case 'Aunt' | 'Biological mother' | 'Foster mother' | 'Grandmother'| 'Guardian' | 'MOB' | 'Mother'|'Adoptive Mother':
                return 'MOB'
            case 'Adoptive father' | 'Biological father' | 'FOB' | 'Foster father'|'Step-Father':
                return 'FOB'
            case 'Other' if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'F')):
                return 'MOB'
            case 'Other'if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'M')):
                return 'FOB'
            case _ if pd.isna(fdf['Mob Gender']):
                return 'MOB'
            case _:
                return F'Unrecognized Value{fdf['Adult1TGTRelation']}'
            ### TODO ASKJOE: Maybe add options from 'Adult2TGTRelation': 'Other'.
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Primary Relation']:
            case _ if pd.isna(fdf['Primary Relation']):
                return pd.NA 
            ###
            case 'MOTHER OF CHILD':
                return 'MOB'
            case 'PRIMARY CAREGIVER' if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'F')):
                return 'MOB'
            case 'Bio parent' if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'F')):
                return 'MOB'
            case 'Grandparent' if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'F')):
                return 'MOB'
            case 'Grandmother' if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'F')): #Y13Q4: adding option for Grandmother and Other for FOB
                return 'MOB'
            case 'Other' if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'F')):
                return 'MOB'
            ###
            case 'FATHER OF CHILD':
                return 'FOB' 
            case 'PRIMARY CAREGIVER' if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'M')):
                return 'FOB'
            case 'Bio parent' if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'M')):
                return 'FOB'
            case 'Grandparent' if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'M')):
                return 'FOB'
            case 'Other'if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'M')):
                return 'FOB'
            ###
            case _ if pd.isna(fdf['Mob Gender']):
                return 'MOB'
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_MOB TGT Relation'] = df_14t_edits1_tb3.apply(func=fn_MOB_TGT_Relation, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_MOB TGT Relation']) 
# #%%
# print(df_14t_edits1_tb3[['_MOB TGT Relation', 'Adult1TGTRelation', 'Primary Relation', 'Mob Gender']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
#%% 
### RESOLVED: UV when LL & 'Primary Relation' is "Bio parent" but 'Mob Gender' is NA. How to assign? Default to "MOB"?
    ### TODO: Answer: See Form 1 & new gender options. Should flag as missing. See which variable generates that table.
        ### TODO SHOWJOE: Let Joe know impact of these changes.
### See "list_14t_unrecognized_values_tb3". Rows with "Unrecognized Value":
# df_14t_edits1_tb3[['Project Id','Year','Quarter', '_MOB TGT Relation', 'Adult1TGTRelation', 'Primary Relation', 'Mob Gender']].query(f'`_MOB TGT Relation` == "Unrecognized Value"')
# #%% ### Run list_14t_unrecognized_values_tb3 code below first:
# ### [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_MOB TGT Relation']
# #%%
# inspect_col(df_14t_edits1_tb3['Adult1TGTRelation']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Primary Relation']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Mob Gender']) 

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
                case 'Aunt' | 'Biological mother' | 'Foster mother' | 'MOB' | 'Mother'|"Adoptive mother":
                    return 'MOB'
                case 'Adoptive father' | 'Biological father' | 'FOB' | 'Foster father'|"Step-Father": #Y13Q4: adding "Step-Father" and "Adoptive mother" and Other for FOB
                    return 'FOB'
                case 'Grandmother': 
                    return 'MOB' ###: Joe: Review whether should match MOB version where is 'MOB'.
                case 'Guardian' if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'M')):
                    return 'FOB' ### Add logic check Mob Gender.
                case 'Guardian' if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'F')):
                    return 'MOB' ### Add logic check Mob Gender.
                case 'Other'if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'M')):
                    return 'FOB'
                case 'Other'if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'F')):
                    return 'MOB'
                case 'Other'if (False if pd.isna(fdf['Mob Gender']) else (fdf['Mob Gender'] == 'M')):
                    return 'FOB'
                case _ if pd.isna(fdf['Mob Gender']):
                    return 'MOB'
                case _:
                    return f'Unrecognized: {fdf['Adult2TGTRelation']}'
        else:
            return pd.NA ### If (fdf['Fob Involved'] != True). then return nothing per Joe's instruction
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        if pd.isna(fdf['Fob Involved1']):
            return pd.NA 
        elif (fdf['Fob Involved1'] == 'Y'):
            return 'FOB'
        ### TODO: No need for other relationships?
        else:
            return pd.NA
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
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
df_14t_edits1_tb3['_FOB Relation'] = df_14t_edits1_tb3.apply(func=fn_FOB_Relation, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_FOB Relation']) 

#%%###################################

def fn_T12_MOB_Housing_Status(fdf):
    ###########
    ### FW.
    if pd.notna(fdf['_Agency']):
        if (fdf['_Agency'] != "ll"):
            if pd.isna(fdf['Housing Status']):
                return "Unknown/Did Not Report"
            else:
                match fdf['Housing Status'].lower():
                    case "owns or shares own home, condominium, or apartment":
                        return "Owns or shares own home, condominium, or apartment" 
                    case (
                        "rents of shares own home or apartment" | 
                        "rents or shares own home or apartment" |"or apartment" #Y13Q4: new option "or apartment"
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
                        return "Some other arrangement" ### Not sure this is the right category.
                    ### case pd.NA:
                    ###     return "Unknown/Did Not Report"
                    case _:
                        return "Unrecognized Value" ### will have to add new FW values as they come in, they aren't all here.
        ###########
    ### LLCHD.
    elif pd.notna(fdf['_Agency']):
        if (fdf['_Agency'] == "ll"):
            if pd.isna(fdf['Mob Living Arrangement']):
                return "Unknown/Did Not Report"
            else:
                match fdf['Mob Living Arrangement']:
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
                    ### case pd.NA:
                    ###     return "Unknown/Did Not Report"
                    case _:
                        return "Unrecognized Value"
    ###########
    ### /// Tableau Calculation:
    # IF [_Agency]<> "ll" THEN CASE [Housing Status] //FW
    #     WHEN "Homeless and living in an emergency or transitional shelter" THEN "Homeless and living in an emergency or transition shelter" //Homeless and living in emergency or transitional shelter
    #     WHEN "Homeless and sharing housing" THEN "Homeless and sharing housing"
    #     WHEN "Live in public housing" THEN "Lives in public housing"
    #     WHEN "Lives with parent or family member" THEN "Lives with parent or family member"
    #     WHEN "Other" THEN  "Some other arrangement" //Not sure this is the right category
    #     WHEN "Owns or shares own home, condominium, or apartment" THEN "Owns or shares own home, condominium, or apartment"
    #     WHEN "Rents of shares own home or apartment" THEN "Rents or shares own home or apartment"
    #     WHEN "Rents or shares own home or apartment" THEN "Rents or shares own home or apartment"
    #     WHEN "Some other arrangement" THEN "Some other arrangement"
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value" //will have to add new FW values as they come in, they aren't all here
    #     END
    # ELSEIF [_Agency] = "ll" THEN CASE[Mob Living Arrangement] //LLCHD
    #     WHEN 1 THEN "Owns or shares own home, condominium, or apartment" //Owns or shared own home, condo, or apartment
    #     WHEN 2 THEN "Rents or shares own home or apartment" //Rents or shared own home or apartment
    #     WHEN 3 THEN "Lives in public housing" //Lives in public housing
    #     WHEN 4 THEN "Lives with parent or family member" //Lives with parent or family member
    #     WHEN 5 THEN "Not homeless, some other arrangement" //Some other arrangement
    #     WHEN 6 THEN "Homeless and sharing housing" //Homeless and sharing housing
    #     WHEN 7 THEN "Homeless and living in an emergency or transition shelter" //Homeless and living in emergency or transitional shelter
    #     WHEN 8 THEN "Homeless, some other arrangement" //Homeless with some other arrangement
    #     WHEN NULL THEN "Unknown/Did Not Report"
    #     ELSE "Unrecognized Value"
    #     END
    # END
df_14t_edits1_tb3['_T12 MOB Housing Status'] = df_14t_edits1_tb3.apply(func=fn_T12_MOB_Housing_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T12 MOB Housing Status']) 

#%%###################################
#int_fpg_base = 9680 
#int_fpg_increment = 5380 
### REMINDER: Update to new year's federal poverty guidelines in RUNME.py.
df_14t_edits1_tb3['_T14 Federal Poverty Level update'] = (int_fpg_base + (int_fpg_increment * df_14t_edits1_tb3['Household Size'])).astype('Int64')
    ### /// Tableau Calculation Q2:
    ### //uses 2022 federal guidelines, will need to update to 2023 guidelines when they become available
    ### 8870 + (4720 * [Household Size])
    ###################
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_T14 Federal Poverty Level update']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Household Size']) 

#%%###################################

### Dependent on '_T14 Federal Poverty Level update' above.
def fn_T14_Poverty_Percent(fdf):
    ### LLCHD.
    if pd.notna(fdf['_Agency']):
        if (fdf['_Agency'] == "ll" and pd.isna(fdf['Household Income'])):
            return pd.NA  
        elif (fdf['_Agency'] == "ll" and pd.isna(fdf['Household Size'])):
            return pd.NA 
        elif (fdf['_Agency'] == "ll"):
            return fdf['Household Income'] / fdf['_T14 Federal Poverty Level update']
        ### FW.
        elif (fdf['_Agency'] != "ll"):
            return fdf['Poverty Level'] 
    ###########
    ### /// Tableau Calculation:
    # IF [_Agency] = "ll" AND ISNULL([Household Income]) THEN NULL //LLCHD
    # ELSEIF [_Agency] = "ll" AND ISNULL([Household Size]) THEN NULL
    # ELSEIF [_Agency] = "ll" THEN [Household Income]/[_T14 Federal Poverty Level update]
    # ELSEIF [_Agency] <> "ll" THEN [Poverty Level] //FW
    # END
df_14t_edits1_tb3['_T14 Poverty Percent'] = df_14t_edits1_tb3.apply(func=fn_T14_Poverty_Percent, axis=1).astype('Float64') 
    ### Data Type in Tableau: float.
# inspect_col(df_14t_edits1_tb3['_T14 Poverty Percent']) 
# #%%
# inspect_col(df_14t_edits1_tb3['']) 

#%%###################################

### Dependent on '_T14 Poverty Percent' above.
### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T14_Federal_Poverty_Categories(fdf):
    if (pd.isna(fdf['_T14 Poverty Percent'])):
        return 'Unknown/Did Not Report' ### TODO ASKJOE: Check that this is desired.
    elif (fdf['_T14 Poverty Percent'] <= .50):
        return '50% and Under'
    elif (fdf['_T14 Poverty Percent'] <= 1.00):
        return '51-100%'
    elif (fdf['_T14 Poverty Percent'] <= 1.33):
        return '101-133%'
    elif (fdf['_T14 Poverty Percent'] <= 2.00):
        return '134-200%'
    elif (fdf['_T14 Poverty Percent'] <= 3.00):
        return '201-300%'
    elif (fdf['_T14 Poverty Percent'] > 3.00):
        return '>300%'
    ###########
    ### /// Tableau Calculation:
    # IF [_T14 Poverty Percent] <= .50 THEN "50% and Under"
    # ELSEIF [_T14 Poverty Percent] <= 1.00 THEN "51-100%"
    # ELSEIF [_T14 Poverty Percent] <= 1.33 THEN "101-133%"
    # ELSEIF [_T14 Poverty Percent] <= 2.00 THEN "134-200%"
    # ELSEIF [_T14 Poverty Percent] <= 3.00 THEN "201-300%"
    # ELSEIF [_T14 Poverty Percent] > 3.00  THEN ">300%"
    # ELSEIF NULL THEN "Unknown/Did Not Report"
    # END
df_14t_edits1_tb3['_T14 Federal Poverty Categories'] = df_14t_edits1_tb3.apply(func=fn_T14_Federal_Poverty_Categories, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T14 Federal Poverty Categories']) 
# #%%
# inspect_col(df_14t_edits1_tb3['_T14 Poverty Percent']) 

#%%###################################

def fn_T17_Discharge_Reason(fdf):
    ### LLCHD, see full reasons below.
    if (pd.notna(fdf['Discharge Dt'])):
        match fdf['Discharge Reason'].lower():
            case "1" | "family has met program goals":
                return "Completed Services"
            case _:
                return "Stopped Services Before Completion"
    ### FW.
    elif (pd.notna(fdf['Termination Date'])):
        match fdf['Termination Status'].lower():
            case "family graduated/met all program goals":
                return "Completed Services"
            case _:
                return "Stopped Services Before Completion"
    else:
        return "Currently Receiving Services"
    ###########
    ### /// Tableau Calculation:
    # IF NOT ISNULL([Discharge Dt]) THEN CASE [Discharge Reason] //LLCHD, see full reasons below
    #     WHEN "1" THEN "Completed Services" 
    #     WHEN "Family Has Met Program Goals" THEN "Completed Services"
    #     ELSE "Stopped Services Before Completion"
    #     END
    # ELSEIF NOT ISNULL([Termination Date]) THEN CASE [Termination Status] //FW
    #     WHEN "Family graduated/met all program goals" THEN "Completed Services"
    #     ELSE "Stopped Services Before Completion"
    #     END
    # ELSE "Currently Receiving Services"
    # END
    # //LLCHD discharge reasons
    # //1Family graduated/met all program goals
    # //2Family moved out of service area
    # //3Parent/guardian returned to school
    # //4Parent/guardian returned to work
    # //5Parent/guardian refused service
    # //6Death of participant
    # //7Unable to locate family
    # //8Target child adopted
    # //9Target child entered foster care
    # //10Target child living with another care giverx
    # //11Target child entered school/child care
    # //12Family never engaged
    # //13Unknown & a text box
df_14t_edits1_tb3['_T17 Discharge Reason'] = df_14t_edits1_tb3.apply(func=fn_T17_Discharge_Reason, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_T17 Discharge Reason']) 

#%%###################################

def fn_Need_Exclusion_1_Sub_Abuse(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Need Exclusion1']:
            case _ if pd.isna(fdf['Need Exclusion1']):
                return pd.NA 
            case 'Substance Abuse' | 'Drug Abuse' | 'Alcohol Abuse':
                return 'Alcohol/Drug Abuse'
            case _:
                return pd.NA ### TODO ASKJOE: Clarify if this is what's wanted.
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['need exclusion1 (LLCHD)']:
            case _ if pd.isna(fdf['need exclusion1 (LLCHD)']):
                return pd.NA 
            case 'Y':
                return 'Alcohol/Drug Abuse'
            case _:
                return pd.NA ### TODO ASKJOE: Clarify if this is what's wanted.
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
    # IF [Need Exclusion1] = "Substance Abuse" THEN "Alcohol/Drug Abuse" //FW
    #     ELSEIF [Need Exclusion1] = "Drug Abuse" THEN "Alcohol/Drug Abuse"
    #     ELSEIF [Need Exclusion1] = "Alcohol Abuse" THEN "Alcohol/Drug Abuse"
    # ELSEIF [need exclusion1 (LLCHD)] = "Y" THEN "Alcohol/Drug Abuse" //LLCHD
    # END
df_14t_edits1_tb3['_Need Exclusion 1 - Sub Abuse'] = df_14t_edits1_tb3.apply(func=fn_Need_Exclusion_1_Sub_Abuse, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_Need Exclusion 1 - Sub Abuse']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Need Exclusion1']) ### Options: 3 listed above or NA.
# #%%
# inspect_col(df_14t_edits1_tb3['need exclusion1 (LLCHD)']) ### Options: "Y", "N", "U", NA. ### TODO ASKJOE: Do we care about the other options?

#%%###################################

def fn_Need_Exclusion_2_Fam_Plan(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Need Exclusion2']:
            case _ if pd.isna(fdf['Need Exclusion2']):
                return pd.NA 
            case 'Family Planning':
                return 'Family Planning'
            case _:
                return pd.NA ### TODO ASKJOE: Clarify if this is what's wanted.
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['need exclusion2 (LLCHD)']:
            case _ if pd.isna(fdf['need exclusion2 (LLCHD)']):
                return pd.NA 
            case 'Y':
                return 'Family Planning'
            case _:
                return pd.NA ### TODO ASKJOE: Clarify if this is what's wanted.
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
    # IF [Need Exclusion2] = "Family Planning" THEN "Family Planning" //FW
    # ELSEIF [need exclusion2 (LLCHD)] = "Y" THEN "Family Planning" //LLCHD
    # END
df_14t_edits1_tb3['_Need Exclusion 2 - Fam Plan'] = df_14t_edits1_tb3.apply(func=fn_Need_Exclusion_2_Fam_Plan, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_Need Exclusion 2 - Fam Plan']) 

#%%###################################

def fn_Need_Exclusion_3_Mental_Health(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Need Exclusion3']:
            case _ if pd.isna(fdf['Need Exclusion3']):
                return pd.NA 
            case 'Mental Health':
                return 'Mental Health'
            case _:
                return pd.NA ### TODO ASKJOE: Clarify if this is what's wanted.
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['need exclusion3 (LLCHD)']:
            case _ if pd.isna(fdf['need exclusion3 (LLCHD)']):
                return pd.NA 
            case 'Y':
                return 'Mental Health'
            case _:
                return pd.NA ### TODO ASKJOE: Clarify if this is what's wanted.
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
    # IF [Need Exclusion3] = "Mental Health" THEN "Mental Health" //FW
    # ELSEIF [need exclusion3 (LLCHD)] = "Y" THEN "Mental Health" //LLCHD
    # END
df_14t_edits1_tb3['_Need Exclusion 3 - Mental Health'] = df_14t_edits1_tb3.apply(func=fn_Need_Exclusion_3_Mental_Health, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_Need Exclusion 3 - Mental Health']) 

#%%###################################

def fn_Need_Exclusion_5_IPV(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Need Exclusion5']:
            case _ if pd.isna(fdf['Need Exclusion5']):
                return pd.NA 
            case 'IPV Services':
                return 'IPV Services'
            case _:
                return pd.NA ### TODO ASKJOE: Clarify if this is what's wanted.
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['need exclusion5 (LLCHD)']:
            case _ if pd.isna(fdf['need exclusion5 (LLCHD)']):
                return pd.NA 
            case 'Y':
                return 'IPV Services'
            case _:
                return pd.NA ### TODO ASKJOE: Clarify if this is what's wanted.
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
    # IF [Need Exclusion5] = "IPV Services" THEN "IPV Services" //FW
    # ELSEIF [need exclusion5 (LLCHD)] = "Y" THEN "IPV Services" //LLCHD
    # END
df_14t_edits1_tb3['_Need Exclusion 5 - IPV'] = df_14t_edits1_tb3.apply(func=fn_Need_Exclusion_5_IPV, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_Need Exclusion 5 - IPV']) 

#%%###################################

### TODO: fix var name difference of Excludion6 between Form2 & Form1.
def fn_Need_Exclusion_6_Tobacco(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Need Exclusion6']:
            case _ if pd.isna(fdf['Need Exclusion6']):
                return pd.NA 
            case 'Tobacco Cessation':
                return 'Tobacco Cessation'
            case _:
                return pd.NA ### TODO ASKJOE: Clarify if this is what's wanted.
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['need_exclusion6']:
            case _ if pd.isna(fdf['need_exclusion6']):
                return pd.NA 
            case 'Y':
                return 'Tobacco Cessation'
            case _:
                return pd.NA ### TODO ASKJOE: Clarify if this is what's wanted.
    ###########
    else:
        return 'Unrecognized Value' ### if not FW or LL.
    ###########
    ### /// Tableau Calculation:
    # IF [Need Exclusion6] = "Tobacco Cessation" THEN "Tobacco Cessation" //FW
    # ELSEIF [need_exclusion6] = "Y" THEN "Tobacco Cessation" //LLCHD
    # END
df_14t_edits1_tb3['_Need Exclusion 6 - Tobacco'] = df_14t_edits1_tb3.apply(func=fn_Need_Exclusion_6_Tobacco, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_Need Exclusion 6 - Tobacco']) 

#%%###################################

### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
### TODO: No opportunity to catch Unknown Values.
def fn_T15_5_Tobacco_Use_in_Home(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Tobacco Use In Home']:
            case _ if pd.isna(fdf['Tobacco Use In Home']):
                return 0
            case 'Yes':
                return 1 
            case 'No':
                return 0 
            case _:
                return 0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Priority Tobacco Use']:
            case _ if pd.isna(fdf['Priority Tobacco Use']):
                return 0 
            case 'Y':
                return 1 
            case 'N':
                return 0 
            case _:
                return 0 
    ###########
    else:
        return 0 
    ###########
    ### /// Tableau Calculation:
    # IF[Tobacco Use In Home] = "Yes" THEN 1 //FW
    # ELSEIF [Tobacco Use In Home] = "No" THEN 0
    # ELSEIF [Priority Tobacco Use] = "Y" THEN 1 //LLCHD
    # ELSEIF [Priority Tobacco Use] = "N" THEN 0
    # ELSE 0
    # END
df_14t_edits1_tb3['_T15-5 Tobacco Use in Home'] = df_14t_edits1_tb3.apply(func=fn_T15_5_Tobacco_Use_in_Home, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_T15-5 Tobacco Use in Home']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Priority Tobacco Use']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Tobacco Use In Home']) 
### DONE: extra value "Unknown" not being addressed. 
### Fixed: Added extra "case _" in each section to catch.
### Actually reverted back to if-elif clauses instead of match-case. Changed because can't use .lower() on any fload np.nan.
### TODO: Discuss whether need to use .lower() a lot or not.
### Answer: Adjust so that if not values or NA is "Unrecognized" --- won't work because needs to be integer.
    ### TODO: Need different function that catches if there WOULD have been Unrecognized values & mark at end so can fix.
    ### Check if other variables like this.

#%%###################################

### Required for '_C19 IPV Screen Result' below.
### Returns both NaN & None at the moment.
def fn_IPV_Score_FW(fdf):
    if pd.isna(fdf['Agency']):
        return pd.NA 
    elif (fdf['Agency'] != "ll"):
        if (
            (False if pd.isna(fdf['Assess Afraid']) else (fdf['Assess Afraid'] == True))
            or (False if pd.isna(fdf['Assess IPV']) else (fdf['Assess IPV'] == True))
            or (False if pd.isna(fdf['Assess Police']) else (fdf['Assess Police'] == True))
        ):
            return "P"
        else:
            return "N" 
    ###########
    ### /// Tableau Calculation:
    # IF [Agency] <> "ll" THEN
    #     (IF [Assess Afraid] = TRUE 
    #     OR [Assess IPV] = TRUE 
    #     OR [Assess Police] = TRUE
    #     THEN "P" ELSE "N" END)
    # END
df_14t_edits1_tb3['_IPV Score FW'] = df_14t_edits1_tb3.apply(func=fn_IPV_Score_FW, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb3['_IPV Score FW']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Agency']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Assess Afraid']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Assess IPV']) 
# #%%
# inspect_col(df_14t_edits1_tb3['Assess Police']) 
# #%%
# # df_14t_comparison_csv_tb3[['_IPV Score FW']].compare(df_14t__final_from_csv_tb3[['_IPV Score FW']])
# (
#     df_14t_comparison_csv_tb3
#     .compare(df_14t__final_from_csv_tb3, keep_equal=True, keep_shape=True)
#     .loc[:, ['Project Id', 'Agency', '_IPV Score FW', 'Assess Afraid', 'Assess IPV', 'Assess Police']]
#     .dropna(how='all', subset=[('_IPV Score FW', 'self'), ('_IPV Score FW', 'other')])
#     .loc[(lambda df: df[('_IPV Score FW', 'self')] != df[('_IPV Score FW', 'other')]), :]
# )
# #%%
# (
#     df_14t_edits1_tb3
#     .sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)
#     .loc[(lambda df: pd.isna(df['Agency'])), ['Project Id', 'Agency', '_IPV Score FW']]
# )
# ### SO: When 'Agency' is NaN, code at first marked it as 'N' instead of NaN, because NaN is != '11', so needed to rewrite.

#%%###################################

### In Child2 & Adult3.
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
    ###########
    ### /// Tableau Calculation:
    # IF [Dt Edc] = DATE(1/1/1900) THEN NULL //LLCHD
    # ELSEIF [EDC Date] = DATE(1/1/1900) THEN NULL //FW
    # ELSE IFNULL([Dt Edc],[EDC Date])
    # END
df_14t_edits1_tb3['_TGT EDC Date'] = df_14t_edits1_tb3.apply(func=fn_TGT_EDC_Date, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb3['_TGT EDC Date']) 

#%%###################################

### TODO: No opportunity to flag Unrecognized Values.
def fn_UNCOPE_U_Recode(fdf):
    ### ONLY for FW:
    if (fdf['source'] == 'FW'):
        match fdf['U']:
            case _ if pd.isna(fdf['U']):
                return pd.NA 
            case 'Yes':
                return 1 
            case 'No':
                return 0 
            case _:
                return pd.NA ### TODO ASKJOE: Return a different number?
    ###########
    else:
        return pd.NA
    ###########
    ### /// Tableau Calculation:
    # IF [U] = "Yes" THEN INT(1)
    # ELSEIF [U] = "No" THEN INT(0)
    # END
df_14t_edits1_tb3['_UNCOPE U Recode'] = df_14t_edits1_tb3.apply(func=fn_UNCOPE_U_Recode, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_UNCOPE U Recode']) 

#%%###################################

### TODO: No opportunity to flag Unrecognized Values.
def fn_UNCOPE_N_Recode(fdf):
    ### ONLY for FW:
    if (fdf['source'] == 'FW'):
        match fdf['N']:
            case _ if pd.isna(fdf['N']):
                return pd.NA 
            case 'Yes':
                return 1 
            case 'No':
                return 0 
            case _:
                return pd.NA ### TODO ASKJOE: Return a different number?
    ###########
    else:
        return pd.NA
    ###########
    ### /// Tableau Calculation:
    # IF [N] = "Yes" THEN INT(1)
    # ELSEIF [N] = "No" THEN INT(0)
    # END
df_14t_edits1_tb3['_UNCOPE N Recode'] = df_14t_edits1_tb3.apply(func=fn_UNCOPE_N_Recode, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_UNCOPE N Recode']) 

#%%###################################

### TODO: No opportunity to flag Unrecognized Values.
def fn_UNCOPE_C_Recode(fdf):
    ### ONLY for FW:
    if (fdf['source'] == 'FW'):
        match fdf['C']:
            case _ if pd.isna(fdf['C']):
                return pd.NA 
            case 'Yes':
                return 1 
            case 'No':
                return 0 
            case _:
                return pd.NA ### TODO ASKJOE: Return a different number?
    ###########
    else:
        return pd.NA
    ###########
    ### /// Tableau Calculation:
    # IF [C] = "Yes" THEN INT(1)
    # ELSEIF [C] = "No" THEN INT(0)
    # END
df_14t_edits1_tb3['_UNCOPE C Recode'] = df_14t_edits1_tb3.apply(func=fn_UNCOPE_C_Recode, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_UNCOPE C Recode']) 

#%%###################################

### TODO: No opportunity to flag Unrecognized Values.
def fn_UNCOPE_O_Recode(fdf):
    ### ONLY for FW:
    if (fdf['source'] == 'FW'):
        match fdf['O']:
            case _ if pd.isna(fdf['O']):
                return pd.NA 
            case 'Yes':
                return 1 
            case 'No':
                return 0 
            case _:
                return pd.NA ### TODO ASKJOE: Return a different number?
    ###########
    else:
        return pd.NA
    ###########
    ### /// Tableau Calculation:
    # IF [O] = "Yes" THEN INT(1)
    # ELSEIF [O] = "No" THEN INT(0)
    # END
df_14t_edits1_tb3['_UNCOPE O Recode'] = df_14t_edits1_tb3.apply(func=fn_UNCOPE_O_Recode, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_UNCOPE O Recode']) 

#%%###################################

### TODO: No opportunity to flag Unrecognized Values.
def fn_UNCOPE_P_Recode(fdf):
    ### ONLY for FW:
    if (fdf['source'] == 'FW'):
        match fdf['P']:
            case _ if pd.isna(fdf['P']):
                return pd.NA 
            case 'Yes':
                return 1 
            case 'No':
                return 0 
            case _:
                return pd.NA ### TODO ASKJOE: Return a different number?
    ###########
    else:
        return pd.NA
    ###########
    ### /// Tableau Calculation:
    # IF [P] = "Yes" THEN INT(1)
    # ELSEIF [P] = "No" THEN INT(0)
    # END
df_14t_edits1_tb3['_UNCOPE P Recode'] = df_14t_edits1_tb3.apply(func=fn_UNCOPE_P_Recode, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_UNCOPE P Recode']) 

#%%###################################

### TODO: No opportunity to flag Unrecognized Values.
def fn_UNCOPE_E_Recode(fdf):
    ### ONLY for FW:
    if (fdf['source'] == 'FW'):
        match fdf['E']:
            case _ if pd.isna(fdf['E']):
                return pd.NA 
            case 'Yes':
                return 1 
            case 'No':
                return 0 
            case _:
                return pd.NA ### TODO ASKJOE: Return a different number?
    ###########
    else:
        return pd.NA
    ###########
    ### /// Tableau Calculation:
    # IF [E] = "Yes" THEN INT(1)
    # ELSEIF [E] = "No" THEN INT(0)
    # END
df_14t_edits1_tb3['_UNCOPE E Recode'] = df_14t_edits1_tb3.apply(func=fn_UNCOPE_E_Recode, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_UNCOPE E Recode']) 

#%%###################################

### Sum of UNCOPE scores in the FW dataset.
def fn_UNCOPE_Score_FW(fdf):
    return (
        fdf['_UNCOPE U Recode'] + 
        fdf['_UNCOPE N Recode'] + 
        fdf['_UNCOPE C Recode'] + 
        fdf['_UNCOPE O Recode'] + 
        fdf['_UNCOPE P Recode'] + 
        fdf['_UNCOPE E Recode']
    )
    ###########
    ### /// Tableau Calculation:
    # [_UNCOPE U Recode]+[_UNCOPE N Recode]+[_UNCOPE C Recode]+[_UNCOPE O Recode]+[_UNCOPE P Recode]+[_UNCOPE E Recode]
    # //sum of UNCOPE scores in the FW dataset
df_14t_edits1_tb3['_UNCOPE Score FW'] = df_14t_edits1_tb3.apply(func=fn_UNCOPE_Score_FW, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_UNCOPE Score FW']) 

#%%###################################

### TODO: No opportunity to flag Unrecognized Values.
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
            case 'Y':
                return 1 
            case 'N':
                return 0 
            case _:
                return 0 
    ###########
    else:
        return 0 
    ###########
    ### /// Tableau Calculation:
    # IF [History Inter Welfare Adult] = True THEN 1 //FW
    # ELSEIF  [History Inter Welfare Adult] = False THEN 0
    # ELSEIF[Priority Child Welfare] = "Y" THEN 1 //LLCHD
    # ELSEIF [Priority Child Welfare] = "N" THEN 0
    # ELSE 0
    # END
df_14t_edits1_tb3['_T15-3 History Welfare Interaction'] = df_14t_edits1_tb3.apply(func=fn_T15_3_History_Welfare_Interaction, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_T15-3 History Welfare Interaction']) 

#%%###################################

### TODO: No opportunity to flag Unrecognized Values.
### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T15_6_Low_Achievement(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Low Achievement']:
            case _ if pd.isna(fdf['Low Achievement']):
                return 0
            case 'Yes':
                return 1 
            case 'No':
                return 0 
            case _:
                return 0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Priority Low Student']:
            case _ if pd.isna(fdf['Priority Low Student']):
                return 0 
            case 'Y':
                return 1 
            case 'N':
                return 0 
            case _:
                return 0 
    ###########
    else:
        return 0 
    ###########
    ### /// Tableau Calculation:
    # IF[Low Achievement] = "Yes" THEN 1 //FW
    # ELSEIF [Low Achievement] = "No" THEN 0
    # ELSEIF [Priority Low Student] = "Y" THEN 1 //LLCHD
    # ELSEIF [Priority Low Student] = "N" THEN 0
    # ELSE 0
    # END
df_14t_edits1_tb3['_T15-6 Low Achievement'] = df_14t_edits1_tb3.apply(func=fn_T15_6_Low_Achievement, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_T15-6 Low Achievement']) 

#%%###################################

### TODO: No opportunity to flag Unrecognized Values.
### In Adult3-Form2 & Adult4-Form1. Same Tableau Calculation. Python modified.
def fn_T15_8_Military(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['Military']:
            case _ if pd.isna(fdf['Military']):
                return 0
            case 'Y':
                return 1 
            case 'N':
                return 0 
            case _:
                return 0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['Priority Military']:
            case _ if pd.isna(fdf['Priority Military']):
                return 0 
            case 'Y':
                return 1 
            case 'N':
                return 0 
            case _:
                return 0 
    ###########
    else:
        return 0 
    ###########
    ### /// Tableau Calculation:
    # IF [Military]= "Y" THEN 1 //FW
    # ELSEIF [Military] = "N" THEN 0
    # ELSEIF [Priority Military] = "Y" THEN 1 //LLCHD
    # ELSEIF [Priority Military] = "N" THEN 0 
    # ELSE 0
    # END
df_14t_edits1_tb3['_T15-8 Military'] = df_14t_edits1_tb3.apply(func=fn_T15_8_Military, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb3['_T15-8 Military']) 

#%%##################################################
### COALESCING 2 

### Dependent on '_IPV Score FW'.
df_14t_edits1_tb3['_C19 IPV Screen Result'] = df_14t_edits1_tb3['_IPV Score FW'].combine_first(df_14t_edits1_tb3['Ipv Screen']).astype('string') 
    ### IFNULL([_IPV Score FW],[Ipv Screen])
    ### Data Type in Tableau: 'string'.

df_14t_edits1_tb3['_UNCOPE Score'] = df_14t_edits1_tb3['Uncope Score'].combine_first(df_14t_edits1_tb3['_UNCOPE Score FW']).astype('Int64') 
    ### IFNULL([Uncope Score],[_UNCOPE Score FW]) 
    ### Data Type in Tableau: integer.



#%%##################################################
### DATE CALCULATIONS

### These calculations assume all date variables are dtype "datetime64".

df_14t_edits1_tb3['_90 Day UNCOPE Date'] = (df_14t_edits1_tb3['_UNCOPE Date'] + pd.DateOffset(days=90)).astype('datetime64[ns]')
    ### DATE(DATEADD('day',90,[_UNCOPE Date]))
    ### Data Type in Tableau: date.

df_14t_edits1_tb3['_C05 TGT 30 Day Date'] = (df_14t_edits1_tb3['_TGT DOB'] + pd.DateOffset(days=30)).astype('datetime64[ns]')
    ### DATE(DATEADD('day',30,[_TGT DOB]))
    ### Data Type in Tableau: date.

df_14t_edits1_tb3['_C05 TGT 56 Day Date'] = (df_14t_edits1_tb3['_TGT DOB'] + pd.DateOffset(days=56)).astype('datetime64[ns]')
    ### DATE(DATEADD('day',56,[_TGT DOB]))
    ### Data Type in Tableau: date.

df_14t_edits1_tb3['_C17 90 Day CES-D Date'] = (df_14t_edits1_tb3['_C03 CES-D Date'] + pd.DateOffset(days=90)).astype('datetime64[ns]')
    ### DATE(DATEADD('day',90, [_C03 CES-D Date]))
    ### Data Type in Tableau: date.

df_14t_edits1_tb3['_C19 90 Day IPV Date'] = (df_14t_edits1_tb3['_C14 IPV Date'] + pd.DateOffset(days=90)).astype('datetime64[ns]')
    ### DATE(DATEADD('day',90, [_C14 IPV Date]))
    ### Data Type in Tableau: date.

### In Child2 & Adult3 (but based on a different variable).
df_14t_edits1_tb3['_Enroll 3 Month Date'] = (df_14t_edits1_tb3['_Enrollment Date'] + pd.DateOffset(months=3)).astype('datetime64[ns]')
    ### DATE(DATEADD('month',3,[_Enrollment Date]))
    ### Data Type in Tableau: date.

### In Child2 & Adult3.
df_14t_edits1_tb3['_TGT 3 Month Date'] = (df_14t_edits1_tb3['_TGT DOB'] + pd.DateOffset(months=3)).astype('datetime64[ns]')
    ### DATE(DATEADD('month',3,[_TGT DOB]))
    ### Data Type in Tableau: date.


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
### For testing/comparisons
### df_14t_edits1_tb3_sorted = df_14t_edits1_tb3.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)[[*df_14t_comparison_csv_tb3]].copy() ### Rows then Columns sorted.


#%%##################################################
### Identify/FLAG "Unrecognized Value" ###
#####################################################

### FLAG any "Unrecognized Value" --- new value & needs to be edited earlier in the Data Source process.
### Across many variables.

# df_14t_edits1_tb3[df_14t_edits1_tb3 == 'Unrecognized Value'].query('@df_14t_edits1_tb3 == "Unrecognized Value"')
# df_14t_edits1_tb3.query('@df_14t_edits1_tb3 == "Unrecognized Value"')

#%%
print(f'Columns that have "Unrecognized Value":')
list_14t_unrecognized_values_tb3 = fn_find_value(df_14t_edits1_tb3.query(f'`Year` == {int_nehv_year} & `Quarter` == {int_nehv_quarter}'), if_print=True)

# #%%
# len(list_14t_unrecognized_values_tb3)
# #%%
# print(f'Columns that have "Unrecognized Value":\n{[x['col'] for x in list_14t_unrecognized_values_tb3]}')

#%%
### Look at one column:
# list_14t_unrecognized_values_tb3[0]

# ### New values Y12Q4.
# [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_MOB TGT Relation'] ### New situation. How to code?
# [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_C16 CG Insurance 1 Status'] ### New values. How to code?

# ### ONLY Y12Q1.
# [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_C16 CG Insurance 9 Status'] ### Only Y12Q1.
# [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_C16 CG Insurance 10 Status'] ### Only Y12Q1.
# [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_C16 CG Insurance 11 Status'] ### Only Y12Q1.
# [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_C16 CG Insurance 12 Status'] ### Only Y12Q1.
# [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_C16 CG Insurance 13 Status'] ### Only Y12Q1.
# [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_C16 CG Insurance 14 Status'] ### Only Y12Q1.
# [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_C16 CG Insurance 15 Status'] ### Only Y12Q1.
# [x for x in list_14t_unrecognized_values_tb3 if x["col"] == '_C16 CG Insurance 16 Status'] ### Only Y12Q1.
# [x for x in list_14t_unrecognized_values_tb3 if x["col"] in ['_C16 CG Insurance 9 Status', '_C16 CG Insurance 10 Status', '_C16 CG Insurance 11 Status', '_C16 CG Insurance 12 Status', '_C16 CG Insurance 13 Status', '_C16 CG Insurance 14 Status', '_C16 CG Insurance 15 Status', '_C16 CG Insurance 16 Status']] ### Only Y12Q1.


### !TESTRUNHERE!

print('____________\n')


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
### Prepare CSV ###
#####################################################

#%%################################
### REMOVE extra COLUMNS

### Remove columns created in merge.
df_14t_edits2_tb3 = df_14t_edits1_tb3.drop(columns=['LJ_tb3_2CI', 'LJ_tb3_3FW', 'LJ_tb3_4LL'])

#%%################################
### ORDER COLUMNS

### Final order for columns:
# [*df_14t_comparison_csv_tb3]

#%%
### Reorder Columns.
### df_14t_edits2_tb3 = df_14t_edits2_tb3[[*df_14t_comparison_csv_tb3]]
### 2024-01-16: Not using comparison because different variables now. Note: This also keeps in "source" that was previously removed.

#%%################################
### SORT ROWS

df_14t_edits2_tb3 = df_14t_edits2_tb3.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)





#%%##################################################
### WRITE ###
#####################################################

#%%
### Created Final DF.
df_14t__final_tb3 = df_14t_edits2_tb3.copy()

#%%
### Write out df.
df_14t__final_tb3.to_csv(path_14t_output_tb3, index=False, date_format="%#m/%#d/%Y")


##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################

### As of Y13Q1, there is no comparison file because we are only using this python code!



# #%%
# ### Read back in df for comparison.
# df_14t__final_from_csv_tb3 = pd.read_csv(path_14t_output_tb3, dtype=object, keep_default_na=False, na_values=list_na_values_to_read)

# #%%##################################################
# ### COMPARE CSVs ###
# #####################################################

# #%%###################################
# ### Make comparison have the same columns.

# #%%
# ### Extra columns created:
# df_14t_comparison_csv_tb3['source'] = (
#     df_14t_comparison_csv_tb3
#     .apply(func=(
#         lambda df: 'FW' if pd.notna(df['Project ID']) else ('LL' if pd.notna(df['project id (LLCHD)']) else 'um... problem')
#     ), axis=1)
#     .astype('string') 
# )

# df_14t_comparison_csv_tb3['Home Visit Type IP'] = 1 ### Placeholder.
# df_14t_comparison_csv_tb3['Home Visit Type V'] = 1 ### Placeholder.
# df_14t_comparison_csv_tb3['Home Visit Type All'] = 1 ### Placeholder.

# #%%
# ### Columns Renamed:
# df_14t_comparison_csv_tb3 = df_14t_comparison_csv_tb3.rename(columns={
#     'home_visits_pre': 'Home Visits Pre'
#     ,'home_visits_post': 'Home Visits Post'
#     ,'home_visits_person': 'Home Visits Person'
#     ,'home_visits_virtual': 'Home Visits Virtual'
#     ,'HomeVisitsPrenatal': 'Home Visits Prenatal'
#     ,'HomeVisitsTotal': 'Home Visits Total'
#     # ,'HomeVisitTypeIP': 'Home Visit Type IP'
#     # ,'HomeVisitTypeV': 'Home Visit Type V'
#     # ,'HomeVisitTypeAll': 'Home Visit Type All'
# })

# #%%
# ### Columns removed from code:
# df_14t_comparison_csv_tb3 = df_14t_comparison_csv_tb3.drop(columns=['_T04 MOB Age', '_T04 FOB Age'])

# #%%
# ### Reorder Columns.
# df_14t_comparison_csv_tb3 = df_14t_comparison_csv_tb3[[*df_14t__final_from_csv_tb3]]

# #%%###################################

# #%%
# ### Column names:
# [*df_14t__final_from_csv_tb3]
# #%%
# ### Column names:
# [*df_14t_comparison_csv_tb3]

# #%%
# ### Overlap / Similarities: Columns in both.
# set([*df_14t_comparison_csv_tb3]).intersection([*df_14t__final_from_csv_tb3])

# #%%###################################
# ### COLUMNS:

# #%%
# ### Check if all Column names identical & in same order.
# [*df_14t__final_from_csv_tb3] == [*df_14t_comparison_csv_tb3]

# #%%
# ### Differences: Columns only in one.
# set([*df_14t_comparison_csv_tb3]).symmetric_difference([*df_14t__final_from_csv_tb3])

# #%%###################################

# ####### Compare values
# ### including row count, distinct ids, 

# #%%
# # Check rows & cols:
# print(f'df_14t__final_from_csv_tb3 Rows: {len(df_14t__final_from_csv_tb3)}')
# print(f'df_14t_comparison_csv_tb3 Rows: {len(df_14t_comparison_csv_tb3)}')

# print(f'df_14t__final_from_csv_tb3 Columns: {len(df_14t__final_from_csv_tb3.columns)}')
# print(f'df_14t_comparison_csv_tb3 Columns: {len(df_14t_comparison_csv_tb3.columns)}')

# #%%
# #!HERE #TODO
# ### When deduplicated: Identify which rows are not coming through. Problem is that multiple columns are not aligned, so can't tell.
# # df_14t_comparison_csv_tb3[['Project Id','Year','Quarter']].merge(df_14t__final_from_csv_tb3[['Project Id','Year','Quarter']], indicator=True, how='left').loc[lambda x : x['_merge']!='both']


# #%%
# df_14t__final_from_csv_tb3 == df_14t_comparison_csv_tb3

# #%%
# ### Checking ID columns used in Join >> DF should be empty (meaning all the same).
# df_14t_comp_compare_tb3 = df_14t_comparison_csv_tb3[['Project Id','Year','Quarter']].compare(df_14t__final_from_csv_tb3[['Project Id','Year','Quarter']])
# df_14t_comp_compare_tb3

# ###################################
# ###################################
# ###################################

# #%%
# ### Now comparing ALL columns. DF created shows all differences:
# # df_14t_comp_compare_tb3 = df_14t_comparison_csv_tb3.compare(df_14t__final_from_csv_tb3)
# # df_14t_comp_compare_tb3 = df_14t_comparison_csv_tb3.query(f'Year=="12" & Quarter=="4"').compare(df_14t__final_from_csv_tb3.query(f'Year=="12" & Quarter=="4"'))

# df_14t_comp_compare_tb3 = (
#         df_14t_comparison_csv_tb3.query(f'Year=="12" & Quarter=="4"')
#         ### To remove these vars that should be fixed earlier in the DS process:
#         .assign(**{
#             'Asq3 Referral 18Mm': (lambda df: pd.to_datetime(df['Asq3 Referral 18Mm'].astype('float64'), unit='D', origin='1899-12-30').dt.strftime('%#m/%#d/%Y'))
#             ,'Asq3 Referral 24Mm': (lambda df: pd.to_datetime(df['Asq3 Referral 24Mm'].astype('float64'), unit='D', origin='1899-12-30').dt.strftime('%#m/%#d/%Y'))
#             ,'Asq3 Referral 30Mm': (lambda df: pd.to_datetime(df['Asq3 Referral 30Mm'].astype('float64'), unit='D', origin='1899-12-30').dt.strftime('%#m/%#d/%Y'))
#         })
#     ).compare(df_14t__final_from_csv_tb3.query(f'Year=="12" & Quarter=="4"'))

# # df_14t_comp_compare_tb3

# #%%
# ### Number of columns with different values/types:
# len([*df_14t_comp_compare_tb3]) / 2 
#     ### Start: 50.

# #%%
# ### Columns:
# [*df_14t_comp_compare_tb3]


# #%% !TESTRUNHERE!


# ###################################
# #### completed
# ###################################

# #%%
# # df_14t_comparison_csv_tb3[['_IPV Score FW']].compare(df_14t__final_from_csv_tb3[['_IPV Score FW']], keep_equal=True).loc[(lambda df: df[('_IPV Score FW', 'self')] != df[('_IPV Score FW', 'other')]), ['_IPV Score FW']]
# # df_14t_comparison_csv_tb3[['_IPV Score FW']].compare(df_14t__final_from_csv_tb3[['_IPV Score FW']])
# # print(df_14t_comp_compare_tb3[['_IPV Score FW']].to_string())

# ###################################

# # #%%
# # # inspect_col(df_14t_comparison_csv_tb3['FOB Race White']) 
# # inspect_col(df_14t__final_from_csv_tb3['FOB Race White']) 
# # # inspect_col(df_14t_edits1_tb3_sorted['FOB Race White']) ### Float before changing in Read above.
# # # inspect_col(df_14t_edits1_tb3_sorted['FOB Race Asian']) ### Float before changing in Read above.
# # # inspect_col(df_14t_edits1_tb3_sorted['FOB Race Black']) ### Float before changing in Read above.
# # # inspect_col(df_14t_edits1_tb3_sorted['FOB Race Hawaiian Pacific']) ### Float before changing in Read above.
# # # inspect_col(df_14t_edits1_tb3_sorted['FOB Race Indian Alaskan']) ### Float before changing in Read above.
# # # inspect_col(df_14t_edits1_tb3_sorted['FOB Race Other']) ### Float before changing in Read above.
# # #%%
# # # var_to_compare = 'FOB Race Asian'
# # # var_to_compare = 'FOB Race Black'
# # # var_to_compare = 'FOB Race Hawaiian Pacific'
# # # var_to_compare = 'FOB Race Indian Alaskan'
# # # var_to_compare = 'FOB Race Other'
# # var_to_compare = 'FOB Race White'
# # var_list_for_comparison = ['_T07 FOB Race', 'FOB Race Asian', 'FOB Race Black', 'FOB Race Hawaiian Pacific', 'FOB Race Indian Alaskan', 'FOB Race Other', 'FOB Race White']
# # (
# #     df_14t_comparison_csv_tb3
# #     .compare(
# #         df_14t__final_from_csv_tb3
# #         # df_14t_edits1_tb3_sorted
# #         , keep_equal=True, keep_shape=True
# #     )
# #     .loc[:, ['Project Id', 'Fob Involved', 'Fob Involved1'] + var_list_for_comparison]
# #     .dropna(how='all', subset=[(var_to_compare, 'self'), (var_to_compare, 'other')])
# #     .loc[(lambda df: df[(var_to_compare, 'self')] != df[(var_to_compare, 'other')]), :]
# # )
# # ### Fixed by changing the 6 "FOB Race" columns to Boolean at end.
# # ### TODO:
# #     ### Change earlier in pipeline so Excel has True/Fasle instead of 1/0. (Needed?)

# ###################################

# #%%
# # var_to_compare = 'AD1InsChangeDate.1'
# # var_to_compare = 'AD1InsChangeDate.2'
# # var_to_compare = 'AD1InsChangeDate.3'
# # var_to_compare = 'AD1InsChangeDate.4'
# # var_to_compare = 'AD1InsChangeDate.5'
# # var_to_compare = 'AD1InsChangeDate.6'
# # var_to_compare = 'AD1InsChangeDate.7'
# # var_to_compare = 'AD1InsChangeDate.8'
# # var_to_compare = 'AD1InsChangeDate.9'
# # var_to_compare = 'AD1InsChangeDate.10'
# # var_to_compare = 'AD1InsChangeDate.11'
# # var_to_compare = 'AD1InsChangeDate.12'
# # var_to_compare = 'AD1InsChangeDate.13'
# # var_to_compare = 'AD1InsChangeDate.14'
# # var_to_compare = 'AD1InsChangeDate.15'
# # var_to_compare = 'AD1InsChangeDate.16'

# # (
# #     df_14t_comparison_csv_tb3
# #     .compare(df_14t__final_from_csv_tb3, keep_equal=True, keep_shape=True)
# #     .loc[:, ['Project Id', 'Agency', '_C16 CG Insurance 9 Status', 'AD1PrimaryIns.9', 'AD1InsChangeDate.9', 'AD1PrimaryIns.10', 'AD1InsChangeDate.10']]
# #     .dropna(how='all', subset=[('AD1InsChangeDate.9', 'self'), ('AD1InsChangeDate.9', 'other')])
# #     .loc[(lambda df: df[('AD1InsChangeDate.9', 'self')] != df[('AD1InsChangeDate.9', 'other')]), :]
# # )

# ### DONE (but not for historical data): Fix Tab "Caregiver Insurance":
#     ### Found that for 'Project Id's "hs123-1" & "hs123-2", from column "AD1PrimaryIns.9" to "AD1PrimaryIns.16" the dates & strings are switched. 
#     ### "AD1InsChangeDate.16" is blank, so maybe everything got shifted to the left?
#     ### It seems that for every person with ".9" & higher, the same pattern of incorrect entry exists.
# ### Answer: Corrected for Q2!

# ### Y12Q4: When filting to Y12Q4, no longer any differences.

# ###################################
# #### need work
# ###################################

# ###################################

# #%%
# # inspect_col(df_14t_comparison_csv_tb3['Asq3 Referral 9Mm']) 
# # inspect_col(df_14t_comparison_csv_tb3['Asq3 Referral 18Mm'])
# # inspect_col(df_14t_comparison_csv_tb3['Asq3 Referral 24Mm'])
# # inspect_col(df_14t_comparison_csv_tb3['Asq3 Referral 30Mm'])

# #%%
# # var_to_compare = 'Asq3 Referral 18Mm'
# # var_to_compare = 'Asq3 Referral 24Mm'

# # var_to_compare = 'Asq3 Referral 30Mm'
# # var_list_for_comparison = ['Asq3 Referral 18Mm', 'Asq3 Referral 24Mm', 'Asq3 Referral 30Mm', 'Asq3 Referral 9Mm']

# #%%
# # compare_col(df_14t_comparison_csv_tb3, df_14t__final_from_csv_tb3, var_to_compare, info_or_value_counts='value_counts')

# #%%
# # print(( df_14t_comparison_csv_tb3[var_list_for_comparison]
# #     .assign(**{
# #         # 'Asq3 Referral 18Mm': (lambda df: df['Asq3 Referral 18Mm'].astype('float64'))
# #         # ,'Asq3 Referral 24Mm': (lambda df: df['Asq3 Referral 24Mm'].astype('float64'))
# #         # ,'Asq3 Referral 30Mm': (lambda df: df['Asq3 Referral 30Mm'].astype('float64'))
# #         ###
# #         'Asq3 Referral 18Mm': (lambda df: pd.to_datetime(df['Asq3 Referral 18Mm'].astype('float64'), unit='D', origin='1899-12-30'))
# #         ,'Asq3 Referral 24Mm': (lambda df: pd.to_datetime(df['Asq3 Referral 24Mm'].astype('float64'), unit='D', origin='1899-12-30'))
# #         ,'Asq3 Referral 30Mm': (lambda df: pd.to_datetime(df['Asq3 Referral 30Mm'].astype('float64'), unit='D', origin='1899-12-30'))
# #     }) 
# #     .dropna(how='all', subset=[var_to_compare])
# # ).to_string())

# #%%
# # print((
# #     df_14t_comparison_csv_tb3
# #     .assign(**{
# #         'Asq3 Referral 18Mm': (lambda df: pd.to_datetime(df['Asq3 Referral 18Mm'].astype('float64'), unit='D', origin='1899-12-30').dt.strftime('%#m/%#d/%Y'))
# #         ,'Asq3 Referral 24Mm': (lambda df: pd.to_datetime(df['Asq3 Referral 24Mm'].astype('float64'), unit='D', origin='1899-12-30').dt.strftime('%#m/%#d/%Y'))
# #         ,'Asq3 Referral 30Mm': (lambda df: pd.to_datetime(df['Asq3 Referral 30Mm'].astype('float64'), unit='D', origin='1899-12-30').dt.strftime('%#m/%#d/%Y'))
# #     }) ### After this no rows returned because all self/other dates match when 5-digit numbers turned to dates.
# #     .compare(df_14t__final_from_csv_tb3, keep_equal=True, keep_shape=True)
# #     .loc[:, ['Project Id'] + var_list_for_comparison]
# #     .loc[lambda df: df.apply(fn_keep_row_differences, axis=1, variable2compare=var_to_compare), :] 
# # ).to_string())
# ### Now shows NO differences after columns edited.

# ### In Tableau, these 3 "Asq3 Referral" vars are Integers & [Asq3 Referral 9Mm] is a String (completely empty for Q1).
# ### None of these 3 "Asq3 Referral" vars are used in calculations in this code.
# ### For these 3 "Asq3 Referral" vars, the output should be in Date format, BUT the original output is an Int.

# ### Status: Nothing to do in this code. #TODO: should fix earlier in DS process.
# ### TODO: Check that this output is read in correctly in the Report Tableau Workbooks.

# ###################################

# #%%
# # var_to_compare = 'Fob Edu' ### DONE: Fix: NOT supposed to be an int, but is int in Tableau.

# var_to_compare = '_T09 FOB Education Status' ### Temporarily fixed, but need to actually fix for 'Fob Edu'.
# var_list_for_comparison = ['_T09 FOB Education Status', 'AD2EDLevel', 'Fob Edu']

# #%%
# # compare_col(df_14t_comparison_csv_tb3, df_14t__final_from_csv_tb3, var_to_compare, info_or_value_counts='value_counts')


# ### DONE: Fixed code above: Reading in 'Fob Edu' as string instead of int (which was making it all NA).

# ###################################

# # var_to_compare = 'Poverty Level' ### Both are floats, but Tableau output drops all non-significatn digits (like ".0").
# ### TODO: Test matching numeric style in output.

# ###################################

# # var_to_compare = '_T15-5 Tobacco Use in Home'
# ### Answer: Adjust so that if not values or NA is "Unrecognized" --- won't work because needs to be integer.
#     ### TODO: Need different function that catches if there WOULD have been Unrecognized values & mark at end so can fix.

# ###################################

# # var_to_compare = '_T04 FOB Age'
# # var_to_compare = '_T04 MOB Age'
# ### TODO: move to Tableau reports.

# ###################################



# ###################################
# ### investigation
# ###################################

# #%%
# ### Columns still different:
# [*df_14t_comp_compare_tb3]

# #%%

# # var_to_compare = '_Family ID'
# # var_to_compare = '_T06 FOB Ethnicity'
# # var_to_compare = '_T06 FOB Ethnicity (1)'
# # var_to_compare = '_T06 MOB Ethnicity'
# # var_to_compare = '_T10 FOB Educational Enrollment'
# # var_to_compare = '_T11 FOB Employment'
# # var_to_compare = '_T11 MOB Employment'
# # var_to_compare = '_T14 Federal Poverty Categories'
# # var_to_compare = '_T14 Poverty Percent'
# # var_to_compare = '_Zip'


# #######




# #%%################################
# ### Column Comparisons
# ###################################

# #!HERE

# # var_to_compare = 'www'

# # var_list_for_comparison = [var_to_compare]

# # var_list_keys_or_ids = ['Project Id']
# var_list_keys_or_ids = ['Project Id','Year','Quarter']
# # var_list_keys_or_ids = ['Project Id', 'Agency']
# # var_list_keys_or_ids = ['Project Id', 'Agency', 'Fob Involved', 'Fob Involved1']

# print((
#     # df_14t_comparison_csv_tb3.compare(df_14t__final_from_csv_tb3, keep_shape=True, keep_equal=True) 
#     df_14t_comparison_csv_tb3.query(f'Year=="12" & Quarter=="4"').compare(df_14t__final_from_csv_tb3.query(f'Year=="12" & Quarter=="4"'), keep_shape=True, keep_equal=True) 
#     .loc[:, var_list_keys_or_ids + var_list_for_comparison]
#     .loc[lambda df: df.apply(fn_keep_row_differences, axis=1, variable2compare=var_to_compare), :] 
#     ##########
#     ### Testing numeric vars:
#     # .apply(lambda df: df[(var_to_compare, 'self')] == df[(var_to_compare, 'other')], axis=1) ### Outputs a Series.
#     # .apply(lambda df: float(df[(var_to_compare, 'self')]) == float(df[(var_to_compare, 'other')]), axis=1)
#     # .all()
#     ##########
#     ### Testing date vars:
#     # .apply(lambda df: pd.to_datetime(df[(var_to_compare, 'self')]) == pd.to_datetime(df[(var_to_compare, 'other')]), axis=1)
#     # .all()
# ).to_string())


# ##########
# #%%
# # compare_col(df_14t_comparison_csv_tb3, df_14t__final_from_csv_tb3, var_to_compare, info_or_value_counts='info')
# compare_col(df_14t_comparison_csv_tb3, df_14t__final_from_csv_tb3, var_to_compare, info_or_value_counts='value_counts')
# #%%
# inspect_col(df_14t__final_from_csv_tb3[var_to_compare]) 
# #%%
# inspect_col(df_14t_comparison_csv_tb3[var_to_compare]) 
# #%%
# inspect_col(df_14t_edits1_tb3[var_to_compare]) 
# #%%
# # print(df_14t_comp_compare_tb3[[var_to_compare]].to_string())


#%%################################





### GENERAL:
### TODO: Every column function should build in what to do if any NA present in any var.


# %%

#%%
### END Adult3! SUCCESS!
print('END Adult3! SUCCESS!')

