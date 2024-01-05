
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.

#%%##################################################
### SETUP ###
#####################################################

#%%
print('File that is running: ', os.path.basename(__file__))

#%%
### The following is run if running this file by itself interactively (& ignored when run from RUNME):
if (os.path.basename(__file__) == '_1_4_replacing_tableau_2_Child_Activities.py'):
    from _1_4tab_RUNME import * 
    print('Imported "_1_4tab_RUNME"')
else:
    print("Did NOT run RUNME again... because it's already running!")

#%%
bool_14t_deduplicate_tb2 = False

#%%##################################################
### Comparison File ###
#####################################################

df_14t_comparison_csv_tb2 = pd.read_csv(path_14t_comparison_csv_tb2, dtype=object, keep_default_na=False, na_values=[''])
print(f'df_14t_comparison_csv_tb2 Rows: {len(df_14t_comparison_csv_tb2)}')

#%%
### Y12Q4 deduplicated rows to 3109 rows vs. original comparison of 3155.
if bool_14t_deduplicate_tb2:
    df_14t_comparison_csv_tb2 = df_14t_comparison_csv_tb2.drop_duplicates(ignore_index=True) 
print(f'df_14t_comparison_csv_tb2 Rows: {len(df_14t_comparison_csv_tb2)}')
df_14t_comparison_csv_tb2 = df_14t_comparison_csv_tb2.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)

#%%##################################################
### COLUMN DEFINITIONS ###
#####################################################

#######################
#%%### df_14t_piece_tb2_1: 'Project ID'.
list_14t_col_detail_tb2_1 = [
    ['project_id', 'Project Id', '', 'string'], 
    ['year', 'Year', '', 'Int64'], 
    ['quarter', 'Quarter', '', 'Int64']
]
#%%### df_14t_piece_tb2_1: 'Project ID'.
### For Renaming, we only need a dictionary of the columns with names changing.
### If x[2] == 'same' or x[0] == x[1] then that column is not included in df_colnames.
dict_14t_colnames_tb2_1 = {x[0]:x[1] for x in list_14t_col_detail_tb2_1 if x[2] != 'same' and x[0] != x[1]}
# dict_14t_colnames_tb2_1
#%%### df_14t_piece_tb2_1: 'Project ID'.
dict_14t_col_dtypes_tb2_1 = {x[0]:x[3] for x in list_14t_col_detail_tb2_1}
print(dict_14t_col_dtypes_tb2_1)
print(collections.Counter(list(dict_14t_col_dtypes_tb2_1.values())))

#######################
#%%### df_14t_piece_tb2_2: 'ER Injury'.
list_14t_col_detail_tb2_2 = [
    ['Project ID', 'Project ID (ER Injury)', '', 'string'],
    ['year', 'year (ER Injury)', '', 'Int64'],
    ['quarter', 'quarter (ER Injury)', '', 'Int64'],
    ['agency', 'agency (ER Injury)', '', 'string'],
    ['FAMILY NUMBER', 'FAMILY NUMBER (ER Injury)', '', 'string'],
    ['ChildNumber', 'ChildNumber (ER Injury)', '', 'string'],
    ['funding', 'funding (ER Injury)', '', 'string'],
    ['IncidentDate', 'Incident Date', '', 'datetime64[ns]'],
    ['IncidentDate2', 'IncidentDate2', 'same', 'datetime64[ns]']
]
#%%### df_14t_piece_tb2_2: 'ER Injury'.
dict_14t_colnames_tb2_2 = {x[0]:x[1] for x in list_14t_col_detail_tb2_2 if x[2] != 'same' and x[0] != x[1]}
# dict_14t_colnames_tb2_2
#%%### df_14t_piece_tb2_2: 'ER Injury'.
dict_14t_col_dtypes_tb2_2 = {x[0]:x[3] for x in list_14t_col_detail_tb2_2}
print(dict_14t_col_dtypes_tb2_2)
print(collections.Counter(list(dict_14t_col_dtypes_tb2_2.values())))

#######################
#%%### df_14t_piece_tb2_3: 'Family Wise'.
list_14t_col_detail_tb2_3 = [
    ['Project ID', 'Project ID', 'same', 'string']
    ,['year', 'year (Family Wise)', '', 'Int64']
    ,['quarter', 'quarter (Family Wise)', '', 'Int64']
    ,['agency', 'Agency', '', 'string']
    ,['FAMILY NUMBER', 'Family Number', '', 'string']
    ,['ChildNumber', 'Child Number', '', 'string']
    ,['MinOfHVDate', 'Min Of HV Date', '', 'datetime64[ns]']
    ,['TERMINATION DATE', 'Termination Date', '', 'datetime64[ns]']
    ,['TERMINATION STATUS', 'Termination Status', '', 'string']
    ,['MaxofHVDate', 'Maxof HV Date', '', 'datetime64[ns]']
    ,['TGT DOB-CR', 'Tgt Dob-Cr', '', 'datetime64[ns]']
    ,['EDC Date', 'EDC Date', 'same', 'datetime64[ns]']
    ,['MinHVDateBFYes', 'Min HV Date BF Yes', '', 'datetime64[ns]']
    ,['BreastFeeding', 'Breast Feeding', '', 'string'] ### 'Breast Feeding (Count)'. ### 'string' in Tableau & needs to be read in as such.
    ,['MinOfDateDiscontinueBF', 'Min Of Date Discontinue BF', '', 'datetime64[ns]']
    ### ,['SleepOnBack', 'Sleep On Back', '', 'string'] ### taken out by Y12Q4.
    ### ,['CoSleeping', 'Co Sleeping', '', 'string'] ### taken out by Y12Q4.
    ### ,['SoftBedding', 'Soft Bedding', '', 'string'] ### taken out by Y12Q4.
    ,['SafeSleepDate', 'Safe Sleep Date', '', 'datetime64[ns]']
    ,['SafeSleepPartialDate', 'Safe Sleep Partial Date', '', 'datetime64[ns]']
    ,['ASQ9MoDate', 'ASQ9MoDate', 'same', 'datetime64[ns]']
    ,['ASQ9MoTiming', 'ASQ9MoTiming', 'same', 'Int64']
    ,['ASQ9MoCom', 'ASQ9MoCom', 'same', 'Int64']
    ,['ASQ9MoGross', 'ASQ9MoGross', 'same', 'Int64']
    ,['ASQ9MoFine', 'ASQ9MoFine', 'same', 'Int64']
    ,['ASQ9MoProblem', 'ASQ9MoProblem', 'same', 'Int64']
    ,['ASQ9MoPersonal', 'ASQ9MoPersonal', 'same', 'Int64']
    ,['ASQ18MoDate', 'ASQ18MoDate', 'same', 'datetime64[ns]']
    ,['ASQ18MoTiming', 'ASQ18MoTiming', 'same', 'Int64']
    ,['ASQ18MoCom', 'ASQ18MoCom', 'same', 'Int64']
    ,['ASQ18MoGross', 'ASQ18MoGross', 'same', 'Int64']
    ,['ASQ18MoFine', 'ASQ18MoFine', 'same', 'Int64']
    ,['ASQ18MoProblem', 'ASQ18MoProblem', 'same', 'Int64']
    ,['ASQ18MoPersonal', 'ASQ18MoPersonal', 'same', 'Int64']
    ,['ASQ24MoDate', 'ASQ24MoDate', 'same', 'datetime64[ns]']
    ,['ASQ24MoTiming', 'ASQ24MoTiming', 'same', 'Int64']
    ,['ASQ24MoCom', 'ASQ24MoCom', 'same', 'Int64']
    ,['ASQ24MoGross', 'ASQ24MoGross', 'same', 'Int64']
    ,['ASQ24MoFine', 'ASQ24MoFine', 'same', 'Int64']
    ,['ASQ24MoProblem', 'ASQ24MoProblem', 'same', 'Int64']
    ,['ASQ24MoPersonal', 'ASQ24MoPersonal', 'same', 'Int64']
    ,['ASQ30MoDate', 'ASQ30MoDate', 'same', 'datetime64[ns]']
    ,['ASQ30MoTiming', 'ASQ30MoTiming', 'same', 'Int64']
    ,['ASQ30MoCom', 'ASQ30MoCom', 'same', 'Int64']
    ,['ASQ30MoGross', 'ASQ30MoGross', 'same', 'Int64']
    ,['ASQ30MoFine', 'ASQ30MoFine', 'same', 'Int64']
    ,['ASQ30MoProblem', 'ASQ30MoProblem', 'same', 'Int64']
    ,['ASQ30MoPersonal', 'ASQ30MoPersonal', 'same', 'Int64']
    ,['MaxEarlyLiteracyDate', 'Max Early Literacy Date', '', 'datetime64[ns]']
    ,['ReadTellStorySing', 'Read Tell Story Sing', '', 'string'] ### 'string' in Tableau & needs to be read in as such.
    ,['BehaviorDenom', 'Behavior Denom', '', 'Int64']
    ,['BehaviorNumer', 'Behavior Numer', '', 'Int64']
    ### ,['HomeVisitsPrental', 'Home Visits Prental', '', 'Int64'] ### Variable in Y12Q1&Q2, but missing from Y12Q3. RESOLVED: Why? Answer: HV should be calculated from Adult data, so removed.
    ### ,['HomeVisitsTotal', 'Home Visits Total', '', 'Int64'] ### Variable in Y12Q1&Q2, but missing from Y12Q3. RESOLVED: Why? Answer: HV should be calculated from Adult data, so removed.
    ,['TGTInsureChangeDate', 'TGT Insure Change Date', '', 'datetime64[ns]']
    ,['CHINSPrimaryIns', 'CHINS Primary Ins', '', 'string']
    ,['MOB LANGUAGE', 'Mob Language', '', 'string']
    ,['ChildMedCareSource', 'Child Med Care Source', '', 'string']
    ,['ChildDentalCareSource', 'Child Dental Care Source', '', 'string']
    ,['NTChildDevDelay', 'NT Child Dev Delay', '', 'string']
    ,['NTChildLowAchievement', 'NT Child Low Achievement', '', 'string']
    ,['HistoryInterWelfareChild', 'History Inter Welfare Child', '', 'boolean']
    ,['ASQ9MoRefDate', 'ASQ9MoRefDate', 'same', 'datetime64[ns]']
    ,['ASQ9MoRefLocation', 'ASQ9MoRefLocation', 'same', 'string']
    ,['ASQ9MoRefEIDate', 'ASQ9MoRefEIDate', 'same', 'datetime64[ns]']
    ,['ASQ9MoRefCSDate', 'ASQ9MoRefCSDate', 'same', 'datetime64[ns]']
    ,['ASQ18MoRefDate', 'ASQ18MoRefDate', 'same', 'datetime64[ns]']
    ,['ASQ18MoRefLocation', 'ASQ18MoRefLocation', 'same', 'string']
    ,['ASQ18MoEIDate', 'ASQ18MoEIDate', 'same', 'datetime64[ns]']
    ,['ASQ18MoCSDate', 'ASQ18MoCSDate', 'same', 'datetime64[ns]']
    ,['ASQ24MoRefDate', 'ASQ24MoRefDate', 'same', 'datetime64[ns]']
    ,['ASQ24MoRefLocation', 'ASQ24MoRefLocation', 'same', 'string']
    ,['ASQ24MoEIDate', 'ASQ24MoEIDate', 'same', 'datetime64[ns]']
    ,['ASQ24MoCSDate', 'ASQ24MoCSDate', 'same', 'datetime64[ns]']
    ,['ASQ30MoRefDate', 'ASQ30MoRefDate', 'same', 'datetime64[ns]']
    ,['ASQ30MoRefLocation', 'ASQ30MoRefLocation', 'same', 'string']
    ,['ASQ30MoEIDate', 'ASQ30MoEIDate', 'same', 'datetime64[ns]']
    ,['ASQ30MoCSDate', 'ASQ30MoCSDate', 'same', 'datetime64[ns]']
    ,['TGT Gender', 'TGT Gender', 'same', 'string']
    ,['TGT ETHNICITY', 'Tgt Ethnicity', '', 'string']
    ,['TGTRaceWhite', 'TGT Race White', '', 'boolean']
    ,['TGTRaceBlack', 'TGT Race Black', '', 'boolean']
    ,['TGTRaceIndianAlaskan', 'TGT Race Indian Alaskan', '', 'boolean']
    ,['TGTRaceAsian', 'TGT Race Asian', '', 'boolean']
    ,['TGTRaceHawaiianPacific', 'TGT Race Hawaiian Pacific', '', 'boolean']
    ,['TGTRaceOther', 'TGT Race Other', '', 'boolean']
    ,['Adaptation', 'Adaptation', 'same', 'string']
    ,['12 - 09 ASQ3_WhyNotDone', '12 - 09 ASQ3 WhyNotDone', '', 'string']
    ,['12 - 18 ASQ3_WhyNotDone', '12 - 18 ASQ3 WhyNotDone', '', 'string']
    ,['12 - 24 ASQ3_WhyNotDone', '12 - 24 ASQ3 WhyNotDone', '', 'string']
    ,['12 - 30 ASQ3_WhyNotDone', '12 - 30 ASQ3 WhyNotDone', '', 'string']
    ,['GESTATIONAL AGE', 'Gestational Age', '', 'string']
    ,['need_exclusion4', 'Need Exclusion4', '', 'string']
    ,['ZIP Code', 'ZIP Code', 'same', 'Int64']
]
#%%### df_14t_piece_tb2_3: 'Family Wise'.
dict_14t_colnames_tb2_3 = {x[0]:x[1] for x in list_14t_col_detail_tb2_3 if x[2] != 'same' and x[0] != x[1]}
# dict_14t_colnames_tb2_3
#%%### df_14t_piece_tb2_3: 'Family Wise'.
dict_14t_col_dtypes_tb2_3 = {x[0]:x[3] for x in list_14t_col_detail_tb2_3}
print(dict_14t_col_dtypes_tb2_3)
print(collections.Counter(list(dict_14t_col_dtypes_tb2_3.values())))

#######################
#%%### df_14t_piece_tb2_4: 'LLCHD'.
list_14t_col_detail_tb2_4 = [
    ['project_id', 'project id (LLCHD)', '', 'string']
    ,['year', 'year (LLCHD)', '', 'Int64']
    ,['quarter', 'quarter (LLCHD)', '', 'Int64']
    ,['site_id', 'Site Id', '', 'string']
    ,['family_id', 'Family Id', '', 'string']
    ,['tgt_id', 'Tgt Id', '', 'string']
    ,['tgt_dob', 'Tgt Dob', '', 'datetime64[ns]']
    ,['tgt_gender', 'Tgt Gender', '', 'string']
    ,['tgt_ethnicity', 'Tgt Ethnicity1', '', 'string']
    ,['tgt_race_indian', 'Tgt Race Indian', '', 'string']
    ,['tgt_race_asian', 'Tgt Race Asian', '', 'string']
    ,['tgt_race_black', 'Tgt Race Black', '', 'string']
    ,['tgt_race_hawaiian', 'Tgt Race Hawaiian', '', 'string']
    ,['tgt_race_white', 'Tgt Race White', '', 'string']
    ,['tgt_race_other', 'Tgt Race Other', '', 'string']
    ,['tgt_GestationalAge', 'tgt GestationalAge', '', 'Int64']
    ,['tgt_medical_home', 'Tgt Medical Home', '', 'Int64']
    ,['tgt_medical_home_dt', 'Tgt Medical Home Dt', '', 'datetime64[ns]']
    ,['tgt_dental_home', 'Tgt Dental Home', '', 'Int64']
    ,['tgt_dental_home_dt', 'Tgt Dental Home Dt', '', 'datetime64[ns]']
    ,['dt_edc', 'Dt Edc', '', 'datetime64[ns]']
    ,['enroll_dt', 'Enroll Dt', '', 'datetime64[ns]']
    ,['enroll_preg_status', 'Enroll Preg Status', '', 'string']
    ,['current_pregnancy', 'Current Pregnancy', '', 'string']
    ,['discharge_reason', 'Discharge Reason', '', 'string'] ### SHOULD be 'string' even through was 'int' in Tableau.
    ,['discharge_dt', 'Discharge Dt', '', 'datetime64[ns]']
    ,['last_home_visit', 'Last Home Visit', '', 'datetime64[ns]']
    ,['home_visits_num', 'Home Visits Num', '', 'Int64']
    ,['home_visits_pre', 'Home Visits Pre', '', 'Int64']
    ,['home_visits_post', 'Home Visits Post', '', 'Int64']
    ,['home_visits_person', 'Home Visits Person', '', 'Int64']
    ,['home_visits_virtual', 'Home Visits Virtual', '', 'Int64']
    ,['funding', 'Funding', '', 'string']
    ,['c_fundingdate', 'C Fundingdate', '', 'datetime64[ns]']
    ,['p_funding', 'P Funding', '', 'string']
    ,['p_fundingdate', 'P Fundingdate', '', 'datetime64[ns]']
    ,['primary_id', 'Primary Id', '', 'string']
    ,['primary_relation', 'Primary Relation', '', 'string']
    ,['mob_id', 'Mob Id', '', 'string']
    ,['mob_dob', 'Mob Dob', '', 'datetime64[ns]']
    ,['mob_gender', 'Mob Gender', '', 'string']
    ,['mob_ethnicity', 'Mob Ethnicity', '', 'string']
    ,['mob_race', 'Mob Race', '', 'string']
    ,['mob_race_indian', 'Mob Race Indian', '', 'string']
    ,['mob_race_asian', 'Mob Race Asian', '', 'string']
    ,['mob_race_black', 'Mob Race Black', '', 'string']
    ,['mob_race_hawaiian', 'Mob Race Hawaiian', '', 'string']
    ,['mob_race_white', 'Mob Race White', '', 'string']
    ,['mob_race_other', 'Mob Race Other', '', 'string']
    ,['mob_marital_status', 'Mob Marital Status', '', 'string']
    ,['mob_living_arrangement', 'Mob Living Arrangement', '', 'string']
    ,['mob_living_arrangement_dt', 'Mob Living Arrangement Dt', '', 'datetime64[ns]']
    ,['fob_id', 'Fob Id', '', 'string']
    ,['fob_dob', 'Fob Dob', '', 'datetime64[ns]']
    ,['fob_gender', 'Fob Gender', '', 'string']
    ,['fob_ethnicity', 'Fob Ethnicity', '', 'string']
    ,['fob_race', 'Fob Race', '', 'string']
    ,['fob_race_indian', 'Fob Race Indian', '', 'string']
    ,['fob_race_asian', 'Fob Race Asian', '', 'string']
    ,['fob_race_black', 'Fob Race Black', '', 'string']
    ,['fob_race_hawaiian', 'Fob Race Hawaiian', '', 'string']
    ,['fob_race_white', 'Fob Race White', '', 'string']
    ,['fob_race_other', 'Fob Race Other', '', 'string']
    ,['fob_marital_status', 'Fob Marital Status', '', 'string']
    ,['fob_living_arrangement', 'Fob Living Arrangement', '', 'string']
    ,['fob_living_arrangement_dt', 'Fob Living Arrangement Dt', '', 'datetime64[ns]']
    ,['fob_edu_dt', 'Fob Edu Dt', '', 'datetime64[ns]']
    ,['fob_edu', 'Fob Edu', '', 'string']
    ,['fob_employ_dt', 'Fob Employ Dt', '', 'datetime64[ns]']
    ,['fob_employ', 'Fob Employ', '', 'string']
    ,['fob_involved', 'Fob Involved', '', 'string']
    ,['zip', 'zip', 'same', 'Int64']
    ,['fob_visits', 'Fob Visits', '', 'string']
    ,['household_income', 'Household Income', '', 'string']
    ,['household_size', 'Household Size', '', 'string']
    ,['mcafss_income_dt', 'Mcafss Income Dt', '', 'datetime64[ns]']
    ,['mcafss_income', 'Mcafss Income', '', 'string']
    ,['mcafss_edu_dt1', 'Mcafss Edu Dt1', '', 'datetime64[ns]']
    ,['mcafss_edu1', 'Mcafss Edu1', '', 'string']
    ,['mcafss_edu1_enroll', 'Mcafss Edu1 Enroll', '', 'string']
    ,['mcafss_edu1_enroll_dt', 'Mcafss Edu1 Enroll Dt', '', 'datetime64[ns]']
    ,['mcafss_edu1_prog', 'Mcafss Edu1 Prog', '', 'string']
    ,['mcafss_edu_dt2', 'Mcafss Edu Dt2', '', 'datetime64[ns]']
    ,['mcafss_edu2', 'Mcafss Edu2', '', 'string']
    ,['mcafss_edu2_enroll', 'Mcafss Edu2 Enroll', '', 'string']
    ,['mcafss_edu2_enroll_dt', 'Mcafss Edu2 Enroll Dt', '', 'datetime64[ns]']
    ,['mcafss_edu2_prog', 'Mcafss Edu2 Prog', '', 'string']
    ,['mcafss_employ_dt', 'Mcafss Employ Dt', '', 'datetime64[ns]']
    ,['mcafss_employ', 'Mcafss Employ', '', 'string']
    ,['language_primary', 'Language Primary', '', 'string']
    ,['priority_child_welfare', 'Priority Child Welfare', '', 'string']
    ,['priority_substance_abuse', 'Priority Substance Abuse', '', 'string']
    ,['priority_tobacco_use', 'Priority Tobacco Use', '', 'string']
    ,['priority_low_student', 'Priority Low Student', '', 'string']
    ,['priority_develop_delays', 'Priority Develop Delays', '', 'string']
    ,['priority_military', 'Priority Military', '', 'string']
    ,['uncope_dt', 'Uncope Dt', '', 'datetime64[ns]']
    ,['uncope_score', 'Uncope Score', '', 'string']
    ,['substance_abuse_ref_dt', 'Substance Abuse Ref Dt', '', 'datetime64[ns]']
    ,['tobacco_use', 'Tobacco Use', '', 'string']
    ,['tobacco_use_dt', 'Tobacco Use Dt', '', 'datetime64[ns]']
    ,['tobacco_ref_dt', 'Tobacco Ref Dt', '', 'datetime64[ns]']
    ,['safe_sleep_yes', 'Safe Sleep Yes', '', 'string']
    ,['safe_sleep_yes_dt', 'Safe Sleep Yes Dt', '', 'datetime64[ns]']
    ,['cheeers_date', 'Cheeers Date', '', 'datetime64[ns]']
    ,['early_language', 'Early Language', '', 'string']
    ,['early_language_dt', 'Early Language Dt', '', 'datetime64[ns]']
    ,['asq3_dt_9mm', 'Asq3 Dt 9Mm', '', 'datetime64[ns]']
    ,['asq3_timing_9mm', 'Asq3 Timing 9Mm', '', 'Int64']
    ,['asq3_comm_9mm', 'Asq3 Comm 9Mm', '', 'Int64']
    ,['asq3_gross_9mm', 'Asq3 Gross 9Mm', '', 'Int64']
    ,['asq3_fine_9mm', 'Asq3 Fine 9Mm', '', 'Int64']
    ,['asq3_problem_9mm', 'Asq3 Problem 9Mm', '', 'Int64']
    ,['asq3_social_9mm', 'Asq3 Social 9Mm', '', 'Int64']
    ,['asq3_feedback_9mm', 'Asq3 Feedback 9Mm', '', 'string']
    ,['asq3_referral_9mm', 'Asq3 Referral 9Mm', '', 'datetime64[ns]'] ### 'date' in Tableau & needs to be read in as such.
    ,['asq3_dt_18mm', 'Asq3 Dt 18Mm', '', 'datetime64[ns]']
    ,['asq3_timing_18mm', 'Asq3 Timing 18Mm', '', 'Int64']
    ,['asq3_comm_18mm', 'Asq3 Comm 18Mm', '', 'Int64']
    ,['asq3_gross_18mm', 'Asq3 Gross 18Mm', '', 'Int64']
    ,['asq3_fine_18mm', 'Asq3 Fine 18Mm', '', 'Int64']
    ,['asq3_problem_18mm', 'Asq3 Problem 18Mm', '', 'Int64']
    ,['asq3_social_18mm', 'Asq3 Social 18Mm', '', 'Int64']
    ,['asq3_feedback_18mm', 'Asq3 Feedback 18Mm', '', 'string']
    ,['asq3_referral_18mm', 'Asq3 Referral 18Mm', '', 'datetime64[ns]']
    ,['asq3_dt_24mm', 'Asq3 Dt 24Mm', '', 'datetime64[ns]']
    ,['asq3_timing_24mm', 'Asq3 Timing 24Mm', '', 'Int64']
    ,['asq3_comm_24mm', 'Asq3 Comm 24Mm', '', 'Int64']
    ,['asq3_gross_24mm', 'Asq3 Gross 24Mm', '', 'Int64']
    ,['asq3_fine_24mm', 'Asq3 Fine 24Mm', '', 'Int64']
    ,['asq3_problem_24mm', 'Asq3 Problem 24Mm', '', 'Int64']
    ,['asq3_social_24mm', 'Asq3 Social 24Mm', '', 'Int64']
    ,['asq3_feedback_24mm', 'Asq3 Feedback 24Mm', '', 'string']
    ,['asq3_referral_24mm', 'Asq3 Referral 24Mm', '', 'datetime64[ns]']
    ,['asq3_dt_30mm', 'Asq3 Dt 30Mm', '', 'datetime64[ns]']
    ,['asq3_timing_30mm', 'Asq3 Timing 30Mm', '', 'Int64']
    ,['asq3_comm_30mm', 'Asq3 Comm 30Mm', '', 'Int64']
    ,['asq3_gross_30mm', 'Asq3 Gross 30Mm', '', 'Int64']
    ,['asq3_fine_30mm', 'Asq3 Fine 30Mm', '', 'Int64']
    ,['asq3_problem_30mm', 'Asq3 Problem 30Mm', '', 'Int64']
    ,['asq3_social_30mm', 'Asq3 Social 30Mm', '', 'Int64']
    ,['asq3_feedback_30mm', 'Asq3 Feedback 30Mm', '', 'string']
    ,['asq3_referral_30mm', 'Asq3 Referral 30Mm', '', 'datetime64[ns]']
    ,['behavioral_concerns', 'Behavioral Concerns', '', 'Int64']
    ,['ipv_screen', 'Ipv Screen', '', 'string']
    ,['ipv_screen_dt', 'Ipv Screen Dt', '', 'datetime64[ns]']
    ,['ipv_referral_dt', 'Ipv Referral Dt', '', 'datetime64[ns]']
    ,['prim_care_dt', 'Prim Care Dt', '', 'datetime64[ns]']
    ,['cesd_dt', 'Cesd Dt', '', 'datetime64[ns]']
    ,['cesd_score', 'Cesd Score', '', 'string']
    ,['ment_hlth_ref_dt', 'Ment Hlth Ref Dt', '', 'datetime64[ns]']
    ,['lsp_bf_initiation_dt', 'Lsp Bf Initiation Dt', '', 'datetime64[ns]']
    ,['lsp_bf_discon_dt', 'Lsp Bf Discon Dt', '', 'datetime64[ns]']
    ,['hlth_insure_mob', 'Hlth Insure Mob', '', 'Int64']
    ,['hlth_insure_mob_dt', 'Hlth Insure Mob Dt', '', 'datetime64[ns]']
    ,['hlth_insure_tgt', 'Hlth Insure Tgt', '', 'Int64']
    ,['hlth_insure_tgt_dt', 'Hlth Insure Tgt Dt', '', 'datetime64[ns]']
    ,['last_well_child_visit', 'Last Well Child Visit', '', 'datetime64[ns]']
    ,['hlth_insure_fob', 'Hlth Insure Fob', '', 'Int64']
    ,['hlth_insure_fob_dt', 'Hlth Insure Fob Dt', '', 'Int64'] ### should be 'datetime64[ns]'.
    ,['need_exclusion1', 'Need Exclusion1', '', 'string']
    ,['need_exclusion2', 'Need Exclusion2', '', 'string']
    ,['need_exclusion3', 'Need Exclusion3', '', 'string']
    ,['need_exclusion4', 'need exclusion4 (LLCHD)', '', 'string']
    ,['need_exclusion5', 'Need Exclusion5', '', 'string']
    ,['need_exclusion6', 'Need Exclusion6', '', 'string']
    ,['Has_ChildWelfareAdaptation', 'Has ChildWelfareAdaptation', '', 'string']
]
#%%### df_14t_piece_tb2_4: 'LLCHD'.
dict_14t_colnames_tb2_4 = {x[0]:x[1] for x in list_14t_col_detail_tb2_4 if x[2] != 'same' and x[0] != x[1]}
# dict_14t_colnames_tb2_4
#%%### df_14t_piece_tb2_4: 'LLCHD'.
dict_14t_col_dtypes_tb2_4 = {x[0]:x[3] for x in list_14t_col_detail_tb2_4}
print(dict_14t_col_dtypes_tb2_4)
print(collections.Counter(list(dict_14t_col_dtypes_tb2_4.values())))

#######################
#%%### df_14t_piece_tb2_5: 'Well Child'.
list_14t_col_detail_tb2_5 = [
    ['ProjectID', 'Project ID1', '', 'string']
    ,['year', 'year (Well Child)', '', 'Int64']
    ,['quarter', 'quarter (Well Child)', '', 'Int64']
    ,['agency', 'agency (Well Child)', '', 'string']
    ,['FAMILYNUMBER', 'Familynumber', '', 'string']
    ,['ChildNumber', 'ChildNumber (Well Child)', '', 'string']
    ,['funding', 'funding (Well Child)', '', 'string']
    ,['WellVisitDate.1', 'WellVisitDate.1', 'same', 'datetime64[ns]']
    ,['WellVisitDate.2', 'WellVisitDate.2', 'same', 'datetime64[ns]']
    ,['WellVisitDate.3', 'WellVisitDate.3', 'same', 'datetime64[ns]']
    ,['WellVisitDate.4', 'WellVisitDate.4', 'same', 'datetime64[ns]']
    ,['WellVisitDate.5', 'WellVisitDate.5', 'same', 'datetime64[ns]']
    ,['WellVisitDate.6', 'WellVisitDate.6', 'same', 'datetime64[ns]']
    ,['WellVisitDate.7', 'WellVisitDate.7', 'same', 'datetime64[ns]']
    ,['WellVisitDate.8', 'WellVisitDate.8', 'same', 'datetime64[ns]']
    ,['WellVisitDate.9', 'WellVisitDate.9', 'same', 'datetime64[ns]']
    ,['WellVisitDate.10', 'WellVisitDate.10', 'same', 'datetime64[ns]']
    ,['WellVisitDate.11', 'WellVisitDate.11', 'same', 'datetime64[ns]']
    ,['WellVisitDate.12', 'WellVisitDate.12', 'same', 'datetime64[ns]']
    ,['WellVisitDate.13', 'WellVisitDate.13', 'same', 'datetime64[ns]']
    ,['WellVisitDate.14', 'WellVisitDate.14', 'same', 'datetime64[ns]']
    ,['WellVisitDate.15', 'WellVisitDate.15', 'same', 'datetime64[ns]']
    ,['WellVisitDate.16', 'WellVisitDate.16', 'same', 'datetime64[ns]']
    ,['WellVisitDate.17', 'WellVisitDate.17', 'same', 'datetime64[ns]']
    ,['WellVisitDate.18', 'WellVisitDate.18', 'same', 'datetime64[ns]']
    ,['WellVisitDate.19', 'WellVisitDate.19', 'same', 'datetime64[ns]']
    ,['WellVisitDate.20', 'WellVisitDate.20', 'same', 'datetime64[ns]']
    ,['WellVisitDate.21', 'WellVisitDate.21', 'same', 'datetime64[ns]']
    ,['WellVisitDate.22', 'WellVisitDate.22', 'same', 'datetime64[ns]']
    ,['WellVisitDate.23', 'WellVisitDate.23', 'same', 'datetime64[ns]']
    ,['WellVisitDate.24', 'WellVisitDate.24', 'same', 'datetime64[ns]']
    ,['WellVisitDate.25', 'WellVisitDate.25', 'same', 'datetime64[ns]']
    ,['WellVisitDate.26', 'WellVisitDate.26', 'same', 'datetime64[ns]']
]
#%%### df_14t_piece_tb2_5: 'Well Child'.
dict_14t_colnames_tb2_5 = {x[0]:x[1] for x in list_14t_col_detail_tb2_5 if x[2] != 'same' and x[0] != x[1]}
# dict_14t_colnames_tb2_5
#%%### df_14t_piece_tb2_5: 'Well Child'.
dict_14t_col_dtypes_tb2_5 = {x[0]:x[3] for x in list_14t_col_detail_tb2_5}
print(dict_14t_col_dtypes_tb2_5)
print(collections.Counter(list(dict_14t_col_dtypes_tb2_5.values())))


#%%##################################################
### READ ###
#####################################################

#%%
### Performance benefit for reading in file to memory only once by creating an ExcelFile class object.
xlsx_14t_tb2 = pd.ExcelFile(path_14t_data_source_file_tb2)

#%% 
### CHECK that all list_path_14t_data_source_sheets_tb2 same as xlsx.sheet_names (different order ok):
print(sorted(list_path_14t_data_source_sheets_tb2))
print([x for x in sorted(xlsx_14t_tb2.sheet_names) if x != 'Birth File'])
sorted(list_path_14t_data_source_sheets_tb2) == [x for x in sorted(xlsx_14t_tb2.sheet_names) if x != 'Birth File']

#%%
### READ all sheets:
# df_14t_piece_tb2_1 = pd.read_excel(xlsx_14t_tb2, sheet_name=list_path_14t_data_source_sheets_tb2[0], keep_default_na=False, na_values=[''])#, dtype=dict_14t_col_dtypes_tb2_1)
# df_14t_piece_tb2_2 = pd.read_excel(xlsx_14t_tb2, sheet_name=list_path_14t_data_source_sheets_tb2[1], keep_default_na=False, na_values=[''])#, dtype=dict_14t_col_dtypes_tb2_2)
# df_14t_piece_tb2_3 = pd.read_excel(xlsx_14t_tb2, sheet_name=list_path_14t_data_source_sheets_tb2[2], keep_default_na=False, na_values=[''], dtype={'BreastFeeding':'string', 'ReadTellStorySing':'string'})#, dtype=dict_14t_col_dtypes_tb2_3) ### object not string because code can't handle NA's until end.
# df_14t_piece_tb2_4 = pd.read_excel(xlsx_14t_tb2, sheet_name=list_path_14t_data_source_sheets_tb2[3], keep_default_na=False, na_values=[''], dtype={'asq3_referral_9mm': 'datetime64[ns]'})#, dtype=dict_14t_col_dtypes_tb2_4)
# df_14t_piece_tb2_5 = pd.read_excel(xlsx_14t_tb2, sheet_name=list_path_14t_data_source_sheets_tb2[4], keep_default_na=False, na_values=[''])#, dtype=dict_14t_col_dtypes_tb2_5)

#%%###################################
### READ in all sheets as strings.

### To read in EVERYTHING as a string WITH NA:
df_14t_allstring_tb2_1 = pd.read_excel(xlsx_14t_tb2, sheet_name=list_path_14t_data_source_sheets_tb2[0], keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_14t_col_dtypes_tb2_1)
df_14t_allstring_tb2_2 = pd.read_excel(xlsx_14t_tb2, sheet_name=list_path_14t_data_source_sheets_tb2[1], keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_14t_col_dtypes_tb2_2)
df_14t_allstring_tb2_3 = pd.read_excel(xlsx_14t_tb2, sheet_name=list_path_14t_data_source_sheets_tb2[2], keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_14t_col_dtypes_tb2_3)
df_14t_allstring_tb2_4 = pd.read_excel(xlsx_14t_tb2, sheet_name=list_path_14t_data_source_sheets_tb2[3], keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_14t_col_dtypes_tb2_4)
df_14t_allstring_tb2_5 = pd.read_excel(xlsx_14t_tb2, sheet_name=list_path_14t_data_source_sheets_tb2[4], keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_14t_col_dtypes_tb2_5)

df_14t_piece_tb2_1 = df_14t_allstring_tb2_1.copy()
df_14t_piece_tb2_2 = df_14t_allstring_tb2_2.copy()
df_14t_piece_tb2_3 = df_14t_allstring_tb2_3.copy()
df_14t_piece_tb2_4 = df_14t_allstring_tb2_4.copy()
df_14t_piece_tb2_5 = df_14t_allstring_tb2_5.copy()


#%%##################################################
### CLEAN ###
#####################################################

#%%###################################
### df_14t_piece_tb2_1: 'Project ID'.
df_14t_piece_tb2_1 = (
    df_14t_piece_tb2_1
    ###.astype(dict_14t_col_dtypes_tb2_1)
    .pipe(fn_apply_dtypes, dict_14t_col_dtypes_tb2_1)
)
# inspect_df(df_14t_piece_tb2_1)

#%%###################################
### df_14t_piece_tb2_2: 'Caregiver Insurance'.
df_14t_piece_tb2_2 = (
    df_14t_piece_tb2_2
    ###.astype(dict_14t_col_dtypes_tb2_2)
    .pipe(fn_apply_dtypes, dict_14t_col_dtypes_tb2_2)
)
# inspect_df(df_14t_piece_tb2_2)

#%%###################################
### df_14t_piece_tb2_3: 'Family Wise'.
df_14t_piece_tb2_3 = (
    df_14t_piece_tb2_3
    ###.astype(dict_14t_col_dtypes_tb2_3)
    .pipe(fn_apply_dtypes, dict_14t_col_dtypes_tb2_3)
) 
# inspect_df(df_14t_piece_tb2_3)

#%%###################################
### df_14t_piece_tb2_4: 'LLCHD'.
df_14t_piece_tb2_4 = (
    df_14t_piece_tb2_4
    ###.astype(dict_14t_col_dtypes_tb2_4)
    .pipe(fn_apply_dtypes, dict_14t_col_dtypes_tb2_4)
)
# inspect_df(df_14t_piece_tb2_4)

#%%###################################
### df_14t_piece_tb2_5: 'MOB or FOB'.
df_14t_piece_tb2_5 = (
    df_14t_piece_tb2_5
    ###.astype(dict_14t_col_dtypes_tb2_5)
    .pipe(fn_apply_dtypes, dict_14t_col_dtypes_tb2_5)
)
# inspect_df(df_14t_piece_tb2_5)


#%%###################################
### Review each sheet:
### Note: Even empty DFs merge fine below.

# #%%
# inspect_df(df_14t_piece_tb2_1)
# #%%
# inspect_df(df_14t_piece_tb2_2)
# #%%
# inspect_df(df_14t_piece_tb2_3)
# #%%
# inspect_df(df_14t_piece_tb2_4)
# #%%
# inspect_df(df_14t_piece_tb2_5)


#%%##################################################
### Rename Columns ###
#####################################################

#######################
#%%### df_14t_piece_tb2_1: 'Project ID'.
### Rename df_14t_piece_tb2_1 
# [*df_14t_piece_tb2_1]
# dict_14t_colnames_tb2_1
df_14t_piece_tb2_1 = df_14t_piece_tb2_1.rename(columns=dict_14t_colnames_tb2_1)
# [*df_14t_piece_tb2_1]

#######################
#%%### df_14t_piece_tb2_2: 'ER Injury'.
### Rename df_14t_piece_tb2_1 
# [*df_14t_piece_tb2_2]
# dict_14t_colnames_tb2_2
df_14t_piece_tb2_2 = df_14t_piece_tb2_2.rename(columns=dict_14t_colnames_tb2_2)
# [*df_14t_piece_tb2_2]

#######################
#%%### df_14t_piece_tb2_3: 'Family Wise'.
### Rename df_14t_piece_tb2_1 
# [*df_14t_piece_tb2_3]
# dict_14t_colnames_tb2_3
df_14t_piece_tb2_3 = df_14t_piece_tb2_3.rename(columns=dict_14t_colnames_tb2_3)
# [*df_14t_piece_tb2_3]

#######################
#%%### df_14t_piece_tb2_4: 'LLCHD'.
### Rename df_14t_piece_tb2_1 
# [*df_14t_piece_tb2_4]
# dict_14t_colnames_tb2_4
df_14t_piece_tb2_4 = df_14t_piece_tb2_4.rename(columns=dict_14t_colnames_tb2_4)
# [*df_14t_piece_tb2_4]

#######################
#%%### df_14t_piece_tb2_5: 'Well Child'.
### Rename df_14t_piece_tb2_1 
# [*df_14t_piece_tb2_5]
# dict_14t_colnames_tb2_5
df_14t_piece_tb2_5 = df_14t_piece_tb2_5.rename(columns=dict_14t_colnames_tb2_5)
# [*df_14t_piece_tb2_5]


#%%##################################################
### Prep for JOIN ###
#####################################################

### Each row SHOULD be unique on these sheets, especially the 'Project ID' sheet.

#%%### Restart deduplication
### df_14t_piece_tb2_1 = df_14t_bf_ddup_tb2_1.copy()
### df_14t_piece_tb2_2 = df_14t_bf_ddup_tb2_2.copy()
### df_14t_piece_tb2_3 = df_14t_bf_ddup_tb2_3.copy()
### df_14t_piece_tb2_4 = df_14t_bf_ddup_tb2_4.copy()
### df_14t_piece_tb2_5 = df_14t_bf_ddup_tb2_5.copy()

#######################
### NOTE: Q1?Q2?: 6 duplicate rows. #TODO: Fix in Master File creation.
### NOTE: Y12Q4: 12 duplicate rows. #TODO: Fix in Master File creation.
#%%### df_14t_piece_tb2_1: 'Project ID'. 
### Backup:
df_14t_bf_ddup_tb2_1 = df_14t_piece_tb2_1.copy()
#%%### df_14t_piece_tb2_1: 'Project ID'. 
### Duplicate rows:
df_14t_piece_tb2_1[df_14t_piece_tb2_1.duplicated()]
#%%### df_14t_piece_tb2_1: 'Project ID'. 
### Dropping duplicate rows:
if bool_14t_deduplicate_tb2:
    df_14t_piece_tb2_1 = df_14t_piece_tb2_1.drop_duplicates(ignore_index=True)
df_14t_piece_tb2_1
#%%### df_14t_piece_tb2_1: 'Project ID'. 
### Test
len(df_14t_bf_ddup_tb2_1) - len(df_14t_piece_tb2_1) == len(df_14t_bf_ddup_tb2_1[df_14t_bf_ddup_tb2_1.duplicated()])
#%%### df_14t_piece_tb2_1: 'Project ID'. 
if (len(df_14t_bf_ddup_tb2_1) != len(df_14t_piece_tb2_1)):
    print(f'{len(df_14t_bf_ddup_tb2_1) - len(df_14t_piece_tb2_1)} duplicate rows dropped.')
elif (len(df_14t_bf_ddup_tb2_1) == len(df_14t_piece_tb2_1)):
    print('No duplicate rows.')
else:
    print("Don't know what's going on here!")
#######################
#%%### df_14t_piece_tb2_1: 'Project ID'. 
### join columns: ['Project Id','Year','Quarter']
### Show rows where join columns are same BUT some other columns are not:
df_14t_piece_tb2_1[df_14t_piece_tb2_1[['Project Id','Year','Quarter']].duplicated(keep=False)]

#######################
### NOTE: Q1?Q2?: NO ROWS.
### NOTE: Y12Q4: No duplicate rows.
#%%### df_14t_piece_tb2_2: 'ER Injury'.
df_14t_bf_ddup_tb2_2 = df_14t_piece_tb2_2.copy()
#%%### df_14t_piece_tb2_2: 'ER Injury'.
df_14t_piece_tb2_2[df_14t_piece_tb2_2.duplicated()]
# df_14t_piece_tb2_2[df_14t_piece_tb2_2.duplicated(keep=False, subset=['Project ID (ER Injury)','year (ER Injury)','quarter (ER Injury)'])]
#%%### df_14t_piece_tb2_2: 'ER Injury'.
if bool_14t_deduplicate_tb2:
    df_14t_piece_tb2_2 = df_14t_piece_tb2_2.drop_duplicates(ignore_index=True)
df_14t_piece_tb2_2
#%%### df_14t_piece_tb2_2: 'ER Injury'.
len(df_14t_bf_ddup_tb2_2) - len(df_14t_piece_tb2_2) == len(df_14t_bf_ddup_tb2_2[df_14t_bf_ddup_tb2_2.duplicated()])
#%%### df_14t_piece_tb2_2: 'ER Injury'.
if (len(df_14t_bf_ddup_tb2_2) != len(df_14t_piece_tb2_2)):
    print(f'{len(df_14t_bf_ddup_tb2_2) - len(df_14t_piece_tb2_2)} duplicate rows dropped.')
elif (len(df_14t_bf_ddup_tb2_2) == len(df_14t_piece_tb2_2)):
    print('No duplicate rows.')
else:
    print("Don't know what's going on here!")
#######################
#%%### df_14t_piece_tb2_2: 'ER Injury'.
### join columns: ['Project ID (ER Injury)','year (ER Injury)','quarter (ER Injury)']
### Show rows where join columns are same BUT some other columns are not:
df_14t_piece_tb2_2[df_14t_piece_tb2_2[['Project ID (ER Injury)','year (ER Injury)','quarter (ER Injury)']].duplicated(keep=False)]

#######################
### NOTE: Q1?Q2?: 6 duplicate rows. #TODO: Fix in Master File creation.
### NOTE: Y12Q4: 22 duplicate rows. #TODO: Fix in Master File creation.
#%%### df_14t_piece_tb2_3: 'Family Wise'.
df_14t_bf_ddup_tb2_3 = df_14t_piece_tb2_3.copy()
#%%### df_14t_piece_tb2_3: 'Family Wise'.
df_14t_piece_tb2_3[df_14t_piece_tb2_3.duplicated()]
# df_14t_piece_tb2_3[df_14t_piece_tb2_3.duplicated(keep=False, subset=['Project ID','year (Family Wise)','quarter (Family Wise)'])]
#%%### df_14t_piece_tb2_3: 'Family Wise'.
if bool_14t_deduplicate_tb2:
    df_14t_piece_tb2_3 = df_14t_piece_tb2_3.drop_duplicates(ignore_index=True)
df_14t_piece_tb2_3
#%%### df_14t_piece_tb2_3: 'Family Wise'.
len(df_14t_bf_ddup_tb2_3) - len(df_14t_piece_tb2_3) == len(df_14t_bf_ddup_tb2_3[df_14t_bf_ddup_tb2_3.duplicated()])
#%%### df_14t_piece_tb2_3: 'Family Wise'.
if (len(df_14t_bf_ddup_tb2_3) != len(df_14t_piece_tb2_3)):
    print(f'{len(df_14t_bf_ddup_tb2_3) - len(df_14t_piece_tb2_3)} duplicate rows dropped.')
elif (len(df_14t_bf_ddup_tb2_3) == len(df_14t_piece_tb2_3)):
    print('No duplicate rows.')
else:
    print("Don't know what's going on here!")
#######################
#%%### df_14t_piece_tb2_3: 'Family Wise'.
### join columns: ['Project ID','year (Family Wise)','quarter (Family Wise)']
### Show rows where join columns are same BUT some other columns are not:
cols_14t_forJoin_tb2_3 = ['Project ID','year (Family Wise)','quarter (Family Wise)']
df_14t_piece_tb2_3[df_14t_piece_tb2_3[cols_14t_forJoin_tb2_3].duplicated(keep=False)]

#%%
TESTdf_14t_piece_tb2_3 = df_14t_piece_tb2_3[df_14t_piece_tb2_3[cols_14t_forJoin_tb2_3].duplicated(keep=False)]
print([col for col in [col for col in [*TESTdf_14t_piece_tb2_3] if col not in cols_14t_forJoin_tb2_3] if (len(TESTdf_14t_piece_tb2_3.loc[TESTdf_14t_piece_tb2_3.index[0:2], col].value_counts(dropna=False)) != 1)])
print([col for col in [col for col in [*TESTdf_14t_piece_tb2_3] if col not in cols_14t_forJoin_tb2_3] if (len(TESTdf_14t_piece_tb2_3.loc[TESTdf_14t_piece_tb2_3.index[2:4], col].value_counts(dropna=False)) != 1)])

#%%
TESTdf_14t_piece_tb2_3.loc[TESTdf_14t_piece_tb2_3.index[0:2], cols_14t_forJoin_tb2_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb2_3] if col not in cols_14t_forJoin_tb2_3] if (len(TESTdf_14t_piece_tb2_3.loc[TESTdf_14t_piece_tb2_3.index[0:2], col].value_counts(dropna=False)) != 1)]]
#%%
TESTdf_14t_piece_tb2_3.loc[TESTdf_14t_piece_tb2_3.index[2:4], cols_14t_forJoin_tb2_3 + [col for col in [col for col in [*TESTdf_14t_piece_tb2_3] if col not in cols_14t_forJoin_tb2_3] if (len(TESTdf_14t_piece_tb2_3.loc[TESTdf_14t_piece_tb2_3.index[2:4], col].value_counts(dropna=False)) != 1)]]
### TODO: fix duplicates in Excel.

#######################
### NOTE: NO duplicate rows.
#%%### df_14t_piece_tb2_4: 'LLCHD'.
df_14t_bf_ddup_tb2_4 = df_14t_piece_tb2_4.copy()
#%%### df_14t_piece_tb2_4: 'LLCHD'.
df_14t_piece_tb2_4[df_14t_piece_tb2_4.duplicated()]
# df_14t_piece_tb2_4[df_14t_piece_tb2_4.duplicated(keep=False, subset=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)'])]
#%%### df_14t_piece_tb2_4: 'LLCHD'.
if bool_14t_deduplicate_tb2:
    df_14t_piece_tb2_4 = df_14t_piece_tb2_4.drop_duplicates(ignore_index=True)
df_14t_piece_tb2_4
#%%### df_14t_piece_tb2_4: 'LLCHD'.
len(df_14t_bf_ddup_tb2_4) - len(df_14t_piece_tb2_4) == len(df_14t_bf_ddup_tb2_4[df_14t_bf_ddup_tb2_4.duplicated()])
#%%### df_14t_piece_tb2_4: 'LLCHD'.
if (len(df_14t_bf_ddup_tb2_4) != len(df_14t_piece_tb2_4)):
    print(f'{len(df_14t_bf_ddup_tb2_4) - len(df_14t_piece_tb2_4)} duplicate rows dropped.')
elif (len(df_14t_bf_ddup_tb2_4) == len(df_14t_piece_tb2_4)):
    print('No duplicate rows.')
else:
    print("Don't know what's going on here!")
#######################
#%%### df_14t_piece_tb2_4: 'LLCHD'.
### join columns: ['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)'] 
### Show rows where join columns are same BUT some other columns are not:
df_14t_piece_tb2_4[df_14t_piece_tb2_4[['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)']].duplicated(keep=False)]

#######################
### NOTE: NO duplicate rows.
#%%### df_14t_piece_tb2_5: 'Well Child'.
df_14t_bf_ddup_tb2_5 = df_14t_piece_tb2_5.copy()
#%%### df_14t_piece_tb2_5: 'Well Child'.
df_14t_piece_tb2_5[df_14t_piece_tb2_5.duplicated()]
# df_14t_piece_tb2_5[df_14t_piece_tb2_5.duplicated(keep=False, subset=['Project ID1','year (Well Child)','quarter (Well Child)'])]
#%%### df_14t_piece_tb2_5: 'Well Child'.
if bool_14t_deduplicate_tb2:
    df_14t_piece_tb2_5 = df_14t_piece_tb2_5.drop_duplicates(ignore_index=True)
df_14t_piece_tb2_5
#%%### df_14t_piece_tb2_5: 'Well Child'.
len(df_14t_bf_ddup_tb2_5) - len(df_14t_piece_tb2_5) == len(df_14t_bf_ddup_tb2_5[df_14t_bf_ddup_tb2_5.duplicated()])
#%%### df_14t_piece_tb2_5: 'Well Child'.
if (len(df_14t_bf_ddup_tb2_5) != len(df_14t_piece_tb2_5)):
    print(f'{len(df_14t_bf_ddup_tb2_5) - len(df_14t_piece_tb2_5)} duplicate rows dropped.')
elif (len(df_14t_bf_ddup_tb2_5) == len(df_14t_piece_tb2_5)):
    print('No duplicate rows.')
else:
    print("Don't know what's going on here!")
#######################
#%%### df_14t_piece_tb2_5: 'Well Child'.
### join columns: ['Project ID1','year (Well Child)','quarter (Well Child)'] 
### Show rows where join columns are same BUT some other columns are not:
df_14t_piece_tb2_5[df_14t_piece_tb2_5[['Project ID1','year (Well Child)','quarter (Well Child)']].duplicated(keep=False)]

#%%##################################################
### JOIN ###
#####################################################

# #%%
# df_14t_base_tb2 = (
#     pd.merge(
#         df_14t_piece_tb2_1 ### 'Project ID'.
#         ,df_14t_piece_tb2_2 ### 'ER Injury'.
#         ,how='left' 
#         ,left_on=['Project Id','Year','Quarter'] 
#         ,right_on=['Project ID (ER Injury)','year (ER Injury)','quarter (ER Injury)'] 
#         ,indicator='LJ_tb2_2ER'
#         ,validate='one_to_one'
#     ).merge(
#         df_14t_piece_tb2_3 ### 'Family Wise'.
#         ,how='left' 
#         ,left_on=['Project Id','Year','Quarter'] 
#         ,right_on=['Project ID','year (Family Wise)','quarter (Family Wise)'] 
#         ,indicator='LJ_tb2_3FW'
#         # ,validate='one_to_one'
#     ).merge(
#         df_14t_piece_tb2_4 ### 'LLCHD'.
#         ,how='left' 
#         ,left_on=['Project Id','Year','Quarter'] 
#         ,right_on=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)'] 
#         ,indicator='LJ_tb2_4LL'
#         # ,validate='one_to_one'
#     ).merge(
#         df_14t_piece_tb2_5 ### 'Well Child'.
#         ,how='left' 
#         ,left_on=['Project Id','Year','Quarter'] 
#         ,right_on=['Project ID1','year (Well Child)','quarter (Well Child)'] 
#         ,indicator='LJ_tb2_5WC'
#         # ,validate='one_to_one'
#         ### ,validate='one_to_many'
#     ) 
# )

#%%
### New join: issues: (1) df_14t_piece_tb2_2_ER is one_to_many, (2) df_14t_piece_tb2_3_FW has 2 pairs of kind-of duplicate rows.
df_14t_base_tb2 = (
    pd.merge(
        df_14t_piece_tb2_1, ### 'Project ID'.
        df_14t_piece_tb2_4 ### 'LLCHD'.
        ,how='left' 
        ,left_on=['Project Id','Year','Quarter'] 
        ,right_on=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)'] 
        ,indicator='LJ_tb2_4LL'
        # ,validate='one_to_one' ### Y12Q4: Works for LL.
    ).merge(
        df_14t_piece_tb2_3 ### 'Family Wise'.
        ###,df_14t_piece_tb2_3[remove duplicate rows #TODO] ### 'Family Wise'.
        ,how='left' 
        ,left_on=['Project Id','Year','Quarter'] 
        ,right_on=['Project ID','year (Family Wise)','quarter (Family Wise)'] 
        ,indicator='LJ_tb2_3FW'
        #,validate='one_to_one'
    ).merge(
        df_14t_piece_tb2_5 ### 'Well Child'.
        ,how='left' 
        ,left_on=['Project Id','Year','Quarter'] 
        ,right_on=['Project ID1','year (Well Child)','quarter (Well Child)'] 
        ,indicator='LJ_tb2_5WC'
        # ,validate='one_to_one' ### works for only LL... but does does it apply to LL?
    ).merge(
        df_14t_piece_tb2_2 ### 'ER Injury'.
        ,how='left' 
        ,left_on=['Project Id','Year','Quarter'] 
        ,right_on=['Project ID (ER Injury)','year (ER Injury)','quarter (ER Injury)'] 
        ,indicator='LJ_tb2_2ER'
        # ,validate='one_to_one' ### works for only LL.
        ### ,validate='one_to_many'
    )
)

### Note: FW & LLCHD are created first.
### There could be duplicates in the FW table.
### Then Project ID tab created from IDs in two other tabs & then is deduplicated.
### Then other tabs added (auxiliary tables): In these there may be clients that do not match those active in the Project ID tab.


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
df_14t_edits1_tb2 = df_14t_base_tb2.copy()  ### Make a deep-ish copy of the DF's Data. Does NOT copy embedded mutable objects.


#####################################################
#####################################################
#####################################################
#%%##################################################

df_14t_edits1_tb2['Number of Records'] = 1 
# inspect_col(df_14t_edits1_tb2['Number of Records'])

#%%
df_14t_edits1_tb2['source'] = (
    df_14t_edits1_tb2
    .apply(func=(
        lambda df: 'FW' if pd.notna(df['Project ID']) else ('LL' if pd.notna('project id (LLCHD)') else 'um... problem')
    ), axis=1)
    .astype('string') 
)
# inspect_col(df_14t_edits1_tb2['source'])


#%%##################################################
### DUPLICATING

df_14t_edits1_tb2['_C18 ASQ 18 Mo Ref Location'] = df_14t_edits1_tb2['ASQ18MoRefLocation'].astype('string') 
    ### [ASQ18MoRefLocation]
    ### Data Type in Tableau: 'string'.

df_14t_edits1_tb2['_C18 ASQ 24 Mo Ref Location'] = df_14t_edits1_tb2['ASQ24MoRefLocation'].astype('string') 
    ### [ASQ24MoRefLocation]
    ### Data Type in Tableau: 'string'.

df_14t_edits1_tb2['_C18 ASQ 30 Mo Ref Location'] = df_14t_edits1_tb2['ASQ30MoRefLocation'].astype('string') 
    ### [ASQ30MoRefLocation]
    ### Data Type in Tableau: 'string'.

df_14t_edits1_tb2['_C18 ASQ 9 Mo Ref Location'] = df_14t_edits1_tb2['ASQ9MoRefLocation'].astype('string') 
    ### [ASQ9MoRefLocation]
    ### Data Type in Tableau: 'string'.

#%%##################################################
### COALESCING

    ### combine_first() fills in NA/Missing locations in the first column with values from the 2nd. Can still have missing at end if both NA/Missing.

df_14t_edits1_tb2['_Agency'] = df_14t_edits1_tb2['Agency'].combine_first(df_14t_edits1_tb2['Site Id']).astype('string') 
    ### IFNULL([Agency],[Site Id])
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_Agency'])
# #%%
# inspect_col(df_14t_edits1_tb2['Agency'])
# #%%
# inspect_col(df_14t_edits1_tb2['Site Id'])
# #%%

#%%###################################

df_14t_edits1_tb2['_C11 Literacy'] = df_14t_edits1_tb2['Max Early Literacy Date'].combine_first(df_14t_edits1_tb2['Early Language Dt']).astype('datetime64[ns]') 
    ### IFNULL([Max Early Literacy Date],[Early Language Dt])
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb2['_C11 Literacy'])
# #%%
# inspect_col(df_14t_edits1_tb2['Max Early Literacy Date'])
# #%%
# inspect_col(df_14t_edits1_tb2['Early Language Dt'])
# #%%

df_14t_edits1_tb2['_C12 ASQ 18 Mo Date'] = df_14t_edits1_tb2['ASQ18MoDate'].combine_first(df_14t_edits1_tb2['Asq3 Dt 18Mm']).astype('datetime64[ns]') 
    ### IFNULL([ASQ18MoDate],[Asq3 Dt 18Mm])
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C12 ASQ 24 Mo Date'] = df_14t_edits1_tb2['ASQ24MoDate'].combine_first(df_14t_edits1_tb2['Asq3 Dt 24Mm']).astype('datetime64[ns]') 
    ### IFNULL([ASQ24MoDate],[Asq3 Dt 24Mm])
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C12 ASQ 30 Mo Date'] = df_14t_edits1_tb2['ASQ30MoDate'].combine_first(df_14t_edits1_tb2['Asq3 Dt 30Mm']).astype('datetime64[ns]') 
    ### IFNULL([ASQ30MoDate],[Asq3 Dt 30Mm])
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C12 ASQ 9 Mo Date'] = df_14t_edits1_tb2['Asq3 Dt 9Mm'].combine_first(df_14t_edits1_tb2['ASQ9MoDate']).astype('datetime64[ns]') 
    ### IFNULL([Asq3 Dt 9Mm],[ASQ9MoDate])
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C18 ASQ 18 Mo Referral Date'] = df_14t_edits1_tb2['Asq3 Referral 18Mm'].combine_first(df_14t_edits1_tb2['ASQ18MoRefDate']).astype('datetime64[ns]') 
    ### IFNULL([Asq3 Referral 18Mm],[ASQ18MoRefDate])
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C18 ASQ 24 Mo Referral Date'] = df_14t_edits1_tb2['ASQ24MoRefDate'].combine_first(df_14t_edits1_tb2['Asq3 Referral 24Mm']).astype('datetime64[ns]') 
    ### IFNULL([ASQ24MoRefDate],[Asq3 Referral 24Mm])
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C18 ASQ 30 Mo Referral Date'] = df_14t_edits1_tb2['ASQ30MoRefDate'].combine_first(df_14t_edits1_tb2['Asq3 Referral 30Mm']).astype('datetime64[ns]') 
    ### IFNULL([ASQ30MoRefDate],[Asq3 Referral 30Mm])
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C2 BF Discontinuation Date'] = df_14t_edits1_tb2['Min Of Date Discontinue BF'].combine_first(df_14t_edits1_tb2['Lsp Bf Discon Dt']).astype('datetime64[ns]') 
    ### IFNULL([Min Of Date Discontinue BF],[Lsp Bf Discon Dt])
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C2 BF Initiation Date'] = df_14t_edits1_tb2['Min HV Date BF Yes'].combine_first(df_14t_edits1_tb2['Lsp Bf Initiation Dt']).astype('datetime64[ns]') 
    ### IFNULL([Min HV Date BF Yes],[Lsp Bf Initiation Dt])
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_Discharge Date'] = df_14t_edits1_tb2['Discharge Dt'].combine_first(df_14t_edits1_tb2['Termination Date']).astype('datetime64[ns]') 
    ### IFNULL([Discharge Dt],[Termination Date])
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_Enroll'] = df_14t_edits1_tb2['Enroll Dt'].combine_first(df_14t_edits1_tb2['Min Of HV Date']).astype('datetime64[ns]') 
    ### IFNULL([Enroll Dt],[Min Of HV Date])
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_Max HV Date'] = df_14t_edits1_tb2['Maxof HV Date'].combine_first(df_14t_edits1_tb2['Last Home Visit']).astype('datetime64[ns]') 
    ### IFNULL([Maxof HV Date],[Last Home Visit])
    ### Data Type in Tableau: date.

#%%###################################

df_14t_edits1_tb2['_C12 ASQ 18 Mo Communication'] = df_14t_edits1_tb2['Asq3 Comm 18Mm'].combine_first(df_14t_edits1_tb2['ASQ18MoCom']).astype('Int64') 
    ### IFNULL([Asq3 Comm 18Mm],[ASQ18MoCom])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 18 Mo Fine Motor'] = df_14t_edits1_tb2['ASQ18MoFine'].combine_first(df_14t_edits1_tb2['Asq3 Fine 18Mm']).astype('Int64') 
    ### IFNULL([ASQ18MoFine],[Asq3 Fine 18Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 18 Mo Gross Motor'] = df_14t_edits1_tb2['ASQ18MoGross'].combine_first(df_14t_edits1_tb2['Asq3 Gross 18Mm']).astype('Int64') 
    ### IFNULL([ASQ18MoGross],[Asq3 Gross 18Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 18 Mo Personal Social'] = df_14t_edits1_tb2['ASQ18MoPersonal'].combine_first(df_14t_edits1_tb2['Asq3 Social 18Mm']).astype('Int64') 
    ### IFNULL([ASQ18MoPersonal],[Asq3 Social 18Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 18 Mo Problem Solving'] = df_14t_edits1_tb2['ASQ18MoProblem'].combine_first(df_14t_edits1_tb2['Asq3 Problem 18Mm']).astype('Int64') 
    ### IFNULL([ASQ18MoProblem],[Asq3 Problem 18Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 24 Mo Communication'] = df_14t_edits1_tb2['Asq3 Comm 24Mm'].combine_first(df_14t_edits1_tb2['ASQ24MoCom']).astype('Int64') 
    ### IFNULL([Asq3 Comm 24Mm],[ASQ24MoCom])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 24 Mo Fine Motor'] = df_14t_edits1_tb2['ASQ24MoFine'].combine_first(df_14t_edits1_tb2['Asq3 Fine 24Mm']).astype('Int64') 
    ### IFNULL([ASQ24MoFine],[Asq3 Fine 24Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 24 Mo Gross Motor'] = df_14t_edits1_tb2['ASQ24MoGross'].combine_first(df_14t_edits1_tb2['Asq3 Gross 24Mm']).astype('Int64') 
    ### IFNULL([ASQ24MoGross],[Asq3 Gross 24Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 24 Mo Personal Social'] = df_14t_edits1_tb2['ASQ24MoPersonal'].combine_first(df_14t_edits1_tb2['Asq3 Social 24Mm']).astype('Int64') 
    ### IFNULL([ASQ24MoPersonal],[Asq3 Social 24Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 24 Mo Problem Solving'] = df_14t_edits1_tb2['ASQ24MoProblem'].combine_first(df_14t_edits1_tb2['Asq3 Problem 24Mm']).astype('Int64') 
    ### IFNULL([ASQ24MoProblem],[Asq3 Problem 24Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 30 Mo Communication'] = df_14t_edits1_tb2['ASQ30MoCom'].combine_first(df_14t_edits1_tb2['Asq3 Comm 30Mm']).astype('Int64') 
    ### IFNULL([ASQ30MoCom],[Asq3 Comm 30Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 30 Mo Fine Motor'] = df_14t_edits1_tb2['ASQ30MoFine'].combine_first(df_14t_edits1_tb2['Asq3 Fine 30Mm']).astype('Int64') 
    ### IFNULL([ASQ30MoFine],[Asq3 Fine 30Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 30 Mo Gross Motor'] = df_14t_edits1_tb2['ASQ30MoGross'].combine_first(df_14t_edits1_tb2['Asq3 Gross 30Mm']).astype('Int64') 
    ### IFNULL([ASQ30MoGross],[Asq3 Gross 30Mm])
    ### Data Type in Tableau: integer.

#%%###################################
df_14t_edits1_tb2['_C12 ASQ 30 Mo Personal Social'] = df_14t_edits1_tb2['ASQ30MoPersonal'].combine_first(df_14t_edits1_tb2['Asq3 Social 30Mm']).astype('Int64') 
    ### IFNULL([ASQ30MoPersonal],[Asq3 Social 30Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 30 Mo Problem Solving'] = df_14t_edits1_tb2['ASQ30MoProblem'].combine_first(df_14t_edits1_tb2['Asq3 Problem 30Mm']).astype('Int64') 
    ### IFNULL([ASQ30MoProblem],[Asq3 Problem 30Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 9 Mo Communication'] = df_14t_edits1_tb2['ASQ9MoCom'].combine_first(df_14t_edits1_tb2['Asq3 Comm 9Mm']).astype('Int64') 
    ### IFNULL([ASQ9MoCom],[Asq3 Comm 9Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 9 Mo Fine Motor'] = df_14t_edits1_tb2['ASQ9MoFine'].combine_first(df_14t_edits1_tb2['Asq3 Fine 9Mm']).astype('Int64') 
    ### IFNULL([ASQ9MoFine],[Asq3 Fine 9Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 9 Mo Gross Motor'] = df_14t_edits1_tb2['ASQ9MoGross'].combine_first(df_14t_edits1_tb2['Asq3 Gross 9Mm']).astype('Int64') 
    ### IFNULL([ASQ9MoGross],[Asq3 Gross 9Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 9 Mo Personal Social'] = df_14t_edits1_tb2['ASQ9MoPersonal'].combine_first(df_14t_edits1_tb2['Asq3 Social 9Mm']).astype('Int64') 
    ### IFNULL([ASQ9MoPersonal],[Asq3 Social 9Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C12 ASQ 9 Mo Problem Solving'] = df_14t_edits1_tb2['ASQ9MoProblem'].combine_first(df_14t_edits1_tb2['Asq3 Problem 9Mm']).astype('Int64') 
    ### IFNULL([ASQ9MoProblem],[Asq3 Problem 9Mm])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C13 Behavioral Concerns Asked'] = df_14t_edits1_tb2['Behavior Numer'].combine_first(df_14t_edits1_tb2['Behavioral Concerns']).astype('Int64') 
    ### IFNULL([Behavior Numer],[Behavioral Concerns])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_C13 Behavioral Concerns Visits'] = df_14t_edits1_tb2['Behavior Denom'].combine_first(df_14t_edits1_tb2['Home Visits Post']).astype('Int64') 
    ### IFNULL([Behavior Denom],[Home Visits Post])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_TGT Number'] = df_14t_edits1_tb2['Tgt Id'].combine_first(df_14t_edits1_tb2['Child Number']).astype('Int64') 
    ### IFNULL([Tgt Id],[Child Number])
    ### Data Type in Tableau: integer.

df_14t_edits1_tb2['_Zip'] = df_14t_edits1_tb2['zip'].combine_first(df_14t_edits1_tb2['ZIP Code']).astype('Int64') 
    ### IFNULL([zip],[ZIP Code])
    ### Data Type in Tableau: integer.

#%%###################################

### In Y12Q3 this variable is broken. 
### df_14t_edits1_tb2['_T16 Total Home Visits'] = df_14t_edits1_tb2['Home Visits Total'].combine_first(df_14t_edits1_tb2['Home Visits Num'])
    ### IFNULL([Home Visits Total],[Home Visits Num])
    ### Data Type in Tableau: integer.
### RESOLVED: Why did 'Home Visits Total' disappear? Is '_T16' not needed in Form 1? 
### Answer: HV should be calculated from Adult data, so removed. / change to TRUE in Tableau.
df_14t_edits1_tb2['_T16 Total Home Visits'] = True 

#%%###################################
### If variables are already dtypes "datetime64", then this should be a date too:
df_14t_edits1_tb2['_T20 TGT Insurance Date'] = df_14t_edits1_tb2['TGT Insure Change Date'].combine_first(df_14t_edits1_tb2['Hlth Insure Tgt Dt']).astype('datetime64[ns]') 
    ### DATE(IFNULL([TGT Insure Change Date],[Hlth Insure Tgt Dt]))
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb2['_T20 TGT Insurance Date'])

#%%##################################################
### DATE CALCULATIONS

### These calculations assume all date variables are dtype "datetime64".

df_14t_edits1_tb2['_C18 ASQ 18 Mo 30 Day Date'] = (df_14t_edits1_tb2['_C12 ASQ 18 Mo Date'] + pd.DateOffset(days=30)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',30,[_C12 ASQ 18 Mo Date])) 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb2['_C18 ASQ 18 Mo 30 Day Date'])
# #%%
# inspect_col(df_14t_edits1_tb2['_C12 ASQ 18 Mo Date'])
# #%%

df_14t_edits1_tb2['_C18 ASQ 18 Mo 45 Day Date'] = (df_14t_edits1_tb2['_C12 ASQ 18 Mo Date'] + pd.DateOffset(days=45)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',45,[_C12 ASQ 18 Mo Date])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C18 ASQ 24 Mo 30 Day Date'] = (df_14t_edits1_tb2['_C12 ASQ 24 Mo Date'] + pd.DateOffset(days=30)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',30,[_C12 ASQ 24 Mo Date])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C18 ASQ 24 Mo 45 Day Date'] = (df_14t_edits1_tb2['_C12 ASQ 24 Mo Date'] + pd.DateOffset(days=45)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',45,[_C12 ASQ 24 Mo Date])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C18 ASQ 30 Mo 30 Day Date'] = (df_14t_edits1_tb2['_C12 ASQ 30 Mo Date'] + pd.DateOffset(days=30)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',30,[_C12 ASQ 30 Mo Date])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C18 ASQ 30 Mo 45 Day Date'] = (df_14t_edits1_tb2['_C12 ASQ 30 Mo Date'] + pd.DateOffset(days=45)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',45,[_C12 ASQ 30 Mo Date])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C18 ASQ 9 Mo 30 Day Date'] = (df_14t_edits1_tb2['_C12 ASQ 9 Mo Date'] + pd.DateOffset(days=30)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',30,[_C12 ASQ 9 Mo Date])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_C18 ASQ 9 Mo 45 Day Date'] = (df_14t_edits1_tb2['_C12 ASQ 9 Mo Date'] + pd.DateOffset(days=45)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',45,[_C12 ASQ 9 Mo Date])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_Enroll 3 Month Date'] = (df_14t_edits1_tb2['_Enroll'] + pd.DateOffset(months=3)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',3,[_Enroll])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_Enroll 6 Month Date'] = (df_14t_edits1_tb2['_Enroll'] + pd.DateOffset(months=6)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',6,[_Enroll])) 
    ### Data Type in Tableau: date.

#%%##################################################
### IF/ELSE, CASE/WHEN

### fdf == "function DataFrame"

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
    ### IF [Dt Edc] = DATE(1/1/1900) THEN NULL //LLCHD
    ### ELSEIF [EDC Date] = DATE(1/1/1900) THEN NULL //FW
    ### ELSE IFNULL([Dt Edc],[EDC Date])
    ### END
df_14t_edits1_tb2['_TGT EDC Date'] = df_14t_edits1_tb2.apply(func=fn_TGT_EDC_Date, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb2['_TGT EDC Date'])
# #%%
# inspect_col(df_14t_edits1_tb2['Dt Edc'])
# #%%
# inspect_col(df_14t_edits1_tb2['EDC Date'])
# #%%
# print(df_14t_edits1_tb2[['_TGT EDC Date', 'Dt Edc', 'EDC Date']].query('`Dt Edc` == "1900-01-01" or `EDC Date` == "1900-01-01"').to_string())

#%%###################################

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
df_14t_edits1_tb2['_TGT DOB'] = df_14t_edits1_tb2.apply(func=fn_TGT_DOB, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb2['_TGT DOB'])
# #%%
# inspect_col(df_14t_edits1_tb2['Tgt Dob'])
# #%%
# inspect_col(df_14t_edits1_tb2['Tgt Dob-Cr'])
# #%%
# print(df_14t_edits1_tb2[['_TGT DOB', 'Tgt Dob', 'Tgt Dob-Cr']].query('`Tgt Dob` == "1900-01-01" or `Tgt Dob-Cr` == "1900-01-01"').to_string())

#%%###################################

### TODO: Rename to "No".
### In Form2, var used in [Include2]. "Partial Date" means date checked for Safe Sleep but answer was "No" -- so go into Denominator & not Missing.
### Rename to ['_C7 Safe Sleep No Date'].
########
### OLD: ### df_14t_edits1_tb2['_C7 Safe Sleep Partial Date'] = df_14t_edits1_tb2['Safe Sleep Partial Date']
########
### 2023-12-13: Rewriting:
def fn_C7_Safe_Sleep_Partial_Date(fdf):
    ### LL:
    if (fdf['_Agency'] == 'll'):
        if pd.isna(fdf['Safe Sleep Yes']) or (fdf['Safe Sleep Yes'] != 'Y'):
            return fdf['Safe Sleep Yes Dt']
        else:
            return pd.NaT
    ### FW:
    else:
        return fdf['Safe Sleep Partial Date']
    #######
    ### // IFNULL(
    ### [Safe Sleep Partial Date]  // FW
    ### // ,[Safe Sleep Yes Dt]) // LLCHD needs to provide a safe sleep partial date
    ### // END
df_14t_edits1_tb2['_C7 Safe Sleep Partial Date'] = df_14t_edits1_tb2.apply(func=fn_C7_Safe_Sleep_Partial_Date, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb2['_C7 Safe Sleep Partial Date'])

#%%###################################

### Identical to ['_C7 Safe Sleep Yes Date'].
########
### OLD: ### df_14t_edits1_tb2['_C7 Safe Sleep Date'] = df_14t_edits1_tb2['Safe Sleep Date'].combine_first(df_14t_edits1_tb2['Safe Sleep Yes Dt'])
########
### 2023-12-13: Rewriting:
def fn_C7_Safe_Sleep_Date(fdf):
    ### LL:
    if (fdf['_Agency'] == 'll'):
        if (fdf['Safe Sleep Yes'] == 'Y'):
            return fdf['Safe Sleep Yes Dt']
        else:
            return pd.NaT
    ### FW:
    else:
        return fdf['Safe Sleep Date']
    #######
    ### IFNULL([Safe Sleep Date],[Safe Sleep Yes Dt])
df_14t_edits1_tb2['_C7 Safe Sleep Date'] = df_14t_edits1_tb2.apply(func=fn_C7_Safe_Sleep_Date, axis=1).astype('datetime64[ns]') 
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb2['_C7 Safe Sleep Date'])
# #%%
# inspect_col(df_14t_edits1_tb2['_Agency'])
# #%%
# inspect_col(df_14t_edits1_tb2['Safe Sleep Yes']) ### LL.
# #%%
# inspect_col(df_14t_edits1_tb2['Safe Sleep Yes Dt']) ### LL.
# #%%
# inspect_col(df_14t_edits1_tb2['Safe Sleep Date']) ### FW.
# #%%
# inspect_col(df_14t_edits1_tb2['Safe Sleep Partial Date']) ### FW.
#%%
### Check LL:
print(df_14t_edits1_tb2[['_Agency', 'Safe Sleep Yes', 'Safe Sleep Yes Dt', '_C7 Safe Sleep Partial Date', '_C7 Safe Sleep Date']].query('`_Agency` == "ll"').drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
#%%
### Check FW:
# print(df_14t_edits1_tb2[['_Agency', 'Safe Sleep Date', 'Safe Sleep Partial Date', '_C7 Safe Sleep Partial Date', '_C7 Safe Sleep Date']].query('`_Agency` != "ll"').drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
print(df_14t_edits1_tb2[['_Agency', 'Safe Sleep Date', 'Safe Sleep Partial Date']].query('`_Agency` != "ll"').drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
#%%
# print(df_14t_edits1_tb2[['_Agency', 'Safe Sleep Yes', 'Safe Sleep Yes Dt', 'Safe Sleep Date', 'Safe Sleep Partial Date', '_C7 Safe Sleep Partial Date', '_C7 Safe Sleep Date']]
print(df_14t_edits1_tb2[['_Agency', 'Safe Sleep Yes', 'Safe Sleep Yes Dt', 'Safe Sleep Date', 'Safe Sleep Partial Date']]
    .assign(**{
        # 'Safe Sleep Yes': lambda df: pd.isna(df['Safe Sleep Yes'])
        # ,'Safe Sleep Yes Dt': lambda df: pd.isna(df['Safe Sleep Yes Dt'])
        # ,'Safe Sleep Date': lambda df: pd.isna(df['Safe Sleep Date'])
        # ,'Safe Sleep Partial Date': lambda df: pd.isna(df['Safe Sleep Partial Date'])
        # ,'_C7 Safe Sleep Partial Date': lambda df: pd.isna(df['_C7 Safe Sleep Partial Date'])
        # ,'_C7 Safe Sleep Date': lambda df: pd.isna(df['_C7 Safe Sleep Date'])
        ###
        '_Agency': lambda df: df.apply(func=fn_if_else, axis=1, args=((lambda df: df['_Agency'] == 'll'), 'll', 'fw'))
        ,'Safe Sleep Yes': lambda df: df.apply(func=fn_if_else, axis=1, args=((lambda df: pd.notna(df['Safe Sleep Yes'])), 'Y', '.'))
        ,'Safe Sleep Yes Dt': lambda df: df.apply(func=fn_if_else, axis=1, args=((lambda df: pd.notna(df['Safe Sleep Yes Dt'])), 'date', '.'))
        ,'Safe Sleep Date': lambda df: df.apply(func=fn_if_else, axis=1, args=((lambda df: pd.notna(df['Safe Sleep Date'])), 'date', '.'))
        ,'Safe Sleep Partial Date': lambda df: df.apply(func=fn_if_else, axis=1, args=((lambda df: pd.notna(df['Safe Sleep Partial Date'])), 'date', '.'))
        # ,'_C7 Safe Sleep Partial Date': lambda df: df.apply(func=fn_if_else, axis=1, args=((lambda df: pd.notna(df['_C7 Safe Sleep Partial Date'])), 'date', '.'))
        # ,'_C7 Safe Sleep Date': lambda df: df.apply(func=fn_if_else, axis=1, args=((lambda df: pd.notna(df['_C7 Safe Sleep Date'])), 'date', '.'))
    })
    .drop_duplicates(ignore_index=True)
    .pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string()
)
### RESOLVED: Discuss what logic is needed. NOTE: "partial date" is BOTH yes & no, it seems.
#### Decision: 12/21/23: 
    ### LL: yes: = Y+date, no= NULL+date, missingg = both NULL
#### FW: yes = both, no = one.
### TODO: fix.
### Check if FW dates are the same & if want earliest or latest.


#%%###################################

### Identical to ['_C7 Safe Sleep Date'].
### In Form2, was used for OLD style, but no longer used.
### updated Y12Q3-Q4:
df_14t_edits1_tb2['_C7 Safe Sleep Yes Date'] = df_14t_edits1_tb2['Safe Sleep Date'].combine_first(df_14t_edits1_tb2['Safe Sleep Yes Dt']).astype('datetime64[ns]') 
    ### IFNULL([Safe Sleep Date],[Safe Sleep Yes Dt])
########
### OLD:
### def fn_C7_Safe_Sleep_Yes_Date(fdf):
#     ### if ( 
#     ###     fdf['Sleep On Back'] == "Yes" ### FW.
#     ###     and fdf['Co Sleeping'] == "No"
#     ###     and fdf['Soft Bedding'] == "No"
#     ### ):
#     ###     return fdf['Safe Sleep Date']
#     ### else:
#     ###     return fdf['Safe Sleep Yes Dt'] ### LLCHD.
#     ### ### IF [Sleep On Back] = "Yes" //FW
#     ### ### AND [Co Sleeping] = "No"
#     ### ### AND [Soft Bedding] = "No"
#     ### ### THEN [Safe Sleep Date]
#     ### ### ELSE [Safe Sleep Yes Dt] //LLCHD
#     ### ### END
### df_14t_edits1_tb2['_C7 Safe Sleep Yes Date'] = df_14t_edits1_tb2.apply(func=fn_C7_Safe_Sleep_Yes_Date, axis=1)
########
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb2['_C7 Safe Sleep Yes Date'])
# #%%
# inspect_col(df_14t_edits1_tb2['Sleep On Back']) ### OLD: ### Supposed to be string in Tableau. BUT Empty, so read in as float np.nan.
# #%%
# inspect_col(df_14t_edits1_tb2['Co Sleeping']) ### OLD: ### Supposed to be string in Tableau. BUT Empty, so read in as float np.nan.
# #%%
# inspect_col(df_14t_edits1_tb2['Soft Bedding']) ### OLD: ### Supposed to be string in Tableau. BUT Empty, so read in as float np.nan.
# #%%
# inspect_col(df_14t_edits1_tb2['Safe Sleep Date'])
# #%%
# inspect_col(df_14t_edits1_tb2['Safe Sleep Yes Dt'])
### DONE: Investigate why all 3 FW safe sleep variable all empty. ### Y12Q3: Investigate why all 3 disappeared?
### DONE: Fix var: Only use date now or what? ### Answer: yes, see new "IFNULL" logic.
### Answer 2023-10-24: Should be comparing dates now instead of T/F. See updated Tableau variable.

#%%###################################

### RESOLVED: Ask Joe why ALL values are the same. Fixed: no longer.
### RESOLVED: need to check values for FW reasons. Fixed: now accepting strings.
def fn_Discharge_Reason(fdf):
    ### LLCHD, see full reasons below.
    if (fdf['Discharge Dt'] is not pd.NaT):
        match fdf['Discharge Reason']: 
            ### case 1: ### OLD. Fixed from expecting a number to expecting a string. Note: No examples of "1" or 1.
            ### This change will make the output different from Y12Q1 for this var.
            case '1' | 'Family Has Met Program Goals':
                return "Completed Services" 
            case _:
                return "Stopped Services Before Completion"
    ### FW.
    ### need to check values for FW reasons
    elif (fdf['Termination Date'] is not pd.NaT):
        match fdf['Termination Status']: 
            case "Family graduated/met all program goals":
                return "Completed Services"
            case _:
                return "Stopped Services Before Completion"
    else:
        return "Currently Receiving Services"
    ### IF NOT ISNULL([Discharge Dt]) THEN CASE [Discharge Reason] //LLCHD, see full reasons below
    ###     WHEN 1 THEN "Completed Services" 
    ###     ELSE "Stopped Services Before Completion"
    ###     END
    ### ELSEIF NOT ISNULL([Termination Date]) THEN CASE [Termination Status] //FW
    ###     WHEN "Family graduated/met all program goals" THEN "Completed Services"
    ###     ELSE "Stopped Services Before Completion"
    ###     //need to check values for FW reasons
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
df_14t_edits1_tb2['_Discharge Reason'] = df_14t_edits1_tb2.apply(func=fn_Discharge_Reason, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_Discharge Reason'])
# #%%
# inspect_col(df_14t_edits1_tb2['Discharge Dt']) 
# #%%
# inspect_col(df_14t_edits1_tb2['Discharge Reason']) ### Is a string, but no examples of "1" or 1. NOTE: Notably, most are "Other".
# #%%
# inspect_col(df_14t_edits1_tb2['Termination Date']) 
# #%%
# inspect_col(df_14t_edits1_tb2['Termination Status'])
#%%
# print(df_14t_edits1_tb2[['_Discharge Reason', 'Discharge Dt', 'Discharge Reason', 'Termination Date', 'Termination Status']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
# print(df_14t_edits1_tb2[['_Discharge Reason', 'Discharge Reason', 'Termination Status']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
#%%
### COMPARISON (Note: "df_14t_comp_compare_tb2" created at end):
# print(df_14t_comp_compare_tb2[['_Discharge Reason', 'Discharge Reason']].to_string()) ### 'Termination Status' & 'Discharge Dt' & 'Termination Date' had no differences.
# print(df_14t_comp_compare_tb2[['Discharge Reason']].to_string())
### Where are these extra values coming from?? ### Fixed code in Tableau was wrong (wasn't expecting stings). So now won't match until Tableau CSV created again.


#%%###################################

def fn_C2_BF_Status(fdf):
    ### FW.
    if (fdf['_Agency'] != "ll"):
        match fdf['Breast Feeding']:  
            case "YES":
                return 1
            case "1":
                return 1
            case "0":
                return 0
            case "-1":
                return -1
            case _:
                return np.nan  
    ### add CASE for LLCHD values when they add them to their dataset.
    elif (fdf['_Agency'] == "ll"):
        return np.nan  
    ### IF [_Agency] <> "ll" THEN CASE [Breast Feeding]  // FW
    ###     WHEN "YES" THEN 1
    ###     WHEN "1" THEN 1
    ###     WHEN "0" THEN 0
    ###     WHEN "-1" THEN -1
    ###     ELSE NULL   
    ###     END
    ### ELSEIF [_Agency] = "ll" THEN NULL  // add CASE for LLCHD values when they add them to their dataset
    ### END
df_14t_edits1_tb2['_C2 BF Status'] = df_14t_edits1_tb2.apply(func=fn_C2_BF_Status, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb2['_C2 BF Status'])
# #%%
# inspect_col(df_14t_edits1_tb2['_Agency'])
# #%%
# inspect_col(df_14t_edits1_tb2['Breast Feeding']) ### Originally, csv read in as int, then NA's converted to a float. Should be a string. Fixed in Read above.
# #%%
# # pd.crosstab(df_14t_edits1_tb2['_Agency'], df_14t_edits1_tb2['_C2 BF Status'], dropna=False)
# pd.crosstab(df_14t_edits1_tb2['_C2 BF Status'], df_14t_edits1_tb2['_Agency'], dropna=False, margins=True)

#%%###################################

def fn_FW_Gestation_Age_Recode(fdf):
    match fdf['Gestational Age']: 
        case '29 weeks':
            return 29
        case '31 weeks':
            return 31
        case '33 weeks':
            return 33
        case '34 weeks':
            return 34
        case '35 weeks':
            return 35
        case '36 weeks':
            return 36
        case '37 weeks':
            return 37
        case '38 weeks':
            return 38
        case '39 weeks':
            return 39
        case '40 weeks':
            return 40
        case '41 weeks':
            return 41
        case '42 weeks':
            return 42
        case 'Unknown':
            return np.nan
        # case np.nan: ### no error, but doesn't work.
        # case pd.isna(): ### no error, but doesn't work.
        # case pd.isna(): ### TypeError: called match pattern must be a type.
        # case '':
        # case pd.NA: ### TypeError: boolean value of NA is ambiguous.
        # case _ if pd.isna(_): ###ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all(). ### Note: This error only notice when running first time, not after var already created.
        #     return 100
        case _:
            return np.nan
    ### CASE [Gestational Age]
    ###     WHEN '29 weeks' THEN 29
    ###     WHEN '31 weeks' THEN 31
    ###     WHEN '33 weeks' THEN 33
    ###     WHEN '34 weeks' THEN 34
    ###     WHEN '35 weeks' THEN 35
    ###     WHEN '36 weeks' THEN 36
    ###     WHEN '37 weeks' THEN 37
    ###     WHEN '38 weeks' THEN 38
    ###     WHEN '39 weeks' THEN 39
    ###     WHEN '40 weeks' THEN 40
    ###     WHEN '41 weeks' THEN 41
    ###     WHEN '42 weeks' THEN 42
    ###     WHEN 'Unknown' THEN NULL
    ### ELSE NULL
    ### END
df_14t_edits1_tb2['_FW Gestation Age Recode'] = df_14t_edits1_tb2.apply(func=fn_FW_Gestation_Age_Recode, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb2['_FW Gestation Age Recode'])
# #%%
# inspect_col(df_14t_edits1_tb2['Gestational Age']) ### From FW.
### DONE: Check with Joe -- but need to add in values for "22/24/27/28/30/32 weeks".
    ### OR change to check if follows pattern, then pull first 2 chars, & then turn to int.
    ### TODO: Fix with pattern.
# #%%
# inspect_col(df_14t_edits1_tb2['Gestational Age'])

#%%###################################

### TODO: Move downstream "Funding" variables ([_Funding (use this one)]) out of Form2 into this code.
### This variable actually NOT used in Form2. ### TODO: Check Form1.
def fn_Funding(fdf):
    if (fdf['_Agency'] != "ll"):
        match fdf['Agency']:
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
            case _:
                return "Unrecognized Value"
    elif (fdf['_Agency'] == "ll"):
        return fdf['Funding']
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
df_14t_edits1_tb2['_Funding'] = df_14t_edits1_tb2.apply(func=fn_Funding, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_Funding'])
#%%
# print(df_14t_edits1_tb2[['_Funding', '_Agency', 'Funding']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
# print(df_14t_edits1_tb2[['_Agency', '_Funding', 'Funding']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
#%% 
### Rows with "Unrecognized Value" (See "list_14t_unrecognized_values_tb2".):
# df_14t_edits1_tb2[['Project Id','Year','Quarter', '_Funding', '_Agency', 'Funding']].query(f'`_Funding` == "Unrecognized Value"')
### DONE: How to code "wc"? ### "wc" is probably old & no longer used. ### See comments above.

#%%###################################

def fn_Need_Exclusion_4_Dev_Delay(fdf):
    ### FW.
    if (fdf['Need Exclusion4'] == "Developmental Delay"):
        return "Developmental Delay" 
    ### LLCHD.
    elif (fdf['need exclusion4 (LLCHD)'] == "Y"):
        return "Developmental Delay" 
    ### IF [Need Exclusion4] = "Developmental Delay" THEN "Developmental Delay" //FW
    ### ELSEIF [need exclusion4 (LLCHD)] = "Y" THEN "Developmental Delay" //LLCHD
    ### END
df_14t_edits1_tb2['_Need Exclusion 4 - Dev Delay'] = df_14t_edits1_tb2.apply(func=fn_Need_Exclusion_4_Dev_Delay, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_Need Exclusion 4 - Dev Delay'])

#%%###################################

### DONE: Add option for "null" string currently marked as "Unrecognized".
### NOTE: Tableau doesn't seem to care about Case Sensitivity for this logic.
    ### In Tableau "Hispanic" data for [Tgt Ethnicity1] is caught by "HISPANIC", but Python doesn't because it's case sensitive.
    ### Adjusting Python to be case insensitive.
def fn_T06_TGT_Ethnicity(fdf):
    ### FW.
    if (pd.notna(fdf['Tgt Ethnicity'])):
        match fdf['Tgt Ethnicity'].lower():
            case "non hispanic/latino":
                return "Not Hispanic or Latino"
            case "hispanic/latino":
                return "Hispanic or Latino"
            case "unknown" | "null": ### Y12Q4 "null" option added.
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ### LLCDH.
    elif (pd.notna(fdf['Tgt Ethnicity1'])):
        match fdf['Tgt Ethnicity1'].lower():
            case "hispanic/latino" | "hispanic":
                return "Hispanic or Latino"
            case "not hispanic/latino" | "non-hispanic":
                return "Not Hispanic or Latino"
            case "unreported/refused to report":
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    else:
        return "Unknown/Did Not Report"
    ### //FW
    ### IF NOT ISNULL([Tgt Ethnicity]) THEN CASE [Tgt Ethnicity]
    ###     WHEN "Non Hispanic/Latino" THEN "Not Hispanic or Latino"
    ###     WHEN "Hispanic/Latino" THEN "Hispanic or Latino"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### //LLCDH
    ### ELSEIF NOT ISNULL([Tgt Ethnicity1]) THEN CASE [Tgt Ethnicity1] 
    ###     WHEN "HISPANIC/LATINO" THEN "Hispanic or Latino" 
    ###     WHEN "HISPANIC" THEN "Hispanic or Latino"
    ###     WHEN "NOT HISPANIC/LATINO" THEN "Not Hispanic or Latino"
    ###     WHEN "NON-Hispanic" THEN "Not Hispanic or Latino"
    ###     WHEN "UNREPORTED/REFUSED TO REPORT" THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE "Unknown/Did Not Report"
    ### END
df_14t_edits1_tb2['_T06 TGT Ethnicity'] = df_14t_edits1_tb2.apply(func=fn_T06_TGT_Ethnicity, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_T06 TGT Ethnicity'])
# #%%
# inspect_col(df_14t_edits1_tb2['Tgt Ethnicity']) ### FW.
# ### DONE: need to bring in 3 rows that have text "null" in them. Python reads those in as NaN. Need to check how written out.
# #%%
# inspect_col(df_14t_edits1_tb2['Tgt Ethnicity1'])
# #%%
# print(df_14t_edits1_tb2[['_T06 TGT Ethnicity', 'Tgt Ethnicity', 'Tgt Ethnicity1']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
# #%% 
# ### Rows with "Unrecognized Value" (See "list_14t_unrecognized_values_tb2".):
# df_14t_edits1_tb2[['Project Id','Year','Quarter', '_T06 TGT Ethnicity', 'Tgt Ethnicity', 'Tgt Ethnicity1']].query(f'`_T06 TGT Ethnicity` == "Unrecognized Value"')
### DONE: resolved Y12Q4.

#%%###################################

def fn_T1_Tgt_Gender(fdf):
    ### FW.
    if (pd.notna(fdf['TGT Gender'])):
        match fdf['TGT Gender'].lower():
            case "female":
                return "Female"
            case "male":
                return "Male"
            case "non-binary":
                return "Non-Binary"
            case "unknown" | "null" | "unknown/did not" | "unknown/did not repo": ### FY12Q4 added new values "Unknown/Did Not" & "Unknown/Did Not Repo".
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ### LLCHD.
    elif (pd.notna(fdf['Tgt Gender'])):
        match fdf['Tgt Gender']:
            case "F":
                return "Female"
            case "M":
                return "Male"
            ### case "N": return "Non-Binary" ### Don't have this value yet - confirm what it means if seen.
            case "Unknown":
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    else:
        return "Unknown/Did Not Report"
    ### //FW
    ### IF NOT ISNULL([TGT Gender]) THEN CASE [TGT Gender]
    ###     WHEN "Female" THEN "Female"
    ###     WHEN "Male" THEN "Male"
    ###     WHEN "Non-Binary" THEN "Non-Binary"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "Null" THEN "Unknown/Did Not Report"
    ###     WHEN "null" THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### //LLCHD
    ### ELSEIF NOT ISNULL([Tgt Gender]) THEN CASE [Tgt Gender]
    ###     WHEN "F" THEN "Female"
    ###     WHEN "M" THEN "Male"
    ###     // WHEN "N" THEN "Non-Binary" // Don't have this value yet - confirm
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE "Unknown/Did Not Report"
    ### END
df_14t_edits1_tb2['_T1 Tgt Gender'] = df_14t_edits1_tb2.apply(func=fn_T1_Tgt_Gender, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_T1 Tgt Gender'])
# #%%
# inspect_col(df_14t_edits1_tb2['TGT Gender'])
# #%%
# inspect_col(df_14t_edits1_tb2['Tgt Gender'])
# #%%
# ### Crosstabs not giving expected results.
# # pd.crosstab(df_14t_edits1_tb2['TGT Gender'], df_14t_edits1_tb2['Tgt Gender'], dropna=False)
# # pd.crosstab(df_14t_edits1_tb2['_T1 Tgt Gender'], df_14t_edits1_tb2['Tgt Gender'], dropna=False, margins=True)
# #%%
# print(df_14t_edits1_tb2[['_T1 Tgt Gender', 'TGT Gender', 'Tgt Gender']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
# #%% 
### Rows with "Unrecognized Value" (See "list_14t_unrecognized_values_tb2".):
# df_14t_edits1_tb2[['Project Id','Year','Quarter', '_T1 Tgt Gender', 'TGT Gender', 'Tgt Gender']].query(f'`_T1 Tgt Gender` == "Unrecognized Value"')
### DONE: resolved Y12Q4.

#%%###################################

### Another example of Tableau not being case sensitive. Fixing.
def fn_T13_TGT_Language(fdf):
    if (pd.notna(fdf['Mob Language'])):
        match fdf['Mob Language'].lower():
            case "english":
                return "English"
            case "spanish":
                return "Spanish"
            case _:
                return "Other"
    elif (pd.notna(fdf['Language Primary'])):
        match fdf['Language Primary'].lower():
            case "english":
                return "English"
            case "spanish":
                return "Spanish"
            case _:
                return "Other"
    else:
        return "Unknown/Did Not Report"
    ### IF NOT ISNULL([Mob Language]) THEN CASE [Mob Language]
    ###     WHEN "English" THEN "English"
    ###     WHEN "Spanish" THEN "Spanish"
    ###     ELSE "Other"
    ###     END
    ### ELSEIF NOT ISNULL([Language Primary]) THEN CASE [Language Primary]
    ###     WHEN "ENGLISH" THEN "English"
    ###     WHEN "SPANISH" THEN "Spanish"
    ###     ELSE "Other"
    ###     END
    ### ELSE "Unknown/Did Not Report"
    ### END
df_14t_edits1_tb2['_T13 TGT Language'] = df_14t_edits1_tb2.apply(func=fn_T13_TGT_Language, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_T13 TGT Language'])
# #%%
# df_14t_comp_compare_tb2_lang = (
#     df_14t_comparison_csv_tb2[['_T13 TGT Language', 'Mob Language', 'Language Primary']]
#     .compare(df_14t__final_from_csv_tb2[['_T13 TGT Language', 'Mob Language', 'Language Primary']], keep_equal=True, keep_shape=True)
#     .loc[(lambda df: df[('_T13 TGT Language', 'self')] != df[('_T13 TGT Language', 'other')]), :]
# )
# print(df_14t_comp_compare_tb2_lang.to_string())
# #%%
# ### What is "Other":
# print(df_14t_edits1_tb2[['_T13 TGT Language', 'Mob Language', 'Language Primary']].loc[df_14t_edits1_tb2['_T13 TGT Language'] == 'Other', :].to_string())


#%%###################################

def fn_T15_7_Household_Developmental_Delay(fdf):
    ### To determine priority population, positive ASQ results also need to be considered.
    ### FW.
    if (fdf['NT Child Dev Delay'] == "Yes"):
        return 1 
    elif (fdf['NT Child Dev Delay'] == "No"):
        return 0
    ### LLCHD.
    elif (fdf['Priority Develop Delays'] == "Y"):
        return 1 
    elif (fdf['Priority Develop Delays'] == "N"):
        return 0
    ### IF [NT Child Dev Delay] = "Yes" THEN 1 //FW
    ### ELSEIF [NT Child Dev Delay] = "No" THEN 0
    ### ELSEIF [Priority Develop Delays] = "Y" THEN 1 //LLCHD
    ### ELSEIF [Priority Develop Delays] = "N" THEN 0
    ### END
    ### //To determine priority population, positive ASQ results also need to be considered
df_14t_edits1_tb2['_T15-7 Household Developmental Delay'] = df_14t_edits1_tb2.apply(func=fn_T15_7_Household_Developmental_Delay, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb2['_T15-7 Household Developmental Delay'])

#%%###################################

def fn_T20_TGT_Insurance_Status(fdf):
    ### FW.
    if (pd.notna(fdf['CHINS Primary Ins'])):
        match fdf['CHINS Primary Ins']:
            case "Medicaid":
                return "Medicaid or CHIP"
            case "Medicare/Medicaid":
                return "Medicare/Medicaid" ### TODO ASKJOE: Need to ask FW how to code this. Added Y13Q1.
            case "None":
                return "No Insurance Coverage"
            case "Medicare" | "Other" | "Private" | "Blue Cross Blue Shield" | "Aetna":
                return "Private or Other"
            case "Tri-Care":
                return "Tri-Care"
            case "Unknown" | "null":
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ### LLCHD.
    elif (pd.notna(fdf['Hlth Insure Tgt'])):
        match fdf['Hlth Insure Tgt']:
            case 1:
                return "Medicaid or CHIP" ### 1=Medicaid.
            case 2:
                return "Tri-Care" ### 2=Tricare.
            case 3:
                return "Private or Other" ### 3=Private/Other.
            case 4:
                return "FamilyChildHealthPlus" ### 4=Unknown/Did Not Report.
            case 5 | 0:
                return "No Insurance Coverage" ### 5=None.
            case 6 | 99:
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    else:
        return "Unknown/Did Not Report"
    ### IF NOT ISNULL([CHINS Primary Ins]) THEN CASE [CHINS Primary Ins] //FW
    ###     WHEN "Medicaid" THEN "Medicaid or CHIP"
    ###     WHEN "Medicare" THEN "Private or Other"
    ###     WHEN "None" THEN "No Insurance Coverage"
    ###     WHEN "Other" THEN "Private or Other"
    ###     WHEN "Private" THEN "Private or Other"
    ###     WHEN "Tri-Care" THEN "Tri-Care"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     WHEN "null" THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSEIF NOT ISNULL([Hlth Insure Tgt]) THEN CASE [Hlth Insure Tgt] //LLCHD
    ###     WHEN 0 THEN "No Insurance Coverage"
    ###     WHEN 1 THEN "Medicaid or CHIP" //1=Medicaid
    ###     WHEN 2 THEN "Tri-Care" //2=Tricare
    ###     WHEN 3 THEN "Private or Other" //3=Private/Other
    ###     WHEN 4 THEN "FamilyChildHealthPlus" //4=Unknown/Did Not Report
    ###     WHEN 5 THEN "No Insurance Coverage" //5=None
    ###     WHEN 6 THEN "Unknown/Did Not Report"
    ###     WHEN 99 THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE "Unknown/Did Not Report"
    ### END
df_14t_edits1_tb2['_T20 TGT Insurance Status'] = df_14t_edits1_tb2.apply(func=fn_T20_TGT_Insurance_Status, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_T20 TGT Insurance Status'])
# #%%
# inspect_col(df_14t_edits1_tb2['CHINS Primary Ins'])
# #%%
# inspect_col(df_14t_edits1_tb2['Hlth Insure Tgt'])
# #%%
# print(df_14t_edits1_tb2[['_T20 TGT Insurance Status', 'CHINS Primary Ins', 'Hlth Insure Tgt']].drop_duplicates(ignore_index=True).pipe(lambda df: df.sort_values(by=list(df.columns), ignore_index=True)).to_string())
# #%% 
# ### Rows with "Unrecognized Value" (See "list_14t_unrecognized_values_tb2".):
# df_14t_edits1_tb2[['Project Id','Year','Quarter', '_T20 TGT Insurance Status', 'CHINS Primary Ins', 'Hlth Insure Tgt']].query(f'`_T20 TGT Insurance Status` == "Unrecognized Value"')
### DONE: How to code new Y12Q4 values?: "Aetna", "Blue Cross Blue Shield", "Medicare/Medicaid". Added above Y13Q1.

#%%###################################

def fn_T21_TGT_Usual_Source_of_Medical_Care(fdf):
    ### FW.
    if (pd.notna(fdf['Child Med Care Source'])):
        match fdf['Child Med Care Source']:
            case "Doctor/Nurse Practitioner":
                return "Doctor's/Nurse Practitioner's Office"
            case "Federally Qualified Health Center":
                return "Federally Qualified Health Center"
            case "Hospital ER":
                return "Hospital Emergency Room"
            case "Hospital Outpatient":
                return "Hospital Outpatient"
            case "None":
                return "None"
            case "Other":
                return "Other"
            case "Retail or Minute Clinic":
                return "Retail Store or Minute Clinic"
            case "Prenatal Client":
                return "Prenatal Client"
            case _:
                return "Unrecognized Value"
    ### LLCHD, coded values are = to form 1 categories.
    elif (pd.notna(fdf['Tgt Medical Home'])):
        match fdf['Tgt Medical Home']:
            case 0:
                return "None"
            case 1:
                return "Doctor's/Nurse Practitioner's Office"
            case 2:
                return "Hospital Emergency Room"
            case 3:
                return "Hospital Outpatient"
            case 4:
                return "Federally Qualified Health Center"
            case 5:
                return "Retail Store or Minute Clinic"
            case 6:
                return "Other"
            case 7:
                return "None"
            case 8:
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    else:
        return "Unknown/Did Not Report"
    ### IF NOT ISNULL([Child Med Care Source]) THEN CASE [Child Med Care Source] //FW
    ###     WHEN "Doctor/Nurse Practitioner" THEN "Doctor's/Nurse Practitioner's Office"
    ###     WHEN "Federally Qualified Health Center" THEN "Federally Qualified Health Center"
    ###     WHEN "Hospital ER" THEN "Hospital Emergency Room"
    ###     WHEN "Hospital Outpatient" THEN "Hospital Outpatient"
    ###     WHEN "None" THEN "None"
    ###     WHEN "Other" THEN "Other"
    ###     WHEN "Retail or Minute Clinic" THEN "Retail Store or Minute Clinic"
    ###     WHEN "Prenatal Client" THEN "Prenatal Client"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSEIF NOT ISNULL([Tgt Medical Home]) THEN CASE [Tgt Medical Home] //LLCHD, coded values are = to form 1 categories
    ###     WHEN 0 THEN "None"
    ###     WHEN 1 THEN "Doctor's/Nurse Practitioner's Office"
    ###     WHEN 2 THEN "Hospital Emergency Room"
    ###     WHEN 3 THEN "Hospital Outpatient"
    ###     WHEN 4 THEN "Federally Qualified Health Center"
    ###     WHEN 5 THEN "Retail Store or Minute Clinic"
    ###     WHEN 6 THEN "Other"
    ###     WHEN 7 THEN "None"
    ###     WHEN 8 THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE "Unknown/Did Not Report"
    ### END
df_14t_edits1_tb2['_T21 TGT Usual Source of Medical Care'] = df_14t_edits1_tb2.apply(func=fn_T21_TGT_Usual_Source_of_Medical_Care, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_T21 TGT Usual Source of Medical Care'])

#%%###################################

def fn_T22_TGT_Usual_Souce_of_Dental_Care(fdf):
    ### FW.
    if (pd.notna(fdf['Child Dental Care Source'])):
        match fdf['Child Dental Care Source']:
            case "Do not have a usual source of dental care":
                return "Do not have a usual source of dental care"
            case "Does not have a usual source of dental care":
                return "Do not have a usual source of dental care"
            case "Has a usual source of dental care":
                return "Have a usual source of dental care"
            case "Have a usual source of dental care":
                return "Have a usual source of dental care"
            case "Prenatal Client":
                return "Prenatal Client"
            case "Unknown":
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    ### LLCHD, coded values are = to form 1 categories.
    elif (pd.notna(fdf['Tgt Dental Home'])):
        match fdf['Tgt Dental Home']:
            case 1:
                return "Have a usual source of dental care" 
            case 2:
                return "Do not have a usual source of dental care"
            case 3:
                return "Unknown/Did Not Report"
            case 6:
                return "Unknown/Did Not Report"
            case _:
                return "Unrecognized Value"
    else:
        return "Unknown/Did Not Report"
    ### IF NOT ISNULL([Child Dental Care Source]) THEN CASE [Child Dental Care Source] //FW
    ###     WHEN "Do not have a usual source of dental care" THEN "Do not have a usual source of dental care"
    ###     WHEN "Does not have a usual source of dental care" THEN "Do not have a usual source of dental care"
    ###     WHEN "Has a usual source of dental care" THEN "Have a usual source of dental care"
    ###     WHEN "Have a usual source of dental care" THEN "Have a usual source of dental care"
    ###     WHEN "Prenatal Client" THEN "Prenatal Client"
    ###     WHEN "Unknown" THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSEIF NOT ISNULL([Tgt Dental Home]) THEN CASE [Tgt Dental Home] //LLCHD, coded values are = to form 1 categories
    ###     WHEN 1 THEN "Have a usual source of dental care" 
    ###     WHEN 2 THEN "Do not have a usual source of dental care"
    ###     WHEN 3 THEN "Unknown/Did Not Report"
    ###     WHEN 6 THEN "Unknown/Did Not Report"
    ###     ELSE "Unrecognized Value"
    ###     END
    ### ELSE "Unknown/Did Not Report"
    ### END
df_14t_edits1_tb2['_T22 TGT Usual Souce of Dental Care'] = df_14t_edits1_tb2.apply(func=fn_T22_TGT_Usual_Souce_of_Dental_Care, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_T22 TGT Usual Souce of Dental Care'])

#%%###################################

### Difference from Tableau in that last "if NULL" case not in this Python code.
def fn_TGT_Race(fdf):
    ### LLCHD.
    ### multiracial:
    if (
        (
            (0 if pd.isna(fdf['Tgt Race Asian']) else (1 if fdf['Tgt Race Asian']=="Y" else 0)) + 
            (0 if pd.isna(fdf['Tgt Race Black']) else (1 if fdf['Tgt Race Black']=="Y" else 0)) + 
            (0 if pd.isna(fdf['Tgt Race Hawaiian']) else (1 if fdf['Tgt Race Hawaiian']=="Y" else 0)) + 
            (0 if pd.isna(fdf['Tgt Race Indian']) else (1 if fdf['Tgt Race Indian']=="Y" else 0)) + 
            (0 if pd.isna(fdf['Tgt Race White']) else (1 if fdf['Tgt Race White']=="Y" else 0)) +
            (0 if pd.isna(fdf['Tgt Race Other']) else (1 if fdf['Tgt Race Other']=="Y" else 0)) 
        ) > 1 
    ):
        return "More than one race"
        # return (
        #     (0 if pd.isna(fdf['Tgt Race Asian']) else (1 if fdf['Tgt Race Asian']=="Y" else 0)) + 
        #     (0 if pd.isna(fdf['Tgt Race Black']) else (1 if fdf['Tgt Race Black']=="Y" else 0)) + 
        #     (0 if pd.isna(fdf['Tgt Race Hawaiian']) else (1 if fdf['Tgt Race Hawaiian']=="Y" else 0)) + 
        #     (0 if pd.isna(fdf['Tgt Race Indian']) else (1 if fdf['Tgt Race Indian']=="Y" else 0)) + 
        #     (0 if pd.isna(fdf['Tgt Race White']) else (1 if fdf['Tgt Race White']=="Y" else 0)) +
        #     (0 if pd.isna(fdf['Tgt Race Other']) else (1 if fdf['Tgt Race Other']=="Y" else 0)) 
        # )
    ### single race:
    elif (fdf['Tgt Race Asian'] == "Y"):
        return "Asian"
    elif (fdf['Tgt Race Black'] == "Y"):
        return "Black or African American"
    elif (fdf['Tgt Race Hawaiian'] == "Y"):
        return "Native Hawaiian or Other Pacific Islander"
    elif (fdf['Tgt Race Indian'] == "Y"):
        return "American Indian or Alaska Native"
    elif (fdf['Tgt Race White'] == "Y"):
        return "White"
    elif (fdf['Tgt Race Other'] == "Y"):
        return "Other"
    ### FW.
    ### multiracial, == "True" is not required in IIF statement because race is boolean.
    elif (
        (
            (0 if pd.isna(fdf['TGT Race Asian']) else (1 if fdf['TGT Race Asian'] else 0)) + 
            (0 if pd.isna(fdf['TGT Race Black']) else (1 if fdf['TGT Race Black'] else 0)) + 
            (0 if pd.isna(fdf['TGT Race Hawaiian Pacific']) else (1 if fdf['TGT Race Hawaiian Pacific'] else 0)) + 
            (0 if pd.isna(fdf['TGT Race Indian Alaskan']) else (1 if fdf['TGT Race Indian Alaskan'] else 0)) + 
            (0 if pd.isna(fdf['TGT Race White']) else (1 if fdf['TGT Race White'] else 0)) + 
            (0 if pd.isna(fdf['TGT Race Other']) else (1 if fdf['TGT Race Other'] else 0)) 
        ) > 1 
    ):
        return "More than one race"
        # return (
        #     (0 if pd.isna(fdf['TGT Race Asian']) else (1 if fdf['TGT Race Asian'] else 0)) + 
        #     (0 if pd.isna(fdf['TGT Race Black']) else (1 if fdf['TGT Race Black'] else 0)) + 
        #     (0 if pd.isna(fdf['TGT Race Hawaiian Pacific']) else (1 if fdf['TGT Race Hawaiian Pacific'] else 0)) + 
        #     (0 if pd.isna(fdf['TGT Race Indian Alaskan']) else (1 if fdf['TGT Race Indian Alaskan'] else 0)) + 
        #     (0 if pd.isna(fdf['TGT Race White']) else (1 if fdf['TGT Race White'] else 0)) + 
        #     (0 if pd.isna(fdf['TGT Race Other']) else (1 if fdf['TGT Race Other'] else 0)) 
        # ) * 10
    ### single race:
    elif (fdf['TGT Race Asian'] == True):
        return "Asian"
    elif (fdf['TGT Race Black'] == True):
        return "Black or African American"
    elif (fdf['TGT Race Hawaiian Pacific'] == True):
        return "Native Hawaiian or Other Pacific Islander"
    elif (fdf['TGT Race Indian Alaskan'] == True):
        return "American Indian or Alaska Native"
    elif (fdf['TGT Race White'] == True):
        return "White"
    elif (fdf['TGT Race Other'] == True):
        return "Other"
    #######
    else: 
        return "Unknown/Did Not Report"
    ### //LLCHD
    ### //multiracial
    ### IF IIF([Tgt Race Asian]="Y",1,0,0)+IIF([Tgt Race Black]="Y",1,0,0)+IIF([Tgt Race Hawaiian]="Y",1,0,0)+IIF([Tgt Race Indian]="Y",1,0,0)
    ### +IIF([Tgt Race Other]="Y",1,0,0)+IIF([Tgt Race White]="Y",1,0,0) > 1 THEN "More than one race"
    ### //single race
    ### ELSEIF [Tgt Race Asian] = "Y" THEN "Asian"
    ### ELSEIF [Tgt Race Black] = "Y" THEN "Black or African American"
    ### ELSEIF [Tgt Race Hawaiian] = "Y" THEN "Native Hawaiian or Other Pacific Islander"
    ### ELSEIF [Tgt Race Indian] = "Y" THEN "American Indian or Alaska Native"
    ### ELSEIF [Tgt Race White] = "Y" THEN "White"
    ### ELSEIF [Tgt Race Other] = "Y" THEN "Other"
    ### //FW
    ### //multiracial, = "True" is not required in IIF statement because race is boolean
    ### ELSEIF IIF([TGT Race Asian],1,0,0)+IIF([TGT Race Black],1,0,0)+IIF([TGT Race Hawaiian Pacific],1,0,0)
    ### +IIF([TGT Race Indian Alaskan],1,0,0)+IIF([TGT Race White],1,0,0)+IIF([TGT Race Other],1,0,0) > 1 
    ### THEN "More than one race"
    ### //single race
    ### ELSEIF [TGT Race Asian] = True THEN "Asian"
    ### ELSEIF [TGT Race Black] = True THEN "Black or African American"
    ### ELSEIF [TGT Race Hawaiian Pacific] = True THEN "Native Hawaiian or Other Pacific Islander"
    ### ELSEIF [TGT Race Indian Alaskan] = True THEN "American Indian or Alaska Native"
    ### ELSEIF [TGT Race White] = True THEN "White"
    ### ELSEIF [TGT Race Other] = True THEN "Other"
    ### ELSE "Unknown/Did Not Report"
    ### END
df_14t_edits1_tb2['_TGT Race'] = df_14t_edits1_tb2.apply(func=fn_TGT_Race, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_TGT Race'])

# #%%
# df_14t_comp_compare_tb2_race = (
#     df_14t_comparison_csv_tb2[['Project Id', '_TGT Race']]
#     .compare(df_14t__final_from_csv_tb2[['Project Id', '_TGT Race']], keep_equal=True, keep_shape=True)
#     .loc[(lambda df: df[('_TGT Race', 'self')] != df[('_TGT Race', 'other')]), :]
# )
# print(df_14t_comp_compare_tb2_race.to_string())
# # df_14t_comp_compare_tb2_race.index
# # df_14t_comp_compare_tb2_race[('Project Id', 'self')].values

# #%%
# print(
#     # df_14t__final_from_csv_tb2[[
#     df_14t_edits1_tb2[[
#         'Project Id',
#         '_TGT Race', 
#         'Tgt Race Asian', 
#         'Tgt Race Black', 
#         'Tgt Race Hawaiian', 
#         'Tgt Race Indian', 
#         'Tgt Race White', 
#         'Tgt Race Other', 
#         'TGT Race Asian', 
#         'TGT Race Black', 
#         'TGT Race Hawaiian Pacific', 
#         'TGT Race Indian Alaskan', 
#         'TGT Race White', 
#         'TGT Race Other' 
#     ]]
#     ### .loc[df_14t_edits1_tb2['_TGT Race'] == 'More than one race', :]
#     # .loc[df_14t_comp_compare_tb2_race.index, :]
#     .loc[df_14t_edits1_tb2['Project Id'].isin(df_14t_comp_compare_tb2_race[('Project Id', 'self')].values), :]
#     .to_string()
# )
# ### Shows that code is returning "More than one race" for when all 6 columns are "N"/False. Should be "Unknown".

#%%###################################

def fn_C11_Literacy_Read_Sing(fdf):
    ### FW.
    if (fdf['_Agency'] != "ll"):
        if (pd.isna(fdf['Read Tell Story Sing'])):
            return np.nan
        else:
            match fdf['Read Tell Story Sing']:
                case "0":
                    return 0
                case "1":
                    return 1
                case "2":
                    return 2
                case "3":
                    return 3
                case "4":
                    return 4
                case "5":
                    return 5
                case "6":
                    return 6
                case "7":
                    return 7
                case "YES":
                    return 7
                case _:
                    return np.nan 
    ### LLCHD.
    elif (fdf['_Agency'] == "ll"):
        match fdf['Early Language']:
            case "N":
                return 0
            case "Y":
                return 7
                ### Y = "Every day of the week / Most days of the week / Several days of the week"
            case _:
                return np.nan
    ### IF [_Agency] <> "ll" THEN CASE [Read Tell Story Sing]  // FW
    ###     WHEN "0" THEN 0
    ###     WHEN "1" THEN 1
    ###     WHEN "2" THEN 2
    ###     WHEN "3" THEN 3
    ###     WHEN "4" THEN 4
    ###     WHEN "5" THEN 5
    ###     WHEN "6" THEN 6
    ###     WHEN "7" THEN 7
    ###     WHEN "YES" THEN 7
    ###     ELSE NULL   
    ###     END
    ### ELSEIF [_Agency] = "ll" THEN CASE [Early Language]  // LLCHD
    ###     WHEN "N" THEN 0
    ###     WHEN "Y" THEN 7
    ### // Y = "Every day of the week / 
    ###     // Most days of the week / 
    ###     // Several days of the week"
    ###     ELSE NULL
    ###     END
    ### END
df_14t_edits1_tb2['_C11 Literacy Read Sing'] = df_14t_edits1_tb2.apply(func=fn_C11_Literacy_Read_Sing, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb2['_C11 Literacy Read Sing'])
### DONE?: from RUNME, function fn_C11_Literacy_Read_Sing throws error: "TypeError: boolean value of NA is ambiguous".
### TODO: Fix data typing.
# #%%
# inspect_col(df_14t_edits1_tb2['Read Tell Story Sing']) ### Originally, csv read in as float64. Should be a string. But that breaks this is/else logic. Fixed in Read above by reading in as object.
    ### Above read in as "object" not "string" so that same data type
# #%%
# inspect_col(df_14t_edits1_tb2['Early Language'])

#%%###################################

def fn_Child_Welfare_Interaction(fdf):
    ### For priority population, current maltreatment reports also need to be considered.
    ### FW.
    if (fdf['History Inter Welfare Child'] == True):
        return 1 
    elif (fdf['History Inter Welfare Child'] == False):
        return 0
    ### LLCHD.
    elif (fdf['Priority Child Welfare'] == "Y"):
        return 1 
    elif (fdf['Priority Child Welfare'] == "N"):
        return 0
    ### IF [History Inter Welfare Child] = True THEN 1 //FW
    ### ELSEIF [History Inter Welfare Child] = False THEN 0
    ### ELSEIF [Priority Child Welfare] = "Y" THEN 1 //LLCHD
    ### ELSEIF [Priority Child Welfare] = "N" THEN 0
    ### END
    ### //For priority population, current maltreatment reports also need to be considered
df_14t_edits1_tb2['_Child Welfare Interaction'] = df_14t_edits1_tb2.apply(func=fn_Child_Welfare_Interaction, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb2['_Child Welfare Interaction'])

#%%###################################

def fn_T15_6_Low_Student_Achievement(fdf):
    ### FW.
    if (fdf['NT Child Low Achievement'] == "No"):
        return 0 
    elif (fdf['NT Child Low Achievement'] == "Yes"):
        return 1
    ### LLCHD.
    elif (fdf['Priority Low Student'] == "N"):
        return 0 
    elif (fdf['Priority Low Student'] == "Y"):
        return 1
    ### IF [NT Child Low Achievement] = "No" THEN 0 //FW
    ### ELSEIF [NT Child Low Achievement] = "Yes" THEN 1
    ### ELSEIF [Priority Low Student] = "N" THEN 0 //LLCHD
    ### ELSEIF [Priority Low Student] = "Y" THEN 1
    ### END
df_14t_edits1_tb2['_T15-6 Low Student Achievement'] = df_14t_edits1_tb2.apply(func=fn_T15_6_Low_Student_Achievement, axis=1).astype('Int64') 
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb2['_T15-6 Low Student Achievement'])

#%%##################################################
### COALESCING

### Dependent on '_FW Gestation Age Recode'.
df_14t_edits1_tb2['_TGT Gestational Age'] = df_14t_edits1_tb2['tgt GestationalAge'].combine_first(df_14t_edits1_tb2['_FW Gestation Age Recode']).astype('Int64') 
    ### IFNULL([tgt GestationalAge], [_FW Gestation Age Recode])
    ### Data Type in Tableau: integer.

#%%##################################################
### DATE CALCULATIONS

### These calculations assume all date variables are dtype "datetime64".
### All in section Dependent on '_TGT DOB'.

df_14t_edits1_tb2['_TGT 2 Week Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(weeks=2)).astype('datetime64[ns]') 
    ### DATE(DATEADD('week',2,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 3 Day Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(days=3)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',3,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 30 Day Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(days=30)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',30,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 5 Week Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(weeks=5)).astype('datetime64[ns]') 
    ### DATE(DATEADD('week',5,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 56 Day Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(days=56)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',56,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 7 Day Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(days=7)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',7,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 8 Day Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(days=8)).astype('datetime64[ns]') 
    ### DATE(DATEADD('day',8,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 4 Week Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(weeks=4)).astype('datetime64[ns]') 
    ### DATE(DATEADD('week',4,[_TGT DOB])) 
    ### Data Type in Tableau: date.

### TODO: Fix Space in variable name! (but not yet.)
df_14t_edits1_tb2['_TGT 10 Month Date '] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(months=10)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',10,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 11 Month Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(months=11)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',11,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 2 Month Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(months=2)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',2,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 3 Month Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(months=3)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',3,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 4 Month Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(months=4)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',4,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 5 Month Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(months=5)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',5,[_TGT DOB])) 
    ### Data Type in Tableau: date.

### TODO: Fix Space in variable name! (but not yet.)
df_14t_edits1_tb2['_TGT 6 Month Date '] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(months=6)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',6,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 7 Month Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(months=7)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',7,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 8 Month Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(months=8)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',8,[_TGT DOB])) 
    ### Data Type in Tableau: date.

df_14t_edits1_tb2['_TGT 9 Month Date'] = (df_14t_edits1_tb2['_TGT DOB'] + pd.DateOffset(months=9)).astype('datetime64[ns]') 
    ### DATE(DATEADD('month',9,[_TGT DOB])) 
    ### Data Type in Tableau: date.

#%%##################################################
### COLUMNS with DIFFERENT VALUES from the Comparison:

#%%
df_14t_edits1_tb2['_C18 ASQ 9 Mo Referral Date'] = df_14t_edits1_tb2['Asq3 Referral 9Mm'].combine_first(df_14t_edits1_tb2['ASQ9MoRefDate']).astype('datetime64[ns]') 
    ### IFNULL([Asq3 Referral 9Mm],[ASQ9MoRefDate])
    ### Data Type in Tableau: date.
# inspect_col(df_14t_edits1_tb2['_C18 ASQ 9 Mo Referral Date'])
# #%%
# inspect_col(df_14t_edits1_tb2['Asq3 Referral 9Mm']) ### Empty & so did not read in as a Date. Fixed above in Read.
# #%%
# inspect_col(df_14t_edits1_tb2['ASQ9MoRefDate'])
# #%%
# compare_col(df_14t_comparison_csv_tb2, df_14t_edits1_tb2, '_C18 ASQ 9 Mo Referral Date')
# #%%
# compare_col(df_14t_comparison_csv_tb2, df_14t_edits1_tb2, '_C18 ASQ 9 Mo Referral Date', 'value_counts')
# #%%
# df_14t_comparison_csv_tb2[['_C18 ASQ 9 Mo Referral Date']].compare(df_14t_edits1_tb2[['_C18 ASQ 9 Mo Referral Date']])
# #%%
# # df_14t_edits1_tb2['_C18 ASQ 9 Mo Referral Date'].dtypes
# df_14t_edits1_tb2.dtypes

###################################

#%%
# df_14t_edits1_tb2['_Family Number'] = df_14t_edits1_tb2['Family Id'].combine_first(df_14t_edits1_tb2['Family Number'].astype('Int64'))
df_14t_edits1_tb2['_Family Number'] = df_14t_edits1_tb2['Family Id'].combine_first(df_14t_edits1_tb2['Family Number'].astype('Int64')).astype('string') 
    ### IFNULL([Family Id],[Family Number])
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_Family Number'])
### Most are integers, some are long string ID's of letters & numbers.
# #%%
# inspect_col(df_14t_edits1_tb2['Family Id']) ### Long IDs.
# #%%
# inspect_col(df_14t_edits1_tb2['Family Number']) ### Just Integers.
# #%%
# ### 2024-01-05: All ID variables should be strings.
# ### This var should be an integer. Adjusting above.
# df_14t_edits1_tb2['Family Number'].fillna(-9999).apply(float.is_integer).all()

#%%###################################

### TODO: Move to Tableau & pin to start/end of Reporting period.

### RECOMMEND that [_T05 TGT Age in Months] & ['_T05 Age Categories'] be created in the Form 1/2 Tableau Workbooks because the calculations do not match exactly between Tableau & Python. 

### Questions: (1) When dividing by "1 month" in Python & Tableau, what exact number is used? (2) Float > Int: truncated or rounded? 
### Testing in Tableau on DATEDIFF shows that it rounds to an integer, so implemented here.
### TODO: FIX: first if clause.
### TODO: Ask Joe purpose of IF clause. WHY!?!?!?!?
### Would love this var to be a Pandas Int (that allows NAs), but breaks later calculations based on this var.
### TODO: Fix PROBLEM!!!: Should NOT base calculations of Age off of Today's date -- changes every time runs! Should be based off of end of reporting period/a specific date..
# now = pd.Timestamp('now')
# date_for_age_calcs_14t_tb2 = now

### Base Age off [Report End Date]. *** Check how these are used in the F1/F2.

date_for_age_calcs_14t_tb2 = pd.Timestamp("2023-02-08T14:12")
def fn_T05_TGT_Age_in_Months(fdf):
    if (fdf['_TGT DOB'] is pd.NaT):
        return np.nan
        # return 0 ### Testing.
    ### Break.
    # if (fdf['_TGT DOB'] > (date_for_age_calcs_14t_tb2 - pd.DateOffset(months=
    #         (pd.Series((date_for_age_calcs_14t_tb2 - fdf['_TGT DOB']) / np.timedelta64(1, 'M')).astype('Float64').astype('Int64')) ### Must be int.
    #     ))):
    if ((fdf['_TGT DOB'] is not pd.NaT) and 
        (fdf['_TGT DOB'] > (date_for_age_calcs_14t_tb2 - pd.DateOffset(months=np.where(
            (fdf['_TGT DOB'] is not pd.NaT)
            ,(pd.Series((date_for_age_calcs_14t_tb2 - fdf['_TGT DOB']) / np.timedelta64(1, 'M')).astype('Float64').astype('Int64')) ### Must be int.
            ,0 ### Missing DOB's should be removed in "if" but pd.DateOffset can't handle missing values, so need this np.where.
        ))))):
        return pd.Series(((date_for_age_calcs_14t_tb2 - pd.DateOffset(days=1)) - fdf['_TGT DOB']) / np.timedelta64(1, 'M'))#.astype('Float64')#.astype('Int64')
        # return 1 ### Testing.
    else:
        ### return (((date_for_age_calcs_14t_tb2 - fdf['_TGT DOB'])) / pd.DateOffset(months=1)).astype('Float64').astype('Int64')
        return pd.Series((date_for_age_calcs_14t_tb2 - fdf['_TGT DOB']) / np.timedelta64(1, 'M'))#.astype('Float64')#.astype('Int64')
        # return 0 ### Testing.
    ### IF [_TGT DOB]> DATEADD('month',-DATEDIFF('month',[_TGT DOB],TODAY()),TODAY())
    ### THEN DATEDIFF('month',[_TGT DOB],TODAY()-1)
    ### ELSE DATEDIFF('month',[_TGT DOB],TODAY())
    ### END
df_14t_edits1_tb2['_T05 TGT Age in Months'] = df_14t_edits1_tb2.apply(func=fn_T05_TGT_Age_in_Months, axis=1).round()#.astype('Float64').astype('Int64')
    ### Data Type in Tableau: integer.
# inspect_col(df_14t_edits1_tb2['_T05 TGT Age in Months'])

# #%%
# # print(df_14t_edits1_tb2[['_T05 TGT Age in Months', '_TGT DOB']].query('`_T05 TGT Age in Months` == 1').to_string())
# # print(df_14t_edits1_tb2[['_T05 TGT Age in Months', '_TGT DOB']].value_counts(dropna=False).to_string())
# # print(df_14t_edits1_tb2[['_T05 TGT Age in Months']].value_counts(dropna=False, sort=False).to_string())
# print(df_14t_edits1_tb2[['Project Id', '_T05 TGT Age in Months', '_TGT DOB']].query('`_T05 TGT Age in Months` < 1').to_string())

# #%%
# inspect_col(df_14t_edits1_tb2['_TGT DOB'])
# #%%
# print(df_14t_comp_compare_tb2[['_T05 TGT Age in Months']].to_string())
# #%%
# df_T05_TGT_Age_in_Months = (
#     pd.merge(
#         df_14t_comparison_csv_tb2[['Project Id','Year','Quarter', '_T05 TGT Age in Months']],
#         df_14t__final_from_csv_tb2[['Project Id','Year','Quarter', '_T05 TGT Age in Months', '_TGT DOB']],
#         how='outer', 
#         on=['Project Id','Year','Quarter'], 
#         suffixes=(' {comp}', ''),
#         indicator=True
#     )
# )
# #%%
# print(df_T05_TGT_Age_in_Months.to_string())

#%%###################################

### TODO: Move to Tableau & pin to start/end of Reporting period.

### TODO: Adjust to deal with "-1" months old.
def fn_T05_Age_Categories(fdf):
    if (fdf['_T05 TGT Age in Months'] < 12):
        return "< 1 year"
    elif (fdf['_T05 TGT Age in Months'] < 36):
        return "1-2 years" ### there is no group for 2-3 years old on F1 so they are lumped in here.
    elif (fdf['_T05 TGT Age in Months'] < 48):
        return "3-4 years"
    elif (fdf['_T05 TGT Age in Months'] <= 60):
        return "5-6 years"
    elif (fdf['_T05 TGT Age in Months'] > 60):
        return "6+ years"
    else:
        return "Unknown/Did Not Report"
    ### IF [_T05 TGT Age in Months] < 12 THEN "< 1 year"
    ### ELSEIF [_T05 TGT Age in Months] < 36 THEN "1-2 years" //there is no group for 2-3 years old on F1 so they are lumped in here
    ### ELSEIF [_T05 TGT Age in Months] < 48 THEN "3-4 years"
    ### ELSEIF [_T05 TGT Age in Months] <= 60 THEN "5-6 years"
    ### ELSEIF [_T05 TGT Age in Months] > 60 THEN "6+ years"
    ### ELSE "Unknown/Did Not Report"
    ### END
df_14t_edits1_tb2['_T05 Age Categories'] = df_14t_edits1_tb2.apply(func=fn_T05_Age_Categories, axis=1).astype('string') 
    ### Data Type in Tableau: 'string'.
# inspect_col(df_14t_edits1_tb2['_T05 Age Categories'])


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

#%%
# list_14t_unrecognized_values_tb2 = fn_find_unrecognized_value(df_14t_edits1_tb2)
list_14t_unrecognized_values_tb2 = fn_find_unrecognized_value(df_14t_edits1_tb2.query(f'`Year` == 12 & `Quarter` == 4'))

#%%
len(list_14t_unrecognized_values_tb2)
#%%
### Columns that have "Unrecognized Value":
[x['col'] for x in list_14t_unrecognized_values_tb2]

#%%
### Look at one column:
# list_14t_unrecognized_values_tb2[0]

# ### New values Y12Q4.
# [x for x in list_14t_unrecognized_values_tb2 if x["col"] == '_Funding'] 
# [x for x in list_14t_unrecognized_values_tb2 if x["col"] == '_T20 TGT Insurance Status'] 

### Fixed:
# [x for x in list_14t_unrecognized_values_tb2 if x["col"] == '_T06 TGT Ethnicity'] 
# [x for x in list_14t_unrecognized_values_tb2 if x["col"] == '_T1 Tgt Gender'] 

### !TESTRUNHERE!


#%%##################################################
### Prepare CSV ###
#####################################################

#%%################################
### REMOVE extra COLUMNS

### Remove columns created in merge.
df_14t_edits2_tb2 = df_14t_edits1_tb2.drop(columns=['LJ_tb2_2ER', 'LJ_tb2_3FW', 'LJ_tb2_4LL', 'LJ_tb2_5WC'])

#%%################################
### ORDER COLUMNS

### Final order for columns:
[*df_14t_comparison_csv_tb2]

#%%
### Reorder Columns.
df_14t_edits2_tb2 = df_14t_edits2_tb2[[*df_14t_comparison_csv_tb2]]

#%%################################
### SORT ROWS

df_14t_edits2_tb2 = df_14t_edits2_tb2.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)

###################################
### SET DATA TYPES
### Set the data type of each column like it should be in output.

#%%
### Identify columns that should be Integers:
cols_14t_int_tb2 = df_14t_edits2_tb2.select_dtypes(include=['float']).fillna(-9999).applymap(float.is_integer).all().loc[lambda x: x==True].index.to_series()
print(cols_14t_int_tb2.to_string())
#%%
print(df_14t_edits2_tb2.dtypes.to_string())

#%%
### Turn all columns that should be into Integers:
df_14t_edits2_tb2[cols_14t_int_tb2] = df_14t_edits2_tb2[cols_14t_int_tb2].astype('Int64')
#%%
print(df_14t_edits2_tb2.dtypes.to_string())

#%%##################################################
### WRITE ###
#####################################################

#%%
### Created Final DF.
df_14t__final_tb2 = df_14t_edits2_tb2.copy()

#%%
### Write out df.
df_14t__final_tb2.to_csv(path_14t_output_tb2, index=False, date_format="%#m/%#d/%Y")


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
df_14t__final_from_csv_tb2 = pd.read_csv(path_14t_output_tb2, dtype=object, keep_default_na=False, na_values=[''])

#%%##################################################
### COMPARE CSVs ###
#####################################################

#%%###################################

#%%
### Column names:
[*df_14t__final_from_csv_tb2]
#%%
### Column names:
[*df_14t_comparison_csv_tb2]

#%%
### Overlap / Similarities: Columns in both.
set([*df_14t_comparison_csv_tb2]).intersection([*df_14t__final_from_csv_tb2])

#%%###################################
### COLUMNS:

#%%
### Check if all Column names identical & in same order.
[*df_14t__final_from_csv_tb2] == [*df_14t_comparison_csv_tb2]

#%%
### Differences: Columns only in one.
set([*df_14t_comparison_csv_tb2]).symmetric_difference([*df_14t__final_from_csv_tb2])

#%%###################################

####### Compare values
### including row count, distinct ids, 

#%%
# Check rows & cols:
print(f'df_14t__final_from_csv_tb2 Rows: {len(df_14t__final_from_csv_tb2)}')
print(f'df_14t_comparison_csv_tb2 Rows: {len(df_14t_comparison_csv_tb2)}')

print(f'df_14t__final_from_csv_tb2 Columns: {len(df_14t__final_from_csv_tb2.columns)}')
print(f'df_14t_comparison_csv_tb2 Columns: {len(df_14t_comparison_csv_tb2.columns)}')

#%%
df_14t__final_from_csv_tb2 == df_14t_comparison_csv_tb2

#%%
### Checking ID columns used in Join >> DF should be empty (meaning all the same).
df_14t_comp_compare_tb2 = df_14t_comparison_csv_tb2[['Project Id','Year','Quarter']].compare(df_14t__final_from_csv_tb2[['Project Id','Year','Quarter']])
df_14t_comp_compare_tb2

###################################
###################################
###################################

#%%
### Now comparing ALL columns. DF created shows all differences:
# df_14t_comp_compare_tb2 = df_14t_comparison_csv_tb2.compare(df_14t__final_from_csv_tb2)
df_14t_comp_compare_tb2 = df_14t_comparison_csv_tb2.query(f'Year=="12" & Quarter=="4"').compare(df_14t__final_from_csv_tb2.query(f'Year=="12" & Quarter=="4"'))
df_14t_comp_compare_tb2

#%%
### Number of columns with different values/types:
len([*df_14t_comp_compare_tb2]) / 2 
    ### Was 13 before read out & then back in. 
    ### 120 when read in with no dtypes set (a lot of them are dates).
    ### 241 columns different when both CSV's are ready in with dtype=object (string) for everything (now lots of Floats that should be Integers).
    ### 130 after fixing Date output.
    ### 9 after fixing Integers.

#%%
### Columns:
[*df_14t_comp_compare_tb2]

### !TESTRUNHERE!


### Columns with different values after fixing dates & integers:
# ['_C18 ASQ 9 Mo Referral Date', ### Fixed: Made sure both needed variables read in as dates (one wasn't).
#  '_Discharge Reason', ### Fixed: Changed "if not None" to "if not pd.NaT".
#  '_Family Number', ### Fixed: Is a string, half of which needed to be first an integer.
#  '_T05 Age Categories',
#  '_T05 TGT Age in Months',
#  '_T06 TGT Ethnicity',
#  '_T13 TGT Language',
#  '_TGT Race',
#  'Discharge Reason']


### KEY DIFFERENCES:
    ### Integers as integers (no decimal).
    ### Dates as format "5/5/2020" instead of "2019-09-11".
    ### Then a few columns might actually have calculation issues.

#%%
print(df_14t_comp_compare_tb2[['_Discharge Reason', 'Discharge Reason']].to_string())
# print(df_14t_comp_compare_tb2[['Discharge Reason']].to_string())
### Where are these extra values coming from?? ### Fixed code in Tableau was wrong (wasn't expecting stings). So now won't match until Tableau CSV created again.

#%%
# print(df_14t_comp_compare_tb2[['_T05 TGT Age in Months']].to_string())
print(df_14t_comp_compare_tb2[['_T05 TGT Age in Months', '_T05 Age Categories']].to_string())
### Age in Month calculation is off by 1 many times. Is it exactly what number is used in the division? Something else?
### TODO: Recommend moving both of these variables to the Form 1&2 Tableau Workbooks.

#%%
# print(df_14t_comp_compare_tb2[['_Family Number']].to_string())
# print(df_14t_comp_compare_tb2[['_T06 TGT Ethnicity']].to_string()) ### Fixed, so no longer in comparsion.

# #%%
# ### Fixed, so no longer in comparsion.
# df_14t_comp_compare_tb2_ethnicity = (
#     df_14t_comparison_csv_tb2[['_T06 TGT Ethnicity', 'Tgt Ethnicity', 'Tgt Ethnicity1']]
#     .compare(df_14t__final_from_csv_tb2[['_T06 TGT Ethnicity', 'Tgt Ethnicity', 'Tgt Ethnicity1']], keep_equal=True, keep_shape=True)
#     # .iloc[lambda df: [0], :] ### !!! Want to filter rows by only where columns 0 & 1 are different.
#     .loc[(lambda df: df[('_T06 TGT Ethnicity', 'self')] != df[('_T06 TGT Ethnicity', 'other')]), :]
# )
# print(df_14t_comp_compare_tb2_ethnicity.to_string())

# #%%
# df_14t__final_from_csv_tb2.loc[[379, 456, 463], ['Project Id', '_T06 TGT Ethnicity', 'Tgt Ethnicity', 'Tgt Ethnicity1']]
# #%%
# df_14t_comparison_csv_tb2.loc[[379, 456, 463], ['Project Id', '_T06 TGT Ethnicity', 'Tgt Ethnicity', 'Tgt Ethnicity1']]
# ### Python, even when reading everything in as "object," is still changing the text "null" into NaN.
# ### FIXED: Edited Read settings so only blank cells read in as NA.

# #%%
# print(df_14t_comp_compare_tb2[['_T13 TGT Language']].to_string()) ### FIXED above by making case-insensitive.

# #%%
# print(df_14t_comp_compare_tb2[['_TGT Race']].to_string()) ### FIXED above by making case-insensitive.

#%%##################################################
### Columns not reconciled:
[*df_14t_comp_compare_tb2]


#%%##################################################
### END: ALL GOOD.

### Comparision:
# df_14t_comparison_csv_tb2.compare(df_14t__final_from_csv_tb2)[['Project Id', 'www', 'www']]



#%%################################
### Column Comparisons
###################################

#!HERE

var_to_compare = '_T13 TGT Language'

var_list_for_comparison = [var_to_compare]

# var_list_keys_or_ids = ['Project Id']
var_list_keys_or_ids = ['Project Id','Year','Quarter']
# var_list_keys_or_ids = ['Project Id', 'Agency']
# var_list_keys_or_ids = ['Project Id', 'Agency', 'Fob Involved', 'Fob Involved1']

print((
    # df_14t_comparison_csv_tb2.compare(df_14t__final_from_csv_tb2, keep_shape=True, keep_equal=True) 
    df_14t_comparison_csv_tb2.query(f'Year=="12" & Quarter=="4"').compare(df_14t__final_from_csv_tb2.query(f'Year=="12" & Quarter=="4"'), keep_shape=True, keep_equal=True) 
    .loc[:, var_list_keys_or_ids + var_list_for_comparison]
    .loc[lambda df: df.apply(fn_keep_row_differences, axis=1, variable2compare=var_to_compare), :] 
    ##########
    ### Testing numeric vars:
    # .apply(lambda df: df[(var_to_compare, 'self')] == df[(var_to_compare, 'other')], axis=1) ### Outputs a Series.
    # .apply(lambda df: float(df[(var_to_compare, 'self')]) == float(df[(var_to_compare, 'other')]), axis=1)
    # .all()
    ##########
    ### Testing date vars:
    # .apply(lambda df: pd.to_datetime(df[(var_to_compare, 'self')]) == pd.to_datetime(df[(var_to_compare, 'other')]), axis=1)
    # .all()
).to_string())


##########
#%%
# compare_col(df_14t_comparison_csv_tb2, df_14t__final_from_csv_tb2, var_to_compare, info_or_value_counts='info')
compare_col(df_14t_comparison_csv_tb2, df_14t__final_from_csv_tb2, var_to_compare, info_or_value_counts='value_counts')
#%%
inspect_col(df_14t__final_from_csv_tb2[var_to_compare]) 
#%%
inspect_col(df_14t_comparison_csv_tb2[var_to_compare]) 
#%%
inspect_col(df_14t_edits1_tb2[var_to_compare]) 
#%%
# print(df_14t_comp_compare_tb2[[var_to_compare]].to_string())


#%%################################













#%%##################################################
### Things to CHANGE when part of a fully-coded data pipeline ###
#####################################################

### Set names & dtypes at beginning & be consistent.
### Consolidate long match-case statements with "|" (or) case statements.


#%%
### END Child2! SUCCESS!


# %%



### TODO in Form2:
    ### Delete "sets", relics of user management on Server.


