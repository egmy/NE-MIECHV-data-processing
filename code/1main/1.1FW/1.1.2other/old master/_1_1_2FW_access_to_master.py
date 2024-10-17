
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
if (os.path.basename(__file__) == '_1_1_2FW_access_to_master.py'):
    from _1_1_2FW_RUNME import * 
    print('Imported "_1_1_2FW_RUNME"')
else:
    print("Did NOT run RUNME again... because it's already running!")


#%%##############################################!>>>
### >>> COLUMN DEFINITIONS 
#####################################################

#%%### df_112FW_1: '04 Well Child v2 no MAX - use this one.xlsx'.
#%%### df_112FW_2: '08 Child ER Injury.xlsx'.
#%%### df_112FW_3: '16 - Caregiver Insurance v2 - USE THIS ONE.xlsx'.
#%%### df_112FW_4: 'Adult Activities Query.xlsx'.
#%%### df_112FW_5: 'Child Activities Query.xlsx'.
#%%### df_112FW_6: 'Adult UNCOPE Query.xlsx'.
#%%### df_112FW_7: 'F1 - Home Visit Type Query.xlsx'.
#%%### df_112FW_8: 'Referral Exclusions 1 thru 6.xlsx'.
#%%###
pd.set_option('display.max_columns', None)

#######################

### List = [name, dtype]

#######################
#%%### df_112FW_1: '04 Well Child v2 no MAX - use this one.xlsx'.
list_112FW_col_detail_1 = [

    ['Project ID', 'string']
    ,['agency', 'string']
    ,['FAMILY NUMBER', 'Int64']
    ,['ChildNumber', 'Int64'] 
    ,['WellVisitDate', 'datetime64[ns]']
]
#%%### df_112FW_1: '04 Well Child v2 no MAX - use this one.xlsx'.
dict_112FW_col_dtypes_1 = {x[0]:x[1] for x in list_112FW_col_detail_1}
print(dict_112FW_col_dtypes_1)
print(collections.Counter(list(dict_112FW_col_dtypes_1.values())))
### List of date columns:
list_112FW_date_cols_1 = [key for key, value in dict_112FW_col_dtypes_1.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_112FW_2: '08 Child ER Injury.xlsx'.

list_112FW_col_detail_2 = [
    ['Project ID', 'string']
    ,['agency', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['FAMILY NUMBER', 'Int64']
    ,['ChildNumber', 'Int64']
    ,['ERVisitReason', 'string']
    ,['IncidentDate', 'datetime64[ns]']
]
#%%### df_112FW_2: '08 Child ER Injury.xlsx'.
dict_112FW_col_dtypes_2 = {x[0]:x[1] for x in list_112FW_col_detail_2}
print(dict_112FW_col_dtypes_2)
print(collections.Counter(list(dict_112FW_col_dtypes_2.values())))
### List of date columns:
list_112FW_date_cols_2 = [key for key, value in dict_112FW_col_dtypes_2.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_112FW_3: '16 - Caregiver Insurance v2 - USE THIS ONE.xlsx'.

list_112FW_col_detail_3 = [
    ['Project ID', 'string']
    ,['agency', 'string'] 
    ,['FAMILY NUMBER', 'Int64']
    ,['ChildNumber', 'Int64'] 
    ,['funding', 'string']
    ,['AD1InsChange', 'Int64']
    ,['AD1InsChangeDate', 'datetime64[ns]']
    ,['AD1PrimaryIns', 'string']
]
#%%### df_112FW_3: '16 - Caregiver Insurance v2 - USE THIS ONE.xlsx'.
dict_112FW_col_dtypes_3 = {x[0]:x[1] for x in list_112FW_col_detail_3}
print(dict_112FW_col_dtypes_3)
print(collections.Counter(list(dict_112FW_col_dtypes_3.values())))
### List of date columns:
list_112FW_date_cols_3 = [key for key, value in dict_112FW_col_dtypes_3.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_112FW_4: 'Adult Activities Query.xlsx'.

#The below code helps me read in the column names in the right format, so use it if the columns change, otherwise it's not needed

# df = pd.read_excel('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other\\0in\\Y13Q1 (Oct 2023 - Dec 2023)\\Adult Activities Query.xlsx') # can also index sheet by name or fetch all sheets
# list_112FW_col_detail_4 = df.columns.tolist()
# for x in list_112FW_col_detail_4:
#     print("['{}',],".format(x))

list_112FW_col_detail_4= [

    ['Project ID','string'],
    ['agency','string'],
    ['FamilyNumber','Int64'],
    ['ChildNumber','Int64'],
    ['TGT DOB-CR','datetime64[ns]'],
    ['EDCDate','datetime64[ns]'],
    ['MinOfHVDate', 'datetime64[ns]'],
    ['TERMINATION STATUS', 'string'],
    ['TERMINATION DATE','datetime64[ns]'],
    ['Pregnancystatus','Int64'],
    ['MaxOfHVDate', 'datetime64[ns]'],
    ['MaxOfVISIT NUMBER','Int64'],
    ['BehaviorNumer', 'Int64'],
    ['BehaviorDenom','Int64'],
    ['Home Visits Prenatal','Int64'],
    ['Home Visits Total','Int64'],
    ['MOBDOB','datetime64[ns]'],
    ['FOBDOB','datetime64[ns]'],
    ['MinEducationDate','datetime64[ns]'],
    ['AD1MinEdu','string'],
    ['MinEduEnroll','string'],
    ['MaxEduDate','datetime64[ns]'],
    ['AD1MaxEdu','string'],
    ['MaxEduEnroll','string'],
    ['TobaccoUseDate', 'datetime64[ns]'],
    ['TobaccoRefDate','datetime64[ns]'],
    ['AssessAfraid', 'boolean'],
    ['AssessPolice','boolean'],
    ['AssessIPV', 'boolean'],
    ['IPV Assess Date','datetime64[ns]'],
    ['IPVRefDate','datetime64[ns]'],
    ['PostpartumCheckupDate','datetime64[ns]'],
    ['MinOfCESDDATE','datetime64[ns]'],
    ['CESDTotal','Int64'],
    ['MinOfMHRefDate','datetime64[ns]'],
    ['MaxCHEEERSDate','datetime64[ns]'],
    ['AD2EduDateMax','datetime64[ns]'],
    ['AD2EDLevel','string'],
    ['AD2InSchool','string'],
    ['MaxOfMaxOfAD2InsChangeDate','datetime64[ns]'],
    ['AD2InsPrimary','string'],
    ['MaxOfAD1EmpChangeDate','datetime64[ns]'],
    ['AD1EmpStatus','string'],
    ['MaxOfAD2EmplChangeDate','datetime64[ns]'],
    ['AD2EmployStatus','string'],
    ['MaxOfMaxOfAD1MSChangeDate','datetime64[ns]'],
    ['Adult1MaritalStatus','string'],
    ['MaxOfAD2MSChangeDate','datetime64[ns]'],
    ['Adult2MaritalStatus','string'],
    ['ANNUAL INCOME','Float64'],
    ['POVERTY LEVEL','Float64'],
    ['TYPE HOUSING','string'],
    ['HomelessStatus','string'],
    ['HousingStatus','string'],
    ['HistoryInterWelfareAdult','boolean'],
    ['MOB SUBSTANCE ABUSE','boolean'],
    ['FOB SUBSTANCE ABUSE','boolean'],
    ['TobaccoUseInHome','string'],
    ['LowAchievement','string'],
    ['NTChildLowAchievement','string'],
    ['NTChildDevDelay','string'],
    ['Military','string'],
    ['Adult1Gender','string'],
    ['Adult1TGTRelation','string'],
    ['MOB ETHNIC','string'],
    ['MOBRaceWhite','boolean'],
    ['MOBRaceBlack','boolean'],
    ['MOBRaceIndianAlaskan','boolean'],
    ['MOBRaceAsian','boolean'],
    ['MOBRaceHawaiianPacific','boolean'],
    ['MOBRaceOther','boolean'],
    ['FOB INVOLVED','boolean'],
    ['Adult2Gender','string'],
    ['Adult2TGTRelation','string'],
    ['FOB ETHNICITY','string'],
    ['FOBRaceWhite','boolean'],
    ['FOBRaceBlack','boolean'],
    ['FOBRaceIndianAlaskan','boolean'],
    ['FOBRaceAsian','boolean'],
    ['FOBRaceHawaiianPacific','boolean'],
    ['FOBRaceOther','boolean'],
    ['MOB ZIP','Int64'],
    ['Adaptation','string'], ##not sure about this one, all values seem to be 'NA'
    ]


#%%### df_112FW_4: Adult Activities Query.xlsx'.
dict_112FW_col_dtypes_4 = {x[0]:x[1] for x in list_112FW_col_detail_4}
print(dict_112FW_col_dtypes_4)
print(collections.Counter(list(dict_112FW_col_dtypes_4.values())))
### List of date columns:
list_112FW_date_cols_4 = [key for key, value in dict_112FW_col_dtypes_4.items() if value == 'datetime64[ns]'] 


#%%### df_112FW_5: 'Child Activities Query.xlsx'.
# df = pd.read_excel('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other\\0in\\Y13Q1 (Oct 2023 - Dec 2023)\\Child Activities Query.xlsx') # can also index sheet by name or fetch all sheets
# list_112FW_col_detail_5 = df.columns.tolist()
# print(len(list_112FW_col_detail_5))
# for x in list_112FW_col_detail_5:
#     print("['{}',],".format(x))
list_112FW_col_detail_5 = [
    ['Project ID','string'],
    ['agency','string'],
    ['FAMILY NUMBER','Int64'],
    ['ChildNumber','Int64'],
    ['MinOfHVDate','datetime64[ns]'],
    ['TERMINATION DATE','datetime64[ns]'],
    ['TERMINATION STATUS','string'],
    ['MaxOfHVDate','datetime64[ns]'],
    ['TGT DOB-CR','datetime64[ns]'],
    ['EDCDate','datetime64[ns]'],
    ['MinHVDateBFYes','datetime64[ns]'],
    ['BreastFeeding','Int64'],
    ['MinOfDateDiscontinueBF','datetime64[ns]'],
    ['SafeSleepDate','datetime64[ns]'],
    ['SafeSleepPartialDate','datetime64[ns]'],
    ['ASQ9MoDate','datetime64[ns]'],
    ['ASQ9MoTiming','Int64'],
    ['ASQ9MoCom','Int64'],
    ['ASQ9MoGross','Int64'],
    ['ASQ9MoFine','Int64'],
    ['ASQ9MoProblem','Int64'],
    ['ASQ9MoPersonal','Int64'],
    ['ASQ18MoDate','datetime64[ns]'],
    ['ASQ18MoTiming','Int64'],
    ['ASQ18MoCom','Int64'],
    ['ASQ18MoGross','Int64'],
    ['ASQ18MoFine', 'Int64'],
    ['ASQ18MoProblem','Int64'],
    ['ASQ18MoPersonal','Int64'],
    ['ASQ24MoDate','datetime64[ns]'],
    ['ASQ24MoTiming','Int64'],
    ['ASQ24MoCom','Int64'],
    ['ASQ24MoGross','Int64'],
    ['ASQ24MoFine','Int64'],
    ['ASQ24MoProblem','Int64'],
    ['ASQ24MoPersonal','Int64'],
    ['ASQ30MoDate','datetime64[ns]'],
    ['ASQ30MoTiming','Int64'],
    ['ASQ30MoCom','Int64'],
    ['ASQ30MoGross','Int64'],
    ['ASQ30MoFine','Int64'],
    ['ASQ30MoProblem','Int64'],
    ['ASQ30MoPersonal','Int64'],
    ['MaxOfReadToChild','Int64'],
    ['MaxEarlyLiteracyDate','datetime64[ns]'],
    ['BehaviorDenom','Int64'],
    ['BehaviorNumer','Int64'],
    ['TGTInsureChangeDate','datetime64[ns]'],
    ['CHINSPrimaryIns','string'],
    ['MOB LANGUAGE','string'],
    ['ChildMedCareSource', 'string'],
    ['ChildDentalCareSource','string'],
    ['NTChildDevDelay','string'],
    ['NTChildLowAchievement','string'],
    ['HistoryInterWelfareChild','boolean'],
    ['ASQ9MoRefDate','datetime64[ns]'],
    ['ASQ9MoRefLocation','string'],
    ['ASQ9MoRefEIDate','datetime64[ns]'],
    ['ASQ9MoRefCSDate','datetime64[ns]'],
    ['ASQ18MoRefDate','datetime64[ns]'],
    ['ASQ18MoRefLocation','string'],
    ['ASQ18MoEIDate','datetime64[ns]'],
    ['ASQ18MoCSDate','datetime64[ns]'],
    ['ASQ24MoRefDate','datetime64[ns]'],
    ['ASQ24MoRefLocation','string'],
    ['ASQ24MoEIDate','datetime64[ns]'],
    ['ASQ24MoCSDate','datetime64[ns]'],
    ['ASQ30MoRefDate','datetime64[ns]'],
    ['ASQ30MoRefLocation','string'],
    ['ASQ30MoEIDate','datetime64[ns]'],
    ['ASQ30MoCSDate','datetime64[ns]'],
    ['TGT GENDER','string'],
    ['TGT ETHNICITY','string'],
    ['TGTRaceWhite','boolean'],
    ['TGTRaceBlack','boolean'],
    ['TGTRaceIndianAlaskan','boolean'],
    ['TGTRaceAsian','boolean'],
    ['TGTRaceHawaiianPacific','boolean'],
    ['TGTRaceOther','boolean'],
    ['Adaptation','string'], #Also NA values
    ['12 - 09 ASQ3_WhyNotDone','string'],
    ['12 - 18 ASQ3_WhyNotDone','string'],
    ['12 - 24 ASQ3_WhyNotDone','string'],
    ['12 - 30 ASQ3_WhyNotDone','string'],
    ['GESTATIONAL AGE','string'],

]
#%%### df_112FW_5: 'Child Activities Query.xlsx'.
print(len(list_112FW_col_detail_5))
for x in list_112FW_col_detail_5:
    print(x[0],x[1])
dict_112FW_col_dtypes_5 = {x[0]:x[1] for x in list_112FW_col_detail_5}
print(dict_112FW_col_dtypes_5)
print(collections.Counter(list(dict_112FW_col_dtypes_5.values())))
### List of date columns:
list_112FW_date_cols_5 = [key for key, value in dict_112FW_col_dtypes_5.items() if value == 'datetime64[ns]'] 



#%%### df_112FW_6: 'Adult UNCOPE Query.xlsx'.
# df = pd.read_excel('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other\\0in\\Y13Q1 (Oct 2023 - Dec 2023)\\Adult UNCOPE Query.xlsx') # can also index sheet by name or fetch all sheets
# list_112FW_col_detail_6 = df.columns.tolist()
# for x in list_112FW_col_detail_6:
#     print("['{}',],".format(x))

list_112FW_col_detail_6 = [

    ['Project ID','string'],
    ['agency','string'],
    ['FamilyNumber','Int64'],
    ['ChildNumber','Int64'],
    ['TGT DOB-CR','datetime64[ns]'],
    ['MinOfHVDate','datetime64[ns]'],
    ['TERMINATION STATUS','string'],
    ['TERMINATION DATE','datetime64[ns]'],
    ['Pregnancystatus','boolean'], ## this is a 0/1 value
    ['MaxOfHVDate','datetime64[ns]'],
    ['MaxOfVISIT NUMBER','Int64'],
    ['Home Visits Prenatal','Int64'],
    ['Home Visits Total','Int64'],
    ['HistoryInterWelfareAdult','boolean'],
    ['MOB SUBSTANCE ABUSE','boolean'],
    ['FOB SUBSTANCE ABUSE','boolean'],
    ['DATEUNCOPE','datetime64[ns]'],
    ['U', 'string'], ##these are yes/no values, so could be boolean?
    ['N','string'],
    ['C','string'],
    ['O','string'],
    ['P','string'],
    ['E','string'],
    ['ReferralDATE','datetime64[ns]'],
    ['Category','string'],


]
#%%### df_112FW_6: 'Adult UNCOPE Query.xlsx'.
dict_112FW_col_dtypes_6 = {x[0]:x[1] for x in list_112FW_col_detail_6}
print(dict_112FW_col_dtypes_6)
print(collections.Counter(list(dict_112FW_col_dtypes_6.values())))
### List of date columns:
list_112FW_date_cols_6 = [key for key, value in dict_112FW_col_dtypes_6.items() if value == 'datetime64[ns]'] 




#%%### df_112FW_7: 'F1 - Home Visit Type Query.xlsx'.
# df = pd.read_excel('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other\\0in\\Y13Q1 (Oct 2023 - Dec 2023)\\F1 - Home Visit Type Query.xlsx') # can also index sheet by name or fetch all sheets
# list_112FW_col_detail_7 = df.columns.tolist()
# for x in list_112FW_col_detail_7:
#     print("['{}',],".format(x))

list_112FW_col_detail_7 = [
    ['Project ID','string'],
    ['agency','string'],
    ['FAMILY NUMBER','Int64'],
    ['HomeVisitTypeIP','Int64'],
    ['HomeVisitTypeAll','Int64'],

]
#%%### df_112FW_7: 'F1 - Home Visit Type Query.xlsx'.
dict_112FW_col_dtypes_7 = {x[0]:x[1] for x in list_112FW_col_detail_7}
print(dict_112FW_col_dtypes_7)
print(collections.Counter(list(dict_112FW_col_dtypes_7.values())))
### List of date columns:
list_112FW_date_cols_7 = [key for key, value in dict_112FW_col_dtypes_7.items() if value == 'datetime64[ns]'] 






#%%### df_112FW_8: 'Referral Exclusions 1 thru 6.xlsx'.
# df = pd.read_excel('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other\\0in\\Y13Q1 (Oct 2023 - Dec 2023)\\Referral Exclusions 1 thru 6.xlsx') # can also index sheet by name or fetch all sheets
# list_112FW_col_detail_8 = df.columns.tolist()
# for x in list_112FW_col_detail_8:
#     print("['{}',],".format(x))

list_112FW_col_detail_8 = [
    ['Project ID','string'],
    ['agency','string'],
    ['FAMILY NUMBER','Int64'],
    ['ChildNumber','Int64'],
    ['need_ex1','string'],
    ['need_ex2','string'],
    ['need_ex3','string'],
    ['need_ex4','string'],
    ['need_ex5','string'],
    ['need_ex6','string'],

]
#%%### df_112FW_8: 'Referral Exclusions 1 thru 6.xlsx'.
dict_112FW_col_dtypes_8 = {x[0]:x[1] for x in list_112FW_col_detail_8}
print(dict_112FW_col_dtypes_8)
print(collections.Counter(list(dict_112FW_col_dtypes_8.values())))
### List of date columns:
list_112FW_date_cols_8 = [key for key, value in dict_112FW_col_dtypes_8.items() if value == 'datetime64[ns]'] 


#%%### df_112FW_9: '14 IPV FROG - use this one'.
# df = pd.read_excel('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other\\0in\\Y13Q2 (Oct 2023 - Mar 2024)\\14 IPV Screen FROG - use this one.xlsx') # can also index sheet by name or fetch all sheets
# list_112FW_col_detail_8 = df.columns.tolist()
# for x in list_112FW_col_detail_8:
#     print("['{}',],".format(x))

list_112FW_col_detail_9 = [
    ['Project ID','string'],
    ['agency','string'],
    ['FN - COUNTY CODE','Int64'],
    ['FN - PROGRAM WITHIN COUNTY', 'Int64'],
    ['FAMILY NUMBER','Int64'],
    ['A1IPV','boolean'],
    ['A1Police','boolean'],
    ['A1Afraid','boolean'],
    ['MOB ASSESSMENT DATE','datetime64[ns]'],

]

#%%### df_112FW_9: '14 IPV FROG - use this one'.
dict_112FW_col_dtypes_9 = {x[0]:x[1] for x in list_112FW_col_detail_9}
print(dict_112FW_col_dtypes_9)
print(collections.Counter(list(dict_112FW_col_dtypes_9.values())))
### List of date columns:
list_112FW_date_cols_9 = [key for key, value in dict_112FW_col_dtypes_9.items() if value == 'datetime64[ns]'] 


#%%##############################################!>>>
### >>> READ 
#####################################################

#%%
### Performance benefit for reading in file to memory only once by creating an ExcelFile class object.
xlsx_112FW_input_well_child = pd.ExcelFile(path_112FW_input_well_child)
xlsx_112FW_input_child_injury = pd.ExcelFile(path_112FW_input_child_injury)
xlsx_112FW_input_cg_ins = pd.ExcelFile(path_112FW_input_cg_ins)
xlsx_112FW_input_adult_act = pd.ExcelFile(path_112FW_input_adult_act)
xlsx_112FW_input_child_act = pd.ExcelFile(path_112FW_input_child_act)
xlsx_112FW_input_adult_uncope = pd.ExcelFile(path_112FW_input_adult_uncope)
xlsx_112FW_input_home_visit = pd.ExcelFile(path_112FW_input_home_visit)
xlsx_112FW_input_ref_excl = pd.ExcelFile(path_112FW_input_ref_excl)
xlsx_112FW_input_frog = pd.ExcelFile(path_112FW_input_frog)


#%%###################################
### READ in all sheets as strings.

### Read in EVERYTHING as a string WITH pd.NA for empty cells:
df_112FW_allstring_1 = pd.read_excel(xlsx_112FW_input_well_child, keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_112FW_col_dtypes_1)
df_112FW_allstring_2 = pd.read_excel(xlsx_112FW_input_child_injury, keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_112FW_col_dtypes_2)
df_112FW_allstring_3 = pd.read_excel(xlsx_112FW_input_cg_ins, keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_112FW_col_dtypes_3)
df_112FW_allstring_4 = pd.read_excel(xlsx_112FW_input_adult_act, keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_112FW_col_dtypes_4)
df_112FW_allstring_5 = pd.read_excel(xlsx_112FW_input_child_act, keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_112FW_col_dtypes_5)
df_112FW_allstring_6 = pd.read_excel(xlsx_112FW_input_adult_uncope, keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_112FW_col_dtypes_6)
df_112FW_allstring_7 = pd.read_excel(xlsx_112FW_input_home_visit, keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_112FW_col_dtypes_7)
df_112FW_allstring_8 = pd.read_excel(xlsx_112FW_input_ref_excl, keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_112FW_col_dtypes_8)
df_112FW_allstring_9 = pd.read_excel(xlsx_112FW_input_frog, keep_default_na=False, na_values=list_na_values_to_read, dtype='string')# dtype=dict_112FW_col_dtypes_9)
#%%##############################################!>>>
### >>> CLEAN 
#####################################################



#%%###################################
### >>> df_112FW_5: 'Child Activity Export'.

#%%###################################
### >>> df_112FW_5: 'Child Activity Export'.
df_112FW_child_act = df_112FW_allstring_5.copy() 
### >>> df_112FW_4: 'Adult Activity Export'.
df_112FW_adult_act=df_112FW_allstring_4.copy()
### >>> df_112FW_8: 'Referral Exclusions 1 thru 6'.
df_112FW_ref_excl=df_112FW_allstring_8.copy()
### >>> df_112FW_6: 'Adult UNCOPE Query.xlsx'.
df_112FW_adult_uncope=df_112FW_allstring_6.copy()
### >>> df_112FW_7: 'F1 - Home Visit Type Query.xlsx'.
df_112FW_home_visit=df_112FW_allstring_7.copy()
### >>> df_112FW_1: 'F1 - Home Visit Type Query.xlsx'.
df_112FW_well_child=df_112FW_allstring_1.copy()
### >>> df_112FW_2: 'F1 - Home Visit Type Query.xlsx'.
df_112FW_child_injury=df_112FW_allstring_2.copy()
### >>> df_112FW_3: '08 Child ER Injury.xlsx'.
df_112FW_cg_ins=df_112FW_allstring_3.copy()
### >>> df_112FW_9: '14 IPV FROG - use this one'.
df_112FW_adult_frog=df_112FW_allstring_9.copy()


#%%### 1. Strip surrounding whitespace
df_112FW_child_act = (
    df_112FW_child_act.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)
df_112FW_adult_act = (
    df_112FW_adult_act.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)
df_112FW_ref_excl = (
    df_112FW_ref_excl.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)
df_112FW_adult_uncope = (
    df_112FW_adult_uncope.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)
df_112FW_home_visit = (
    df_112FW_home_visit.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)
df_112FW_well_child = (
    df_112FW_well_child.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)
df_112FW_child_injury = (
    df_112FW_child_injury.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)
df_112FW_cg_ins = (
    df_112FW_cg_ins.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)
df_112FW_adult_frog = (
    df_112FW_adult_frog.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)


#%%### 2. Find & replace "null" values  
list_112FW_values_to_find_and_replace= ['null']
df_112FW_child_act= (
df_112FW_child_act.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_112FW_values_to_find_and_replace, pd.NA)
)
df_112FW_adult_act= (
df_112FW_adult_act.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_112FW_values_to_find_and_replace, pd.NA)
)
df_112FW_ref_excl= (
df_112FW_ref_excl.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_112FW_values_to_find_and_replace, pd.NA)
)
df_112FW_adult_uncope= (
df_112FW_adult_uncope.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_112FW_values_to_find_and_replace, pd.NA)
)
df_112FW_home_visit= (
df_112FW_home_visit.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_112FW_values_to_find_and_replace, pd.NA)
)
df_112FW_well_child= (
df_112FW_well_child.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_112FW_values_to_find_and_replace, pd.NA)
)
df_112FW_child_injury= (
df_112FW_child_injury.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_112FW_values_to_find_and_replace, pd.NA)
)
df_112FW_cg_ins= (
df_112FW_cg_ins.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_112FW_values_to_find_and_replace, pd.NA)
)
df_112FW_adult_frog= (
df_112FW_adult_frog.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_112FW_values_to_find_and_replace, pd.NA)
)


#%%### 3. Add nanoseconds to strings of datetimes missing them   
regex_112FW_dates_to_fix = r'(^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$)' 
regex_112FW_dates_replacement = r'\1.000000000' ### 9 zeros for nanoseconds! 

df_112FW_child_act = (
    df_112FW_child_act
    .replace({col:regex_112FW_dates_to_fix for col in list_112FW_date_cols_5}, regex_112FW_dates_replacement, regex=True) ### Checking all date columns.
)
df_112FW_adult_act = (
    df_112FW_adult_act
    .replace({col:regex_112FW_dates_to_fix for col in list_112FW_date_cols_4}, regex_112FW_dates_replacement, regex=True) ### Checking all date columns.
)
df_112FW_ref_excl = (
    df_112FW_ref_excl
    .replace({col:regex_112FW_dates_to_fix for col in list_112FW_date_cols_8}, regex_112FW_dates_replacement, regex=True) ### Checking all date columns.
)
df_112FW_adult_uncope = (
    df_112FW_adult_uncope
    .replace({col:regex_112FW_dates_to_fix for col in list_112FW_date_cols_6}, regex_112FW_dates_replacement, regex=True) ### Checking all date columns.
)
df_112FW_home_visit = (
    df_112FW_home_visit
    .replace({col:regex_112FW_dates_to_fix for col in list_112FW_date_cols_7}, regex_112FW_dates_replacement, regex=True) ### Checking all date columns.
)
df_112FW_well_child = (
    df_112FW_well_child
    .replace({col:regex_112FW_dates_to_fix for col in list_112FW_date_cols_1}, regex_112FW_dates_replacement, regex=True) ### Checking all date columns.
)
df_112FW_child_injury = (
    df_112FW_child_injury
    .replace({col:regex_112FW_dates_to_fix for col in list_112FW_date_cols_2}, regex_112FW_dates_replacement, regex=True) ### Checking all date columns.
)
df_112FW_cg_ins = (
    df_112FW_cg_ins
    .replace({col:regex_112FW_dates_to_fix for col in list_112FW_date_cols_3}, regex_112FW_dates_replacement, regex=True) ### Checking all date columns.
)
df_112FW_adult_frog = (
    df_112FW_adult_frog
    .replace({col:regex_112FW_dates_to_fix for col in list_112FW_date_cols_9}, regex_112FW_dates_replacement, regex=True) ### Checking all date columns.
)


#%%### 4. Set data types  
df_112FW_child_act = (
    df_112FW_child_act
    .pipe(fn_apply_dtypes, dict_112FW_col_dtypes_5) ### Checking all date columns.
)
df_112FW_adult_act = (
    df_112FW_adult_act
    .pipe(fn_apply_dtypes, dict_112FW_col_dtypes_4) ### Checking all date columns.
)

df_112FW_ref_excl = (
    df_112FW_ref_excl
    .pipe(fn_apply_dtypes, dict_112FW_col_dtypes_8) ### Checking all date columns.
)

df_112FW_adult_uncope = (
    df_112FW_adult_uncope
    .pipe(fn_apply_dtypes, dict_112FW_col_dtypes_6) ### Checking all date columns.
)

df_112FW_home_visit = (
    df_112FW_home_visit
    .pipe(fn_apply_dtypes, dict_112FW_col_dtypes_7) ### Checking all date columns.
)

df_112FW_well_child = (
    df_112FW_well_child
    .pipe(fn_apply_dtypes, dict_112FW_col_dtypes_1) ### Checking all date columns.
)

df_112FW_child_injury = (
    df_112FW_child_injury
    .pipe(fn_apply_dtypes, dict_112FW_col_dtypes_2) ### Checking all date columns.
)

df_112FW_cg_ins = (
    df_112FW_cg_ins
    .pipe(fn_apply_dtypes, dict_112FW_col_dtypes_3) ### Checking all date columns.
)

df_112FW_adult_frog = (
    df_112FW_adult_frog
    .pipe(fn_apply_dtypes, dict_112FW_col_dtypes_9) ### Checking all date columns.
)

#%%##############################################!>>>
### >>> RESTRUCTURING  
#####################################################

######################################
### >>> df_112FW_5: 'Child Activity Export'.
## 

#%%### 1. Remove any rows with a discharge date (TERMINATION DATE) before the current reporting year 
df_112FW_child_act = df_112FW_child_act[(df_112FW_child_act['TERMINATION DATE'] >= pd.Timestamp(f'2023-10-01')) | (df_112FW_child_act['TERMINATION DATE'].isna())]
df_112FW_child_act['TERMINATION DATE'].astype('datetime64[ns]')
df_112FW_child_act.columns
#%%### 2. Remove rows that do not have a first home visit date
df_112FW_child_act=df_112FW_child_act.dropna(subset=['MinOfHVDate'],inplace=False)
#%%### 3. Remove any rows with a MaxofHVDate before the current report year 
df_112FW_before_child_act=df_112FW_child_act.copy()

df_112FW_child_act = df_112FW_child_act[df_112FW_child_act['MaxOfHVDate'] >= pd.Timestamp(f'2023-10-01')]
df_112FW_child_act['MaxOfHVDate'].astype('datetime64[ns]')

# if (df_112FW_before_child_act.equals(df_112FW_after_child_act)):
#     print('error, columns unchanged')
# else:
#     df_112FW_child_act=df_112FW_after_child_act.copy()
df_112FW_child_act.columns

######################################
### >>> df_112FW_5: 'Adult Activity Export'.
##

#%%### 1. Remove any rows with a discharge date (TERMINATION DATE) before the current reporting year  
df_112FW_adult_act = df_112FW_adult_act[(df_112FW_adult_act['TERMINATION DATE'] >= pd.Timestamp(f'2023-10-01')) | (df_112FW_adult_act['TERMINATION DATE'].isna())]

df_112FW_adult_act['TERMINATION DATE'].astype('datetime64[ns]')
#%%### 2. Remove rows that do not have a first home visit date OR Max Visit Number is 0 or blank 
df_112FW_adult_act = (
    df_112FW_adult_act.dropna(subset=['MinOfHVDate'], inplace=False)
)
df_112FW_adult_act = (
    df_112FW_adult_act.dropna(subset=['MaxOfVISIT NUMBER'], inplace=False)
)
df_112FW_adult_act= (
    df_112FW_adult_act[df_112FW_adult_act['MaxOfVISIT NUMBER'] !=0]
)
df_112FW_adult_act['MaxOfVISIT NUMBER']
#%%### 3. Remove any rows with a MaxofHVDate before the current report year 
df_112FW_adult_act = df_112FW_adult_act[df_112FW_adult_act['MaxOfHVDate'] >= pd.Timestamp(f'2023-10-01')]
df_112FW_adult_act['MaxOfHVDate'].astype('datetime64[ns]')



######################################
### >>> 'Referral Exclusions 1 thru 6' VLOOKUP with 'Child Activity Export'.
#%%### 1. - Use VLOOKUP to pull exclusion 4 into the Child Activity spreadsheet

# Merge the DataFrames based on the common column ('Project ID')
df_112FW_ref_excl_columns_c = df_112FW_ref_excl[['Project ID','need_ex4']]
df_112FW_child_act = pd.merge(df_112FW_child_act, df_112FW_ref_excl_columns_c, on=['Project ID'], how='left')
df_112FW_child_act
df_112FW_child_act.columns

#%%### 2. -  Use VLOOKUP on Child Activity for ZIP from Adult Activity 

# Merge the DataFrames based on the common column ('Project ID')
df_112FW_adult_columns = df_112FW_adult_act[['Project ID','MOB ZIP']]
df_112FW_child_act = pd.merge(df_112FW_child_act, df_112FW_adult_columns, on=['Project ID'], how='left')
df_112FW_child_act
df_112FW_child_act.columns

df_112FW_child_act=df_112FW_child_act.drop_duplicates()

######################################
### >>> 'Referral Exclusions 1 thru 6' VLOOKUP with 'Adult Activity Export'.
#%%### 1. - Use VLOOKUP to pull exclusions 1, 2, 3, 5 , and 6 into the Adult Activity spreadsheet (Note: Exclusion 4 is on the Child Activity export)

# Merge the DataFrames based on the common column ('Project ID')
df_112FW_ref_excl_columns_a = df_112FW_ref_excl[['Project ID','need_ex1','need_ex2', 'need_ex3', 'need_ex5', 'need_ex6']]
df_112FW_adult_act= pd.merge(df_112FW_adult_act, df_112FW_ref_excl_columns_a, on=['Project ID'], how='left')
print(df_112FW_adult_act.columns)
df_112FW_adult_act


######################################
### >>> 'Adult UNCOPE Query' Inclusion.
#%%### 1. - Take UNCOPE columns and insert into Adult Activities
df_112FW_adult_uncope_columns = df_112FW_adult_uncope[['Project ID','DATEUNCOPE', 'U', 'N', 'C', 'O', 'P', 'E', 'ReferralDATE', 'Category']]
df_112FW_adult_act = pd.merge(df_112FW_adult_act, df_112FW_adult_uncope_columns, on=['Project ID'], how='left')
df_112FW_adult_act


######################################
### >>> '14 IPV Screen FROG - use this one.xlsx' Inclusion.
#%%### 1. - Take FROG columns and insert into Adult Activities
df_112FW_adult_frog_columns = df_112FW_adult_frog[['Project ID','A1IPV', 'A1Police', 'A1Afraid', 'MOB ASSESSMENT DATE']]
df_112FW_adult_act = pd.merge(df_112FW_adult_act, df_112FW_adult_frog_columns, on=['Project ID'], how='left')
df_112FW_adult_act

######################################
### >>> Virtual Visits' Inclusion.
#%%### 1. - Take Virtual Visit columns and insert into Adult Activities
df_112FW_home_visit_columns = df_112FW_home_visit[['Project ID','HomeVisitTypeIP', 'HomeVisitTypeAll']]
df_112FW_adult_act = pd.merge(df_112FW_adult_act, df_112FW_home_visit_columns, on=['Project ID'], how='left')
df_112FW_adult_act
#%%### 2. - Subtract new columns in adult to create column ‘HomeVisitTypeV’
df_112FW_adult_act['HomeVisitTypeV'] = df_112FW_adult_act['HomeVisitTypeAll'] - df_112FW_adult_act['HomeVisitTypeIP']
df_112FW_adult_act

df_112FW_adult_act=df_112FW_adult_act.drop_duplicates()

### TODO: put in documentation:
### tgt = child
### mob = primary caregiver
### fob = secondary caregiver
### Expectation of target children: only first child (unless multiples); secondary children not tracked.

#%%###################################
### <> Inspect DFs

#%%
inspect_df(df_112FW_child_act)
#%%
inspect_df(df_112FW_adult_act)
#%%
inspect_df(df_112FW_child_injury)

#%%##############################################!>>>
### >>> RESTRUCTURING  
#####################################################

### Compare to files here: 
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\previous\before restructure\Y13Q1 (Oct 2023 - Dec 2023) 
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.3combine\previous\after restructuring\Y13Q1 (Oct 2023 - Dec 2023) 

#%%###################################
### <> 08 Child ER Injury.xlsx

### Add funding column:
df_112FW_child_injury['funding']='addedlater'
df_112FW_child_injury=df_112FW_child_injury.query('IncidentDate >= @date_fy_start')
### Pivot the DataFrame:
df_112FW_pivoted_child_injury = df_112FW_child_injury.pivot_table(
    index=['Project ID', 'agency', 'FAMILY NUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_112FW_child_injury.groupby(['Project ID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['ERVisitReason', 'IncidentDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
df_112FW_pivoted_child_injury

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_112FW_pivoted_child_injury = df_112FW_pivoted_child_injury.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
df_112FW_pivoted_child_injury

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_112FW_pivoted_child_injury.columns = [f'{col[0]}.{col[1]}' for col in df_112FW_pivoted_child_injury.columns]
df_112FW_pivoted_child_injury

#%%
### Reset row & column indices:
df_112FW_pivoted_child_injury = df_112FW_pivoted_child_injury.reset_index()
df_112FW_pivoted_child_injury

#%%###################################
### <> Caregiver Insurance v2 - USE THIS ONE.xlsx

### Add funding column:
df_112FW_cg_ins['funding']='addedlater'
### Pivot the DataFrame:
df_112FW_pivoted_cg_ins = df_112FW_cg_ins.pivot_table(
    index=['Project ID', 'agency', 'FAMILY NUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_112FW_cg_ins.groupby(['Project ID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['AD1PrimaryIns', 'AD1InsChangeDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
df_112FW_pivoted_cg_ins

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_112FW_pivoted_cg_ins = df_112FW_pivoted_cg_ins.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
df_112FW_pivoted_cg_ins

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_112FW_pivoted_cg_ins.columns = [f'{col[0]}.{col[1]}' for col in df_112FW_pivoted_cg_ins.columns]
df_112FW_pivoted_cg_ins

#%%
### Reset row & column indices:
df_112FW_pivoted_cg_ins = df_112FW_pivoted_cg_ins.reset_index()
df_112FW_pivoted_cg_ins

#%%###################################
### <> WellChildVisits 

### Add funding column:
df_112FW_well_child['funding']='addedlater'
df_112FW_well_child= df_112FW_well_child[df_112FW_well_child['WellVisitDate'] >= pd.Timestamp(f'2017-10-01')]
### Pivot the DataFrame:
df_112FW_pivoted_well_child = df_112FW_well_child.pivot_table(
    index=['Project ID', 'agency','FAMILY NUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_112FW_well_child.groupby(['Project ID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['WellVisitDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
# df_112FW_pivoted_well_child

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_112FW_pivoted_well_child = df_112FW_pivoted_well_child.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
# df_112FW_pivoted_well_child

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_112FW_pivoted_well_child.columns = [f'{col[0]}.{col[1]}' for col in df_112FW_pivoted_well_child.columns]
# df_112FW_pivoted_well_child

#%%
### Reset row & column indices:
df_112FW_pivoted_well_child = df_112FW_pivoted_well_child.reset_index()
# df_112FW_pivoted_well_child

#%%
###: Create year & quarter columns.
#int_nehv_quarter = 2
#str_nehv_quarter = 'Y13Q2 (Oct 2023 - Mar 2024)'

df_112FW_child_act.insert(loc=1, column='year', value=int_nehv_year)
df_112FW_child_act.insert(loc=2, column='quarter', value=int_nehv_quarter)

df_112FW_adult_act.insert(loc=1, column='year', value=int_nehv_year)
df_112FW_adult_act.insert(loc=2, column='quarter', value=int_nehv_quarter)

df_112FW_pivoted_well_child.insert(loc=1, column='year', value=int_nehv_year)
df_112FW_pivoted_well_child.insert(loc=2, column='quarter', value=int_nehv_quarter)

df_112FW_pivoted_child_injury.insert(loc=1, column='year', value=int_nehv_year)
df_112FW_pivoted_child_injury.insert(loc=2, column='quarter', value=int_nehv_quarter)

df_112FW_pivoted_cg_ins.insert(loc=1, column='year', value=int_nehv_year)
df_112FW_pivoted_cg_ins.insert(loc=2, column='quarter', value=int_nehv_quarter)


#%%##############################################!>>>
### >>> WRITE OUT FILES   
#####################################################

### Note: Date columns written out without timestamps.

df_112FW_adult_act.to_csv(Path(path_112FW_dir_output, 'df_112FW_adult_act.csv'), index = False, date_format="%m/%d/%Y")
df_112FW_child_act.to_csv(Path(path_112FW_dir_output, 'df_112FW_child_act.csv'), index = False, date_format="%m/%d/%Y")
df_112FW_pivoted_child_injury.to_csv(Path(path_112FW_dir_output, 'df_112FW_pivoted_child_injury.csv'), index = False, date_format="%m/%d/%Y")
df_112FW_pivoted_cg_ins.to_csv(Path(path_112FW_dir_output, 'df_112FW_pivoted_cg_ins.csv'), index = False, date_format="%m/%d/%Y")
df_112FW_pivoted_well_child.to_csv(Path(path_112FW_dir_output, 'df_112FW_pivoted_well_child.csv'), index = False, date_format="%m/%d/%Y")

#%%##############################################!>>>
### >>> Remove old objects  
#####################################################

#%%
[o for o in list(globals().keys()) if o.startswith(('date', 'int', 'path', 'str'))]
### Keep.

#%%
# [o for o in list(globals().keys()) if o.startswith('df')]
#%%
del df_112FW_allstring_1, df_112FW_allstring_2, df_112FW_allstring_3, df_112FW_allstring_4

#%%
# [o for o in list(globals().keys()) if o.startswith('dict')]
#%%
del dict_112FW_col_dtypes_1, dict_112FW_col_dtypes_2, dict_112FW_col_dtypes_3, dict_112FW_col_dtypes_4 

#%%
# [o for o in list(globals().keys()) if o.startswith('list')]
#%%
del list_112FW_col_detail_1, list_112FW_date_cols_1, list_112FW_col_detail_2, list_112FW_date_cols_2, list_112FW_col_detail_3, list_112FW_date_cols_3, list_112FW_col_detail_4, list_112FW_date_cols_4, list_112FW_values_to_find_and_replace 

#%%
# [o for o in list(globals().keys()) if o.startswith(('regex', 'xlsx'))]
#%%
del  regex_112FW_dates_to_fix, regex_112FW_dates_replacement 

#%%
### Is what's left over what is wanted?:
[o for o in list(globals().keys()) if o.startswith(('df', 'dict', 'list', 'regex', 'xlsx'))]
### Should only be:
# ['df_112FW_child_act',
#  'df_112FW_pivoted_child_injury',
#  'df_112FW_pivoted_cg_ins',
#  'df_112FW_pivoted_well_child']



#%%##############################################!>>>
### >>> END 
#################################################!>>>

print('Congrats! You ran 1.1.2 FW!')

# %%
### TODO:
### Remove duplciate rows (like how tried in 1.4 code).
### see if "logger" package would be useful.




