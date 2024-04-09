



### Process for receiving data from Lincoln Lancaster County Health Department (LL):
    # 1. Original files transferred from external vendor (LL) to NE State, who then puts the files in folder "U:\SFTP".
    # 2. Then we organize the files in folder "Master (Files Used For Quarterly Reports)".
    # 3. Then we copy the files into: "U:\Working" >> "Tableau" >> year >> quarter >> "LL" folder (for example: "U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Y12Q4 (Oct 2022 - Sep 2023)\LLCHD").


### Copying over file from: "U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Y12Q4 (Oct 2022 - Sep 2023)\LLCHD":

### To here: "U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\0in\Y12Q4 (Oct 2022 - Sep 2023)"






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
if (os.path.basename(__file__) == '_1_1FW_raw_to_master.py'):
    from _1_1FW_RUNME import * 
    print('Imported "_1_1_FW_RUNME"')
else:
    print("Did NOT run RUNME again... because it's already running!")


#%%##############################################!>>>
### >>> COLUMN DEFINITIONS 
#####################################################

#%%### df_11FW_1: '04 Well Child v2 no MAX - use this one.xlsx'.
#%%### df_11FW_2: '08 Child ER Injury.xlsx'.
#%%### df_11FW_3: '16 - Caregiver Insurance v2 - USE THIS ONE.xlsx'.
#%%### df_11FW_4: 'Adult Activities Query.xlsx'.
#%%### df_11FW_5: 'Child Activities Query.xlsx'.
#%%### df_11FW_6: 'Adult UNCOPE Query.xlsx'.
#%%### df_11FW_7: 'F1 - Home Visit Type Query.xlsx'.
#%%### df_11FW_8: 'Referral Exclusions 1 thru 6.xlsx'.
#%%###
pd.set_option('display.max_columns', None)

#######################

### List = [name, dtype]

#######################
#%%### df_11FW_1: '04 Well Child v2 no MAX - use this one.xlsx'.
list_11FW_col_detail_1 = [

    ['Project ID', 'string']
    ,['agency', 'string']
    ,['FAMILY NUMBER', 'Int64']
    ,['ChildNumber', 'Int64'] 
    ,['WellVisitDate', 'datetime64[ns]']
]
#%%### df_11FW_1: '04 Well Child v2 no MAX - use this one.xlsx'.
dict_11FW_col_dtypes_1 = {x[0]:x[1] for x in list_11FW_col_detail_1}
print(dict_11FW_col_dtypes_1)
print(collections.Counter(list(dict_11FW_col_dtypes_1.values())))
### List of date columns:
list_11FW_date_cols_1 = [key for key, value in dict_11FW_col_dtypes_1.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_11FW_2: '08 Child ER Injury.xlsx'.

list_11FW_col_detail_2 = [
    ['Project ID', 'string']
    ,['agency', 'string'] ### Could be 'Int64'; however, ids left as strings.
    ,['FAMILY NUMBER', 'Int64']
    ,['ChildNumber', 'Int64']
    ,['ERVisitReason', 'string']
    ,['IncidentDate', 'datetime64[ns]']
]
#%%### df_11FW_2: '08 Child ER Injury.xlsx'.
dict_11FW_col_dtypes_2 = {x[0]:x[1] for x in list_11FW_col_detail_2}
print(dict_11FW_col_dtypes_2)
print(collections.Counter(list(dict_11FW_col_dtypes_2.values())))
### List of date columns:
list_11FW_date_cols_2 = [key for key, value in dict_11FW_col_dtypes_2.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_11FW_3: '16 - Caregiver Insurance v2 - USE THIS ONE.xlsx'.

list_11FW_col_detail_3 = [
    ['Project ID', 'string']
    ,['agency', 'string'] 
    ,['FAMILY NUMBER', 'Int64']
    ,['ChildNumber', 'Int64'] 
    ,['AD1InsChange', 'Int64']
    ,['AD1InsChangeDate', 'datetime64[ns]']
    ,['AD1PrimaryIns', 'string']
]
#%%### df_11FW_3: '16 - Caregiver Insurance v2 - USE THIS ONE.xlsx'.
dict_11FW_col_dtypes_3 = {x[0]:x[1] for x in list_11FW_col_detail_3}
print(dict_11FW_col_dtypes_3)
print(collections.Counter(list(dict_11FW_col_dtypes_3.values())))
### List of date columns:
list_11FW_date_cols_3 = [key for key, value in dict_11FW_col_dtypes_3.items() if value == 'datetime64[ns]'] 

#######################
#%%### df_11FW_4: 'Adult Activities Query.xlsx'.
import pandas as pd
df = pd.read_excel('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other\\0in\\Y13Q1 (Oct 2023 - Dec 2023)\\Adult Activities Query.xlsx') # can also index sheet by name or fetch all sheets
list_11FW_col_detail_4 = df.columns.tolist()
for x in list_11FW_col_detail_4:
    print("['{}',],".format(x))

list_11FW_col_detail_4= [

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
    ['MOB ZIP','string'],
    ['Adaptation','string'], ##not sure about this one, all values seem to be 'NA'
    
    ]



#%%### df_11FW_4: Adult Activities Query.xlsx'.
dict_11FW_col_dtypes_4 = {x[0]:x[1] for x in list_11FW_col_detail_4}
print(dict_11FW_col_dtypes_4)
print(collections.Counter(list(dict_11FW_col_dtypes_4.values())))
### List of date columns:
list_11FW_date_cols_4 = [key for key, value in dict_11FW_col_dtypes_4.items() if value == 'datetime64[ns]'] 


#%%### df_11FW_5: 'Child Activities Query.xlsx'.
import pandas as pd
df = pd.read_excel('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other\\0in\\Y13Q1 (Oct 2023 - Dec 2023)\\Child Activities Query.xlsx') # can also index sheet by name or fetch all sheets
list_11FW_col_detail_5 = df.columns.tolist()
print(len(list_11FW_col_detail_5))
for x in list_11FW_col_detail_5:
    print("['{}',],".format(x))
list_11FW_col_detail_5 = [
    
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
#%%### df_11FW_5: 'Child Activities Query.xlsx'.
print(len(list_11FW_col_detail_5))
for x in list_11FW_col_detail_5:
    print(x[0],x[1])
dict_11FW_col_dtypes_5 = {x[0]:x[1] for x in list_11FW_col_detail_5}
print(dict_11FW_col_dtypes_5)
print(collections.Counter(list(dict_11FW_col_dtypes_5.values())))
### List of date columns:
list_11FW_date_cols_5 = [key for key, value in dict_11FW_col_dtypes_5.items() if value == 'datetime64[ns]'] 



#%%### df_11FW_6: 'Adult UNCOPE Query.xlsx'.
import pandas as pd
df = pd.read_excel('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other\\0in\\Y13Q1 (Oct 2023 - Dec 2023)\\Adult UNCOPE Query.xlsx') # can also index sheet by name or fetch all sheets
list_11FW_col_detail_6 = df.columns.tolist()
for x in list_11FW_col_detail_6:
    print("['{}',],".format(x))

list_11FW_col_detail_6 = [

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
#%%### df_11FW_6: 'Adult UNCOPE Query.xlsx'.
dict_11FW_col_dtypes_6 = {x[0]:x[1] for x in list_11FW_col_detail_6}
print(dict_11FW_col_dtypes_6)
print(collections.Counter(list(dict_11FW_col_dtypes_6.values())))
### List of date columns:
list_11FW_date_cols_6 = [key for key, value in dict_11FW_col_dtypes_6.items() if value == 'datetime64[ns]'] 




#%%### df_11FW_7: 'F1 - Home Visit Type Query.xlsx'.
import pandas as pd
df = pd.read_excel('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other\\0in\\Y13Q1 (Oct 2023 - Dec 2023)\\F1 - Home Visit Type Query.xlsx') # can also index sheet by name or fetch all sheets
list_11FW_col_detail_7 = df.columns.tolist()
for x in list_11FW_col_detail_7:
    print("['{}',],".format(x))

list_11FW_col_detail_7 = [
    ['Project ID','string'],
    ['agency','string'],
    ['FAMILY NUMBER','Int64'],
    ['HomeVisitTypeIP','Int64'],
    ['HomeVisitTypeAll','Int64'],

]
#%%### df_11FW_7: 'F1 - Home Visit Type Query.xlsx'.
dict_11FW_col_dtypes_7 = {x[0]:x[1] for x in list_11FW_col_detail_7}
print(dict_11FW_col_dtypes_7)
print(collections.Counter(list(dict_11FW_col_dtypes_7.values())))
### List of date columns:
list_11FW_date_cols_7 = [key for key, value in dict_11FW_col_dtypes_7.items() if value == 'datetime64[ns]'] 






#%%### df_11FW_8: 'Referral Exclusions 1 thru 6.xlsx'.
import pandas as pd
df = pd.read_excel('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.2other\\0in\\Y13Q1 (Oct 2023 - Dec 2023)\\Referral Exclusions 1 thru 6.xlsx') # can also index sheet by name or fetch all sheets
list_11FW_col_detail_8 = df.columns.tolist()
for x in list_11FW_col_detail_8:
    print("['{}',],".format(x))

list_11FW_col_detail_8 = [
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
#%%### df_11FW_8: 'Referral Exclusions 1 thru 6.xlsx'.
dict_11FW_col_dtypes_8 = {x[0]:x[1] for x in list_11FW_col_detail_8}
print(dict_11FW_col_dtypes_8)
print(collections.Counter(list(dict_11FW_col_dtypes_8.values())))
### List of date columns:
list_11FW_date_cols_8 = [key for key, value in dict_11FW_col_dtypes_8.items() if value == 'datetime64[ns]'] 

#%%##############################################!>>>
### >>> READ 
#####################################################

#%%
### Performance benefit for reading in file to memory only once by creating an ExcelFile class object.
xlsx_11FW_input_well_child = pd.ExcelFile(path_11FW_input_well_child)
xlsx_11FW_input_child_injury = pd.ExcelFile(path_11FW_input_child_injury)
xlsx_11FW_input_cg_ins = pd.ExcelFile(path_11FW_input_cg_ins)
xlsx_11FW_input_adult_act = pd.ExcelFile(path_11FW_input_adult_act)
xlsx_11FW_input_child_act = pd.ExcelFile(path_11FW_input_child_act)
xlsx_11FW_input_adult_uncope = pd.ExcelFile(path_11FW_input_adult_uncope)
xlsx_11FW_input_home_visit = pd.ExcelFile(path_11FW_input_home_visit)
xlsx_11FW_input_ref_excl = pd.ExcelFile(path_11FW_input_ref_excl)


#%%###################################
### READ in all sheets as strings.

### Read in EVERYTHING as a string WITH pd.NA for empty cells:
df_11FW_allstring_1 = pd.read_excel(xlsx_11FW_input_well_child, keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_11FW_col_dtypes_1)
df_11FW_allstring_2 = pd.read_excel(xlsx_11FW_input_child_injury, keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_11FW_col_dtypes_2)
df_11FW_allstring_3 = pd.read_excel(xlsx_11FW_input_cg_ins, keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_11FW_col_dtypes_3)
df_11FW_allstring_4 = pd.read_excel(xlsx_11FW_input_adult_act, keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_11FW_col_dtypes_4)
df_11FW_allstring_5 = pd.read_excel(xlsx_11FW_input_child_act, keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_11FW_col_dtypes_5)
df_11FW_allstring_6 = pd.read_excel(xlsx_11FW_input_adult_uncope, keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_11FW_col_dtypes_6)
df_11FW_allstring_7 = pd.read_excel(xlsx_11FW_input_home_visit, keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_11FW_col_dtypes_7)
df_11FW_allstring_8 = pd.read_excel(xlsx_11FW_input_ref_excl, keep_default_na=False, na_values=[''], dtype='string')# dtype=dict_11FW_col_dtypes_8)
#%%##############################################!>>>
### >>> CLEAN 
#####################################################



#%%###################################
### >>> df_11FW_5: 'Child Activity Export'.

#%%###################################
### >>> df_11FW_5: 'Child Activity Export'.
df_11FW_child_act = df_11FW_allstring_5.copy() 
### >>> df_11FW_4: 'Adult Activity Export'.
df_11FW_adult_act=df_11FW_allstring_4.copy()
### >>> df_11FW_8: 'Referral Exclusions 1 thru 6'.
df_11FW_ref_excl=df_11FW_allstring_8.copy()
### >>> df_11FW_6: 'Adult UNCOPE Query.xlsx'.
df_11FW_adult_uncope=df_11FW_allstring_6.copy()

#%%### 1. Strip surrounding whitespace
df_11FW_child_act = (
    df_11FW_child_act.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)
df_11FW_adult_act = (
    df_11FW_adult_act.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)
df_11FW_ref_excl = (
    df_11FW_ref_excl.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)
df_11FW_adult_uncope = (
    df_11FW_adult_uncope.map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)

#%%### 2. Find & replace "null" values  
list_11FW_values_to_find_and_replace= ['null']
df_11FW_child_act= (
df_11FW_child_act.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_11FW_values_to_find_and_replace, pd.NA)
)
df_11FW_adult_act= (
df_11FW_adult_act.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_11FW_values_to_find_and_replace, pd.NA)
)
df_11FW_ref_excl= (
df_11FW_ref_excl.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_11FW_values_to_find_and_replace, pd.NA)
)
df_11FW_adult_uncope= (
df_11FW_adult_uncope.pipe(fn_find_and_replace_value_in_df, 'Project ID', list_11FW_values_to_find_and_replace, pd.NA)
)


#%%### 3. Add nanoseconds to strings of datetimes missing them   
regex_11FW_dates_to_fix = r'(^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$)' 
regex_11FW_dates_replacement = r'\1.000000000' ### 9 zeros for nanoseconds! 

df_11FW_child_act = (
    df_11FW_child_act
    .replace({col:regex_11FW_dates_to_fix for col in list_11FW_date_cols_5}, regex_11FW_dates_replacement, regex=True) ### Checking all date columns.
)
df_11FW_adult_act = (
    df_11FW_adult_act
    .replace({col:regex_11FW_dates_to_fix for col in list_11FW_date_cols_4}, regex_11FW_dates_replacement, regex=True) ### Checking all date columns.
)
df_11FW_ref_excl = (
    df_11FW_ref_excl
    .replace({col:regex_11FW_dates_to_fix for col in list_11FW_date_cols_8}, regex_11FW_dates_replacement, regex=True) ### Checking all date columns.
)
df_11FW_adult_uncope = (
    df_11FW_adult_uncope
    .replace({col:regex_11FW_dates_to_fix for col in list_11FW_date_cols_6}, regex_11FW_dates_replacement, regex=True) ### Checking all date columns.
)


#%%### 4. Set data types  
df_11FW_child_act = (
    df_11FW_child_act
    .pipe(fn_apply_dtypes, dict_11FW_col_dtypes_5) ### Checking all date columns.
)
df_11FW_adult_act = (
    df_11FW_adult_act
    .pipe(fn_apply_dtypes, dict_11FW_col_dtypes_4) ### Checking all date columns.
)

df_11FW_ref_excl = (
    df_11FW_ref_excl
    .pipe(fn_apply_dtypes, dict_11FW_col_dtypes_8) ### Checking all date columns.
)

df_11FW_adult_uncope = (
    df_11FW_adult_uncope
    .pipe(fn_apply_dtypes, dict_11FW_col_dtypes_6) ### Checking all date columns.
)
######################################


######################################

### >>> df_11FW_5: 'Child Activity Export'.
## 

#%%### 1. Remove any rows with a discharge date (TERMINATION DATE) before the current reporting year 
df_11FW_child_act = df_11FW_child_act[df_11FW_child_act['TERMINATION DATE'] >= pd.Timestamp(f'2023-01-01')]
df_11FW_child_act['TERMINATION DATE'].astype('datetime64[ns]')

df_11FW_child_act
#%%### 2. Remove rows that do not have a first home visit date  
df_11FW_child_act=df_11FW_child_act.dropna(subset=['MinOfHVDate'],inplace=False)
df_11FW_child_act = (
    df_11FW_adult_act.dropna(subset=['MaxOfVISIT NUMBER'], inplace=False)
)
df_11FW_child_act
#%%### 3. Remove any rows with a MaxofHVDate before the current report year 
df_11FW_child_act = df_11FW_child_act[df_11FW_child_act['MaxOfHVDate'] >= pd.Timestamp(f'2023-01-01')]
df_11FW_child_act['MaxOfHVDate'].astype('datetime64[ns]')

df_11FW_child_act
#%%### 2. Remove rows that do not have a first home visit date  

######################################
### >>> df_11FW_5: 'Adult Activity Export'.
##

#%%### 1. 1. Remove any rows with a discharge date (TERMINATION DATE) before the current reporting year  
df_11FW_adult_act = df_11FW_adult_act[df_11FW_adult_act['TERMINATION DATE'] >= pd.Timestamp(f'2023-01-01')]
df_11FW_adult_act['TERMINATION DATE'].astype('datetime64[ns]')
#%%### 2. Remove rows that do not have a first home visit date OR Max Visit Number is 0 or blank 
df_11FW_adult_act = (
    df_11FW_adult_act.dropna(subset=['MinOfHVDate'], inplace=False)
)
df_11FW_adult_act = (
    df_11FW_adult_act.dropna(subset=['MaxOfVISIT NUMBER'], inplace=False)
)
df_11FW_adult_act['MaxOfVISIT NUMBER']= (
    df_11FW_adult_act['MaxOfVISIT NUMBER'] != '0' #currently this returns True/False
)
df_11FW_adult_act['MaxOfVISIT NUMBER']
#%%### 3. Remove any rows with a MaxofHVDate before the current report year 
df_11FW_adult_act = df_11FW_adult_act[df_11FW_adult_act['MaxOfHVDate'] >= pd.Timestamp(f'2023-01-01')]
df_11FW_adult_act['MaxOfHVDate'].astype('datetime64[ns]')




######################################
### >>> 'Referral Exclusions 1 thru 6' VLOOKUP with 'Adult Activity Export'.
#%%### 1. - Use VLOOKUP to pull exclusions 1, 2, 3, 5 , and 6 into the Adult Activity spreadsheet (Note: Exclusion 4 is on the Child Activity export)

# Merge the DataFrames based on the common column ('Project ID')
df_11FW_adult_act= pd.merge(df_11FW_adult_act, df_11FW_ref_excl, on=['Project ID', 'ChildNumber', 'agency'], how='left')
print(df_11FW_adult_act.columns)
df_11FW_adult_act

#%% Exclusion 4 not needed so drop exclusion 4
df_11FW_adult_act=df_11FW_adult_act.drop(['need_ex4'], axis=1)
df_11FW_adult_act


######################################
### >>> 'Referral Exclusions 1 thru 6' VLOOKUP with 'Child Activity Export'.
#%%### 1. - Use VLOOKUP to pull exclusion 4 into the Child Activity spreadsheet

# Merge the DataFrames based on the common column ('Project ID')

df_11FW_child_act = pd.merge(df_11FW_child_act, df_11FW_ref_excl, on=['Project ID','ChildNumber','agency'], how='left')

# drop irrelevant columns
df_11FW_child_act = df_11FW_child_act.drop(['need_ex1', 'need_ex2', 'need_ex3', 'need_ex5', 'need_ex6'], axis=1)
df_11FW_child_act

######################################
### >>> 'Child Activity Export' ZIP Code change. --MOB ZIP already brought over so not necessary


######################################
### >>> 'Adult UNCOPE Query' Inclusion.
#%%### 1. - Take UNCOPE columns and insert into Adult Activities
df_11FW_adult_uncope_columns = df_11FW_adult_uncope.filter(['Project ID','ChildNumber','agency','DATEUNCOPE', 'U', 'N', 'C', 'O', 'P', 'E', 'ReferralDATE', 'Category'])
df_11FW_adult_act = pd.merge(df_11FW_adult_act, df_11FW_adult_uncope_columns, on=['Project ID','ChildNumber','agency'], how='left')
df_11FW_adult_act

######################################
### >>> 'Adult UNCOPE Query' Inclusion.
#%%### 1. - Take UNCOPE columns and insert into Adult Activities
#%%### 1. - drop irrelevant columns

### TODO: put in documentation:
### tgt = child
### mob = primary caregiver
### fob = secondary caregiver
### Expectation of target children: only first child (unless multiples); secondary children not tracked.


######################################
#%%###################################
### <> 11. Remove identifying variables 

#%%
### 1. Test that DFs identical:
df_11FW_before_child_act.equals(df_11FW_child_act)

#%%
### Search for specific columns:
### Want to remove: first and last name of tgt, mob, and fob; SSNs of tgt, mob, and fob; address; city; and worker_id. (Leave ZIP).
list(filter(lambda col: re.search(r'(?i)(name|ssn|address|worker|((?<!ethni)city))', col), [*df_11FW_child_act]))

#%%
### 2. Make change:
print(f'Columns: {len(df_11FW_child_act.columns)}')
print('Remove identifying variables') 
df_11FW_child_act = (
    df_11FW_child_act
    .drop(columns=['worker_id', 'tgt_first_name', 'tgt_last_name', 'tgt_ssn', 'mob_first_name', 'mob_last_name', 'mob_ssn', 'fob_first_name', 'fob_last_name', 'fob_ssn', 'address', 'city']) 
)
print(f'Columns: {len(df_11FW_child_act.columns)}')
### Note: LEAVE ZIP Code!

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_child_act.equals(df_11FW_child_act)}')

#%%
### 4. Programmatically test change:
print('For change "Remove identifying variables"...') 
### ________________________________

if (len(df_11FW_before_child_act) == len(df_11FW_child_act)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if (len(df_11FW_before_child_act.columns) >= len(df_11FW_child_act.columns)):
    print('Passed Test 3: Columns have been removed (unless no change).')
else:
    raise Exception('**Test 3 Failed: Greater number of columns after.')
### ________________________________

if ((len(list(filter(lambda col: re.search(r'(?i)(name|ssn|address|worker|((?<!ethni)city))', col), [*df_11FW_before_child_act]))) >= 0)
    and (len(list(filter(lambda col: re.search(r'(?i)(name|ssn|address|worker|((?<!ethni)city))', col), [*df_11FW_child_act]))) == 0)): 
    print('Passed Test 4: Variables to delete possibly present before but definitely not after.')
else:
    raise Exception('**Test 4 Failed: Variables to delete not present before or present after.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_child_act)}')
print(f'Columns: {len(df_11FW_child_act.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_child_act = df_11FW_child_act.copy() 



######################################
#%%###################################
### <> 12. Create year & quarter columns (after all filtering)

#%%
### 1. Test that DFs identical:
df_11FW_before_child_act.equals(df_11FW_child_act)

#%%
### 2. Make change:
print(f'Columns: {len(df_11FW_child_act.columns)}')
print('Create year & quarter columns') 
df_11FW_child_act = (
    df_11FW_child_act
    ### Add year & quarter columns AFTER all filters:
    .assign(year = int_nehv_year, quarter = int_nehv_quarter).astype({'year': 'Int64', 'quarter': 'Int64'})
)
print(f'Columns: {len(df_11FW_child_act.columns)}')

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_child_act.equals(df_11FW_child_act)}')

### #%%
### ### See differences:
### df_11FW_before_child_act.compare(df_11FW_child_act) ### Can't because columns different.

#%% 
inspect_col(df_11FW_child_act['year'])
#%%
inspect_col(df_11FW_child_act['quarter'])

#%%
### 4. Programmatically test change:
print('For change "Create year & quarter columns"...') 
### ________________________________

### Note: Should have no new NA because new column should be entirely filled.
if (df_11FW_before_child_act.isna().sum().sum() == df_11FW_child_act.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_11FW_before_child_act) == len(df_11FW_child_act)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_11FW_before_child_act.columns) + 2 == len(df_11FW_child_act.columns))
    and (sorted([*df_11FW_before_child_act] + ['year', 'quarter']) == sorted([*df_11FW_child_act]))): 
    print('Passed Test 3: Exactly 2 more columns named "year" & "quarter".')
else:
    raise Exception('**Test 3 Failed: Not exactly 2 more columns named "year" & "quarter".')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_child_act)}')
print(f'Columns: {len(df_11FW_child_act.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_child_act = df_11FW_child_act.copy() 



######################################
#%%###################################
### <> 13. Reorder columns 

df_11FW_child_act = df_11FW_child_act[['project_id', 'year', 'quarter'] + [c for c in df_11FW_child_act.columns if c not in ['project_id', 'year', 'quarter']]]

### TODO: check number of columns.



#%%###################################
### <> df_11FW_child_act
df_11FW_child_act = df_11FW_child_act.copy()



#%%
### <> NOTE: Previously, FW & LL combined before the following restructuring and joining.


### TODO: add to 1.3:
### NOTE for 1.3 step: intention is to have all quarters represented in DS, but NO data from previous FYs. Purpose: allow local users to check & clean their data throughout the year.


### !>>> 
#%%###################################
### <> df_11FW_2: 'KU_CHILDERINJ'.
df_11FW_ChildERInj_2 = (
    df_11FW_allstring_2
    ### 1. Strip surrounding whitespace:
    .map(lambda cell: cell.strip(), na_action='ignore').astype('string')
    ### 2. Find & replace "null" values:
    .pipe(fn_find_and_replace_value_in_df, 'family_id', list_11FW_values_to_find_and_replace, pd.NA)
    ### 3. Add nanoseconds to datetimes missing them:
    .replace({col:regex_11FW_dates_to_fix for col in list_11FW_date_cols_2}, regex_11FW_dates_replacement, regex=True) 
    ### 4. Set data types:
    .pipe(fn_apply_dtypes, dict_11FW_col_dtypes_2)
    ### 5v2. Column agency set to "ll":
    .assign(agency = 'll').astype({'agency': 'string'})
    ### 6. Column tgt_id fill NA with "0":
    .assign(tgt_id = lambda df: (df['tgt_id'].fillna('0')).astype('string')) 
    ### 7v2. Create project_id column:
    .assign(project_id = lambda df: (df['agency'] + df['family_id'] + '-' + df['tgt_id']).astype('string'))
    ### new8. Filter dates: Only want current FY for Form 2 Construct 8.
    .query('date >= @date_fy_start')
    ### TODO: rename section?: filter column reason to only accept "ER Visit".
    .query('reason == "ER Visit"')
    ### new9. Add year & quarter columns AFTER filter:
    .assign(year = int_nehv_year, quarter = int_nehv_quarter).astype({'year': 'Int64', 'quarter': 'Int64'})
    ### new10. Sort rows:
    .sort_values(by=['project_id', 'date'], na_position='first', ignore_index=True)
    ### new11. Reorder columns:
    [['project_id', 'year', 'quarter', 'agency', 'family_id', 'tgt_id', 'funding', 'reason', 'date']]
    ### new12. Rename columns:
    .rename(columns={'project_id': 'ProjectID', 'family_id': 'FAMILYNUMBER', 'tgt_id': 'ChildNumber', 'reason': 'ERVisitReason', 'date': 'IncidentDate'})
)



#%%###################################
### <> df_11FW_3: 'KU_MATERNALINS'.
df_11FW_MaternalIns_3 = (
    df_11FW_allstring_3
    ### 1. Strip surrounding whitespace:
    .map(lambda cell: cell.strip(), na_action='ignore').astype('string')
    ### 2. Find & replace "null" values:
    .pipe(fn_find_and_replace_value_in_df, 'family_id', list_11FW_values_to_find_and_replace, pd.NA)
    ### 3. Add nanoseconds to datetimes missing them:
    .replace({col:regex_11FW_dates_to_fix for col in list_11FW_date_cols_3}, regex_11FW_dates_replacement, regex=True) 
    ### 4. Set data types:
    .pipe(fn_apply_dtypes, dict_11FW_col_dtypes_3)
    ### 5v2. Column agency set to "ll":
    .assign(agency = 'll').astype({'agency': 'string'})
    ### 6. Column tgt_id fill NA with "0":
    .assign(tgt_id = lambda df: (df['tgt_id'].fillna('0')).astype('string')) 
    ### 7v2. Create project_id column:
    .assign(project_id = lambda df: (df['agency'] + df['family_id'] + '-' + df['tgt_id']).astype('string'))
    ### Note: Do NOT filter dates. Need insurance change dates before current FY for Form 2 Construct 16.
    ### new9. Add year & quarter columns AFTER filter:
    .assign(year = int_nehv_year, quarter = int_nehv_quarter).astype({'year': 'Int64', 'quarter': 'Int64'})
    ### new10. Sort rows:
    .sort_values(by=['project_id', 'date'], na_position='first', ignore_index=True)
    ### new11. Reorder columns:
    [['project_id', 'year', 'quarter', 'agency', 'family_id', 'tgt_id', 'funding', 'insurance', 'date']]
    ### new12. Rename columns:
    .rename(columns={'project_id': 'ProjectID', 'family_id': 'FAMILYNUMBER', 'tgt_id': 'ChildNumber', 'insurance': 'AD1PrimaryIns', 'date': 'AD1InsChangeDate'})
)
### from instructions "Insert a column B and enter this formula =COUNTIF($A$2:A2,A2) and move to column 4". 
### Answer: probably not needed. Was a count of rows per person.
### TODO: check later in code & see if a count column like this is needed.



#%%###################################
### <> df_11FW_4: 'KU_WELLCHILDVISITS'.
df_11FW_WellChildVisits_4 = (
    df_11FW_allstring_4
    ### 1. Strip surrounding whitespace:
    .map(lambda cell: cell.strip(), na_action='ignore').astype('string')
    ### 2. Find & replace "null" values:
    .pipe(fn_find_and_replace_value_in_df, 'family_id', list_11FW_values_to_find_and_replace, pd.NA)
    ### 3. Add nanoseconds to datetimes missing them:
    .replace({col:regex_11FW_dates_to_fix for col in list_11FW_date_cols_4}, regex_11FW_dates_replacement, regex=True) 
    ### 4. Set data types:
    .pipe(fn_apply_dtypes, dict_11FW_col_dtypes_4)
    ### 5v2. Column agency set to "ll":
    .assign(agency = 'll').astype({'agency': 'string'})
    ### 6. Column tgt_id fill NA with "0":
    .assign(tgt_id = lambda df: (df['tgt_id'].fillna('0')).astype('string')) 
    ### 7v2. Create project_id column:
    .assign(project_id = lambda df: (df['agency'] + df['family_id'] + '-' + df['tgt_id']).astype('string'))
    ### new8. Filter dates: Filter out bad data earlier than "2017-10-01". Need WCVisits before current FY for Form 2 Construct 4: ### TODO: Check C04.
        ### TODO: Add to wiki NE doc notably what is being filtered out & what is being kept. Just overview.
    .query('date >= "2017-10-01"') 
    ### new9. Add year & quarter columns AFTER filter:
    .assign(year = int_nehv_year, quarter = int_nehv_quarter).astype({'year': 'Int64', 'quarter': 'Int64'})
    ### new10. Sort rows:
    .sort_values(by=['project_id', 'date'], na_position='first', ignore_index=True)
    ### new11. Reorder columns:
    [['project_id', 'year', 'quarter', 'agency', 'family_id', 'tgt_id', 'funding', 'date']]
    ### new12. Rename columns:
    .rename(columns={'project_id': 'ProjectID', 'family_id': 'FAMILYNUMBER', 'tgt_id': 'ChildNumber', 'date': 'WellVisitDate'})
)



#%%###################################
### <> Inspect DFs

#%%
# inspect_df(df_11FW_child_act)
# ### Counts of dtypes:
# print(collections.Counter(df_11FW_child_act.dtypes))

#%%
inspect_df(df_11FW_ChildERInj_2)

#%%
inspect_df(df_11FW_MaternalIns_3)

#%%
inspect_df(df_11FW_WellChildVisits_4)



#%%##############################################!>>>
### >>> RESTRUCTURING  
#####################################################

### Compare to files here: 
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\previous\before restructure\Y13Q1 (Oct 2023 - Dec 2023) 
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.3combine\previous\after restructuring\Y13Q1 (Oct 2023 - Dec 2023) 


#%%###################################
### <> ChildERInj 

### Pivot the DataFrame:
df_11FW_pivoted_ChildERInj_2 = df_11FW_ChildERInj_2.pivot_table(
    index=['ProjectID', 'year', 'quarter', 'agency', 'FAMILYNUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_11FW_ChildERInj_2.groupby(['ProjectID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['ERVisitReason', 'IncidentDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
df_11FW_pivoted_ChildERInj_2

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_11FW_pivoted_ChildERInj_2 = df_11FW_pivoted_ChildERInj_2.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
df_11FW_pivoted_ChildERInj_2

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_11FW_pivoted_ChildERInj_2.columns = [f'{col[0]}.{col[1]}' for col in df_11FW_pivoted_ChildERInj_2.columns]
df_11FW_pivoted_ChildERInj_2

#%%
### Reset row & column indices:
df_11FW_pivoted_ChildERInj_2 = df_11FW_pivoted_ChildERInj_2.reset_index()
df_11FW_pivoted_ChildERInj_2



#%%###################################
### <> MaternalIns 

### Pivot the DataFrame:
df_11FW_pivoted_MaternalIns_3 = df_11FW_MaternalIns_3.pivot_table(
    index=['ProjectID', 'year', 'quarter', 'agency', 'FAMILYNUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_11FW_MaternalIns_3.groupby(['ProjectID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['AD1PrimaryIns', 'AD1InsChangeDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
df_11FW_pivoted_MaternalIns_3

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_11FW_pivoted_MaternalIns_3 = df_11FW_pivoted_MaternalIns_3.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
df_11FW_pivoted_MaternalIns_3

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_11FW_pivoted_MaternalIns_3.columns = [f'{col[0]}.{col[1]}' for col in df_11FW_pivoted_MaternalIns_3.columns]
df_11FW_pivoted_MaternalIns_3

#%%
### Reset row & column indices:
df_11FW_pivoted_MaternalIns_3 = df_11FW_pivoted_MaternalIns_3.reset_index()
df_11FW_pivoted_MaternalIns_3



#%%###################################
### <> WellChildVisits 

### Pivot the DataFrame:
df_11FW_pivoted_WellChildVisits_4 = df_11FW_WellChildVisits_4.pivot_table(
    index=['ProjectID', 'year', 'quarter', 'agency', 'FAMILYNUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_11FW_WellChildVisits_4.groupby(['ProjectID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['WellVisitDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
# df_11FW_pivoted_WellChildVisits_4

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_11FW_pivoted_WellChildVisits_4 = df_11FW_pivoted_WellChildVisits_4.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
# df_11FW_pivoted_WellChildVisits_4

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_11FW_pivoted_WellChildVisits_4.columns = [f'{col[0]}.{col[1]}' for col in df_11FW_pivoted_WellChildVisits_4.columns]
# df_11FW_pivoted_WellChildVisits_4

#%%
### Reset row & column indices:
df_11FW_pivoted_WellChildVisits_4 = df_11FW_pivoted_WellChildVisits_4.reset_index()
# df_11FW_pivoted_WellChildVisits_4



#%%##############################################!>>>
### >>> WRITE OUT FILES   
#####################################################

### Note: Date columns written out without timestamps.

df_11FW_child_act.to_csv(Path(path_11FW_dir_output, 'df_11FW_child_act.csv'), index = False, date_format="%m/%d/%Y")
df_11FW_pivoted_ChildERInj_2.to_csv(Path(path_11FW_dir_output, 'df_11FW_pivoted_ChildERInj_2.csv'), index = False, date_format="%m/%d/%Y")
df_11FW_pivoted_MaternalIns_3.to_csv(Path(path_11FW_dir_output, 'df_11FW_pivoted_MaternalIns_3.csv'), index = False, date_format="%m/%d/%Y")
df_11FW_pivoted_WellChildVisits_4.to_csv(Path(path_11FW_dir_output, 'df_11FW_pivoted_WellChildVisits_4.csv'), index = False, date_format="%m/%d/%Y")



#%%##############################################!>>>
### >>> Remove old objects  
#####################################################

#%%
[o for o in list(globals().keys()) if o.startswith(('date', 'int', 'path', 'str'))]
### Keep.

#%%
# [o for o in list(globals().keys()) if o.startswith('df')]
#%%
del df_11FW_allstring_1, df_11FW_allstring_2, df_11FW_allstring_3, df_11FW_allstring_4, df_11FW_before_child_act, df_11FW_child_act, df_11FW_ChildERInj_2, df_11FW_MaternalIns_3, df_11FW_WellChildVisits_4 

#%%
# [o for o in list(globals().keys()) if o.startswith('dict')]
#%%
del dict_11FW_col_dtypes_1, dict_11FW_col_dtypes_2, dict_11FW_col_dtypes_3, dict_11FW_col_dtypes_4 

#%%
# [o for o in list(globals().keys()) if o.startswith('list')]
#%%
del list_path_11FW_input_raw_sheets, list_11FW_col_detail_1, list_11FW_date_cols_1, list_11FW_col_detail_2, list_11FW_date_cols_2, list_11FW_col_detail_3, list_11FW_date_cols_3, list_11FW_col_detail_4, list_11FW_date_cols_4, list_11FW_values_to_find_and_replace 

#%%
# [o for o in list(globals().keys()) if o.startswith(('regex', 'xlsx'))]
#%%
del xlsx_11FW, regex_11FW_dates_to_fix, regex_11FW_dates_replacement 

#%%
### Is what's left over what is wanted?:
[o for o in list(globals().keys()) if o.startswith(('df', 'dict', 'list', 'regex', 'xlsx'))]
### Should only be:
# ['df_11FW_child_act',
#  'df_11FW_pivoted_ChildERInj_2',
#  'df_11FW_pivoted_MaternalIns_3',
#  'df_11FW_pivoted_WellChildVisits_4']



#%%##############################################!>>>
### >>> END 
#################################################!>>>

print('Congrats! You ran 1.2LL!')




# %%
### TODO:
    ### Remove duplciate rows (like how tried in 1.4 code).
    ### see if "logger" package would be useful.




