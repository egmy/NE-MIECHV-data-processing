



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
    ['MOB ZIP','Int64'],
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
    ['ASQ24MoRefDate''datetime64[ns]'],
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
    ['GESTATIONAL AGE','string']

]
#%%### df_11FW_5: 'Child Activities Query.xlsx'.
for x in list_11FW_col_detail_5:
    print(x[1])
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
dict_11FW_col_dtypes_8 = {x[0]:x[1] for x in list_11FW_col_detail_7}
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


# ### Backup:
# df_11FW_1 = df_11FW_allstring_1.copy()
# df_11FW_2 = df_11FW_allstring_2.copy()
# df_11FW_3 = df_11FW_allstring_3.copy()
# df_11FW_4 = df_11FW_allstring_4.copy()

# #%% 
# ### Test if all read in as strings:
# print(df_11FW_allstring_1.dtypes.to_string())
# print(df_11FW_allstring_2.dtypes.to_string())
# print(df_11FW_allstring_3.dtypes.to_string())
# print(df_11FW_allstring_4.dtypes.to_string())



#%%##############################################!>>>
### >>> CLEAN 
#####################################################



#%%###################################
### >>> df_11FW_1: 'KU_BASETABLE'.

#%%###################################
### <> Before & After 
### df_11FW_1: 'KU_BASETABLE'.

df_11FW_before_BaseTable = df_11FW_allstring_1.copy()
df_11FW_after_BaseTable = df_11FW_allstring_1.copy() 

#%% ### If needed, fo rtesting:
# df_11FW_after_BaseTable = df_11FW_before_BaseTable.copy() 



######################################
#%%###################################
### <> 1. Strip surrounding whitespace

#%%
### 1. Test that DFs identical:
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

#%%
### 2. Make change:
print('Strip surrounding whitespace')
df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    .map(lambda cell: cell.strip(), na_action='ignore').astype('string')
)

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

#%%
### See differences:
df_11FW_before_BaseTable.compare(df_11FW_after_BaseTable)

#%%
### 4. Programmatically test change:
print('For change "Strip surrounding whitespace"...') 
### ________________________________

if (df_11FW_before_BaseTable.isna().sum().sum() == df_11FW_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_11FW_before_BaseTable) == len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_11FW_before_BaseTable.columns) == len(df_11FW_after_BaseTable.columns))
    and ([*df_11FW_before_BaseTable] == [*df_11FW_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



######################################
#%%###################################
### <> 2. Find & replace "null" values

#%%
### 1. Test that DFs identical:
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

#%%
### 2. Make change:
print('Find & replace "null" values')
list_11FW_values_to_find_and_replace = ['null'] 
df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    .pipe(fn_find_and_replace_value_in_df, one_id_var='family_id', list_of_values_to_find=list_11FW_values_to_find_and_replace, replacement_value=pd.NA)
)
### Note: ### TODO: At the moment, searching is case-insensitive. Could make option for case sensitive.
### Note: ### TODO: At the moment, the entire cell must match. Could make an option for matching with substrings.

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

# print(f'{len(df_11FW_before_BaseTable.compare(df_11FW_after_BaseTable)) == 0}')
# print(f'{df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')
# print(f'{assert_frame_equal(df_11FW_before_BaseTable, df_11FW_after_BaseTable) is None}')

#%%
### See differences:
df_11FW_before_BaseTable.compare(df_11FW_after_BaseTable)

#%%
### 4. Programmatically test change:
print('For change "Find & replace "null" values"...') 
### ________________________________

if (df_11FW_before_BaseTable.isna().sum().sum() <= df_11FW_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: There are more NA after (unless no change).')
else:
    raise Exception('**Test 1 Failed: Fewer NA after.')
### ________________________________

if (len(df_11FW_before_BaseTable) == len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_11FW_before_BaseTable.columns) == len(df_11FW_after_BaseTable.columns))
    and ([*df_11FW_before_BaseTable] == [*df_11FW_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if ((fn_find_and_count_value_in_df(df_11FW_before_BaseTable, list_11FW_values_to_find_and_replace) >= 0)
    and (fn_find_and_count_value_in_df(df_11FW_after_BaseTable, list_11FW_values_to_find_and_replace) == 0)): 
    print('Passed Test 4: Values to find NOT found after, but maybe found before.')
else:
    raise Exception('**Test 4 Failed: Vales to find found after.')
### ________________________________

### TODO: Test 5: Find where before is different & is "null" & see if after turned that NA.
if (True): #TODO
    print('Passed Test 4:')
else:
    raise Exception('**Test 4 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



######################################
#%%###################################
### <> 3. Add nanoseconds to strings of datetimes missing them   

#%%
### 1. Test that DFs identical:
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

#%%
### 2. Make change:
print('Add nanoseconds to datetimes strings missing them') 

### Columns that later have problems with "fn_apply_dtypes":
### list_date_cols_to_edit = ['c_fundingdate', 'mob_living_arrangement_dt', 'fob_edu_dt', 'mcafss_edu_dt1', 'mcafss_edu_dt2', 'hlth_insure_tgt_dt'] ### Specific columns causing errors Y13Q1.

regex_11FW_dates_to_fix = r'(^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$)' 
regex_11FW_dates_replacement = r'\1.000000000' ### 9 zeros for nanoseconds! 

df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    .replace({col:regex_11FW_dates_to_fix for col in list_11FW_date_cols_1}, regex_11FW_dates_replacement, regex=True) ### Checking all date columns.
)
### Format causing errors: "2019-12-04 17:48:04" (missing nanoseconds).
### Want this format: "2019-12-10 14:02:37.223000000"

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

#%%
### See differences:
df_11FW_before_BaseTable.compare(df_11FW_after_BaseTable)

#%%
print((
    df_11FW_before_BaseTable
    ### Compare: Keep shape so ID column not dropped when the same. Keep equal so can see ID values.
    # .compare(df_11FW_after_BaseTable, keep_shape=True, keep_equal=True) 
    ### Select desired columns:
    .loc[:, ['c_fundingdate', 'mob_living_arrangement_dt', 'fob_edu_dt', 'mcafss_edu_dt1', 'mcafss_edu_dt2', 'hlth_insure_tgt_dt']]
    ### Keep rows where columns different:
    # .loc[lambda df: df.apply(fn_keep_row_differences, variable2compare=str_col_to_compare, axis=1), :] 
).to_string())

#%%
### 4. Programmatically test change:
print('For change "Add nanoseconds to datetimes strings missing them"...') 
### ________________________________

if (df_11FW_before_BaseTable.isna().sum().sum() == df_11FW_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_11FW_before_BaseTable) == len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_11FW_before_BaseTable.columns) == len(df_11FW_after_BaseTable.columns))
    and ([*df_11FW_before_BaseTable] == [*df_11FW_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

### TODO: Add a test on the regex values & see if changed.
if (True): #TODO
    print('Passed Test 4:')
else:
    raise Exception('**Test 4 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



######################################
#%%###################################
### <> 4. Set data types 

#%%
### 1. Test that DFs identical:
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

#%%
### 2. Make change:
print('Set data types') 
df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    .pipe(fn_apply_dtypes, dict_11FW_col_dtypes_1)
)
### Note: Needed to edit date strings above before applying dtypes.

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

#%%
### See differences:
df_11FW_before_BaseTable.compare(df_11FW_after_BaseTable)

#%%
### Check if data types changed:
print(df_11FW_before_BaseTable.dtypes.to_string())
print('-------------------------------------')
print(df_11FW_after_BaseTable.dtypes.to_string())
# print('-------------------------------------')
# print(df_11FW_allstring_1.dtypes.to_string())

#%%
df_11FW_before_BaseTable.dtypes.to_string() == df_11FW_after_BaseTable.dtypes.to_string()

#%%
### 4. Programmatically test change:
print('For change "Set data types"...') 
### ________________________________

if (df_11FW_before_BaseTable.isna().sum().sum() == df_11FW_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_11FW_before_BaseTable) == len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_11FW_before_BaseTable.columns) == len(df_11FW_after_BaseTable.columns))
    and ([*df_11FW_before_BaseTable] == [*df_11FW_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

### TODO: Test 4: Not every column is string (Not always true for every dataset!):
if (True): #TODO
    print('Passed Test 4:')
else:
    raise Exception('**Test 4 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



######################################
#%%###################################
### <> 5. Column site_id set to "ll" 

#%%
### 1. Test that DFs identical:
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

#%%
### 2. Make change:
print('Column site_id should now be all "ll".') 
df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    .assign(site_id = 'll').astype({'site_id': 'string'})
)

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

#%%
### See differences:
df_11FW_before_BaseTable.compare(df_11FW_after_BaseTable)

#%%
compare_col(df_11FW_before_BaseTable, df_11FW_after_BaseTable, 'site_id', 'value_counts')

#%%
### 4. Programmatically test change:
print('For change "Column site_id set to "ll""...') 
### ________________________________

if (df_11FW_before_BaseTable.isna().sum().sum() == df_11FW_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_11FW_before_BaseTable) == len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_11FW_before_BaseTable.columns) == len(df_11FW_after_BaseTable.columns))
    and ([*df_11FW_before_BaseTable] == [*df_11FW_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if ((all(df_11FW_before_BaseTable['site_id']=='01')) and (all(df_11FW_after_BaseTable['site_id']=='ll'))): 
    print('Passed Test 4: site_id is "01" before and "ll" after.')
else:
    raise Exception('**Test 4 Failed: site_id is either not "01" before or not "ll" after.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



######################################
#%%###################################
### <> 6. Column tgt_id fill NA with "0" 


#     ####
#     .pipe(fn_print_fstring_and_return_df, '-----\nColumn tgt_id before: Rows with NA that will be changed to "0":')
#     .pipe(fn_print_expression_and_return_df, (lambda df: df.loc[(lambda df: pd.isna(df['tgt_id'])), 'tgt_id'].index.tolist()))
#     
#     .pipe(fn_print_fstring_and_return_df, 'Column tgt_id after: Rows with "0":')
#     .pipe(fn_print_expression_and_return_df, (lambda df: df.loc[(df['tgt_id'] == "0"), 'tgt_id'].index.tolist()))


#%%
### 1. Test that DFs identical:
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

#%%
### 2. Make change:
print('All NAs in column tgt_id should be replaced with "0".') 
df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    .assign(tgt_id = lambda df: (df['tgt_id'].fillna('0')).astype('string')) 
)

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

#%%
### See differences:
df_11FW_before_BaseTable.compare(df_11FW_after_BaseTable)

#%%
### 4. Programmatically test change:
print('For change "Column tgt_id fill NA with "0""...') 
### ________________________________

if (df_11FW_before_BaseTable.isna().sum().sum() > df_11FW_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Now less NA.')
else:
    raise Exception('**Test 1 Failed: Number of NA not less.')
### ________________________________

if (len(df_11FW_before_BaseTable) == len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_11FW_before_BaseTable.columns) == len(df_11FW_after_BaseTable.columns))
    and ([*df_11FW_before_BaseTable] == [*df_11FW_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if (df_11FW_after_BaseTable['tgt_id'].isna().sum() == 0): 
    print('Passed Test 4: Column tgt_id has no NA after change.')
else:
    raise Exception('**Test 4 Failed: Column tgt_id does have NA after change.')
### ________________________________

# if (df_11FW_before_BaseTable['tgt_id'].isna().sum() == df_11FW_after_BaseTable['tgt_id']): #TODO
#     print('Passed Test 5: Number of NA before equals number of 0 after... (maybe below better)')
# else:
#     raise Exception('**Test 5 Failed:')
### ________________________________

### Test 6: show that all before 0's are now NA -- filtering on those indices?
if (True): #TODO
    print('Passed Test 4:')
else:
    raise Exception('**Test 4 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



######################################
#%%###################################
### <> 7. Create project_id column 

#%%
### 1. Test that DFs identical:
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

#%%
### 2. Make change:
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')
print('Create project_id column') 
df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    .assign(project_id = lambda df: (df['site_id'] + df['family_id'] + '-' + df['tgt_id']).astype('string'))
)
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

### #%%
### ### See differences:
### df_11FW_before_BaseTable.compare(df_11FW_after_BaseTable) ### Can't because columns different.

#%% 
inspect_col(df_11FW_after_BaseTable['project_id'])

#%%
### 4. Programmatically test change:
print('For change "Create project_id column"...') 
### ________________________________

### Note: Should have no new NA because new column should be entirely filled.
if (df_11FW_before_BaseTable.isna().sum().sum() == df_11FW_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_11FW_before_BaseTable) == len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_11FW_before_BaseTable.columns) + 1 == len(df_11FW_after_BaseTable.columns))
    and (sorted([*df_11FW_before_BaseTable] + ['project_id']) == sorted([*df_11FW_after_BaseTable]))): 
    print('Passed Test 3: Exactly one more column with name "project_id".')
else:
    raise Exception('**Test 3 Failed: Not exactly one more column named "project_id".')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



######################################
#%%
### Review columns that will be used to filter rows:
print((
    df_11FW_before_BaseTable
    .loc[:, ['project_id', 'discharge_dt', 'last_home_visit', 'home_visits_num']]
    .dtypes
).to_string())



######################################
#%%###################################
### <> 8. Filter out families that discharged before the current reporting year (using "discharge_dt"). 

### Note: Filtering rows (parent-child combinations), not really families.

#%%
### 1. Test that DFs identical:
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

# #%%
# df_11FW_after_BaseTable['discharge_dt'].dtypes

# #%%
# print(df_11FW_before_BaseTable.dtypes.to_string())
# print('-------------------------------------')
# print(df_11FW_after_BaseTable.dtypes.to_string())

# discharge_dt                  datetime64[ns]

#%%
### 2. Make change:
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print('Filter "discharge_dt" to remove families discharged before current reporting year') 
df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    ### Keep both later dates AND where NO discharge date:
    .query('discharge_dt >= @date_fy_start or discharge_dt.isna()')
)
print(f'Rows: {len(df_11FW_after_BaseTable)}')

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

# #%%
# ### See differences:
# df_11FW_before_BaseTable.compare(df_11FW_after_BaseTable) ### Cannot .compare when different rows.

#%%
### 4. Programmatically test change:
print('For change "Filter "discharge_dt" to remove families discharged before current reporting year"...') 
### ________________________________

# ### Don't use Test 1: Because removing rows, NA might be very different.
# if (df_11FW_before_BaseTable.isna().sum().sum() == df_11FW_after_BaseTable.isna().sum().sum()):
#     print('Passed Test 1: Number of NA unchanged.')
# else:
#     raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_11FW_before_BaseTable) >= len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Rows have been removed (unless no change).')
else:
    raise Exception('**Test 2 Failed: Greater number of rows after.')
### TODO: More specific test of row numbers?
### ________________________________

if ((len(df_11FW_before_BaseTable.columns) == len(df_11FW_after_BaseTable.columns))
    and ([*df_11FW_before_BaseTable] == [*df_11FW_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if (all((df_11FW_after_BaseTable['discharge_dt'] >= date_fy_start) | pd.isna(df_11FW_after_BaseTable['discharge_dt']))
    and all(~(df_11FW_after_BaseTable['discharge_dt'] < date_fy_start))): 
    print('Passed Test 4: After change, all "discharge_dt" dates on or after the Fiscal Year start date (or are NA).')
else:
    raise Exception('**Test 4 Failed: After change, at least one "discharge_dt" date before the Fiscal Year start date.')
### ________________________________

if (all(df_11FW_before_BaseTable[~df_11FW_before_BaseTable.index.isin(df_11FW_after_BaseTable.index)]['discharge_dt'] < date_fy_start)): 
    print('Passed Test 5: All rows filtered out had "discharge_dt" dates before the Fiscal Year start date.')
else:
    raise Exception('**Test 5 Failed: At least one row filtered out had a "discharge_dt" date not before the Fiscal Year start date.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



######################################
#%%###################################
### <> 9. Filter out families without a home visit in the current fiscal year (using "last_home_visit"). 

### Note: Filtering rows (parent-child combinations), not really families. (Joe approves!)
### TODO: Check later (in 1.4 or Report) (maybe "active child") where DOB are checked to filter out "subsequent children".

#%%
### 1. Test that DFs identical:
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

# #%%
# inspect_col(df_11FW_after_BaseTable['last_home_visit'])

#%%
### 2. Make change:
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print('Filter "last_home_visit" to remove families without a home visit in the current fiscal year') 
df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    .query('last_home_visit >= @date_fy_start and last_home_visit < @date_fy_end_day_after')
)
print(f'Rows: {len(df_11FW_after_BaseTable)}')

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

#%%
### 4. Programmatically test change:
print('For change "Filter "last_home_visit" to remove families without a home visit in the current fiscal year"...') 
### ________________________________

if (len(df_11FW_before_BaseTable) >= len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Rows have been removed (unless no change).')
else:
    raise Exception('**Test 2 Failed: Greater number of rows after.')
### TODO: More specific test of row numbers?
### ________________________________

if ((len(df_11FW_before_BaseTable.columns) == len(df_11FW_after_BaseTable.columns))
    and ([*df_11FW_before_BaseTable] == [*df_11FW_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if (all((df_11FW_after_BaseTable['last_home_visit'] >= date_fy_start) & (df_11FW_after_BaseTable['last_home_visit'] < date_fy_end_day_after))
    and all(~((df_11FW_after_BaseTable['last_home_visit'] < date_fy_start) | (df_11FW_after_BaseTable['last_home_visit'] >= date_fy_end_day_after)))): 
    print('Passed Test 4: After change, all "last_home_visit" dates within the Fiscal Year.')
else:
    raise Exception('**Test 4 Failed: After change, at least one "last_home_visit" date outside the Fiscal Year.')
### ________________________________

if (all((df_11FW_before_BaseTable[~df_11FW_before_BaseTable.index.isin(df_11FW_after_BaseTable.index)]['last_home_visit'] < date_fy_start)
    | (df_11FW_before_BaseTable[~df_11FW_before_BaseTable.index.isin(df_11FW_after_BaseTable.index)]['last_home_visit'] >= date_fy_end_day_after))): 
    print('Passed Test 5: All rows filtered out had "last_home_visit" dates outside the Fiscal Year.')
else:
    raise Exception('**Test 5 Failed: At least one row filtered out had a "last_home_visit" within the Fiscal Year.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



######################################
#%%###################################
### <> 10. Filter out families with 0 home visits (using "home_visits_num"). 

### Note: Filtering rows (parent-child combinations), not really families.

#%%
### 1. Test that DFs identical:
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

# #%%
# inspect_col(df_11FW_after_BaseTable['home_visits_num']) ### Int64.

#%%
### 2. Make change:
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print('Filter "home_visits_num" to remove families without a home visit') 
df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    .query('home_visits_num > 0')
)
print(f'Rows: {len(df_11FW_after_BaseTable)}')
### Note: By this filter, all families to remove should have been removed above.

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

#%%
### 4. Programmatically test change:
print('For change "Filter "home_visits_num" to remove families without a home visit"...') 
### ________________________________

if (len(df_11FW_before_BaseTable) >= len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Rows have been removed (unless no change).')
else:
    raise Exception('**Test 2 Failed: Greater number of rows after.')
### TODO: More specific test of row numbers?
### ________________________________

if ((len(df_11FW_before_BaseTable.columns) == len(df_11FW_after_BaseTable.columns))
    and ([*df_11FW_before_BaseTable] == [*df_11FW_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if (all(df_11FW_after_BaseTable['home_visits_num'] > 0)
    and all(~(df_11FW_after_BaseTable['home_visits_num'] <= 0))): 
    print('Passed Test 4: After change, all "home_visits_num" numbers greater than 0.')
else:
    raise Exception('**Test 4 Failed: After change, at least one "home_visits_num" number less than or equal to 0.')
### ________________________________

if (all(df_11FW_before_BaseTable[~df_11FW_before_BaseTable.index.isin(df_11FW_after_BaseTable.index)]['home_visits_num'] <= 0)): 
    print('Passed Test 5: All rows filtered out had "home_visits_num" numbers less than or equal to 0.')
else:
    raise Exception('**Test 5 Failed: At least one row filtered out had a "home_visits_num" number greater than 0.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



### <> NOTE: CREATE CFS ID File here.


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
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

#%%
### Search for specific columns:
### Want to remove: first and last name of tgt, mob, and fob; SSNs of tgt, mob, and fob; address; city; and worker_id. (Leave ZIP).
list(filter(lambda col: re.search(r'(?i)(name|ssn|address|worker|((?<!ethni)city))', col), [*df_11FW_after_BaseTable]))

#%%
### 2. Make change:
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')
print('Remove identifying variables') 
df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    .drop(columns=['worker_id', 'tgt_first_name', 'tgt_last_name', 'tgt_ssn', 'mob_first_name', 'mob_last_name', 'mob_ssn', 'fob_first_name', 'fob_last_name', 'fob_ssn', 'address', 'city']) 
)
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')
### Note: LEAVE ZIP Code!

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

#%%
### 4. Programmatically test change:
print('For change "Remove identifying variables"...') 
### ________________________________

if (len(df_11FW_before_BaseTable) == len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if (len(df_11FW_before_BaseTable.columns) >= len(df_11FW_after_BaseTable.columns)):
    print('Passed Test 3: Columns have been removed (unless no change).')
else:
    raise Exception('**Test 3 Failed: Greater number of columns after.')
### ________________________________

if ((len(list(filter(lambda col: re.search(r'(?i)(name|ssn|address|worker|((?<!ethni)city))', col), [*df_11FW_before_BaseTable]))) >= 0)
    and (len(list(filter(lambda col: re.search(r'(?i)(name|ssn|address|worker|((?<!ethni)city))', col), [*df_11FW_after_BaseTable]))) == 0)): 
    print('Passed Test 4: Variables to delete possibly present before but definitely not after.')
else:
    raise Exception('**Test 4 Failed: Variables to delete not present before or present after.')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



######################################
#%%###################################
### <> 12. Create year & quarter columns (after all filtering)

#%%
### 1. Test that DFs identical:
df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)

#%%
### 2. Make change:
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')
print('Create year & quarter columns') 
df_11FW_after_BaseTable = (
    df_11FW_after_BaseTable
    ### Add year & quarter columns AFTER all filters:
    .assign(year = int_nehv_year, quarter = int_nehv_quarter).astype({'year': 'Int64', 'quarter': 'Int64'})
)
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_11FW_before_BaseTable.equals(df_11FW_after_BaseTable)}')

### #%%
### ### See differences:
### df_11FW_before_BaseTable.compare(df_11FW_after_BaseTable) ### Can't because columns different.

#%% 
inspect_col(df_11FW_after_BaseTable['year'])
#%%
inspect_col(df_11FW_after_BaseTable['quarter'])

#%%
### 4. Programmatically test change:
print('For change "Create year & quarter columns"...') 
### ________________________________

### Note: Should have no new NA because new column should be entirely filled.
if (df_11FW_before_BaseTable.isna().sum().sum() == df_11FW_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_11FW_before_BaseTable) == len(df_11FW_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_11FW_before_BaseTable.columns) + 2 == len(df_11FW_after_BaseTable.columns))
    and (sorted([*df_11FW_before_BaseTable] + ['year', 'quarter']) == sorted([*df_11FW_after_BaseTable]))): 
    print('Passed Test 3: Exactly 2 more columns named "year" & "quarter".')
else:
    raise Exception('**Test 3 Failed: Not exactly 2 more columns named "year" & "quarter".')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_11FW_after_BaseTable)}')
print(f'Columns: {len(df_11FW_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_11FW_before_BaseTable = df_11FW_after_BaseTable.copy() 



######################################
#%%###################################
### <> 13. Reorder columns 

df_11FW_after_BaseTable = df_11FW_after_BaseTable[['project_id', 'year', 'quarter'] + [c for c in df_11FW_after_BaseTable.columns if c not in ['project_id', 'year', 'quarter']]]

### TODO: check number of columns.



#%%###################################
### <> df_11FW_BaseTable
df_11FW_BaseTable = df_11FW_after_BaseTable.copy()



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
# inspect_df(df_11FW_BaseTable)
# ### Counts of dtypes:
# print(collections.Counter(df_11FW_BaseTable.dtypes))

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

df_11FW_BaseTable.to_csv(Path(path_11FW_dir_output, 'df_11FW_BaseTable.csv'), index = False, date_format="%m/%d/%Y")
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
del df_11FW_allstring_1, df_11FW_allstring_2, df_11FW_allstring_3, df_11FW_allstring_4, df_11FW_before_BaseTable, df_11FW_after_BaseTable, df_11FW_ChildERInj_2, df_11FW_MaternalIns_3, df_11FW_WellChildVisits_4 

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
# ['df_11FW_BaseTable',
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




