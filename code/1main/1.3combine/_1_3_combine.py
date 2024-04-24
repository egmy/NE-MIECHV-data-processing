
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
if (os.path.basename(__file__) == '_1_3_combine.py'):
    from _1_3_RUNME import * 
    print('Imported "_1_3_RUNME"')
else:
    print("Did NOT run RUNME again... because it's already running!")


#%%##############################################!>>>
### >>> COLUMN DEFINITIONS 
#####################################################

#%%### df_13_1: '04 Well Child v2 no MAX - use this one.xlsx'.
#%%### df_13_2: '08 Child ER Injury.xlsx'.
#%%### df_13_3: '16 - Caregiver Insurance v2 - USE THIS ONE.xlsx'.
#%%### df_13_4: 'Adult Activities Query.xlsx'.
#%%### df_13_5: 'Child Activities Query.xlsx'.
#%%### df_13_6: 'Adult UNCOPE Query.xlsx'.
#%%### df_13_7: 'F1 - Home Visit Type Query.xlsx'.
#%%### df_13_8: 'Referral Exclusions 1 thru 6.xlsx'.
#%%###
pd.set_option('display.max_columns', None)

#%%### df_13_8: 'Referral Exclusions 1 thru 6.xlsx'.

#%%##############################################!>>>
### >>> READ 
#####################################################

#%%
### Performance benefit for reading in file to memory only once by creating an ExcelFile class object.
xlsx_13_input_well_child_FW = pd.ExcelFile(path_13_input_well_child_FW)
xlsx_13_input_well_child_LL = pd.ExcelFile(path_13_input_well_child_LL)
xlsx_13_input_child_injury_FW = pd.ExcelFile(path_13_input_child_injury_FW)
xlsx_13_input_child_injury_LL = pd.ExcelFile(path_13_input_child_injury_LL)
xlsx_13_input_cg_ins_FW = pd.ExcelFile(path_13_input_cg_ins_FW)
xlsx_13_input_cg_ins_LL = pd.ExcelFile(path_13_input_cg_ins_LL)
xlsx_13_input_adult_act = pd.ExcelFile(path_13_input_adult_act)
xlsx_13_input_child_act = pd.ExcelFile(path_13_input_child_act)
xlsx_13_input_base_table = pd.ExcelFile(path_13_input_base_table)


#%%###################################
### READ in all sheets.

### Read in EVERYTHING WITH pd.NA for empty cells:
df_13_well_child_FW = pd.read_excel(xlsx_13_input_well_child_FW, keep_default_na=False, na_values=[''])
df_13_well_child_LL = pd.read_excel(xlsx_13_input_well_child_LL, keep_default_na=False, na_values=[''])
df_13_child_injury_FW = pd.read_excel(xlsx_13_input_child_injury_FW, keep_default_na=False, na_values=[''])
df_13_child_injury_LL= pd.read_excel(xlsx_13_input_child_injury_LL, keep_default_na=False, na_values=[''])
df_13_cg_ins_FW = pd.read_excel(xlsx_13_input_cg_ins_FW, keep_default_na=False, na_values=[''])
df_13_cg_ins_LL = pd.read_excel(xlsx_13_input_cg_ins_LL, keep_default_na=False, na_values=[''])
df_13_adult_act = pd.read_excel(xlsx_13_input_adult_act, keep_default_na=False, na_values=[''])
df_13_child_act = pd.read_excel(xlsx_13_input_child_act, keep_default_na=False, na_values=[''])
df_13_base_table = pd.read_excel(xlsx_13_input_base_table, keep_default_na=False, na_values=[''])
#%%##############################################!>>>
### >>> CLEAN 
#####################################################

#%%### 1. Strip surrounding whitespace
df_13_child_act = (
    df_13_child_act.map(lambda cell: cell.strip(), na_action='ignore')
)
df_13_adult_act = (
    df_13_adult_act.map(lambda cell: cell.strip(), na_action='ignore')
)
df_13_well_child_FW = (
    df_13_well_child_FW.map(lambda cell: cell.strip(), na_action='ignore')
)
df_13_well_child_LL = (
    df_13_well_child_LL.map(lambda cell: cell.strip(), na_action='ignore')
)
df_13_child_injury_FW = (
    df_13_child_injury_FW.map(lambda cell: cell.strip(), na_action='ignore')
)
df_13_child_injury_LL = (
    df_13_child_injury_LL.map(lambda cell: cell.strip(), na_action='ignore')
)
df_13_cg_ins_FW = (
    df_13_cg_ins_FW.map(lambda cell: cell.strip(), na_action='ignore')
)
df_13_cg_ins_LL = (
    df_13_cg_ins_LL.map(lambda cell: cell.strip(), na_action='ignore')
)
df_13_base_table = (
    df_13_base_table.map(lambda cell: cell.strip(), na_action='ignore')
)

#%%##############################################!>>>
### >>> COMBINING
#####################################################

######################################
#%%### 1. Add 'year' and 'quarter' columns to all FamilyWise dataframes

nehv_year = 13

nehv_quarter = 1
if 'Q1' in str_nehv_quarter: 
    nehv_quarter = 1
elif 'Q2'in str_nehv_quarter: 
    nehv_quarter = 2
elif 'Q3'in str_nehv_quarter: 
    nehv_quarter = 3
elif 'Q4'in str_nehv_quarter: 
    nehv_quarter = 4

df_13_child_act.insert(loc=1, column='year', value=nehv_year)
df_13_child_act.insert(loc=2, column='quarter', value=nehv_quarter)

df_13_adult_act.insert(loc=1, column='year', value=nehv_year)
df_13_adult_act.insert(loc=2, column='quarter', value=nehv_quarter)

df_13_well_child_FW.insert(loc=1, column='year', value=nehv_year)
df_13_well_child_FW.insert(loc=2, column='quarter', value=nehv_quarter)

df_13_child_injury_FW.insert(loc=1, column='year', value=nehv_year)
df_13_child_injury_FW.insert(loc=2, column='quarter', value=nehv_quarter)

df_13_cg_ins_FW.insert(loc=1, column='year', value=nehv_year)
df_13_cg_ins_FW.insert(loc=2, column='quarter', value=nehv_quarter)




#%%### 2. Create Project ID sheet for adult and child sheets
child_frames = [df_13_child_act[['Project ID']], df_13_base_table[['project_id']]]
df_child_project_id=pd.concat(child_frames)

df_child_project_id['year']=nehv_year
df_child_project_id['quarter']=nehv_quarter

adult_frames = [df_13_adult_act[['Project ID']], df_13_base_table[['project_id']]]
df_adult_project_id=pd.concat(adult_frames)

df_adult_project_id['year']=nehv_year
df_adult_project_id['quarter']=nehv_quarter


#%%### 3. if not 1st quarter, write to existing files
if nehv_quarter!=1:
    path_master_file=Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.4Tableau')
    path_13_master_input = Path(path_master_file, '0in', str_nehv_quarter)

    path_child_master_file= Path(path_13_master_input, 'Child Activity Master File.xlsx')
    df_child_master_file = pd.read_excel(path_child_master_file)

    path_adult_master_file= Path(path_13_master_input, 'Adult Activity Master File.xlsx')
    df_adult_master_file = pd.read_excel(path_adult_master_file)

#%%### 4. Write to child file 

    df_13_child_act.to_excel(path_child_master_file, engine='openpyxl', mode='a', index=False, sheet_name='Family Wise')
    df_13_base_table.to_excel(path_child_master_file, engine='openpyxl', mode='a', index=False, sheet_name='LLCHD')
    df_13_well_child_FW.to_excel(path_child_master_file, engine='openpyxl', mode='a', index=False, sheet_name='Well Child')
    df_13_well_child_LL.to_excel(path_child_master_file, engine='openpyxl', mode='a', index=False, sheet_name='Well Child')
    df_13_child_injury_FW.to_excel(path_child_master_file, engine='openpyxl', mode='a', index=False, sheet_name='ER Injury')
    df_13_child_injury_LL.to_excel(path_child_master_file, engine='openpyxl', mode='a', index=False, sheet_name='ER Injury')

else:
    0

df_adult_master_file

df_13_child_id= df_13FW_adult_act[['Project ID','MOB ZIP']]
#%%### 3. Remove any rows with a MaxofHVDate before the current report year 
df_13_before_child_act=df_13_child_act.copy()

df_13_child_act = df_13_child_act[df_13_child_act['MaxOfHVDate'] >= pd.Timestamp(f'2023-10-01')]
df_13_child_act['MaxOfHVDate'].astype('datetime64[ns]')

# if (df_13_before_child_act.equals(df_13_after_child_act)):
#     print('error, columns unchanged')
# else:
#     df_13_child_act=df_13_after_child_act.copy()
df_13_child_act.columns

######################################
### >>> df_13_5: 'Adult Activity Export'.
##

#%%### 1. 1. Remove any rows with a discharge date (TERMINATION DATE) before the current reporting year  
df_13_adult_act = df_13_adult_act[(df_13_adult_act['TERMINATION DATE'] >= pd.Timestamp(f'2023-10-01')) | (df_13_adult_act['TERMINATION DATE'].isna())]

df_13_adult_act['TERMINATION DATE'].astype('datetime64[ns]')
#%%### 2. Remove rows that do not have a first home visit date OR Max Visit Number is 0 or blank 
df_13_adult_act = (
    df_13_adult_act.dropna(subset=['MinOfHVDate'], inplace=False)
)
df_13_adult_act = (
    df_13_adult_act.dropna(subset=['MaxOfVISIT NUMBER'], inplace=False)
)
df_13_adult_act= (
    df_13_adult_act[df_13_adult_act['MaxOfVISIT NUMBER'] !=0]
)
df_13_adult_act['MaxOfVISIT NUMBER']
#%%### 3. Remove any rows with a MaxofHVDate before the current report year 
df_13_adult_act = df_13_adult_act[df_13_adult_act['MaxOfHVDate'] >= pd.Timestamp(f'2023-10-01')]
df_13_adult_act['MaxOfHVDate'].astype('datetime64[ns]')



######################################
### >>> 'Referral Exclusions 1 thru 6' VLOOKUP with 'Child Activity Export'.
#%%### 1. - Use VLOOKUP to pull exclusion 4 into the Child Activity spreadsheet

# Merge the DataFrames based on the common column ('Project ID')
df_13_ref_excl_columns_c = df_13_ref_excl[['Project ID','need_ex4']]
df_13_child_act = pd.merge(df_13_child_act, df_13_ref_excl_columns_c, on=['Project ID'], how='left')
df_13_child_act
df_13_child_act.columns

#%%### 2. -  Use VLOOKUP on Child Activity for ZIP from Adult Activity 

# Merge the DataFrames based on the common column ('Project ID')
df_13_adult_columns = df_13_adult_act[['Project ID','MOB ZIP']]
df_13_child_act = pd.merge(df_13_child_act, df_13_adult_columns, on=['Project ID'], how='left')
df_13_child_act
df_13_child_act.columns

df_13_child_act=df_13_child_act.drop_duplicates()

######################################
### >>> 'Referral Exclusions 1 thru 6' VLOOKUP with 'Adult Activity Export'.
#%%### 1. - Use VLOOKUP to pull exclusions 1, 2, 3, 5 , and 6 into the Adult Activity spreadsheet (Note: Exclusion 4 is on the Child Activity export)

# Merge the DataFrames based on the common column ('Project ID')
df_13_ref_excl_columns_a = df_13_ref_excl[['Project ID','need_ex1','need_ex2', 'need_ex3', 'need_ex5', 'need_ex6']]
df_13_adult_act= pd.merge(df_13_adult_act, df_13_ref_excl_columns_a, on=['Project ID'], how='left')
print(df_13_adult_act.columns)
df_13_adult_act

######################################
### >>> 'Child Activity Export' ZIP Code change. --MOB ZIP already brought over so not necessary



######################################
### >>> 'Adult UNCOPE Query' Inclusion.
#%%### 1. - Take UNCOPE columns and insert into Adult Activities
df_13_adult_uncope_columns = df_13_adult_uncope[['Project ID','DATEUNCOPE', 'U', 'N', 'C', 'O', 'P', 'E', 'ReferralDATE', 'Category']]
df_13_adult_act = pd.merge(df_13_adult_act, df_13_adult_uncope_columns, on=['Project ID'], how='left')
df_13_adult_act

######################################
### >>> Virtual Visits' Inclusion.
#%%### 1. - Take Virtual Visit columns and insert into Adult Activities
df_13_home_visit_columns = df_13_home_visit[['Project ID','HomeVisitTypeIP', 'HomeVisitTypeAll']]
df_13_adult_act = pd.merge(df_13_adult_act, df_13_home_visit_columns, on=['Project ID'], how='left')
df_13_adult_act
#%%### 2. - Subtract new columns in adult to create column ‘HomeVisitTypeV’
df_13_adult_act['HomeVisitTypeV'] = df_13_adult_act['HomeVisitTypeAll'] - df_13_adult_act['HomeVisitTypeIP']
df_13_adult_act

df_13_adult_act=df_13_adult_act.drop_duplicates()

### TODO: put in documentation:
### tgt = child
### mob = primary caregiver
### fob = secondary caregiver
### Expectation of target children: only first child (unless multiples); secondary children not tracked.

#%%###################################
### <> Inspect DFs

#%%
inspect_df(df_13_child_act)
#%%
inspect_df(df_13_adult_act)
#%%
inspect_df(df_13_child_injury)

#%%##############################################!>>>
### >>> RESTRUCTURING  
#####################################################

### Compare to files here: 
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.2LL\previous\before restructure\Y13Q1 (Oct 2023 - Dec 2023) 
    ### U:\Working\nehv_ds_data_files\2mid\1main\1.3combine\previous\after restructuring\Y13Q1 (Oct 2023 - Dec 2023) 

#%%###################################
### <> 08 Child ER Injury.xlsx

### Add funding column:
df_13_child_injury['funding']='fixthislater'
### Pivot the DataFrame:
df_13_pivoted_child_injury = df_13_child_injury.pivot_table(
    index=['Project ID', 'agency', 'FAMILY NUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_13_child_injury.groupby(['Project ID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['ERVisitReason', 'IncidentDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
df_13_pivoted_child_injury

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_13_pivoted_child_injury = df_13_pivoted_child_injury.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
df_13_pivoted_child_injury

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_13_pivoted_child_injury.columns = [f'{col[0]}.{col[1]}' for col in df_13_pivoted_child_injury.columns]
df_13_pivoted_child_injury

#%%
### Reset row & column indices:
df_13_pivoted_child_injury = df_13_pivoted_child_injury.reset_index()
df_13_pivoted_child_injury

#%%###################################
### <> Caregiver Insurance v2 - USE THIS ONE.xlsx

### Add funding column:
df_13_cg_ins['funding']='fixthislater'
### Pivot the DataFrame:
df_13_pivoted_cg_ins = df_13_cg_ins.pivot_table(
    index=['Project ID', 'agency', 'FAMILY NUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_13_cg_ins.groupby(['Project ID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['AD1PrimaryIns', 'AD1InsChangeDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
df_13_pivoted_cg_ins

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_13_pivoted_cg_ins = df_13_pivoted_cg_ins.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
df_13_pivoted_cg_ins

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_13_pivoted_cg_ins.columns = [f'{col[0]}.{col[1]}' for col in df_13_pivoted_cg_ins.columns]
df_13_pivoted_cg_ins

#%%
### Reset row & column indices:
df_13_pivoted_cg_ins = df_13_pivoted_cg_ins.reset_index()
df_13_pivoted_cg_ins

#%%###################################
### <> WellChildVisits 

### Add funding column:
df_13_well_child['funding']='fixthislater'
### Pivot the DataFrame:
df_13_pivoted_well_child = df_13_well_child.pivot_table(
    index=['Project ID', 'agency','FAMILY NUMBER', 'ChildNumber', 'funding'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_13_well_child.groupby(['Project ID']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['WellVisitDate'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
# df_13_pivoted_well_child

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_13_pivoted_well_child = df_13_pivoted_well_child.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 
# df_13_pivoted_well_child

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_13_pivoted_well_child.columns = [f'{col[0]}.{col[1]}' for col in df_13_pivoted_well_child.columns]
# df_13_pivoted_well_child

#%%
### Reset row & column indices:
df_13_pivoted_well_child = df_13_pivoted_well_child.reset_index()
# df_13_pivoted_well_child



#%%##############################################!>>>
### >>> WRITE OUT FILES   
#####################################################

### Note: Date columns written out without timestamps.

df_13_adult_act.to_csv(Path(path_13_dir_output, 'df_13_adult_act.csv'), index = False, date_format="%m/%d/%Y")
df_13_child_act.to_csv(Path(path_13_dir_output, 'df_13_child_act.csv'), index = False, date_format="%m/%d/%Y")
df_13_pivoted_child_injury.to_csv(Path(path_13_dir_output, 'df_13_pivoted_child_injury.csv'), index = False, date_format="%m/%d/%Y")
df_13_pivoted_cg_ins.to_csv(Path(path_13_dir_output, 'df_13_pivoted_cg_ins.csv'), index = False, date_format="%m/%d/%Y")
df_13_pivoted_well_child.to_csv(Path(path_13_dir_output, 'df_13_pivoted_well_child.csv'), index = False, date_format="%m/%d/%Y")

#%%##############################################!>>>
### >>> Remove old objects  
#####################################################

#%%
[o for o in list(globals().keys()) if o.startswith(('date', 'int', 'path', 'str'))]
### Keep.

#%%
# [o for o in list(globals().keys()) if o.startswith('df')]
#%%
del df_13_allstring_1, df_13_allstring_2, df_13_allstring_3, df_13_allstring_4,  df_13_child_act, df_13_child_injury, df_13_cg_ins, df_13_well_child

#%%
# [o for o in list(globals().keys()) if o.startswith('dict')]
#%%
del dict_13_col_dtypes_1, dict_13_col_dtypes_2, dict_13_col_dtypes_3, dict_13_col_dtypes_4 

#%%
# [o for o in list(globals().keys()) if o.startswith('list')]
#%%
del list_13_col_detail_1, list_13_date_cols_1, list_13_col_detail_2, list_13_date_cols_2, list_13_col_detail_3, list_13_date_cols_3, list_13_col_detail_4, list_13_date_cols_4, list_13_values_to_find_and_replace 

#%%
# [o for o in list(globals().keys()) if o.startswith(('regex', 'xlsx'))]
#%%
del  regex_13_dates_to_fix, regex_13_dates_replacement 

#%%
### Is what's left over what is wanted?:
[o for o in list(globals().keys()) if o.startswith(('df', 'dict', 'list', 'regex', 'xlsx'))]
### Should only be:
# ['df_13_child_act',
#  'df_13_pivoted_child_injury',
#  'df_13_pivoted_cg_ins',
#  'df_13_pivoted_well_child']



#%%##############################################!>>>
### >>> END 
#################################################!>>>

print('Congrats! You ran 1.1.2 FW!')

# %%
### TODO:
### Remove duplciate rows (like how tried in 1.4 code).
### see if "logger" package would be useful.




