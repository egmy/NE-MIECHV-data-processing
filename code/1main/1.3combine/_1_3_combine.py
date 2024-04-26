
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
pd.__version__

#%%### df_13_8: 'Referral Exclusions 1 thru 6.xlsx'.

#%%##############################################!>>>
### >>> READ 
#####################################################

#%%
### Performance benefit for reading in file to memory only once by creating an ExcelFile class object.


#%%###################################
### READ in all sheets.

### Read in EVERYTHING WITH pd.NA for empty cells:
df_13_well_child_FW = pd.read_csv(path_13_input_well_child_FW, keep_default_na=False, na_values=[''])
df_13_well_child_LL = pd.read_csv(path_13_input_well_child_LL, keep_default_na=False, na_values=[''])
df_13_child_injury_FW = pd.read_csv(path_13_input_child_injury_FW, keep_default_na=False, na_values=[''])
df_13_child_injury_LL= pd.read_csv(path_13_input_child_injury_LL, keep_default_na=False, na_values=[''])
df_13_cg_ins_FW = pd.read_csv(path_13_input_cg_ins_FW, keep_default_na=False, na_values=[''])
df_13_cg_ins_LL = pd.read_csv(path_13_input_cg_ins_LL, keep_default_na=False, na_values=[''])
df_13_adult_act = pd.read_csv(path_13_input_adult_act, keep_default_na=False, na_values=[''])
df_13_child_act = pd.read_csv(path_13_input_child_act, keep_default_na=False, na_values=[''])
df_13_base_table = pd.read_csv(path_13_input_base_table, keep_default_na=False, na_values=[''])
#%%##############################################!>>>
### >>> CLEAN 
#####################################################

# #%%### 1. Strip surrounding whitespace
# df_13_child_act = (
#     df_13_child_act.map(lambda cell: cell.strip(), na_action='ignore')
# )
# df_13_adult_act = (
#     df_13_adult_act.map(lambda cell: cell.strip(), na_action='ignore')
# )
# df_13_well_child_FW = (
#     df_13_well_child_FW.map(lambda cell: cell.strip(), na_action='ignore')
# )
# df_13_well_child_LL = (
#     df_13_well_child_LL.map(lambda cell: cell.strip(), na_action='ignore')
# )
# df_13_child_injury_FW = (
#     df_13_child_injury_FW.map(lambda cell: cell.strip(), na_action='ignore')
# )
# df_13_child_injury_LL = (
#     df_13_child_injury_LL.map(lambda cell: cell.strip(), na_action='ignore')
# )
# df_13_cg_ins_FW = (
#     df_13_cg_ins_FW.map(lambda cell: cell.strip(), na_action='ignore')
# )
# df_13_cg_ins_LL = (
#     df_13_cg_ins_LL.map(lambda cell: cell.strip(), na_action='ignore')
# )
# df_13_base_table = (
#     df_13_base_table.map(lambda cell: cell.strip(), na_action='ignore')
# )

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
df_13_base_table.rename(columns={"project_id": "Project ID"})
child_frames = [df_13_child_act[['Project ID']], df_13_base_table[['Project ID']]]
df_child_project_id=pd.concat(child_frames)


df_child_project_id['year']=nehv_year
df_child_project_id['quarter']=nehv_quarter

adult_frames = [df_13_adult_act[['Project ID']], df_13_base_table[['project_id']]]
df_adult_project_id=pd.concat(adult_frames)

df_adult_project_id['year']=nehv_year
df_adult_project_id['quarter']=nehv_quarter

path_master_file=Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.4Tableau')
path_13_master_input = Path(path_master_file, '0in', str_nehv_quarter)

path_child_master_file= Path(path_13_master_input, 'Child Activity Master File.xlsx')
df_child_master_file = pd.read_excel(path_child_master_file)

path_adult_master_file= Path(path_13_master_input, 'Adult Activity Master File.xlsx')
df_adult_master_file = pd.read_excel(path_adult_master_file)

#%%### 3. if not 1st quarter, write to existing files
if nehv_quarter!=1:

    with pd.ExcelWriter(path_child_master_file, engine='openpyxl', mode='a') as writer:
        df_13_child_act.to_excel(writer, index=False, sheet_name='Family Wise')
        df_13_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
        df_13_well_child_FW.to_excel(writer, index=False, sheet_name='Well Child')
        df_13_well_child_LL.to_excel(writer, index=False, sheet_name='Well Child')
        df_13_child_injury_FW.to_excel(writer, index=False, sheet_name='ER Injury')
        df_13_child_injury_LL.to_excel( writer, index=False, sheet_name='ER Injury')
        df_child_project_id.to_excel(writer, index=False, sheet_name='Project ID')

else:
    with pd.ExcelWriter(Path(path_13_dir_output, 'Child Activity Master File.xlsx'), engine='openpyxl') as writer:
        df_13_child_act.to_excel(writer, index=False, sheet_name='Family Wise')
        df_13_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
        df_13_well_child_FW.to_excel(writer, index=False, sheet_name='Well Child')
        df_13_well_child_LL.to_excel(writer, index=False, sheet_name='Well Child')
        df_13_child_injury_FW.to_excel(writer, index=False, sheet_name='ER Injury')
        df_13_child_injury_LL.to_excel(writer,index=False, sheet_name='ER Injury')
        df_child_project_id.to_excel(writer, index=False, sheet_name='Project ID')



#%%##############################################!>>>
### >>> WRITE OUT FILES   
#####################################################

### Note: Date columns written out without timestamps.

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

print('Congrats! You ran 1.3!')
# %%
### TODO:
### Remove duplciate rows (like how tried in 1.4 code).
### see if "logger" package would be useful.




