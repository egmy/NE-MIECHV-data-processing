
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%##############################################!>>>
### >>>  INSTRUCTIONS 
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.

#%%##############################################!>>> 
### >>>  SETUP 
#####################################################

from _1_3_RUNME import * 

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
pd.set_option('display.max_columns', None)
pd.__version__

#%%##############################################!>>>
### >>> READ 
#####################################################
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
### >>> COMBINING
#####################################################

######################################
#%%### 1. Add 'year' and 'quarter' columns to all FamilyWise dataframes

# int_nehv_quarter = 2
# str_nehv_quarter = 'Y13Q2 (Oct 2023 - Mar 2024)'

#%%
### TODO: Move creation of year & quarter columns to 1.1.2.

df_13_child_act.insert(loc=1, column='year', value=int_nehv_year)
df_13_child_act.insert(loc=2, column='quarter', value=int_nehv_quarter)

df_13_adult_act.insert(loc=1, column='year', value=int_nehv_year)
df_13_adult_act.insert(loc=2, column='quarter', value=int_nehv_quarter)

df_13_well_child_FW.insert(loc=1, column='year', value=int_nehv_year)
df_13_well_child_FW.insert(loc=2, column='quarter', value=int_nehv_quarter)

df_13_child_injury_FW.insert(loc=1, column='year', value=int_nehv_year)
df_13_child_injury_FW.insert(loc=2, column='quarter', value=int_nehv_quarter)

df_13_cg_ins_FW.insert(loc=1, column='year', value=int_nehv_year)
df_13_cg_ins_FW.insert(loc=2, column='quarter', value=int_nehv_quarter)




#%%### 2. Create Project ID sheet for adult and child sheets
child_frames = [df_13_child_act[['Project ID']], df_13_base_table[['project_id']]]
df_child_project_id=pd.concat(child_frames)

#%%
df_child_project_id['project_id_new']=df_child_project_id['Project ID'].combine_first(df_child_project_id['project_id']).astype('string') 

#%%
df_child_project_id = df_child_project_id.drop(columns=['project_id', 'Project ID'])
df_child_project_id.rename(columns={"project_id_new": "project_id"}, inplace=True)


#%%
df_child_project_id['year']=int_nehv_year
df_child_project_id['quarter']=int_nehv_quarter

adult_frames = [df_13_adult_act[['Project ID']], df_13_base_table[['project_id']]]
df_adult_project_id=pd.concat(adult_frames)
#%%
df_adult_project_id['project_id_new']=df_adult_project_id['Project ID'].combine_first(df_adult_project_id['project_id']).astype('string')
#%%
df_adult_project_id = df_adult_project_id.drop(columns=['project_id', 'Project ID'])
df_adult_project_id.rename(columns={"project_id_new": "project_id"}, inplace=True)

#%%

df_adult_project_id['year']=int_nehv_year
df_adult_project_id['quarter']=int_nehv_quarter
df_adult_project_id['join_id']=1

#%%
### Reading previous quarter's Master Files:
path_master_file=Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.4tableau')
path_13_master_input = Path(path_master_file, '0in', str_nehv_quarter)

path_child_master_file= Path(path_13_master_input, 'Child Activity Master File.xlsx')
df_child_master_file = pd.read_excel(path_child_master_file)


path_adult_master_file= Path(path_13_master_input, 'Adult Activity Master File.xlsx')
df_adult_master_file = pd.read_excel(path_adult_master_file)

print(df_adult_master_file)

#%%### 3. restructure to combine frames for caregiver insurance sheet for Adult Activity
df_13_cg_ins_FW.rename(columns={"Project ID": "ProjectID"}, inplace=True)
df_13_cg_ins_FW.rename(columns={"FAMILY NUMBER": "FAMILYNUMBER"}, inplace=True)

frames=[df_13_cg_ins_LL, df_13_cg_ins_FW]

df_13_cg_ins=pd.concat(frames)


#%%### 4. restructure to combine frames for well child and ER Injury sheets for Adult Activity
df_13_well_child_FW.rename(columns={"Project ID": "ProjectID"}, inplace=True)
df_13_well_child_FW.rename(columns={"FAMILY NUMBER": "FAMILYNUMBER"}, inplace=True)

frames=[df_13_well_child_LL, df_13_well_child_FW]

df_13_well_child=pd.concat(frames)

df_13_child_injury_FW.rename(columns={"Project ID": "ProjectID"}, inplace=True)
df_13_child_injury_FW.rename(columns={"FAMILY NUMBER": "FAMILYNUMBER"}, inplace=True)

frames=[df_13_child_injury_FW, df_13_child_injury_LL]

df_13_child_injury=pd.concat(frames)



#%%### 5. Create FOB and DOB sheet for Adult Activity
df_13_mob_fob=pd.DataFrame({
    "join_id": [1, 1],
    "MOB_or_FOB": ["MOB", "FOB"]
})


print(int_nehv_quarter)


## change this method to read-in old files and append new ones, then rewrite

#%%### 6. if not 1st quarter, write to existing files
if int_nehv_quarter!=1:
    ### 7. Pull from existing file and append new quarter to old file, write new combined to output location for Child file
    df_13_child_base_table_previous = pd.read_excel(path_child_master_file, sheet_name='LLCHD', keep_default_na=False, na_values=[''])
    df_13_child_base_table = pd.concat([df_13_child_base_table_previous, df_13_base_table], ignore_index=True)
    df_13_child_base_table = df_13_child_base_table.drop_duplicates()

    df_13_child_act_previous = pd.read_excel(path_child_master_file, sheet_name='Family Wise', keep_default_na=False, na_values=[''])
    df_13_child_act = pd.concat([df_13_child_act_previous, df_13_child_act], ignore_index=True)
    df_13_child_act = df_13_child_act.drop_duplicates()

    df_13_well_child_previous = pd.read_excel(path_child_master_file, sheet_name='Well Child', keep_default_na=False, na_values=[''])
    df_13_well_child = pd.concat([df_13_well_child_previous, df_13_adult_act], ignore_index=True)
    df_13_well_child = df_13_well_child.drop_duplicates()

    df_13_child_injury_previous = pd.read_excel(path_child_master_file, sheet_name='ER Injury', keep_default_na=False, na_values=[''])
    df_13_child_injury = pd.concat([df_13_child_injury_previous, df_13_adult_act], ignore_index=True)
    df_13_child_injury  = df_13_child_injury.drop_duplicates()

    df_13_child_id_previous = pd.read_excel(path_child_master_file, sheet_name='Project ID', keep_default_na=False, na_values=[''])
    df_child_project_id = pd.concat([df_13_child_id_previous, df_child_project_id], ignore_index=True)
    df_child_project_id = df_child_project_id.drop_duplicates()

    ### 8. repeat for Adult files
    df_13_adult_base_table_previous = pd.read_excel(path_adult_master_file, sheet_name='LLCHD', keep_default_na=False, na_values=[''])
    df_13_adult_base_table = pd.concat([df_13_adult_base_table_previous, df_13_base_table], ignore_index=True)
    df_13_adult_base_table = df_13_adult_base_table.drop_duplicates()

    df_13_adult_act_previous = pd.read_excel(path_adult_master_file, sheet_name='Family Wise', keep_default_na=False, na_values=[''])
    df_13_adult_act = pd.concat([df_13_adult_act_previous, df_13_adult_act], ignore_index=True)
    df_13_adult_act = df_13_adult_act.drop_duplicates()

    df_13_adult_id_previous = pd.read_excel(path_adult_master_file, sheet_name='Project ID', keep_default_na=False, na_values=[''])
    df_adult_project_id = pd.concat([df_13_adult_id_previous, df_adult_project_id], ignore_index=True)
    df_adult_project_id = df_adult_project_id.drop_duplicates()

    df_13_cg_ins_previous = pd.read_excel(path_adult_master_file, sheet_name='Caregiver Insurance', keep_default_na=False, na_values=[''])
    df_13_cg_ins = pd.concat([df_13_cg_ins_previous, df_13_cg_ins], ignore_index=True)
    df_13_cg_ins = df_13_cg_ins.drop_duplicates()



    with pd.ExcelWriter(Path(path_13_dir_output, 'Child Activity Master File.xlsx'), engine='openpyxl') as writer:
        df_13_child_act.to_excel(writer, index=False, sheet_name='Family Wise')
        df_13_child_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
        df_13_well_child.to_excel(writer, index=False, sheet_name='Well Child')
        df_13_child_injury.to_excel(writer, index=False, sheet_name='ER Injury')
        df_child_project_id.to_excel(writer, index=False, sheet_name='Project ID')
    

    with pd.ExcelWriter(Path(path_13_dir_output, 'Adult Activity Master File.xlsx'), engine='openpyxl') as writer:
        df_13_adult_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
        df_13_adult_act.to_excel(writer, index=False, sheet_name='Family Wise')
        df_adult_project_id.to_excel(writer, index=False, sheet_name='Project ID')
        df_13_mob_fob.to_excel(writer, index=False, sheet_name='MOB or FOB')
        df_13_cg_ins.to_excel(writer,index=False, sheet_name='Caregiver Insurance')

else:
    ### 8. Otherwise, if Q1, write to new file 
    with pd.ExcelWriter(Path(path_13_dir_output, 'Child Activity Master File.xlsx'), engine='openpyxl') as writer:
        df_13_child_act.to_excel(writer, index=False, sheet_name='Family Wise')
        df_13_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
        df_13_well_child.to_excel(writer, index=False, sheet_name='Well Child')
        df_13_child_injury.to_excel(writer, index=False, sheet_name='ER Injury')
        df_child_project_id.to_excel(writer, index=False, sheet_name='Project ID')

    with pd.ExcelWriter(Path(path_13_dir_output, 'Adult Activity Master File.xlsx'), engine='openpyxl') as writer:
        df_13_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
        df_13_adult_act.to_excel(writer, index=False, sheet_name='Family Wise')
        df_adult_project_id.to_excel(writer, index=False, sheet_name='Project ID')
        df_13_mob_fob.to_excel(writer, index=False, sheet_name='MOB or FOB')
        df_13_cg_ins.to_excel(writer,index=False, sheet_name='Caregiver Insurance')


#%%##############################################!>>>
### >>> Remove old objects  
#####################################################

#%%
[o for o in list(globals().keys()) if o.startswith(('date', 'int', 'path', 'str'))]
### Keep.

#%%
# [o for o in list(globals().keys()) if o.startswith('df')]
#%%
del  df_13_child_act, df_13_base_table,  df_13_well_child_FW, df_13_well_child_LL

#%%
# [o for o in list(globals().keys()) if o.startswith('dict')]
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




