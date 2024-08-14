
### Purpose: 

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.


#%%##################################################
### PACKAGES & FUNCTIONS ###
#####################################################

from pathlib import Path
import os
import sys
path_1_1FW=Path(os.path.dirname(Path.cwd()))/'1.1FW/1.1.2other'
path_1_2LL=Path(os.path.dirname(Path.cwd()))/'1.2LL/'
sys.path+=[str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']),str(path_1_1FW), str(path_1_2LL)]#'C:\\Users\\Eric.Myers\\git\\nehv_ds_code_repository\\code\\1main\\1.1FW\\1.1.2other']str(*[d for d in os.listdir(Path.cwd()) if os.path.isdir(d)])])
from RUNME import * 
print('Local Code Repository: ', str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))

#%%
# from importlib import import_module
# mod = import_module("_1_1FW_raw_to_master.py")
# var = getattr(mod, "df_112FW_pivoted_well_child")
if read_from_file==False:
    from _1_1_2FW_RUNME import df_112FW_pivoted_well_child, df_112FW_adult_act, df_112FW_child_act, df_112FW_pivoted_child_injury, df_112FW_pivoted_cg_ins
    from _1_2LL_raw_to_master import df_12LL_BaseTable, df_12LL_pivoted_ChildERInj_2,df_12LL_pivoted_MaternalIns_3,df_12LL_pivoted_WellChildVisits_4  


#%%##################################################
### PATHS ###
#####################################################
### TODO: Bring up all 1.3 paths to here.
### TODO: Also, keep 1.3 siloed: Only read in & out of 1.3 folders. Manually copy files as needed between silos. (We'll use other code to copy between silos.)

#U:\Working\nehv_ds_data_files\2mid\1main\1.3combine\0in\Y13Q1 (Oct 2023 - Dec 2023)
path_13_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.3combine')
path_14t_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.4tableau')

path_13_dir_input = Path(path_13_files_base, '0in', str_nehv_quarter)
path_13_dir_mid = Path(path_13_files_base, '2mid', str_nehv_quarter)
path_13_dir_output = Path(path_13_files_base, '9out', str_nehv_quarter)
path_13_backup=Path(path_13_files_base, 'backup')
path_14t_dir_input = Path(path_14t_files_base, '0in', str_nehv_quarter)

###########################

### Input:
### U:\Working\Tableau\Y## (<date_range>)\Y##Q# (<date_range>)\LLCHD ### oldest file.
### U:\SFTP ### Should see same file here.
# path_13_input_raw = Path(path_13_dir_input, 'Flatfile_CHSR_231001.xlsx')
path_13_input_well_child_FW = Path(path_13_dir_input, 'df_112FW_pivoted_well_child.csv')
path_13_input_well_child_LL = Path(path_13_dir_input, 'df_12LL_pivoted_WellChildVisits_4.csv')
path_13_input_child_injury_FW = Path(path_13_dir_input, 'df_112FW_pivoted_child_injury.csv')
path_13_input_child_injury_LL = Path(path_13_dir_input, 'df_12LL_pivoted_ChildERInj_2.csv')
path_13_input_cg_ins_FW = Path(path_13_dir_input, 'df_112FW_pivoted_cg_ins.csv')
path_13_input_cg_ins_LL = Path(path_13_dir_input, 'df_12LL_pivoted_MaternalIns_3.csv')
path_13_input_adult_act= Path(path_13_dir_input, 'df_112FW_adult_act.csv')
path_13_input_child_act = Path(path_13_dir_input, 'df_112FW_child_act.csv')
path_13_input_base_table = Path(path_13_dir_input, 'df_12LL_BaseTable.csv')


### Output:
path_13_output = Path(path_13_dir_output, 'LL and FW combined.csv')



#%%##################################################
### END of Setup ###
#####################################################

print('end setup')


#%%##################################################
### RUN CODE FILES ###
#####################################################

### The following is run if running this file by itself interactively (& ignored when run from one of the code files):
    ### Using exec() instead of import so that code files can "see" packages, functions, & any objects created in RUNME.

# if (os.path.basename(__file__) in ('_1_3_RUNME.py', 'RUNME.py') and __name__ == "__main__"):
#     from _1_3_combine import *
#     print('Executed code files')

pd.set_option('display.max_columns', None)
pd.__version__

#%%##############################################!>>>
### >>> READ 
#####################################################
### READ in all sheets.

### Read in EVERYTHING WITH pd.NA for empty cells:
if read_from_file==True:
    df_13_well_child_FW = pd.read_csv(path_13_input_well_child_FW, keep_default_na=False, na_values=[''])
    df_13_well_child_LL = pd.read_csv(path_13_input_well_child_LL, keep_default_na=False, na_values=[''])
    df_13_child_injury_FW = pd.read_csv(path_13_input_child_injury_FW, keep_default_na=False, na_values=[''])
    df_13_child_injury_LL= pd.read_csv(path_13_input_child_injury_LL, keep_default_na=False, na_values=[''])
    df_13_cg_ins_FW = pd.read_csv(path_13_input_cg_ins_FW, keep_default_na=False, na_values=[''])
    df_13_cg_ins_LL = pd.read_csv(path_13_input_cg_ins_LL, keep_default_na=False, na_values=[''])
    df_13_adult_act = pd.read_csv(path_13_input_adult_act, keep_default_na=False, na_values=[''])
    df_13_child_act = pd.read_csv(path_13_input_child_act, keep_default_na=False, na_values=[''])
    df_13_base_table = pd.read_csv(path_13_input_base_table, keep_default_na=False, na_values=[''])
else:
    df_13_well_child_FW=df_112FW_pivoted_well_child 
    df_13_well_child_LL=df_12LL_pivoted_WellChildVisits_4
    df_13_child_injury_FW=df_112FW_pivoted_child_injury
    df_13_child_injury_LL=df_12LL_pivoted_ChildERInj_2
    df_13_cg_ins_FW=df_112FW_pivoted_cg_ins
    df_13_cg_ins_LL=df_12LL_pivoted_MaternalIns_3
    df_13_adult_act=df_112FW_adult_act
    df_13_child_act=df_112FW_child_act
    df_13_base_table=df_12LL_BaseTable

#%%##############################################!>>>
### >>> COMBINING
#####################################################

######################################

#%%### 1. Create Project ID sheet for adult and child sheets
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

#%%### 2. restructure to combine frames for caregiver insurance sheet for Adult Activity
df_13_cg_ins_FW.rename(columns={"Project ID": "ProjectID"}, inplace=True)
df_13_cg_ins_FW.rename(columns={"FAMILY NUMBER": "FAMILYNUMBER"}, inplace=True)

frames=[df_13_cg_ins_LL, df_13_cg_ins_FW]

df_13_cg_ins=pd.concat(frames)


#%%### 3. restructure to combine frames for well child and ER Injury sheets for Adult Activity
df_13_well_child_FW.rename(columns={"Project ID": "ProjectID"}, inplace=True)
df_13_well_child_FW.rename(columns={"FAMILY NUMBER": "FAMILYNUMBER"}, inplace=True)

frames=[df_13_well_child_LL, df_13_well_child_FW]

df_13_well_child=pd.concat([df_13_well_child_LL, df_13_well_child_FW])

df_13_child_injury_FW.rename(columns={"Project ID": "ProjectID"}, inplace=True)
df_13_child_injury_FW.rename(columns={"FAMILY NUMBER": "FAMILYNUMBER"}, inplace=True)
df_13_child_injury_FW.rename(columns={"IncidentDate.1": "IncidentDate"}, inplace=True)
df_13_child_injury_FW.rename(columns={"ERVisitReason.1": "ERVisitReason"}, inplace=True)
df_13_child_injury_FW=df_13_child_injury_FW.drop(columns={"ERVisitReason"})


df_13_child_injury_LL.rename(columns={"IncidentDate.1": "IncidentDate"}, inplace=True)
df_13_child_injury_LL.rename(columns={"ERVisitReason.1": "ERVisitReason"}, inplace=True)
df_13_child_injury_LL=df_13_child_injury_LL.drop(columns={"ERVisitReason"})
df_13_child_injury=pd.concat([df_13_child_injury_FW, df_13_child_injury_LL])

df_13_adult_act.rename(columns={"EDCDate": "EDC Date"}, inplace=True)
df_13_adult_act.rename(columns={"need_ex1": "need_exclusion1"}, inplace=True)
df_13_adult_act.rename(columns={"need_ex2": "need_exclusion2"}, inplace=True)
df_13_adult_act.rename(columns={"need_ex2": "need_exclusion2"}, inplace=True)
df_13_adult_act.rename(columns={"need_ex3": "need_exclusion3"}, inplace=True)
df_13_adult_act.rename(columns={"need_ex5": "need_exclusion5"}, inplace=True)
df_13_adult_act.rename(columns={"need_ex6": "need_exclusion6"}, inplace=True)
df_13_adult_act.rename(columns={"Home Visits Prenatal": "HomeVisitsPrenatal"}, inplace=True)
#df_13_adult_act['IPVRefDate']=pd.to_datetime(df_13_adult_act['IPVRefDate'])
df_13_adult_act.rename(columns={"ReferralDATE": "IPVRefDate"}, inplace=True)
df_13_adult_act.rename(columns={"Category": "UNCOPERefCategory"}, inplace=True)
df_13_adult_act.rename(columns={"Home Visits Total": "HomeVisitsTotal"}, inplace=True)
df_13_adult_act.rename(columns={"A1IPV": "AssessIPV"}, inplace=True)
df_13_adult_act.rename(columns={"A1Police": "AssessPolice"}, inplace=True)
df_13_adult_act.rename(columns={"A1Afraid": "AssessAfraid"}, inplace=True)
df_13_adult_act.rename(columns={"MOB ASSESSMENT DATE": "IPV Assess Date"}, inplace=True)
def same_merge(x): return ','.join(x[x.notnull()].astype(str))
df_13_adult_act = df_13_adult_act.groupby(level=0, axis=1).apply(lambda x: x.apply(same_merge, axis=1))

def keep_last_item(cell_value):
    # Split the cell value by comma and strip any leading/trailing spaces
    if not pd.isna(cell_value):  # Check if cell_value is not NaN or NaT
            items = [item.strip() for item in str(cell_value).split(',') if item.strip()]
            return items[-1] if items else np.nan  # Return last item or NaN if no items after stripping
    else:
        return pd.NA
def last_item_merge(x):
    return x.apply(keep_last_item)

df_13_adult_act = df_13_adult_act.apply(last_item_merge)
df_13_adult_act['AssessIPV'] = df_13_adult_act['AssessIPV'].astype(str).replace({'True': True, 'False': False})
df_13_adult_act['AssessIPV'] = df_13_adult_act['AssessIPV'].astype('Int64')
df_13_adult_act['AssessPolice'] = df_13_adult_act['AssessPolice'].astype(str).replace({'True': True, 'False': False})
df_13_adult_act['AssessPolice'] = df_13_adult_act['AssessPolice'].astype('Int64')
df_13_adult_act['AssessAfraid'] = df_13_adult_act['AssessAfraid'].astype(str).replace({'True': True, 'False': False})
df_13_adult_act['AssessAfraid'] = df_13_adult_act['AssessAfraid'].astype('Int64')



df_13_child_act.rename(columns={"MaxOfHVDate": "MaxofHVDate"}, inplace=True)
df_13_child_act.rename(columns={"EDCDate": "EDC Date"}, inplace=True)
df_13_child_act.rename(columns={"MOB ZIP": "ZIP Code"}, inplace=True)
df_13_child_act['ZIP Code'] = pd.to_numeric(df_13_child_act['ZIP Code'], errors='coerce')
df_13_child_act.rename(columns={"MaxOfReadToChild": "ReadTellStorySing"}, inplace=True)
df_13_child_act.rename(columns={"TGT GENDER": "TGT Gender"}, inplace=True)
df_13_child_act.rename(columns={"need_ex4": "need_exclusion4"}, inplace=True)


#%%### 4. Create FOB and DOB sheet for Adult Activity
df_13_mob_fob=pd.DataFrame({
    "join_id": [1, 1],
    "MOB_or_FOB": ["MOB", "FOB"]
})


print(int_nehv_quarter)


## change this method to read-in old files and append new ones, then rewrite

#%%### 5. if not 1st quarter, write to existing files
if int_nehv_quarter!=1:
    
    ### Reading previous quarter's Master Files:
    #path_13_master_input=Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.4tableau\\0in\\Y13Q1 (Oct 2023 - Dec 2023)')
    path_13_master_input = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.4tableau\\0in', previous_str_nehv_quarter)

    path_child_master_file= Path(path_13_master_input, 'Child Activity Master File.xlsx')
    df_child_master_file = pd.read_excel(path_child_master_file)


    path_adult_master_file= Path(path_13_master_input, 'Adult Activity Master File.xlsx')
    df_adult_master_file = pd.read_excel(path_adult_master_file)


    ### 6. Pull from existing file and append new quarter to old file, write new combined to output location for Child file
    df_13_child_base_table_previous = pd.read_excel(path_child_master_file, sheet_name='LLCHD', keep_default_na=False, na_values=[''])
    df_13_child_base_table = pd.concat([df_13_child_base_table_previous, df_13_base_table], ignore_index=True)
    df_13_child_base_table = df_13_child_base_table.drop_duplicates()

    df_13_child_act_previous = pd.read_excel(path_child_master_file, sheet_name='Family Wise', keep_default_na=False, na_values=[''])
    df_13_child_act = pd.concat([df_13_child_act_previous, df_13_child_act], ignore_index=True)
    df_13_child_act = df_13_child_act.drop_duplicates()

    df_13_well_child_previous = pd.read_excel(path_child_master_file, sheet_name='Well Child', keep_default_na=False, na_values=[''])
    df_13_well_child = pd.concat([df_13_well_child_previous, df_13_well_child], ignore_index=True)
    df_13_well_child = df_13_well_child.drop_duplicates()

    df_13_child_injury_previous = pd.read_excel(path_child_master_file, sheet_name='ER Injury', keep_default_na=False, na_values=[''])
    df_13_child_injury_previous.rename(columns={"Project ID": "ProjectID"}, inplace=True)
    df_13_child_injury_previous.rename(columns={"FAMILY NUMBER": "FAMILYNUMBER"}, inplace=True)
    df_13_child_injury = pd.concat([df_13_child_injury_previous, df_13_child_injury], ignore_index=True)
    df_13_child_injury  = df_13_child_injury.drop_duplicates()

    df_13_child_id_previous = pd.read_excel(path_child_master_file, sheet_name='Project ID', keep_default_na=False, na_values=[''])
    df_child_project_id = pd.concat([df_13_child_id_previous, df_child_project_id], ignore_index=True)
    df_child_project_id = df_child_project_id.drop_duplicates()

    ### 7. repeat for Adult files
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



    # with pd.ExcelWriter(Path(path_13_dir_output, 'Child Activity Master File.xlsx'), engine='openpyxl') as writer:
    #     df_13_child_act.to_excel(writer, index=False, sheet_name='Family Wise')
    #     df_13_child_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
    #     df_13_well_child.to_excel(writer, index=False, sheet_name='Well Child')
    #     df_13_child_injury.to_excel(writer, index=False, sheet_name='ER Injury')
    #     df_child_project_id.to_excel(writer, index=False, sheet_name='Project ID')
    

    # with pd.ExcelWriter(Path(path_13_dir_output, 'Adult Activity Master File.xlsx'), engine='openpyxl') as writer:
    #     df_13_adult_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
    #     df_13_adult_act.to_excel(writer, index=False, sheet_name='Family Wise')
    #     df_adult_project_id.to_excel(writer, index=False, sheet_name='Project ID')
    #     df_13_mob_fob.to_excel(writer, index=False, sheet_name='MOB or FOB')
    #     df_13_cg_ins.to_excel(writer,index=False, sheet_name='Caregiver Insurance')
    ## write to 1.4 input
    with pd.ExcelWriter(Path(path_14t_dir_input, 'Child Activity Master File.xlsx'), engine='openpyxl') as writer:
        df_13_child_act.to_excel(writer, index=False, sheet_name='Family Wise')
        df_13_child_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
        df_13_well_child.to_excel(writer, index=False, sheet_name='Well Child')
        df_13_child_injury.to_excel(writer, index=False, sheet_name='ER Injury')
        df_child_project_id.to_excel(writer, index=False, sheet_name='Project ID')
    

    with pd.ExcelWriter(Path(path_14t_dir_input, 'Adult Activity Master File.xlsx'), engine='openpyxl') as writer:
        df_13_adult_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
        df_13_adult_act.to_excel(writer, index=False, sheet_name='Family Wise')
        df_adult_project_id.to_excel(writer, index=False, sheet_name='Project ID')
        df_13_mob_fob.to_excel(writer, index=False, sheet_name='MOB or FOB')
        df_13_cg_ins.to_excel(writer,index=False, sheet_name='Caregiver Insurance')
else:
    ### 8. Otherwise, if Q1, write to new file 
    # with pd.ExcelWriter(Path(path_13_dir_output, 'Child Activity Master File.xlsx'), engine='openpyxl') as writer:
    #     df_13_child_act.to_excel(writer, index=False, sheet_name='Family Wise')
    #     df_13_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
    #     df_13_well_child.to_excel(writer, index=False, sheet_name='Well Child')
    #     df_13_child_injury.to_excel(writer, index=False, sheet_name='ER Injury')
    #     df_child_project_id.to_excel(writer, index=False, sheet_name='Project ID')

    # with pd.ExcelWriter(Path(path_13_dir_output, 'Adult Activity Master File.xlsx'), engine='openpyxl') as writer:
    #     df_13_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
    #     df_13_adult_act.to_excel(writer, index=False, sheet_name='Family Wise')
    #     df_adult_project_id.to_excel(writer, index=False, sheet_name='Project ID')
    #     df_13_mob_fob.to_excel(writer, index=False, sheet_name='MOB or FOB')
    #     df_13_cg_ins.to_excel(writer,index=False, sheet_name='Caregiver Insurance')

    ## write to 1.4 input
    with pd.ExcelWriter(Path(path_14t_dir_input, 'Child Activity Master File.xlsx'), engine='openpyxl') as writer:
        df_13_child_act.to_excel(writer, index=False, sheet_name='Family Wise')
        df_13_base_table.to_excel(writer, index=False, sheet_name='LLCHD')
        df_13_well_child.to_excel(writer, index=False, sheet_name='Well Child')
        df_13_child_injury.to_excel(writer, index=False, sheet_name='ER Injury')
        df_child_project_id.to_excel(writer, index=False, sheet_name='Project ID')

    with pd.ExcelWriter(Path(path_14t_dir_input, 'Adult Activity Master File.xlsx'), engine='openpyxl') as writer:
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






#%%##################################################
### END ###
#####################################################

print('end file :)')



# %%

### TODO: Test that 1.4 works with 1.3 output.
