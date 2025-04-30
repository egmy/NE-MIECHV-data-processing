
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.


# %% ################################################
### PACKAGES ###
#####################################################

import pandas as pd
from pathlib import Path
import tableauserverclient as TSC
import numpy as np
import sys
from openpyxl import load_workbook
from pyxlsb import open_workbook
import xlwings as xw
sys.path+=[str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository'])]#'C:\\Users\\Eric.Myers\\git\\nehv_ds_code_repository\\code\\1main\\1.1FW\\1.1.2other']str(*[d for d in os.listdir(Path.cwd()) if os.path.isdir(d)])])
from RUNME import *

# %% ################################################
### READ ###
#####################################################

### https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html

# tableau_auth = TSC.TableauAuth('Eric.Myers', '!') #site_id=' site_id="Chris"')
# server = TSC.Server('https://datanexus-prd-dhhs.ne.gov')
# print(server.version)
# server.auth.sign_in(tableau_auth)

### Establish paths and read in input ID Files:

path_22_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\2CFS\\2.2fromCFS')
path_22_dir_input = Path(path_22_files_base, '0in', str_nehv_quarter)
path_22_dir_output = Path(path_22_files_base, '9out', str_nehv_quarter)

## Joe: Which file am I using?
##NOTE: make sure you have the file in the input path and you put the file name as it occurs in there

##
path_22_input_CFS_file = Path(path_22_dir_input, 'MIECHV Report - Child.xls')
path_22_input_child_file = Path(path_22_dir_input, 'Child Activity Master File.xlsx')
path_22_input_master_file = Path(path_22_dir_input, 'Child CPS Master File.xlsx')


with xw.App(visible=True) as app:  # Keep Excel hidden while running
    wb = xw.Book(path_22_input_CFS_file)
    
    # Access the desired sheet (adjust the index as needed)
    sheet = wb.sheets['Sheet1']  # or use wb.sheets['SheetName'] for a specific sheet name

    # Read the data into a DataFrame
    data = sheet.range('A1').expand().value  # Read all data starting from A1

# Convert to DataFrame
if isinstance(data, list) and len(data) > 0:
    df_22_CFS_file = pd.DataFrame(data[1:], columns=data[0])

#list_path_21_input_raw_sheets=['LLCHD','FamilyWise', 'Project ID']
df_22_child_LL = pd.read_excel(path_22_input_child_file, sheet_name='LLCHD', keep_default_na=False)
df_22_child_FW = pd.read_excel(path_22_input_child_file, sheet_name='Family Wise', keep_default_na=False)
df_22_child_pID = pd.read_excel(path_22_input_child_file, sheet_name='Project ID', keep_default_na=False)# dtype=dict_12LL_col_dtypes_1)

# %% ################################################
### Clean File ###
#####################################################

##1. Delete ID columns – only leave Intake Received Date, Current Status Reason, and Finding

df_22_CFS_file = df_22_CFS_file[['Project Id', 'Intake Received', 'Current Status Reason', 'Finding']]

##2. – only keep these values: acc, Accept for Initial Assessment, Accept for Out of Home Assessment, Accept for APS Investigation, Law Enforcement, Accept for Placement Assmn, Unable to Identify - Accepted

valid_status_values = [
    'Accept for Initial Assessment',
    'Accept for Out of Home Assessment',
    'Accept for APS Investigation',
    'Law Enforcement',
    'Accept for Placement Assmnt',
    'Unable to Identify - Accepted',
    'Accept for Out of Home Assmnt',
    'Accept for Initial Assmnt'

]

# Filter the DataFrame to only keep rows where 'Status' is one of the valid values
df_22_CFS_file = df_22_CFS_file[df_22_CFS_file['Current Status Reason'].isin(valid_status_values)]


##3. Delete all records without a Status 
df_22_CFS_file = df_22_CFS_file.dropna(subset=['Current Status Reason'])
df_22_CFS_file.rename(columns={'Current Status Reason': 'Status'}, inplace=True)

##4. Remove duplicates across Project ID and Intake Received Date (oldest to newest)
df_22_CFS_file['Intake Received'] = pd.to_datetime(df_22_CFS_file['Intake Received'], errors='coerce')

df_22_CFS_file = df_22_CFS_file.drop_duplicates(subset=['Intake Received', 'Project Id'], keep='first')

# Sort the DataFrame by 'Intake Received Date' (oldest to newest)
df_22_CFS_file = df_22_CFS_file.sort_values(by='Intake Received', ascending=True)
df_22_CFS_file.rename(columns={'Intake Received': 'IntakeReceivedDate'}, inplace=True)
df_22_CFS_file['IntakeReceivedDate'] = df_22_CFS_file['IntakeReceivedDate'].dt.strftime('%m/%d/%Y')


### check that this matches Joe's v2 file --it does!! for Y13Q4
#df_22_CFS_file.to_csv(Path(path_22_dir_output, 'CPS File v2.csv'), index = False, date_format="%m/%d/%Y")

#%%##############################################!>>>
### >>> RESTRUCTURING  
#####################################################


#%%###################################
### <> CPS File

##need to create these columns: 'year', 'quarter', 'agency_code','family_id', 'tgt_id','funding','Has_ChildWelfareAdaptation','Adaptation'

##after that restructure, and cross-check against Joe's MIECHV report v5


## Create the columns for v5
df_22_CFS_file['year']=int_nehv_year
df_22_CFS_file['quarter']=int_nehv_quarter
df_22_CFS_file['agency_code'] = df_22_CFS_file['Project Id'].str[:2]
df_22_CFS_file['family_id'] = df_22_CFS_file['Project Id'].str[2:].str.split('-').str[0]
df_22_CFS_file['tgt_id'] = df_22_CFS_file['Project Id'].str.split('-').str[1]

print(df_22_CFS_file)



### Pivot the DataFrame:
df_22_CFS_file['group_count'] = df_22_CFS_file.groupby(['Project Id']).cumcount() + 1

df_22_CFS_file_pivoted = df_22_CFS_file.pivot_table(
    index=['Project Id', 'year', 'quarter', 'agency_code','family_id','tgt_id']  ### All columns that do not change (if not listed will be deleted).
    ,columns=df_22_CFS_file.groupby(['Project Id']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['Finding', 'IntakeReceivedDate', 'Status'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_22_CFS_file = df_22_CFS_file_pivoted.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False)

# Flatten the MultiIndex columns and rename in the style of SPSS (e.g., 'Finding.1', 'Intake Received.2')
df_22_CFS_file.columns = [f'{col[0]}.{col[1]}' for col in df_22_CFS_file.columns]

df_22_CFS_file = df_22_CFS_file.reset_index()

df_22_CFS_file.drop_duplicates(inplace=True)

#%%##############################################!>>>
### >>> VLOOKUPS  
#####################################################
##Bring in funding, CWA, and Adapation columns from Child Activity File
# Merge the DataFrames based on the common column ('Project ID')

# Rename columns to ensure consistency in column names
df_22_CFS_file.rename(columns={'Project Id': 'project_id'}, inplace=True)
#df_22_child_LL.rename(columns={'project_id': 'Project ID'}, inplace=True)

# Check column names to verify the rename
print(df_22_CFS_file.columns)
print(df_22_child_LL.columns)


# Perform the first merge with df_22_child_LL
# df_22_LL_columns = df_22_child_LL[['Project ID', 'funding', 'Has_ChildWelfareAdaptation']]
# df_22_CFS_file = pd.merge(df_22_CFS_file, df_22_LL_columns, on=['Project ID'], how='left')

# #Perform the second merge with df_22_child_FW
# df_22_FW_columns = df_22_child_FW[['Project ID', 'Adaptation', ]]
# df_22_CFS_file = pd.merge(df_22_CFS_file, df_22_FW_columns, on=['Project ID'], how='left')

##for some reason when I bring over these columns more rows are added, (I'm assuming because of more unique column values)
# After the merge, check for NaN values in the new columns

df_22_CFS_file['Adaptation']=None
df_22_CFS_file['Has_ChildWelfareAdaptation']= None
df_22_CFS_file['funding']=None

## Check that output matches Joe's v5
#df_22_CFS_file.to_csv(Path(path_22_dir_output, 'CPS File v5.csv'), index = False, date_format="%m/%d/%Y")


### 6. Pull from existing file and append new quarter to old file, write new combined to output location for CPS Master File

if int_nehv_quarter != 1:
    df_22_CPS_previous = pd.read_excel(path_22_input_master_file, sheet_name='CPS Data', keep_default_na=False, na_values=[''])
    df_22_CPS_master = pd.concat([df_22_CPS_previous, df_22_CFS_file], ignore_index=True)
else:
    df_22_CPS_master=df_22_CFS_file


with pd.ExcelWriter(Path(path_22_dir_output, 'Child CPS Master File auto.xlsx'), engine='openpyxl') as writer:
        df_22_CPS_master.to_excel(writer, index=False, sheet_name='CPS Data')
        df_22_child_FW .to_excel(writer, index=False, sheet_name='Family Wise')
        df_22_child_LL.to_excel(writer, index=False, sheet_name='LLCHD')
        df_22_child_pID.to_excel(writer, index=False, sheet_name='Project ID')

#the output of this version currently matches what Joe has for Q4 file for CPS Master File
#this works! touch in with Joe about Zip? Not sure where it's coming from if it is needed 




# %% ################################################
### TABLEAU CALCULATIONS ###
#####################################################

df_22_CPS_agg= (
    pd.merge( 
        df_22_child_pID ### 'Project ID'
        ,df_22_child_FW ### 'Family Wise'.
        ,how='left'
        ,left_on=['project_id']
        ,right_on=['Project ID']
        ,indicator='LJ_tb4_1FW'
        # ,validate='one_to_one'
    ).merge(
        df_22_child_LL ### 'LLCHD'.
        ,how='left'
        ,left_on=['project_id']
        ,right_on=['project_id']
        ,indicator='LJ_tb4_2LL'
        # ,validate='one_to_one'
    )
) 

df_22_CPS_agg['_Agency'] = df_22_child_FW['agency'].fillna(df_22_child_LL['site_id']).astype('string')


def fn_Agency(fdf):
    if pd.notna(fdf['_Agency']):
        match fdf['_Agency']:
            case _ if pd.isna(fdf['_Agency']):
                return pd.NA
            case "hs":
                return "NCHS"
            case "ll":
                return "LLCHD"
            case "nc":
                return "NENCAP"
            case "ph":
                return "Panhandle"
            case "ps":
                return "Public Health Solutions"
            case "vn":
                return "VNA"
            case "se":
                return "SEDHD"
            case "lb":
                return "Loup Basin" 
            case "cd":
                return "Central District"
            case "fc":
                return "Four Corners"
            case 'sh':
                return 'South Heartland'
            case 'tr':
                return "Two Rivers" 
            case 'np':
                return "NCHS - North Platte"
            case _:
                return "Unidentified Agency"
df_22_CPS_agg['_Agency Name']= df_22_CPS_agg.apply(func=fn_Agency, axis=1).astype('string') 

def fn_PrimaryID(row):
    return row['project_id'].split('-')[0]
df_22_CPS_agg['_Primary Caregiver ID']= df_22_CPS_agg.apply(func=fn_PrimaryID, axis=1).astype('string') 
print(df_22_CPS_agg['project_id'])
print(df_22_CPS_agg['_Primary Caregiver ID'])


def fn_ProblemNENCAP(row):
    if row['_Primary Caregiver ID'] in ['nc328','nc335','nc338','nc342','nc346','nc349','nc351','nc352', 'nc353','nc354','nc358','nc359']:
        return True
    else:
        False
df_22_CPS_agg['Problem NENCAP Families for Funding Filter']= df_22_CPS_agg.apply(func=fn_ProblemNENCAP, axis=1).astype('boolean') 

def fn_Funding(row):
    if pd.notna(row['_Agency']):
        if row['_Agency'] == "ll" and row['funding'] =="Y":
            return "CWA"
        elif row['_Agency'] =="ll" and row['funding'] == "CHE" : 
            return "CHE"
        elif row['_Agency'] == "ll" and row['funding'] =="City":
            return "O"
        elif row['_Agency'] == "ll" and row['funding'] == "CWA":
            return "CWA"
        elif row['_Agency'] == "ll" and row['funding'] == "F" :
            return "F"
        elif row['_Agency'] == "ll" and row['funding'] == "F-1":
            return "F"
        elif row['_Agency'] == "ll" and row['funding'] == "F-2":
            return "F"
        elif row['_Agency'] == "ll" and row['funding'] == "F-3":
            return "F"
        elif row['_Agency'] == "ll" and row['funding'] == "O":
            return "O"
        elif row['_Agency'] == "ll" and row['funding'] == "S":
            return "S"
        elif row['_Agency'] == "ll" and row['funding'] == "Null":
            return "F" 
        elif row['_Agency'] == "ll" and (pd.isna(row['funding'])):
            return "F" ## LLCHD end
        
        elif row['_Agency'] =="hs" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA"):
            return "F"
        elif row['_Agency'] =="nc" and ['Problem NENCAP Families for Funding Filter']:
            return "O"
        elif row['_Agency'] =="nc" and row['Adaptation'] == "CWA":
            return "CWA"
        elif row['_Agency'] =="nc" and row['Adaptation'] == "Sixpence":
            return "Sixpence"
        elif row['_Agency'] =="nc" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA" or row['Adaptation'] != "Sixpence"):
            return "S"
        elif row['_Agency'] =="ph" and row['Adaptation'] == "CWA":
            return "CWA"
        elif row['_Agency'] =="ph" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA"):
            return "F"
        elif row['_Agency'] =="ps" and row['Adaptation'] == "CWA":
            return "CWA"
        elif row['_Agency'] =="ps" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA"):
            return "S"
        elif row['_Agency'] =="vn" and row['Adaptation'] == "CWA":
            return "CWA"
        elif row['_Agency'] =="vn" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA"):
            return "S"
        elif row['_Agency'] =="se" and row['Adaptation'] == "CWA":
            return "CWA"
        elif row['_Agency'] =="se" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA"):
            return "F"
        elif row['_Agency'] =="lb" and row['Adaptation'] == "CWA":
            return "CWA"
        elif row['_Agency'] =="lb" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA"):
            return "O"
        elif row['_Agency'] =="tr" and row['Adaptation'] == "CWA":
            return "CWA"
        elif row['_Agency'] =="tr" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA"):
            return "F"
        elif row['_Agency'] =="sh" and row['Adaptation'] == "CWA":
            return "CWA"
        elif row['_Agency'] =="sh" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA"):
            return "F"
        elif row['_Agency'] =="fc" and row['Adaptation'] == "CWA":
            return "CWA"
        elif row['_Agency'] =="fc" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA"):
            return "F"
        elif row['_Agency'] =="cd" and row['Adaptation'] == "CWA":
            return "CWA"
        elif row['_Agency'] =="cd" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA"):
            return "F"
        elif row['_Agency'] =="np" and row['Adaptation'] == "CWA":
            return "CWA"
        elif row['_Agency'] =="np" and (pd.isna(row['Adaptation']) or row['Adaptation'] != "CWA"):
            return "F"
df_22_CPS_agg['_Funding (use this one)']= df_22_CPS_agg.apply(func=fn_Funding, axis=1).astype('string') 

df_22_CPS_agg['_Enroll'] = df_22_CPS_agg['enroll_dt'].fillna(df_22_CPS_agg['MinOfHVDate']).astype('datetime64[ns]')

df_22_CPS_agg['_Max HV Date'] = df_22_CPS_agg['MaxofHVDate'].fillna(df_22_CPS_agg['last_home_visit']).astype('datetime64[ns]')

df_22_CPS_agg['_Discharge Date'] = df_22_CPS_agg['discharge_dt'].fillna(df_22_CPS_agg['TERMINATION DATE']).astype('datetime64[ns]')

df_22_CPS_agg['_TGT DOB'] = df_22_CPS_agg['tgt_dob'].fillna(df_22_CPS_agg['TGT DOB-CR']).astype('datetime64[ns]')

df_22_CPS_agg['_TGT Number'] = df_22_CPS_agg['tgt_id'].fillna(df_22_CPS_agg['ChildNumber']).astype('datetime64[ns]')

#df_22_CPS_agg['_Primary Caregiver ID'] = df_22_CPS_agg['project_id'].apply(lambda x: x[:x.find('-')])


def fn_First_Child_DOB(row):
        if pd.notna(row['_TGT Number']) and pd.notna(row['_Agency']):
            if row['_TGT Number']==1 and row['_Agency'] != 'll':
                return True
            else:
                return False
#(lambda row: row['_TGT DOB'] if row['_TGT Number'] ==1 and row['Agency'] != 'll' else None, axis=1)
df_22_CPS_agg['First Child Filter']= df_22_CPS_agg.apply(func=fn_First_Child_DOB, axis=1).astype('boolean')
df_22_CPS_agg_FC= df_22_CPS_agg[df_22_CPS_agg['First Child Filter'] == True]
df_22_CPS_agg_FC_final=df_22_CPS_agg_FC.groupby('_Primary Caregiver ID', as_index=False)['_TGT DOB'].max()
df_22_CPS_agg['_First child DOB for Filter']=df_22_CPS_agg['_Primary Caregiver ID'].map(df_22_CPS_agg_FC_final)

def fn_children_filter(row):
    if  row['_Agency'] != 'll':
        if row['_TGT Number'] == 1:
            return True
        elif row['_TGT Number'] == 2:
            date_diff = row['First child DOB for Filter'] - row['_TGT DOB']
            if date_diff.days < 4:
                return True
        elif row['_TGT Number'] == 3:
            date_diff = row['First child DOB for Filter'] - row['_TGT DOB']
            if date_diff.days < 4:
                return True
        else:
            return False
df_22_CPS_agg['Subsequent Children Filter']=df_22_CPS_agg.apply(func=fn_children_filter, axis=1).astype('boolean')      


def fn_child_alive_date(row):
    if (not pd.isna(row['_TGT DOB'])) and row['_TGT DOB'] <=date_fy_end_day_after:
        return True
df_22_CPS_agg['Child alive sometime during date range']=df_22_CPS_agg.apply(func=fn_child_alive_date, axis=1).astype('boolean')

def fn_child_alive_HV_date(row):
    if not pd.isna(row['_TGT DOB']) and row['_TGT DOB'] <= row['_Max HV Date']:
        return True
df_22_CPS_agg['Child alive sometime before Max HV Date']=df_22_CPS_agg.apply(func=fn_child_alive_HV_date, axis=1).astype('boolean')

def fn_child_alive_discharge(row):
    if not pd.isna(row['_TGT DOB']) and (not pd.isna(row['_Discharge Date'])
    and row['_TGT DOB'] <= row['_Discharge Date']) or pd.isna(row['_Discharge Date']):
        return True
df_22_CPS_agg['Child alive sometime before Discharge Date']=df_22_CPS_agg.apply(func=fn_child_alive_discharge, axis=1).astype('boolean')

def fn_Family_active_date(row):
    if row['_Enroll'] <= date_fy_end_day_after and (pd.isna(['_Discharge Date']) or date_fy_start < ['_Discharge Date']):
        return True
df_22_CPS_agg['Family active sometime during date range']=df_22_CPS_agg.apply(func=fn_Family_active_date, axis=1).astype('boolean')


def fn_c09_child_Denominator(row):
    if row['year'] == int_nehv_year and row['quarter'] == int_nehv_quarter and row['Family active sometime during date range'] and ['Child alive sometime during date range'] and['Child alive sometime before Max HV Date'] and ['Child alive sometime before Discharge Date']:
        return True
df_22_CPS_agg['_C09 Child Denominator Status']=df_22_CPS_agg.apply(func=fn_c09_child_Denominator, axis=1).astype('boolean')
    

def fn_c09_Denominator(df):
    # Apply the IIF logic for each row: if '_C09 Child Denominator Status' is 1, return 1; else return 0
    df['_C09 Denominator Status'] = df['_C09 Child Denominator Status'].apply(lambda status: 1 if status == 1 else 0)
    # Return the MAX of the 'IIF_Result' column
    return df['_C09 Denominator Status'].max() #is the groupby necessary here?

df_22_CPS_agg=df_22_CPS_agg.apply(func=fn_c09_Denominator, axis=1)

def fn_c09_Numerator_CPS(row):
    if ((date_fy_start <=  pd.Timestamp(row['IntakeReceivedDate.5'])) and
        pd.Timestamp(row['IntakeReceivedDate.5']) <= date_fy_end_day_after) or (date_fy_start <=  pd.Timestamp(row['IntakeReceivedDate.4']) and
        pd.Timestamp(row['IntakeReceivedDate.4']) <= date_fy_end_day_after) or (date_fy_start <=  pd.Timestamp(row['IntakeReceivedDate.3']) and
        pd.Timestamp(row['IntakeReceivedDate.3']) <= date_fy_end_day_after) or(date_fy_start <=  pd.Timestamp(row['IntakeReceivedDate.2']) and
        pd.Timestamp(row['IntakeReceivedDate.2']) <= date_fy_end_day_after) or (date_fy_start <=  pd.Timestamp(row['IntakeReceivedDate.1']) and
        pd.Timestamp(row['IntakeReceivedDate.1']) <= date_fy_end_day_after):
        return True
df_22_CPS_agg['_C09 Numerator CPS True (Update)']=df_22_CPS_agg.apply(func=fn_c09_Numerator_CPS, axis=1).astype('boolean')

def fn_c09_Numerator(df):
    # Define a new column based on the logic
    df['_C09 Numerator Status'] = df.apply(lambda row: 1 if row['_C09 Denominator Status'] and row['_C09 Child Denominator Status'] and row['_C09 Numerator CPS True (Update)'] else None, axis=1)
    
    # Apply the logic with group-by, like FIXED in Tableau, to get the MAX value per Project Id
    df['_C09 Numerator Status'] = df.groupby('Project Id')['_C09 Numerator Status'].transform('max')
    return df
df_22_CPS_agg=fn_c09_Numerator(df_22_CPS_agg)

df_22_CPS_agg = df_22_CPS_agg[df_22_CPS_agg['Subsequent Children Filter'] == True]
df_22_CPS_agg = df_22_CPS_agg[df_22_CPS_agg['year'] == int_nehv_year & df_22_CPS_agg['quarter'] == int_nehv_quarter]
df_22_CPS_agg = df_22_CPS_agg[df_22_CPS_agg['_C09 Denominator Status'] == True]
#df_22_CPS_agg = df_22_CPS_agg[df_22_CPS_agg['_Agency Name Filter'] == True]
df_22_CPS_agg = df_22_CPS_agg[df_22_CPS_agg['_Funding Source Filter'] == True]
df_22_CPS_agg = df_22_CPS_agg[df_22_CPS_agg['_Agency Name'] != 'Unidentified Agency']
column_mapping = {
    'year',
    'quarter',
    '_Agency Name',
    '_Funding (use this one)',
    '_C09 Numerator Status',
    'Children'
    'Percent of Children',
    'Total Children'
}

filtered_columns = list(column_mapping.values())
df_22_CPS_agg_9 = df_22_CPS_agg[filtered_columns]

with pd.ExcelWriter(Path(path_22_dir_output, 'Child CPS Aggregate File.xlsx'), engine='openpyxl') as writer:
        df_22_CPS_agg_9.to_excel(writer, index=False, sheet_name='Sheet 1')

#df_21_final_CPS = pd.concat([df_21_previous_CPS, df_21_final_filtered], ignore_index=True)







#All the columns are done. need to test them and put them into the Aggregate file (need Joe's input on how he gets those specific rows from the workbook)


#def fn_c09_Numerator(row):
#    1 = IF [_C09 Denominator Status] THEN
#    {FIXED [Project Id]: MAX(IIF([_C09 Child Denominator Status]
#        AND 
#        (
#           [_C09 Numerator CPS True (Update)]
#        )
#    ,1,0,0))}
#ELSE NULL
#END





