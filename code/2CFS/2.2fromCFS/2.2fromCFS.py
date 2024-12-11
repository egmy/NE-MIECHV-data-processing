
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.


# %% ################################################
### PACKAGES ###
#####################################################

import pandas as pd
from pathlib import Path
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

### Establish paths and read in input ID Files:

path_22_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\2CFS\\2.2fromCFS')
path_22_dir_input = Path(path_22_files_base, '0in', str_nehv_quarter)
path_22_dir_output = Path(path_22_files_base, '9out', str_nehv_quarter)

## Joe: Which file am I using?
##NOTE: make sure you have the file in the input path and you put the file name as it occurs in there

##
path_22_input_CFS_file = Path(path_22_dir_input, 'MIECHV Report - Child Y13Q4.xls')
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
df_22_CFS_file.to_csv(Path(path_22_dir_output, 'CPS File v2.csv'), index = False, date_format="%m/%d/%Y")

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
df_22_CFS_file.to_csv(Path(path_22_dir_output, 'CPS File v5.csv'), index = False, date_format="%m/%d/%Y")


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



