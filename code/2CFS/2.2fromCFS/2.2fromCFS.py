
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

##4. Remove duplicates across Project ID and Intake Received Date (oldest to newest)
df_22_CFS_file['Intake Received'] = pd.to_datetime(df_22_CFS_file['Intake Received'], errors='coerce')

df_22_CFS_file = df_22_CFS_file.drop_duplicates(subset=['Intake Received', 'Project Id'], keep='first')

# Sort the DataFrame by 'Intake Received Date' (oldest to newest)
df_22_CFS_file = df_22_CFS_file.sort_values(by='Intake Received', ascending=True)


### check that this matches Joe's v2 file --it does!! for Y13Q4
##df_22_CFS_file.to_csv(Path(path_22_dir_output, 'CPS File v2.csv'), index = False, date_format="%m/%d/%Y")

#%%##############################################!>>>
### >>> RESTRUCTURING  
#####################################################


#%%###################################
### <> CPS File

##need to create these columns: 'year', 'quarter', 'agency_code','family_id', 'tgt_id','funding','Has_ChildWelfareAdaptation','Adaptation'

##after that restructure, and cross-check against Joe's MIECHV report v5



### Pivot the DataFrame:
df_22_CFS_file = df_22_CFS_file.pivot_table(
    index=['Project Id', 'year', 'quarter', 'agency_code','family_id', 'tgt_id','funding','Has_ChildWelfareAdaptation','Adaptation'] ### All columns that do not change (if not listed will be deleted).
    ,columns=df_22_CFS_file.groupby(['Project Id']).cumcount() + 1 ### Cumulative count of rows within groupings so groups of data stack vertically. DF should be sorted beforehand. 
    ,values=['Finding', 'Intake Recieved', 'Current Status Reason'] ### Columns that change.
    ,aggfunc='first' ### To use the values themselves and not an aggregation.
)
### TODO ASKJOE: Do we need col 'ERVisitReason'? Joe does not keep.

#%%
### Reorder exploded columns (while all other columns still in the row index & while column names still a MultiIndex):
df_22_CFS_file = df_22_CFS_file.sort_index(axis=1, level=0, ascending=False).sort_index(axis=1, level=1, sort_remaining=False) 

#%%
### Flatten the column MultiIndex & rename columns in the style of SPSS:
df_22_CFS_file.columns = [f'{col[0]}.{col[1]}' for col in df_12LL_pivoted_ChildERInj_2.columns]

#%%
### Reset row & column indices:
df_22_CFS_file = df_22_CFS_file.reset_index()





