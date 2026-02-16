
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.


# %% ################################################
### PACKAGES ###
#####################################################

import pandas as pd
from pathlib import Path
import numpy as np
import sys
import xlwings as xw
import shutil
import re
sys.path+=[str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository'])]#'C:\\Users\\Eric.Myers\\git\\nehv_ds_code_repository\\code\\1main\\1.1FW\\1.1.2other']str(*[d for d in os.listdir(Path.cwd()) if os.path.isdir(d)])])
from RUNME import *

# %% ################################################
### READ ###
#####################################################

### https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html

### Establish paths and read in input ID Files:

path_21_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\2CFS\\2.1forCFS')
path_22_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\2CFS\\2.2fromCFS')
path_22_file_ID = Path(f'U:\\Working\\Tableau\\{str_nehv_year}\\{str_nehv_quarter}\\FamilyWise')

path_22_dir_input = Path(path_22_files_base, '0in', str_nehv_quarter)

path_21_dir_input = Path(path_21_files_base, '0in', str_nehv_quarter)
path_21_dir_output = Path(path_21_files_base, '9out', str_nehv_quarter)


path_21_dir_output = Path(path_21_files_base, '9out', str_nehv_quarter)


for path in [path_21_dir_input, path_21_dir_output]:
    path.mkdir(parents=True, exist_ok=True)

#Copy ID file from original location to input location for this script
shutil.copy2(Path(path_22_file_ID, 'ID File.xlsx'), Path(path_21_dir_input, 'FW ID File.xlsx'))

path_21_input_id_file_FW = Path(path_21_dir_input, 'FW ID File.xlsx')
path_21_input_id_file_LL = Path(path_21_dir_input, 'LL_ID_File_base.xlsx')
#path_21_input_id_file_combined = Path(path_21_dir_input, 'Combined ID File.xlsx')

##NOTE: you will have to make sure this file is in the input path, and input the password when Excel is started once running in order to read in
path_21_input_CPS_file = Path(path_21_dir_input, f"{re.search(r'Y\d{2}Q\d', previous_str_nehv_quarter).group()} ID File for CPS.xlsx")

CPS_file_password = previous_str_nehv_quarter  # Password in string format

### Python will start the Excel application and open the Excel file for the previous quarter ID File and prompt you for the password 
with xw.App(visible=True) as app:  # Keep Excel hidden while running
    wb = xw.Book(path_21_input_CPS_file)
    #pv = app.api.ProtectedViewWindows.Open(path_21_input_CPS_file)

    # Exit protected view and open normally
    #wb = pv.Edit()
    # Access the desired sheet (adjust the index as needed)
    sheet = wb.sheets['final']  # or use wb.sheets['SheetName'] for a specific sheet name

    # Read the data into a DataFrame
    data = sheet.range('A1').expand().value  # Read all data starting from A1

### Once opened, read the file and convert to a dataframe to be used in script 
if isinstance(data, list) and len(data) > 0:
    df_21_previous_CPS = pd.DataFrame(data[1:], columns=data[0])

#list_path_21_input_raw_sheets=['LLCHD','FamilyWise', 'Project ID']

# Create data frames for the FW and LL ID Files
df_21_id_file_FW = pd.read_excel(path_21_input_id_file_FW, keep_default_na=False, na_values=[''])
df_21_id_file_LL = pd.read_excel(path_21_input_id_file_LL, keep_default_na=False, na_values=[''])

        

# df_21_id_file_combined_pID = pd.DataFrame(columns=df_21_id_file_combined_pID_template.columns)
# df_21_id_file_combined_LL = pd.DataFrame(columns=df_21_id_file_combined_LL_template.columns)
# df_21_id_file_combined_FW = pd.DataFrame(columns=df_21_id_file_combined_FW_template.columns)



# %% ################################################
### ID File ###
#####################################################
#%%### 1. In the FW ID Export, Remove any rows with a discharge date before the current reporting year, 
# rows that do not have an enrollment date, and rows that have 0 # of MAX home visits, may need to update ZIP Code to remove -_ symbols 
df_21_id_file_FW = df_21_id_file_FW[
    (df_21_id_file_FW['DischargeDate'].isna()) |  # Include rows where DischargeDate is null
    ((df_21_id_file_FW['DischargeDate'] >= pd.Timestamp('2024-10-01')) &  # Include rows where DischargeDate is on or after 2024-10-01
    (df_21_id_file_FW['EnrollDate'].notna()))  # Ensure EnrollDate is not null
]

df_21_id_file_FW = (
    df_21_id_file_FW.dropna(subset=['MaxOfVISIT NUMBER'], inplace=False)
)
df_21_id_file_FW = (
    df_21_id_file_FW[df_21_id_file_FW['MaxOfVISIT NUMBER'] !=0]
)

#df_21_id_file_FW['MOB ZIP'] =df_21_id_file_FW['MOB ZIP'].str.replace('_', '', regex=False)

print(df_21_id_file_FW)


#%%### 2. ii.	Copy the FW ID file data and paste into the appropriate tab, ensuring labels line up
#iii.	Copy the LLCHD ID file data and paste into the appropriate tab, ensuring labels line up
#iv.	Copy all project IDs from the FW and LLCHD tabs and paste into the Project ID tab. Deduplicate ids.

##Family Wise file
df_21_id_file_combined_FW = df_21_id_file_FW
df_21_id_file_combined_FW=df_21_id_file_combined_FW.drop_duplicates()
df_21_id_file_combined_FW

##LL File (renaming pID so I can combine later)
df_21_id_file_combined_LL = df_21_id_file_LL
df_21_id_file_combined_LL.rename(columns={"project_id": "project_id (LLCHD)"}, inplace=True)
df_21_id_file_combined_LL=df_21_id_file_combined_LL.drop_duplicates()

##Creating project ID tab from LL and FW
combined_project_ids = pd.concat([df_21_id_file_combined_LL['project_id (LLCHD)'], df_21_id_file_combined_FW['Project ID']]).reset_index(drop=True)
df_21_id_file_combined_pID=pd.DataFrame({'project_id': combined_project_ids})
df_21_id_file_combined_pID  = df_21_id_file_combined_pID.drop_duplicates()
#Combine ID File matches Joe's. Next step is Tableau Calculations

# with pd.ExcelWriter(Path(path_21_dir_output, 'Combined ID File.xlsx'), engine='openpyxl') as writer:
#     df_21_id_file_combined_pID.to_excel(writer, index=False, sheet_name='Project ID')
#     df_21_id_file_combined_LL.to_excel(writer, index=False, sheet_name='LLCHD')
#     df_21_id_file_combined_FW.to_excel(writer, index=False, sheet_name='FamilyWise')

## Creating one dataframe with all sheet information for coding (combining on project_ID from project ID file)
df_21_final_combined= (
    pd.merge( 
        df_21_id_file_combined_pID ### 'Project ID'
        ,df_21_id_file_combined_FW ### 'Family Wise'.
        ,how='left'
        ,left_on=['project_id']
        ,right_on=['Project ID']
        ,indicator='LJ_tb4_1FW'
        # ,validate='one_to_one'
    ).merge(
        df_21_id_file_combined_LL ### 'LLCHD'.
        ,how='left'
        ,left_on=['project_id']
        ,right_on=['project_id (LLCHD)']
        ,indicator='LJ_tb4_2LL'
        # ,validate='one_to_one'
    )
) 

# %% ################################################
### RECREATE every Tableau Calculation ###
#####################################################

df_21_final_combined['_01 Project ID'] = df_21_final_combined['project_id'].astype('string')

df_21_final_combined['_02 Agency'] = df_21_final_combined['agency'].fillna(df_21_id_file_combined_LL['site_id']).astype('string')

df_21_final_combined['_03 Family Number'] = df_21_final_combined['family_id'].fillna(df_21_final_combined['FAMILY NUMBER']).astype('string')

df_21_final_combined['_04 Child Number'] = (df_21_final_combined['tgt_id'].fillna(df_21_final_combined['ChildNumber']).astype('Int64'))

df_21_final_combined['_05 Enrollment Date'] = df_21_final_combined['EnrollDate'].fillna(df_21_final_combined['enroll_dt']).astype('datetime64[ns]')

df_21_final_combined['_06 Discharge Date'] = df_21_final_combined['DischargeDate'].fillna(df_21_final_combined['discharge_dt']).astype('datetime64[ns]')
# Drop duplicates based on project ID, keeping the row with the discharge date
df_21_final_combined = df_21_final_combined.sort_values('_06 Discharge Date', ascending=False)  # Sort to prioritize rows with discharge dates
df_21_final_combined = df_21_final_combined.drop_duplicates(subset='_01 Project ID', keep='first')  # Keep the first occurrence (with discharge date)
# Reset index if needed
df_21_final_combined.reset_index(drop=True, inplace=True)


# Adding a source tag to distinguish if the project ID for that row is in the project ID column for LL, flag that row as LL, else flag it as FW
df_21_final_combined['source'] = np.where(
    df_21_final_combined['_01 Project ID'].isin(df_21_final_combined['project_id (LLCHD)']),
    'LL', 'FW'
)

def fn_Discharge_Reason(row):
    ### Check for source
    if row['source'] == 'LL':  # Check for LL source 
        if pd.notna(row['discharge_dt']):
            match row['discharge_reason']:
                case _ if pd.isna(row['discharge_reason']):
                    return pd.NA 
                case "1":
                    return "Completed Services"
                case "Family Has Met Program Goals":
                    return "Completed Services"
                case _:
                    return "Stopped Services Before Completion"
    elif row['source'] == 'FW':  # Check for FW source
        # Fetch the corresponding row in FW_df
            if pd.notna(row['DischargeDate']):
                match row['DischargeStatus']:
                    case _ if pd.isna(row['DischargeStatus']):
                        return pd.NA 
                    case "Death of child/fetus/primary caregiver":
                        return "Stopped Services Before Completion"
                    case "Did not respond to Creative Outreach":
                        return "Stopped Services Before Completion"
                    case "CPS involvement, no longer serving family":
                        return "Stopped Services Before Completion"
                    case "Custody change, involuntary":
                        return "Stopped Services Before Completion"
                    case "Custody change, voluntary":
                        return "Stopped Services Before Completion"
                    case "Family/Child aged out":
                        return "Stopped Services Before Completion"
                    case "Lack of capacity":
                        return "Stopped Services Before Completion"
                    case "Linked with more appropriate services":
                        return "Stopped Services Before Completion"
                    case "Moved, Link to HFA or other programs initiated successfully":
                        return "Stopped Services Before Completion"
                    case "Moved, no links made":
                        return "Stopped Services Before Completion"
                    case "Never Enrolled - Client Refused Initiation of Services":
                        return "Stopped Services Before Completion"
                    case "Never Enrolled - No Information Available":
                        return "Stopped Services Before Completion"
                    case "Never fully engaged":
                        return "Stopped Services Before Completion"
                    case "No longer able to contact or locate":
                        return "Stopped Services Before Completion"
                    case "Not available for services - jail, hospitalization, treatment":
                        return "Stopped Services Before Completion"
                    case "Refused continuation of services":
                        return "Stopped Services Before Completion"
                    case "Safety Issues":
                        return "Stopped Services Before Completion"
                    case "Family graduated/met all program goals":
                        return "Completed Services"
                    case _:
                        return "Unrecognized Value"
    
    return None  # Return None if both dates are null or source is unrecognized

# Apply the function to each row of the DataFrame
df_21_final_combined['_07 Discharge Reason'] = df_21_final_combined.apply(fn_Discharge_Reason, axis=1).astype('string')

###1Family graduated/met all program goals
###2Family moved out of service area
###3Parent/guardian returned to school
###4Parent/guardian returned to work
###5Parent/guardian refused service
###6Death of participant
###7Unable to locate family
###8Target child adopted
###9Target child entered foster care
###10Target child living with another care giverx
###11Target child entered school/child care
###12Family never engaged
###13Unknown & a text box
df_21_final_combined['_08 Home Visit Number'] = df_21_final_combined['home_visits_num'].fillna(df_21_final_combined['MaxOfVISIT NUMBER']).astype('Int64')


def fn_Funding(row):
    if pd.notna(row['_02 Agency']):
        if row['_02 Agency'] == "ll":
            # Check if the DataFrame is not empty before accessing the funding value
            return row['funding'] if pd.notna(row['funding']) else None
        elif row['_02 Agency'] in ["ps", "nc", "vn"]:
            return "S"
        elif row['_02 Agency'] in ["ph", "fs", "hs", "cd", "fc"]:
            return "F"
        elif row['_02 Agency'] == "se":
            return "TANF"
        else:
            return "Unrecognized Value"

# Apply the function to each row of the DataFrame
df_21_final_combined['_09 Funding'] = df_21_final_combined.apply(fn_Funding, axis=1).astype('string')

df_21_final_combined['_10 MOB First Name'] = df_21_final_combined['mob_first_name'].fillna(df_21_final_combined['MOB FIRST-CR']).astype('string')

df_21_final_combined['_11 MOB Last Name'] = df_21_final_combined['mob_last_name'].fillna(df_21_final_combined['MOB LAST-CR']).astype('string')

def fn_mob_dob(row):
    if row['source'] == 'FW':  # Check for FW source 
        if row['MOB DOB']==pd.Timestamp('1900-01-01'):
            return pd.NA
    elif row['source'] == 'LL': 
        mob_dob1 =row['mob_dob']
        if mob_dob1 == pd.Timestamp('1900-01-01'):
            return pd.NA
    else:
        return row['MOB DOB'] if pd.notnull(row['MOB DOB']) else mob_dob1
    return pd.NA  

#TODO: fix above function
df_21_final_combined['_12 MOB DOB'] =df_21_final_combined['MOB DOB'].fillna(df_21_final_combined['mob_dob']).astype('string')

def fn_MOB_SSN(row):
    ###########
    ### FW.
    if (row['source'] == 'FW'):
        if pd.notna(row['MOB SS.']):
            match row['MOB SS.']:
                case _ if pd.isna(row['MOB SS.']):
                    return pd.NA
                case "555555555"|"999999999"|"000000000"|"333333333":
                    return pd.NA
                case _:
                    return row['MOB SS.']
    ###########
    ### LLCHD.
    elif (row['source'] == 'LL'):
        if pd.notna(row['mob_ssn']):
            match row['mob_ssn']:
                case _ if pd.isna(row['mob_ssn']):
                    return pd.NA
                case "555-55-5555"|"999-99-9999" |"000-00-0000"|"333-33-3333":
                    return pd.NA
                case _:
                    return row['mob_ssn']
    ###########
    else:
        return pd.NA

df_21_final_combined['_13 MOB SSN'] = df_21_final_combined.apply(func=fn_MOB_SSN, axis=1).astype('string')

df_21_final_combined['_14 TGT First Name'] = df_21_final_combined['tgt_first_name'].fillna(df_21_final_combined['TGT FIRST-CR'])

df_21_final_combined['_15 TGT Last Name'] = df_21_final_combined['tgt_last_name'].fillna(df_21_final_combined['TGT LAST-CR'])

def fn_TGT_DOB(row):
    if row['source'] == 'FW': 
        if row['TGT DOB-CR']==pd.Timestamp('1900-01-01'):
            return pd.NA
    elif row['source'] == 'LL': 
        if row['tgt_dob'] == pd.Timestamp('1900-01-01'):
            return pd.NA
    else:
        return row['tgt_dob'].fillna(row['TGT DOB-CR'])
    
#TODO: FIX THE ABOVE FUNCTION
df_21_final_combined['_16 TGT DOB'] = df_21_final_combined['tgt_dob'].fillna( df_21_final_combined['TGT DOB-CR'])

def fn_TGT_SSN(row):
        ###########
    ### FW.
    if (row['source'] == 'FW'):
        if pd.notna(row['TGT SS NUMBER']):
            match row['MOB SS.']:
                case _ if pd.isna(row['TGT SS NUMBER']):
                    return pd.NA
                case "555555555"|"999999999"|"000000000"|"333333333":
                    return pd.NA
                case _:
                    return row['TGT SS NUMBER']
    ###########
    ### LLCHD.
    elif (row['source'] == 'LL'):
        if pd.notna(row['tgt_ssn']):
            match row['mob_ssn']:
                case _ if pd.isna(row['tgt_ssn']):
                    return pd.NA
                case "555-55-5555"|"999-99-9999" |"000-00-0000"|"333-33-3333":
                    return pd.NA
                case _:
                    return row['tgt_ssn']
    ###########
    else:
        return pd.NA

df_21_final_combined['_17 TGT SSN'] = df_21_final_combined.apply(func=fn_MOB_SSN, axis=1).astype('string')

df_21_final_combined['_18 TGT Gender'] = df_21_final_combined['TGT GENDER'].fillna(df_21_final_combined['tgt_gender'])

def fn_FOB_Involved(fdf):
    ###########
    ### FW.
    if (fdf['source'] == 'FW'):
        match fdf['FOB INVOLVED']:
            case _ if pd.isna(fdf['FOB INVOLVED']):
                return 0 
            case True:
                return 1 
            case False:
                return 0 
            case _:
                return 0 
    ###########
    ### LLCHD.
    elif (fdf['source'] == 'LL'):
        match fdf['fob_involved']:
            case _ if pd.isna(fdf['fob_involved']):
                return 0 
            case "Y":
                return 1 
            case "N":
                return 0 
            case _:
                return 0 
    ###########
    else:
        return 0 

df_21_final_combined['_19 FOB Involved'] = df_21_final_combined.apply(func=fn_FOB_Involved, axis=1).astype('Int64')


def fn_FOB_first_name(row):
    if row['FOB INVOLVED'] is False:  # Check if Fob Involved is False
        return pd.NA  # or np.nan
    elif row['fob_involved'] == "N":  # Check if Fob Involved1 is "N"
        return pd.NA # or np.nan
    else:
        # Return Fob First if not null, otherwise return Fob First Name
        return row['FOB FIRST'] if pd.notnull(row['FOB FIRST']) else row['fob_first_name']

df_21_final_combined['_20 FOB First Name'] = df_21_final_combined.apply(fn_FOB_first_name, axis=1).astype('string')


def fn_FOB_last_name(row):
    if row['FOB INVOLVED'] is False:  # Check if Fob Involved is False
        return pd.NA  # or np.nan
    elif row['fob_involved'] == "N":  # Check if Fob Involved1 is "N"
        return pd.NA # or np.nan
    else:
        # Return Fob First if not null, otherwise return Fob First Name
        return row['fob_last_name'] if pd.notnull(row['fob_last_name']) else row['FOB LAST']
    
df_21_final_combined['_21 FOB Last Name'] = df_21_final_combined.apply(fn_FOB_last_name, axis=1).astype('string')


def fn_FOB_DOB(row):
    if row['source'] == 'FW':
        if row['FOB INVOLVED'] is True:  # Check for FW source 
            if row['FOB DOB']==pd.Timestamp('1900-01-01'):
                return pd.NA
        elif pd.isna(row['FOB INVOLVED']):
            return pd.NA
        else:
            return row['FOB DOB']
    elif row['source'] == 'LL': 
        if row['fob_involved'] == "Y":
            fob_dob1 =row['fob_dob']
            if fob_dob1 == pd.Timestamp('1900-01-01'):
                return pd.NA
            elif pd.isna(row['fob_involved']):
                return pd.NA
            else:
                return fob_dob1
    else:
        return pd.NA  

df_21_final_combined['_22 FOB DOB'] = df_21_final_combined.apply(fn_FOB_DOB, axis=1).astype('datetime64[ns]')

def fn_FOB_SSN(row):
        ###########
    ### FW.
    if (row['source'] == 'FW'):
        if row['FOB INVOLVED'] is True:
            if pd.notna(row['FOB SS NUMBER']):
                match row['MOB SS.']:
                    case _ if pd.isna(row['FOB SS NUMBER']):
                        return pd.NA
                    case "555555555"|"999999999"|"000000000"|"333333333":
                        return pd.NA
                    case _:
                        return row['FOB SS NUMBER']
    ###########
    ### LLCHD.
    elif (row['source'] == 'LL'):
        if row['fob_involved'] == "Y":
            if pd.notna(row['fob_ssn']):
                match row['mob_ssn']:
                    case _ if pd.isna(row['fob_ssn']):
                        return pd.NA
                    case "555-55-5555"|"999-99-9999" |"000-00-0000"|"333-33-3333":
                        return pd.NA
                    case _:
                        return row['fob_ssn']
    ###########
    else:
        return pd.NA
    

df_21_final_combined['_23 FOB SSN'] = df_21_final_combined.apply(fn_FOB_SSN, axis=1).astype('string')

df_21_final_combined['_24 Address'] = df_21_final_combined['address'].fillna(df_21_final_combined['MOB ADDRESS']).astype('string')

df_21_final_combined['_25 City'] = df_21_final_combined['city'].fillna(df_21_final_combined['MOB CITY']).astype('string')

df_21_final_combined['zip'] = df_21_final_combined['zip'].replace('null', np.nan)
df_21_final_combined['MOB ZIP'] = df_21_final_combined['MOB ZIP'].replace('null', np.nan)
df_21_final_combined['_26 Zip'] = pd.to_numeric(
    df_21_final_combined['zip'].fillna(df_21_final_combined['MOB ZIP']),
    errors='coerce'  # This will turn non-convertible values into NaN
)

df_21_final_combined['_26 Zip'] = df_21_final_combined['_26 Zip'].astype('Int64')

#with pd.ExcelWriter(Path(path_21_dir_output, f'Test ID File for CPS.xlsx'), engine='openpyxl') as writer:
    #df_21_final_combined.to_excel(writer, index=False, sheet_name='final')

# %% ################################################
### Drop all columns expect for _ ###
#####################################################
df_21_final_combined = df_21_final_combined[[col for col in df_21_final_combined.columns if col.startswith('_')]]

# %% ################################################
### PART 2: FOR CPS  ###
#####################################################
#%%### 1. Open previous quarter’s “ID File for CPS (make sure previous quarter's id file is placed into input folder)
#2.	Rename all of ord1 (column A ) to “Old-record do not process”
df_21_previous_CPS['ord1'] = "Old-record do not process"

#%%### 3. Open previous quarter’s “ID File for CPS (make sure previous quarter's id file is placed into input folder)
## 4. for new records, For new records, rename ord1 (column A) to “New-record process”

# Create a mapping dictionary
column_mapping = {
    '_01 Project ID': 'project_id',
    '_14 TGT First Name': 'tgt_first_name',
    '_15 TGT Last Name': 'tgt_last_name',
    '_16 TGT DOB': 'tgt_dob',
    '_10 MOB First Name': 'mob_first_name',
    '_11 MOB Last Name': 'mob_last_name',
    '_12 MOB DOB': 'mob_dob',
    '_24 Address': 'address',
    '_25 City': 'city',
    '_26 Zip': 'zip'
}

df_21_final_combined.rename(columns=column_mapping, inplace=True)
filtered_columns = list(column_mapping.values())
df_21_final_filtered = df_21_final_combined[filtered_columns]
df_21_final_filtered['ord1'] = "New-record process"
df_21_final_CPS = pd.concat([df_21_previous_CPS, df_21_final_filtered], ignore_index=True)



#5.	Remove any records that don’t include a TGT First and Last Name
# df_21_final_CPS = df_21_final_CPS.dropna(subset=['tgt_first_name', 'tgt_last_name']) #Not sure if this is still relevant because doesn't apply to your previous quarter


# %% ################################################
### WRITE ###
#####################################################

# df_21_final_combined.sort_values(by='_01 Project ID', ascending=True, inplace=True) ##This is the end for recreating the 'ID File'

# ### Output for 1st Tableau file: ID File.
# with pd.ExcelWriter(Path(path_21_dir_output, 'Final ID File.xlsx'), engine='openpyxl') as writer:
#     df_21_final_combined.to_excel(writer, index=False, sheet_name='final')

#6. Sort file by project_id and ord1 (Z-A)
df_21_final_CPS = df_21_final_CPS.sort_values(by=['project_id', 'ord1'], ascending=[True, False]) ##This is the end for recreating the 'for CPS file'

### Output for 1st Tableau file: ID File.
with pd.ExcelWriter(Path(path_21_dir_output, f"{re.search(r'Y\d{2}Q\d', str_nehv_quarter).group()} ID File for CPS.xlsx"), engine='openpyxl') as writer:
    df_21_final_CPS.to_excel(writer, index=False, sheet_name='final')
    

##JOE: did a Q4 check and the files match exactly so I think this works!
print("You successfully ran CFS part 2.1!")








