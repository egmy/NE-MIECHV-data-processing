
### Purpose: Run all 1.4 "tableau replacement code" that recodes variables into the formats needed for Reports.
    ### This step 1.4 read in the Excel Child & Adult "Master Files" and outputs 1 child & 2 adult CSV data sources used by the Form 1, 2, & 4 Tableau Reports.

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### Every Quarter:

### 1. Create new folders for the quarter:
    ### Find the new quarter's name in the folder for the current fiscal year:
        ### Example: In year folder "U:\Working\Tableau\Y13 (Oct 2023 - Sept 2024)" there is a quarter subfolder "Y13Q2 (Oct 2023 - Mar 2024)".
    ### Copy that quarter's folder's name (for example, "Y13Q2 (Oct 2023 - Mar 2024)"), and create new folders in these locations:
        ### U:\Working\nehv_ds_data_files\2mid\1main\1.4tableau\0in  
        ### U:\Working\nehv_ds_data_files\2mid\1main\1.4tableau\9out  

### 2. Update this RUNME file's key values:
    ### 2.1 Below, make a new line for object "str_nehv_quarter" & set to the same text name of the new quarter used for folders above.
    ### 2.2 Update Federal Poverty Guidelines:
        ### If new quarter is in the same calendar year as last quarter, add quarter's name to the "str_nehv_quarter in (...)" clause.
        ### If new quarter is in a new calendar year, copy the previous year's code chunk & adapt it:
            ### Find the new year's "int_fpg_base" & "int_fpg_increment" values here:
                ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines 
                ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines/prior-hhs-poverty-guidelines-federal-register-references 
                ### *** or https://kucpprds.mywikis.wiki/wiki/Federal_Poverty_Guidelines

### 3. After the Excel Child & Adult "Master Files" are created, copy the files into "input" folder:
    ### From the year folder (for example, "U:\Working\Tableau\Y13 (Oct 2023 - Sept 2024)"), copy these files:
        ### "Adult Activity Master File.xlsx"  
        ### "Child Activity Master File.xlsx"  
    ### To new quarter's "in" folder (for example, "U:\Working\nehv_ds_data_files\2mid\1main\1.4tableau\0in\Y13Q1 (Oct 2023 - Dec 2023)").
    ### Also, copy file "source_input_excels_Y##Q#.txt" from previous quarter's folder into the current quarter's folder, & update file name & contents as needed (to track sources of files).

### 4. Run this "_1_4tab_RUNME.py" file:
    ### This RUNME file will run the following 3 code files:
        ### "_1_4_replacing_tableau_2_Child_Activities.py"
        ### "_1_4_replacing_tableau_3_Adult_Activities_Form2DS.py"
        ### "_1_4_replacing_tableau_4_Adult_Activities_Form1DS.py"
    ### Check output of sections 'Identify/FLAG "Unrecognized Value"' to see if any adjustment is needed in the code (check with Data Team).

### 5. Update the Tableau Reports' data source files:
    ### 5.1 Backup old data source files:
        ### Move all files from folder "U:\Working\nehv_ds_data_files\3_final_output" into subfolder "z old". Save all versions if asked.
    ### 5.2 Copy in new data source files:
        ### Copy 1.4 output from folder: U:\Working\nehv_ds_data_files\2mid\1main\1.4tableau\9out\<quarter>  
        ### To: U:\Working\nehv_ds_data_files\3_final_output  
        ### It should be 3 files:
            ### "Adult Activity data source Form 1.csv"
            ### "Adult Activity data source Form 2 and 4.csv"
            ### "Child Activity data source Form 1 and 2 and 4.csv"

### 6. Check that the Tableau Reports connect correctly to the data sources:
    ### Tableau reports are found here: U:\Working\Tableau\<year>\<quarter>\Tableau Queries  
    ### For the following reports, check 2 data sources each (ignore other data sources):
        ### Form 1 (for example, "Nebraska Form 1 Y13 2022.1.5 v36 - Y13Q1.twb")
            ### data source: "Adult Activity data source Form 1"
            ### data source: "Child Activity data source Form 1 and 2 and 4"
        ### Form 2 & 4 (for example, "Nebraska Form 2 and Form 4 Y13 2022.1.5 v131 - Y13Q1.twb")
            ### data source: "Adult Activity data source Form 2 and 4"
            ### data source: "Child Activity data source Form 1 and 2 and 4"
    ### Check that the reports read in the data sources files from folder "U:\Working\nehv_ds_data_files\3_final_output"
    ### Check that the data sources update/refresh.
    ### Check that there are no broken variables:
        ### Only in the Form 2 Child data source are there outstanding broken variables:
            ### "_C01 Child Denominator Status"
            ### "_C01 Denominator Status"
            ### "_C01 Gestational 37th Week Date"
            ### "_C01 Gestational Birth Date"
            ### "_C01 Numerator Status"
            ### "DateImp"
            ### "_C01 Label (Sum)"
    ### Fix broken variables as needed (check with Data Team).

### 7. Let the Data Team know that reports are updated!

### 8. Give yourself a high five!


#%%##################################################
### PACKAGES & FUNCTIONS ###
#####################################################

from pathlib import Path
print('Local Code Repository: ', str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))

#%%
import sys
sys.path.append(str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))
from packages_and_functions import * 


#%%##################################################
### SETTINGS ###
#####################################################


#%%##################################################
### KEY VALUES ###
#####################################################

# str_nehv_quarter = 'Y12Q1 (Oct 2022 - Dec 2023)'
# str_nehv_quarter = 'Y12Q2 (Oct 2022 - Mar 2023)'
# str_nehv_quarter = 'Y12Q3 (Oct 2022 - Jun 2023)'
# str_nehv_quarter = 'Y12Q4 (Oct 2022 - Sep 2023)'
#str_nehv_quarter = 'Y13Q1 (Oct 2023 - Dec 2023)'
str_nehv_quarter = 'Y13Q2 (Oct 2023 - Mar 2024)'

# # date_fy_start = pd.Timestamp("2022-10-01")
# # date_fy_end = pd.Timestamp("2023-09-30")
# date_fy_start = pd.Timestamp("2023-10-01")
# date_fy_end = pd.Timestamp("2024-09-30")


#%%########################
### Federal Poverty Guidelines
    ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines 
    ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines/prior-hhs-poverty-guidelines-federal-register-references 

### 2022: https://www.federalregister.gov/documents/2022/01/21/2022-01166/annual-update-of-the-hhs-poverty-guidelines 
if (str_nehv_quarter in ('Y12Q1 (Oct 2022 - Dec 2023)')):
    ### 8870 + (4720 * [household size])
    int_fpg_base = 8870 
    int_fpg_increment = 4720 

### 2023: https://www.federalregister.gov/documents/2023/01/19/2023-00885/annual-update-of-the-hhs-poverty-guidelines 
elif (str_nehv_quarter in ('Y12Q2 (Oct 2022 - Mar 2023)', 'Y12Q3 (Oct 2022 - Jun 2023)', 'Y12Q4 (Oct 2022 - Sep 2023)', 'Y13Q1 (Oct 2023 - Dec 2023)')):
    ### 9440 + (5140 * [household size])
    int_fpg_base = 9440 
    int_fpg_increment = 5140 

### 2024: https://www.govinfo.gov/content/pkg/FR-2024-01-17/pdf/2024-00796.pdf 
elif (str_nehv_quarter in ('Y13Q2 (Oct 2023 - Mar 2024)')):
    ### 9680 + (5380 * [household size])
    int_fpg_base = 9680 
    int_fpg_increment = 5380 


#%%##################################################
### PATHS ###
#####################################################

path_14t_code_base = Path.cwd()

path_14t_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.4tableau')

path_14t_dir_input = Path(path_14t_files_base, '0in', str_nehv_quarter)
path_14t_dir_mid = Path(path_14t_files_base, '2mid', str_nehv_quarter)
path_14t_dir_output = Path(path_14t_files_base, '9out', str_nehv_quarter)

###########################
### Data Source for 2nd Tableau file:
### path_14t_data_source_file_tb2 = 'U:\\Working\\Tableau\\Y12 (Oct 2022 - Sept 2023)\\Child Activity Master File.xlsx' 
### local:
path_14t_data_source_file_tb2 = Path(path_14t_dir_input, 'Child Activity Master File.xlsx')

list_path_14t_data_source_sheets_tb2 = [
    'Project ID' # 1.
    ,'ER Injury' # 2.
    ,'Family Wise' # 3.
    ,'LLCHD' # 4.
    ,'Well Child' # 5.
]

### Output for 2nd Tableau file:
### path_14t_output_tb2 = Path(path_14t_dir_output, 'Child Activity Master File from Excel on NE Server.csv') ### Old name.
path_14t_output_tb2 = Path(path_14t_dir_output, 'Child Activity data source Form 1 and 2 and 4.csv')

###########################
### Data Source for 3rd Tableau file, 1st Data Source (for Form 2):
### DS: "Adult Activity Master File from Excel on NE Server".
### path_14t_data_source_tb3 = 'U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Adult Activity Master File Y12.xlsx' 
### local:
path_14t_data_source_file_tb3 = Path(path_14t_dir_input, 'Adult Activity Master File.xlsx')

list_path_14t_data_source_sheets_tb3 = [
    'Project ID' # 1.
    ,'Caregiver Insurance' # 2.
    ,'Family Wise' # 3.
    ,'LLCHD' # 4.
]

### Output for 3rd Tableau file:
### path_14t_output_tb3 = Path(path_14t_dir_output, 'Adult Activity Master File from Excel on NE Server.csv') ### Old name.
path_14t_output_tb3 = Path(path_14t_dir_output, 'Adult Activity data source Form 2 and 4.csv') 

###########################
### Data Source for 4th Tableau file, 2nd Data Source (for Form 1):
### DS: "Adult Activity Master File for Form 1 from Excel on NE Server".
### path_14t_data_source_tb4 = 'U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Adult Activity Master File Y12.xlsx'
### Actually used for Q1:
### path_14t_data_source_tb4 = 'U:\\Working\\Tableau\\Y12 (Oct 2022 - Sept 2023)\\Y12Q1 (Oct 2022 - Dec 2023)\\Tableau Queries\\TESTING\WORKS\\Master Files\\Adult Activity Master File - Deleting Rows.xlsx'
### local:
path_14t_data_source_file_tb4 = Path(path_14t_dir_input, 'Adult Activity Master File.xlsx')

list_path_14t_data_source_sheets_tb4 = [
    'Project ID' # 1.
    ,'Caregiver Insurance' # 2.
    ,'Family Wise' # 3.
    ,'LLCHD' # 4.
    ,'MOB or FOB' # 5.
]

### Output for 4th Tableau file:
### path_14t_output_tb4 = Path(path_14t_dir_output, 'Adult Activity Master File for Form 1 from Excel on NE Server.csv') ### Old name.
path_14t_output_tb4 = Path(path_14t_dir_output, 'Adult Activity data source Form 1.csv') 


###########################
### Comparison Files ###
### Files created for selected quarter by the old data sourcing process with Tableau.

# path_14t_dir_comparison_csv = Path(path_14t_files_base, 'previous/output', str_nehv_quarter)

# path_14t_comparison_csv_tb2 = Path(path_14t_dir_comparison_csv, 'Child Activity Master File from Excel on NE Server.csv')
# path_14t_comparison_csv_tb3 = Path(path_14t_dir_comparison_csv, 'Adult Activity Master File from Excel on NE Server.csv')
# path_14t_comparison_csv_tb4 = Path(path_14t_dir_comparison_csv, 'Adult Activity Master File for Form 1 from Excel on NE Server.csv')

### As of Y13Q1, there are no comparison files being made because old process no longer used.


#%%##################################################
### END of Setup ###
#####################################################

print('end setup')


#%%##################################################
### RUN CODE FILES ###
#####################################################

### The following is run if running this file by itself interactively (& ignored when run from one of the code files):
    ### Using exec() instead of import so that code files can "see" packages, functions, & any objects created in RUNME.

#%%
### "_1_4_replacing_tableau_2_Child_Activities.py"
if __name__ == "__main__":
    print('\nExecuting file "_1_4_replacing_tableau_2_Child_Activities.py"')
    exec(open(Path(path_14t_code_base, '_1_4_replacing_tableau_2_Child_Activities.py')).read())
    print('\nExecuted file "_1_4_replacing_tableau_2_Child_Activities.py"')

#%%
### "_1_4_replacing_tableau_3_Adult_Activities_Form2DS.py"
if __name__ == "__main__":
    print('\nExecuting file "_1_4_replacing_tableau_3_Adult_Activities_Form2DS.py"')
    exec(open(Path(path_14t_code_base, '_1_4_replacing_tableau_3_Adult_Activities_Form2DS.py')).read())
    print('\nExecuted file "_1_4_replacing_tableau_3_Adult_Activities_Form2DS.py"')

#%%
### "_1_4_replacing_tableau_4_Adult_Activities_Form1DS.py"
if __name__ == "__main__":
    print('\nExecuting file "_1_4_replacing_tableau_4_Adult_Activities_Form1DS.py"')
    exec(open(Path(path_14t_code_base, '_1_4_replacing_tableau_4_Adult_Activities_Form1DS.py')).read())
    print('\nExecuted file "_1_4_replacing_tableau_4_Adult_Activities_Form1DS.py"')


#%%##################################################
### END ###
#####################################################

print('end file :)')



# %%

### TODO:
    ### CHECK all "Unrecognized Values".
    ### Function to stop & show/flag Unrecognized Values.
    ### Read in all with desired dtypes.
    ### After testing, make all ASQ3 scores floats (document that should be).

