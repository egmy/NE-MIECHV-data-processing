
### Purpose: 

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TODO: Instructions for how to get into environment & how to edit/run code files.

#%%##################################################
### PREPARATION ###
#####################################################

### In SETTINGS below:
    ### Change Quarter variable.

### Create 4 folders with name of str_nehv_quarter in these locations:
    ### U:\Working\nebraska_miechv_coded_data_source\data\01_input 
    ### U:\Working\nebraska_miechv_coded_data_source\previous\previous_output 
    ### U:\Working\nebraska_miechv_coded_data_source\previous\previous_tableau 
    ### U:\Working\nebraska_miechv_coded_data_source\data\03_output 
### ... and then copy over the associated files for the first 3 locations (not this code's output).
    ### In old folders see "source....txt" files to see where to copy from.


#%%##################################################
### PACKAGES & FUNCTIONS ###
#####################################################

import sys
sys.path.append('U:\\Working\\nehv_ds_code_repository')
from packages_and_functions import * 


#%%##################################################
### SETTINGS ###
#####################################################

# str_nehv_quarter = 'Y12Q1 (Oct 2022 - Dec 2023)'
# str_nehv_quarter = 'Y12Q2 (Oct 2022 - Mar 2023)'
# str_nehv_quarter = 'Y12Q3 (Oct 2022 - Jun 2023)'
str_nehv_quarter = 'Y12Q4 (Oct 2022 - Sep 2023)'

date_fy_start = pd.Timestamp("2022-10-01")

date_fy_end = pd.Timestamp("2023-09-30")

#%%########################
### Federal Poverty Guidelines
    ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines 
    ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines/prior-hhs-poverty-guidelines-federal-register-references 

### 2022: https://www.federalregister.gov/documents/2022/01/21/2022-01166/annual-update-of-the-hhs-poverty-guidelines 
if (str_nehv_quarter == 'Y12Q1 (Oct 2022 - Dec 2023)'):
    ### 8870 + (4720 * [household size])
    int_fpg_base = 8870 
    int_fpg_increment = 4720 

### 2023: https://www.federalregister.gov/documents/2023/01/19/2023-00885/annual-update-of-the-hhs-poverty-guidelines 
elif (str_nehv_quarter in ('Y12Q2 (Oct 2022 - Mar 2023)', 'Y12Q3 (Oct 2022 - Jun 2023)', 'Y12Q4 (Oct 2022 - Sep 2023)')):
    ### 9440 + (5140 * [household size])
    int_fpg_base = 9440 
    int_fpg_increment = 5140 

#%%##################################################
### PATHS ###
#####################################################

path_14t_code_base = Path('U:\\Working\\nehv_ds_code_repository\\code\\1main\\1.4tableau')

path_14t_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.4tableau')

path_14t_dir_input = Path(path_14t_files_base, '0in', str_nehv_quarter)
path_14t_dir_mid = Path(path_14t_files_base, '2mid', str_nehv_quarter)
path_14t_dir_output = Path(path_14t_files_base, '9out', str_nehv_quarter)

###########################
### Data Source for 2nd Tableau file:
### path_14t_data_source_file_tb2 = 'U:\\Working\\Tableau\\Y12 (Oct 2022 - Sept 2023)\\Child Activity Master File.xlsx' ### old.
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
path_14t_output_tb2 = Path(path_14t_dir_output, 'Child Activity Master File from Excel on NE Server.csv')

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
path_14t_output_tb3 = Path(path_14t_dir_output, 'Adult Activity Master File from Excel on NE Server.csv')

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
path_14t_output_tb4 = Path(path_14t_dir_output, 'Adult Activity Master File for Form 1 from Excel on NE Server.csv')


###########################
### Comparison Files ###
### Files created for selected quarter by the old data sourcing process with Tableau.

path_14t_dir_comparison_csv = Path(path_14t_files_base, 'previous/output', str_nehv_quarter)

path_14t_comparison_csv_tb2 = Path(path_14t_dir_comparison_csv, 'Child Activity Master File from Excel on NE Server.csv')
path_14t_comparison_csv_tb3 = Path(path_14t_dir_comparison_csv, 'Adult Activity Master File from Excel on NE Server.csv')
path_14t_comparison_csv_tb4 = Path(path_14t_dir_comparison_csv, 'Adult Activity Master File for Form 1 from Excel on NE Server.csv')


#%%##################################################
### END of Setup ###
#####################################################

print('end setup')


#%%##################################################
### RUN CODE FILES ###
#####################################################

if __name__ == "__main__":
    # import ne_replacing_tableau_2_Child_Activities
    # import ne_replacing_tableau_3_Adult_Activities_Form2DS
    # import ne_replacing_tableau_4_Adult_Activities_Form1DS

    # exec(open(Path(path_14t_code_base, 'ne_replacing_tableau_2_Child_Activities.py')).read())
    # exec(open(Path(path_14t_code_base, 'ne_replacing_tableau_3_Adult_Activities_Form2DS.py')).read())
    # exec(open(Path(path_14t_code_base, 'ne_replacing_tableau_4_Adult_Activities_Form1DS.py')).read())

    print('Imported code files')


#%%##################################################
### END ###
#####################################################

print('end file :)')



# %%

### TODO:
    ### Function to stop & show/flag Unrecognized Values.
    ### Read in all with desired dtypes.
    ### After testing, make all ASQ3 scores floats (document that should be).

