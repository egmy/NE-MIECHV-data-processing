
### Purpose: 

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### Every Quarter:

### 1. Update this RUNME file's key values:
    ### 2.1 Below, make a new line for objects "str_nehv_quarter" & "previous_str_nehv_quarter" -- set to the same text name of the new quarter used for creating input/output folders.
    ### 2.2 Update "int_nehv_quarter" & "int_nehv_year" & "date_fy_start" to match new quarter.
    ### 2.3 Update Federal Poverty Guidelines:
        ### If new quarter is in the same calendar year as last quarter, add quarter's name to the "str_nehv_quarter in (...)" clause.
        ### If new quarter is in a new calendar year, copy the previous year's code chunk & adapt it:
            ### Find the new year's "int_fpg_base" & "int_fpg_increment" values here:
                ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines 
                ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines/prior-hhs-poverty-guidelines-federal-register-references 
                ### *** or https://kucpprds.mywikis.wiki/wiki/Federal_Poverty_Guidelines


#%%##################################################
### PREPARATION ###
#####################################################



#%%##################################################
### PACKAGES & FUNCTIONS ###
#####################################################

from packages_and_functions import * 
import runpy


#%%##################################################
### SETTINGS ###
#####################################################


#%%##################################################
### KEY VALUES ###
#####################################################

repo_name = 'nehv_ds_code_repository'

# str_nehv_quarter = 'Y12Q1 (Oct 2022 - Dec 2023)'
# str_nehv_quarter = 'Y12Q2 (Oct 2022 - Mar 2023)'
# str_nehv_quarter = 'Y12Q3 (Oct 2022 - Jun 2023)'
# str_nehv_quarter = 'Y12Q4 (Oct 2022 - Sep 2023)'
# str_nehv_quarter = 'Y13Q2 (Oct 2023 - Mar 2024)'
# str_nehv_quarter = 'Y13Q3 (Oct 2023 - Jun 2024)'
# str_nehv_quarter = 'Y13Q4 (Oct 2023 - Sep 2024)'
str_nehv_quarter = 'Y14Q1 (Oct 2024 - Dec 2024)'


# previous_str_nehv_quarter = 'Y13Q1 (Oct 2023 - Dec 2023)'
previous_str_nehv_quarter = 'Y13Q4'

int_nehv_year = 14 

# int_nehv_quarter = 1 
# int_nehv_quarter = 2 
int_nehv_quarter = 1 


date_fy_start = pd.Timestamp("2024-10-01") ### Midnight.

date_fy_end_day_after = date_fy_start + pd.DateOffset(years=1) ### Midnight.

bool_14t_deduplicate = True  


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
elif (str_nehv_quarter in ('Y13Q2 (Oct 2023 - Mar 2024)', 'Y13Q3 (Oct 2023 - Jun 2024)','Y13Q4 (Oct 2023 - Sep 2024)', 'Y14Q1 (Oct 2024 - Dec 2024)' )):
    ### 9680 + (5380 * [household size])
    int_fpg_base = 9680 
    int_fpg_increment = 5380 


#%%##################################################
### PATHS ###
#####################################################

read_from_file=True

### Paths for all sub folders & sub files.
if __name__ == "__main__":
    home_path = Path.cwd() ### Only works when running from main RUNME, otherwise Path.cwd() grabs subfolder instead of main repo directory!
else:
    # if read_from_file==False:
    #     home_path = [path for path in Path.cwd().parents if path.name == repo_name][0] 
    # else:
    home_path = Path.cwd() ### For when running from "lower" files.
#home_path = Path.cwd()

path_112FW_code_base = home_path/'code/1main/1.1FW/1.1.2other'
path_112FW_code_RUNME = path_112FW_code_base / '_1_1_2FW_RUNME.py'

path_12LL_code_base = home_path/'code/1main/1.2LL'
path_12LL_code_RUNME = path_12LL_code_base / '_1_2LL_RUNME.py'

path_13_code_base = home_path/'code/1main/1.3combine'
path_13_code_RUNME = path_13_code_base / '_1_3_RUNME.py'

path_14t_code_base = home_path/'code/1main/1.4tableau'
path_14t_code_RUNME = path_14t_code_base / '_1_4tab_RUNME.py'

#sys.path.insert(0, 'C:\\Users\\Eric.Myers\\git\\nehv_ds_code_repository\\code\\1main\\1.1FW\\1.1.2other')

sys.path+=[str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']),str(path_112FW_code_base), str(path_12LL_code_base), str(path_13_code_base), str(path_14t_code_base)]
#%%##################################################
### RUN ALL STEPS ###
#####################################################


if read_from_file==True: ##run all silos separate
### Step 1.1.2 FamilyWise
    if __name__ == "__main__":
        print('\nExecuting step 1.1.2FW')
        ### runpy.run_path(path_name = path_112FW_code_RUNME)
        exec(open(path_112FW_code_RUNME).read())
        print('\nSuccessfully executed step 1.1.2FW!"')
    ### Step 1.2 Lincoln Lancaster
    if __name__ == "__main__":
        print('\nExecuting step 1.2LL')
        ### runpy.run_path(path_name = path_12LL_code_RUNME)
        exec(open(path_12LL_code_RUNME).read())
        print('\nSuccessfully executed step 1.2LL!"')
    ### Step 1.3 combine
    if __name__ == "__main__":
        print('\nExecuting step 1.3combine')
        #runpy.run_path(path_name = path_13_code_RUNME)
        exec(open(path_13_code_RUNME).read())
        print('\nSuccessfully executed step 1.3combine!"')
    ### Final Step 1.4 Tableau replacement
    if __name__ == "__main__":
        print('\nExecuting step 1.4 Tableau replacement')
        ### runpy.run_path(path_name = path_14t_code_RUNME)
        exec(open(path_14t_code_RUNME).read())
        print('\nSuccessfully executed step 1.4 Tableau replacement!')
        print('\nYou successfully ran all steps of the NE Data cleanup process!! Congrats')

else: #run them all together 
    if __name__ == "__main__":
        print('\nExecuting step 1.4 Tableau replacement')
        ### runpy.run_path(path_name = path_14t_code_RUNME)
        exec(open(path_14t_code_RUNME).read())
        print('\nSuccessfully executed step 1.4 Tableau replacement!')
        print('\nYou successfully ran all steps of the NE Data cleanup process!! Congrats')
### TODO: Write code that (1) backs up input files into an "old" folder & (2) overwrites input files with output from previous steps.



#%%##################################################
### END ###
#####################################################

print('Yay! You ran the main RUNME!')

# %%

### TODO: Autogenerate in/out folders instead of needing to create them.

