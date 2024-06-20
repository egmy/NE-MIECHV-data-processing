
### Purpose: 

#%%##################################################
### INSTRUCTIONS ###
#####################################################

### Every Quarter:

### 1. Update this RUNME file's key values:
    ### 2.1 Below, make a new line for object "str_nehv_quarter" & set to the same text name of the new quarter used for creating input/output folders.
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
previous_str_nehv_quarter = 'Y13Q1 (Oct 2023 - Dec 2023)'
str_nehv_quarter = 'Y13Q2 (Oct 2023 - Mar 2024)'

int_nehv_year = 13 

# int_nehv_quarter = 1 
int_nehv_quarter = 2 

date_fy_start = pd.Timestamp("2023-10-01") ### Midnight.

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
elif (str_nehv_quarter in ('Y13Q2 (Oct 2023 - Mar 2024)')):
    ### 9680 + (5380 * [household size])
    int_fpg_base = 9680 
    int_fpg_increment = 5380 


#%%##################################################
### PATHS ###
#####################################################



### TODO: Write code that (1) backs up input files into an "old" folder & (2) overwrites input files with output from previous steps.


