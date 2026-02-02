
### Every Quarter:
### STEP 1: INPUT for 1.1.1
    1. Copy this quarter's year and month NETable access file from U:\SFTP into U:\Working\nehv_ds_data_files\2mid\1main\1.1FW\1.1.1access\0in. This should be the most recent file in the SFTP folder, in a format that is "NETables20251008.accdb", with the date name being the date within or directly after the end of the quarter date you are running. 
    2. Input the file name into the path_111FW_input_NETable variable at  H:\git\nehv_ds_code_repository\code\1main\1.1FW\1.1.1access\_1_1_1FW_access.py
    It will be labeled for you to 'INPUT HERE!!!!'. Make sure the file name matches exactly the file name you have in the input folder, 
    i.e. "NETables20251008.accdb"
    3. 

### STEP 2: Running STEPS 1.1.2 - 1.4
    ### Prepare the RUNME for this correct quarter. 
     1. Update this RUNME file's key values:
    ### 2.1 Below, make a new line for objects "str_nehv_quarter" & "previous_str_nehv_quarter" -- set to the same text name of the new quarter (or previous quarter) used for creating input/output folders.
    ### 2.2 Update "str_nehv_year", "small_str_nehv_quarter", "small_str_nehv_previous_quarter", "int_nehv_quarter" & "int_nehv_year" & "date_fy_start" to match new quarter (or previous quarter for appropriate variables).

    ### 2.3 Update Federal Poverty Guidelines:
        ### If new quarter is in the same calendar year as last quarter, add quarter's name to the "str_nehv_quarter in (...)" clause.
        ### If new quarter is in a new calendar year, copy the previous year's code chunk & adapt it:
            ### Find the new year's "int_fpg_base" & "int_fpg_increment" values here:

    ## Once all variables have been updated, right click on the RUNME file and select Run in Interactive Window -> Run All Cells. 
    This should start the process for activating running all steps of the data cleaning process in order. The excel files from the 1.1.1 process will be automatically copied into 1.1.2 input, and all corresponding new folders and excel files will be created automatically. 

    ## If a step fails, try running the RUNME file for that step independently.




### STEP 3: Update the Tableau files:
    ## Make sure to count the number of WellVisitDates and add them to Form 2 _C04 AAP Date within AAP Range - Update
    ### 2.1 Below, make a new line for objects "str_nehv_quarter" & "previous_str_nehv_quarter" -- set to the same text name of the new quarter used for creating input/output folders.
    ### 2.2 Update "int_nehv_quarter" & "int_nehv_year" & "date_fy_start" to match new quarter.
    ### 2.3 Update Federal Poverty Guidelines:
        ### If new quarter is in the same calendar year as last quarter, add quarter's name to the "str_nehv_quarter in (...)" clause.
        ### If new quarter is in a new calendar year, copy the previous year's code chunk & adapt it:
            ### Find the new year's "int_fpg_base" & "int_fpg_increment" values here:
                ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines 
                ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines/prior-hhs-poverty-guidelines-federal-register-references 
                ### *** or https://kucpprds.mywikis.wiki/wiki/Federal_Poverty_Guidelines

