
### Every Quarter:
### STEP 1: INPUT for 1.1.1 Access DB Queries
### This step currently takes about 2 hours to completely run, and has to be completed before the previous steps can be run. 

1. Copy this quarter's year and month NETable access file from U:\SFTP into U:\Working\nehv_ds_data_files\2mid\1main\1.1FW\1.1.1access\0in. This should be the most recent file in the SFTP folder, in a format that is "NETables20251008.accdb", with the date name being the date within or directly after the end of the quarter date you are running. 
2. Input the file name into the path_111FW_input_NETable variable at  H:\git\nehv_ds_code_repository\code\1main\1.1FW\1.1.1access\_1_1_1FW_access.py
    It will be labeled for you to 'INPUT HERE!!!!'. Make sure the file name matches exactly the file name you have in the input folder, 
    i.e. "NETables20251008.accdb"
3. Right click on the python file and select Run in Interactive Window -> Run All Cells. You will need the previous quarter's access file, but this should be copied over from the previous quarter's input upon running the file. 
If you get an error that either of the input Access Files do not exist, check the input file paths and file names at the top of the Python script
4. Wait for the script to finish running. This may take a couple of hours. You can exit from your Virtual Machine and the script will stay running. You will know it is complete when all 10 of the output excel files are in the output. The Adult Activities Query - FROG.xlsx will take the longest to populate.


### STEP 2: Running STEPS 1.1.2 - 1.4
### Prepare the Main RUNME for this correct quarter. 
1. The main RUNME file is located at H:\git\nehv_ds_code_repository\RUNME.py. Update this RUNME file's key values by following steps below. 
2. Make a new line for objects "str_nehv_quarter" & "previous_str_nehv_quarter" -- set to the same text name of the new quarter you are running the data for (or previous quarter for previous quarter variable) used for creating input/output folders.
3.  Update "str_nehv_year", "small_str_nehv_quarter", "small_str_nehv_previous_quarter", "int_nehv_quarter" & "int_nehv_year" & "date_fy_start" to match new quarter (or previous quarter for appropriate variables).

4.  Update Federal Poverty Guidelines:
    If new quarter is in the same calendar year as last quarter, add quarter's name to the "str_nehv_quarter in (...)" clause.
     If new quarter is in a new calendar year, copy the previous year's code chunk & adapt it:
    Find the new year's "int_fpg_base" & "int_fpg_increment" values here:
        ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines 
                ### https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines/prior-hhs-poverty-guidelines-federal-register-references 
                ### *** or https://kucpprds.mywikis.wiki/wiki/Federal_Poverty_Guidelines

5. read_from_file variable should be set to True

6.  Once all variables have been updated, right click on the RUNME file and select Run in Interactive Window -> Run All Cells. 

7. This should start the process for activating running all steps of the data cleaning process in order. The excel files from the 1.1.1 process will be automatically copied into 1.1.2 input, and all corresponding new folders and excel files will be created automatically. Note that the 1.3 step is not currently setup to output to 1.3 output folder, but instead outputs to the 1.4 input folder. 

### If a step fails, try running the RUNME file for that step independently. This will sometimes fix the issue, or give you a more specific error message.

8. If a new fiscal year, proceed to step 9. If not a new fiscal year, take the 3 output files from the 1.4 step located at U:\Working\nehv_ds_data_files\2mid\1main\1.4tableau\9out\current quarter. Copy them from this location and paste them into U:\Working\nehv_ds_data_files\3_final_output. Replace the current files in the destination. This will be the files the Tableau workbooks for the current year read from. Proceed to STEP 3: Update the Tableau Files.

9. Create a new Year folder for the previous fiscal year (i.e. Y15). Copy the current files in the 3_output folder into the newly created year folder. 

10. Open up the Tableau workbooks from the previous fiscal year, located at "U:\Working\Tableau\previous year\previous quarter\Tableau Queries. Navigate to the '3. Bar' Worksheet (color blue). 

11. Once on the 3. Bar worksheet, click on the data source Tab on the bottom left of the worksheet. This will take you to the Adult Activity data source Form 2 and 4. Navigate to the upper left "Connections" section, and hover over the Adult Activity data source. Under the dropdown arrow, click on Edit Connection. Edit the Connection so it pulls from the files pasted into the previous year folder inside of the 3_output folder. i.e. U:\Working\nehv_ds_data_files\3_final_output\Y14

12. Click on the Extract button in the top right corner of the data source tab. Click on any other sheet in the workbook. This will start the extract. If asked where to save the extract, save to the Extracts folder located at U:\Working\Tableau\previous year\previous quarter\Tableau Queries\Extracts. Make sure you are saving to the previous year Extracts folder. If not asked where to save the extract at this stage, you will be asked upon trying to save and close the workbook. 

13. Repeat Steps 10-12 for the Child Activity data source Form 1 and 2 and 4, starting from the '4. Bar - Update' (red color) worksheet instead of 3. Bar. 

14. Once the data has been refreshed and the two extracts populated, play with the Quarter filter on one of the green colored Dashboards to make sure there is data populating for every quarter. Save As a new version number of the workbook, and specify in the name that is for previous year. Close the workbook. Save the Extracts into U:\Working\Tableau\previous year\previous quarter\Tableau Queries\Extracts, if not done in Step 12. 

15. Repeat Steps 10 and 11 for the Nebraska Form 1 Tableau workbook located at "U:\Working\Tableau\previous year\previous quarter\Tableau Queries". The sheet to navigate to for the Adult activity data source Form 1 data source is 'T1 Adult' (or any of the sheets labeled Adult). Once you have updated the connection to read from the previous year folder in 3_output, navigate back to any sheet without, making sure the Live button is clicked in the upper right corner. You do not have to save extracts for These data sources. Repeat for the Child Activity data source Form 1 and 2 and 4 in Form 1. The sheet to navigate to first for this data source is 'T1 Child' (or any of the sheets labeled Child). Save the workbook as a new version and Close the workbook. 


16. Take the 3 output files from the 1.4 step located at U:\Working\nehv_ds_data_files\2mid\1main\1.4tableau\9out\current quarter. Copy them from this location and paste them into U:\Working\nehv_ds_data_files\3_final_output. Replace the current files in the destination. This will be the files the Tableau workbooks for the current year read from. Proceed to STEP 3: Update the Tableau Files.

### STEP 3: Update the Tableau files

1. Open the most recent version of The Nebraska Form 2 and Form 4 workbook, located at "U:\Working\Tableau\current year\current quarter\Tableau Queries". Navigate to the 1. Benchmark 1 dashboard tab (green color). Click on the Construct 3 bar, and click on the "Go to Sheet" icon. This will take you to the 3. Bar worksheet, colored blue (Alternatively, navigate directly to this worksheet in the workbook). 

2. Once on 3. Bar worksheet, click on the data Source tab on the bottom left of the worksheet.  This will take you to the Adult Activity data source Form 2 and 4. Click on the Refresh button in the top left corner. This will refresh the data source. 

3. Click on the Extract button in the top right corner of the data source tab. Click on any other sheet in the workbook. This will start the extract. If asked where to save the extract, save to the Extracts folder located at U:\Working\Tableau\current year\current quarter\Tableau Queries\Extracts. Make sure you are saving to the current quarter Extracts folder, the quarter you are currently running the data for. If not asked where to save the extract at this stage, you will be asked upon trying to save and close the workbook. 

4. Repeat Steps 2 and 3 for the Child Activity data source Form 1 and 2 and 4, starting from the '4. Bar - Update' (red color) worksheet instead of 3. Bar.

5. Go to the Child Activity data source Form 1 and 2 and 4 Excel worksheet located at U:\Working\nehv_ds_data_files\3_final_output. Open the worksheet, and search for the WellVisitDate.# columns. Count how may columns there are. Go back to the Form 2 workbook. Search for the _C04 AAP Date within AAP Range - Update variable. Right-click the variable and click Edit... . Make sure the number of columns being used in the variable matches the number recorded from the worksheet. If there are more columns that need to be added, i.e. .25, .26, .27, add them to the TOP of the syntax. 

6. Once the data has been refreshed and the two extracts populated, play with the Quarter filter on one of the green colored Dashboards to make sure there is data populating for the newest quarter data you just added. Save As a new version number of the workbook. Close the workbook. Save the Extracts into U:\Working\Tableau\current year\current quarter\Tableau Queries\Extracts, if not done in Step 12. 

7. Repeat Step 2 for the most recent Nebraska Form 1 Tableau workbook located at "U:\Working\Tableau\current year\current quarter\Tableau Queries". The sheet to navigate to for the Adult activity data source Form 1 data source is 'T1 Adult' (or any of the sheets labeled Adult). Once you have refreshed the data source, navigate back to any sheet, making sure the Live button is clicked in the upper right corner. You do not have to save extracts for These data sources. Repeat for the Child Activity data source Form 1 and 2 and 4 in Form 1. The sheet to navigate to first for this data source is 'T1 Child' (or any of the sheets labeled Child). Once refreshed, save the workbook as a new version and close the workbook.

8. Proceed to INSTRUCTIONS_for_2CFS.md located at H:\git\nehv_ds_code_repository\INSTRUCTIONS_for_2CFS.md to follow the instructions for running the 2.1 step of the process. 



