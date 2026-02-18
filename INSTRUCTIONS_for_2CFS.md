________________________________________________________________________________

NOTE: 'current quarter' means quarter you are running the data for currently. e.g. Y15Q1 (Oct 2025 - Dec 2025). 
'current year' means current fiscal year e.g. Y15 (Oct 2025 - Sept 2026)

# 2.1. For CFS

1. Navigate to the python file H:\git\nehv_ds_code_repository\code\2CFS\2.1forCFS\2.1forCFS.py in VSCode. Run the code by right-clicking, and selecting 'Run in Interactive Window'--> 'Run All Cells'. Make sure your interactive window python kernel is set to the correct conda environment that you setup to run the code. 
The script should finish quickly. You will know it is done running when the words "Congrats! you ran 2.2, the last part of the process!" appears in the interactive window.

2. The input file "LL_ID_File_base.xlsx" will be copied over after running the 1.2LL RUNME in the first step of the process. The "FW ID File.xlsx" will be copied over upon running the 2.1forCFS script. 
If you get an error that either file does not exist upon running, check that you have run the previous steps, and that the ID File is in the current quarter FamilyWise ID file folder.  

3. Navigate to U:\Working\nehv_ds_data_files\2mid\2CFS\2.1forCFS\9out\current quarter. In the output will be "Y#Q# ID File for CPS". 
Copy this file from the output folder into U:\Working\Tableau\current year\current quarter\ID File

4. Notify Joe that the ID File is ready for CPS in ID File folder.


__________________________________________________________________________
________________________________________________________________________________

# 2.2. From CFS: You will have to wait for Joe to notify you that partner has provided the MIECHV Report file needed to run this step. 


1. Navigate to the python file H:\git\nehv_ds_code_repository\code\2CFS\2.2fromCFS\2.2fromCFS.py in VSCode.
 Run the code by right-clicking, and selecting 'Run in Interactive Window'--> 'Run All Cells'. Make sure your interactive window python kernel is set to the correct conda environment that you setup to run the code. The script should finish quickly. You will know it is done running when the words "Congrats! you ran 2.2, the last part of the process!" appears in the interactive window.

2. The "Child Activity Master File.xlsx", 'Child CPS Master File.xlsx' and 'MIECHV Report - Child.xls' will be copied into input from the their appropriate locations. As long as they exist in these locations, they will copied over upon running the code. If you receive an error that these files do not exist upon running, check their pathways, and make sure the names match exactly as listed in the ###INPUT!!!!! section of the script. 

3. After running, check the  
U:\Working\nehv_ds_data_files\2mid\2CFS\2.2fromCFS\9out\current quarter folder. The output will be "Child CPS Master File.xlsx" to be used for next quarter's process, and the "Child CPS Aggregate Master File from Excel on NE Server_Migrated Data.csv". 

4. navigate to  U:\Working\Tableau\current year\Tableau Data Master Files (e.g. U:\Working\Tableau\Y15 (Oct 2025 - Sept 2026)\Tableau Data Master Files). Move the current "Child CPS Aggregate Master File from Excel on NE Server_Migrated Data.csv" file into the Archive folder. Copy the new csv file from your 2.2 output and paste it into the Tableau Data Master Files folder.

5. Open the most recent version of The Nebraska Form 2 and Form 4 workbook, located in "U:\Working\Tableau\current year\current quarter\Tableau Queries\Nebraska Form 2 and Form 4. Navigate to the 2. Benchmarks 2 and 3 dashboard tab (green color) (Alternately, navigate directly to this sheet in the workbook). Click on the Construct 9 bar, and then click on the icon in the top left that is "Go to Sheet". This should take you to the 9.Bar Agg sheet. There may not be updated data from the new csv file we just created for the quarter displayed on this sheet. 

6. Click on the Data Source Tab in the Bottom Left Corner of the Workbook. This will take you to the data source of the Construct 9 sheet, which is the 
Child CPS Aggregate Master File from Excel on NE Server_Migrated Data.csv file. Click on the refresh icon in the top left corner of the workbook. This will refresh the workbook with the new file we just created. 
If the workbook give you an error about finding the data source, navigate to the Connections section in the top left and click Edit Connection. Make sure the data source is reading from the file path where you just pasted the new file, which is 
U:\Working\Tableau\current year\Tableau Data Master Files\Child CPS Aggregate Master File from Excel on NE Server_Migrated Data.csv

7. Navigate back to the 2. Benchmarks 2 and 3 dashboard. You should now see data showing for the current quarter you are running data for in the Construct 9 sheet on the dashboard. 

8. Notify Joe that workbooks are ready for partner to review. 


___________________________________________________________________________