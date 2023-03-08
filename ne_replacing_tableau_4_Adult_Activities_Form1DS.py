
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#####################################################
### PACKAGES ###
#####################################################

### pip list ### See packages installed in this virtual environment that can be imported.

import pandas as pd

# import matplotlib.pyplot as plt
# import numpy as np

### Test that pandas imported:
print(pd.__version__)

#####################################################
### READ ###
#####################################################

### https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html

### Data Source for 3rd Tableau file, 2nd Data Source (for Form 1):
### DS: "Adult Activity Master File for Form 1 from Excel on NE Server".
path_4_data_source = 'U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Adult Activity Master File Y12.xlsx'

### DS: "Adult Activity Master File for Form 1 from Excel on NE Server".
df4_1 = pd.read_excel(path_4_data_source, sheet_name='Project ID')
df4_2 = pd.read_excel(path_4_data_source, sheet_name='Caregiver Insurance')
df4_3 = pd.read_excel(path_4_data_source, sheet_name='Family Wise')
df4_4 = pd.read_excel(path_4_data_source, sheet_name='LLCHD')
df4_5 = pd.read_excel(path_4_data_source, sheet_name='MOB or FOB')

#####################################################
### JOIN ###
#####################################################

### join with pandas.merge() or pandas.df.join()
### https://pandas.pydata.org/docs/reference/api/pandas.merge.html

### DS: "Adult Activity Master File for Form 1 from Excel on NE Server".
df4 = pd.merge(df4_1, df4_2, how='left', left_on=['Project Id','Year','Quarter'], right_on=['Project ID','year (Caregiver Insurance)','quarter (Caregiver Insurance)'], indicator='LJ_df4_2').merge(df4_3, how='left', left_on=['Project Id','Year','Quarter'], right_on=['Project ID 1','year (Family Wise)','quarter (Family Wise)'], indicator='LJ_df4_3').merge(df4_4, how='left', left_on=['Project Id','Year','Quarter'], right_on=['project id (LLCHD)','year (LLCHD)','quarter (LLCHD)'], indicator='LJ_df4_4').merge(df4_5, how='left', left_on=['Join Id'], right_on=['join id (MOB or FOB)'], indicator='LJ_df4_5') 

#####################################################
### RECODE ###
#####################################################

### numpy.where() --- Sami: use to treat NULL values weird at times; may be quicker than apply.
### pandas

#####################################################
### Identify/FLAG "Unrecognized Value" ###
#####################################################

### FLAG any "Unrecognized Value" --- new value & needs to be edited earlier in the Data Source process.
### Across many variables.


#####################################################
### Data Types ###
#####################################################

### REMEMBER to check/set the data type of each column like it should be in output.


#####################################################
### WRITE ###
#####################################################

### Output for 3rd Tableau file, 2nd Data Source (for Form 1):
path_4_output = ''







