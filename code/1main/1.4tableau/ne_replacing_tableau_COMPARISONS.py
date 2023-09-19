
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%
exec(open('RUNME.py').read())


#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TO DO: Instructions for how to get into environment & how to edit/run code files.

#%%##################################################
### PACKAGES ###
#####################################################

import pandas as pd
from pathlib import Path
import numpy as np
import sys
import IPython

print('Version Of Python: ' + sys.version)
print('Version Of Pandas: ' + pd.__version__)
print('Version Of Numpy: ' + np.version.version)

#%%##################################################
### SETTINGS ###
#####################################################

### None for now.

#%%##################################################
### PATHS & READING ###
#####################################################

### Files created for Y12Q1 by the old data sourcing process with Tableau.

###path_2_comparison_csv = Path('U:\\Working\\nebraska_miechv_coded_data_source\\previous\\previous_output\\Y12Q1 (Oct 2022 - Dec 2023)\\Child Activity Master File from Excel on NE Server.csv')
df2_comparison_csv = pd.read_csv(path_2_comparison_csv, dtype=object, keep_default_na=False, na_values=[''])
df2_comparison_csv = df2_comparison_csv.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)

###path_3_comparison_csv = Path('U:\\Working\\nebraska_miechv_coded_data_source\\previous\\previous_output\\Y12Q1 (Oct 2022 - Dec 2023)\\Adult Activity Master File from Excel on NE Server.csv')
df3_comparison_csv = pd.read_csv(path_3_comparison_csv, dtype=object, keep_default_na=False, na_values=[''])
df3_comparison_csv = df3_comparison_csv.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)

###path_4_comparison_csv = Path('U:\\Working\\nebraska_miechv_coded_data_source\\previous\\previous_output\\Y12Q1 (Oct 2022 - Dec 2023)\\Adult Activity Master File for Form 1 from Excel on NE Server.csv')
df4_comparison_csv = pd.read_csv(path_4_comparison_csv, dtype=object, keep_default_na=False, na_values=[''])
df4_comparison_csv = df4_comparison_csv.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)


#%%##################################################
### COMPARE 2(Child) to 3(Adult) ###
#####################################################

#%%
### Differences: Columns only in one.
set([*df2_comparison_csv]).symmetric_difference([*df3_comparison_csv])

#%%
### Overlap / Similarities: Columns in both.
set([*df2_comparison_csv]).intersection([*df3_comparison_csv])

### Created columns in both:
    ###  '_Agency',
    ###  '_C13 Behavioral Concerns Asked',
    ###  '_C13 Behavioral Concerns Visits',
    ###  '_Discharge Date',
    ###  '_Enroll 3 Month Date',
    ###  '_Max HV Date',
    ###  '_TGT 3 Month Date',
    ###  '_TGT DOB',
    ###  '_TGT EDC Date',
    ###  '_Zip',


#%%##################################################
### COMPARE 3(Adult-Form2) to 4(Adult-Form1) ###
#####################################################

#%%
### Differences: Columns only in one.
set([*df3_comparison_csv]).symmetric_difference([*df4_comparison_csv])

### Created columns in only df3_comparison_csv:

### Created columns in only df4_comparison_csv:



#%%
### Overlap / Similarities: Columns in both.
set([*df3_comparison_csv]).intersection([*df4_comparison_csv])

### Created columns in both:
    ### Lots!




#%%
