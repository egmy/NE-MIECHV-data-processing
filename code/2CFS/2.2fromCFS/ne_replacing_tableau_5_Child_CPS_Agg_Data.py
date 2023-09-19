
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%
exec(open('RUNME.py').read())

# %% ################################################
### PACKAGES ###
#####################################################

# import pandas as pd
# from pathlib import Path
# import numpy as np
# import sys
# import IPython

# %% ################################################
### READ ###
#####################################################

### https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html

### Data Source for 5th Tableau data source (4th file):
path_5_data_source = ''

df5 = pd.read_excel(path_5_data_source, sheet_name='')


# %% ################################################
### REVIEW / EDIT dataframes ###
#####################################################

### Need to prepare dataframes for joining.
    ### address NULL values on key variables.


# %% ################################################
### JOIN ###
#####################################################

### join with pandas.merge() or pandas.df.join()
### https://pandas.pydata.org/docs/reference/api/pandas.merge.html


# %% ################################################
### RECODE ###
#####################################################



# %% ################################################
### Identify/FLAG "Unrecognized Value" ###
#####################################################

### FLAG any "Unrecognized Value" --- new value & needs to be edited earlier in the Data Source process.
### Across many variables.


# %% ################################################
### Data Types ###
#####################################################

### REMEMBER to check/set the data type of each column like it should be in output.



# %% ################################################
### WRITE ###
#####################################################

### Output for 5th Tableau data source:
path_5_output = ''








