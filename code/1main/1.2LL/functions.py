

#%%##################################################
### PACKAGES ###
#####################################################

import pandas as pd
from pathlib import Path
import numpy as np
import sys
import runpy
import IPython
import collections
import re

print('Version Of Python: ' + sys.version)
print('Version Of Pandas: ' + pd.__version__)
print('Version Of Numpy: ' + np.version.version)


#%%##################################################
### Utility Functions ###
#####################################################

def inspect_df (df):
    print(df.describe(include='all', datetime_is_numeric=True))
    print('\n')
    print(df.dtypes.to_string())
    print('\n')
    print(df.info(verbose=True, show_counts=True))
    print('\n')
    print(f'Rows: {len(df)}')
    print(f'Columns: {len(df.columns)}')
    print('\n')
    IPython.display.display(df)

### fSeries = df column or Series: e.g., df['colname'].
def inspect_col(fSeries):
    print(fSeries.info())
    print('\n')
    print('value_counts:')
    print(fSeries.value_counts(dropna=False).to_string())
    print('\n')
    print(fSeries)

def compare_col(fdf_1, fdf_2, fcol, info_or_value_counts='info'): ### or 'value_counts'.
    if info_or_value_counts=='info':
        print(f'DataFrame 1:\n')
        print(fdf_1[fcol].info())
        print('\n')
        print(f'DataFrame 2:\n')
        print(fdf_2[fcol].info())
    elif info_or_value_counts=='value_counts':
        print(f'DataFrame 1:\n')
        print(fdf_1[fcol].value_counts(dropna=False).to_string())
        print('\n')
        print(f'DataFrame 2:\n')
        print(fdf_2[fcol].value_counts(dropna=False).to_string())

def fn_all_value_counts(fdf):
    for column in fdf.columns:
        print('Column: ', column)
        print(fdf[column].value_counts(dropna=False).to_string(), '\n')

### Function to keep rows where comparison column values are different. 
    ### For use on ".compare" pandas DataFrames with multi-index column names broken into "self" & "other".
def fn_keep_row_differences(fdf, variable2compare):
    if pd.isna(fdf[(variable2compare, 'self')]) and pd.isna(fdf[(variable2compare, 'other')]):
        return False 
    elif pd.isna(fdf[(variable2compare, 'self')] != fdf[(variable2compare, 'other')]):
        return True 
    else:
        return fdf[(variable2compare, 'self')] != fdf[(variable2compare, 'other')] 


#%%##################################################
### Project Functions ###
#####################################################

def fn_find_unrecognized_value(fdf):
    fn_list = []
    for col_index, col in enumerate(fdf.columns):
        if (fdf[col].isin(['Unrecognized Value']).any()):
            fn_list.append({
                'col': col
                ,'col_index': col_index
                ,'col_row_indices': fdf[['Project Id','Year','Quarter', col]].query(f'`{col}` == "Unrecognized Value"').index.tolist()
                ,'col_row_ids': fdf.query(f'`{col}` == "Unrecognized Value"')['Project Id'].tolist()
                ,'col_df': fdf[['Project Id','Year','Quarter', col]].query(f'`{col}` == "Unrecognized Value"')
            })
    print(fn_list)
    return fn_list 

### Function designed to turn a Pandas DataFrame whose columns are all dtype 'string' into dtypes specified in a dictionary.
def fn_apply_dtypes(fdf, dict_col_dtypes):
    for column in fdf.columns:
        if (dict_col_dtypes[column] == 'datetime64[ns]'):
            try:
                fdf[column] = pd.to_datetime(fdf[column])
                ### Because .astype() cannot handle a 'string' dtype with multiple date formats, but .to_datetime() can!
            except Exception as e:
                print('Error for column: ', column)
                print('Attempted dtype: ', dict_col_dtypes[column])
                print(e, '\n')
        elif (dict_col_dtypes[column] == 'Int64'):
            try:
                fdf[column] = fdf[column].astype('Float64').astype('Int64')
                ### Because .astype() cannot turn a string of a float (e.g., "3.0") into Int64. This solution might not be needed by every column to be turned into Int64.
            except Exception as e:
                print('Error for column: ', column)
                print('Attempted dtype: ', dict_col_dtypes[column])
                print(e, '\n')
        elif (dict_col_dtypes[column] == 'boolean' and all([(x in set(['True', 'False', '1', '0', '1.0', '0.0', pd.NA])) for x in set(fdf[column])])):
            try:
                fdf[column] = fdf[column].map({'True': True, 'False': False, '1': True, '0': False, '1.0': True, '0.0': False}).astype('boolean')
                ### Because .astype() cannot turn strings ['True', 'False'] into dtype 'boolean' (or correctly/at all for 'bool', depending if has NA).
                ### Also: .astype() can turn integer & float 0/1 into boolean, but not strings of ints/floats.
                ### Solution only for cases where column has only the values listed -- because other values will be forcibly coerced to NA (which we don't want).
            except Exception as e:
                print('Error for column: ', column)
                print('Attempted dtype: ', dict_col_dtypes[column])
                print(e, '\n')
        else:
            try:
                fdf[column] = fdf[column].astype(dict_col_dtypes[column])
                ### Default. I wish this worked with all dtypes.
            except Exception as e:
                print('Error for column: ', column)
                print('Attempted dtype: ', dict_col_dtypes[column])
                print(e, '\n')
    return fdf 


#%%##################################################

print('Imported functions!')


