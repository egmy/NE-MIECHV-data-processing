

#%%##################################################
### PACKAGES ###
#####################################################

import pandas as pd
from pathlib import Path
import numpy as np
import sys
import IPython
import collections
import re
### import runpy

print('Version Of Python: ' + sys.version)
print('Version Of Pandas: ' + pd.__version__)
print('Version Of Numpy: ' + np.version.version)

### From 1.2LL:
from pandas.testing import assert_frame_equal


#%%##################################################
### KEY VALUES ###
#####################################################

list_na_values_to_read = ['', ' ']


#%%##################################################
### UTILITY FUNCTIONS ###
#####################################################

def inspect_df (df):
    print(df.describe(include='all'))
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

#%%####################

### Function to keep rows where comparison column values are different. 
    ### For use on ".compare" pandas DataFrames with multi-index column names broken into "self" & "other".
def fn_keep_row_differences(fdf, variable2compare):
    ### Drop row if NA in both columns:
        ### If CSVs read in as 'object': np.nan is always different from np.nan, so row would stay when looking for differences.
    if pd.isna(fdf[(variable2compare, 'self')]) and pd.isna(fdf[(variable2compare, 'other')]):
        return False 
    ### Keep row if pd.NA in only 1 column:
        ### If CSVs read in as 'string': would filter out rows resolving to <NA> (e.g., "pd.NA==x" or "pd.NA!=x").
    elif pd.isna(fdf[(variable2compare, 'self')] != fdf[(variable2compare, 'other')]):
        return True 
    ### Keep rows where column values are different:
    else:
        return fdf[(variable2compare, 'self')] != fdf[(variable2compare, 'other')] 

def fn_compare_col_across_dfs(
    df1
    ,df2
    ,str_col_to_compare
    ,list_key_or_id_cols=[]
    ,list_other_cols_for_comparison=[]
):
    list_cols_for_comparison = [str_col_to_compare] + list_other_cols_for_comparison 
    ### Preparation: Can only compare DF's with same structure (row/column counts same & indices identical).
        ### "Can only compare identically-labeled (i.e. same shape, identical row and column labels) DataFrames".
    print((
        ### Compare: Keep shape so ID column not dropped when the same. Keep equal so can see ID values.
        df1.compare(df2, keep_shape=True, keep_equal=True) 
        ### Select desired columns:
        .loc[:, list_key_or_id_cols + list_cols_for_comparison]
        ### Keep rows where columns different:
        .loc[lambda df: df.apply(fn_keep_row_differences, variable2compare=str_col_to_compare, axis=1), :] 
    ).to_string())

#%%####################


#%%##################################################
### PROJECT FUNCTIONS ###
#####################################################

### Function designed to turn a Pandas DataFrame whose columns are all dtype 'string' into dtypes specified in a dictionary.
def fn_apply_dtypes(fdf, dict_col_dtypes):
    for column in fdf.columns:
        if (dict_col_dtypes[column] == 'datetime64[ns]'):
            try:
                fdf[column] = pd.to_datetime(fdf[column])
                ### Because .astype() cannot handle a 'string' dtype with multiple date formats, but .to_datetime() can!
                ### Well, actually, there are some limitations: If the formats are too different, even pd.to_datetime() can't reconcile them.
                ### Adding argument `format='mixed'` might help (looks at each cell individually), but could cause problem if month & day are not consistently in order across the varying formats.
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
    print(f'Data types changed to dictionary specifications.')
    return fdf 

### Function designed to turn a Pandas DataFrame whose columns are all dtype 'string' into dtypes specified in a dictionary.
def fn_fix_mixed_date_dtypes(fdf, dict_col_dtypes):
    for column in fdf.columns:
        if (dict_col_dtypes[column] == 'datetime64[ns]'):
            try:
                fdf[column] = pd.to_datetime(fdf[column], format='mixed')
                ### Because .astype() cannot handle a 'string' dtype with multiple date formats, but .to_datetime() can!
                ### Well, actually, there are some limitations: If the formats are too different, even pd.to_datetime() can't reconcile them.
                ### Adding argument `format='mixed'` might help (looks at each cell individually), but could cause problem if month & day are not consistently in order across the varying formats.
            except Exception as e:
                print('Error for column: ', column)
                print('Attempted dtype: ', dict_col_dtypes[column])
                print(e, '\n')
    print(f"Date columns changed with `format='mixed'`. Each cell is considered separately & it is assumed that month comes first before day (e.g., 3/4/24 is in March).")
    return fdf 

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

def fn_find_value(fdf, value_to_find='Unrecognized Value', one_id_var='Project Id', list_of_other_vars=[None], if_print=False, if_print_dfs=False):
    if (list_of_other_vars[0] is None):
        vars_to_select = [one_id_var]
    else:
        vars_to_select = [one_id_var] + list_of_other_vars
    ####
    fn_list = []
    for col_index, col in enumerate(fdf.columns):
        if (fdf[col].isin([value_to_find]).any()):
            fn_list.append({
                'col': col
                ,'col_index': col_index
                ,'col_row_indices': fdf[vars_to_select + [col]].query(f'`{col}` == @value_to_find').index.tolist() 
                ,'col_row_ids': fdf.query(f'`{col}` == @value_to_find')[one_id_var].tolist() 
                ,'col_df': fdf[vars_to_select + [col]].query(f'`{col}` == @value_to_find') 
            }) 
    ####
    if (if_print):
        for list_index, col in enumerate(fn_list):
            print({k: fn_list[list_index][k] for k in fn_list[list_index] if k != 'col_df'})
    ####
    if (if_print_dfs):
        for list_index, col in enumerate(fn_list):
            print(fn_list[list_index]['col_df'])
    ####
    return fn_list 

def fn_find_and_replace_value_in_df(fdf, one_id_var='mandatory', list_of_values_to_find=['Unrecognized Value'], replacement_value=pd.NA):
    # Normalize search values
    fdf.rename(columns={fdf.columns[0]: 'Project ID'}, inplace=True)
    list_of_values_to_find = [str(x).lower() for x in list_of_values_to_find]
    string_of_values_to_find = '|'.join(list_of_values_to_find)

    # Safely find actual column name for ID field
    actual_id_col = None
    for col in fdf.columns:
        if col.strip().lower() == one_id_var.strip().lower():
            actual_id_col = col
            break

    if not actual_id_col:
        print(f"❌ Could not find column matching ID field: '{one_id_var}'")
        actual_id_col = 'UNKNOWN_ID'

    fn_list = []

    for col_index, col in enumerate(fdf.columns):
        try:
            mask = fdf[col].astype(str).str.lower().isin(list_of_values_to_find)
            if mask.any():
                row_indices = fdf[mask].index.tolist()
                values = fdf.loc[mask, col].tolist()
                ids = fdf.loc[mask, actual_id_col].tolist() if actual_id_col in fdf.columns else ['UNKNOWN_ID'] * len(row_indices)

                fn_list.append({
                    'col': col,
                    'col_index': col_index,
                    'row_indices': row_indices,
                    'values': values,
                    'replaced_with': replacement_value,
                    'ids': ids
                })

        except Exception as e:
            print(f"❌ Error processing column '{col}': {e}")

    print('\n🔄 These values were replaced:')
    for entry in fn_list:
        print(f"Column: {entry['col']} | Rows: {len(entry['row_indices'])} | Sample IDs: {entry['ids'][:3]}...")

    fdf = fdf.replace(fr'(?i)^({string_of_values_to_find})$', replacement_value, regex=True)

    return fdf




def fn_find_and_count_value_in_df(fdf, list_of_values_to_find=['Unrecognized Value']):
    list_of_values_to_find = [str(x).lower() for x in list_of_values_to_find]
    return pd.Series(fdf.values.flatten()).astype('string').str.lower().isin(list_of_values_to_find).sum() 
    ### TODO: At the moment, searching is case-insensitive. Could make option for case sensitive.
    ### TODO: At the moment, the entire cell must match. Could make an option for matching with substrings.

def fn_print_and_return_object(object):
    print(object)
    return object

def fn_print_col_and_return_df(fdf, col, additional_text_before=''):
    print(additional_text_before, '\n', fdf[col])
    return fdf

def fn_print_fstring_and_return_df(df, fstring_to_print=''):
    print(fstring_to_print)
    return df

def fn_print_expression_and_return_df(df, expression, additional_text_before=''):
    print(additional_text_before, expression(df))
    return df

def fn_if_else(fdf, lambda_expression, if_true, if_false=None, if_na=None):
    if pd.isna(lambda_expression(fdf)):
        return if_na
    elif (lambda_expression(fdf)):
        return if_true
    elif (lambda_expression(fdf)) == False:
        return if_false
    else:
        return None



#%%##################################################
### END ###
#####################################################

print('Imported functions!')


