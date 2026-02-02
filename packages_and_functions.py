

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
    
COUNTY_ZIPS = {
    "Adams": {68901, 68902, 68925, 68950, 68955, 68956, 68973},
    "Antelope": {68636, 68720, 68726, 68756, 68761, 68764, 68773},
    "Arthur": {69121},
    "Banner": {69345},
    "Blaine": {68821, 68833, 69157},
    "Boone": {68603, 68627, 68652, 68655, 68658, 68660},
    "Box Butte": {69301, 69348},
    "Boyd": {68719, 68722, 68746, 68755, 68777},
    "Brown": {69210, 69214, 69217},
    "Buffalo": {68812, 68836, 68840, 68845, 68847, 
                68848, 68849, 68858, 68861, 68866, 68869, 
                68870, 68876},
    "Burt": {68019, 68020, 68038, 68045, 68061},
    "Butler": {68001, 68014,68036, 68624, 68626, 68632, 68635, 68667, 68668, 68669},
    "Cass": {68016, 68037, 68048, 68058, 68305, 68347, 68349, 68366, 68403, 
            68407, 68409, 68413, 68455, 68463},
    "Cedar": {68717, 68727, 68736, 68739, 68745, 68749, 68771, 68774, 68792},
    "Chase": {69023, 69027, 69032, 69045},
    "Cherry": {69135, 69201, 69211, 69216, 69218, 69219, 69220, 69221},
    "Cheyenne": {69131, 69141, 69149, 69156, 69160, 69162},
    "Clay": {68452, 68933, 68934, 68935, 68938, 68941, 68944, 68954, 68975, 68979, 68980},
    "Colfax": {68629, 68641, 68643, 68659, 68661},
    "Cuming":{68004, 68716, 68788, 68791},
    "Custer":{68813, 68814, 68822, 68825, 68828, 68855, 68856, 68860, 68874, 68881, 69120},
    "Dakota": {68030, 68731, 68741, 68743, 68776},
    "Dawes": {69337, 69339, 69354, 69367}, 
    "Dawson": {68834, 68850, 68863, 68878, 69029, 69130, 69138, 69171},
    "Deuel":{69122, 69129}, 
    "Dixon": {68710, 68728, 68732, 68733, 68751, 68757, 68770, 68784, 68785},
    "Dodge":{68025, 68026, 68031, 68044, 68057, 68063, 68072, 68621, 68633, 68649, 68664},
    "Douglas": {
        68007, 68010, 68022, 68064, 68069,
        68101, 68102, 68103, 68104, 68105, 68106,
        68107, 68108, 68109, 68110, 68111, 68112,
        68114, 68116, 68117, 68118, 68119, 68120,
        68122, 68124, 68127, 68130, 68131, 68132,
        68134, 68135, 68137, 68139, 68142, 68144,
        68145, 68152, 68154, 68155, 68164, 68172,
        68175, 68176, 68178, 68179, 68180, 68181,
        68182, 68183, 68197, 68198
    },
    "Dundy":{69021, 69030, 69037, 69041},
    "Filmore": {68351, 68354, 68361, 68365, 68406, 68416, 68436,
            68444},
    "Franklin": {68929, 68932, 68939, 68947, 68960, 68972, 68981},
    "Frontier": {69025, 69028, 69038, 69039, 69042},
    "Furnas": {68922, 68926, 68936, 68946, 68948, 68967, 69022,
            69046},
    "Gage": {68301, 68307, 68309, 68310, 68318,
            68328, 68331, 68357, 68381, 68415,
            68422, 68458, 68466},
    "Garden": {69147, 69148, 69154, 69190},
    "Garfield": {68823},
    "Gosper": {68937, 68976},
    "Grant" : {69333, 69350, 69366},
    "Greeley": {68665, 68842, 68875, 68882},
    "Hall":{68801, 68802, 68803, 68810, 68824, 68832, 68883},
    "Hamilton": {68818, 68841, 68843, 68846, 68854, 68865},
    "Harlan": {68920, 68965, 68966, 68969, 68971, 68977},
    "Hayes": {69031},
    "Hitchcock": {69024, 69040, 69043, 69044},
    "Holt": {68711, 68713, 68725, 68734, 68735, 68742, 68763,
        68766, 68780},
    "Hooker": {69152},
    "Howard": {68820, 68831, 68835, 68838, 68872, 68873},
    "Jefferson": {68338, 68342, 68350, 68352, 68377, 68424, 68429,
        68440},
    "Johnson": {68329, 68332, 68348, 68443, 68450},
    "Kearney": {68924, 68945, 68959, 68982},
    "Keith": {69127, 69144, 69146, 69153, 69155},
    "Keya Paha": {68753, 68778},
    "Kimball": {69128, 69133, 69145},
    "Knox": {68718, 68724, 68729, 68730, 68760, 68783, 68786,
        68789}, 
    "Lancaster": {
        68317, 68336, 68339, 68358, 68368, 68372,
        68402, 68404, 68419, 68428, 68430, 68438,
        68461, 68462,
        68501, 68502, 68503, 68504, 68505, 68506,
        68507, 68508, 68509, 68510, 68511, 68512,
        68514, 68516, 68517, 68520, 68521, 68522,
        68523, 68524, 68526, 68527, 68528, 68529,
        68531, 68532, 68542, 68544, 68583, 68588
    },
    "Lincoln": {69101, 69103, 69123, 69132, 69143, 69151, 69165, 69169, 69170},
    "Logan": {69163},
    "Loup": {68879},
    "Madison": {68701, 68702, 68715, 68748, 68752, 68758, 68781},
    "McPherson": {69167},
    "Merrick": {68628, 68663, 68816, 68826, 68827, 68864},
    "Morrill": {69125, 69331, 69334, 69336},
    "Nance": {68623, 68638, 68640},
    "Nemaha": {68304, 68320, 68321, 68378, 68379, 68414, 68421},
    "Nuckolls":{68943, 68957, 68961, 68964, 68974, 68978},
    "Otoe": {68324, 68344, 68346, 68382, 68410, 68417, 68418, 68446,
        68448, 68454},
    "Pawnee": {68323, 68345, 68380, 68420, 68441, 68447},
    "Perkins": {69134, 69139, 69140, 69150, 69168},
    "Phelps":{68923, 68927, 68940, 68949, 68958},
    "Pierce": {68738, 68747, 68765, 68767, 68769},
    "Platte": {68601, 68602, 68631, 68634, 68642, 68644, 68647, 68653},
    "Polk": {68651, 68654, 68662, 68666},
    "Red Willow": {69001, 69020, 69026, 69033, 69034, 69036},
    "Richardson":{68337, 68355, 68376, 68431, 68433, 68437, 68442,
        68457},
    "Rock": {68714, 68759},
    "Saline": {68333, 68341, 68343, 68359, 68445, 68453, 68464,
        68465},
    "Sarpy": {
        68005, 68028, 68046, 68056, 68059,
        68113, 68123, 68128, 68133, 68136,
        68138, 68147, 68157
    },
    "Saunders": {68003, 68015, 68017,
        68018, 68033, 68040, 68041,
        68042, 68050, 68065, 68066, 68070, 68073, 68648
    },
    "Scotts Bluff": {69341, 69352, 69353, 69355, 69356, 69357, 69358,
        69361, 69363},
    "Seward": {68313, 68314, 68330, 68360, 68364, 68405, 68423, 68434, 68439,
        68456},
    "Sheridan": {69335, 69340, 69343, 69347, 69351, 69360, 69365},
    "Sherman" : {68817, 68844, 68852, 68853, 68871},
    "Sioux": {69346},
    "Stanton": {68768, 68779},
    "Thayer": {68303, 68315, 68322, 68325, 68326, 68327, 68335, 68340,
        68362, 68370, 68375},
    "Thomas": {69142, 69161, 69166},
    "Thurston": {68039, 68047, 68055, 68062, 68067, 68071},
    "Todd": {69212},
    "Valley": {68815, 68837, 68859, 68862},
    "Washington": {68002, 68008, 68009, 68023, 68029, 68034, 68068},
    "Wayne": {68723, 68740, 68787, 68790},
    "Webster": {68928, 68930, 68942, 68952, 68970},
    "Wheeler": {68622, 68637},
    "York": {68316, 68319, 68367, 68371, 68401, 68460, 68467}

}


#%%##################################################
### END ###
#####################################################

print('Imported functions!')


