







######################################
#%%###################################
### <> TEMPLATE 

#%%
### 1. Test that DFs identical:
df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)

#%%
### 2. Make change:
print('TEMPLATE') #TODO
df_12LL_after_BaseTable = (
    df_12LL_after_BaseTable
    #. #TODO
)

#%%
### 3. Manual/Visual checks:
print(f'Still equal?: {df_12LL_before_BaseTable.equals(df_12LL_after_BaseTable)}')

#%%
### See differences:
df_12LL_before_BaseTable.compare(df_12LL_after_BaseTable)

#%%
### 4. Programmatically test change:
print('For change "TEMPLATE"...') #TODO
### ________________________________

if (df_12LL_before_BaseTable.isna().sum().sum() == df_12LL_after_BaseTable.isna().sum().sum()):
    print('Passed Test 1: Number of NA unchanged.')
else:
    raise Exception('**Test 1 Failed: Number of NA has changed.')
### ________________________________

if (len(df_12LL_before_BaseTable) == len(df_12LL_after_BaseTable)): 
    print('Passed Test 2: Number of rows unchanged.')
else:
    raise Exception('**Test 2 Failed: Number of rows has changed.')
### ________________________________

if ((len(df_12LL_before_BaseTable.columns) == len(df_12LL_after_BaseTable.columns))
    and ([*df_12LL_before_BaseTable] == [*df_12LL_after_BaseTable])): 
    print('Passed Test 3: Number and names of columns unchanged.')
else:
    raise Exception('**Test 3 Failed: Number or names of columns has changed.')
### ________________________________

if (True): #TODO
    print('Passed Test 4:')
else:
    raise Exception('**Test 4 Failed:')
### ________________________________

print('All tests passed!')
print(f'Rows: {len(df_12LL_after_BaseTable)}')
print(f'Columns: {len(df_12LL_after_BaseTable.columns)}')

#%%
### 5. Make DFs identical:
df_12LL_before_BaseTable = df_12LL_after_BaseTable.copy() 







