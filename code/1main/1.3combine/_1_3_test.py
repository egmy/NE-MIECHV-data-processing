
#%%
### Run 1.3 first!

path_test_path_13output = Path(path_13_dir_output, 'Child Activity Master File.xlsx')
path_test_path_13joe = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.3combine\\previous\\final Master File output\\Y13Q2 (Oct 2023 - Mar 2024)', 'Child Activity Master File.xlsx')

#%%
### Read:
test_df_13output = pd.read_excel(path_test_path_13output, sheet_name='Well Child', keep_default_na=False, na_values=list_na_values_to_read, dtype='string')
test_df_13joe = pd.read_excel(path_test_path_13joe, sheet_name='Well Child', keep_default_na=False, na_values=list_na_values_to_read, dtype='string')

#%%
test_df_13output = test_df_13output.assign(source = 'output')
#%%
test_df_13joe = test_df_13joe.assign(source = 'joe')
#%%
test_df_union = pd.concat([test_df_13output, test_df_13joe]).query('(quarter.isna()) or (quarter == "2")')
test_df_union

#%%
names_of_cols = [*test_df_union]
names_of_cols.remove('source')
names_of_cols.remove('year')
names_of_cols.remove('quarter')
names_of_cols.remove('funding')

#%%
### Show duplicate rows:
test_df_13_differences = (
    test_df_union[~(test_df_union.duplicated(keep = False, subset = names_of_cols))]
    .sort_values(by='ProjectID')
)

# %%
test_df_13_differences
###

# %%
