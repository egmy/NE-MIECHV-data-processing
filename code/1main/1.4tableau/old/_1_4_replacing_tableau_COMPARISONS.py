
### Purpose: In the Nebraska MIECHV data sourcing process, replace the steps currently completed by Tableau.

#%%
exec(open('RUNME.py').read())


#%%##################################################
### INSTRUCTIONS ###
#####################################################

### TO DO: Instructions for how to get into environment & how to edit/run code files.

#%%##################################################
### SETUP ###
#####################################################

if __name__ == "__main__":
    from _1_4tab_RUNME import * 
    print('Imported "_1_4tab_RUNME"')


#%%##################################################
### PATHS & READING ###
#####################################################

### Files created for Y12Q1 by the old data sourcing process with Tableau.

###path_14t_comparison_csv_tb2 = Path('U:\\Working\\nebraska_miechv_coded_data_source\\previous\\previous_output\\Y12Q1 (Oct 2022 - Dec 2023)\\Child Activity Master File from Excel on NE Server.csv')
df_14t_comparison_csv_tb2 = pd.read_csv(path_14t_comparison_csv_tb2, dtype=object, keep_default_na=False, na_values=list_na_values_to_read)
df_14t_comparison_csv_tb2 = df_14t_comparison_csv_tb2.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)

###path_14t_comparison_csv_tb3 = Path('U:\\Working\\nebraska_miechv_coded_data_source\\previous\\previous_output\\Y12Q1 (Oct 2022 - Dec 2023)\\Adult Activity Master File from Excel on NE Server.csv')
df_14t_comparison_csv_tb3 = pd.read_csv(path_14t_comparison_csv_tb3, dtype=object, keep_default_na=False, na_values=list_na_values_to_read)
df_14t_comparison_csv_tb3 = df_14t_comparison_csv_tb3.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)

###path_14t_comparison_csv_tb4 = Path('U:\\Working\\nebraska_miechv_coded_data_source\\previous\\previous_output\\Y12Q1 (Oct 2022 - Dec 2023)\\Adult Activity Master File for Form 1 from Excel on NE Server.csv')
df_14t_comparison_csv_tb4 = pd.read_csv(path_14t_comparison_csv_tb4, dtype=object, keep_default_na=False, na_values=list_na_values_to_read)
df_14t_comparison_csv_tb4 = df_14t_comparison_csv_tb4.sort_values(by=['Project Id','Year','Quarter'], ignore_index=True)


#%%##################################################
### COMPARE 2(Child) to 3(Adult) ###
#####################################################

#%%
### Differences: Columns only in one.
set([*df_14t_comparison_csv_tb2]).symmetric_difference([*df_14t_comparison_csv_tb3])

#%%
### Overlap / Similarities: Columns in both.
set([*df_14t_comparison_csv_tb2]).intersection([*df_14t_comparison_csv_tb3])

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
set([*df_14t_comparison_csv_tb3]).symmetric_difference([*df_14t_comparison_csv_tb4])

### Created columns in only df_14t_comparison_csv_tb3:

### Created columns in only df_14t_comparison_csv_tb4:



#%%
### Overlap / Similarities: Columns in both.
set([*df_14t_comparison_csv_tb3]).intersection([*df_14t_comparison_csv_tb4])

### Created columns in both:
    ### Lots!




#%%
