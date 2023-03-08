
### Purpose: Testing.

# %% ################################################
### PACKAGES ###
#####################################################

### pip list ### See packages installed in this virtual environment that can be imported.

import pandas as pd
import numpy as np
import random 
from faker import Faker

# from datetime import date
# import matplotlib.pyplot as plt

### Test that pandas imported:
print('pandas verion: ' + pd.__version__)

# %% ################################################
### Create Fake Data ###
#####################################################

### https://pypi.org/project/Faker/
### https://insightsndata.com/how-to-generate-test-data-using-python-eb854a88d817

### Test
# fake = Faker()
# fake.name()
# fake.address()
# fake.text()

# %% 
def generate_fake_data(number_of_records):
    fake = Faker()
    data_list = []
    ### columns_list = ['id', 'FirstName', 'LastName','Gender', 'DateOfBirth','PhoneNumber','Occupation','Company','PersonalEmail','OfficialEmail','BSNNumber','IBAN','HouseNumber','StreetName','PostCode','City','Country']
    columns_list = ['id', 'Name1', 'Name2','Gender', 'DateOfBirth_text','PhoneNumber','Occupation','OfficialEmail','BSNNumber','HouseNumber','PostCode']
    for val in range(number_of_records):
        ### data_list.append([fake.unique.random_int(), fake.first_name(), fake.last_name(), fake.profile()['sex'], fake.date_of_birth(),fake.phone_number(), fake.job(), fake.company(), fake.email(), fake.company_email(), fake.ssn(), fake.iban(), fake.building_number(),fake.street_name(), fake.postcode(), fake.city(), 'USA'])
        data_list.append([fake.unique.random_int(), fake.first_name(), fake.last_name(), fake.profile()['sex'], fake.date_of_birth(),fake.phone_number(), fake.job(), fake.company_email(), fake.ssn(), fake.building_number(), fake.postcode()])

    df = pd.DataFrame(data_list,columns=columns_list)
    return(df)

# %% 
df = generate_fake_data(1000)

# %% ################################################
### Editing Pandas DataFrame ###
#####################################################

# %% 
### DOB to date type.
df.dtypes
df['DateOfBirth_datetime'] = pd.to_datetime(df['DateOfBirth_text'])

# %% 
### Add in NaN:
df['Name1'] = df['Name1'].sample(frac=0.5)

# %% 
### New column:
df['_Agency'] = list(map(str, random.choices(list(range(1,21)), k=len(df))))
df['_Agency'].value_counts()

# %% 
### New column:
df['strings1'] = random.choices(['YES','1','0','-1','other'], k=len(df))
df['strings1'].value_counts()

# %% ################################################
### Test Editing Pandas DataFrames ###
#####################################################

df

# %% 
### DUPLICATING
df['new1'] = df['OfficialEmail']
df

# %% 
### COALESCING
df['Name'] = df['Name1'].combine_first(df['Name2'])
df

# %% 
### DATE CALCULATIONS
### These calculations assume all date variables are dtype "datetime64".
df['DOB_plus30days'] = df['DateOfBirth_datetime'] + pd.Timedelta(days=30) 
df

# %% 
### IF/ELSE, CASE/WHEN
def function1(fdf):
    if ('11' not in fdf['_Agency']):
        match fdf['strings1']:  ### FW
            case "YES":
                return 1
            case "1":
                return 1
            case "0":
                return 0
            case "-1":
                return -1
            case _:
                return None
    elif ('11' in fdf['_Agency']):
        return None  ### add CASE for LLCHD values when they add them to their dataset

# %% 
df['_C2_BF_Status'] = df.apply(func=function1, axis=1)
df

# %%
def function2(fdf):
    if ('11' not in fdf['_Agency']):
        return False
    elif ('11' in fdf['_Agency']):
        return True

# %%
df['AgencyTest1'] = df.apply(func=function2, axis=1)
df

# %% 
### Inspect:
df.groupby(['AgencyTest1','strings1'])._C2_BF_Status.value_counts(sort=False, dropna=False)

# %% 
### Inspect:
df['_Agency'].value_counts(sort=False, dropna=False)

# %%
df['AgencyTest1'].value_counts(sort=False, dropna=False)

# %% 
### Inspect:
df['strings1'].value_counts(sort=False, dropna=False)

# %% 
### Inspect:
df['_C2_BF_Status'].value_counts(sort=False, dropna=False)

# %% 
df[['strings1','_C2_BF_Status']].value_counts(sort=False, dropna=False)

################################################
# %%
df['AgencyTest2'] = df.apply(
        lambda fdf: (
            False if ('11' not in fdf['_Agency']) 
            else ( True if ('11' in fdf['_Agency']) 
                else None )
        ), 
        axis=1
    )
df

# %%
df[['AgencyTest1','AgencyTest2']].value_counts(sort=False, dropna=False)

################################################
# %%
# df['_C2test1'] = df.apply(
#         lambda fdf: (
#             if ('11' not in fdf['_Agency']):
#                 match fdf['strings1']:  
#                     case "YES":
#                         return 1
#                     case "1":
#                         return 1
#                     case "0":
#                         return 0
#                     case "-1":
#                         return -1
#                     case _:
#                         return None
#             elif ('11' in fdf['_Agency']):
#                 return None  
#         ), 
#         axis=1
#     )




# %%
