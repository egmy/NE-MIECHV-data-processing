
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

### Data Source for 1st Tableau file:
    ### ID File
path_1_data_source = 'U:\Working\Tableau\Y12 (Oct 2022 - Sept 2023)\Combined ID File.xlsx'

df1_1 = pd.read_excel(path_1_data_source, sheet_name='Project ID')
df1_2 = pd.read_excel(path_1_data_source, sheet_name='FamilyWise')
df1_3 = pd.read_excel(path_1_data_source, sheet_name='LLCHD')

#####################################################
### JOIN ###
#####################################################

### join with pandas.merge() or pandas.df.join()
### https://pandas.pydata.org/docs/reference/api/pandas.merge.html
    ### Can add a "validate" argument.

df1_12 = pd.merge(df1_1, df1_2, how='left', left_on='Project Id', right_on='Project ID', indicator=True) 
df1 = pd.merge(df1_12, df1_3, how='left', left_on='Project Id', right_on='project id (LLCHD)', indicator=True)


#####################################################
### RECREATE every Tableau Calculation ###
#####################################################

### numpy.where() --- Sami: use to treat NULL values weird at times; may be quicker than apply.
### pandas

### Not needed?
    ### Number of Records

df1['_01 Project ID'] = [Project Id]

df1['_02 Agency'] = IFNULL([Agency],[Site Id])

df1['_03 Family Number'] = IFNULL([Family Id],[Family Number])

df1['_04 Child Number'] = IFNULL([Tgt Id],[Child Number])

df1['_05 Enrollment Date'] = IFNULL([Enroll Date],[Enroll Dt])

df1['_06 Discharge Date'] = IFNULL([DischargeDate],[Discharge Dt])

df1['_07 Discharge Reason'] = 
IF NOT ISNULL([Discharge Dt]) THEN CASE [Discharge Reason] ###LLCHD, see full reasons below
    WHEN "1" THEN "Completed Services" 
    WHEN "Family Has Met Program Goals" THEN "Completed Services"
    ELSE "Stopped Services Before Completion"
    END
ELSEIF NOT ISNULL([DischargeDate]) THEN CASE [Discharge Status] ###FW
    WHEN "Death of child/fetus/primary caregiver" THEN "Stopped Services Before Completion"
    WHEN "Did not respond to Creative Outreach" THEN "Stopped Services Before Completion"
    WHEN "CPS involvement, no longer serving family" THEN "Stopped Services Before Completion"
    WHEN "Custody change, involuntary" THEN "Stopped Services Before Completion"
    WHEN "Custody change, voluntary" THEN "Stopped Services Before Completion"
    WHEN "Family/Child aged out" THEN "Stopped Services Before Completion"
    WHEN "Lack of capacity" THEN "Stopped Services Before Completion"
    WHEN "Linked with more appropriate services" THEN "Stopped Services Before Completion"
    WHEN "Moved, Link to HFA or other programs initiated successfully" THEN "Stopped Services Before Completion"
    WHEN "Moved, no links made" THEN "Stopped Services Before Completion"
    WHEN "Never Enrolled - Client Refused Initiation of Services" THEN "Stopped Services Before Completion"
    WHEN "Never Enrolled - No Information Available" THEN "Stopped Services Before Completion"
    WHEN "Never fully engaged" THEN "Stopped Services Before Completion"
    WHEN "No longer able to contact or locate" THEN "Stopped Services Before Completion"
    WHEN "Not available for services - jail, hospitalization, treatment" THEN "Stopped Services Before Completion"
    WHEN "Refused continuation of services" THEN "Stopped Services Before Completion"
    WHEN "Safety Issues" THEN "Stopped Services Before Completion"
    WHEN "Family graduated/met all program goals" THEN "Completed Services" ### Completed Services   
    WHEN NULL THEN "Stopped Services Before Completion"
    ###need to add all FW reasons here
    ELSE "Unrecognized Value"
    END
ELSE NULL
END
###LLCHD discharge reasons
###1Family graduated/met all program goals
###2Family moved out of service area
###3Parent/guardian returned to school
###4Parent/guardian returned to work
###5Parent/guardian refused service
###6Death of participant
###7Unable to locate family
###8Target child adopted
###9Target child entered foster care
###10Target child living with another care giverx
###11Target child entered school/child care
###12Family never engaged
###13Unknown & a text box

df1['_08 Home Visit Number'] = IFNULL([home_visits_num],[MaxOfVISIT NUMBER])

df1['_09 Funding'] = 
IF [_02 Agency]="ll" THEN [Funding]
ELSEIF [_02 Agency] = "ps" THEN "S"
ELSEIF [_02 Agency] = "nc" THEN "S"
ELSEIF [_02 Agency] = "vn" THEN "S"
ELSEIF [_02 Agency] = "ph" THEN "F"
ELSEIF [_02 Agency] = "fs" THEN "F"
ELSEIF [_02 Agency] = "hs" THEN "F"
ELSEIF [_02 Agency] = "se" THEN "TANF"
ELSE "Unrecognized Value"
END

df1['_10 MOB First Name'] = IFNULL([Mob First Name], [Mob First-Cr])

df1['_11 MOB Last Name'] = IFNULL([Mob Last Name],[Mob Last-Cr])

df1['_12 MOB DOB'] = 
IF [Mob Dob] = DATE(1/1/1900) THEN NULL ###FW
ELSEIF [Mob Dob1] = DATE(1/1/1900) THEN NULL ###LLCHD
ELSE IFNULL([Mob Dob],[Mob Dob1])
END

df1['_13 MOB SSN'] = 
IF NOT ISNULL([Mob Ss.]) THEN CASE [Mob Ss.] ###FW
    WHEN "555555555" THEN NULL
    WHEN "999999999" THEN NULL
    WHEN "000000000" THEN NULL
    WHEN "333333333" THEN NULL
    ELSE [Mob Ss.]
    END
ELSEIF NOT ISNULL([Mob Ssn]) THEN CASE [Mob Ssn] ###LLCHD
    WHEN "555-55-5555" THEN NULL
    WHEN "999-99-9999" THEN NULL
    WHEN "000-00-0000" THEN NULL
    WHEN "333-33-3333" THEN NULL
    ELSE [Mob Ssn]
    END
ELSE NULL
END

df1['_14 TGT First Name'] = IFNULL([Tgt First Name],[Tgt First-Cr])

df1['_15 TGT Last Name'] = IFNULL([Tgt Last Name],[Tgt Last-Cr])

df1['_16 TGT DOB'] = 
IF [Tgt Dob] = DATE(1/1/1900) THEN NULL
ELSEIF [Tgt Dob-Cr] = DATE(1/1/1900) THEN NULL
ELSE IFNULL([Tgt Dob],[Tgt Dob-Cr])
END

df1['_17 TGT SSN'] = 
IF NOT ISNULL([Tgt Ss Number]) THEN CASE [Tgt Ss Number] ###FW
    WHEN "555555555" THEN NULL
    WHEN "999999999" THEN NULL
    WHEN "000000000" THEN NULL
    WHEN "333333333" THEN NULL
    ELSE [Tgt Ss Number]
    END
ELSEIF NOT ISNULL([Tgt Ssn]) THEN CASE [Tgt Ssn] ###LLCHD
    WHEN "555-55-5555" THEN NULL
    WHEN "999-99-9999" THEN NULL
    WHEN "000-00-0000" THEN NULL
    WHEN "333-33-3333" THEN NULL
    ELSE [Tgt Ssn]
    END
ELSE NULL
END

df1['_18 TGT Gender'] = IFNULL([TGT GENDER],[Tgt Gender])

df1['_19 FOB Involved'] = 
IF [Fob Involved] = True THEN 1
ELSEIF [Fob Involved] = False THEN 0
ELSEIF [Fob Involved1] = "Y" THEN 1
ELSEIF [Fob Involved1] = "N" THEN 0
END

df1['_20 FOB First Name'] = 
IF [Fob Involved] = False THEN NULL ###FW
ELSEIF [Fob Involved1] = "N" THEN NULL ###LLCHD
ELSE IFNULL([Fob First], [Fob First Name])
END

df1['_21 FOB Last Name'] = 
IF [Fob Involved] = False THEN NULL ###FW
ELSEIF [Fob Involved1] = "N" THEN NULL ###LLCHD
ELSE IFNULL([Fob Last Name], [Fob Last])
END

df1['_22 FOB DOB'] = 
IF [Fob Involved] = True   ### FW
    THEN 
        CASE [Fob Dob] ###FW
            WHEN DATE(1/1/1900) THEN NULL
            WHEN NULL THEN NULL
        ELSE [Fob Dob]
        END
ELSEIF[Fob Involved1] = "Y"   ### LLCHD
    THEN 
        CASE [Fob Dob1] ###LLCHD
            WHEN DATE(1/1/1900) THEN NULL
            WHEN NULL THEN NULL
        ELSE [Fob Dob1]
        END
ELSE NULL
END

df1['_23 FOB SSN'] = 
IF [Fob Involved] = True THEN CASE [Fob Ss Number] ###FW
    WHEN "555555555" THEN NULL
    WHEN "999999999" THEN NULL
    WHEN "000000000" THEN NULL
    WHEN "333333333" THEN NULL
    ELSE [Fob Ss Number]
    END
ELSEIF [Fob Involved1] = "Y" THEN CASE [Fob Ssn] ###LLCHD
    WHEN "555-55-5555" THEN NULL
    WHEN "999-99-9999" THEN NULL
    WHEN "000-00-0000" THEN NULL
    WHEN "333-33-3333" THEN NULL
    ELSE [Fob Ssn]
    END
ELSE NULL
END

df1['_24 Address'] = IFNULL([Address],[Mob Address])

df1['_25 City'] = IFNULL([City],[Mob City])

df1['_26 Zip'] = IFNULL(([Zip]),[Mob Zip])


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

### Output for 1st Tableau file: ID File.
path_1_output = ''

### Export as Excel:









