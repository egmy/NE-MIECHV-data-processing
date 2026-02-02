


# These are original compressed files from just FW (which are then extracted into next path below?):
# U:\MasterCompressed

#%%
import os
import shutil
import datetime
import pyodbc
import sys
import pandas as pd
from pathlib import Path
import win32com.client as win32
import tempfile

from pathlib import Path
print('Local Code Repository: ', str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))

sys.path.append(str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))
from RUNME import * 

if (os.path.basename(__file__) == '_1_1_1FW_access.py'):
    print('running 1.1FW')
    import sys
    sys.path.append(str(*[path for path in Path.cwd().parents if path.name == 'nehv_ds_code_repository']))
    from RUNME import *
else:
    print("Did NOT run RUNME again... because it's already running!")

# --- PATHS ---
path_111_FW_files_base = Path('U:\\Working\\nehv_ds_data_files\\2mid\\1main\\1.1FW\\1.1.1access')
path_111_FW_dir_input = Path(path_111_FW_files_base, '0in', str_nehv_quarter)
path_111_FW_dir_output = Path(path_111_FW_files_base, '9out', str_nehv_quarter)

path_111_FW_files_orig = Path(f'U:\\Working\\Tableau\\{str_nehv_year}\\{str_nehv_quarter}\\FamilyWise')


for path in [path_111_FW_dir_input, path_111_FW_dir_output, path_111_FW_files_orig]:
    path.mkdir(parents=True, exist_ok=True)


# --- FILE PATH NAME --- INPUT HERE!!!!!!!
path_111FW_input_NETable = Path(path_111_FW_dir_input, 'NETables20260108.accdb')


# --- Paths ---
db_path = str(Path(path_111_FW_dir_input, 'NETables20260108.accdb'))
dq_path = str(Path(path_111_FW_dir_input, f"{small_str_nehv_quarter} Data and Query.accdb"))

# --- Open Access ---
access = win32.Dispatch("Access.Application")
#access.Visible = True

# Open original database
access.closeCurrentDatabase()
access.OpenCurrentDatabase(db_path)

# Close it (NO ARGUMENTS)
access.CloseCurrentDatabase()

# Copy the file at OS level
shutil.copy2(db_path, dq_path)

# Open the new quarter file
access.OpenCurrentDatabase(dq_path)

access.closeCurrentDatabase()


# Paths
current_qtr = Path(f"U:\\Working\\Tableau\\{str_nehv_year}\\{str_nehv_quarter}\\FamilyWise\\{small_str_nehv_quarter} Data and Query.accdb")
previous_qtr = Path(f"U:\\Working\\Tableau\\{str_nehv_year}\\{str_nehv_quarter}\\FamilyWise\\{small_str_nehv_previous_quarter} Data and Query.accdb")

access.OpenCurrentDatabase(str(current_qtr))
temp_dir = Path(tempfile.gettempdir())
trusted_prev_db = temp_dir / previous_qtr.name

shutil.copy2(previous_qtr, trusted_prev_db)

db_prev = access.DBEngine.OpenDatabase(str(trusted_prev_db))
query_names = [
    qd.Name
    for qd in db_prev.QueryDefs
    if not qd.Name.startswith("~") and not qd.Name.startswith("MSys")
]

for q in query_names:
    try:
        access.DoCmd.TransferDatabase(
            0, #acImport
            "Microsoft Access",
            str(trusted_prev_db),
            1, #acQuery
            q,
            q
        )
        print(f"✔ Imported query: {q}")
    except Exception as e:
        print(f"⚠️ Skipped {q}: {e}")

print("✔ All queries imported successfully")


# Table you want to import explicitly
make_tables = [
    "15 Caregiver Education Make Table",
    "F1 - Employment Make Table",
    "F1 - Marital Status Make Table 2",
    "F1 - Misc Make Table",
    "Site IDs"
]

# Start Access
#access = win32.Dispatch("Access.Application")
#access.Visible = False

# Open the current quarter file
#access.OpenCurrentDatabase(str(current_qtr))

# Import from previous quarter
for q in make_tables:
    print(f"Importing table: {q}")
    try:
        access.DoCmd.TransferDatabase(
            0,  # 0 = acImport
            "Microsoft Access",
            str(trusted_prev_db),
            0,  # 0 = acTable
            q,  # source object name
            q   # destination name (can rename if needed)
        )
    except Exception as e:
        print(f"⚠️ Could not import {q}: {e}")

make_table_queries = [
    "15 Caregiver Education Make Table 2",
    "F1 - Employment Make Table",
    "F1 - Marital Status Make Table 2",
    "F1 - Misc Make Table",
]


for q in make_table_queries:
    print(f"Running Make Table query: {q}")
    try:
        access.DoCmd.OpenQuery(q, 0)  # 0 = acViewNormal (RUN)
        access.DoCmd.Close(1, q, 2)   # 1 = acQuery, 2 = acSaveNo
        print(f"✔ {q} completed")
    except Exception as e:
        print(f"❌ Error running {q}: {e}")


# --- Verify Data Tables ---
conn_str = (
    r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
    fr"DBQ={dq_path};"
)
conn = pyodbc.connect(conn_str)

# --- Add AutoNum Field to Referrals table ---
cursor = conn.cursor()
try:
    cursor.execute("ALTER TABLE Referrals ADD COLUMN AutoNum COUNTER;")
    conn.commit()
except Exception as e:
    print("AutoNum field may already exist:", e)


# --- SETTINGS ---

print("Checking date filters...\n")


##08 Child ER Injury - IncidentDate >= #date_fy_start# And IncidentDate <= #fyend#
##11 Early Literacy - HVDate >= #date_fy_start#
##13a Behavioral Concerns - HVDate>=#date_fy_start# And >=[TGT DOB-CR]
##F1 - Home Visit Type a - HVDate >= #date_fy_start#
##Home Visits Prenatal and Total Part1 - HVDate >= #date_fy_start#

# --- QUERIES AND THEIR REQUIRED FILTERS ---
# Each entry defines:
# query name : (field_to_filter, "custom_condition")

#access.OpenCurrentDatabase(current_qtr)
db=access.CurrentDb()
filters = {
    "08 Child ER Injury":
        ("IncidentDate", "{field} >= {fy_start} And {field} <= {fy_end}"),

    "11 Early Literacy":
        ("HVDate", "{field} >= {fy_start}"),

    "13a Behavioral Concerns":
        ("HVDate", "{field} >= {fy_start} And {field} >= [TGT DOB-CR]"),

    "F1 - Home Visit Type a":
        ("HVDate", "{field} >= {fy_start}"),

    "Home Visits Prenatal and Total Part1":
        ("HVDate", "{field} >= {fy_start}"),
}


def update_where_clause(sql, filter_text):
    # Normalize spacing
    sql_clean = " ".join(sql.split())

    # Detect WHERE clause
    where_match = re.search(r"\bWHERE\b(.*)", sql_clean, flags=re.IGNORECASE)

    if where_match:
        where_content = where_match.group(1)

        # If a date filter already exists → replace it
        new_where = re.sub(
            r"(IncidentDate|HVDate)\s*>?=?\s*#\d{1,2}/\d{1,2}/\d{4}#(.*?)(And\s+#\d{1,2}/\d{1,2}/\d{4}#)?",
            filter_text,
            where_content,
            flags=re.IGNORECASE
        )

        sql_updated = sql_clean.replace(where_content, " " + new_where)
        return sql_updated

    else:
        # No WHERE clause — add one
        return sql_clean + f" WHERE {filter_text}"

# --- PROCESS EACH QUERY ---
for q_name, (field, template) in filters.items():
    print(f"--- Updating {q_name} ---")
    try:
        qd = db.QueryDefs(q_name)
        sql = qd.SQL

        # Build filter text
        filter_text = template.format(field=field, fy_start=date_fy_start, fy_end=date_fy_end_day_after)

        # Update SQL
        new_sql = update_where_clause(sql, filter_text)

        if sql.strip() == new_sql.strip():
            print("✔ Already correct or no change needed.")
        else:
            qd.SQL = new_sql
            print("✔ Updated query with:", filter_text)

    except Exception as e:
        print(f"Error reading/updating {q_name}: {e}")

    print()



# --- Export Queries to Excel ---
export_queries = [
    "ID File",
    "04 Well Child Visit v2 no MAX - use this one",
    "08 Child ER Injury",
    "14 IPV Screen FROG",
    "16 Caregiver Insurance v2 - USE THIS ONE",
    "Child Activities Query",
    "Adult Activities Query FROG",
    "Adult UNCOPE Query",
    "F1 - Home Visit Type Query",
    "Referral Exclusions 1 thru 6"
]



acExport = 1
acSpreadsheetTypeExcel12Xml = 10  # .xlsx

for q in export_queries:
    print(f"Exporting {q} via Access...")
    out_path = path_111_FW_dir_output / f"{q}.xlsx"

    try:
        access.DoCmd.TransferSpreadsheet(
            acExport,
            acSpreadsheetTypeExcel12Xml,
            q,
            str(out_path),
            True
        )
        print(f"✔ Saved {q} → {out_path}")
    except Exception as e:
        print(f"❌ Failed exporting {q}: {e}")

# --- CLEANUP ---
conn.close()
access.CloseCurrentDatabase()
access.Quit()
print("All done.")
