import os
import sqlite3
import pandas as pd

dir = "./scorecard"

################################################################################

# Read the data dictionary
dict_path = os.path.join(dir, "CollegeScorecardDataDictionary.xlsx")
dict_df = pd.read_excel(dict_path, sheet_name="Institution_Data_Dictionary")

# Extract the required columns and drop missing rows
dict_df = dict_df[["VARIABLE NAME", "dev-category", "API data type", "developer-friendly name"]]
dict_df = dict_df.dropna(subset=["VARIABLE NAME"])

print(dict_df.head())

# Group the variables by their category
categories = dict_df.groupby('dev-category')

category_mappings = {}
for category, group in categories:
    if category != "root":
        category_mappings[category] = ["cohort", "UNITID"] + group['VARIABLE NAME'].tolist()
    else:
        category_mappings[category] = ["cohort"] + group['VARIABLE NAME'].tolist()

print("Successfully mapped variables to categories.")

################################################################################

conn = sqlite3.connect('education.db')

data_type_mapping = {
    "autocomplete": "TEXT",
    "integer": "INTEGER",
    "float": "REAL",
    "long": "INTEGER",
    "string": "TEXT"
}

# Drop the previously created tables to start fresh
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
tables = [table[0] for table in tables]
for table in tables:
    conn.execute(f"DROP TABLE IF EXISTS {table};")

# For each category, excluding "root", create a SQL table
for category, columns in category_mappings.items():
    # Create a table for the category
    table_name = category.replace("-", "_")  # Make sure table name is SQL friendly

    input_columns = [col for col in columns if col not in ["cohort", "UNITID"]]
    column_types = [
        f"{col} " +
        f"{data_type_mapping[dict_df[dict_df['VARIABLE NAME'] == col]['API data type'].values[0]]}"
        for col in input_columns
    ]

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            UNITID INTEGER,
            cohort TEXT,
            {', '.join(column_types)},
            PRIMARY KEY (UNITID, cohort),
            FOREIGN KEY(UNITID, cohort) REFERENCES root(UNITID, cohort)
        );"""

    conn.execute(create_table_sql)

# Commit the changes
conn.commit()

# Fetch the names of all tables in the database to confirm creation
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
tables = [table[0] for table in tables]

conn.close()

print("Successfully created database tables.")
print(tables)

################################################################################

dfs = []

def convert_data_types(df, dict_df):
    for col in df.columns:
        if col in dict_df['VARIABLE NAME'].values:
            desired_dtype = dict_df[dict_df['VARIABLE NAME'] == col]['API data type'].values[0]
            if desired_dtype == 'integer':
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
            elif desired_dtype == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')
            # no explicit conversion required for string type
    return df

# Read and process the institutional data
for filename in os.listdir(dir):
    filepath = os.path.join(dir, filename)

    # Read CSV files with the "MERGED" prefix
    if filename.startswith("MERGED") and filename.endswith(".csv"):
        df = pd.read_csv(filepath)

        # Replace privacy suppressed notes with NULL
        df.replace('PrivacySuppressed', pd.NA)

        df['cohort'] = filename.replace("MERGED", "", 1) \
                               .replace("_PP.csv", "", 1)
        dfs.append(df)

# Concatenate all institutional data
print("Now merging the CSV files...")
merged_df = pd.concat(dfs, axis=0, ignore_index=True, sort=False)
print("Successfully merged CSV files.")

################################################################################

# Function to split and upload data to the respective tables
def upload_to_db(df, category_mappings, conn):

    # For each category in the mapping, extract the relevant columns and upload to its table
    for category, columns in category_mappings.items():
        table_name = category.replace("-", "_")

        # Check if data dictionary columns exist in the data
        valid_columns = [col for col in columns if col in df.columns]
        invalid_columns = [col for col in columns if col not in df.columns]
        print("Warning:", category, "is missing", invalid_columns)

        subset_df = df[valid_columns].drop_duplicates()
        subset_df.to_sql(table_name, conn, if_exists='append', index=False)

# Connect to database and write SQL tables
conn = sqlite3.connect('education.db')
upload_to_db(merged_df, category_mappings, conn)
conn.commit()
conn.close()
