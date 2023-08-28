import os
import sqlite3
import pandas as pd

dir = "./scorecard"

# Read the FieldOfStudy data dictionary
dict_path = os.path.join(dir, "CollegeScorecardDataDictionary.xlsx")
dict_df = pd.read_excel(dict_path, sheet_name="FieldOfStudy_Data_Dictionary")

# Extract the required columns and drop missing rows
dict_df = dict_df[["VARIABLE NAME", "API data type"]]
dict_df = dict_df.dropna(subset=["VARIABLE NAME"])

# Create a function to convert column data types based on the data dictionary
def convert_data_types(df, dict_df):
    for col in df.columns:
        if col in dict_df['VARIABLE NAME'].values:
            desired_dtype = dict_df[dict_df['VARIABLE NAME'] == col]['API data type'].values[0]
            if desired_dtype == 'integer':
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
            elif desired_dtype == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')
    return df

dfs = []

# Read and process the FieldOfStudy data
for filename in os.listdir(dir):
    filepath = os.path.join(dir, filename)

    # Read CSV files with the "FieldOfStudyData" prefix
    if filename.startswith("FieldOfStudyData") and filename.endswith(".csv"):
        df = pd.read_csv(filepath)

        # Replace privacy suppressed notes with NULL
        df.replace('PrivacySuppressed', pd.NA, inplace=True)

        # Convert data types
        df = convert_data_types(df, dict_df)

        dfs.append(df)

# Concatenate all FieldOfStudy data
print("Now merging the CSV files...")
merged_df = pd.concat(dfs, axis=0, ignore_index=True, sort=False)
print("Successfully merged CSV files.")

# Create the "fieldofstudy" SQL table
conn = sqlite3.connect('education.db')
conn.execute("DROP TABLE IF EXISTS fieldofstudy;")

data_type_mapping = {
    "integer": "INTEGER",
    "float": "REAL",
    "string": "TEXT",
    "autocomplete": "TEXT"
}

columns_with_types = [f"{col} {data_type_mapping[dict_df[dict_df['VARIABLE NAME'] == col]['API data type'].values[0]]}" for col in merged_df.columns]

create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS fieldofstudy (
        {', '.join(columns_with_types)}
    );"""

conn.execute(create_table_sql)
conn.commit()

# Upload data to the "fieldofstudy" table
merged_df.to_sql('fieldofstudy', conn, if_exists='append', index=False)
conn.close()
