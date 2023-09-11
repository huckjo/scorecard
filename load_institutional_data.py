import os
import sqlite3
import pandas as pd

DATA_DIR = "./scorecard"
DB_PATH = 'scorecard.db'

def read_data_dictionary():
    """Reads the data dictionary and extracts relevant information."""
    dict_path = os.path.join(DATA_DIR, "CollegeScorecardDataDictionary.xlsx")
    dict_df = pd.read_excel(dict_path, sheet_name="Institution_Data_Dictionary")
    dict_df = dict_df[["VARIABLE NAME", "dev-category", "API data type", "developer-friendly name"]]
    dict_df = dict_df.dropna(subset=["VARIABLE NAME"])
    return dict_df

def map_variables_to_categories(dict_df):
    """Maps variables to their respective categories."""
    categories = dict_df.groupby('dev-category')
    category_mappings = {}

    for category, group in categories:
        columns = ["cohort", "UNITID"] + group['VARIABLE NAME'].tolist()
        if category != "root":
            category_mappings[category] = columns

    return category_mappings

def create_database_tables(category_mappings, dict_df):
    """Creates the database tables based on the category mappings."""
    data_type_mapping = {
        "autocomplete": "TEXT",
        "integer": "INTEGER",
        "float": "REAL",
        "long": "INTEGER",
        "string": "TEXT"
    }

    with sqlite3.connect(DB_PATH) as conn:
        # Drop previously created tables
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        for table_name, in tables:
            conn.execute(f"DROP TABLE IF EXISTS {table_name};")

        # Create tables for each category
        for category, columns in category_mappings.items():
            table_name = category.replace("-", "_")
            column_types = [
                f"{col} {data_type_mapping[dict_df[dict_df['VARIABLE NAME'] == col]['API data type'].values[0]]}"
                for col in columns if col not in ["cohort", "UNITID"]
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

        return [table_name[0] for table_name in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]

def read_institutional_data():
    """Reads and processes the institutional data."""
    dfs = []

    for filename in os.listdir(DATA_DIR):
        if filename.startswith("MERGED") and filename.endswith(".csv"):
            filepath = os.path.join(DATA_DIR, filename)
            df = pd.read_csv(filepath)
            df.replace('PrivacySuppressed', pd.NA, inplace=True)
            df['cohort'] = filename.replace("MERGED", "").replace("_PP.csv", "")
            dfs.append(df)

    print("Now merging the CSV files...")
    return pd.concat(dfs, axis=0, ignore_index=True, sort=False)

def upload_to_db(df, category_mappings):
    """Uploads the dataframe to the database based on the category mappings."""

    with sqlite3.connect(DB_PATH) as conn:
        for category, columns in category_mappings.items():
            print("Now loading data to the", category, "table...")
            valid_columns = [col for col in columns if col in df.columns]
            subset_df = df[valid_columns].drop_duplicates()
            subset_df.to_sql(category, conn, if_exists='append', index=False)

def main():
    dict_df = read_data_dictionary()
    print(dict_df.head())

    category_mappings = map_variables_to_categories(dict_df)
    print("Successfully mapped variables to categories.")

    created_tables = create_database_tables(category_mappings, dict_df)
    print("Successfully created database tables.")
    print(created_tables)

    merged_df = read_institutional_data()
    print("Successfully merged CSV files.")

    upload_to_db(merged_df, category_mappings)

if __name__ == "__main__":
    main()
