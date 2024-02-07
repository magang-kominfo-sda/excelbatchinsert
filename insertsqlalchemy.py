import os
import pandas as pd
from sqlalchemy import create_engine, inspect

database_connection_string = 'postgresql://postgres:090503@localhost:5432/CC112NEW'

# Buat SQLAlchemy engine
engine = create_engine(database_connection_string)

direktori_excel = 'Perbulan2'

for filename in os.listdir(direktori_excel):
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        file_path = os.path.join(direktori_excel, filename)
        print(file_path)

        xl = pd.ExcelFile(file_path)
        sheet_names = xl.sheet_names
        for sheet_name in sheet_names:

            df_excel = pd.read_excel(file_path, sheet_name=sheet_name)
            print(sheet_name)

            df_excel.columns = df_excel.columns.str.replace('.', '_')
            df_excel = df_excel.map(lambda x: x.strip("'") if isinstance(x, str) else x)
            df_excel = df_excel.drop_duplicates()

            table_name = sheet_name.lower().replace(' ','')

            inspector = inspect(engine)

            if table_name in inspector.get_table_names():
                # Read existing data from the table
                existing_data = pd.read_sql_table(table_name, con=engine)

                # Check if any rows in the DataFrame already exist in the table
                duplicates = df_excel[df_excel.duplicated(subset=existing_data.columns, keep='first')]

                # Append only the non-duplicate rows to the table
                if not duplicates.empty:
                    non_duplicates = df_excel[~df_excel.duplicated(subset=existing_data.columns, keep='first')]
                    non_duplicates.to_sql(table_name, con=engine, if_exists='append', index=False)
                    print(f"{len(non_duplicates)} rows appended to {table_name}.")
                else:
                    print("No new rows to append.")
            else:
                # If the table doesn't exist, create it and append the DataFrame
                df_excel.to_sql(table_name, con=engine, if_exists='append', index=False)
                print(f"{len(df_excel)} rows appended to {table_name}.")
