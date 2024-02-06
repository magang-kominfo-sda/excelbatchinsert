import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database_structure import Lampiran, Laporan, LogDinas, LogL3, TiketDinas

database_connection_string = 'postgresql://postgres:090503@localhost:5432/CC112NEW'
engine = create_engine(database_connection_string)

Session = sessionmaker(bind=engine)
session = Session()

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

            if engine.dialect.has_table(engine, table_name):
                for _, row in df_excel.iterrows():
                    if table_name == 'lampiran':
                        data_entry = Lampiran(**row.to_dict())
                    elif table_name == 'laporan':
                        data_entry = Laporan(**row.to_dict())
                    elif table_name == 'logdinas':
                        data_entry = LogDinas(**row.to_dict())
                    elif table_name == 'logl3':
                        data_entry = LogL3(**row.to_dict())
                    elif table_name == 'tiketdinas':
                        data_entry = TiketDinas(**row.to_dict())

                    session.add(data_entry)

                session.commit()
                print(f"{len(df_excel)} rows added to {table_name}.")
            else:
                df_excel.to_sql(table_name, con=engine, if_exists='append', index=False)
                print(f"{len(df_excel)} rows appended to {table_name}.")

session.close()
