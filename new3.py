import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

# total_data_all_excel = 0
# total_data_all_insert = 0

# Koneksi ke database PostgreSQL
with psycopg2.connect(
    dbname='CC112NEW',
    user='postgres',
    password='090503',
    host='localhost',
    port=5432
) as conn:
    with conn.cursor() as cursor:
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

                    table_name = sheet_name.lower().replace(' ','')

                    cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
                    columns = cursor.fetchall()
                    column_mapping = {column[0]: column[1] for column in columns}

                    df_columns = df_excel.columns.tolist()

                    for column_name, data_type in column_mapping.items():
                        upper_column_name = column_name.upper().replace('_', ' ')
                        if upper_column_name in df_excel.columns:
                            if data_type == 'timestamp without time zone':
                                df_excel[upper_column_name] = pd.to_datetime(df_excel[upper_column_name], errors='coerce')
                                df_excel[upper_column_name] = df_excel[upper_column_name].apply(lambda x: x if pd.notna(x) else None)
                            elif data_type == 'bigint':
                                df_excel[upper_column_name] = pd.to_numeric(df_excel[upper_column_name], errors='coerce')
                                df_excel[upper_column_name] = df_excel[upper_column_name].apply(lambda x: float(x) if pd.notna(x) else None)
                                df_excel[upper_column_name] = df_excel[upper_column_name].astype(float)
                            else:
                                df_excel[upper_column_name] = df_excel[upper_column_name].astype(str).fillna('-')

                    select_sql = f"SELECT * FROM {table_name};"
                    cursor.execute(select_sql)
                    rows = cursor.fetchall()
                    df_existing = pd.DataFrame(rows, columns=df_excel.columns)
                    for column_name, data_type in column_mapping.items():
                        upper_column_name = column_name.upper().replace('_', ' ')
                        if upper_column_name in df_excel.columns:
                            if data_type == 'timestamp without time zone':
                                df_existing[upper_column_name] = pd.to_datetime(df_existing[upper_column_name], errors='coerce')
                                df_existing[upper_column_name] = df_existing[upper_column_name].apply(lambda x: x if pd.notna(x) else None)
                            elif data_type == 'bigint':
                                df_existing[upper_column_name] = pd.to_numeric(df_existing[upper_column_name], errors='coerce')
                                df_existing[upper_column_name] = df_existing[upper_column_name].apply(lambda x: float(x) if pd.notna(x) else None)
                                df_existing[upper_column_name] = df_existing[upper_column_name].astype(float)
                            else:
                                df_existing[upper_column_name] = df_existing[upper_column_name].astype(str).fillna('-')
                    
                    kesamaan = df_excel.equals(df_existing)

                    data_baru_tidak_ada_di_lama = df_excel[~df_excel.isin(df_existing.to_dict(orient='list')).all(axis=1)]

                    data_new = []
                    for row in data_baru_tidak_ada_di_lama.itertuples(index=False, name=None):
                        data_row = tuple(None if pd.isnull(value) else value for value in row)
                        data_new.append(data_row)

                    data_excel = []
                    for row in df_excel.itertuples(index=False, name=None):
                        data_row = tuple(None if pd.isnull(value) else value for value in row)
                        data_excel.append(data_row)

                    data_urut = []
                    for row in data_excel:
                        if row in data_new:
                            data_urut.append(row)            

                    columns = []
                    for col in df_columns:
                        if ' ' in col:
                            col = col.replace(" ", "_")
                        columns.append(col.lower())

                    columns2 = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(df_excel.columns))
                    insert_data_sql = f'INSERT INTO {table_name} ({columns2}) VALUES ({placeholders});'

                    total_now_data = len(df_excel)
                    total_now_insert_data = len(data_new)

                    try:
                        # Menjalankan SQL untuk insert data baru dalam batch
                        execute_batch(cursor, insert_data_sql, data_urut, page_size=1000)
                        # Commitperubahan
                        conn.commit()
                        print(str(total_now_insert_data) + " Data berhasil diinsert. Total data di excel : " + str(total_now_data))
                        
                    except (Exception, psycopg2.Error) as error:
                        print("Error saat menginsert data:", error)
                    


# print("total semua data di excel : "+str(total_data_all_excel))
# print("total semua data di insert : "+str(total_data_all_insert))


                    # data_new = []
                    # for row in df_new_data.itertuples(index=False, name=None):
                    #     data_row = tuple(None if pd.isnull(value) else value for value in row)
                    #     data_new.append(data_row)

                    # data_old = []
                    # for row in df_existing.itertuples(index=False, name=None):
                    #     data_row = tuple(None if pd.isnull(value) else value for value in row)
                    #     data_old.append(data_row)

                    # new_data = []
                    # for row in data_new:
                    #     if row not in data_old:
                    #         new_data.append(row)
                    
                    # new_data2 = set()
                    # for row in data_excel:
                    #     if row not in data_old:
                    #         new_data2.add(row)

                    # print(new_data)
                    # print(new_data2)

                    # data_urut = []
                    # for row in data_excel:
                    #     if row in new_data:
                    #         data_urut.append(row)  