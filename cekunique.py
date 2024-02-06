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

                    # Mengubah tipe data kolom di Excel sesuai dengan struktur tabel
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
                            # Tambahkan kondisi tipe data lainnya sesuai kebutuhan
                    
                    # print(df_excel.dtypes)

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

                    # print(df_existing.dtypes)            
                    duplicated_rows = df_excel[df_excel.duplicated(keep=False)]
                    if not duplicated_rows.empty:
                        print("Baris yang sama di semua kolom:")
                        print(duplicated_rows)
                    else:
                        print("Tidak ada baris yang sama di semua kolom.")

                    


# print("total semua data di excel : "+str(total_data_all_excel))
# print("total semua data di insert : "+str(total_data_all_insert))


