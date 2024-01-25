import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

total_data_all_excel = 0
total_data_all_insert = 0

# Koneksi ke database PostgreSQL
with psycopg2.connect(
    dbname='CC112NEW',
    user='postgres',
    password='090503',
    host='localhost',
    port=5432
) as conn:
    with conn.cursor() as cursor:
        direktori_excel = 'Perbulan'

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
                    if df_excel.iloc[:, 1].isnull().any():
                        df_excel.iloc[:, 1] = df_excel.iloc[:, 1].fillna('-')

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
                            else:
                                df_excel[upper_column_name] = df_excel[upper_column_name].astype(str)
                            # Tambahkan kondisi tipe data lainnya sesuai kebutuhan

                    # Menyiapkan data untuk dimasukkan ke dalam tabel
                    data = []
                    for row in df_excel.itertuples(index=False, name=None):
                        data_row = tuple(None if pd.isnull(value) else value for value in row)
                        data.append(data_row)

                    column_types = df_excel.dtypes
                    string_columns = df_excel.select_dtypes(include=['object']).columns
                    if 'NO' in df_excel.columns:
                        string_columns = (pd.Index(['NO'])).append(string_columns)
                    df_copy = df_excel[string_columns]

                    kolom_tidak_null = []

                    for i in range(len(string_columns)):
                        if df_copy[string_columns[i]].notnull().all() and string_columns[i] != "NO":
                            kolom_tidak_null.append(string_columns[i])

                    kolom_periksa = kolom_tidak_null[0]

                    existing_data = set()
                    select_sql = f"SELECT * FROM {table_name};"
                    cursor.execute(select_sql)
                    rows = cursor.fetchall()
                    df_existing = pd.DataFrame(rows, columns=df_excel.columns)
                    df_existing = df_existing[string_columns]
                    for column_name, data_type in column_mapping.items():
                        upper_column_name = column_name.upper()
                        if upper_column_name in df_existing.columns:
                            if data_type == 'bigint':
                                df_existing[upper_column_name] = pd.to_numeric(df_existing[upper_column_name], errors='coerce')
                                df_existing[upper_column_name] = df_existing[upper_column_name].apply(lambda x: float(x) if pd.notna(x) else None)
                            else:
                                df_existing[upper_column_name] = df_existing[upper_column_name].astype(str)

                    for row in df_existing.itertuples(index=False, name=None):
                        data_row = tuple(None if pd.isnull(value) else str(value) for value in row)
                        existing_data.add(data_row)

                    data2 = []
                    for row in df_copy.itertuples(index=False, name=None):
                        data_row = tuple(None if pd.isnull(value) else str(value) for value in row)
                        data2.append(data_row)

                    new_data = set()
                    for row in data2:
                        if row not in existing_data:
                            new_data.add(row)

                    df_new = pd.DataFrame(new_data, columns=df_copy.columns)
                    df_merged = df_excel.merge(df_new, on=kolom_periksa, how='left', indicator=True)
                    df_hasil = df_merged[df_merged['_merge'] == 'both'].drop('_merge', axis=1)
                    kolom_lama = [
                        kolom + "_x" if kolom != kolom_periksa and kolom in string_columns else kolom
                        for kolom in df_excel.columns
                    ]
                    df_hasil = df_hasil[kolom_lama]

                    data3 = set()
                    for row in df_hasil.itertuples(index=False, name=None):
                        data_row = tuple(None if pd.isnull(value) else value for value in row)
                        data3.add(data_row)

                    data4 = []
                    for row in data:
                        if row in data3:
                            data4.append(row)

                    columns = []
                    for col in df_columns:
                        if ' ' in col:
                            col = col.replace(" ", "_")
                        columns.append(col.lower())

                    columns2 = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(df_excel.columns))
                    insert_data_sql = f'INSERT INTO {table_name} ({columns2}) VALUES ({placeholders});'

                    total_now_data = len(df_excel)
                    total_now_insert_data = len(data4)
                    total_data_all_excel += total_now_data
                    total_data_all_insert += total_now_insert_data

                    try:
                        # Menjalankan SQL untuk insert data baru dalam batch
                        execute_batch(cursor, insert_data_sql, data4, page_size=1000)
                        # Commitperubahan
                        conn.commit()
                        print(str(total_now_insert_data) + " Data berhasil diinsert. Total data di excel : " + str(total_now_data))
                        
                    except (Exception, psycopg2.Error) as error:
                        print("Error saat menginsert data:", error)


print("total semua data di excel : "+str(total_data_all_excel))
print("total semua data di insert : "+str(total_data_all_insert))


