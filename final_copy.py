import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

tabel_filter_manual = "tiketdinas" # ubah menjadi "" jika tidak ingin menggunakan filter manual atau isi dengan None
kolom_filter_manual = ['no_laporan', 'no_tiket_dinas', 'dinas', 'status', 'tiket dibuat']

with psycopg2.connect(
    dbname='CC112',
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
                    
                    text_exist = ""

                    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
                    ada = cursor.fetchone()[0]
                    if ada:
                        text_exist = "Table exist, "
                    else :
                        df_cek = pd.read_excel(file_path,sheet_name=sheet_name)

                        df_cek.columns = df_cek.columns.str.replace('.', '_')
                        df_cek = df_cek.map(lambda x: x.strip("'") if isinstance(x, str) else x)
                        df_cek.columns = df_cek.columns.str.replace(' ', '_')
                        df_cek.columns = df_cek.columns.str.lower()

                        tipe_data_tabel = {}

                        for kolom in df_cek.columns:
                            tipe = df_cek[kolom].dtype
                            data_cek = df_cek[kolom].dropna()
                            nilai_pertama = "-"
                            if not data_cek.empty:
                                nilai_pertama = data_cek.iloc[0]
                            if "uid" in kolom.lower():
                                tipe_data_tabel[kolom] = "UUID"
                            else:
                                if tipe == 'int64':
                                    if kolom == "no":
                                        tipe_data_tabel[kolom] = "BIGINT"
                                    else:
                                        tipe_data_tabel[kolom] = "DOUBLE PRECISION"
                                elif tipe == 'float64':
                                    if kolom == "no":
                                        tipe_data_tabel[kolom] = "BIGINT"
                                    elif df_cek[kolom].astype(str).apply(lambda x: x.strip() == "0" or x.strip == "").any() or nilai_pertama == 0:
                                        tipe_data_tabel[kolom] = "DOUBLE PRECISION"
                                    else:
                                        tipe_data_tabel[kolom] = "VARCHAR"
                                elif tipe == 'object':
                                    if df_cek[kolom].astype(str).apply(lambda x: x.strip() == "").all():
                                        tipe_data_tabel[kolom] = "VARCHAR"
                                    else:
                                        try:
                                            if not pd.isna(nilai_pertama):
                                                pd.to_datetime(nilai_pertama)
                                                tipe_data_tabel[kolom] = "TIMESTAMP"
                                            else:
                                                tipe_data_tabel[kolom] = "VARCHAR"
                                        except ValueError:
                                            tipe_data_tabel[kolom] = "VARCHAR"
                                else:
                                    tipe_data_tabel[kolom] = "VARCHAR"
                        query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                        for kolom, tipe_data in tipe_data_tabel.items():
                            query += f"{kolom} {tipe_data}, "
                        query = query.rstrip(", ") + ");"  # Menghapus koma ekstra dan menambahkan tanda tutup kurung
                        cursor.execute(query)
                        text_exist = "Table doesn't exist, Succes created tabel. "

                    cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
                    columns = cursor.fetchall()
                    column_mapping = {column[0]: column[1] for column in columns}

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

                    df_excel = df_excel.drop_duplicates()
                    df_existing = df_existing.drop_duplicates()

                    df_columns = df_excel.columns.tolist()

                    kolom_tidak_null = []
                    
                    for col in df_columns:
                        if df_excel[col].apply(lambda x: pd.isnull(x) or x == 'nan').sum() == 0 and col != "NO":
                            kolom_tidak_null.append(col)

                    if table_name == tabel_filter_manual:
                        kolom_cek = []
                        for teks in kolom_filter_manual:
                            idx = teks.find("_", teks.find("_") + 1)
                            if idx != -1:
                                text = teks[:idx] + " " + teks[idx+1:]
                                kolom_cek.append(text.upper())
                            else:
                                kolom_cek.append(teks.upper())

                        df_diff = pd.concat([df_existing,df_excel]).drop_duplicates(subset=kolom_cek, keep=False)
                    else:
                        df_diff = pd.concat([df_existing,df_excel]).drop_duplicates(subset=kolom_tidak_null, keep=False)

                    data_final = []
                    for row in df_diff.itertuples(index=False, name=None):
                        data_row = tuple(None if pd.isnull(value) or pd.isna(value) else value for value in row)
                        data_final.append(data_row)
                    
                    columns = []
                    for col in df_columns:
                        if ' ' in col:
                            col = col.replace(" ", "_")
                        columns.append(col.lower())

                    columns2 = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(df_excel.columns))
                    insert_data_sql = f'INSERT INTO {table_name} ({columns2}) VALUES ({placeholders});'

                    total_now_data = len(df_excel)
                    total_now_insert_data = len(data_final)

                    try:
                        # Menjalankan SQL untuk insert data baru dalam batch
                        execute_batch(cursor, insert_data_sql, data_final, page_size=1000)
                        # Commitperubahan
                        conn.commit()
                        print(text_exist + str(total_now_insert_data) + " Data berhasil diinsert. Total data di excel : " + str(total_now_data))
                        
                    except (Exception, psycopg2.Error) as error:
                        print("Error saat menginsert data:", error)

