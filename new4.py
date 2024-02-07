import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import Error

kolom_yang_difilter = "tiketdinas"
isi_filter_manual = ['no_laporan', 'no_tiket_dinas', 'dinas', 'status', 'tiket dibuat']
direktori_excel = 'Perbulan2'

def create_connection():
    try:
        connection = psycopg2.connect(
            dbname='CC112',
            user='postgres',
            password='090503',
            host='localhost',
            port=5432
        )
        return connection
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

def ambil_data_db(connection, table_name, df):
    cursor = connection.cursor()
    select_sql = f"SELECT * FROM {table_name};"
    cursor.execute(select_sql)
    rows = cursor.fetchall()
    df_existing = pd.DataFrame(rows, columns=df.columns)
    return df_existing


def insert_data(connection, table_name, df_columns, data):
    cursor = connection.cursor()
    
    columns = []
    for col in df_columns:
        if ' ' in col:
            col = col.replace(" ", "_")
        columns.append(col.lower())

    columns2 = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(df_excel.columns))
    insert_data_sql = f'INSERT INTO {table_name} ({columns2}) VALUES ({placeholders});'

    try:
        execute_batch(cursor, insert_data_sql, data, page_size=1000)
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error saat menginsert data:", error)
    
    cursor.close()

def get_table_schema(connection, table_name):
    cursor = connection.cursor()
    query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
    cursor.execute(query)
    schema = cursor.fetchall()
    cursor.close()
    return schema

def process_data(df_a,df_b,columns,filter_manual,tabel_now,tabel_cek_manual):

    kolom_tidak_null = []
                    
    for col in columns:
        if df_excel[col].apply(lambda x: pd.isnull(x) or x == 'nan').sum() == 0 and col != "NO":
            kolom_tidak_null.append(col)
    
    if tabel_now == tabel_cek_manual:
        kolom_cek = []
        for teks in filter_manual:
            idx = teks.find("_", teks.find("_") + 1)
            if idx != -1:
                text = teks[:idx] + " " + teks[idx+1:]
                kolom_cek.append(text.upper())
            else:
                kolom_cek.append(teks.upper())

        df_diff = pd.concat([df_b,df_a]).drop_duplicates(subset=kolom_cek, keep=False)
    else:
        df_diff = pd.concat([df_b,df_a]).drop_duplicates(subset=kolom_tidak_null, keep=False)
    data_final = []
    for row in df_diff.itertuples(index=False, name=None):
        data_row = tuple(None if pd.isnull(value) else value for value in row)
        data_final.append(data_row)
    
    return data_final

def preprocessdf(df, table_schema):
    df = df.drop_duplicates()
    for column_name, data_type in table_schema:
        upper_column_name = column_name.upper().replace('_', ' ')
        if column_name in df.columns:
            # Konversi tipe data DataFrame sesuai dengan tipe data schema tabel
            if data_type == 'timestamp without time zone':
                df[upper_column_name] = pd.to_datetime(df[upper_column_name], errors='coerce')
                df[upper_column_name] = df[upper_column_name].apply(lambda x: x if pd.notna(x) else None)
            elif data_type == 'bigint':
                df[upper_column_name] = pd.to_numeric(df[upper_column_name], errors='coerce')
                df[upper_column_name] = df[upper_column_name].apply(lambda x: float(x) if pd.notna(x) else None)
                df[upper_column_name] = df[upper_column_name].astype(float)
            else:
                df[upper_column_name] = df[upper_column_name].astype(str).fillna('-')
    return df


connection = create_connection()

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
            
            df_db = ambil_data_db(connection,table_name,df_excel)

            schema = get_table_schema(connection,table_name)

            df_excel = preprocessdf(df_excel,schema)
            df_db = preprocessdf(df_db,schema)

            df_excel = df_excel.astype(df_db.dtypes)

            df_columns = df_excel.columns.tolist()
            print(df_columns)

            data_final = process_data(df_excel,df_db,df_columns,isi_filter_manual,table_name,kolom_yang_difilter)

            total_now_data = len(df_excel)
            total_now_insert_data = len(data_final)

            insert_data(connection,table_name,df_columns,data_final)
            print(str(total_now_insert_data) + " Data berhasil diinsert. Total data di excel : " + str(total_now_data))

            # Tutup koneksi ke database
            
connection.close()