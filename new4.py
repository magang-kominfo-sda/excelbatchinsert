import pandas as pd
import psycopg2
from psycopg2 import Error
import os
from datetime import datetime

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

def check_data_existence(connection, table_name, column_names, data):
    cursor = connection.cursor()
    conditions = []
    converted_data = []
    for column_name, value in zip(column_names, data):
        if pd.isnull(value):
            conditions.append(f"{column_name} IS NULL")
        elif isinstance(value, datetime):
            formatted_value = value.strftime('%Y-%m-%d %H:%M:%S')
            conditions.append(f"{column_name} = %s")
            converted_data.append(formatted_value)
        elif isinstance(value, str):
            conditions.append(f"{column_name} = %s")
            converted_data.append(value)
        else:
            conditions.append(f"{column_name} = %s")
            converted_data.append(value)

    conditions_str = " AND ".join(conditions)
    query = f"SELECT COUNT(*) FROM {table_name} WHERE {conditions_str}"
    
    # Buat tuple dari data yang sudah dikonversi
    data_tuple = tuple(converted_data)
    
    cursor.execute(query, data_tuple)
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

def insert_data(connection, table_name, column_names, data):
    cursor = connection.cursor()
    
    columns = ", ".join(column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    cursor.execute(query, data)
    
    connection.commit()
    
    cursor.close()

def get_table_schema(connection, table_name):
    cursor = connection.cursor()
    # Query untuk mendapatkan schema tabel
    query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
    cursor.execute(query)
    # Ambil hasil query
    schema = cursor.fetchall()
    # Tutup kursor
    cursor.close()
    return schema

def adjust_dataframe_data_types(df, table_schema):
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
            # Membersihkan data string di setiap kolom
            df_excel = df_excel.map(lambda x: x.strip("'") if isinstance(x, str) else x)

            # Lakukan koneksi ke database
            connection = create_connection()

            # Tentukan nama tabel di database
            table_name = sheet_name.lower().replace(' ','')

            table_schema = get_table_schema(connection, table_name)

            df_excel_adjusted = adjust_dataframe_data_types(df_excel, table_schema)

            column_types = df_excel_adjusted.dtypes
            print(column_types)

            df_columns = df_excel_adjusted.columns.tolist()

            columns = []
            for col in df_columns:
                if ' ' in col:
                    col = col.replace(" ", "_")
                columns.append(col.lower())

            # Iterasi melalui setiap baris data dalam DataFrame
            for index, row in df_excel_adjusted.iterrows():
                # Lakukan pengecekan apakah data sudah ada di database
                if not check_data_existence(connection, table_name, columns, row.tolist()):
                    insert_data(connection, table_name, columns, row.tolist())

            # Tutup koneksi ke database
            connection.close()
