import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

class ExcelToTable:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_df_from_db(self, table_name, df):
        select_sql = f"SELECT * FROM {table_name};"
        self.cursor.execute(select_sql)
        rows = self.cursor.fetchall()
        df_existing = pd.DataFrame(rows, columns=df.columns)
        return df_existing

    def create_table(self, table_name, file_path,sheet_name):
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
        self.cursor.execute(query)

    def check_table_existence(self, table_name, file_path, sheet_name):
        self.cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
        ada = self.cursor.fetchone()[0]
        if ada:
            return "Table " + str(table_name) + " is exist"
        else :
            try:
                self.create_table(table_name, file_path, sheet_name)
                return "Table " + str(table_name) + " doesn't exist. \nSucces created Table"
            except:
                return "Error creating table " + str(table_name)

    def insert_data(self, table_name, df_final):
        df_columns = df_final.columns.tolist()
        columns = []
        for col in df_columns:
            if ' ' in col:
                col = col.replace(" ", "_")
            columns.append(col.lower())
        columns2 = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(df_final.columns))
        insert_data_sql = f'INSERT INTO {table_name} ({columns2}) VALUES ({placeholders});'

        data_final = []
        for row in df_final.itertuples(index=False, name=None):
            data_row = tuple(None if pd.isnull(value) or pd.isna(value) else value for value in row)
            data_final.append(data_row)
        try:
            execute_batch(self.cursor, insert_data_sql, data_final, page_size=1000)
            self.conn.commit()
            return str(len(data_final)) + ' rows inserted. '
        except:
            return 'Error inserting'
    
    def adjust_df_type(self, df, column_mapping):
        for column_name, data_type in column_mapping.items():
            upper_column_name = column_name.upper().replace('_', ' ')
            if upper_column_name in df.columns:
                if data_type == 'timestamp without time zone':
                    df[upper_column_name] = pd.to_datetime(df[upper_column_name], errors='coerce')
                    df[upper_column_name] = df[upper_column_name].apply(lambda x: x if pd.notna(x) else None)
                elif data_type == 'bigint':
                    df[upper_column_name] = pd.to_numeric(df[upper_column_name], errors='coerce')
                    df[upper_column_name] = df[upper_column_name].apply(lambda x: float(x) if pd.notna(x) else None)
                    df[upper_column_name] = df[upper_column_name].astype(float)
                else:
                    df[upper_column_name] = df[upper_column_name].astype(str).fillna('-')
        df = df.drop_duplicates()
        return df

    def concat_df(self,df_a,df_b,table_filter,filter_word,table_name):
        df_columns = df_a.columns.tolist()
        kolom_tidak_null = []
                    
        for col in df_columns:
            if df_a[col].apply(lambda x: pd.isnull(x) or x == 'nan').sum() == 0 and col != "NO":
                kolom_tidak_null.append(col)
        if table_name == table_filter:
            kolom_cek = []
            for teks in filter_word:
                idx = teks.find("_", teks.find("_") + 1)
                if idx != -1:
                    text = teks[:idx] + " " + teks[idx+1:]
                    kolom_cek.append(text.upper())
                else:
                    kolom_cek.append(teks.upper())
            df_diff = pd.concat([df_b,df_a]).drop_duplicates(subset=kolom_cek, keep=False)
            return df_diff
        else:
            df_diff = pd.concat([df_b,df_a]).drop_duplicates(subset=kolom_tidak_null, keep=False)
            return df_diff


    def process_excel(self, direktori_excel,table_filter,text_filter):
        for filename in os.listdir(direktori_excel):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(direktori_excel, filename)
                print(file_path)

                xl = pd.ExcelFile(file_path)
                sheet_names = xl.sheet_names
                for sheet_name in sheet_names:
                    df_excel = pd.read_excel(file_path, sheet_name=sheet_name)
                    df_excel.columns = df_excel.columns.str.replace('.', '_')
                    df_excel = df_excel.map(lambda x: x.strip("'") if isinstance(x, str) else x)

                    table_name = sheet_name.lower().replace(' ', '')

                    text_exist = self.check_table_existence(table_name, file_path, sheet_name)

                    self.cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
                    columns = self.cursor.fetchall()
                    column_mapping = {column[0]: column[1] for column in columns}

                    df_excel = self.adjust_df_type(df_excel, column_mapping)

                    df_exist = self.get_df_from_db(table_name, df_excel)

                    df_exist = self.adjust_df_type(df_exist, column_mapping)

                    df_final = self.concat_df(df_excel, df_exist,table_filter,text_filter,table_name)

                    text_insert = self.insert_data(table_name, df_final)

                    print(str(text_exist) + '. ' + str(text_insert))

excel_to_table = ExcelToTable(dbname='CC112', 
                                user='postgres', 
                                password='090503', 
                                host='localhost', 
                                port='5432')

try:
    # Membuka koneksi ke database
    excel_to_table.connect()

    table_filter = "tiketdinas"
    text_filter = ['no_laporan', 'no_tiket_dinas', 'dinas', 'status', 'tiket dibuat']

    # Memproses file Excel dalam direktori yang ditentukan
    excel_to_table.process_excel('perbulan2',table_filter,text_filter)
    excel_to_table.disconnect()
except:
    print("gagal")
    # Memutus koneksi dari database
