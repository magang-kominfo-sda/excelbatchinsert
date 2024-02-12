import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

class ExcelToPostgreSQL:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.tablefilter = ""
        self.columnsfilter = []

    def connect_to_database(self):
        try:
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return conn
        except (Exception, psycopg2.Error) as error:
            print("Error saat menghubungi database:", error)
            return None
        
    def filter_insert(self, table_filter, columns_filter):
        self.tablefilter = table_filter
        self.columnsfilter = columns_filter
    
    def adjust_df(self, df, column_mapping):
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
    
    def get_df_from_db(self,cursor,table_name,df):
        select_sql = f"SELECT * FROM {table_name};"
        cursor.execute(select_sql)
        rows = cursor.fetchall()
        df_existing = pd.DataFrame(rows, columns=df.columns)
        return df_existing

    def check_table_exist(self, cursor, table_name, df):
        try:
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
            exists = cursor.fetchone()[0]
            if not exists:
                self._create_table(cursor, table_name, df)
                return "Tabel berhasil dibuat."
            else:
                return "Tabel sudah ada."
        except (Exception, psycopg2.Error) as error:
            print("Error saat membuat tabel:", error)
    
    def get_columns_mapping(self,cursor,table_name):
        cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
        columns = cursor.fetchall()
        column_mapping = {column[0]: column[1] for column in columns}
        return column_mapping

    def _create_table(self, cursor, table_name, df):
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.lower()
        tipe_data_tabel = {}

        for kolom in df.columns:
            tipe = df[kolom].dtype
            data_cek = df[kolom].dropna()
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
                    elif df[kolom].astype(str).apply(lambda x: x.strip() == "0" or x.strip == "").any() or nilai_pertama == 0:
                        tipe_data_tabel[kolom] = "DOUBLE PRECISION"
                    else:
                        tipe_data_tabel[kolom] = "VARCHAR"
                elif tipe == 'object':
                    if df[kolom].astype(str).apply(lambda x: x.strip() == "").all():
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
        query = query.rstrip(", ") + ");"
        cursor.execute(query)

    def concate_df(self, df_a, df_b, table_name):
        df_columns = df_a.columns.tolist()

        kolom_tidak_null = []
                    
        for col in df_columns:
            if df_a[col].apply(lambda x: pd.isnull(x) or x == 'nan').sum() == 0 and col != "NO":
                kolom_tidak_null.append(col)

        if table_name == self.tablefilter:
            kolom_cek = []
            for teks in self.columnsfilter:
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
        
    def insert_to_db(self,cursor,df, table_name,data_final,text_exist):
        df_columns = df.columns.tolist()
        columns = []
        for col in df_columns:
            if ' ' in col:
                col = col.replace(" ", "_")
            columns.append(col.lower())

        columns2 = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_data_sql = f'INSERT INTO {table_name} ({columns2}) VALUES ({placeholders});'

        total_now_data = len(df)
        total_now_insert_data = len(data_final)

        try:
            # Menjalankan SQL untuk insert data baru dalam batch
            execute_batch(cursor, insert_data_sql, data_final, page_size=1000)
            print(text_exist + str(total_now_insert_data) + " Data berhasil diinsert. Total data di excel : " + str(total_now_data))
                        
        except (Exception, psycopg2.Error) as error:
            print("Error saat menginsert data:", error)

    def insert_data(self, cursor, table_name, df):
        try:
            text_exist = self.check_table_exist(cursor, table_name, df)

            column_mapping = self.get_columns_mapping(cursor, table_name)

            df_a = self.adjust_df(df, column_mapping)

            df_existing = self.get_df_from_db(cursor, table_name, df)

            df_b = self.adjust_df(df_existing, column_mapping)

            df_final = self.concate_df(df_a,df_b,table_name)

            data_final = []
            for row in df_final.itertuples(index=False, name=None):
                data_row = tuple(None if pd.isnull(value) or pd.isna(value) else value for value in row)
                data_final.append(data_row)
            self.insert_to_db(cursor,df_a,table_name,data_final,text_exist)

        except (Exception, psycopg2.Error) as error:
            print("Error saat menyisipkan data:", error)

    def process_excel_files(self, direktori_excel):
        conn = self.connect_to_database()
        if conn:
            try:
                with conn.cursor() as cursor:
                    for filename in os.listdir(direktori_excel):
                        if filename.endswith('.xlsx') or filename.endswith('.xls'):
                            file_path = os.path.join(direktori_excel, filename)
                            xl = pd.ExcelFile(file_path)
                            sheet_names = xl.sheet_names
                            for sheet_name in sheet_names:
                                df_excel = pd.read_excel(file_path, sheet_name=sheet_name)
                                table_name = sheet_name.lower().replace(' ','')
                                df_excel = df_excel.map(lambda x: x.strip("'") if isinstance(x, str) else x)
                                df_excel.columns = df_excel.columns.str.replace('.', '_')
                                self.insert_data(cursor, table_name, df_excel)
                conn.commit()
            except (Exception, psycopg2.Error) as error:
                print("Error:", error)
            finally:
                conn.close()

# Penggunaan
if __name__ == "__main__":
    excel_to_postgresql = ExcelToPostgreSQL(
        dbname='CC112',
        user='postgres',
        password='090503',
        host='localhost',
        port=5432
    )
    table_filter = "tiketdinas" 
    columns_filter = ['no_laporan', 'no_tiket_dinas', 'dinas', 'status', 'tiket dibuat']
    excel_to_postgresql.filter_insert(table_filter, columns_filter)
    excel_to_postgresql.process_excel_files('Perbulan2')
