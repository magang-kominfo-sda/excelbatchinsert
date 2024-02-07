#!/usr/bin/env python
# coding: utf-8

# In[727]:


import os
import pandas as pd
import psycopg2
pd.__version__


# ### Konfigurasi

# In[728]:


# Connect to PostgreSQL
conn = psycopg2.connect(host='localhost', port='5432', database='cc112', user='postgres', password='S1d04rj0')
cursor = conn.cursor()


# ### Function

# In[729]:


def list_tabel():
    sql = """SELECT relname FROM pg_class WHERE relkind='r'
                  AND relname !~ '^(pg_|sql_)';"""
    cursor.execute(sql) # "rel" is short for relation.

    tables = [i[0] for i in cursor.fetchall()] # A list() of tables.
    return tables


# In[730]:


def list_kolom(nama_tabel):
    sql = '''SELECT * FROM %s''' %nama_tabel

    cursor.execute(sql) 
    column_names = [desc[0] for desc in cursor.description]
    return(column_names)


# In[731]:


# List Nama Tabel
print(list_tabel())


# In[732]:


# list Kolom masing2 tabel
for nmkolom in list_tabel():
    print(nmkolom)
    print(list_kolom(nmkolom))
    print()


# # 1. Excel Operation

# In[733]:


# Get Excel file
cur_dir = os.getcwd()
#print(cur_dir)
filexls = os.path.join(cur_dir, '020124-010224_b.xlsx')
#filexls = os.path.join(cur_dir, 'DataB.xlsx')
#print(filexls)


# In[734]:


sheetxls = pd.ExcelFile(filexls).sheet_names
print(sheetxls)

for namasheet in sheetxls:
    print(namasheet)
    file = pd.read_excel(filexls, sheet_name=namasheet)
    print(file.columns)
    print()
# In[735]:


tiketdinas_column = ['no', 'no_laporan', 'uid_dinas', 'no_tiket_dinas', 'dinas', 'l2_notes', 'status', 'tiket_dibuat', \
                     'tiket_selesai', 'durasi_penanganan']

laporan_column = ['no', 'uid', 'no_laporan', 'tipe_saluran', 'waktu_lapor', 'agent_l1', 'tipe_laporan', 'pelapor', 'no_telp', 'kategori', \
                  'sub_kategori_1',  'sub_kategori_2', 'deskripsi', 'lokasi_kejadian', 'kecamatan', 'kelurahan', 'catatan_lokasi', 'latitude' , \
                  'longitude', 'waktu_selesai', 'ditutup_oleh', 'status', 'dinas_terkait', 'durasi_pengerjaan']

logdinas_column = ['no', 'no_laporan', 'no_tiket_dinas' , 'dinas', 'agent_l2', 'status', 'waktu_proses', 'durasi_penanganan', 'catatan', 'foto_1', \
                   'foto_2',  'foto_3', 'foto_4']

logl3_column = ['no', 'no_laporan', 'no_tiket_dinas', 'tiket_l3', 'dinas', 'agent_l3', 'status', 'tanggal', 'deskripsi', 'foto_1', 'foto_2', 'foto_3', \
                'foto_4', 'video']

lampiran_column = ['no', 'no_laporan', 'tipe_file', 'attachment']


# In[736]:


#tiketdinas_filter = ['no_laporan', 'uid_dinas', 'no_tiket_dinas', 'dinas', 'status', 'durasi_penanganan']
tiketdinas_filter = ['no_laporan', 'no_tiket_dinas', 'dinas', 'status', 'tiket_dibuat']

laporan_filter = ['uid', 'no_laporan', 'tipe_saluran', 'agent_l1', 'tipe_laporan', 'pelapor', 'no_telp', 'kategori', 'sub_kategori_1', 'sub_kategori_2', 'kecamatan', 'kelurahan', 'latitude', 'longitude', 'status']

logdinas_filter = ['no_tiket_dinas', 'dinas', 'status']

logl3_filter = ['no_laporan', 'no_tiket_dinas', 'tiket_l3', 'dinas', 'agent_l3', 'status']

lampiran_filter = ['no_laporan', 'tipe_file', 'attachment']


# In[737]:


# Read Excel data using pandas
# Rename Headers excel by Column Names DB
df_xls = pd.read_excel(filexls, sheet_name='TIKET DINAS',names=tiketdinas_column)
# Remove Quotes
df_xls['no_tiket_dinas'] = [x.replace("'", "") if isinstance(x, str) else x for x in df_xls['no_tiket_dinas']]
df_xls['l2_notes'] = [x.replace("'", "") if isinstance(x, str) else x for x in df_xls['l2_notes']]
# Fill NaN by columns
df_xls["no"] = df_xls["no"].ffill()
df_xls["l2_notes"] = df_xls["l2_notes"].fillna("-")


# ALT 1
#df_xls['no_tiket_dinas'] = df_xls['no_tiket_dinas'].apply(lambda x: x.replace("'", "")) # Remove Quotes
#df_xls['l2_notes'] = df_xls['l2_notes'].apply(lambda x: x.replace("'", "")) # Remove Quotes
# ALT 2
#df_xls['no_tiket_dinas'] = df_xls['no_tiket_dinas'].str.replace("'", "") # Remove Quotes
#df_xls['l2_notes'] = df_xls['l2_notes'].str.replace("'", "") # Remove Quotes
# ALT 3
#df_xls['no_tiket_dinas'] = [str(x).replace("'", "") for x in df_xls['no_tiket_dinas']] # Remove Quotes
#df_xls['l2_notes'] = [str(x).replace("'", "") for x in df_xls['no_tiket_dinas']] # Remove Quotes
# ALT 4
#df_xls['no_tiket_dinas'] = df_xls['no_tiket_dinas'].str.replace("'", "", regex=True)
#df_xls['l2_notes'] = df_xls['l2_notes'].str.replace("'", "", regex=True)
# ALT 5
#df_xls.fillna({"l2_notes": "-"}, inplace=True)
#df_xls['l2_notes'].fillna("-")
#df_xls['l2_notes'].fillna("-", inplace = True)
#df_xls['l2_notes']= df_xls['l2_notes'].fillna("-", inplace = True)
#df[col] = df[col].method(value)
#df_xls.fillna("no", method="ffill", inplace=True)
#df_xls["no"].ffill(inplace=True)
#df.method({col: value}, inplace=True)
#df_xls.ffill(col : "no", inplace=True)
#df_xls['no'].fillna(method="ffill", inplace=True)
#df_xls['l2_notes'].fillna( method ='-', inplace = True)
#print(df_xls.head())


# In[738]:


df_xls.head()


# In[739]:


JmlDataXls, JmlKolomXls = df_xls.shape
print('Jumlah Data XLS :',JmlDataXls)
print('Jumlah Kolom XLS :',JmlKolomXls)


# # Iterate through rows and insert non-duplicate data
# for row in df_xls.iterrows():
#     print(row)

# In[740]:


#df_xls


# In[741]:


# Check duplicates
duplicates = df_xls.duplicated()
duplicates


# In[742]:


# Remove Duplicates Before Save To Xls
df_xls = df_xls.drop_duplicates()
# print(df_xls)
#df_xls


# In[743]:


#df_xls.to_excel('TiketDinas_Clean.xlsx', index=False)
df_xls.to_excel('TiketDinas_Xls.xlsx', index=False)


# # 2. Database Operation

# In[744]:


import pandas as pd
from sqlalchemy import create_engine


# In[745]:


engine = create_engine('postgresql://postgres:S1d04rj0@localhost:5432/cc112')


# # Simpan Xls to db
# df_xls.to_sql(name='tiketdinas', con=engine,  if_exists="append", index=False )

# In[746]:


# Read DB
df_pg = pd.read_sql_query('select * from "tiketdinas"',con=engine, index_col=None)
#print(df_pg)
df_pg.head()


# df_pg.dtypes

# In[747]:


df_pg.to_excel('TiketDinas_DB.xlsx', index=False)


# In[748]:


JmlDataDB, JmlKolomDB = df_pg.shape
print('Jumlah Data DB:', JmlDataDB)
print('Jumlah Kolom DB:', JmlKolomDB)


# In[749]:


# Close the database connection
cursor.close()
conn.close()


# In[ ]:





# # Show Difference

# In[750]:


# Ori
df_pg.dtypes


# ### change dtypes
df_pg['uid_dinas'] = df_pg['uid_dinas'].astype(str)
#df_pg['no'] = df_pg['no'].values.astype('int64')
#df_pg['no'] = df_pg['no'].astype('float')
#df_pg['tiket_dibuat'] = df_pg['tiket_dibuat'].astype(str)
#df_pg['tiket_selesai'] = df_pg['tiket_selesai'].astype(str)
#df_pg['tiket_dibuat'] = df_pg['tiket_dibuat'].values.astype(str)
#df_pg['tiket_selesai'] = df_pg['tiket_selesai'].values.astype(str)
# In[751]:


df_pg.dtypes


# In[752]:


# ORI
df_xls.dtypes


# ### Change dtypes, Set XLS.dtypes = DB.dtypes

# In[753]:


df_xls = df_xls.astype(df_pg.dtypes)

df_xls['no'] = df_xls['no'].astype(float)
df_xls['uid_dinas'] = df_xls['uid_dinas'].astype(str)
df_xls['tiket_dibuat']= pd.to_datetime(df_xls['tiket_dibuat'])
df_xls['tiket_selesai']= pd.to_datetime(df_xls['tiket_selesai'])
#df_xls['tiket_dibuat'] = df_xls['tiket_dibuat'].values.astype(str)
#df_xls['tiket_selesai'] = df_xls['tiket_selesai'].values.astype(str)
#df_xls['no'] = df_xls['no'].values.astype('int64')
# In[754]:


df_xls.dtypes


# In[755]:


# Check df_pg.dtypes == df_xls.dtypes ?
print(df_pg.dtypes == df_xls.dtypes)


# # ALT 2 without filter columns
df_diff = pd.concat([df_pg,df_xls]).drop_duplicates(subset=["no_laporan", "uid_dinas", "no_tiket_dinas", "dinas", "l2_notes", "status", "tiket_dibuat" , "tiket_selesai", "durasi_penanganan"], keep=False)
df_diff
# In[756]:


df_diff = pd.concat([df_pg,df_xls]).drop_duplicates(subset=tiketdinas_filter, keep=False)
df_diff


# In[757]:


JmlDataNew, JmlKolomNew = df_diff.shape
print('Jumlah Data New :',JmlDataNew)
print('Jumlah Kolom New :',JmlKolomNew)


# # Simpan Data Baru ke DB

# In[758]:


df_diff.to_sql('tiketdinas', con=engine, index=False, if_exists='append')


# # Report

# In[759]:


print('Jumlah Data DB:',JmlDataDB)
print('Jumlah Data XLS:',JmlDataXls)
print('Jumlah Data New :',JmlDataNew)


# In[ ]:




