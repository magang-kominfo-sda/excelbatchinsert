import pandas as pd

def bandingkan_excel(file1, file2):
    # Membaca file Excel menjadi DataFrame
    df1 = pd.read_excel(file1)
    df2 = pd.read_excel(file2)

    # Membandingkan kedua DataFrame
    kesamaan = df1.equals(df2)

    if kesamaan:
        print("Kedua file Excel identik, tidak ada perbedaan.")
    else:
        print("Kedua file Excel berbeda.")

# Contoh penggunaan
# Gantilah bagian ini dengan nama file Excel yang ingin Anda bandingkan
file_path1 = '020124-010224_a.xlsx'
file_path2 = 'Perbulan2/020124-010224_b.xlsx'

# Pemanggilan fungsi untuk membandingkan Excel dan mengonversi menjadi DataFrame
bandingkan_excel(file_path1, file_path2)