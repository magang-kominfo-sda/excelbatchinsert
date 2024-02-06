from sqlalchemy import Column, String, Float, TIMESTAMP, UUID
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Lampiran(Base):
    __tablename__ = 'lampiran'

    no = Column(Float, nullable=True)
    no_laporan = Column(String)
    tipe_file = Column(String)
    attachment = Column(String)

class Laporan(Base):
    __tablename__ = 'laporan'

    no = Column(Float, nullable=True)
    uid = Column(UUID)  
    no_laporan = Column(String)
    tipe_saluran = Column(String)
    waktu_lapor = Column(TIMESTAMP)
    agent_l1 = Column(String)
    tipe_laporan = Column(String)
    pelapor = Column(String)
    no_telp = Column(String)
    kategori = Column(String)
    sub_kategori_1 = Column(String)
    sub_kategori_2 = Column(String)
    deskripsi = Column(String)
    lokasi_kejadian = Column(String)
    kecamatan = Column(String)
    kelurahan = Column(String)
    catatan_lokasi = Column(String)
    latitude = Column(DOUBLE_PRECISION)
    longitude = Column(DOUBLE_PRECISION)
    waktu_selesai = Column(TIMESTAMP)
    ditutup_oleh = Column(String)
    status = Column(String)
    dinas_terkait = Column(String)
    durasi_pengerjaan = Column(String)

class LogDinas(Base):
    __tablename__ = 'logdinas'

    no = Column(Float, nullable=True)
    no_laporan = Column(String)
    no_tiket_dinas = Column(String)
    dinas = Column(String)
    agent_l2 = Column(String)
    status = Column(String)
    waktu_proses = Column(TIMESTAMP)
    durasi_penanganan = Column(String)
    catatan = Column(String)
    foto_1 = Column(String)
    foto_2 = Column(String)
    foto_3 = Column(String)
    foto_4 = Column(String)

class LogL3(Base):
    __tablename__ = 'logl3'

    no = Column(Float, nullable=True)
    no_laporan = Column(String)
    no_tiket_dinas = Column(String)
    tiket_l3 = Column(String)
    dinas = Column(String)
    agent_l3 = Column(String)
    status = Column(String)
    tanggal = Column(TIMESTAMP)
    deskripsi = Column(String)
    foto_1 = Column(String)
    foto_2 = Column(String)
    foto_3 = Column(String)
    foto_4 = Column(String)
    video = Column(String)

class TiketDinas(Base):
    __tablename__ = 'tiketdinas'

    no = Column(Float, nullable=True)
    no_laporan = Column(String)
    uid_dinas = Column(UUID)  
    no_tiket_dinas = Column(String)
    dinas = Column(String)
    l2_notes = Column(String)
    status = Column(String)
    tiket_dibuat = Column(TIMESTAMP)
    tiket_selesai = Column(TIMESTAMP)
    durasi_penanganan = Column(String)
