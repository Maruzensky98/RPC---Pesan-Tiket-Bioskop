# Reservasi Tiket Bioskop Berbasis gRPC

Proyek Mini Komputasi Paralel dan Terdistribusi (Studi Kasus 1).

**Kelompok:** (isi) | **Anggota:** (nama, NIM)

Demo dijalankan pada **2 laptop fisik**: Laptop A sebagai server, Laptop B sebagai client.

## Struktur folder

```
bioskop-rpc/
├── proto/bioskop.proto        # kontrak layanan (IDL), 4 RPC
├── src/
│   ├── server_bioskop.py      # server (lock, deduplikasi, streaming)
│   ├── client_bioskop.py      # client interaktif (timeout, retry, request_id)
│   └── loadtest.py            # uji race condition dan kinerja
├── tests/test_bioskop.py      # tes otomatis (server dan client di satu laptop)
├── docs/                      # laporan, diagram, screenshot
├── hasil/                     # CSV hasil uji kinerja
├── generate_stubs.sh / .bat
└── requirements.txt
```

## 1. Instalasi (dilakukan di KEDUA laptop)

Butuh Python 3.10 ke atas.

```bash
cd bioskop-rpc
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
./generate_stubs.sh               # Windows: generate_stubs.bat
```

## 2. Menyiapkan jaringan

1. Sambungkan kedua laptop ke **WiFi yang sama**. Jika WiFi kampus memblokir
   komunikasi antar-perangkat, pakai **hotspot HP** dan sambungkan kedua laptop ke sana.
2. Di **Laptop A (server)**, cari alamat IP:
   - Windows: `ipconfig` -> lihat *IPv4 Address* (contoh `192.168.1.10`)
   - Linux: `ip a` atau `hostname -I`
   - macOS: `ipconfig getifaddr en0`
3. Izinkan port **50051** pada firewall Laptop A.
   - Windows (PowerShell sebagai Administrator):
     `New-NetFirewallRule -DisplayName "gRPC Bioskop" -Direction Inbound -Protocol TCP -LocalPort 50051 -Action Allow`
   - Linux (ufw): `sudo ufw allow 50051/tcp`
   - Alternatif: matikan firewall sementara saat uji coba.
4. Di **Laptop B**, tes koneksi dasar: `ping 192.168.1.10` (ganti dengan IP Laptop A).

## 3. Menjalankan

**Laptop A (server):**
```bash
python src/server_bioskop.py
```
Akan tampil: `Server bioskop berjalan di port 50051 (lock=ON)`.

**Laptop B (client):** buka 2-3 terminal, di masing-masing jalankan (ganti IP dan nama):
```bash
python src/client_bioskop.py --server 192.168.1.10:50051 --nama andi
python src/client_bioskop.py --server 192.168.1.10:50051 --nama sari
```

Perintah di client: `lihat`, `pesan A5`, `batal A5`, `keluar`.
Saat satu client memesan kursi, terminal client lain langsung menampilkan baris
`[LIVE] A5 -> terpesan andi` (server streaming).

Untuk uji coba cepat di satu laptop saja, cukup pakai `localhost:50051`.

## 4. Pengujian

Tes otomatis (satu laptop, tanpa perlu menjalankan server manual):
```bash
python -m unittest discover -s tests -v
```

Uji beban, dijalankan dari **Laptop B** ke server di Laptop A:
```bash
python src/loadtest.py race --server 192.168.1.10:50051 --clients 20
python src/loadtest.py kinerja --server 192.168.1.10:50051 --clients 1 5 20
```
- `race`: 20 client memesan kursi yang sama, hasil yang benar tepat **1 sukses**.
- `kinerja`: menghasilkan latensi rata-rata dan throughput, disimpan ke `hasil/hasil_kinerja.csv`
  (bahan tabel dan grafik laporan). Jalankan dari laptop client supaya CPU server tidak
  dibagi dengan program uji.

> Restart server di Laptop A sebelum mengulang uji `race`, karena kursi yang sudah
> terpesan tidak otomatis kosong lagi.

### Eksperimen untuk laporan (jalankan di Laptop A)

| Tujuan | Cara menjalankan server |
|---|---|
| Bukti lock diperlukan | `USE_LOCK=0 RACE_DELAY=0.05 python src/server_bioskop.py`, lalu uji `race` dari Laptop B (hasil: sukses > 1) |
| Bukti retry + deduplikasi | `SLOW_REPLY=0.3 python src/server_bioskop.py`, lalu pesan kursi lewat client |
| Pengaruh jumlah thread | `MAX_WORKERS=1 python src/server_bioskop.py` (bandingkan dengan 4 dan 10) |

Windows PowerShell: `$env:USE_LOCK="0"; $env:RACE_DELAY="0.05"; python src/server_bioskop.py`

## 5. Masalah yang sering muncul

| Gejala | Penyebab dan solusi |
|---|---|
| Client menampilkan `UNAVAILABLE` / `DEADLINE_EXCEEDED` | Firewall Laptop A memblokir port 50051, IP salah, atau beda jaringan WiFi. Cek ulang langkah 2. |
| `ping` berhasil tetapi gRPC gagal | Port 50051 belum diizinkan di firewall. |
| `ping` gagal | WiFi mengisolasi perangkat. Pindah ke hotspot HP. |
| `ModuleNotFoundError: bioskop_pb2` | Belum menjalankan `generate_stubs`. |
| IP server berubah | Cek ulang IP Laptop A setiap kali pindah jaringan. |

## 6. Keputusan desain (untuk laporan)

- **Concurrency:** satu `threading.Lock` global membuat cek-lalu-tulis kursi atomik.
  Cukup untuk skala tugas ini; pengembangan: satu lock per jadwal.
- **Toleransi kegagalan:** client memakai timeout 2 detik dan retry exponential backoff
  (hanya untuk `DEADLINE_EXCEEDED` dan `UNAVAILABLE`) dengan `request_id` tetap.
  Server menyimpan hasil per `request_id` sehingga retry tidak memesan dua kali (at-most-once).
- **Keterbatasan:** data dan tabel `proses` ada di memori, hilang saat server restart.
  Perbaikan: simpan ke database atau file.
