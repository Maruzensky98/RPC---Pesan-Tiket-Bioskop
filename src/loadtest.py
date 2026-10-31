"""Uji beban: mode 'race' (kursi sama) dan 'kinerja' (latensi & throughput).

Contoh:
  python src/loadtest.py race --server localhost:50051 --clients 20
  python src/loadtest.py kinerja --server 192.168.1.10:50051 --clients 1 5 20
"""
import argparse
import csv
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

import grpc

import bioskop_pb2_grpc as pb_grpc
from client_bioskop import batal_pesan, pesan_kursi


def uji_race(server, n, jadwal="J2", kursi="A5"):
    def satu(i):
        with grpc.insecure_channel(server) as ch:
            return pesan_kursi(pb_grpc.LayananBioskopStub(ch), jadwal, kursi, f"user{i}")

    with ThreadPoolExecutor(max_workers=n) as ex:
        hasil = list(ex.map(satu, range(n)))
    sukses = sum(h.sukses for h in hasil)
    print(f"{n} client memesan {kursi}: sukses={sukses}, gagal={n - sukses}")
    print("LULUS (tepat 1 sukses)" if sukses == 1 else "GAGAL: terjadi double booking!")
    return sukses


def uji_kinerja(server, daftar_client, siklus=30, jadwal="J1"):
    baris_csv = []
    for n in daftar_client:
        def satu(i):
            lat = []
            kursi = f"{'ABCDEFGHIJ'[i // 10]}{i % 10 + 1}"  # kursi unik per client
            with grpc.insecure_channel(server) as ch:
                stub = pb_grpc.LayananBioskopStub(ch)
                for _ in range(siklus):
                    for fn in (pesan_kursi, batal_pesan):
                        t = time.perf_counter()
                        fn(stub, jadwal, kursi, f"u{i}")
                        lat.append((time.perf_counter() - t) * 1000)
            return lat

        mulai = time.perf_counter()
        with ThreadPoolExecutor(max_workers=n) as ex:
            semua = [x for lat in ex.map(satu, range(n)) for x in lat]
        total = time.perf_counter() - mulai
        baris_csv.append([n, len(semua), round(total, 3),
                          round(len(semua) / total, 1), round(statistics.mean(semua), 2)])
        print(f"client={n:>2} | req={len(semua)} | total={total:.2f}s | "
              f"{len(semua) / total:.1f} req/s | rata-rata {statistics.mean(semua):.2f} ms")

    os.makedirs("hasil", exist_ok=True)
    with open("hasil/hasil_kinerja.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["client", "jumlah_request", "waktu_total_s", "throughput_rps", "latensi_rata2_ms"])
        w.writerows(baris_csv)
    print("Tersimpan di hasil/hasil_kinerja.csv")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["race", "kinerja"])
    p.add_argument("--server", default="localhost:50051")
    p.add_argument("--clients", type=int, nargs="+", default=[1, 5, 20])
    a = p.parse_args()
    if a.mode == "race":
        uji_race(a.server, a.clients[0])
    else:
        uji_kinerja(a.server, a.clients)
