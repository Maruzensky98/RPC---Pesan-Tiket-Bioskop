"""Program admin: pantau kursi (lengkap dengan nama pemesan), log live, batalkan
pesanan siapa pun, dan reset jadwal.

  python src/admin_bioskop.py --server localhost:50051 --token admin123
"""
import argparse
import os
import threading
import uuid
from datetime import datetime

import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc


def ambil_kursi(stub, jadwal):
    d = stub.LihatKursi(pb.JadwalRequest(jadwal_id=jadwal), timeout=2.0)
    return {k.kode: k.pemesan for k in d.kursi if k.status == "terpesan"}, [k.kode for k in d.kursi]


def denah(stub, jadwal):
    terpesan, semua = ambil_kursi(stub, jadwal)
    baris = {}
    for kode in semua:
        baris.setdefault(kode[0], []).append(kode)
    print(f"\nDenah jadwal {jadwal}   ([X] terpesan, [ ] kosong)")
    for b, kodes in sorted(baris.items()):
        print(b, " ".join("[X]" if k in terpesan else "[ ]" for k in kodes))
    print("   " + "   ".join(str(i) for i in range(1, 11)))
    print(f"\nTerisi {len(terpesan)} dari {len(semua)} kursi")
    if terpesan:
        print("Daftar pemesan:")
        for kode, nama in sorted(terpesan.items()):
            print(f"  {kode:<4} {nama}")
    print()


def log_live(stub, jadwal):
    try:
        for ev in stub.StreamStatusKursi(pb.JadwalRequest(jadwal_id=jadwal)):
            aksi = f"dipesan oleh {ev.pemesan}" if ev.status == "terpesan" else "dikosongkan"
            print(f"\n[{datetime.now():%H:%M:%S}] {ev.kode} {aksi}\nadmin> ", end="", flush=True)
    except grpc.RpcError:
        pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--server", default=os.environ.get("SERVER_ADDR", "localhost:50051"))
    p.add_argument("--token", default="admin123")
    p.add_argument("--jadwal", default="J1")
    a = p.parse_args()
    jadwal = a.jadwal

    with grpc.insecure_channel(a.server) as ch:
        stub = pb_grpc.LayananBioskopStub(ch)
        threading.Thread(target=log_live, args=(stub, jadwal), daemon=True).start()
        print(f"ADMIN | jadwal {jadwal}")
        print("Perintah: denah | batal <kursi> | reset | jadwal <J1|J2> | keluar")
        while True:
            try:
                cmd = input("admin> ").split()
            except EOFError:
                break
            if not cmd:
                continue
            try:
                if cmd[0] == "denah":
                    denah(stub, jadwal)
                elif cmd[0] == "batal" and len(cmd) == 2:
                    r = stub.AdminBatalPesan(pb.AdminBatalRequest(
                        request_id=str(uuid.uuid4()), admin_token=a.token,
                        jadwal_id=jadwal, kode_kursi=cmd[1].upper()), timeout=2.0)
                    print(r.pesan)
                elif cmd[0] == "reset":
                    if input(f"Kosongkan SEMUA kursi {jadwal}? (y/N) ").lower() == "y":
                        r = stub.AdminResetJadwal(pb.AdminResetRequest(
                            admin_token=a.token, jadwal_id=jadwal), timeout=5.0)
                        print(r.pesan)
                elif cmd[0] == "jadwal" and len(cmd) == 2:
                    jadwal = cmd[1].upper()
                    print(f"Sekarang memantau {jadwal} (log live tetap untuk jadwal awal; "
                          "jalankan ulang admin untuk log jadwal ini)")
                elif cmd[0] in ("keluar", "exit", "quit"):
                    break
                else:
                    print("Perintah tidak dikenal")
            except grpc.RpcError as e:
                print("Error:", e.code().name, e.details())


if __name__ == "__main__":
    main()
