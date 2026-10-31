"""Client interaktif reservasi bioskop (timeout + retry + request_id)."""
import argparse
import os
import threading
import time
import uuid

import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc

BISA_DIULANG = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


def panggil_dengan_retry(fungsi, request, maks=3, timeout=2.0):
    """Retry dengan exponential backoff. request (termasuk request_id) TIDAK
    diubah antar percobaan, sehingga server bisa membuang duplikat."""
    for percobaan in range(1, maks + 1):
        try:
            return fungsi(request, timeout=timeout)
        except grpc.RpcError as e:
            if e.code() not in BISA_DIULANG or percobaan == maks:
                raise
            jeda = 0.5 * 2 ** (percobaan - 1)
            print(f"[retry] percobaan {percobaan} gagal ({e.code().name}), ulang {jeda}s")
            time.sleep(jeda)


def pesan_kursi(stub, jadwal, kode, nama, **kw):
    req = pb.PesanRequest(request_id=str(uuid.uuid4()), jadwal_id=jadwal,
                          kode_kursi=kode, pemesan=nama)
    return panggil_dengan_retry(stub.PesanKursi, req, **kw)


def batal_pesan(stub, jadwal, kode, nama, **kw):
    req = pb.BatalRequest(request_id=str(uuid.uuid4()), jadwal_id=jadwal,
                          kode_kursi=kode, pemesan=nama)
    return panggil_dengan_retry(stub.BatalPesan, req, **kw)


def tampilkan_kursi(stub, jadwal):
    daftar = stub.LihatKursi(pb.JadwalRequest(jadwal_id=jadwal), timeout=2.0)
    baris = {}
    for k in daftar.kursi:
        baris.setdefault(k.kode[0], []).append(k)
    for b, ks in sorted(baris.items()):
        print(b, " ".join("[X]" if k.status == "terpesan" else "[ ]" for k in ks))
    print("   " + "   ".join(str(i) for i in range(1, 11)))


def dengarkan(stub, jadwal):
    """Berjalan di thread terpisah: cetak setiap perubahan kursi secara live."""
    try:
        for ev in stub.StreamStatusKursi(pb.JadwalRequest(jadwal_id=jadwal)):
            print(f"\n[LIVE] {ev.kode} -> {ev.status} {ev.pemesan}\n> ", end="", flush=True)
    except grpc.RpcError:
        pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--server", default=os.environ.get("SERVER_ADDR", "localhost:50051"))
    p.add_argument("--nama", default="user")
    p.add_argument("--jadwal", default="J1")
    a = p.parse_args()

    with grpc.insecure_channel(a.server) as ch:
        stub = pb_grpc.LayananBioskopStub(ch)
        threading.Thread(target=dengarkan, args=(stub, a.jadwal), daemon=True).start()
        print("Perintah: lihat | pesan <kursi> | batal <kursi> | keluar")
        while True:
            try:
                cmd = input("> ").split()
            except EOFError:
                break
            if not cmd:
                continue
            try:
                if cmd[0] == "lihat":
                    tampilkan_kursi(stub, a.jadwal)
                elif cmd[0] == "pesan" and len(cmd) == 2:
                    print(pesan_kursi(stub, a.jadwal, cmd[1].upper(), a.nama).pesan)
                elif cmd[0] == "batal" and len(cmd) == 2:
                    print(batal_pesan(stub, a.jadwal, cmd[1].upper(), a.nama).pesan)
                elif cmd[0] in ("keluar", "exit", "quit"):
                    break
                else:
                    print("Perintah tidak dikenal")
            except grpc.RpcError as e:
                print("Error:", e.code().name, e.details())


if __name__ == "__main__":
    main()
