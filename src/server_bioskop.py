"""Server gRPC reservasi tiket bioskop.

Variabel lingkungan (opsional, untuk eksperimen laporan):
  PORT          port server (default 50051)
  MAX_WORKERS   jumlah thread server (default 10)
  USE_LOCK      "1" memakai lock (default), "0" untuk membuktikan race condition
  RACE_DELAY    jeda (detik) antara cek dan tulis kursi, memperbesar peluang race
  SLOW_REPLY    peluang (0-1) balasan ditunda 3 detik SETELAH data tersimpan,
                untuk menguji retry + deduplikasi
"""
import os
import queue
import random
import threading
import time
from concurrent import futures
from contextlib import nullcontext

import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc


def data_awal():
    """Dua jadwal, masing-masing 10 baris (A-J) x 10 kursi."""
    baris = "ABCDEFGHIJ"
    return {
        j: {f"{b}{n}": None for b in baris for n in range(1, 11)}
        for j in ("J1", "J2")
    }


class LayananBioskop(pb_grpc.LayananBioskopServicer):
    def __init__(self, use_lock=True, race_delay=0.0, slow_reply=0.0):
        self.kursi = data_awal()          # jadwal -> kode -> pemesan / None
        self.proses = {}                  # request_id -> response (deduplikasi)
        self.lock = threading.Lock()      # satu lock global; lihat README untuk alasannya
        self.use_lock = use_lock
        self.race_delay = race_delay
        self.slow_reply = slow_reply
        self.subscribers = {}             # jadwal -> daftar queue
        self.sub_lock = threading.Lock()

    # ---------- util ----------
    def _kunci(self):
        return self.lock if self.use_lock else nullcontext()

    def _umumkan(self, jadwal, kode, pemesan):
        event = pb.PerubahanKursi(
            jadwal_id=jadwal, kode=kode,
            status="terpesan" if pemesan else "kosong",
            pemesan=pemesan or "",
        )
        with self.sub_lock:
            for q in self.subscribers.get(jadwal, []):
                q.put(event)

    def _validasi(self, jadwal, kode, context):
        if jadwal not in self.kursi:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Jadwal {jadwal} tidak ada")
        if kode not in self.kursi[jadwal]:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Kursi {kode} tidak ada")

    def _balas_lambat(self):
        if self.slow_reply and random.random() < self.slow_reply:
            time.sleep(3)  # simulasi balasan terlambat: data SUDAH tersimpan

    # ---------- RPC ----------
    def LihatKursi(self, request, context):
        if request.jadwal_id not in self.kursi:
            context.abort(grpc.StatusCode.NOT_FOUND, "Jadwal tidak ada")
        with self._kunci():
            snapshot = dict(self.kursi[request.jadwal_id])
        return pb.DaftarKursi(kursi=[
            pb.Kursi(kode=k, status="terpesan" if v else "kosong", pemesan=v or "")
            for k, v in snapshot.items()
        ])

    def PesanKursi(self, request, context):
        self._validasi(request.jadwal_id, request.kode_kursi, context)
        with self._kunci():
            if request.request_id in self.proses:        # duplikat -> hasil lama
                return self.proses[request.request_id]
            kursi = self.kursi[request.jadwal_id]
            if kursi[request.kode_kursi] is not None:
                hasil = pb.PesanResponse(sukses=False, pesan="Kursi sudah dipesan")
            else:
                if self.race_delay:
                    time.sleep(self.race_delay)          # hanya untuk eksperimen
                kursi[request.kode_kursi] = request.pemesan
                hasil = pb.PesanResponse(sukses=True, pesan="Berhasil")
                self._umumkan(request.jadwal_id, request.kode_kursi, request.pemesan)
            self.proses[request.request_id] = hasil
        self._balas_lambat()
        return hasil

    def BatalPesan(self, request, context):
        self._validasi(request.jadwal_id, request.kode_kursi, context)
        with self._kunci():
            if request.request_id in self.proses:
                return self.proses[request.request_id]
            kursi = self.kursi[request.jadwal_id]
            if kursi[request.kode_kursi] is None:
                hasil = pb.PesanResponse(sukses=False, pesan="Kursi belum dipesan")
            elif kursi[request.kode_kursi] != request.pemesan:
                hasil = pb.PesanResponse(sukses=False, pesan="Bukan pemesan kursi ini")
            else:
                kursi[request.kode_kursi] = None
                hasil = pb.PesanResponse(sukses=True, pesan="Pesanan dibatalkan")
                self._umumkan(request.jadwal_id, request.kode_kursi, None)
            self.proses[request.request_id] = hasil
        self._balas_lambat()
        return hasil

    def StreamStatusKursi(self, request, context):
        if request.jadwal_id not in self.kursi:
            context.abort(grpc.StatusCode.NOT_FOUND, "Jadwal tidak ada")
        q = queue.Queue()
        with self.sub_lock:
            self.subscribers.setdefault(request.jadwal_id, []).append(q)
        try:
            while context.is_active():
                try:
                    yield q.get(timeout=1)
                except queue.Empty:
                    continue
        finally:  # bersihkan saat client putus
            with self.sub_lock:
                self.subscribers[request.jadwal_id].remove(q)


def buat_server(port=50051, max_workers=10, **opsi):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    layanan = LayananBioskop(**opsi)
    pb_grpc.add_LayananBioskopServicer_to_server(layanan, server)
    port_nyata = server.add_insecure_port(f"[::]:{port}")
    return server, layanan, port_nyata


if __name__ == "__main__":
    env = os.environ.get
    server, _, port = buat_server(
        port=int(env("PORT", "50051")),
        max_workers=int(env("MAX_WORKERS", "10")),
        use_lock=env("USE_LOCK", "1") == "1",
        race_delay=float(env("RACE_DELAY", "0")),
        slow_reply=float(env("SLOW_REPLY", "0")),
    )
    server.start()
    print(f"Server bioskop berjalan di port {port} "
          f"(lock={'ON' if env('USE_LOCK', '1') == '1' else 'OFF'})")
    server.wait_for_termination()
