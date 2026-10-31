"""Tes otomatis dengan server in-process.
Jalankan dari folder proyek: python -m unittest discover -s tests -v"""
import os
import sys
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc
from server_bioskop import buat_server


class Dasar(unittest.TestCase):
    use_lock = True
    race_delay = 0.0

    def setUp(self):
        self.server, self.layanan, port = buat_server(
            port=0, max_workers=30, use_lock=self.use_lock, race_delay=self.race_delay)
        self.server.start()
        self.alamat = f"localhost:{port}"

    def tearDown(self):
        self.server.stop(0)

    def pesan(self, kursi, nama, rid=None):
        with grpc.insecure_channel(self.alamat) as ch:
            return pb_grpc.LayananBioskopStub(ch).PesanKursi(pb.PesanRequest(
                request_id=rid or str(uuid.uuid4()), jadwal_id="J1",
                kode_kursi=kursi, pemesan=nama), timeout=5)

    def race(self, n=20):
        with ThreadPoolExecutor(max_workers=n) as ex:
            return sum(h.sukses for h in ex.map(lambda i: self.pesan("A5", f"u{i}"), range(n)))


class TesDenganLock(Dasar):
    race_delay = 0.01

    def test_race_tepat_satu_sukses(self):
        self.assertEqual(self.race(), 1)

    def test_deduplikasi_request_id(self):
        rid = str(uuid.uuid4())
        a = self.pesan("B1", "andi", rid)
        b = self.pesan("B1", "andi", rid)   # retry dengan ID sama
        self.assertTrue(a.sukses)
        self.assertTrue(b.sukses)           # hasil lama dikembalikan, bukan "sudah dipesan"

    def test_batal_oleh_orang_lain_ditolak(self):
        self.pesan("C1", "andi")
        with grpc.insecure_channel(self.alamat) as ch:
            r = pb_grpc.LayananBioskopStub(ch).BatalPesan(pb.BatalRequest(
                request_id=str(uuid.uuid4()), jadwal_id="J1", kode_kursi="C1", pemesan="sari"))
        self.assertFalse(r.sukses)

    def test_stream_menerima_perubahan(self):
        with grpc.insecure_channel(self.alamat) as ch:
            stream = pb_grpc.LayananBioskopStub(ch).StreamStatusKursi(
                pb.JadwalRequest(jadwal_id="J1"))
            time.sleep(0.3)
            self.pesan("D1", "andi")
            ev = next(stream)
            self.assertEqual((ev.kode, ev.status), ("D1", "terpesan"))
            stream.cancel()


class TesAdmin(Dasar):
    def stub_call(self, nama_rpc, req):
        with grpc.insecure_channel(self.alamat) as ch:
            return getattr(pb_grpc.LayananBioskopStub(ch), nama_rpc)(req, timeout=5)

    def test_admin_batal_pesanan_orang_lain(self):
        self.pesan("E1", "andi")
        r = self.stub_call("AdminBatalPesan", pb.AdminBatalRequest(
            request_id=str(uuid.uuid4()), admin_token="admin123", jadwal_id="J1", kode_kursi="E1"))
        self.assertTrue(r.sukses)
        self.assertTrue(self.pesan("E1", "sari").sukses)   # kursi bebas lagi

    def test_admin_token_salah_ditolak(self):
        with self.assertRaises(grpc.RpcError) as c:
            self.stub_call("AdminBatalPesan", pb.AdminBatalRequest(
                request_id=str(uuid.uuid4()), admin_token="salah", jadwal_id="J1", kode_kursi="E1"))
        self.assertEqual(c.exception.code(), grpc.StatusCode.PERMISSION_DENIED)

    def test_admin_reset_jadwal(self):
        self.pesan("F1", "andi"); self.pesan("F2", "sari")
        r = self.stub_call("AdminResetJadwal", pb.AdminResetRequest(
            admin_token="admin123", jadwal_id="J1"))
        self.assertEqual(r.pesan, "2 kursi dikosongkan")
        self.assertTrue(self.pesan("F1", "budi").sukses)


class TesTanpaLock(Dasar):
    use_lock = False
    race_delay = 0.05

    def test_race_condition_terbukti(self):
        self.assertGreater(self.race(), 1, "tanpa lock seharusnya terjadi double booking")


if __name__ == "__main__":
    unittest.main()
