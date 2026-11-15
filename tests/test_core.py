"""核心功能测试, 覆盖需求 1..7:

1. 批量写入 + 乱序归位
2. 时间戳差值编码 + 数值 XOR 压缩 + 压缩率
3. 标签倒排索引组合过滤
4. 三级保留策略与自动降采样
5. 计数器回绕/重启清零的速率处理
6. 时间分组 + sum/max/quantile 查询
7. 掉电: 写一半的块被截断, 恢复到最后一个完整块
"""

import os
import shutil
import tempfile
import unittest

from pytimeseries import benchmark, block as block_mod
from pytimeseries.codec import (KIND_INT, KIND_INT_DOD, decode_block,
                                encode_block, zigzag_decode, zigzag_encode)
from pytimeseries.engine import Engine
from pytimeseries.index import SeriesIndex, canonical_key
from pytimeseries.memtable import Memtable
from pytimeseries.query import QueryRequest, execute, parse_query
from pytimeseries.retention import RetentionManager, safe_delta


class TestCodec(unittest.TestCase):
    def test_zigzag(self):
        for n in [-1, 0, 1, -2, 2, 12345, -12345]:
            self.assertEqual(zigzag_decode(zigzag_encode(n)), n)

    def test_roundtrip_int(self):
        times = [1_700_000_000 + i * 30 for i in range(500)]
        values = [1000 + i * 333 for i in range(500)]
        payload, kind = encode_block(times, values)
        self.assertIn(kind, (KIND_INT, KIND_INT_DOD))
        t2, v2 = decode_block(payload)
        self.assertEqual(t2, times)
        self.assertEqual(v2, values)

    def test_roundtrip_float_and_negative(self):
        times = [100 + i for i in range(200)]
        values = [(-1.5) ** i for i in range(200)]
        payload, kind = encode_block(times, values)
        t2, v2 = decode_block(payload)
        self.assertEqual(t2, times)
        for a, b in zip(values, v2):
            self.assertAlmostEqual(a, b, places=9)

    def test_compression_ratio_under_10pct(self):
        # 平滑计数器 + 固定 30s 间隔, 模拟真实交换机上报
        import random
        rng = random.Random(1)
        n = 50_000
        times = [1_700_000_000 + i * 30 for i in range(n)]
        # 自相关流量: 相邻增量高度接近, DOD 主体落在 ±8 紧凑档
        values = []
        v = 1_000_000
        base = 1_000_000
        for _ in range(n):
            base += rng.randint(-5, 5)
            base = max(10_000, base)
            v += base + rng.randint(-4, 4)
            values.append(v)
        payload, _ = encode_block(times, values)
        raw = n * 16
        self.assertLess(len(payload) / raw, 0.10,
                        f"压缩率 {len(payload) / raw:.3f} 未达 1/10 目标")

    def test_64bit_values(self):
        times = [1, 2, 3, 4, 5]
        # 无符号 64 位计数器, 包含回绕前后的大值(回绕处为异常点, 走旁路)
        values = [(1 << 63) - 1, (1 << 63), (1 << 64) - 1, 12345, 999999]
        payload, _ = encode_block(times, values)
        _, v2 = decode_block(payload)
        self.assertEqual(v2, values)

    def test_unsigned_counter_roundtrip(self):
        # 无符号非负计数器, 含重启清零(值变小)与接近回绕的大值, 精确往返
        import random
        r = random.Random(11)
        times = list(range(1, 60))
        values = []
        v = 1000
        for i in range(59):
            if i == 20:
                v = 50                      # 模拟重启清零
            elif i == 40:
                v = (1 << 64) - 100        # 模拟接近 64 位回绕
            else:
                v += r.randint(1, 50)
            values.append(v)
        payload, _ = encode_block(times, values)
        _, v2 = decode_block(payload)
        self.assertEqual(v2, values)

    def test_non_monotonic_roundtrip(self):
        # 含下降(乱序/重启)的整数序列: 异常点旁路, 正常点 DOD, 往返一致
        times = list(range(1, 22))
        vals = [1, 5, 3, 8, 2, 9, 0] * 3
        payload, kind = encode_block(times, vals)
        self.assertEqual(kind, KIND_INT_DOD)
        t2, v2 = decode_block(payload)
        self.assertEqual(t2, times)
        self.assertEqual(v2, vals)


class TestMemtable(unittest.TestCase):
    def test_out_of_order_insertion(self):
        mem = Memtable()
        for ts, val in [(30, 3), (10, 1), (20, 2), (20, 22), (40, 4)]:
            mem.add(1, ts, val)
        sm = mem.series[1]
        self.assertEqual(sm.times, [10, 20, 30, 40])
        self.assertEqual(sm.values, [1, 22, 3, 4])  # 同时间戳后写覆盖


class TestIndex(unittest.TestCase):
    def test_canonical_and_postings(self):
        d = tempfile.mkdtemp()
        try:
            idx = SeriesIndex(os.path.join(d, "series.log"))
            s1, _ = idx.get_or_create("m", {"device": "a", "idc": "bj"})
            s2, _ = idx.get_or_create("m", {"device": "b", "idc": "bj"})
            s3, _ = idx.get_or_create("m", {"device": "c", "idc": "sh"})
            # 重复注册返回同一 sid
            s1b, created = idx.get_or_create("m", {"idc": "bj", "device": "a"})
            self.assertEqual(s1, s1b)
            self.assertFalse(created)
            self.assertEqual(idx.match("m", {"idc": "bj"}), {s1, s2})
            self.assertEqual(idx.match("m", {"idc": "bj", "device": "b"}), {s2})
            self.assertEqual(idx.match("m", {"idc": ["bj", "sh"]}), {s1, s2, s3})
            # 日志重放
            idx.close()  # 刷盘并释放句柄
            idx2 = SeriesIndex(os.path.join(d, "series.log"))
            idx2.load()
            self.assertEqual(idx2.match("m", {"idc": "sh"}), {s3})
            self.assertEqual(canonical_key("m", {"a": "1", "b": "2"}),
                             canonical_key("m", {"b": "2", "a": "1"}))
            idx2.close()
        finally:
            shutil.rmtree(d, ignore_errors=True)


class EngineTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.engine = Engine(self.dir, flush_points=5000)

    def tearDown(self):
        self.engine.close()
        shutil.rmtree(self.dir, ignore_errors=True)


class TestWriteRead(EngineTestBase):
    def test_batch_write_and_flush(self):
        pts = [("m", {"device": "d1"}, 1000 + i * 30, i * 100)
               for i in range(1000)]
        self.engine.write_batch(pts)
        self.engine.flush()
        sids = self.engine.select_series("m", {"device": "d1"})
        self.assertEqual(len(sids), 1)
        sid = next(iter(sids))
        times, values = self.engine.read_series(sid, 0, 10 ** 9)
        self.assertEqual(len(times), 1000)
        self.assertEqual(times, sorted(times))
        self.assertEqual(values[0], 0)

    def test_out_of_order_on_disk(self):
        # 乱序点跨多次刷盘也要读到正确顺序
        base = 10_000
        for r in range(3):
            batch = []
            for i in range(3000):
                ts = base + r * 3000 * 30 + i * 30
                batch.append(("m", {"d": "x"}, ts, i))
            # 每批混入更早时间戳的乱序点
            batch.append(("m", {"d": "x"}, base + 30, 99999))
            self.engine.write_batch(batch)
            self.engine.flush()
        sid = next(iter(self.engine.select_series("m", {"d": "x"})))
        times, values = self.engine.read_series(sid, 0, 10 ** 9)
        self.assertEqual(times, sorted(times))
        self.assertEqual(dict(zip(times, values))[base + 30], 99999)


class TestCounterWrap(unittest.TestCase):
    def test_safe_delta(self):
        # 正常增量
        self.assertEqual(safe_delta(100, 150), 50)
        # 64 位回绕: 高值 -> 小值
        wrap = safe_delta((1 << 64) - 100, 200)
        self.assertEqual(wrap, 300)
        # 重启清零: 低值计数器下降 -> 丢弃该跳变
        self.assertIsNone(safe_delta(5000, 100))

    def test_rate_query_skips_reset(self):
        d = tempfile.mkdtemp()
        engine = Engine(d, flush_points=1000)
        try:
            # 30s 间隔; 第三个点模拟重启清零
            pts = [("if_port_in_bytes", {"d": "r"}, 0, 1000),
                   ("if_port_in_bytes", {"d": "r"}, 30, 2000),
                   ("if_port_in_bytes", {"d": "r"}, 60, 100),
                   ("if_port_in_bytes", {"d": "r"}, 90, 400)]
            engine.write_batch(pts)
            engine.flush()
            req = QueryRequest("rate(if_port_in_bytes{d=\"r\"})", 0, 1000, 30)
            res = execute(engine, req)
            vals = [v for _, v in res["series"][0]["points"]]
            # 33.33/s 保留; 重启区间被跳过; 之后 10/s 正常
            self.assertAlmostEqual(vals[0], 1000 / 30, places=4)
            self.assertNotIn(round((100 - 2000) / 30, 4), [round(x, 4) for x in vals])
            self.assertAlmostEqual(vals[-1], 300 / 30, places=4)
        finally:
            engine.close()
            shutil.rmtree(d)


class TestQuery(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.engine = Engine(self.dir, flush_points=10000)
        for d_idc in ("bj", "sh"):
            for dev in range(3):
                for i in range(600):  # 5 小时的 30s 点
                    self.engine.write(
                        "if_port_in_bytes",
                        {"idc": d_idc, "device": f"sw{dev}"},
                        10_000 + i * 30,
                        1000 + i * 10 + dev,
                    )
        self.engine.flush()

    def tearDown(self):
        self.engine.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_parse(self):
        q = parse_query('quantile(0.95) by (device) (rate(m{k="v"}))')
        self.assertEqual(q.metric, "m")
        self.assertEqual(q.aggregator, "quantile")
        self.assertAlmostEqual(q.quantile, 0.95)
        self.assertEqual(q.by, ["device"])
        self.assertTrue(q.rate)

    def test_group_sum_max_quantile(self):
        for agg in ("sum", "max", "avg", "count"):
            req = QueryRequest(
                f'{agg} by (idc) (if_port_in_bytes)',
                10_000, 30_000, 600)
            res = execute(self.engine, req)
            self.assertEqual(res["series_count"], 2, agg)
        req = QueryRequest(
            'quantile(0.95) by (idc) (if_port_in_bytes)', 10_000, 30_000, 600)
        res = execute(self.engine, req)
        self.assertEqual(res["series_count"], 2)
        for s in res["series"]:
            self.assertTrue(s["points"])

    def test_filter_combination(self):
        res = execute(self.engine, QueryRequest(
            'if_port_in_bytes{idc="bj",device="sw1"}', 10_000, 30_000, 300))
        self.assertEqual(res["series_count"], 1)


class TestRetention(unittest.TestCase):
    def test_downsample_and_expiry(self):
        d = tempfile.mkdtemp()
        # raw 保留极短以便验证过期; 5m/1h 保留足够久, 降采样产物不会被同轮清掉
        engine = Engine(d, flush_points=10000, retention={
            "raw": 1000, "5m": 100_000_000, "1h": 100_000_000})
        try:
            t0 = 1_000_000
            for i in range(400):  # 200 分钟, 覆盖多个 5m/1h 桶
                engine.write("if_port_in_bytes", {"d": "a"},
                             t0 + i * 30, 1000 + i * 100)
                engine.write("cpu_usage", {"d": "a"},
                             t0 + i * 30, 50.0 + (i % 7))
            engine.flush()
            raw_before = engine.stats()["tiers"]["raw"]["points"]
            self.assertGreater(raw_before, 0)

            # 用可控时钟推进, 保证桶完整且超过保留期
            fake_now = t0 + 400 * 30 + 400
            rm = RetentionManager(engine, now_func=lambda: fake_now)
            report = rm.run_once()
            self.assertEqual(report["errors"], [])
            produced = report["downsampled"]["raw->5m"]
            self.assertGreater(produced, 0)

            # 5m 层能查到降采样点(带 _resolution 标签)
            sids = engine.index.match("if_port_in_bytes",
                                      {"_resolution": "5m"})
            self.assertTrue(sids)
            sid = next(iter(sids))
            times, values = engine.read_series(sid, 0, fake_now, tiers=["5m"])
            self.assertTrue(times)
            # 计数器聚合值是非负增量
            self.assertTrue(all(v >= 0 for v in values))

            # raw 保留期已过 -> 段被删除/重写, 原始块清空
            self.assertEqual(engine.stats()["tiers"]["raw"]["points"], 0)
            self.assertGreater(engine.stats()["tiers"]["5m"]["points"], 0)

            # 水位线持久化: 再跑一轮不重复产出
            report2 = rm.run_once()
            self.assertEqual(report2["downsampled"]["raw->5m"], 0)
        finally:
            engine.close()
            shutil.rmtree(d)


class TestCrashRecovery(unittest.TestCase):
    def test_half_written_block_truncated(self):
        d = tempfile.mkdtemp()
        engine = Engine(d, flush_points=1000)
        try:
            for i in range(3000):
                engine.write("m", {"d": "a"}, 1000 + i * 30, i)
            engine.flush()
            seg_dir = engine.tier_dirs["raw"]
            seg_files = [f for f in os.listdir(seg_dir) if f.endswith(".seg")]
            self.assertTrue(seg_files)
            path = os.path.join(seg_dir, seg_files[0])
            good_size = os.path.getsize(path)
        finally:
            engine.close()

        # 模拟掉电: 追加一个损坏的“写了一半”的块(部分头 + 垃圾)
        with open(path, "ab") as f:
            f.write(b"PTSB" + b"\x01\x00\x00\x00" + b"\x00" * 20 + b"GARBAGE")
            f.flush()
            os.fsync(f.fileno())
        self.assertGreater(os.path.getsize(path), good_size)

        # 重新打开: 恢复扫描截断坏块, 数据回到最后一个完整块
        engine2 = Engine(d)
        try:
            self.assertTrue(engine2._recovered_truncation)
            self.assertEqual(os.path.getsize(path), good_size)
            sid = next(iter(engine2.select_series("m", {"d": "a"})))
            times, values = engine2.read_series(sid, 0, 10 ** 9)
            self.assertEqual(len(times), 3000)
            self.assertEqual(values[-1], 2999)

            # 恢复后继续写入正常
            engine2.write_batch([("m", {"d": "a"}, 1000 + 3000 * 30, 3000)])
            engine2.flush()
            times, values = engine2.read_series(sid, 0, 10 ** 9)
            self.assertEqual(values[-1], 3000)
        finally:
            engine2.close()
            shutil.rmtree(d)

    def test_corrupt_crc_detected(self):
        d = tempfile.mkdtemp()
        engine = Engine(d, flush_points=1000)
        try:
            for i in range(1500):
                engine.write("m", {"d": "a"}, i * 30, i)
            engine.flush()
            path = os.path.join(engine.tier_dirs["raw"],
                                os.listdir(engine.tier_dirs["raw"])[0])
        finally:
            engine.close()
        # 翻转 payload 中一个字节, CRC 必须报错并触发截断
        with open(path, "r+b") as f:
            f.seek(block_mod.HEADER_SIZE + 5)
            f.write(b"\xff")
        engine2 = Engine(d)
        try:
            # 损坏块之后的内容全部丢弃, 引擎仍可打开
            self.assertTrue(engine2._recovered_truncation)
        finally:
            engine2.close()
            shutil.rmtree(d)


class TestBenchmarkTopology(unittest.TestCase):
    def test_topology_and_simulation(self):
        topo = benchmark.make_topology(devices=10, ports=2)
        # 10 设备 * 2 端口 * 4 指标
        self.assertEqual(len(topo), 80)
        import random
        rng = random.Random(0)
        sim = benchmark.CounterSim(rng, wrap_enabled=True)
        sim.value = (1 << 64) - 10
        nxt = sim.next(rng)
        self.assertLess(nxt, 1 << 64)  # 回绕后变小而不是溢出


if __name__ == "__main__":
    unittest.main(verbosity=2)
