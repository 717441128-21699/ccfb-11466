"""存储引擎。

职责:
  * 批量写入, 乱序点在 memtable 中归位
  * memtable 达阈值后由后台线程刷成块文件(每块 fsync)
  * 启动时扫描 segment 恢复块索引, 截断写了一半的块
  * 三层保留: raw(7d) / 5m(90d) / 1h(2y), 各自独立目录
  * 块索引 + 标签倒排支撑查询
"""

import json
import os
import queue
import threading

from . import block as block_mod
from .codec import encode_block
from .index import SeriesIndex
from .memtable import Memtable

# 保留层: 名字 -> (目录名, 保留秒数)
TIERS = {
    "raw": ("raw", 7 * 86400),
    "5m": ("5m", 90 * 86400),
    "1h": ("1h", 2 * 365 * 86400),
}
TIER_ORDER = ["raw", "5m", "1h"]
DEFAULT_SEGMENT_SIZE = 32 * 1024 * 1024  # 32MB 轮转
DEFAULT_FLUSH_POINTS = 200_000           # memtable 点数阈值, 批量落盘摊薄 fsync
TARGET_BLOCK_POINTS = 512                # 单块目标点数; 真实长序列形成满块,
                                         # 短序列尾巴在 flush/close 时落盘
RAW_POINT_BYTES = 16                     # 压缩前口径: ts 8B + value 8B


class Engine:
    def __init__(self, data_dir,
                 flush_points=DEFAULT_FLUSH_POINTS,
                 segment_size=DEFAULT_SEGMENT_SIZE,
                 target_block_points=TARGET_BLOCK_POINTS,
                 retention=None):
        self.data_dir = data_dir
        self.flush_points = flush_points
        self.segment_size = segment_size
        self.target_block_points = target_block_points
        self.retention = dict(retention or {k: v[1] for k, v in TIERS.items()})

        os.makedirs(data_dir, exist_ok=True)
        self.tier_dirs = {}
        for tier, (dirname, _default) in TIERS.items():
            d = os.path.join(data_dir, dirname)
            os.makedirs(d, exist_ok=True)
            self.tier_dirs[tier] = d

        self.index = SeriesIndex(os.path.join(data_dir, "series.log"))
        self.index.load()

        # 块索引: tier -> sid -> list[BlockMeta]
        self.block_index = {tier: {} for tier in TIER_ORDER}
        self._seg_writers = {}     # tier -> SegmentWriter
        self._next_seg = {tier: 1 for tier in TIER_ORDER}
        self._recover()

        self.memtable = Memtable()
        # 每序列的持久化缓冲(已刷出 memtable 但尚未凑成整块的点),
        # 避免每序列每轮只产生几个点的碎块
        self._pending = {}          # sid -> [times_list, values_list]
        self._pending_points = 0
        self._mem_lock = threading.RLock()
        self._flush_lock = threading.RLock()

        # 后台刷盘
        self._flush_queue = queue.Queue()
        self._stopping = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._flush_worker, name="pts-flush", daemon=True)
        self._flush_thread.start()

        # 统计
        self.total_written = 0
        self.total_compressed_bytes = 0
        self.total_raw_bytes = 0
        self.dropped_wraps = 0
        self.stats_path = os.path.join(data_dir, "stats.json")
        self._load_stats()

    # ---------------- 启动恢复 ----------------
    def _recover(self):
        for tier, directory in self.tier_dirs.items():
            truncated_any = False
            for seg_id in block_mod.list_segments(directory):
                path = os.path.join(directory, f"{seg_id:010d}.seg")
                metas, good, truncated = block_mod.recover_segment(path)
                truncated_any |= truncated
                for m in metas:
                    self.block_index[tier].setdefault(m.sid, []).append(m)
                if seg_id >= self._next_seg[tier]:
                    self._next_seg[tier] = seg_id + 1
            for sid in self.block_index[tier]:
                self.block_index[tier][sid].sort(key=lambda m: (m.t0, m.segment, m.offset))
            if truncated_any:
                # 恢复日志在调用方打印
                self._recovered_truncation = True
        self._recovered_truncation = getattr(self, "_recovered_truncation", False)

    # ---------------- 统计持久化 ----------------
    def _load_stats(self):
        if os.path.exists(self.stats_path):
            try:
                with open(self.stats_path, "r", encoding="utf-8") as f:
                    rec = json.load(f)
                self.total_written = rec.get("total_written", 0)
            except (json.JSONDecodeError, OSError):
                self.total_written = 0

    def _save_stats(self):
        # 压缩/原始字节直接从块索引重算, 与磁盘文件保持一致
        rec = {"total_written": self.total_written}
        tmp = self.stats_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rec, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.stats_path)

    def _stored_bytes(self):
        """遍历块索引统计各层实际 (原始字节, 压缩字节)。"""
        raw = comp = 0
        for tier in TIER_ORDER:
            for metas in self.block_index[tier].values():
                for m in metas:
                    raw += m.count * RAW_POINT_BYTES
                    comp += m.length
        return raw, comp

    # ---------------- segment 管理 ----------------
    def _get_writer(self, tier):
        w = self._seg_writers.get(tier)
        if w is None or w.size >= self.segment_size:
            if w is not None:
                w.close()
            seg_id = self._next_seg[tier]
            self._next_seg[tier] += 1
            w = block_mod.SegmentWriter(self.tier_dirs[tier], seg_id)
            self._seg_writers[tier] = w
        return w

    def evict_writer(self, tier, seg_id):
        """删除 segment 前调用: 若它持有打开的写句柄则先关闭(Windows 必需)。"""
        w = self._seg_writers.get(tier)
        if w is not None and w.seg_id == seg_id:
            w.close()
            del self._seg_writers[tier]

    # ---------------- 写入 ----------------
    def register(self, metric, labels):
        """预注册序列, 返回 sid。批量写入前先注册可省去每点的键规范化。"""
        sid, _ = self.index.get_or_create(metric, labels)
        return sid

    def write_points(self, points):
        """快速路径: points 为 [(sid, timestamp, value), ...]。"""
        n = 0
        with self._mem_lock:
            for sid, ts, value in points:
                self.memtable.add(sid, int(ts), value)
                n += 1
            self.total_written += n
            if self.memtable.total_points >= self.flush_points:
                self._request_flush()
        return n

    def write_batch(self, points):
        """批量写入。points: [(metric, labels_dict, timestamp, value), ...]"""
        n = 0
        with self._mem_lock:
            for metric, labels, ts, value in points:
                sid, _created = self.index.get_or_create(metric, labels)
                self.memtable.add(sid, int(ts), value)
                n += 1
            self.total_written += n
            if self.memtable.total_points >= self.flush_points:
                self._request_flush()
        return n

    def write(self, metric, labels, ts, value):
        return self.write_batch([(metric, labels, ts, value)])

    def _request_flush(self):
        # 调用方持有 _mem_lock; 换出内存表, 交给后台线程编码落盘
        old = self.memtable
        self.memtable = Memtable()
        self._flush_queue.put(("mem", old))

    def flush(self):
        """手动触发一次刷盘并等待完成(含 memtable 与 pending 尾巴)。"""
        with self._mem_lock:
            if not self.memtable.is_empty():
                self._request_flush()
        # 再排一个空 mem 触发 pending 排空
        drain = object()
        self._flush_queue.put(("drain", drain))
        done = threading.Event()
        self._flush_queue.put(("barrier", done))
        done.wait()

    def _flush_worker(self):
        while not self._stopping.is_set():
            try:
                kind, payload = self._flush_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if kind == "mem":
                    self._flush_memtable(payload)
                elif kind == "drain":
                    self._drain_pending()
                elif kind == "blocks":
                    # 降采样直接落盘: (tier, [(sid, times, values), ...])
                    tier, series_points = payload
                    self._write_series_blocks(tier, series_points)
                elif kind == "barrier":
                    payload.set()
            except Exception as exc:  # 后台线程不能死
                import traceback
                traceback.print_exc()
                if kind == "barrier":
                    payload.set()

    def _flush_memtable(self, mem):
        raw_bytes = mem.total_raw_bytes
        data = mem.pop_series()
        # 并入每序列持久化缓冲, 凑够目标块大小再编码, 避免碎块
        for sid, (times, values) in data:
            buf = self._pending.get(sid)
            if buf is None:
                buf = [[], []]
                self._pending[sid] = buf
            self._merge_into(buf, times, values)
            self._pending_points += len(times)

        ready = []
        for sid, buf in list(self._pending.items()):
            if len(buf[0]) >= self.target_block_points:
                ready.append((sid, (list(buf[0]), list(buf[1]))))
                self._pending_points -= len(buf[0])
                del self._pending[sid]
        with self._flush_lock:
            self._write_series_blocks("raw", ready, sync=False)
            # 一次 flush 的所有块统一 fsync, 摊薄 sync 开销
            self._sync_all_writers()
        # 序列日志与数据块一起落盘, 保证重启后索引可重放
        self.index.sync()
        self._save_stats()
        # raw 字节按实际进入 flush 的点数统计(与 memtable 口径一致)
        self.total_raw_bytes += raw_bytes

    @staticmethod
    def _merge_into(buf, times, values):
        """把有序的 (times, values) 归并进 buf, 同时间戳以新值覆盖。"""
        bt, bv = buf
        if not bt:
            bt.extend(times)
            bv.extend(values)
            return
        i = j = 0
        nt, nv = [], []
        while i < len(bt) and j < len(times):
            if bt[i] < times[j]:
                nt.append(bt[i]); nv.append(bv[i]); i += 1
            elif bt[i] > times[j]:
                nt.append(times[j]); nv.append(values[j]); j += 1
            else:
                nt.append(times[j]); nv.append(values[j]); i += 1; j += 1
        nt.extend(bt[i:]); nv.extend(bv[i:])
        nt.extend(times[j:]); nv.extend(values[j:])
        buf[0] = nt
        buf[1] = nv

    def _drain_pending(self):
        """把所有未凑成整块的 pending 点强制落盘(关闭/手动 flush 时用)。"""
        if not self._pending:
            return
        ready = []
        for sid, buf in list(self._pending.items()):
            if buf[0]:
                ready.append((sid, (list(buf[0]), list(buf[1]))))
        self._pending.clear()
        self._pending_points = 0
        with self._flush_lock:
            self._write_series_blocks("raw", ready, sync=False)
            self._sync_all_writers()
        self.index.sync()
        self._save_stats()

    @staticmethod
    def _sort_dedup(times, values):
        """按时间戳排序并去重(同时间戳后写覆盖), 满足块编码严格递增前置条件。

        乱序/回拨的迟到点在这里归位, 保证 DOD/delta 只沿时间正向计算,
        不会被乱序插入打断而产生巨大差分。
        """
        if len(times) <= 1:
            return times, values
        order = sorted(range(len(times)), key=lambda i: times[i])
        nt, nv, last_ts = [], [], None
        for i in order:
            if times[i] == last_ts:
                nv[-1] = values[i]  # 同时间戳以靠后的点覆盖
                continue
            nt.append(times[i])
            nv.append(values[i])
            last_ts = times[i]
        return nt, nv

    def _write_series_blocks(self, tier, data, sync=True):
        """把 [(sid, (times, values)), ...] 编码成块写入指定层。

        sync=False 时只写入页缓存不 fsync, 由调用方在批量写完后统一 sync。
        """
        comp_bytes = 0
        touched = []
        for sid, (times, values) in data:
            if not times:
                continue
            times, values = self._sort_dedup(times, values)
            if not times:
                continue
            payload, kind = encode_block(times, values)
            writer = self._get_writer(tier)
            meta = writer.append(sid, times, payload, kind, sync=sync)
            touched.append(writer)
            self.block_index[tier].setdefault(sid, []).append(meta)
            comp_bytes += meta.length
        if sync:
            for w in set(touched):
                w.sync()
        # 块索引保持时间有序(乱序刷盘也能正确归位)
        for sid, _ in data:
            lst = self.block_index[tier].get(sid)
            if lst:
                lst.sort(key=lambda m: (m.t0, m.segment, m.offset))
        self.total_compressed_bytes += comp_bytes

    def _sync_all_writers(self):
        for w in set(self._seg_writers.values()):
            w.sync()

    def write_downsampled(self, tier, series_points):
        """降采样线程使用: 直接把聚合点写到 5m/1h 层(不经过 memtable)。"""
        done = threading.Event()
        self._flush_queue.put(("blocks", (tier, series_points)))
        self._flush_queue.put(("barrier", done))
        done.wait()

    # ---------------- 读取 ----------------
    def _read_block(self, tier, meta):
        from .codec import decode_block
        payload = block_mod.read_block(self.tier_dirs[tier], meta)
        return decode_block(payload)

    def read_series(self, sid, start, end, tiers=None):
        """读一个序列在 [start, end] 内的全部点, 跨层/跨块合并去重。

        同时间戳以 (tier 优先级, 块位置) 靠后者覆盖: raw 优先于降采样层。
        返回 (times, values), 按时间升序。
        """
        tiers = tiers or TIER_ORDER
        merged = {}
        for tier in reversed(tiers):  # 先放低优先级层, raw 最后覆盖
            metas = self.block_index.get(tier, {}).get(sid, ())
            for m in metas:
                if m.t1 < start or m.t0 > end:
                    continue
                times, values = self._read_block(tier, m)
                for t, v in zip(times, values):
                    if start <= t <= end:
                        merged[t] = v
        # pending 缓冲(已出 memtable 未成块)与 memtable 都是最新数据, 最后覆盖
        with self._mem_lock:
            buf = self._pending.get(sid)
            if buf:
                for t, v in zip(buf[0], buf[1]):
                    if start <= t <= end:
                        merged[t] = v
            sm = self.memtable.series.get(sid)
            if sm:
                for t, v in zip(sm.times, sm.values):
                    if start <= t <= end:
                        merged[t] = v
        if not merged:
            return [], []
        ts = sorted(merged)
        return ts, [merged[t] for t in ts]

    def select_series(self, metric=None, filters=None):
        return self.index.match(metric=metric, filters=filters)

    # ---------------- 存储状态 ----------------
    def disk_bytes(self):
        total = 0
        for directory in self.tier_dirs.values():
            for name in os.listdir(directory):
                if name.endswith(".seg"):
                    total += os.path.getsize(os.path.join(directory, name))
        # 序列日志也算存储占用
        log = os.path.join(self.data_dir, "series.log")
        if os.path.exists(log):
            total += os.path.getsize(log)
        return total

    def stats(self):
        block_counts = {tier: 0 for tier in TIER_ORDER}
        point_counts = {tier: 0 for tier in TIER_ORDER}
        for tier in TIER_ORDER:
            for metas in self.block_index[tier].values():
                block_counts[tier] += len(metas)
                point_counts[tier] += sum(m.count for m in metas)
        mem_points = self.memtable.total_points
        mem_raw = self.memtable.total_raw_bytes
        pending_raw = self._pending_points * RAW_POINT_BYTES
        # 已落盘部分从块索引重算(过期删除后仍准确), 再加内存中的点
        stored_raw, stored_comp = self._stored_bytes()
        raw_total = stored_raw + pending_raw + mem_raw
        # 内存点按已观测到的平均压缩率估算, 落盘后即为精确值
        comp_total = stored_comp
        ratio = (comp_total / stored_raw) if stored_raw else 0.0
        return {
            "data_dir": os.path.abspath(self.data_dir),
            "index": self.index.stats(),
            "tiers": {
                tier: {
                    "blocks": block_counts[tier],
                    "points": point_counts[tier],
                    "retention_seconds": self.retention[tier],
                    "segment_files": len(block_mod.list_segments(self.tier_dirs[tier])),
                }
                for tier in TIER_ORDER
            },
            "memtable_points": mem_points,
            "pending_points": self._pending_points,
            "total_written": self.total_written,
            "total_raw_bytes": raw_total,
            "total_compressed_bytes": comp_total,
            "disk_bytes": self.disk_bytes(),
            "compression_ratio": ratio,           # 压缩后/原始, 目标 < 0.1
            "compression_x": (1 / ratio) if ratio else 0.0,
        }

    # ---------------- 关闭 ----------------
    def close(self):
        self.flush()
        self._stopping.set()
        self._flush_thread.join(timeout=10)
        for w in self._seg_writers.values():
            w.close()
        self._seg_writers.clear()
        self._save_stats()
        self.index.close()
