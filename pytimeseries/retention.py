"""降采样与保留策略。

数据流:
    raw(7天) --5分钟桶聚合--> 5m(90天) --1小时桶聚合--> 1h(2年)

计数器(单调递增, 如端口流量/错包数):
    桶内取增量之和(速率口径), 自动处理 64 位回绕与设备重启清零;
仪表量(可能上下波动):
    桶内取 avg/max/min/count。
水位线持久化保证任务可重复执行不产生重复数据;
到期数据先完成下一层降采样再删除, 段整体过期直接删文件,
只有部分过期的段读出重写, 崩溃恢复由块 CRC 兜底。
"""

import json
import os
import threading

from . import block as block_mod
from .codec import decode_block, encode_block
from .engine import TIER_ORDER

BUCKET_SECONDS = {"5m": 300, "1h": 3600}
COUNTER_HINTS = ("bytes", "pkts", "packets", "errors", "drops", "octets",
                 "unicast", "broadcast", "discards")


def is_counter(metric):
    m = metric.lower()
    return any(h in m for h in COUNTER_HINTS)


def safe_delta(prev, cur):
    """计数器两点增量, 处理回绕与重启清零。返回 None 表示该跳变不计入。"""
    if cur >= prev:
        return cur - prev
    # 下降: 64 位计数器回绕, 或设备重启清零
    if prev >= (1 << 63):
        # 高值后跳小: 按无符号 64 位回绕处理
        return cur + (1 << 64) - prev
    # 低值计数器发生下降 -> 重启清零, 无法知道重启期间增量, 丢弃该跳变
    return None


def _bucket_floor(ts, step):
    return (ts // step) * step


def _aggregate(metric, points, step):
    """把一个序列的点按 step 秒分桶聚合。

    返回 [(bucket_ts, value), ...]。
    计数器: 桶内安全增量之和; 仪表量: 桶内平均值。
    """
    counter = is_counter(metric)
    buckets = {}
    order = []
    prev_t = None
    prev_v = None
    for t, v in points:
        b = _bucket_floor(t, step)
        if b not in buckets:
            buckets[b] = {"sum_delta": 0, "g_sum": 0.0, "g_n": 0,
                          "max": v, "min": v, "count": 0}
            order.append(b)
        agg = buckets[b]
        agg["count"] += 1
        agg["max"] = max(agg["max"], v)
        agg["min"] = min(agg["min"], v)
        if counter:
            agg["g_sum"] += float(v)
            agg["g_n"] += 1
            if prev_t is not None and prev_t < t:
                d = safe_delta(prev_v, v)
                if d is not None:
                    agg["sum_delta"] += d
        else:
            agg["g_sum"] += float(v)
            agg["g_n"] += 1
        prev_t, prev_v = t, v

    out = []
    for b in order:
        agg = buckets[b]
        if counter:
            out.append((b, agg["sum_delta"]))
        else:
            out.append((b, agg["g_sum"] / agg["g_n"] if agg["g_n"] else 0.0))
    return out


class RetentionManager:
    def __init__(self, engine, now_func=None):
        self.engine = engine
        self.now_func = now_func or (lambda: _real_now())
        self.watermark_path = os.path.join(engine.data_dir, "downsample.json")
        self.watermarks = self._load_watermarks()
        self._lock = threading.RLock()
        self.last_run = None
        self.last_report = None

    def _load_watermarks(self):
        if os.path.exists(self.watermark_path):
            with open(self.watermark_path, "r", encoding="utf-8") as f:
                return json.load(f)
        # 每个源层记录“已经处理到哪个桶边界”
        return {"raw->5m": 0, "5m->1h": 0}

    def _save_watermarks(self):
        tmp = self.watermark_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.watermarks, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.watermark_path)

    # ---------------- 一轮完整维护 ----------------
    def run_once(self):
        """流水线: 每层先完成向下一层的降采样, 再做本层过期删除。

        顺序保证被删除的原始点已经聚合进下一层, 且新生成的降采样点
        不会被同轮的过期逻辑误删(各层用各自的保留期)。
        """
        with self._lock:
            report = {"downsampled": {}, "expired_segments": [],
                      "rewritten_segments": [], "errors": []}
            now = self.now_func()
            try:
                # 1) raw -> 5m, 然后只过期 raw 层
                self._downsample("raw", "5m", 300, now, report)
                self._expire_tier("raw", now, report)
                # 2) 5m -> 1h, 然后只过期 5m 层
                self._downsample("5m", "1h", 3600, now, report)
                self._expire_tier("5m", now, report)
                # 3) 1h 是最底层, 只做过期
                self._expire_tier("1h", now, report)
            except Exception:
                import traceback
                report["errors"].append(traceback.format_exc())
            self.last_run = now
            self.last_report = report
            return report

    def _source_series(self, tier):
        """列出源层中拥有块的全部 sid。"""
        return list(self.engine.block_index[tier].keys())

    def _downsample(self, src_tier, dst_tier, step, now, report):
        wm_key = f"{src_tier}->{dst_tier}"
        # 只处理已经完整结束的桶, 避免与实时写入竞争
        safe_cutoff = _bucket_floor(now, step) - step
        wm = self.watermarks.get(wm_key, 0)
        if safe_cutoff <= wm:
            report["downsampled"][wm_key] = 0
            return

        produced = 0
        pending = []  # (sid, [(t, v), ...])
        for sid in self._source_series(src_tier):
            meta = self.engine.index.series_meta(sid)
            if meta is None:
                continue
            metric, labels = meta
            times, values = self.engine.read_series(sid, wm, safe_cutoff,
                                                    tiers=[src_tier])
            if not times:
                continue
            agg = _aggregate(metric, list(zip(times, values)), step)
            agg = [(t, v) for t, v in agg if wm <= t < safe_cutoff]
            if not agg:
                continue
            # 降采样点作为新序列写入, 标签带上粒度与聚合方式
            ds_labels = dict(labels)
            ds_labels["_resolution"] = dst_tier
            ds_sid, _ = self.engine.index.get_or_create(metric, ds_labels)
            pending.append((ds_sid, ([t for t, _ in agg], [v for _, v in agg])))
            produced += len(agg)

        if pending:
            self.engine.write_downsampled(dst_tier, pending)
        self.watermarks[wm_key] = safe_cutoff
        self._save_watermarks()
        report["downsampled"][wm_key] = produced

    # ---------------- 过期删除(单层) ----------------
    def _expire_tier(self, tier, now, report):
        retention = self.engine.retention[tier]
        cutoff = now - retention
        directory = self.engine.tier_dirs[tier]

        # 逐 segment 判定: 整段过期删文件; 部分过期按点过滤后重写
        seg_ids = sorted({m.segment
                          for metas in self.engine.block_index[tier].values()
                          for m in metas})
        deleted = set()
        for seg_id in seg_ids:
            seg_metas = [m for metas in self.engine.block_index[tier].values()
                         for m in metas if m.segment == seg_id]
            if all(m.t1 < cutoff for m in seg_metas):
                self.engine.evict_writer(tier, seg_id)
                path = os.path.join(directory, f"{seg_id:010d}.seg")
                if os.path.exists(path):
                    os.remove(path)
                report["expired_segments"].append(f"{tier}/{seg_id}")
                deleted.add(seg_id)
                continue

            if all(m.t0 >= cutoff for m in seg_metas):
                # 整段都在保留期内, 不动文件
                continue

            # 部分块/块内部分点过期: 读出过滤后重写到新 segment
            kept = []
            for m in seg_metas:
                if m.t1 < cutoff:
                    continue
                payload = block_mod.read_block(directory, m)
                times, values = decode_block(payload)
                keep = [(t, v) for t, v in zip(times, values) if t >= cutoff]
                if keep:
                    kept.append((m.sid,
                                 [t for t, _ in keep],
                                 [v for _, v in keep]))
            if kept:
                data = [(sid, (times, values)) for sid, times, values in kept]
                done = threading.Event()
                self.engine._flush_queue.put(("blocks", (tier, data)))
                self.engine._flush_queue.put(("barrier", done))
                done.wait()
            self.engine.evict_writer(tier, seg_id)
            path = os.path.join(directory, f"{seg_id:010d}.seg")
            if os.path.exists(path):
                os.remove(path)
            report["rewritten_segments"].append(f"{tier}/{seg_id}")
            deleted.add(seg_id)

        # 只有确实删除/重写了 segment 才需要重建索引;
        # 否则保持原样(避免误删刚由降采样写入的新块索引)
        if not deleted:
            return
        merged = {}
        for sid, metas in self.engine.block_index[tier].items():
            kept_metas = [m for m in metas if m.segment not in deleted]
            if kept_metas:
                kept_metas.sort(key=lambda m: (m.t0, m.segment, m.offset))
                merged[sid] = kept_metas
        self.engine.block_index[tier] = merged


def _real_now():
    import time
    return int(time.time())


class MaintenanceThread(threading.Thread):
    """后台周期执行降采样与过期清理。"""

    def __init__(self, retention_manager, interval=30):
        super().__init__(name="pts-retention", daemon=True)
        self.rm = retention_manager
        self.interval = interval
        self._stop = threading.Event()

    def run(self):
        while not self._stop.wait(self.interval):
            try:
                self.rm.run_once()
            except Exception:
                import traceback
                traceback.print_exc()

    def stop(self):
        self._stop.set()
