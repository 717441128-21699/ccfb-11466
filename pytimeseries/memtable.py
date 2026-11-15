"""内存表: 每个序列一份按时间戳有序的点表。

乱序到达的点通过二分查找插入正确位置; 同一时间戳重复写入则后写覆盖。
"""

from bisect import bisect_left, insort


class SeriesMem:
    __slots__ = ("times", "values", "bytes_hint", "raw_points")

    def __init__(self):
        self.times = []
        self.values = []
        # 原始点大小估计: 时间戳 8B + 值 8B(与磁盘 raw 统计口径一致)
        self.bytes_hint = 0
        self.raw_points = 0

    def add(self, ts, value):
        idx = bisect_left(self.times, ts)
        if idx < len(self.times) and self.times[idx] == ts:
            # 同一时间戳: 后写覆盖
            self.values[idx] = value
            return
        self.times.insert(idx, ts)
        self.values.insert(idx, value)
        self.bytes_hint += 16
        self.raw_points += 1

    def is_empty(self):
        return not self.times

    def snapshot(self):
        return list(self.times), list(self.values)

    def clear(self):
        self.times.clear()
        self.values.clear()
        self.bytes_hint = 0
        self.raw_points = 0


class Memtable:
    """全部活跃序列的内存表。"""

    def __init__(self):
        self.series = {}          # sid -> SeriesMem
        self.total_points = 0
        self.total_raw_bytes = 0  # 未压缩口径: 每点 16 字节

    def add(self, sid, ts, value):
        sm = self.series.get(sid)
        if sm is None:
            sm = SeriesMem()
            self.series[sid] = sm
        before = sm.raw_points
        sm.add(ts, value)
        delta = sm.raw_points - before
        self.total_points += delta
        self.total_raw_bytes += delta * 16

    def is_empty(self):
        return self.total_points == 0

    def series_ids(self):
        return list(self.series.keys())

    def pop_series(self):
        """取出全部序列快照并清空(刷盘用)。"""
        out = []
        for sid, sm in self.series.items():
            if not sm.is_empty():
                out.append((sid, sm.snapshot()))
        self.series.clear()
        self.total_points = 0
        self.total_raw_bytes = 0
        return out
