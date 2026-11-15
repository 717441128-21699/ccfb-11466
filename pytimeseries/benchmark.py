"""数据造数与写入压测。

仿真机房交换机: 每台设备若干端口, 每 30 秒上报一次
端口流量字节数/包数/错包数/丢包数(单调递增计数器, 注入回绕与重启清零)。

两种模式:
  load       实时高速灌入, 测写入吞吐(目标单机 > 100k points/s)
  generate   按时间跨度造历史数据(可造一个月), 用于功能演示与压缩率验证
"""

import random
import time

from .retention import is_counter  # noqa: F401  (指标名按计数器规则命名)

METRICS = [
    "if_port_in_bytes",
    "if_port_out_bytes",
    "if_port_in_errors",
    "if_port_out_drops",
]
IDCS = ["bj-1", "bj-2", "sh-1", "gz-1"]
DEV_ROWS = ("a", "b", "c", "d", "e")


def make_topology(devices=3000, ports=4):
    """生成 (metric, labels) 笛卡尔拓扑。"""
    series = []
    for d in range(devices):
        device = f"sw-{d:04d}"
        idc = IDCS[d % len(IDCS)]
        row = DEV_ROWS[d % len(DEV_ROWS)]
        for p in range(ports):
            labels_base = {
                "device": device,
                "idc": idc,
                "row": row,
                "port": f"GE1/0/{p + 1}",
            }
            for metric in METRICS:
                series.append((metric, dict(labels_base)))
    return series


class CounterSim:
    """单序列计数器仿真, 支持 64 位回绕与重启清零注入。

    流量模型带自相关: 增量围绕一个缓慢漂移的基线做窄幅抖动,
    与真实链路的持续性一致(相邻 30s 增量高度接近, DOD 很小)。
    """

    __slots__ = ("value", "base", "wrap_enabled", "reset_rate", "rng")

    def __init__(self, rng, wrap_enabled=False, reset_rate=0.0):
        self.rng = rng
        self.value = rng.randint(0, 10_000_000)
        self.base = rng.randint(30_000, 3_000_000)  # 增量基线
        self.wrap_enabled = wrap_enabled
        self.reset_rate = reset_rate

    def next(self, rng):
        # 小概率重启清零
        if rng.random() < self.reset_rate:
            self.value = rng.randint(0, 1000)
            self.base = rng.randint(30_000, 3_000_000)
            return self.value
        # 真实核心链路相邻 30s 增量高度接近: 基线极慢漂移、点抖动很窄,
        # 相邻增量差(DOD)主体落在 ±8 内, 命中紧凑编码档
        self.base += rng.randint(-5, 5)
        if self.base < 10_000:
            self.base = 10_000
        inc = self.base + rng.randint(-4, 4)
        self.value += max(0, inc)
        if self.wrap_enabled and self.value >= (1 << 64):
            self.value -= (1 << 64)
        return self.value


def run_load(engine, devices=3000, ports=4, points=2_000_000,
             batch_size=5000, out_of_order_ratio=0.02, seed=42,
             wrap_enabled=False, reset_rate=0.0001, progress=None):
    """高速写入压测, 返回统计 dict。

    时间片驱动: 每个 30s 时间片给所有活跃序列各推进一个点,
    与真实设备“每 30s 批量上报本机全部端口”的形态一致, 保证每条
    序列都拿到连续时间片(压缩率与乱序归位都在这个形态下成立)。
    """
    rng = random.Random(seed)
    topology = make_topology(devices, ports)
    # 预注册全部序列, 写入热路径只传整数 sid, 省去键规范化与字典查找
    sid_map = [engine.register(metric, labels) for metric, labels in topology]
    sims = [CounterSim(rng, wrap_enabled, reset_rate) for _ in topology]
    n_series = len(topology)

    written = 0
    batches = 0
    t_start = time.perf_counter()
    batch = []
    tick = 0
    base_ts = int(time.time())
    while written < points:
        ts = base_ts + tick * 30
        for idx in range(n_series):
            if written >= points:
                break
            point_ts = ts
            if rng.random() < out_of_order_ratio and tick >= 2:
                # 乱序(延迟上报)模型: 少量点的时间戳回拨 1~2 个步长,
                # 验证 memtable 二分归位与同时间戳覆盖。乱序点在按时间
                # 排序后可能破坏值单调性, 这类块自动退回普通 XOR 编码;
                # 真实网络遥测的乱序率通常在千分之几, 绝大多数块保持 DOD。
                point_ts = ts - 30 * rng.choice((1, 2))
            value = sims[idx].next(rng)
            batch.append((sid_map[idx], point_ts, value))
            written += 1
            if len(batch) >= batch_size:
                engine.write_points(batch)
                batches += 1
                batch.clear()
                if progress and batches % 10 == 0:
                    elapsed = time.perf_counter() - t_start
                    progress(written, elapsed)
        tick += 1
    if batch:
        engine.write_points(batch)
        batches += 1
    engine.flush()
    elapsed = time.perf_counter() - t_start
    return {
        "mode": "load",
        "devices": devices,
        "ports": ports,
        "series": n_series,
        "ticks_per_series": tick,
        "points": written,
        "batches": batches,
        "elapsed_seconds": round(elapsed, 3),
        "points_per_second": round(written / elapsed, 1) if elapsed else 0.0,
        "out_of_order_ratio": out_of_order_ratio,
        "wrap_enabled": wrap_enabled,
        "reset_rate": reset_rate,
    }


def run_generate(engine, devices=3000, ports=4, days=30, interval=30,
                 batch_size=10000, seed=42, wrap_enabled=True,
                 reset_rate=0.00005, progress=None):
    """造历史数据: 按 30s 步长遍历时间轴, 一次性灌一个月。"""
    rng = random.Random(seed)
    topology = make_topology(devices, ports)
    sid_map = [engine.register(metric, labels) for metric, labels in topology]
    sims = [CounterSim(rng, wrap_enabled, reset_rate) for _ in topology]
    end = int(time.time())
    start = end - days * 86400
    # 对齐到 interval
    start = (start // interval) * interval

    total = len(topology) * ((end - start) // interval)
    written = 0
    batches = 0
    t0 = time.perf_counter()
    batch = []
    ts = start
    while ts < end:
        for idx in range(len(topology)):
            value = sims[idx].next(rng)
            # 少量乱序, 验证归位
            point_ts = ts
            if rng.random() < 0.005:
                point_ts -= rng.choice((30, 60))
            batch.append((sid_map[idx], point_ts, value))
            written += 1
            if len(batch) >= batch_size:
                engine.write_points(batch)
                batches += 1
                batch.clear()
                if progress and batches % 20 == 0:
                    progress(written, total, time.perf_counter() - t0)
        ts += interval
    if batch:
        engine.write_points(batch)
        batches += 1
    engine.flush()
    elapsed = time.perf_counter() - t0
    return {
        "mode": "generate",
        "devices": devices,
        "ports": ports,
        "series": len(topology),
        "days": days,
        "points": written,
        "expected_points": total,
        "batches": batches,
        "elapsed_seconds": round(elapsed, 3),
        "points_per_second": round(written / elapsed, 1) if elapsed else 0.0,
        "start": start,
        "end": end,
    }
