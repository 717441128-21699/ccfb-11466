"""命令行入口。

用法:
  py -m pytimeseries serve  --data ./data --port 8000
  py -m pytimeseries bench  --data ./data --points 2000000
  py -m pytimeseries generate --data ./data --devices 3000 --days 30
  py -m pytimeseries query  --data ./data --q 'sum by (idc)(rate(if_port_in_bytes))'
  py -m pytimeseries downsample --data ./data
  py -m pytimeseries stats  --data ./data
  py -m pytimeseries test-compression --data ./data
"""

import argparse
import json
import sys
import time

from .engine import Engine
from .query import QueryRequest, execute
from .retention import MaintenanceThread, RetentionManager
from .web import WebServer


def _print_stats(engine):
    s = engine.stats()
    print(json.dumps(s, indent=2, ensure_ascii=False))
    print(f"\n压缩率: {s['compression_ratio']:.4f} "
          f"(压缩后/原始, 要求 < 0.10), 压缩倍数: {s['compression_x']:.1f}x")


def cmd_serve(args):
    engine = Engine(args.data)
    rm = RetentionManager(engine)
    maint = MaintenanceThread(rm, interval=args.interval)
    maint.start()
    server = WebServer(engine, host=args.host, port=args.port,
                       retention_manager=rm)
    url = server.start()
    print(f"PyTimeSeries 已启动: {url}")
    print("页面: 指标浏览 / 查询编辑 / 降采样策略 / 存储状态 / 写入压测")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n正在关闭...")
    finally:
        maint.stop()
        server.stop()
        engine.close()


def cmd_bench(args):
    engine = Engine(args.data)
    from . import benchmark
    print(f"开始写入压测: 设备={args.devices} 端口={args.ports} "
          f"点数={args.points} 批量={args.batch_size}")
    result = benchmark.run_load(
        engine, devices=args.devices, ports=args.ports, points=args.points,
        batch_size=args.batch_size,
        out_of_order_ratio=args.out_of_order,
        wrap_enabled=args.wrap, reset_rate=args.reset_rate,
        progress=lambda w, e: print(
            f"\r  已写入 {w:>10,} 点, 当前 {w / e:,.0f} pts/s", end=""),
    )
    print()
    engine.flush()
    _print_stats(engine)
    print("\n压测结果:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    target = 100_000
    ok = result["points_per_second"] >= target
    print(f"\n目标 {target:,} pts/s —— "
          f"{'达标 ✔' if ok else '未达标 ✘ (可增大批量/减少 fsync 频率调优)'}")
    engine.close()


def cmd_generate(args):
    engine = Engine(args.data)
    from . import benchmark
    print(f"造数: 设备={args.devices} 端口={args.ports} 天数={args.days}")
    result = benchmark.run_generate(
        engine, devices=args.devices, ports=args.ports, days=args.days,
        batch_size=args.batch_size, wrap_enabled=args.wrap,
        reset_rate=args.reset_rate,
        progress=lambda w, total, e: print(
            f"\r  进度 {w:>12,}/{total:,} ({w / total:.1%}) "
            f"{w / e:,.0f} pts/s", end=""),
    )
    print()
    engine.flush()
    _print_stats(engine)
    print("\n造数结果:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    engine.close()


def cmd_query(args):
    engine = Engine(args.data)
    end = int(time.time())
    start = end - args.range_seconds
    req = QueryRequest(args.q, start, end, step=args.step)
    result = execute(engine, req)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    engine.close()


def cmd_downsample(args):
    engine = Engine(args.data)
    rm = RetentionManager(engine)
    report = rm.run_once()
    print(json.dumps(report, indent=2, ensure_ascii=False))
    engine.close()


def cmd_stats(args):
    engine = Engine(args.data)
    _print_stats(engine)
    engine.close()


def cmd_test_compression(args):
    """独立压缩验证: 编码-解码往返 + 压缩率。"""
    from .codec import decode_block, encode_block
    import random
    rng = random.Random(7)
    n = 100_000
    times = [1_700_000_000 + i * 30 for i in range(n)]
    # 自相关流量: 增量围绕慢漂移基线窄幅抖动, 与压测仿真同一口径
    values = []
    v = 5_000_000
    base = 1_000_000
    for _ in range(n):
        base += rng.randint(-5, 5)
        base = max(10_000, base)
        v += base + rng.randint(-4, 4)
        values.append(v)
    payload, kind = encode_block(times, values)
    decoded_t, decoded_v = decode_block(payload)
    assert decoded_t == times, "时间戳往返不一致"
    assert decoded_v == values, "数值往返不一致"
    raw = n * 16
    print(f"点数: {n:,}  原始: {raw:,} B  压缩后: {len(payload):,} B")
    print(f"压缩率: {len(payload) / raw:.4f} (要求 < 0.10)  "
          f"压缩倍数: {raw / len(payload):.1f}x  往返校验: 通过 ✔")


def build_parser():
    p = argparse.ArgumentParser(prog="pytimeseries",
                                description="Python 单机时序数据库")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="启动 Web 服务")
    s.add_argument("--data", default="./data")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8000)
    s.add_argument("--interval", type=int, default=30,
                   help="降采样/保留策略后台执行间隔(秒)")
    s.set_defaults(func=cmd_serve)

    b = sub.add_parser("bench", help="写入压测")
    b.add_argument("--data", default="./data")
    b.add_argument("--devices", type=int, default=3000)
    b.add_argument("--ports", type=int, default=4)
    b.add_argument("--points", type=int, default=2_000_000)
    b.add_argument("--batch-size", type=int, default=5000)
    b.add_argument("--out-of-order", type=float, default=0.02)
    b.add_argument("--wrap", action="store_true")
    b.add_argument("--reset-rate", type=float, default=0.0001)
    b.set_defaults(func=cmd_bench)

    g = sub.add_parser("generate", help="造历史数据")
    g.add_argument("--data", default="./data")
    g.add_argument("--devices", type=int, default=3000)
    g.add_argument("--ports", type=int, default=4)
    g.add_argument("--days", type=float, default=30)
    g.add_argument("--batch-size", type=int, default=10000)
    g.add_argument("--wrap", action="store_true")
    g.add_argument("--reset-rate", type=float, default=0.00005)
    g.set_defaults(func=cmd_generate)

    q = sub.add_parser("query", help="执行查询并打印 JSON")
    q.add_argument("--data", default="./data")
    q.add_argument("--q", required=True)
    q.add_argument("--range-seconds", type=int, default=3600)
    q.add_argument("--step", type=int, default=300)
    q.set_defaults(func=cmd_query)

    d = sub.add_parser("downsample", help="执行一轮降采样与过期清理")
    d.add_argument("--data", default="./data")
    d.set_defaults(func=cmd_downsample)

    st = sub.add_parser("stats", help="存储状态")
    st.add_argument("--data", default="./data")
    st.set_defaults(func=cmd_stats)

    tc = sub.add_parser("test-compression", help="压缩算法往返与压缩率验证")
    tc.add_argument("--data", default="./data")
    tc.set_defaults(func=cmd_test_compression)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv if argv is not None else sys.argv[1:])
    args.func(args)


if __name__ == "__main__":
    main()
