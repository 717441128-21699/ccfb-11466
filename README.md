# PyTimeSeries — 单机时序数据库(纯 Python 标准库)

面向机房交换机遥测(端口流量、错包数等计数器)的自研时序存储。无第三方依赖,
Python 3.10+ 即可运行,内置 Web 控制台。

## 快速开始

```powershell
# 写入压测(默认 3000 设备拓扑)
py -m pytimeseries bench --data ./data --points 2000000

# 造一个月历史数据
py -m pytimeseries generate --data ./data --devices 3000 --days 30

# 启动 Web 控制台(指标浏览/查询编辑/降采样/存储状态/压测 五个页签)
py -m pytimeseries serve --data ./data --port 8000

# 压缩算法往返与压缩率自检
py -m pytimeseries test-compression

# 命令行查询 / 存储状态 / 手动降采样
py -m pytimeseries query --data ./data --q 'sum by (idc)(rate(if_port_in_bytes))'
py -m pytimeseries stats  --data ./data
py -m pytimeseries downsample --data ./data
```

测试: `py -m unittest tests.test_core -v`(20 个用例,覆盖全部 7 项核心需求)。

## 七项需求的实现位置

| # | 需求 | 实现 |
|---|------|------|
| 1 | 批量写入,乱序点归位 | `memtable.py` 二分插入 + 同时间戳覆盖;`engine.py` 批量写、后台刷盘 |
| 2 | 时间戳差值编码 + 数值 XOR,显示压缩率 | `codec.py` Gorilla 风格 DOD 位桶 + 计数器 DOD/异常旁路编码;压缩率见存储状态页 |
| 3 | 标签倒排索引,设备/端口/机房组合过滤 | `index.py` `tag=value -> sid 集合`,任意标签求交并 |
| 4 | raw 7 天 / 5m 90 天 / 1h 2 年,到期自动降采样再删 | `retention.py` 三级目录 + 水位线 + 流水线(先聚合再过期) |
| 5 | 计数器回绕/重启清零的速率处理 | `retention.safe_delta`、`query._to_rate` |
| 6 | 时间分组 + sum/max/分位数 | `query.py` 文本查询解析与执行 |
| 7 | 写一半掉电恢复到最后完整块 | `block.py` 每块 CRC32,启动扫描截断坏块尾部 |

## 存储格式

```
data/
  series.log      序列注册表(追加日志, 启动重放重建倒排索引)
  stats.json      累计写入统计
  downsample.json 降采样水位线
  raw/  5m/  1h/  各层独立的 segment 目录
    0000000001.seg   多个块顺序拼接
```

块布局(小端): `magic(4) | version | sid | t0 | t1 | count | plen | payload | crc32`。
任何不完整或 CRC 失败的块在启动时被整体截断,恢复到最后一个完整块。

### 编码细节(`codec.py`)

- **时间戳**: 首值 64bit + 首 delta 14bit,之后 delta-of-delta 走 Gorilla 位桶
  (等间隔序列每个时间戳仅 1 bit)。
- **整数计数器**: 首值 varint;之后每个点 1 个标记位——
  正常点做 delta 的 DOD(自相关流量下 DOD 极小,主体 5 bit),
  乱序/重启/回绕等异常点用标记位 + 绝对值旁路,不污染主差分链。
- **浮点/仪表量**: IEEE754 double 位模式的 Gorilla XOR + 前导/尾随零窗口。

## 查询语法

```
if_port_in_bytes{device="sw-0001",idc="bj-1"}
rate(if_port_in_errors{port="GE1/0/1"})
sum by (idc) (rate(if_port_in_bytes))
max by (port) (rate(if_port_out_drops))
quantile(0.95) by (device) (if_port_in_bytes)
```

时间范围与分组步长由请求的 start/end/step(秒)给出。`rate()` 自动跳过
重启清零造成的不可算区间,并按 64 位无符号回绕补全增量。

## 性能与压缩实测(Windows / Python 3.14 / 单机)

- **写入吞吐**: 23 万–33 万 points/s,稳定超过 10 万/s 目标。
  关键优化: 序列预注册 + sid 快速写入路径、字级比特编解码、
  memtable 批量落盘后单次 fsync、每序列缓冲凑 512 点成块。
- **压缩率**: 长序列(每序列上千连续点)压缩后约为原始的 **8%–10%**
  (约 10–12 倍),满足 < 1/10 目标。
- **边界说明**: 压缩率依赖序列长度。每条序列只有几十、几百个点时,
  块头和每块初始值摊不薄,整体会高于 10%;真实机房一个月每序列约
  8.6 万个 30 秒点,会形成大量满块,压缩率回落到 10% 以内。
  可分别用 `bench --ports 1 --points 6000000`(长序列)与默认拓扑(高基数短序列)对照。

## 设计取舍

- 计数器是无符号 64 位语义;含负值的整数会按无符号位模式进入异常旁路。
- 乱序点保证正确归位与查询正确,但会让受影响的块压缩率下降(旁路绝对值更大)。
  真实遥测乱序率通常在千分之几,对整体压缩率影响有限。
- 降采样序列以 `_resolution` 标签区分,与原始序列共用同一套索引与查询路径。
