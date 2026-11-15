"""pytimeseries — 单机时序数据库(纯 Python 标准库实现)。

模块划分:
  codec      比特流 / 时间戳差值编码 / 数值 XOR 压缩 (Gorilla 风格)
  block      块文件格式、CRC 校验、崩溃恢复扫描
  memtable   内存表(乱序点按时间戳归位)
  index      序列注册表 + 标签倒排索引
  engine     存储引擎:批量写入、刷盘、查询、降采样、保留策略
  query      查询解析与执行(分组/聚合/分位数/速率)
  web        内置 HTTP 服务与网页
  benchmark  数据造数与写入压测
"""

__version__ = "1.0.0"
