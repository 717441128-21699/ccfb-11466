"""块文件格式。

一个 segment(.seg) 由若干块顺序拼接而成, 每块布局(小端):

    偏移  长度  字段
    0     4     magic = b'PTSB'
    4     4     格式版本
    8     8     序列 ID
    16    8     起始时间戳(秒)
    24    8     结束时间戳(秒)
    32    4     点数
    36    4     payload 长度
    40    N     payload(codec.encode_block 输出)
    40+N  4     CRC32(覆盖整个头 + payload)

写块时先把数据追加落盘并 fsync, 再写“已提交”的下一块。
恢复时顺序扫描: 任何不完整/校验失败的块都视为掉电时写了一半的块,
直接截断文件尾部, 数据库恢复到最后一个完整块。
"""

import os
import struct
import zlib

MAGIC = b"PTSB"
VERSION = 1
HEADER = struct.Struct("<4sIqqqII")  # magic, ver, sid, t0, t1, n, plen
HEADER_SIZE = 40
CRC_SIZE = 4


class BlockMeta:
    """块索引项: 块在 segment 中的定位信息(常驻内存)。"""

    __slots__ = ("sid", "t0", "t1", "count", "segment", "offset", "length", "kind")

    def __init__(self, sid, t0, t1, count, segment, offset, length, kind):
        self.sid = sid
        self.t0 = t0
        self.t1 = t1
        self.count = count
        self.segment = segment
        self.offset = offset
        self.length = length  # 头 + payload + CRC 的总字节
        self.kind = kind

    def __repr__(self):
        return (
            f"BlockMeta(sid={self.sid}, {self.t0}..{self.t1}, n={self.count}, "
            f"{self.segment}:{self.offset})"
        )


def build_block(sid, times, payload, kind):
    """组装一块的完整字节。"""
    header = HEADER.pack(
        MAGIC, VERSION, sid, times[0], times[-1], len(times), len(payload)
    )
    crc = zlib.crc32(header + payload) & 0xFFFFFFFF
    return header + payload + struct.pack("<I", crc)


class SegmentWriter:
    """单个 segment 文件的追加写句柄, 达到目标大小后轮转到新文件。"""

    __slots__ = ("path", "f", "size", "seg_id")

    def __init__(self, directory, seg_id):
        self.seg_id = seg_id
        self.path = os.path.join(directory, f"{seg_id:010d}.seg")
        self.f = open(self.path, "ab+")
        self.f.seek(0, os.SEEK_END)
        self.size = self.f.tell()

    def append(self, sid, times, payload, kind, sync=True):
        offset = self.size
        data = build_block(sid, times, payload, kind)
        self.f.write(data)
        self.size += len(data)
        if sync:
            # 单块落盘场景; 批量写入时调用方统一 sync 以摊薄 fsync 成本
            self.sync()
        meta = BlockMeta(
            sid, times[0], times[-1], len(times),
            self.seg_id, offset, len(data), kind,
        )
        return meta

    def sync(self):
        self.f.flush()
        os.fsync(self.f.fileno())

    def close(self):
        if self.f:
            self.sync()
            self.f.close()
            self.f = None


def read_block(directory, meta):
    """按索引读出一个块并校验, 返回 payload。"""
    path = os.path.join(directory, f"{meta.segment:010d}.seg")
    with open(path, "rb") as f:
        f.seek(meta.offset)
        raw = f.read(meta.length)
    if len(raw) != meta.length:
        raise IOError(f"块读取不完整: {path} offset={meta.offset}")
    header = raw[:HEADER_SIZE]
    payload = raw[HEADER_SIZE:-CRC_SIZE]
    (crc_stored,) = struct.unpack("<I", raw[-CRC_SIZE:])
    if zlib.crc32(header + payload) & 0xFFFFFFFF != crc_stored:
        raise IOError(f"块 CRC 校验失败: {path} offset={meta.offset}")
    magic, version, sid, t0, t1, n, plen = HEADER.unpack(header)
    if magic != MAGIC:
        raise IOError(f"块 magic 错误: {path}")
    return payload


def recover_segment(path):
    """扫描恢复一个 segment。

    返回 (metas, good_bytes): 所有完整块的索引, 以及完整数据的字节数。
    如果文件尾部存在写了一半的块, 调用方应截断到 good_bytes。
    """
    metas = []
    size = os.path.getsize(path)
    truncated = False
    with open(path, "rb") as f:
        pos = 0
        while pos < size:
            f.seek(pos)
            header = f.read(HEADER_SIZE)
            if len(header) < HEADER_SIZE:
                truncated = True
                break
            magic, version, sid, t0, t1, n, plen = HEADER.unpack(header)
            if magic != MAGIC:
                truncated = True
                break
            block_len = HEADER_SIZE + plen + CRC_SIZE
            if pos + block_len > size:
                truncated = True
                break
            rest = f.read(plen + CRC_SIZE)
            payload = rest[:plen]
            (crc_stored,) = struct.unpack("<I", rest[plen:])
            if zlib.crc32(header + payload) & 0xFFFFFFFF != crc_stored:
                truncated = True
                break
            kind = payload[0]
            metas.append(BlockMeta(sid, t0, t1, n, _seg_id_of(path), pos, block_len, kind))
            pos += block_len
    if truncated and pos < size:
        # 截断掉写了一半的尾部, 并 fsync 保证截断本身落盘
        with open(path, "r+b") as f:
            f.truncate(pos)
            f.flush()
            os.fsync(f.fileno())
    return metas, pos, truncated


def _seg_id_of(path):
    return int(os.path.basename(path).split(".")[0])


def list_segments(directory):
    if not os.path.isdir(directory):
        return []
    out = []
    for name in os.listdir(directory):
        if name.endswith(".seg"):
            try:
                out.append(int(name[:-4]))
            except ValueError:
                continue
    return sorted(out)
