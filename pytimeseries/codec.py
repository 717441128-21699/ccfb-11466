"""Gorilla 风格的比特流编解码。

时间戳: 首个时间戳原样存储, 之后存储 delta-of-delta(差值的差值),
        再做 zigzag + varint 编码。
数值:   全部按 64 bit 处理。整数直接用位模式, 浮点用 IEEE754 double 的位模式,
        相邻值做 XOR, 利用前导零/尾随零窗口压缩(异或压缩)。
"""

import struct

MASK64 = (1 << 64) - 1
MASK32 = (1 << 32) - 1


class BitWriter:
    """按位追加写入, 输出紧凑字节流。"""

    __slots__ = ("buf", "cur", "nbits")

    def __init__(self):
        self.buf = bytearray()
        self.cur = 0
        self.nbits = 0

    def write_bit(self, b):
        self.cur = (self.cur << 1) | (b & 1)
        self.nbits += 1
        if self.nbits == 8:
            self.buf.append(self.cur)
            self.cur = 0
            self.nbits = 0

    def write_bits(self, value, n):
        """字级写入 n 个比特(1..64), 避免逐位循环的函数调用开销。"""
        if n == 0:
            return
        # 先填满当前未完成字节
        if self.nbits:
            room = 8 - self.nbits
            if n <= room:
                self.cur = (self.cur << n) | (value & ((1 << n) - 1))
                self.nbits += n
                if self.nbits == 8:
                    self.buf.append(self.cur)
                    self.cur = 0
                    self.nbits = 0
                return
            head = (value >> (n - room)) & ((1 << room) - 1)
            self.buf.append((self.cur << room) | head)
            value &= (1 << (n - room)) - 1
            n -= room
            self.cur = 0
            self.nbits = 0
        # 剩余整字节直接落盘
        while n >= 8:
            n -= 8
            self.buf.append((value >> n) & 0xFF)
        # 余下不足一字节放进 cur
        if n:
            self.cur = value & ((1 << n) - 1)
            self.nbits = n

    def write_varint(self, value):
        """无符号 varint(每字节低 7 位载荷, 最高位为续位)。"""
        while value > 0x7F:
            self.write_bits((value & 0x7F) | 0x80, 8)
            value >>= 7
        self.write_bits(value & 0x7F, 8)

    def write_uvarint_packed(self, value):
        """位打包变长整数: 前缀标签 + 定宽载荷, 全部按位紧密排列。

        档位(标签为前缀码), 针对计数器 DOD 的高频小值做了紧凑档:
          0    + 4bit     (<16, zigzag 后覆盖 DOD ±8)   —— 主力小差值
          10   + 7bit     (<128)
          110  + 10bit    (<1024)
          1110 + 14bit    (<16384)
          11110+ 21bit    (<2M)
          11111+ 28bit    (<268M), 满值再续标准 varint 兜底
        """
        if value < (1 << 4):
            self.write_bit(0)
            self.write_bits(value, 4)
        elif value < (1 << 7):
            self.write_bits(0b10, 2)
            self.write_bits(value, 7)
        elif value < (1 << 10):
            self.write_bits(0b110, 3)
            self.write_bits(value, 10)
        elif value < (1 << 14):
            self.write_bits(0b1110, 4)
            self.write_bits(value, 14)
        elif value < (1 << 21):
            self.write_bits(0b11110, 5)
            self.write_bits(value, 21)
        elif value < (1 << 28):
            self.write_bits(0b11111, 5)
            self.write_bits(value, 28)
        else:
            self.write_bits(0b11111, 5)
            self.write_bits((1 << 28) - 1, 28)
            self.write_varint(value >> 28)

    def read_uvarint_packed(self):
        if self.read_bit() == 0:
            return self.read_bits(4)
        if self.read_bit() == 0:                    # 前缀 10
            return self.read_bits(7)
        if self.read_bit() == 0:                    # 前缀 110
            return self.read_bits(10)
        if self.read_bit() == 0:                    # 前缀 1110
            return self.read_bits(14)
        if self.read_bit() == 0:                    # 前缀 11110
            return self.read_bits(21)
        low = self.read_bits(28)                    # 前缀 11111
        if low != (1 << 28) - 1:
            return low
        return low + (self.read_varint() << 28)

    def finish(self):
        if self.nbits:
            # 末字节剩余位用 0 填充
            self.buf.append(self.cur << (8 - self.nbits))
            self.cur = 0
            self.nbits = 0
        return bytes(self.buf)

    def bit_count(self):
        return len(self.buf) * 8 + self.nbits


class BitReader:
    """按位读取, 与 BitWriter 对称。"""

    __slots__ = ("buf", "pos", "nbits")

    def __init__(self, data, bit_length=None):
        self.buf = data
        self.pos = 0
        self.nbits = bit_length if bit_length is not None else len(data) * 8

    def read_bit(self):
        if self.pos >= self.nbits:
            raise EOFError("比特流已读完")
        b = (self.buf[self.pos >> 3] >> (7 - (self.pos & 7))) & 1
        self.pos += 1
        return b

    def read_bits(self, n):
        """字级读取 n 个比特, 与 BitWriter.write_bits 对称。"""
        if n == 0:
            return 0
        result = 0
        bit_pos = self.pos
        buf = self.buf
        # 跨字节拼接: 每次取当前字节剩余位
        remaining = n
        while remaining > 0:
            byte_idx = bit_pos >> 3
            in_byte = bit_pos & 7
            avail = 8 - in_byte
            take = remaining if remaining < avail else avail
            if take == 8 and in_byte == 0:
                chunk = buf[byte_idx]
            else:
                chunk = (buf[byte_idx] >> (avail - take)) & ((1 << take) - 1)
            result = (result << take) | chunk
            bit_pos += take
            remaining -= take
        self.pos = bit_pos
        return result

    def read_varint(self):
        shift = 0
        result = 0
        while True:
            b = self.read_bits(8)
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                return result
            shift += 7

    def read_uvarint_packed(self):
        if self.read_bit() == 0:
            return self.read_bits(4)
        if self.read_bit() == 0:                    # 前缀 10
            return self.read_bits(7)
        if self.read_bit() == 0:                    # 前缀 110
            return self.read_bits(10)
        if self.read_bit() == 0:                    # 前缀 1110
            return self.read_bits(14)
        if self.read_bit() == 0:                    # 前缀 11110
            return self.read_bits(21)
        low = self.read_bits(28)                    # 前缀 11111
        if low != (1 << 28) - 1:
            return low
        return low + (self.read_varint() << 28)


def zigzag_encode(n):
    """有符号整数 -> 无符号整数, 小绝对值得到小编码。"""
    if n >= 0:
        return n << 1
    return ((-n) << 1) - 1


def zigzag_decode(u):
    if u & 1:
        return -((u + 1) >> 1)
    return u >> 1


def _leading_zeros(x):
    """64 位前导零个数(x != 0)。"""
    return 64 - x.bit_length()


def _trailing_zeros(x):
    return (x & -x).bit_length() - 1


def _write_xor(bw, xor, state):
    """Gorilla 异或块写入。

    位流布局(与论文一致):
      xor == 0:  [0]
      xor != 0:  [1][ 复用窗口? 0 : 1(leading[5], meaningful-1[6]) ][有效位]
    state 保存上一次窗口, 每个块编码开始时重置。
    """
    if xor == 0:
        bw.write_bit(0)
        return
    bw.write_bit(1)
    leading = min(_leading_zeros(xor), 31)
    trailing = _trailing_zeros(xor)
    meaningful = 64 - leading - trailing
    prev_leading = state["leading"]
    prev_trailing = state["trailing"]
    if leading >= prev_leading and trailing >= prev_trailing:
        # 复用上一次的窗口
        bw.write_bit(0)
        width = 64 - prev_leading - prev_trailing
        bw.write_bits((xor >> prev_trailing) & ((1 << width) - 1), width)
    else:
        bw.write_bit(1)
        bw.write_bits(leading, 5)
        bw.write_bits(meaningful - 1, 6)  # 长度 1..64 -> 0..63
        bw.write_bits((xor >> trailing) & ((1 << meaningful) - 1), meaningful)
        state["leading"] = leading
        state["trailing"] = trailing


def _read_xor(br, prev, state):
    if br.read_bit() == 0:
        # xor == 0, 值与上一个相同
        return prev
    if br.read_bit() == 1:
        # 新窗口
        leading = br.read_bits(5)
        meaningful = br.read_bits(6) + 1
        trailing = 64 - leading - meaningful
        state["leading"] = leading
        state["trailing"] = trailing
    else:
        # 复用旧窗口
        leading = state["leading"]
        trailing = state["trailing"]
        meaningful = 64 - leading - trailing
    chunk = br.read_bits(meaningful)
    return (prev ^ (chunk << trailing)) & MASK64


KIND_FLOAT = 0
KIND_INT = 1
KIND_INT_DOD = 2  # 整数计数器: 增量 DOD 的 zigzag + 位打包 group varint


def encode_block(times, values):
    """编码一个序列的一批(时间戳, 数值)。

    要求 times 严格递增(调用方负责去重排序)。
    返回 (payload: bytes, value_kind: int)。

    时间戳采用 Gorilla delta-of-delta 位桶:
      首时间戳 64bit; 首 delta 14bit 定长; 之后 DOD 按区间落入
      [0] / [2bit 标记+7bit] / [3bit 标记+9bit] / [4bit 标记+12bit有符号] 四档。
    """
    if not times:
        raise ValueError("空块不能编码")
    all_int = all(type(v) is int for v in values)
    # 整数块统一走带“异常点旁路”的 DOD 路径: 正常单调点享受紧凑 DOD,
    # 乱序/重启造成的值倒退点用标记位+绝对值旁路, 不再整块退回 XOR。
    use_dod = all_int and len(values) >= 2
    kind = KIND_INT_DOD if use_dod else (KIND_INT if all_int else KIND_FLOAT)

    bw = BitWriter()
    # ---- 时间戳: 首值 + 首 delta + delta-of-delta 位桶 ----
    bw.write_bits(times[0] & MASK64, 64)
    bw.write_varint(len(times))
    if len(times) > 1:
        first_delta = times[1] - times[0]
        bw.write_bits(first_delta & ((1 << 14) - 1), 14)
        prev_delta = first_delta
        prev_ts = times[1]
        for i in range(2, len(times)):
            delta = times[i] - prev_ts
            dod = delta - prev_delta
            if dod == 0:
                bw.write_bit(0)
            elif -64 <= dod <= 63:
                bw.write_bits(0b10, 2)
                bw.write_bits(dod & 0x7F, 7)
            elif -256 <= dod <= 255:
                bw.write_bits(0b110, 3)
                bw.write_bits(dod & 0x1FF, 9)
            elif -2048 <= dod <= 2047:
                bw.write_bits(0b1110, 4)
                bw.write_bits(dod & 0xFFF, 12)
            else:
                # 罕见大跳变: 32bit 有符号兜底
                bw.write_bits(0b1111, 4)
                bw.write_bits(dod & MASK32, 32)
            prev_delta = delta
            prev_ts = times[i]

    # ---- 数值: XOR 压缩 ----
    state = {"leading": 0xFF, "trailing": 64}
    if kind == KIND_INT_DOD:
        # 计数器路径, 对乱序/重启鲁棒。每个点 1 个标记位:
        #   标记 0: 正常点。相对上一个“正常点”做 delta, 再对 delta 做 DOD。
        #   标记 1: 异常点(值倒退或时间空洞)。直接存绝对值, 不参与差分链,
        #           只更新锚点, 保证后续正常点的 DOD 仍然很小。
        # 首值与首 delta 用位打包 varint(短块友好)。
        # 首值用标准 varint(支持满 64 位无符号计数器)。
        bw.write_varint(values[0] & MASK64)
        anchor_v = values[0]
        anchor_t = times[0]
        prev_delta = None
        for i in range(1, len(values)):
            gap = times[i] - anchor_t
            delta = values[i] - anchor_v
            normal = (values[i] >= anchor_v) and (gap > 0)
            if not normal:
                # 异常旁路: 标记 1 + 绝对值(标准 varint, 支持满 64 位无符号)
                bw.write_bit(1)
                bw.write_varint(values[i] & MASK64)
                anchor_v = values[i]
                anchor_t = times[i]
                prev_delta = None
                continue
            bw.write_bit(0)
            if prev_delta is None:
                # 异常后的第一个正常点: 直接存 delta 重建基线
                bw.write_bit(1)
                bw.write_varint(zigzag_encode(delta))
            else:
                bw.write_bit(0)
                bw.write_varint(zigzag_encode(delta - prev_delta))
            prev_delta = delta
            anchor_v = values[i]
            anchor_t = times[i]
    elif kind == KIND_INT:
        # 普通无符号 64 位整数, 原样保留位模式(支持回绕/清零场景的大值)
        prev = 0
        for v in values:
            x = v & MASK64
            _write_xor(bw, x ^ prev, state)
            prev = x
    else:
        prev = 0
        for v in values:
            x = struct.unpack("<Q", struct.pack("<d", float(v)))[0]
            _write_xor(bw, x ^ prev, state)
            prev = x

    return bytes([kind]) + bw.finish(), kind


def decode_block(payload):
    """encode_block 的逆操作, 返回 (times: list[int], values: list)。"""
    kind = payload[0]
    br = BitReader(payload[1:])
    first_ts = br.read_bits(64)
    count = br.read_varint()

    times = [0] * count
    if count:
        times[0] = first_ts
    if count > 1:
        prev_delta = br.read_bits(14)
        if prev_delta >= (1 << 13):  # 14bit 有符号还原
            prev_delta -= (1 << 14)
        prev_ts = first_ts + prev_delta
        times[1] = prev_ts
        for i in range(2, count):
            if br.read_bit() == 0:
                dod = 0
            else:
                tag2 = br.read_bit()
                if tag2 == 0:
                    dod = br.read_bits(7)
                    if dod >= (1 << 6):
                        dod -= (1 << 7)
                elif br.read_bit() == 0:
                    dod = br.read_bits(9)
                    if dod >= (1 << 8):
                        dod -= (1 << 9)
                elif br.read_bit() == 0:
                    dod = br.read_bits(12)
                    if dod >= (1 << 11):
                        dod -= (1 << 12)
                else:
                    dod = br.read_bits(32)
                    if dod >= (1 << 31):
                        dod -= (1 << 32)
            delta = prev_delta + dod
            prev_ts += delta
            prev_delta = delta
            times[i] = prev_ts

    _read_state = {"leading": 0xFF, "trailing": 64}
    values = [0] * count
    if kind == KIND_INT_DOD:
        values[0] = br.read_varint()
        anchor_v = values[0]
        prev_delta = None
        for i in range(1, count):
            if br.read_bit() == 1:
                # 异常旁路: 绝对值(标准 varint)
                anchor_v = br.read_varint()
                values[i] = anchor_v
                prev_delta = None
                continue
            if br.read_bit() == 1:
                # 异常后首个正常点: 直接 delta
                prev_delta = zigzag_decode(br.read_varint())
            else:
                prev_delta += zigzag_decode(br.read_varint())
            anchor_v += prev_delta
            values[i] = anchor_v
    elif kind == KIND_INT:
        prev = 0
        for i in range(count):
            x = _read_xor(br, prev, _read_state)
            prev = x
            # 无符号 64 位计数器原样返回
            values[i] = x
    else:
        prev = 0
        for i in range(count):
            x = _read_xor(br, prev, _read_state)
            prev = x
            values[i] = struct.unpack("<d", struct.pack("<Q", x))[0]
    return times, values
