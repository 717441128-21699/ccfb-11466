"""查询解析与执行。

查询文本示例:
    if_port_in_bytes{device="sw-0001",idc="bj-1"}
    rate(if_port_in_errors{port="GE1/0/1"})
    sum by (idc) (rate(if_port_in_bytes))
    quantile(0.95) by (device) (if_port_in_bytes)
    max by (idc, port) (rate(if_port_in_bytes))

时间范围、分组步长通过 QueryRequest 的 start/end/step(秒)给出。
执行流程: 倒排索引选序列 -> 读点 -> rate 处理回绕/清零
-> 按 step 时间桶分组 -> 按 by 标签二次分组 -> 聚合。
"""

import re
from dataclasses import dataclass, field

from .retention import is_counter, safe_delta

TOKEN_RE = re.compile(
    r"""
    \s*(?:
        (?P<number>\d+(?:\.\d+)?)
      | (?P<ident>[A-Za-z_][A-Za-z0-9_]*)
      | (?P<op>[(){},=.])
      | (?P<str>"(?:[^"\\]|\\.)*")
    )
    """,
    re.VERBOSE,
)

AGG_FUNCTIONS = {"sum", "max", "min", "avg", "count", "quantile"}


@dataclass
class QueryRequest:
    text: str
    start: int
    end: int
    step: int = 300
    tiers: list = field(default_factory=lambda: ["raw", "5m", "1h"])


@dataclass
class ParsedQuery:
    metric: str
    filters: dict
    rate: bool
    aggregator: str = "none"      # none/sum/max/min/avg/count/quantile
    quantile: float = 0.0
    by: list = field(default_factory=list)


class QueryParseError(ValueError):
    pass


class _Tokenizer:
    def __init__(self, text):
        self.tokens = []
        pos = 0
        while pos < len(text):
            m = TOKEN_RE.match(text, pos)
            if not m or m.end() == pos:
                if text[pos].isspace():
                    pos += 1
                    continue
                raise QueryParseError(f"无法解析的字符: {text[pos:pos + 10]!r}")
            pos = m.end()
            kind = m.lastgroup
            value = m.group()
            self.tokens.append((kind, value.strip()))
        self.i = 0

    def peek(self):
        return self.tokens[self.i] if self.i < len(self.tokens) else (None, None)

    def next(self):
        tok = self.peek()
        self.i += 1
        return tok

    def expect(self, op):
        kind, value = self.next()
        if value != op:
            raise QueryParseError(f"期望 {op!r}, 实际 {value!r}")


def parse_query(text):
    tk = _Tokenizer(text)
    return _parse_expr(tk)


def _parse_expr(tk):
    kind, value = tk.next()
    if kind != "ident":
        raise QueryParseError(f"查询必须以函数或指标名开头, 实际 {value!r}")

    rate = False
    aggregator = "none"
    quantile = 0.0
    by_labels = []

    if value == "rate":
        tk.expect("(")
        inner = _parse_selector(tk)
        tk.expect(")")
        rate = True
        return ParsedQuery(inner[0], inner[1], True)
    if value in AGG_FUNCTIONS:
        if value == "quantile":
            tk.expect("(")
            kind2, num = tk.next()
            if kind2 != "number":
                raise QueryParseError("quantile 需要分位数参数, 如 quantile(0.95)")
            quantile = float(num)
            tk.expect(")")
        # 可选 by(...)
        kind2, val2 = tk.peek()
        if kind2 == "ident" and val2 == "by":
            tk.next()
            by_labels = _parse_by(tk)
        tk.expect("(")
        inner = _parse_expr(tk)
        tk.expect(")")
        inner.aggregator = value
        inner.quantile = quantile
        inner.by = by_labels
        inner.rate = inner.rate  # 允许 sum(rate(...))
        return inner
    if value == "by":
        # by(...) ( metric ) 形式
        by_labels = _parse_by(tk)
        tk.expect("(")
        inner = _parse_expr(tk)
        tk.expect(")")
        inner.by = by_labels
        return inner

    # 裸指标选择器
    tk.i -= 1
    metric, filters = _parse_selector(tk)
    return ParsedQuery(metric, filters, False, by=by_labels)


def _parse_by(tk):
    tk.expect("(")
    labels = []
    while True:
        kind, value = tk.next()
        if kind != "ident":
            raise QueryParseError(f"by 子句需要标签名, 实际 {value!r}")
        labels.append(value)
        kind2, sep = tk.next()
        if sep == ")":
            return labels
        if sep != ",":
            raise QueryParseError(f"by 子句分隔符错误: {sep!r}")


def _parse_selector(tk):
    kind, metric = tk.next()
    if kind != "ident":
        raise QueryParseError(f"需要指标名, 实际 {metric!r}")
    filters = {}
    kind2, val2 = tk.peek()
    if val2 == "{":
        tk.next()
        while True:
            kind3, key = tk.next()
            if kind3 != "ident":
                raise QueryParseError(f"标签 key 错误: {key!r}")
            tk.expect("=")
            kind4, raw = tk.next()
            if kind4 != "str":
                raise QueryParseError(f"标签值需要引号字符串, 实际 {raw!r}")
            filters[key] = bytes(raw[1:-1], "utf-8").decode("unicode_escape")
            kind5, sep = tk.next()
            if sep == "}":
                break
            if sep != ",":
                raise QueryParseError(f"标签分隔符错误: {sep!r}")
    return metric, filters


# ---------------- 执行 ----------------

def _to_rate(times, values):
    """计数器点序列 -> 每秒速率序列, 处理回绕与重启清零。"""
    out_t, out_v = [], []
    for i in range(1, len(times)):
        dt = times[i] - times[i - 1]
        if dt <= 0:
            continue
        d = safe_delta(values[i - 1], values[i])
        if d is None:
            continue  # 重启清零: 该区间不可计算, 跳过而非产生负速率
        out_t.append(times[i])
        out_v.append(d / dt)
    return out_t, out_v


def _quantile(sorted_values, q):
    """最近秩(nearest-rank)分位数。"""
    if not sorted_values:
        return 0.0
    n = len(sorted_values)
    rank = max(1, min(n, int(round(q * n))))
    return sorted_values[rank - 1]


def _aggregate(aggregator, values, quantile):
    if not values:
        return None
    if aggregator == "sum":
        return sum(values)
    if aggregator == "max":
        return max(values)
    if aggregator == "min":
        return min(values)
    if aggregator == "avg":
        return sum(values) / len(values)
    if aggregator == "count":
        return len(values)
    if aggregator == "quantile":
        return _quantile(sorted(values), quantile)
    return values[0]


def execute(engine, req):
    """返回 dict: {分组标签元组: {"labels":..., "points":[(ts, value), ...]}}。"""
    parsed = parse_query(req.text)
    sids = engine.select_series(parsed.metric, parsed.filters or None)

    # 分组键: 有 by 取标签子集; 无聚合时每条序列各自成组
    groups = {}
    for sid in sorted(sids):
        meta = engine.index.series_meta(sid)
        if meta is None:
            continue
        metric, labels = meta
        times, values = engine.read_series(sid, req.start, req.end, tiers=req.tiers)
        if not times:
            continue
        if parsed.rate or (parsed.aggregator != "none" and is_counter(metric)
                           and parsed.aggregator == "sum"):
            # rate() 显式要求; 对计数器做 sum 聚合时同样按速率口径, 避免回绕污染
            times, values = _to_rate(times, values)
            if not times:
                continue

        # 时间桶
        buckets = {}
        for t, v in zip(times, values):
            b = (t // req.step) * req.step
            if req.start <= b <= req.end:
                buckets.setdefault(b, []).append(v)

        if parsed.aggregator == "none":
            gkey = tuple(sorted(labels.items()))
            grp = groups.setdefault(gkey, {"labels": dict(labels), "buckets": {}})
            for b, vs in buckets.items():
                grp["buckets"].setdefault(b, []).extend(vs)
        else:
            gkey = tuple((k, labels.get(k, "")) for k in parsed.by)
            grp = groups.setdefault(gkey, {
                "labels": dict(gkey) if gkey else {}, "buckets": {}})
            for b, vs in buckets.items():
                grp["buckets"].setdefault(b, []).extend(vs)

    result = []
    for gkey, grp in sorted(groups.items(), key=lambda x: str(x[0])):
        points = []
        for b in sorted(grp["buckets"]):
            val = _aggregate(parsed.aggregator if parsed.aggregator != "none"
                             else "avg", grp["buckets"][b], parsed.quantile)
            if val is not None:
                points.append((b, val))
        result.append({"labels": grp["labels"], "points": points})
    return {
        "query": req.text,
        "start": req.start,
        "end": req.end,
        "step": req.step,
        "series_count": len(result),
        "series": result,
    }
