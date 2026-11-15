"""序列注册表 + 标签倒排索引。

序列 = 指标名 + 一组标签(标签按 key 排序后规范化)。
倒排索引: tag_key=tag_value -> 包含该标签的序列 ID 集合,
查询时对设备/端口/机房等任意标签组合做集合交并。
"""

import json
import os
import threading


def canonical_key(metric, labels):
    """生成规范序列键: metric|k1=v1,k2=v2 ...(标签按 key 排序)。"""
    parts = [f"{k}={v}" for k, v in sorted(labels.items())]
    return metric + "|" + ",".join(parts)


class SeriesIndex:
    def __init__(self, path=None):
        self.path = path
        self._lock = threading.RLock()
        self.by_key = {}          # 规范键 -> sid
        self.meta = {}            # sid -> (metric, labels_dict)
        self.next_sid = 1
        # 倒排: "__metric__=<metric>" 以及 "<key>=<value>" -> set(sid)
        self.postings = {}
        # 序列日志保持单句柄缓冲写入, 避免每条序列一次 open+fsync
        self._log_f = open(path, "a", encoding="utf-8") if path else None

    # ---------- 注册 ----------
    def get_or_create(self, metric, labels):
        key = canonical_key(metric, labels)
        with self._lock:
            sid = self.by_key.get(key)
            if sid is not None:
                return sid, False
            sid = self.next_sid
            self.next_sid += 1
            self.by_key[key] = sid
            self.meta[sid] = (metric, dict(labels))
            self.postings.setdefault("__metric__=" + metric, set()).add(sid)
            for k, v in labels.items():
                self.postings.setdefault(f"{k}={v}", set()).add(sid)
            if self._log_f is not None:
                self._append_log(sid, metric, labels)
            return sid, True

    def _append_log(self, sid, metric, labels):
        # 追加式序列日志; 句柄常驻, 由 sync()/close() 统一落盘
        self._log_f.write(json.dumps(
            {"sid": sid, "metric": metric, "labels": labels},
            ensure_ascii=False) + "\n")

    def sync(self):
        """显式把缓冲的序列日志刷盘(批量写入后/关闭前调用)。"""
        with self._lock:
            if self._log_f is not None:
                self._log_f.flush()
                os.fsync(self._log_f.fileno())

    def close(self):
        with self._lock:
            if self._log_f is not None:
                self._log_f.flush()
                os.fsync(self._log_f.fileno())
                self._log_f.close()
                self._log_f = None

    def load(self):
        if not self.path or not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                sid = rec["sid"]
                metric = rec["metric"]
                labels = rec["labels"]
                key = canonical_key(metric, labels)
                self.by_key[key] = sid
                self.meta[sid] = (metric, dict(labels))
                self.postings.setdefault("__metric__=" + metric, set()).add(sid)
                for k, v in labels.items():
                    self.postings.setdefault(f"{k}={v}", set()).add(sid)
                if sid >= self.next_sid:
                    self.next_sid = sid + 1

    # ---------- 查询 ----------
    def series_meta(self, sid):
        return self.meta.get(sid)

    def match_metric(self, metric):
        with self._lock:
            return set(self.postings.get("__metric__=" + metric, ()))

    def match_label(self, key, value):
        with self._lock:
            return set(self.postings.get(f"{key}={value}", ()))

    def match(self, metric=None, filters=None):
        """按指标 + 标签等值过滤求交集, 返回 sid 集合。

        filters: dict[key] = value(或 list[value], list 表示同 key 的并集)。
        """
        with self._lock:
            if metric is None:
                result = set(self.meta.keys())
            else:
                result = self.match_metric(metric)
            if filters:
                for k, v in filters.items():
                    if isinstance(v, (list, tuple, set)):
                        union = set()
                        for one in v:
                            union |= self.match_label(k, one)
                        result &= union
                    else:
                        result &= self.match_label(k, v)
            return result

    def all_metrics(self):
        with self._lock:
            return sorted(k[len("__metric__="):]
                          for k in self.postings if k.startswith("__metric__="))

    def label_values(self, key):
        prefix = key + "="
        with self._lock:
            return sorted(k[len(prefix):]
                          for k in self.postings
                          if k.startswith(prefix) and not k.startswith("__"))

    def stats(self):
        with self._lock:
            return {
                "series": len(self.meta),
                "posting_terms": len(self.postings),
            }
