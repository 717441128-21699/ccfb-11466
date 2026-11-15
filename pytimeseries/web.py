"""内置 HTTP 服务: REST API + 静态网页(无第三方依赖)。

API:
  GET  /api/metrics                 指标列表与标签取值
  GET  /api/stats                   存储状态(含压缩率)
  POST /api/query                   查询 {text,start,end,step,tiers}
  POST /api/downsample/run          立即执行一轮降采样+过期清理
  GET  /api/downsample/report       最近一次维护报告与水位线
  POST /api/benchmark/load          写入压测
  POST /api/benchmark/generate      历史数据造数
  GET  /                            网页(index.html)
"""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import benchmark as bench_mod
from .query import QueryRequest, execute
from .retention import RetentionManager

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


class WebHandler(BaseHTTPRequestHandler):
    server_version = "PyTimeSeries/1.0"

    # 静默常规访问日志, 保留错误
    def log_message(self, fmt, *args):
        if args and str(args[1]).startswith(("4", "5")):
            super().log_message(fmt, *args)

    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        try:
            if self.path.startswith("/api/metrics"):
                idx = self.server.engine.index
                payload = {
                    "metrics": idx.all_metrics(),
                    "labels": {
                        key: idx.label_values(key)
                        for key in ("device", "port", "idc", "row")
                    },
                }
                return self._send_json(payload)
            if self.path.startswith("/api/stats"):
                return self._send_json(self.server.engine.stats())
            if self.path.startswith("/api/downsample/report"):
                rm = self.server.retention
                return self._send_json({
                    "watermarks": rm.watermarks,
                    "last_run": rm.last_run,
                    "last_report": rm.last_report,
                })
            if self.path.startswith("/api/benchmark/status"):
                return self._send_json({
                    "running": self.server.bench_running.is_set(),
                    "result": self.server.bench_last,
                })
            if self.path in ("/", "/index.html"):
                return self._serve_file("index.html", "text/html; charset=utf-8")
            if self.path.startswith("/app.js"):
                return self._serve_file("app.js", "application/javascript; charset=utf-8")
            if self.path.startswith("/styles.css"):
                return self._serve_file("styles.css", "text/css; charset=utf-8")
            self._send_json({"error": "not found"}, 404)
        except Exception as exc:
            self._send_json({"error": str(exc)}, 500)

    def do_POST(self):
        try:
            data = self._read_json()
            if self.path.startswith("/api/query"):
                req = QueryRequest(
                    text=data["text"],
                    start=int(data["start"]),
                    end=int(data["end"]),
                    step=int(data.get("step", 300)),
                    tiers=data.get("tiers", ["raw", "5m", "1h"]),
                )
                return self._send_json(execute(self.server.engine, req))
            if self.path.startswith("/api/downsample/run"):
                report = self.server.retention.run_once()
                return self._send_json({"ok": True, "report": report})
            if self.path.startswith("/api/benchmark/load"):
                return self._run_bench("load", data)
            if self.path.startswith("/api/benchmark/generate"):
                return self._run_bench("generate", data)
            self._send_json({"error": "not found"}, 404)
        except Exception as exc:
            import traceback
            self._send_json({"error": str(exc), "trace": traceback.format_exc()}, 500)

    def _run_bench(self, mode, data):
        engine = self.server.engine
        if self.server.bench_running.is_set():
            return self._send_json({"error": "已有压测任务在运行"}, 409)
        self.server.bench_running.set()

        def job():
            try:
                if mode == "load":
                    result = bench_mod.run_load(
                        engine,
                        devices=int(data.get("devices", 3000)),
                        ports=int(data.get("ports", 4)),
                        points=int(data.get("points", 2_000_000)),
                        batch_size=int(data.get("batch_size", 5000)),
                        out_of_order_ratio=float(data.get("out_of_order_ratio", 0.02)),
                        wrap_enabled=bool(data.get("wrap_enabled", False)),
                        reset_rate=float(data.get("reset_rate", 0.0001)),
                    )
                else:
                    result = bench_mod.run_generate(
                        engine,
                        devices=int(data.get("devices", 3000)),
                        ports=int(data.get("ports", 4)),
                        days=float(data.get("days", 30)),
                        batch_size=int(data.get("batch_size", 10000)),
                        wrap_enabled=bool(data.get("wrap_enabled", True)),
                        reset_rate=float(data.get("reset_rate", 0.00005)),
                    )
                self.server.bench_last = result
            except Exception as exc:
                import traceback
                self.server.bench_last = {"error": str(exc),
                                          "trace": traceback.format_exc()}
            finally:
                self.server.bench_running.clear()

        threading.Thread(target=job, name="pts-bench", daemon=True).start()
        self._send_json({"ok": True, "message": "压测已启动"})

    def _serve_file(self, name, content_type):
        path = os.path.join(STATIC_DIR, name)
        if not os.path.exists(path):
            return self._send_json({"error": f"{name} 缺失"}, 404)
        with open(path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class WebServer:
    def __init__(self, engine, host="127.0.0.1", port=8000,
                 retention_manager=None):
        self.engine = engine
        self.retention = retention_manager or RetentionManager(engine)
        self.httpd = ThreadingHTTPServer((host, port), WebHandler)
        self.httpd.engine = engine
        self.httpd.retention = self.retention
        self.httpd.bench_running = threading.Event()
        self.httpd.bench_last = None
        self.host = host
        self.port = self.httpd.server_address[1]
        self.thread = None

    def start(self):
        self.thread = threading.Thread(
            target=self.httpd.serve_forever, name="pts-web", daemon=True)
        self.thread.start()
        return f"http://{self.host}:{self.port}/"

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
