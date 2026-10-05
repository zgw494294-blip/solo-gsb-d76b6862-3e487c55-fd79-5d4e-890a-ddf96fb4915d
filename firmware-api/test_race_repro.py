"""race_repro.sh 传输异常回归：本地可控传输桩 + 真实脚本进程。

桩实现与服务相同的挂点协议（FIRMWARE_TEST_PAUSE_DIR 标记文件），
scripts/race_repro.sh 无需任何修改即可运行在桩上。覆盖三条路径：

  1. 竞争停滞：竞争声明 B 的响应永不结束 —— 脚本必须在 REQ_MAX 内
     报错退出（修复前 B 的 curl 无超时，会绕过 WAIT_MAX 无限挂住），
     并清理标记与 .held、终止回收后台请求；
  2. 首响应残缺：首请求 A 放行后只收到含 "declared_length":3 的残缺
     响应、curl 返回非零 —— 脚本必须非零退出（修复前丢弃 curl 退出码，
     会误报 PASS）；
  3. 成功路径：完整交错交互 —— 脚本必须报 PASS（A=3、B=6、终态=6）。

运行：python3 test_race_repro.py
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(ROOT, "scripts", "race_repro.sh")

PASS = 0


def check(name, cond, extra=""):
    global PASS
    assert cond, f"FAIL: {name} {extra}"
    PASS += 1
    print(f"  ok - {name}")


class StubHandler(BaseHTTPRequestHandler):
    """可控传输桩：固件 API 必要端点 + 挂点协议 + 故障注入。"""

    # 由测试场景在启动前配置（测试串行执行，类属性即可）
    mode = "ok"            # ok | stall_b | truncated_a
    pause_dir = None
    declared = {}          # device_id -> declared_length

    protocol_version = "HTTP/1.1"  # 响应均带 Content-Length，连接语义明确

    def log_message(self, *args):
        pass  # 静默

    # 与真服务相同的紧凑 JSON（无空格分隔符）：脚本靠 grep '"declared_length":N' 提取
    def _view(self, dev):
        return {
            "device_id": dev,
            "slots": {
                "slot0": {"status": "confirmed", "declared_length": None},
                "slot1": {"status": "in-progress",
                          "declared_length": self.declared.get(dev)},
            },
        }

    def _send_json(self, code, obj):
        body = json.dumps(obj, separators=(",", ":")).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except BrokenPipeError:
            pass  # 对端（被脚本终止的 curl）已断开，属预期内的清理路径

    # 挂点协议：与 app/main.py 的 after_commit_hook 相同 —— 第一个命中标记的
    # 请求原子接管（rename 为 .held）并阻塞到 .held 被删除，其余请求直接放行
    def _pause_point(self, name):
        if not self.pause_dir:
            return False
        marker = os.path.join(self.pause_dir, name)
        held = marker + ".held"
        try:
            os.rename(marker, held)
        except OSError:
            return False
        deadline = time.monotonic() + 30
        while os.path.exists(held):
            if time.monotonic() >= deadline:
                break
            time.sleep(0.01)
        return True

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {"status": "ok"})
            return
        m = re.fullmatch(r"/devices/([^/]+)", self.path)
        if m:
            self._send_json(200, self._view(m.group(1)))
            return
        self._send_json(404, {"detail": "not found"})

    def do_POST(self):
        if self.path != "/devices":
            self._send_json(404, {"detail": "not found"})
            return
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.declared.setdefault(body["device_id"], None)
        self._send_json(201, self._view(body["device_id"]))

    def do_PUT(self):
        m = re.fullmatch(r"/devices/([^/]+)/update", self.path)
        if not m:
            self._send_json(404, {"detail": "not found"})
            return
        dev = m.group(1)
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.declared[dev] = body["length"]
        # 快照在挂起之前生成：与真服务“写事务内生成快照、commit 后才进挂点”
        # 的语义一致 —— 挂起只延迟返回，响应不含窗口内的后续写入
        view = self._view(dev)
        held = self._pause_point("declare_update")

        if self.mode == "stall_b" and not held:
            # 竞争请求（B）：接受后响应永不结束（传输停滞）
            time.sleep(120)
            return
        if self.mode == "truncated_a" and held:
            # 首请求（A）：放行后只发含 "declared_length":3 的残缺前缀，
            # Content-Length 仍声明完整长度 -> curl 以 18（partial file）失败
            full = json.dumps(view, separators=(",", ":")).encode()
            key = b'"declared_length":'
            i = full.index(key, full.index(key) + 1) + len(key)  # slot1 值的起点
            while full[i:i + 1].isdigit():
                i += 1
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(full)))
            self.end_headers()
            self.wfile.write(full[:i])  # 前缀恰含完整 "declared_length":3
            self.close_connection = True
            return
        self._send_json(200, view)


def start_stub(mode, pause_dir):
    StubHandler.mode = mode
    StubHandler.pause_dir = pause_dir
    StubHandler.declared = {}
    server = ThreadingHTTPServer(("127.0.0.1", 0), StubHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def run_script(port, pause_dir, out_dir, wait_max, req_max, a_max, timeout):
    env = os.environ.copy()
    env.update({
        "BASE_URL": f"http://127.0.0.1:{port}",
        "FIRMWARE_TEST_PAUSE_DIR": pause_dir,
        "OUT_DIR": out_dir,
        "WAIT_MAX": str(wait_max),
        "REQ_MAX": str(req_max),
        "A_MAX": str(a_max),
        "DEV": f"stub-{os.getpid()}-{time.time_ns()}",
    })
    t0 = time.monotonic()
    proc = subprocess.run(["bash", SCRIPT], env=env,
                          capture_output=True, text=True, timeout=timeout)
    return proc, time.monotonic() - t0


def pause_dir_clean(pause_dir):
    return (not os.path.exists(os.path.join(pause_dir, "declare_update"))
            and not os.path.exists(os.path.join(pause_dir, "declare_update.held")))


def no_leftover_curl(port):
    """脚本退出后不应残留访问桩端口的 curl 进程（后台请求已终止并回收）。"""
    if not shutil.which("pgrep"):
        print("  skip - pgrep 不可用，跳过后台 curl 残留检查")
        return True
    r = subprocess.run(["pgrep", "-f", f"curl .*127.0.0.1:{port}"],
                       capture_output=True)
    return r.returncode != 0


print("== 1. 成功路径：完整交错交互，必须 PASS（3、6、终态 6） ==")
with tempfile.TemporaryDirectory() as pause_dir, \
        tempfile.TemporaryDirectory() as out_dir:
    server = start_stub("ok", pause_dir)
    port = server.server_address[1]
    try:
        proc, elapsed = run_script(port, pause_dir, out_dir,
                                   wait_max=10, req_max=5, a_max=30, timeout=60)
    finally:
        server.shutdown()
        server.server_close()
    print(proc.stdout)
    check("exit 0", proc.returncode == 0, (proc.returncode, proc.stderr))
    check("PASS printed", "PASS" in proc.stdout, proc.stdout)
    check("A=3 / B=6 / final=6",
          "A 响应 declared_length = 3" in proc.stdout
          and "B 响应 declared_length = 6" in proc.stdout
          and "最终数据库 declared_length = 6" in proc.stdout, proc.stdout)
    check("pause dir clean", pause_dir_clean(pause_dir))
    check("no leftover curl", no_leftover_curl(port))

print("== 2. 竞争停滞：B 的响应永不结束，脚本须在 REQ_MAX 内报错而非挂住 ==")
with tempfile.TemporaryDirectory() as pause_dir, \
        tempfile.TemporaryDirectory() as out_dir:
    server = start_stub("stall_b", pause_dir)
    port = server.server_address[1]
    try:
        # A_MAX=30：若脚本在 B 停滞时不主动终止后台请求而是傻等 A 的预算，
        # 耗时会接近 30s；正常应在 REQ_MAX(2s) 左右报错退出
        proc, elapsed = run_script(port, pause_dir, out_dir,
                                   wait_max=5, req_max=2, a_max=30, timeout=45)
    finally:
        server.shutdown()
        server.server_close()
    print(proc.stdout)
    check("non-zero exit", proc.returncode != 0, (proc.returncode, proc.stdout))
    check("no PASS", "PASS" not in proc.stdout, proc.stdout)
    check("fails fast instead of hanging (well below A_MAX)",
          elapsed < 20, f"elapsed={elapsed:.1f}s")
    check("marker and .held cleaned", pause_dir_clean(pause_dir))
    check("background request killed and reaped", no_leftover_curl(port))

print("== 3. 首响应残缺：A 放行后只收到含 declared_length=3 的残缺响应"
      "（curl 非零），不得误报 PASS ==")
with tempfile.TemporaryDirectory() as pause_dir, \
        tempfile.TemporaryDirectory() as out_dir:
    server = start_stub("truncated_a", pause_dir)
    port = server.server_address[1]
    try:
        proc, elapsed = run_script(port, pause_dir, out_dir,
                                   wait_max=5, req_max=5, a_max=15, timeout=45)
    finally:
        server.shutdown()
        server.server_close()
    print(proc.stdout)
    # 修复前：wait 丢弃 curl 退出码，残缺响应里的 "declared_length":3
    # 被采信，脚本误报 PASS 并以 0 退出
    check("non-zero exit", proc.returncode != 0, (proc.returncode, proc.stdout))
    check("no PASS", "PASS" not in proc.stdout, proc.stdout)
    check("fails fast", elapsed < 20, f"elapsed={elapsed:.1f}s")
    check("marker and .held cleaned", pause_dir_clean(pause_dir))
    check("no leftover curl", no_leftover_curl(port))

print(f"\nALL {PASS} CHECKS PASSED")
