"""端到端测试：用 FastAPI TestClient 覆盖全部需求场景。

运行：python test_api.py
"""
import base64
import importlib
import os
import sqlite3
import sys
import tempfile
import threading
import time
from contextlib import contextmanager

TMP = tempfile.mkdtemp(prefix="fwapi-")
os.environ["FIRMWARE_DB_PATH"] = os.path.join(TMP, "test.db")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import app.main as m  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

m.init_db()  # 生产环境由 lifespan 调用；此处避免 TestClient 未进上下文

PASS = 0


def check(name, cond, extra=""):
    global PASS
    assert cond, f"FAIL: {name} {extra}"
    PASS += 1
    print(f"  ok - {name}")


def b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


def new_client():
    return TestClient(m.app)


client = new_client()

print("== 1. 创建设备：初始镜像确认入库，限制 64 KiB ==")
fw_v1 = bytes(range(256)) * 4  # 1024 字节
r = client.post("/devices", json={
    "device_id": "dev1",
    "initial_image_base64": b64(fw_v1),
    "initial_sha256": __import__("hashlib").sha256(fw_v1).hexdigest(),
})
check("create 201", r.status_code == 201, r.status_code)
st = r.json()
check("initial confirmed slot0", st["confirmed_slot"] == 0 and st["running_slot"] == 0)
check("slot0 confirmed w/ digest",
      st["slots"]["slot0"]["status"] == "confirmed"
      and st["slots"]["slot0"]["image_sha256"] == __import__("hashlib").sha256(fw_v1).hexdigest())
check("slot1 empty", st["slots"]["slot1"]["status"] == "empty"
      and st["pending_token"] is None)

r = client.post("/devices", json={"device_id": "dev1", "initial_image_base64": b64(b"x")})
check("duplicate device 409", r.status_code == 409)

big = bytes(64 * 1024 + 1)
r = client.post("/devices", json={"device_id": "dev-big", "initial_image_base64": b64(big)})
check("image >64KiB 422", r.status_code == 422, r.status_code)

r = client.post("/devices", json={"device_id": "dev-empty", "initial_image_base64": ""})
check("empty image 422", r.status_code == 422)

r = client.post("/devices", json={
    "device_id": "dev-badsum", "initial_image_base64": b64(b"abc"),
    "initial_sha256": "00" * 32})
check("wrong initial sha 422", r.status_code == 422)

r = client.post("/devices", json={
    "device_id": "dev-badsum2", "initial_image_base64": b64(b"abc"),
    "initial_sha256": "not-hex"})
check("malformed initial sha 422", r.status_code == 422)

r = client.put("/devices/dev1/update", json={"length": 10, "sha256": "zz" * 32})
check("malformed declare sha 422", r.status_code == 422)

r = client.post("/devices", json={"device_id": "dev-b64", "initial_image_base64": "not base64!!"})
check("bad base64 422", r.status_code == 422)

r = client.get("/devices/nope")
check("unknown device 404", r.status_code == 404)

print("== 2. 声明更新只写另一槽 ==")
fw_v2 = b"FW2-" + bytes((i * 7 % 256 for i in range(30716)))  # 30720 字节
import hashlib
sha_v2 = hashlib.sha256(fw_v2).hexdigest()

r = client.put("/devices/dev1/update", json={"length": len(fw_v2), "sha256": sha_v2})
check("declare 200", r.status_code == 200, r.status_code)
s1 = r.json()["slots"]["slot1"]
check("slot1 in-progress", s1["status"] == "in-progress"
      and s1["declared_length"] == 30720 and s1["bytes_received"] == 0)
# 未声明设备直接发分块
client.post("/devices", json={"device_id": "dev2", "initial_image_base64": b64(b"hello")})
r = client.put("/devices/dev2/chunks", json={"offset": 0, "data_base64": b64(b"xx")})
check("chunk without declaration 409", r.status_code == 409)

print("== 3. 分块：重传幂等 / 重叠冲突 / 越界 ==")
def put_chunk(off, data):
    return client.put("/devices/dev1/chunks", json={"offset": off, "data_base64": b64(data)})

r = put_chunk(0, fw_v2[:4096])
check("chunk0 accepted", r.status_code == 200 and r.json()["candidate_status"] == "in-progress")
r = put_chunk(0, fw_v2[:4096])
check("identical redelivery 200", r.status_code == 200
      and r.json()["identical_redelivery"] is True)
# 同偏移、前缀相同但短 1 字节：重叠区字节一致（仅更短），允许
r = put_chunk(0, fw_v2[:4095])
check("same prefix shorter chunk allowed", r.status_code == 200
      and r.json()["identical_redelivery"] is False)
# 同偏移、字节不同（重叠区冲突）
bad = bytearray(fw_v2[:4096]); bad[100] ^= 0xFF
r = put_chunk(0, bytes(bad))
check("same offset diff content 409", r.status_code == 409
      and r.json()["detail"]["code"] == "overlap_conflict")
# 部分重叠内容不同
bad2 = bytearray(fw_v2[4090:4110]); bad2[3] ^= 0xFF
r = put_chunk(4090, bytes(bad2))
check("overlapping diff bytes 409", r.status_code == 409)
# 部分重叠内容相同（幂等扩展）
r = put_chunk(4090, fw_v2[4090:4110])
check("overlapping same bytes ok", r.status_code == 200)
# 越界（放在未覆盖区，单纯超声明末端）
r = put_chunk(30719, fw_v2[30719:30720] + b"X")
check("chunk past declared end 422", r.status_code == 422)

print("== 4. 不完整不能启动；摘要错误不能启动，旧镜像仍启动 ==")
r = client.post("/devices/dev1/restart")
j = r.json()
check("incomplete -> old image boots", r.status_code == 200
      and j["result"] == "confirmed" and j["running_slot"] == 0 and j["boot_token"] is None)
st = client.get("/devices/dev1").json()
check("still running confirmed slot0", st["running_slot"] == 0
      and st["pending_token"] is None)

# 用错误摘要声明：传完应得 digest-mismatch
fw_bad = bytes(range(100))
client.put("/devices/dev1/update", json={"length": 100, "sha256": "11" * 32})
r = put_chunk(0, fw_bad)
check("full upload wrong digest -> digest-mismatch",
      r.json()["candidate_status"] == "digest-mismatch" and r.json()["bootable"] is False)
r = client.post("/devices/dev1/restart")
j = r.json()
check("digest-mismatch -> old boots, candidate discarded",
      j["result"] == "confirmed" and j["running_slot"] == 0)
st = client.get("/devices/dev1").json()
check("discarded slot empty", st["slots"]["slot1"]["status"] == "empty"
      and st["slots"]["slot1"]["declared_length"] is None)

print("== 5. 完整且摘要相符 -> 试启，令牌持久化后返回 ==")
client.put("/devices/dev1/update", json={"length": len(fw_v2), "sha256": sha_v2})
for off in range(0, len(fw_v2), 4096):
    r = put_chunk(off, fw_v2[off:off + 4096])
check("last chunk -> complete bootable",
      r.json()["candidate_status"] == "complete" and r.json()["bootable"] is True)

r = client.post("/devices/dev1/restart")
j = r.json()
token = j["boot_token"]
check("trial started on slot1", j["result"] == "trial" and j["running_slot"] == 1
      and isinstance(token, str) and len(token) >= 32 and j["pending_confirmation"] is True)
st = client.get("/devices/dev1").json()
check("state trial-pending, token visible",
      st["trial_state"] == "trial-pending" and st["pending_token"] == token
      and st["running_image"]["slot"] == 1
      and st["running_image"]["sha256"] == sha_v2)

print("== 6. 试启未决：两槽禁止写入 ==")
r = client.put("/devices/dev1/update", json={"length": 10, "sha256": "22" * 32})
check("declare locked 409", r.status_code == 409
      and r.json()["detail"]["code"] == "trial_pending_write_locked")
r = put_chunk(0, fw_v2[:10])
check("chunk locked 409", r.status_code == 409)

print("== 7. 错误令牌 409；正确确认 -> 替换已确认 ==")
r = client.post("/devices/dev1/confirm?token=wrong-token")
check("wrong token 409", r.status_code == 409
      and r.json()["detail"]["code"] == "invalid_token")
r = client.post("/devices/dev1/confirm", params={"token": token})
check("confirm 200", r.status_code == 200, r.status_code)
st = client.get("/devices/dev1").json()
check("confirmed/running slot1, no token",
      st["confirmed_slot"] == 1 and st["running_slot"] == 1
      and st["trial_state"] == "running-confirmed" and st["pending_token"] is None)
check("slot1 confirmed, slot0 superseded",
      st["slots"]["slot1"]["status"] == "confirmed"
      and st["slots"]["slot1"]["image_sha256"] == sha_v2
      and st["slots"]["slot0"]["status"] == "superseded"
      and st["slots"]["slot0"]["image_sha256"] == hashlib.sha256(fw_v1).hexdigest())
r = client.post("/devices/dev1/confirm", params={"token": token})
check("late confirm after success 409", r.status_code == 409)

print("== 8. 更新现在写入另一槽（槽0），试启后不确认再重启 -> 回退废弃 ==")
fw_v3 = b"FW3" + bytes(i % 256 for i in range(4997))
sha_v3 = hashlib.sha256(fw_v3).hexdigest()
client.put("/devices/dev1/update", json={"length": len(fw_v3), "sha256": sha_v3})
st = client.get("/devices/dev1").json()
check("update targets slot0", st["slots"]["slot0"]["status"] == "in-progress"
      and st["slots"]["slot1"]["status"] == "confirmed")
for off in range(0, len(fw_v3), 4096):
    client.put("/devices/dev1/chunks", json={"offset": off, "data_base64": b64(fw_v3[off:off+4096])})
r = client.post("/devices/dev1/restart")
j = r.json()
token3 = j["boot_token"]
check("trial slot0 with token3", j["result"] == "trial" and j["running_slot"] == 0 and token3)

# 未确认再次重启 -> 回退
r = client.post("/devices/dev1/restart")
j = r.json()
check("rollback to slot1", j["result"] == "confirmed" and j["running_slot"] == 1
      and j["boot_token"] is None)
st = client.get("/devices/dev1").json()
check("rollback state clean", st["trial_state"] == "running-confirmed"
      and st["confirmed_slot"] == 1 and st["pending_token"] is None
      and st["slots"]["slot0"]["status"] == "empty")
r = client.post("/devices/dev1/confirm", params={"token": token3})
check("late confirm after rollback 409", r.status_code == 409
      and r.json()["detail"]["code"] == "no_trial_pending")

print("== 9. 服务重启（重新导入模块/重开连接）后状态保留 ==")
# 模拟分块中途服务重启：声明并传一半
fw_v4 = b"FW4" + bytes(i % 256 for i in range(19997))
sha_v4 = hashlib.sha256(fw_v4).hexdigest()
client.put("/devices/dev1/update", json={"length": len(fw_v4), "sha256": sha_v4})
client.put("/devices/dev1/chunks", json={"offset": 0, "data_base64": b64(fw_v4[:8000])})
# 进程重启：丢弃全部连接/模块级状态后重新建 client（DB 文件仍在）
importlib.reload(m)
client2 = TestClient(m.app)  # lifespan 会重新 init_db（CREATE IF NOT EXISTS）
with client2:
    pass
st = client2.get("/devices/dev1").json()
check("chunks survive service restart",
      st["slots"]["slot0"]["status"] == "in-progress"
      and st["slots"]["slot0"]["bytes_received"] == 8000)
# 接着传完并试启
for off in range(8000, len(fw_v4), 4096):
    client2.put("/devices/dev1/chunks", json={"offset": off, "data_base64": b64(fw_v4[off:off+4096])})
r = client2.post("/devices/dev1/restart")
token4 = r.json()["boot_token"]
check("trial after resumed upload", r.json()["result"] == "trial" and token4)
# 再“重启服务”，trial-pending 与令牌必须还在
importlib.reload(m)
client3 = TestClient(m.app)
st = client3.get("/devices/dev1").json()
check("pending trial survives service restart",
      st["trial_state"] == "trial-pending" and st["pending_token"] == token4
      and st["running_slot"] == 0)
# 服务重启后仍禁止写入
r = client3.put("/devices/dev1/update", json={"length": 1, "sha256": "33" * 32})
check("still write-locked after service restart", r.status_code == 409)
r = client3.post("/devices/dev1/confirm", params={"token": token4})
check("confirm works after service restart", r.status_code == 200)
st = client3.get("/devices/dev1").json()
check("now confirmed slot0 fw_v4", st["confirmed_slot"] == 0
      and st["running_image"]["sha256"] == sha_v4)

print("== 10. 并发：多线程同时写声明/分块/重启，不产生损坏状态 ==")
client3.put("/devices/dev1/update", json={"length": 300, "sha256": "aa" * 32})
errors = []
def worker(n):
    try:
        c = TestClient(m.app)
        r = c.put("/devices/dev1/chunks", json={"offset": 100 * n, "data_base64": b64(b"c" * 100)})
        if r.status_code not in (200, 409):
            errors.append((n, r.status_code, r.text))
    except Exception as e:  # 任何序列化失败都不可接受
        errors.append((n, "exc", repr(e)))

ts = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
[t.start() for t in ts]; [t.join() for t in ts]
check("concurrent chunks no 5xx/exception", not errors, errors)
st = client3.get("/devices/dev1").json()
check("300 bytes assembled, digest wrong -> mismatch",
      st["slots"]["slot1"]["bytes_received"] == 300
      and st["slots"]["slot1"]["status"] == "digest-mismatch")

# 并发重启/确认竞争：只能有一个结果方向，且库状态一致
client3.put("/devices/dev1/update", json={"length": 300, "sha256": hashlib.sha256(b"c"*300).hexdigest()})
for i in range(3):
    client3.put("/devices/dev1/chunks", json={"offset": 100*i, "data_base64": b64(b"c"*100)})
st = client3.get("/devices/dev1").json()
check("candidate complete", st["slots"]["slot1"]["status"] == "complete")
tok = {"v": None}
def restart_once():
    c = TestClient(m.app)
    r = c.post("/devices/dev1/restart")
    if r.json().get("boot_token"):
        tok["v"] = r.json()["boot_token"]
def confirm_once():
    # 等令牌出现后确认；restart 与 confirm 并发只可能产生合法终态
    for _ in range(100):
        if tok["v"]:
            break
    if tok["v"]:
        c = TestClient(m.app)
        c.post("/devices/dev1/confirm", params={"token": tok["v"]})
t1 = threading.Thread(target=restart_once)
t2 = threading.Thread(target=confirm_once)
t1.start(); t2.start(); t1.join(); t2.join()
st = client3.get("/devices/dev1").json()
check("post-race state legal",
      st["trial_state"] in ("running-confirmed", "trial-pending"))
if st["trial_state"] == "trial-pending":
    # 确认没赶上：补确认收尾
    r = client3.post("/devices/dev1/confirm", params={"token": st["pending_token"]})
    check("cleanup confirm", r.status_code == 200)

print("== 11. 列表/候选查询/健康 ==")
check("health", client3.get("/health").json()["status"] == "ok")
check("device list", set(client3.get("/devices").json()["devices"]) >= {"dev1", "dev2"})
cand = client3.get("/devices/dev1/candidate").json()
final_st = client3.get("/devices/dev1").json()
expected_candidate = 1 - final_st["confirmed_slot"]
check("candidate view",
      cand["candidate_slot"] == expected_candidate
      and "chunks" in cand and cand["bootable"] is False, cand)

print("== 12. 受控交错：成功响应必须是本次操作完成时的快照，不混入后续写入 ==")

# 挂点在“写事务已 commit、响应尚未返回”时触发。替换它后，第一个命中的
# 请求会阻塞在 gate 上，测试趁此窗口让第二个请求完整提交，再放行第一个，
# 从而确定性地构造出“后写在前写返回查询之前落库”的交错。
#
# 确定性顺序（与 scripts/race_repro.sh 一致）：
#   1. 启动首请求线程；
#   2. 等 parked 事件 —— 首请求已提交且确实停在返回前。不能用数据库可见
#      代替这一确认：首请求可能在 commit 之后、进入挂点之前被调度出去，
#      此时若就启动竞争线程，竞争请求会抢占“第一个”名额并挂起，而测试
#      又在等它完成，形成等待环；
#   3. 再启动竞争线程并等它完整返回；
#   4. 确认首请求仍未返回，才 set(gate) 放行。
@contextmanager
def paused_first_commit(pre_park_delay=0.0):
    gate = threading.Event()    # 主线程放行信号
    parked = threading.Event()  # 首个提交者已进入挂点（已提交且停在返回前）
    pick_lock = threading.Lock()
    state = {"first": True}

    def hook(_name):
        # 仅第一个提交者阻塞；窗口内完成的后续请求直接放行返回
        with pick_lock:
            mine = state["first"]
            state["first"] = False
        if mine:
            if pre_park_delay:
                # 模拟“commit 之后、进入挂点之前被调度出去”：parked 尚未
                # 置位，测试必须停在 parked.wait 上而不是凭数据库可见放行
                time.sleep(pre_park_delay)
            parked.set()
            gate.wait(timeout=10)

    saved = m.after_commit_hook
    m.after_commit_hook = hook
    try:
        yield parked, gate
    finally:
        gate.set()          # 即使断言失败也放行，避免残留守护线程挂死
        m.after_commit_hook = saved


def start(fn, *args, box=None):
    def run():
        try:
            if box is not None:
                box["v"] = fn(*args)
        except BaseException as e:  # 失败留待 join 后在主线程断言
            if box is not None:
                box["exc"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t


print("-- 12a. 两次合法声明（长度 3、6）：响应分别为 3、6，终态为 6 --")
img3 = b"abc"
img6 = b"abcdef"
sha3 = hashlib.sha256(img3).hexdigest()
sha6 = hashlib.sha256(img6).hexdigest()
client.post("/devices", json={"device_id": "race-update", "initial_image_base64": b64(b"init")})

boxA, boxB = {}, {}
# pre_park_delay 模拟首请求 commit 后被调度出去：parked 迟迟不置位，
# 测试必须靠 parked 事件确认挂起，而不是凭数据库可见放行竞争请求。
with paused_first_commit(pre_park_delay=0.3) as (parked, gate):
    tA = start(lambda: TestClient(m.app).put(
        "/devices/race-update/update", json={"length": 3, "sha256": sha3}), box=boxA)
    # 等 A 已提交且停在返回前（挂点确认），再让 B 完整跑完
    check("A committed and parked before its response returns",
          parked.wait(timeout=10))
    check("A's write visible in DB (length 3)",
          client.get("/devices/race-update").json()["slots"]["slot1"]["declared_length"] == 3)

    tB = start(lambda: TestClient(m.app).put(
        "/devices/race-update/update", json={"length": 6, "sha256": sha6}), box=boxB)
    tB.join(timeout=10)
    check("B fully completes (length 6) while A still paused",
          not tB.is_alive()
          and client.get("/devices/race-update").json()["slots"]["slot1"]["declared_length"] == 6)
    check("A response still not returned before release", tA.is_alive())
    gate.set()
    tA.join(timeout=10)
    check("A returns after release", not tA.is_alive())

check("no thread raised", "exc" not in boxA and "exc" not in boxB, (boxA, boxB))
ra, rb = boxA["v"], boxB["v"]
check("both responses 200", ra.status_code == 200 and rb.status_code == 200,
      (ra.status_code, rb.status_code))
a1 = ra.json()["slots"]["slot1"]
b1 = rb.json()["slots"]["slot1"]
check("A snapshot shows its own declaration (3)",
      a1["declared_length"] == 3 and a1["declared_sha256"] == sha3
      and a1["bytes_received"] == 0, a1)
check("B snapshot shows its own declaration (6)",
      b1["declared_length"] == 6 and b1["declared_sha256"] == sha6, b1)
final = client.get("/devices/race-update").json()["slots"]["slot1"]
check("final DB state equals serial execution (6, chunks discarded)",
      final["declared_length"] == 6 and final["declared_sha256"] == sha6
      and final["status"] == "in-progress" and final["bytes_received"] == 0, final)

print("-- 12b. POST /devices：创建响应保持初始状态，不含随后的声明 --")
img_c = b"fw-create"
sha_c = hashlib.sha256(img_c).hexdigest()
boxC, boxD = {}, {}
with paused_first_commit() as (parked, gate):
    tC = start(lambda: TestClient(m.app).post(
        "/devices", json={"device_id": "race-create",
                          "initial_image_base64": b64(img_c)}), box=boxC)
    check("create committed and parked before its response returns",
          parked.wait(timeout=10))
    check("create visible in DB",
          "race-create" in client.get("/devices").json()["devices"])

    tD = start(lambda: TestClient(m.app).put(
        "/devices/race-create/update", json={"length": 99, "sha256": "44" * 32}), box=boxD)
    tD.join(timeout=10)
    check("later declaration landed (99) before create response returns",
          not tD.is_alive()
          and client.get("/devices/race-create").json()["slots"]["slot1"]["declared_length"] == 99)
    check("create response still not returned before release", tC.is_alive())
    gate.set()
    tC.join(timeout=10)
    check("create returns after release", not tC.is_alive())

check("no thread raised", "exc" not in boxC and "exc" not in boxD, (boxC, boxD))
rc, rd = boxC["v"], boxD["v"]
check("create 201 / declare 200", rc.status_code == 201 and rd.status_code == 200,
      (rc.status_code, rd.status_code))
cv = rc.json()
check("create response is the initial snapshot (slot0 confirmed, slot1 empty)",
      cv["confirmed_slot"] == 0 and cv["running_slot"] == 0
      and cv["slots"]["slot0"]["status"] == "confirmed"
      and cv["slots"]["slot0"]["image_length"] == len(img_c)
      and cv["slots"]["slot1"]["status"] == "empty"
      and cv["slots"]["slot1"]["declared_length"] is None, cv)
final = client.get("/devices/race-create").json()["slots"]["slot1"]
check("final DB includes the later declaration (99)",
      final["declared_length"] == 99, final)

print("-- 12c. POST /confirm：确认响应不带入随后的声明，等价串行顺序 confirm->declare --")
# 准备一个 trial-pending 设备：slot0 初始镜像，slot1 完整候选
img_old = b"old-image"
img_new = b"new-image!!"
sha_new = hashlib.sha256(img_new).hexdigest()
client.post("/devices", json={"device_id": "race-confirm",
                              "initial_image_base64": b64(img_old)})
client.put("/devices/race-confirm/update",
           json={"length": len(img_new), "sha256": sha_new})
client.put("/devices/race-confirm/chunks",
           json={"offset": 0, "data_base64": b64(img_new)})
tok = client.post("/devices/race-confirm/restart").json()["boot_token"]
check("race-confirm trial pending",
      client.get("/devices/race-confirm").json()["trial_state"] == "trial-pending"
      and bool(tok))

boxE, boxF = {}, {}
with paused_first_commit() as (parked, gate):
    tE = start(lambda: TestClient(m.app).post(
        "/devices/race-confirm/confirm", params={"token": tok}), box=boxE)
    check("confirm committed and parked before its response returns",
          parked.wait(timeout=10))
    check("confirm visible in DB (running-confirmed)",
          client.get("/devices/race-confirm").json()["trial_state"] == "running-confirmed")

    # 确认已解锁写入：针对新的候选槽（旧确认槽 0）声明 7 字节
    tF = start(lambda: TestClient(m.app).put(
        "/devices/race-confirm/update", json={"length": 7, "sha256": "55" * 32}), box=boxF)
    tF.join(timeout=10)
    check("post-confirm declaration landed (7) on slot0 before confirm response returns",
          not tF.is_alive()
          and client.get("/devices/race-confirm").json()["slots"]["slot0"]["declared_length"] == 7)
    check("confirm response still not returned before release", tE.is_alive())
    gate.set()
    tE.join(timeout=10)
    check("confirm returns after release", not tE.is_alive())

check("no thread raised", "exc" not in boxE and "exc" not in boxF, (boxE, boxF))
re_, rf = boxE["v"], boxF["v"]
check("confirm 200 / declare 200", re_.status_code == 200 and rf.status_code == 200,
      (re_.status_code, rf.status_code))
ev = re_.json()
check("confirm snapshot: slot1 confirmed, slot0 superseded w/o later declaration",
      ev["confirmed_slot"] == 1 and ev["running_slot"] == 1
      and ev["trial_state"] == "running-confirmed" and ev["pending_token"] is None
      and ev["slots"]["slot1"]["status"] == "confirmed"
      and ev["slots"]["slot1"]["image_sha256"] == sha_new
      and ev["slots"]["slot0"]["status"] == "superseded"
      and ev["slots"]["slot0"]["declared_length"] is None, ev)
final = client.get("/devices/race-confirm").json()
check("final DB = serial result: confirmed slot1 + later declaration on slot0",
      final["confirmed_slot"] == 1 and final["trial_state"] == "running-confirmed"
      and final["slots"]["slot0"]["status"] == "in-progress"
      and final["slots"]["slot0"]["declared_length"] == 7
      and final["slots"]["slot1"]["status"] == "confirmed", final)

print("== 13. commit 失败不得报成功：响应 5xx，库状态保持操作前，服务仍可用 ==")
client.post("/devices", json={"device_id": "commit-fail", "initial_image_base64": b64(b"base")})
client.put("/devices/commit-fail/update", json={"length": 4, "sha256": "66" * 32})
before = client.get("/devices/commit-fail").json()["slots"]["slot1"]
check("precondition declared 4", before["declared_length"] == 4, before)

real_connect = m.sqlite3.connect
client500 = TestClient(m.app, raise_server_exceptions=False)
try:
    class FailingConn:
        """代理连接：第一次 commit 抛错，模拟提交失败（此前写入全部随回滚丢弃）。"""

        def __init__(self, real):
            object.__setattr__(self, "_real", real)
            object.__setattr__(self, "_armed", True)

        def commit(self):
            if object.__getattribute__(self, "_armed"):
                object.__setattr__(self, "_armed", False)
                self._real.rollback()
                raise sqlite3.OperationalError("simulated commit failure")
            return self._real.commit()

        def __getattr__(self, name):
            return getattr(object.__getattribute__(self, "_real"), name)

        def __setattr__(self, name, value):
            setattr(object.__getattribute__(self, "_real"), name, value)

    def failing_connect(*a, **kw):
        return FailingConn(real_connect(*a, **kw))

    m.sqlite3.connect = failing_connect
    r = client500.put("/devices/commit-fail/update",
                      json={"length": 8, "sha256": "77" * 32})
finally:
    m.sqlite3.connect = real_connect

check("failed commit -> 5xx, not 2xx", 500 <= r.status_code < 600, r.status_code)
after = client.get("/devices/commit-fail").json()["slots"]["slot1"]
check("DB unchanged after failed commit (still 4)",
      after["declared_length"] == 4 and after["declared_sha256"] == "66" * 32, after)
check("service healthy after commit failure", client.get("/health").status_code == 200)
r = client.put("/devices/commit-fail/update", json={"length": 8, "sha256": "77" * 32})
check("subsequent request succeeds once storage is fine",
      r.status_code == 200 and r.json()["slots"]["slot1"]["declared_length"] == 8)

print(f"\nALL {PASS} CHECKS PASSED")
