"""
双槽固件试启动 HTTP API（设备模拟器，无硬件）。

槽位模型
--------
每个设备有 A/B 两个槽（0 / 1），任一时刻一个为“已确认镜像”（confirmed），
更新只能写入另一个槽（candidate / staging）。

试启动状态机
~~~~~~~~~~~~
- 无试启未决（state = running-confirmed）：运行的就是已确认镜像，两槽允许写。
- 试启未决（state = trial-pending）：候选槽已尝试启动，等待确认；
  在此状态下两槽禁止写入。
  * POST /confirm 且令牌正确   -> 候选升级为已确认，旧槽变为 superseded。
  * 再次 POST /restart 未确认 -> 回到旧镜像，候选槽废弃清空，令牌失效。
  * 迟到确认（已回退或令牌错） -> 409 Conflict。

持久化
~~~~~~
SQLite（WAL 模式）。已确认镜像、候选分块、声明、试启令牌全部入库；
服务重启（进程重启）不丢状态，只有设备 POST /restart 才改变启动状态。

并发
~~~~
所有写事务以 BEGIN IMMEDIATE 获取库级写锁，配合 busy_timeout，
操作序列等价于某种串行执行。写接口的成功响应在**同一写事务内**生成
状态快照，仅在 commit 成功后返回（不另开连接回读），所以响应反映的是
本次操作完成时的状态：即使响应尚未发出时下一个写请求已经提交，其写入
也不会混入本次响应快照。commit 失败直接抛出（5xx），不会返回成功。
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import os
import secrets
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# 常量与配置
# ---------------------------------------------------------------------------

MAX_IMAGE_SIZE = 64 * 1024  # 每个镜像不超过 64 KiB
DB_PATH = os.environ.get("FIRMWARE_DB_PATH", "/data/firmware.db")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# 受控交错挂点：写事务 commit 成功、响应返回之前调用（入参为接口名）。
# 默认空操作，生产环境零行为变化。仅在显式设置环境变量
# FIRMWARE_TEST_PAUSE_DIR=<目录> 时启用：若该目录下存在与接口同名的标记文件
# （declare_update / create_device / confirm），**第一个**命中的请求通过
# 原子 rename 接管标记（变为 <name>.held）并在“已提交但未返回”的窗口阻塞，
# 直到 .held 文件被删除；后续请求见不到原标记，直接放行。
#
# 与复现脚本配合的协议（顺序不可颠倒）：
#   1. 脚本放置标记 <name>，再发出首请求；
#   2. 脚本必须等到 <name>.held **出现** —— 这才是“首请求已提交且停在
#      返回前”的确认。不能用数据库可见代替：首请求可能在 commit 之后、
#      执行本钩子之前被调度出去，此时标记尚未被接管，若就此发出竞争请求，
#      竞争请求会抢占标记并挂起，而脚本又在等它返回，形成等待环；
#   3. 脚本再发竞争请求并等其完整返回（它见不到标记，直接放行）；
#   4. 确认首请求仍未返回后，删除 <name>.held 放行。
# 挂起设有安全上限（默认 30 秒，可用 FIRMWARE_TEST_PAUSE_MAX_SECONDS 调整）：
# 即使脚本异常退出、忘了删除 .held，服务线程也不会被无限占用。
PAUSE_DIR = os.environ.get("FIRMWARE_TEST_PAUSE_DIR")


def _pause_max_seconds() -> float:
    try:
        return float(os.environ.get("FIRMWARE_TEST_PAUSE_MAX_SECONDS", "30"))
    except ValueError:
        return 30.0


PAUSE_MAX_SECONDS = _pause_max_seconds()


def after_commit_hook(name: str) -> None:
    pause_dir = PAUSE_DIR
    if not pause_dir:
        return
    marker = os.path.join(pause_dir, name)
    held = os.path.join(pause_dir, name + ".held")
    try:
        # 原子接管：只有一个请求能成功；其余请求直接放行返回。
        # rename 成功即“已提交且停在返回前”的确认信号（.held 出现）。
        os.rename(marker, held)
    except OSError:
        # 没有标记（含 FileNotFoundError）：直接放行
        return
    deadline = time.monotonic() + PAUSE_MAX_SECONDS
    while os.path.exists(held):
        if time.monotonic() >= deadline:
            # 超时自保：宁可让复现失败，也不无限挂起服务线程
            return
        time.sleep(0.01)


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = _connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS devices (
                device_id      TEXT PRIMARY KEY,
                confirmed_slot INTEGER NOT NULL CHECK (confirmed_slot IN (0, 1)),
                running_slot   INTEGER NOT NULL CHECK (running_slot   IN (0, 1)),
                trial_state    TEXT NOT NULL CHECK (trial_state IN
                                 ('running-confirmed', 'trial-pending')),
                trial_token    TEXT,
                boot_count     INTEGER NOT NULL DEFAULT 0,
                created_at     TEXT NOT NULL
            );

            -- 每设备两个槽：image 为已组装落盘的镜像（分块仅在 staging 表）
            CREATE TABLE IF NOT EXISTS slots (
                device_id    TEXT NOT NULL REFERENCES devices(device_id),
                slot         INTEGER NOT NULL CHECK (slot IN (0, 1)),
                image        BLOB,
                stage_status TEXT CHECK (stage_status IN
                                 ('in-progress', 'complete',
                                  'digest-mismatch', 'superseded')),
                declared_len INTEGER,
                declared_sha TEXT,
                updated_at   TEXT NOT NULL,
                PRIMARY KEY (device_id, slot)
            );

            -- Base64 分块解码后的原始字节，按偏移落库
            CREATE TABLE IF NOT EXISTS chunks (
                device_id TEXT NOT NULL REFERENCES devices(device_id),
                slot      INTEGER NOT NULL CHECK (slot IN (0, 1)),
                offset    INTEGER NOT NULL CHECK (offset >= 0),
                data      BLOB NOT NULL,
                PRIMARY KEY (device_id, slot, offset)
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="双槽固件试启动 API",
    version="1.0.0",
    description="模拟 A/B 双槽设备的固件更新、试启动、确认与回退。",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------

def _validate_sha256(v: str) -> str:
    digest = v.strip().lower()
    if len(digest) != 64:
        raise ValueError("sha256 必须是 64 个十六进制字符")
    try:
        int(digest, 16)
    except ValueError:
        raise ValueError("sha256 只能包含十六进制字符")
    return digest


class CreateDevice(BaseModel):
    device_id: str = Field(..., min_length=1, max_length=128)
    initial_image_base64: str = Field(..., description="已确认初始镜像（Base64），<= 64 KiB")
    initial_sha256: Optional[str] = Field(None, description="可选，提供则校验初始镜像摘要")


class DeclareUpdate(BaseModel):
    length: int = Field(..., ge=1, le=MAX_IMAGE_SIZE)
    sha256: str

    @field_validator("sha256")
    @classmethod
    def _sha(cls, v: str) -> str:
        return _validate_sha256(v)


class ChunkIn(BaseModel):
    offset: int = Field(..., ge=0)
    data_base64: str


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------

def _error(code: str, message: str, status_code: int) -> HTTPException:
    return HTTPException(status_code=status_code, detail={"code": code, "message": message})


def _decode_base64(raw_b64: str, what: str) -> bytes:
    try:
        # validate=True：非标准字符（含内部空白）直接报错
        return base64.b64decode(raw_b64, validate=True)
    except (binascii.Error, ValueError):
        raise _error("invalid_base64", f"{what} 不是合法的 Base64", 422)


def _get_device(conn: sqlite3.Connection, device_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM devices WHERE device_id = ?", (device_id,)
    ).fetchone()
    if row is None:
        raise _error("device_not_found", f"设备 {device_id} 不存在", 404)
    return row


def _get_slots(conn: sqlite3.Connection, device_id: str) -> dict[int, sqlite3.Row]:
    rows = conn.execute(
        "SELECT * FROM slots WHERE device_id = ? ORDER BY slot", (device_id,)
    ).fetchall()
    return {r["slot"]: r for r in rows}


def _reset_staging(conn: sqlite3.Connection, device_id: str, slot: int,
                   length: Optional[int], sha: Optional[str]) -> None:
    """清空候选槽的分块与镜像，并写入新声明（或清空声明）。"""
    conn.execute("DELETE FROM chunks WHERE device_id = ? AND slot = ?",
                 (device_id, slot))
    if length is None:
        conn.execute(
            "UPDATE slots SET image = NULL, stage_status = NULL, "
            "declared_len = NULL, declared_sha = NULL, updated_at = ? "
            "WHERE device_id = ? AND slot = ?",
            (utc_now(), device_id, slot),
        )
    else:
        conn.execute(
            "UPDATE slots SET image = NULL, stage_status = 'in-progress', "
            "declared_len = ?, declared_sha = ?, updated_at = ? "
            "WHERE device_id = ? AND slot = ?",
            (length, sha, utc_now(), device_id, slot),
        )


def _covered_length(chunks: list[sqlite3.Row], total: int) -> int:
    """合并分块区间，返回 [0, total) 内被覆盖的字节数。"""
    intervals = sorted((c["offset"], c["offset"] + len(c["data"])) for c in chunks)
    covered = 0
    cur_start = cur_end = 0
    for start, end in intervals:
        start = max(start, 0)
        end = min(end, total)
        if start >= end:
            continue
        if start <= cur_end:
            if end > cur_end:
                cur_end = end
        else:
            covered += cur_end - cur_start
            cur_start, cur_end = start, end
    covered += cur_end - cur_start
    return max(covered, 0)


def _assemble(chunks: list[sqlite3.Row], total: int) -> bytes:
    buf = bytearray(total)
    for c in chunks:
        c_data = bytes(c["data"])
        start = c["offset"]
        end = start + len(c_data)
        if start < 0 or end > total:
            raise _error("chunk_out_of_bounds", "分块超出声明长度范围", 422)
        buf[start:end] = c_data
    return bytes(buf)


def _slot_status(row: sqlite3.Row) -> str:
    """对外暴露的槽状态。"""
    stage = row["stage_status"]
    if stage is None:
        return "confirmed" if row["image"] is not None else "empty"
    if stage == "superseded":
        return "superseded"
    return stage  # in-progress / complete / digest-mismatch


def _device_view(conn: sqlite3.Connection, dev: sqlite3.Row) -> dict[str, Any]:
    slot_rows = _get_slots(conn, dev["device_id"])
    slots: dict[str, Any] = {}
    for s in (0, 1):
        row = slot_rows[s]
        image: Optional[bytes] = row["image"]
        chunks = conn.execute(
            "SELECT * FROM chunks WHERE device_id = ? AND slot = ? "
            "ORDER BY offset", (dev["device_id"], s)
        ).fetchall()
        declared_len = row["declared_len"]
        covered = None
        if row["stage_status"] in ("in-progress", "digest-mismatch") and declared_len:
            covered = _covered_length(chunks, declared_len)
        slots[f"slot{s}"] = {
            "status": _slot_status(row),
            "image_sha256": hashlib.sha256(image).hexdigest() if image else None,
            "image_length": len(image) if image else None,
            "declared_length": declared_len,
            "declared_sha256": row["declared_sha"],
            "bytes_received": covered,
        }

    running_row = slot_rows[dev["running_slot"]]
    return {
        "device_id": dev["device_id"],
        "trial_state": dev["trial_state"],
        "confirmed_slot": dev["confirmed_slot"],
        "running_slot": dev["running_slot"],
        "running_image": {
            "slot": dev["running_slot"],
            "sha256": hashlib.sha256(running_row["image"]).hexdigest(),
            "length": len(running_row["image"]),
        },
        "pending_token": dev["trial_token"] if dev["trial_state"] == "trial-pending" else None,
        "boot_count": dev["boot_count"],
        "slots": slots,
    }


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/devices", status_code=status.HTTP_201_CREATED)
def create_device(body: CreateDevice) -> dict[str, Any]:
    image = _decode_base64(body.initial_image_base64, "initial_image_base64")
    if len(image) == 0:
        raise _error("empty_image", "初始镜像不能为空", 422)
    if len(image) > MAX_IMAGE_SIZE:
        raise _error("image_too_large",
                     f"镜像超过 {MAX_IMAGE_SIZE} 字节上限", 422)
    digest = hashlib.sha256(image).hexdigest()
    if body.initial_sha256 is not None:
        try:
            want = _validate_sha256(body.initial_sha256)
        except ValueError as e:
            raise _error("invalid_sha256", str(e), 422)
        if want != digest:
            raise _error("sha256_mismatch", "初始镜像 SHA-256 与提供的摘要不一致", 422)

    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        exists = conn.execute(
            "SELECT 1 FROM devices WHERE device_id = ?", (body.device_id,)
        ).fetchone()
        if exists:
            raise _error("device_exists", f"设备 {body.device_id} 已存在", 409)
        now = utc_now()
        conn.execute(
            "INSERT INTO devices (device_id, confirmed_slot, running_slot, "
            "trial_state, trial_token, boot_count, created_at) "
            "VALUES (?, 0, 0, 'running-confirmed', NULL, 0, ?)",
            (body.device_id, now),
        )
        conn.execute(
            "INSERT INTO slots (device_id, slot, image, stage_status, "
            "declared_len, declared_sha, updated_at) VALUES (?, 0, ?, NULL, NULL, NULL, ?)",
            (body.device_id, image, now),
        )
        conn.execute(
            "INSERT INTO slots (device_id, slot, image, stage_status, "
            "declared_len, declared_sha, updated_at) VALUES (?, 1, NULL, NULL, NULL, NULL, ?)",
            (body.device_id, now),
        )
        # 在同一写事务内取本次创建完成时的快照，提交后直接返回，
        # 不另开连接回读：之后（如紧接着的声明）不会混入创建响应。
        view = _device_view(conn, _get_device(conn, body.device_id))
        conn.commit()
    finally:
        conn.close()

    after_commit_hook("create_device")
    return view


@app.get("/devices")
def list_devices() -> dict[str, Any]:
    conn = _connect()
    try:
        conn.execute("BEGIN")
        rows = conn.execute("SELECT device_id FROM devices ORDER BY device_id").fetchall()
        conn.commit()
        return {"devices": [r["device_id"] for r in rows]}
    finally:
        conn.close()


@app.get("/devices/{device_id}")
def get_device(device_id: str) -> dict[str, Any]:
    conn = _connect()
    try:
        conn.execute("BEGIN")
        dev = _get_device(conn, device_id)
        view = _device_view(conn, dev)
        conn.commit()
        return view
    finally:
        conn.close()


@app.put("/devices/{device_id}/update", status_code=status.HTTP_200_OK)
def declare_update(device_id: str, body: DeclareUpdate) -> dict[str, Any]:
    """在另一槽声明新固件：长度 + SHA-256。重新声明会废弃该槽已有分块。"""
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        dev = _get_device(conn, device_id)
        if dev["trial_state"] == "trial-pending":
            raise _error("trial_pending_write_locked",
                         "试启未决期间禁止写入两槽（先确认或重启回退）", 409)
        slot = 1 - dev["confirmed_slot"]
        _reset_staging(conn, device_id, slot, body.length, body.sha256)
        # 快照必须在写事务内、commit 之前生成：这样快照只包含本声明的效果。
        # 若在 commit 之后另开连接回读，下一个声明可能已落库，两次响应会
        # 都显示后一次的 declared_length。
        view = _device_view(conn, _get_device(conn, device_id))
        conn.commit()
    finally:
        conn.close()

    after_commit_hook("declare_update")
    return view


@app.put("/devices/{device_id}/chunks", status_code=status.HTTP_200_OK)
def put_chunk(device_id: str, body: ChunkIn) -> dict[str, Any]:
    """按偏移提交一个 Base64 分块。

    - 同偏移同内容重传：幂等，返回 200（identical_redelivery=True）。
    - 与已存分块重叠且字节不同：整块拒绝（409），不写入任何字节。
    - 越过声明末端：422。
    """
    data = _decode_base64(body.data_base64, "data_base64")
    if len(data) == 0:
        raise _error("empty_chunk", "分块内容不能为空", 422)

    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        dev = _get_device(conn, device_id)
        if dev["trial_state"] == "trial-pending":
            raise _error("trial_pending_write_locked",
                         "试启未决期间禁止写入两槽（先确认或重启回退）", 409)

        slot = 1 - dev["confirmed_slot"]
        slot_row = conn.execute(
            "SELECT * FROM slots WHERE device_id = ? AND slot = ?",
            (device_id, slot),
        ).fetchone()
        declared_len = slot_row["declared_len"]
        declared_sha = slot_row["declared_sha"]
        if declared_len is None:
            raise _error("no_declaration",
                         "候选槽尚未声明更新，请先 PUT /update", 409)

        new_start, new_end = body.offset, body.offset + len(data)
        if new_end > declared_len:
            raise _error("chunk_out_of_bounds",
                         f"分块越过声明长度 {declared_len}", 422)

        # 与所有已存分块做重叠字节比较
        existing = conn.execute(
            "SELECT offset, data FROM chunks WHERE device_id = ? AND slot = ?",
            (device_id, slot),
        ).fetchall()
        for ex in existing:
            ex_data = bytes(ex["data"])
            ov_start = max(ex["offset"], new_start)
            ov_end = min(ex["offset"] + len(ex_data), new_end)
            if ov_start < ov_end:
                old_overlap = ex_data[ov_start - ex["offset"]:ov_end - ex["offset"]]
                new_overlap = data[ov_start - new_start:ov_end - new_start]
                if old_overlap != new_overlap:
                    raise _error(
                        "overlap_conflict",
                        f"偏移 {ov_start}..{ov_end} 与已提交字节不一致，整块被拒绝",
                        409,
                    )

        identical = False
        same = conn.execute(
            "SELECT data FROM chunks WHERE device_id = ? AND slot = ? AND offset = ?",
            (device_id, slot, body.offset),
        ).fetchone()
        if same is not None:
            if bytes(same["data"]) == data:
                identical = True  # 同位置同内容重传
            # 内容不同且不重叠（例如同偏移但更短/更长）的情况已由上面的重叠检查拦截
        else:
            conn.execute(
                "INSERT INTO chunks (device_id, slot, offset, data) VALUES (?, ?, ?, ?)",
                (device_id, slot, body.offset, data),
            )

        # 重新评估候选槽完整度
        chunks = conn.execute(
            "SELECT * FROM chunks WHERE device_id = ? AND slot = ? ORDER BY offset",
            (device_id, slot),
        ).fetchall()
        covered = _covered_length(chunks, declared_len)
        if covered == declared_len:
            image = _assemble(chunks, declared_len)
            actual = hashlib.sha256(image).hexdigest()
            if actual == declared_sha:
                conn.execute(
                    "UPDATE slots SET image = ?, stage_status = 'complete', updated_at = ? "
                    "WHERE device_id = ? AND slot = ?",
                    (image, utc_now(), device_id, slot),
                )
            else:
                conn.execute(
                    "UPDATE slots SET stage_status = 'digest-mismatch', updated_at = ? "
                    "WHERE device_id = ? AND slot = ?",
                    (utc_now(), device_id, slot),
                )
        # covered < declared_len：维持 in-progress（digest-mismatch 在补齐为正确
        # 内容后也会经由上面的分支转为 complete）

        final_status = conn.execute(
            "SELECT stage_status FROM slots WHERE device_id = ? AND slot = ?",
            (device_id, slot),
        ).fetchone()["stage_status"]
        conn.commit()

        return {
            "device_id": device_id,
            "slot": slot,
            "offset": body.offset,
            "chunk_length": len(data),
            "identical_redelivery": identical,
            "declared_length": declared_len,
            "bytes_covered": covered,
            "candidate_status": final_status,
            "bootable": final_status == "complete",
        }
    finally:
        conn.close()


@app.get("/devices/{device_id}/candidate")
def candidate_status(device_id: str) -> dict[str, Any]:
    """候选槽的更新进度与是否可试启动。"""
    conn = _connect()
    try:
        conn.execute("BEGIN")
        dev = _get_device(conn, device_id)
        slot = 1 - dev["confirmed_slot"]
        row = conn.execute(
            "SELECT * FROM slots WHERE device_id = ? AND slot = ?",
            (device_id, slot),
        ).fetchone()
        chunks = conn.execute(
            "SELECT offset, length(data) AS len FROM chunks "
            "WHERE device_id = ? AND slot = ? ORDER BY offset",
            (device_id, slot),
        ).fetchall()
        covered = None
        if row["declared_len"] is not None and row["stage_status"] in (
            "in-progress", "digest-mismatch"
        ):
            full_chunks = conn.execute(
                "SELECT * FROM chunks WHERE device_id = ? AND slot = ?",
                (device_id, slot),
            ).fetchall()
            covered = _covered_length(full_chunks, row["declared_len"])
        out = {
            "device_id": device_id,
            "candidate_slot": slot,
            "status": _slot_status(row),
            "declared_length": row["declared_len"],
            "declared_sha256": row["declared_sha"],
            "bytes_received": covered,
            "bootable": row["stage_status"] == "complete",
            "chunks": [{"offset": c["offset"], "length": c["len"]} for c in chunks],
        }
        conn.commit()
        return out
    finally:
        conn.close()


@app.post("/devices/{device_id}/restart", status_code=status.HTTP_200_OK)
def restart(device_id: str) -> JSONResponse:
    """设备重启（注意：不是服务重启）。

    - 候选槽完整且摘要相符（complete）：优先试启候选，生成唯一启动令牌，
      在返回之前持久化 trial-pending 与令牌。
    - 否则（无声明 / 不完整 / 摘要不符 / 处于试启未决再次重启）：
      启动已确认旧镜像；若有废弃候选则清空，未确认的试启作废。
    """
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        dev = _get_device(conn, device_id)
        slots = _get_slots(conn, device_id)
        was_pending = dev["trial_state"] == "trial-pending"

        candidate_slot = 1 - dev["confirmed_slot"]
        candidate = slots[candidate_slot]

        can_trial = (not was_pending) and candidate["stage_status"] == "complete"

        if can_trial:
            # 标记必须在返回响应前落盘
            token = secrets.token_urlsafe(32)
            conn.execute(
                "UPDATE devices SET running_slot = ?, trial_state = 'trial-pending', "
                "trial_token = ?, boot_count = boot_count + 1 WHERE device_id = ?",
                (candidate_slot, token, device_id),
            )
            conn.commit()
            result = "trial"
            running_slot = candidate_slot
            boot_token = token
            running_image = {
                "slot": candidate_slot,
                "sha256": hashlib.sha256(candidate["image"]).hexdigest(),
                "length": len(candidate["image"]),
            }
        else:
            # 回到旧镜像；废弃候选（清空分块/镜像/声明）
            _reset_staging(conn, device_id, candidate_slot, None, None)
            conn.execute(
                "UPDATE devices SET running_slot = confirmed_slot, "
                "trial_state = 'running-confirmed', trial_token = NULL, "
                "boot_count = boot_count + 1 WHERE device_id = ?",
                (device_id,),
            )
            conn.commit()
            result = "confirmed"
            running_slot = dev["confirmed_slot"]
            boot_token = None
            old = slots[dev["confirmed_slot"]]
            running_image = {
                "slot": dev["confirmed_slot"],
                "sha256": hashlib.sha256(old["image"]).hexdigest(),
                "length": len(old["image"]),
            }

        body: dict[str, Any] = {
            "device_id": device_id,
            "result": result,
            "running_slot": running_slot,
            "running_image": running_image,
        }
        if result == "trial":
            body["boot_token"] = boot_token
            body["pending_confirmation"] = True
            body["message"] = "候选镜像已试启动，请凭 boot_token 调用 /confirm"
        else:
            body["boot_token"] = None
            body["pending_confirmation"] = False
            if was_pending:
                body["message"] = "未确认即重启：已回退到旧镜像，候选已废弃"
            else:
                body["message"] = "候选不可启动（不完整或摘要不符）：旧镜像启动"
        return JSONResponse(status_code=200, content=body)
    finally:
        conn.close()


@app.post("/devices/{device_id}/confirm", status_code=status.HTTP_200_OK)
def confirm(device_id: str, token: str) -> dict[str, Any]:
    """凭启动令牌确认试启：候选替换为已确认镜像。

    - 无试启未决 / 已回退 / 令牌错误：409 Conflict（迟到确认亦在此列）。
    """
    if not token:
        raise _error("missing_token", "必须提供 boot_token", 409)

    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        dev = _get_device(conn, device_id)
        if dev["trial_state"] != "trial-pending":
            raise _error("no_trial_pending",
                         "当前没有待确认的试启（可能已因重启回退，确认迟到）", 409)
        if not secrets.compare_digest(dev["trial_token"] or "", token):
            raise _error("invalid_token", "启动令牌不正确或已失效", 409)

        new_slot = dev["running_slot"]      # 候选槽
        old_slot = 1 - new_slot             # 旧已确认槽

        # 候选升级为已确认：清掉其暂存状态与分块，镜像保留在 image 列
        conn.execute("DELETE FROM chunks WHERE device_id = ? AND slot = ?",
                     (device_id, new_slot))
        conn.execute(
            "UPDATE slots SET stage_status = NULL, declared_len = NULL, "
            "declared_sha = NULL, updated_at = ? WHERE device_id = ? AND slot = ?",
            (utc_now(), device_id, new_slot),
        )
        # 旧槽标记 superseded（镜像保留可查询），其分块不应存在，保险清理
        conn.execute("DELETE FROM chunks WHERE device_id = ? AND slot = ?",
                     (device_id, old_slot))
        conn.execute(
            "UPDATE slots SET stage_status = 'superseded', updated_at = ? "
            "WHERE device_id = ? AND slot = ?",
            (utc_now(), device_id, old_slot),
        )
        conn.execute(
            "UPDATE devices SET confirmed_slot = ?, trial_state = 'running-confirmed', "
            "trial_token = NULL WHERE device_id = ?",
            (new_slot, device_id),
        )
        # 在同一写事务内取确认完成时的快照：随后才提交的新声明（确认解锁
        # 之后即可写入）不会被带进本次确认响应。
        view = _device_view(conn, _get_device(conn, device_id))
        conn.commit()
    finally:
        conn.close()

    after_commit_hook("confirm")
    return view
