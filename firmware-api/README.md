# 双槽固件试启动 HTTP API

用 Python / FastAPI + SQLite 模拟 A/B 双槽设备的固件更新、试启动、确认与回退。
**不连接任何硬件**，所有“刷写/重启”都是数据库状态迁移。

## 启动

单条命令（自动构建镜像、初始化持久卷并启动）：

```bash
docker compose up --build -d
```

- 端口：**`8000`**（HTTP）
- API 文档（Swagger UI）：<http://localhost:8000/docs>
- 健康检查：`GET http://localhost:8000/health`
- 持久化：命名卷 `firmware-api_firmware-data` 挂载到容器 `/data`，
  SQLite 文件 `/data/firmware.db`。卷首次启动时自动创建，
  **服务（进程）重启后分块上传进度、试启令牌等状态全部保留**；
  只有对设备调用 `POST /restart` 才算“设备重启”。

停止 / 重置数据：

```bash
docker compose down            # 停止，保留数据卷
docker compose down -v         # 停止并删除数据卷（清空所有设备）
docker compose logs -f         # 查看日志
```

无 Docker 时本地运行（Python 3.11+）：

```bash
pip install -r requirements.txt
FIRMWARE_DB_PATH=./firmware.db uvicorn app.main:app --port 8000
```

## 核心规则

- 每个镜像不超过 **64 KiB**（65536 字节）。
- 创建设备时写入**已确认的初始镜像**（槽 0），槽 1 为空。
- 更新**只能写另一槽**：先声明长度和 SHA-256，再按偏移提交 Base64 分块。
  - 同一偏移、相同内容可任意重传（幂等）。
  - 与已提交分块**重叠字节不同 → 整块拒绝**（`409 overlap_conflict`），不写入。
  - 分块越界（超过声明长度）→ `422`。
  - 重新声明（再次 `PUT /update`）会废弃候选槽已有分块，从头开始。
- 候选槽**字节完整且 SHA-256 相符**才是 `complete`，才可试启动；
  不完整为 `in-progress`，摘要错误为 `digest-mismatch`，二者都不能启动，旧镜像照常启动。
- `POST /restart`（设备重启）：
  - 候选可启动 → **优先试启候选**，返回唯一 `boot_token`；
    返回响应之前已把 `trial-pending` 与令牌持久化。
  - 候选不可启动 → 启动旧已确认镜像；废弃候选。
- 试启未决（`trial-pending`）期间：
  - `POST /confirm?token=...` 令牌正确 → 候选**替换**已确认镜像。
  - 未确认就再次 `POST /restart` → **回到旧镜像、废弃候选、令牌失效**；
    之后迟到的确认返回 **`409`**。
  - 两槽**禁止任何写入**（声明 / 分块都返回 `409 trial_pending_write_locked`）。
- 并发操作经 SQLite `BEGIN IMMEDIATE` 串行化，等价于某种串行执行。
- **成功响应反映“本次操作完成时”的状态**：`PUT /update`、`POST /devices`、
  `POST /confirm` 的响应快照都在各自的写事务内、`commit` 之前生成，
  只有持久化成功才返回 2xx。因此即使第二个请求在第一个请求“已提交但响应
  尚未发出”的窗口内落库，后写入也不会混入第一个响应；提交失败返回 5xx，
  绝不报成功（事务回滚，库保持操作前状态）。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/devices` | 创建设备（带已确认初始镜像，≤64 KiB） |
| GET | `/devices` | 列出设备 ID |
| GET | `/devices/{id}` | 两槽摘要/状态、运行镜像、待确认令牌 |
| PUT | `/devices/{id}/update` | 在另一槽声明更新（长度 + SHA-256） |
| PUT | `/devices/{id}/chunks` | 提交一个 Base64 分块 `{offset, data_base64}` |
| GET | `/devices/{id}/candidate` | 候选槽进度与是否可试启动 |
| POST | `/devices/{id}/restart` | 设备重启：试启候选或回退旧镜像 |
| POST | `/devices/{id}/confirm?token=...` | 凭令牌确认试启 |
| GET | `/health` | 健康检查 |

## HTTP 示例

以下示例使用 `curl` + `jq`，`python3` 用于算摘要 / 切分块。

### 0. 准备一个固件文件（示例 100 KB 会被拒绝，这里用 ≤64 KiB）

```bash
head -c 20000 /dev/urandom > firmware-v1.bin
SHA=$(sha256sum firmware-v1.bin | cut -d' ' -f1)
LEN=$(stat -c%s firmware-v1.bin)
echo "$SHA $LEN"
```

### 1. 创建设备（初始镜像 = 已确认镜像，写入槽 0）

```bash
curl -s -X POST http://localhost:8000/devices \
  -H 'Content-Type: application/json' \
  -d "$(python3 -c "
import json, base64
print(json.dumps({
  'device_id': 'dev-001',
  'initial_image_base64': base64.b64encode(open('firmware-v1.bin','rb').read()).decode(),
}))")" | jq
```

### 2. 在另一槽声明更新（长度 + SHA-256）

```bash
head -c 30000 /dev/urandom > firmware-v2.bin
SHA2=$(sha256sum firmware-v2.bin | cut -d' ' -f1)
LEN2=$(stat -c%s firmware-v2.bin)

curl -s -X PUT http://localhost:8000/devices/dev-001/update \
  -H 'Content-Type: application/json' \
  -d "{\"length\": $LEN2, \"sha256\": \"$SHA2\"}" | jq '.slots.slot1'
```

### 3. 按偏移提交 Base64 分块（每块 4096 字节）

同位置同内容重跑本脚本是安全的（`identical_redelivery: true`）：

```bash
python3 - "$SHA2" <<'PY'
import base64, json, sys, urllib.request

DEV, CHUNK = "dev-001", 4096
data = open("firmware-v2.bin", "rb").read()
for off in range(0, len(data), CHUNK):
    body = json.dumps({
        "offset": off,
        "data_base64": base64.b64encode(data[off:off+CHUNK]).decode(),
    }).encode()
    req = urllib.request.Request(
        f"http://localhost:8000/devices/{DEV}/chunks",
        data=body, method="PUT",
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        print(json.load(r)["candidate_status"], "@", off)
PY
```

重叠但内容不同会被拒：

```bash
# 拿前 100 字节，改掉一个字节后写到偏移 0（与已存内容重叠冲突）
python3 - <<'PY'
import base64, json, urllib.error, urllib.request
b = bytearray(open("firmware-v2.bin","rb").read(100)); b[5] ^= 0xFF
req = urllib.request.Request(
    "http://localhost:8000/devices/dev-001/chunks",
    data=json.dumps({"offset": 0,
        "data_base64": base64.b64encode(bytes(b)).decode()}).encode(),
    method="PUT", headers={"Content-Type": "application/json"})
try:
    urllib.request.urlopen(req)
except urllib.error.HTTPError as e:
    print(e.code, json.load(e))   # 409 overlap_conflict
PY
```

### 4. 查询状态（两槽摘要、状态、运行镜像、待确认令牌）

```bash
curl -s http://localhost:8000/devices/dev-001 | jq
```

### 5. 设备重启 → 优先试启候选，拿到唯一启动令牌

```bash
TOKEN=$(curl -s -X POST http://localhost:8000/devices/dev-001/restart \
  | tee /tmp/restart.json | jq -r '.boot_token')
cat /tmp/restart.json | jq
echo "TOKEN=$TOKEN"
```

此时查询可见 `trial_state: "trial-pending"`、`pending_token` 非空、`running_slot: 1`。
试启未决时写两槽会被拒：

```bash
curl -s -X PUT http://localhost:8000/devices/dev-001/update \
  -H 'Content-Type: application/json' \
  -d "{\"length\": $LEN2, \"sha256\": \"$SHA2\"}" | jq
# 409 trial_pending_write_locked
```

### 6. 确认试启 → 候选替换已确认镜像

```bash
curl -s -X POST "http://localhost:8000/devices/dev-001/confirm?token=$TOKEN" | jq
# confirmed_slot 变为 1；槽 0 变为 superseded
```

## 回退示例（不确认，直接再次重启）

```bash
# 设备当前已确认运行槽 1。再来一版槽 0 的更新并试启：
head -c 12345 /dev/urandom > firmware-v3.bin
SHA3=$(sha256sum firmware-v3.bin | cut -d' ' -f1); LEN3=$(stat -c%s firmware-v3.bin)

curl -s -X PUT http://localhost:8000/devices/dev-001/update \
  -H 'Content-Type: application/json' \
  -d "{\"length\": $LEN3, \"sha256\": \"$SHA3\"}" | jq '.slots.slot0.status'

python3 - <<'PY'
import base64, json, urllib.request
data = open("firmware-v3.bin","rb").read()
for off in range(0, len(data), 4096):
    req = urllib.request.Request(
        "http://localhost:8000/devices/dev-001/chunks",
        data=json.dumps({"offset": off,
            "data_base64": base64.b64encode(data[off:off+4096]).decode()}).encode(),
        method="PUT", headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req)
print("uploaded")
PY

# 试启 v3（槽 0），拿到令牌但“不确认”
RESP=$(curl -s -X POST http://localhost:8000/devices/dev-001/restart)
TOKEN3=$(echo "$RESP" | jq -r .boot_token); echo "$RESP" | jq

# 再次重启：自动回退到旧已确认镜像（槽 1），候选 v3 被废弃
curl -s -X POST http://localhost:8000/devices/dev-001/restart | jq
#   result: "confirmed"，running_slot: 1

# 迟到的确认 -> 409
curl -s -X POST "http://localhost:8000/devices/dev-001/confirm?token=$TOKEN3" | jq
#   409 no_trial_pending
```

候选不完整 / 摘要错误时重启，旧镜像同样直接启动（候选不会被试启）。

## 并发交错复现（受控、确定性）

**问题背景**：同一设备两次合法声明（长度 3、6），若第二个请求在第一个请求
“写事务已提交、响应还在回读”的窗口内落库，旧实现会在**新连接**里回读状态，
导致**两份响应都显示 `declared_length=6`**。修复后写接口在同一写事务内
生成快照，两份响应必须分别为 `3`、`6`，最终库状态为 `6`（等价串行顺序 A→B）。

为了不靠“碰运气”并发来复现，服务内置了一个**默认关闭**的受控挂点：
设置环境变量 `FIRMWARE_TEST_PAUSE_DIR=<目录>` 后，写接口在
“commit 成功、返回响应之前”检查该目录下与接口同名的标记文件
（`declare_update` / `create_device` / `confirm`）；第一个命中的请求会
原子接管标记（重命名为 `<name>.held`）并阻塞，删除 `.held` 即放行，
其余请求不受影响。正常启动（不设该变量）时此挂点为空操作。

复现必须按下面的顺序推进，否则会形成等待环（脚本挂死）：

1. 放置标记后发出首请求；
2. **等到 `<name>.held` 出现**——这才是“首请求已提交且停在返回前”的
   确认。不能用“数据库里查得到”代替：首请求可能在 commit 之后、进入
   挂点之前被调度出去，此时标记尚未被接管，若就此发出竞争请求，竞争
   请求会抢占标记并挂起，而脚本又在等它返回，两头互等；
3. 再发竞争请求并等它完整返回（它见不到标记，直接放行）；
4. 确认首请求的响应仍未返回，才删除 `.held` 放行。

挂起有安全上限（默认 30 秒，可用 `FIRMWARE_TEST_PAUSE_MAX_SECONDS`
调整）：即使脚本异常退出、忘了删除 `.held`，服务线程也不会被无限占用。

### 方式一：Docker Compose（沿用同一持久卷）

覆盖层只增加挂点目录与环境变量，数据仍在命名卷 `firmware-data`：

```bash
# 宿主目录供脚本与容器共享标记文件
mkdir -p /tmp/fw-pause

docker compose -f docker-compose.yml -f docker-compose.race.yml up --build -d

bash scripts/race_repro.sh
```

脚本输出（PASS）：

```
A 响应 declared_length = 3   （期望 3）
B 响应 declared_length = 6   （期望 6）
最终数据库 declared_length = 6 （期望 6）
PASS：响应快照彼此独立，终态等价串行执行（A -> B）。
```

复现结束恢复常规启动（数据卷不变）：

```bash
docker compose -f docker-compose.yml -f docker-compose.race.yml down
docker compose up --build -d        # 不含挂点，FIRMWARE_TEST_PAUSE_DIR 未设置
```

### 方式二：本地运行（无 Docker）

```bash
pip install -r requirements.txt
mkdir -p /tmp/fw-pause /tmp/fw-data
FIRMWARE_DB_PATH=/tmp/fw-data/firmware.db \
FIRMWARE_TEST_PAUSE_DIR=/tmp/fw-pause \
uvicorn app.main:app --port 8000 &

bash scripts/race_repro.sh

kill %1            # 复现结束，停止服务（数据保留在 /tmp/fw-data）
```

脚本默认打 `http://localhost:8000`、标记目录取 `FIRMWARE_TEST_PAUSE_DIR`
（未设置时为 `/tmp/fw-pause`），可用 `BASE_URL` / `FIRMWARE_TEST_PAUSE_DIR`
环境变量覆盖。

超时配置（全部为可配置的有限等待，脚本不会无限挂起）：

- `WAIT_MAX`（默认 30 秒）：等 `.held` 出现、放行后等首请求返回的预算；
- `REQ_MAX`（默认 10 秒）：单次普通 HTTP 调用（健康检查、创建设备、
  状态查询、竞争声明 B）的 curl 上限；
- `A_MAX`（默认 `2*WAIT_MAX + 2*REQ_MAX`）：首请求 A 从发出到返回的总
  传输预算。它必须覆盖正常暂停与竞争请求（等挂起确认 + 落库核对 +
  竞争声明 + 放行后返回）：调小会让 A 在合法挂起期间被 curl 超时杀掉、
  提前破坏交错；调整 `WAIT_MAX`/`REQ_MAX` 时它自动放大，也可显式覆盖。

失败语义：任何一步失败——传输错误（curl 非零）、HTTP 错误（非 2xx）、
响应不完整（非完整 JSON）或等待超时——脚本都会删除标记与 `.held`、
终止并回收后台请求后以非零退出；只有三份完整有效的响应通过核验
（A=3、B=6、终态=6）才报 PASS，残缺响应即使含有 `declared_length`
片段也不予采信。

### 手工等价 HTTP 调用（想逐步执行时）

```bash
SHA3=$(printf abc  | sha256sum | cut -d' ' -f1)
SHA6=$(printf abcdef | sha256sum | cut -d' ' -f1)
B64=$(printf base | base64 | tr -d '\n')

curl -s -X POST http://localhost:8000/devices -H 'Content-Type: application/json' \
  -d "{\"device_id\":\"dev-race\",\"initial_image_base64\":\"$B64\"}" >/dev/null

# 1) 放置标记，发起声明 A（length=3）——它会提交后停在挂点上
: > /tmp/fw-pause/declare_update
curl -s -X PUT http://localhost:8000/devices/dev-race/update \
  -H 'Content-Type: application/json' \
  -d "{\"length\":3,\"sha256\":\"$SHA3\"}" > /tmp/respA.json &

# 2) 等 .held 出现：确认 A 已提交且停在返回前
#    （不要用数据库可见代替——A 可能在 commit 后、进入挂点前被调度出去）
while [ ! -e /tmp/fw-pause/declare_update.held ]; do sleep 0.05; done
curl -s http://localhost:8000/devices/dev-race | grep -o '"declared_length":[0-9]*'  # 附带核对：3

# 3) A 尚未返回时，完整执行声明 B（length=6）
curl -s -X PUT http://localhost:8000/devices/dev-race/update \
  -H 'Content-Type: application/json' \
  -d "{\"length\":6,\"sha256\":\"$SHA6\"}" | grep -o '"declared_length":[0-9]*'   # 6

# 4) 确认 A 仍未返回（jobs 里仍有它），再删除 .held 放行
jobs
rm -f /tmp/fw-pause/declare_update.held
wait
grep -o '"declared_length":[0-9]*' /tmp/respA.json    # 期望 3
```

## 回归测试

```bash
pip install -r requirements.txt httpx   # TestClient 依赖 httpx
python3 test_api.py
python3 test_race_repro.py              # race_repro.sh 传输异常回归（本地桩，无外部依赖）
```

除既有端到端场景外，包含两组与本问题相关的回归：

- **受控交错（test_api.py 第 12 节）**：通过挂点确定性制造“后写在前写返回前
  落库”的交错，分别核对两次 `PUT /update`（长度 3、6）、`POST /devices`
  （创建响应保持初始状态）、`POST /confirm`（确认响应不带入随后声明）的
  **响应快照**与**最终数据库状态**均等价于串行执行。三组回归都按“确认首请求
  已提交且停在返回前（挂点事件，而非数据库可见）→ 发竞争请求 → 竞争响应
  完成且首响应仍未返回 → 放行”的顺序推进，覆盖首请求 commit 后被调度出去
  的情况。
- **提交失败不报成功（test_api.py 第 13 节）**：注入连接代理让 `commit`
  抛错，断言接口返回 5xx、库状态保持操作前、服务仍可继续处理请求。
- **传输异常（test_race_repro.py）**：用本地可控传输桩（实现同一挂点协议）
  驱动真实的 `scripts/race_repro.sh` 进程，覆盖三条路径：
  - 竞争声明的响应永不结束：脚本必须在 `REQ_MAX` 内报错退出（不绕过
    `WAIT_MAX` 挂住），并清理标记与 `.held`、终止回收后台请求；
  - 首请求放行后只收到含 `declared_length=3` 的残缺响应、curl 返回非零：
    脚本必须非零退出，不得误报 PASS；
  - 成功路径：完整交错交互必须 PASS（A=3、B=6、终态=6）。
