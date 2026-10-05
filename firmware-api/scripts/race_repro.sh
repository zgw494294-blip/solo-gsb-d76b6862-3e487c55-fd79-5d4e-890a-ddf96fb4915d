#!/usr/bin/env bash
# 受控交错回归（黑盒 HTTP）：
# 同一设备两次合法声明，长度分别为 3、6；让第二次在第一次“已提交且停在
# 返回之前”的窗口内完整落库。修复前两份响应都显示 declared_length=6，
# 修复后必须分别为 3、6，且最终数据库状态为 6（等价某种串行执行）。
#
# 依赖服务以 FIRMWARE_TEST_PAUSE_DIR=<目录> 启动（见 README“并发交错复现”）。
# 纯 curl，无第三方工具（jq 仅用于美化，缺失也能跑）。
#
# 确定性顺序（与 test_api.py 第 12 节的线程回归一致）：
#   1. 放置挂点标记后发起声明 A；
#   2. 等到 $PAUSE_DIR/declare_update.held 出现 —— 这表示 A 已提交且确实
#      停在挂点上。不能用“数据库里查得到”代替这一确认：A 可能在 commit
#      之后、进入挂点之前被调度出去，此时标记尚未被接管，若就此发出 B，
#      B 会抢占标记并挂起，而脚本又在等 B 返回，形成等待环（脚本挂死）；
#   3. 再发声明 B 并等它完整返回（它见不到标记，直接放行）；
#   4. 确认 A 的响应仍未返回，才删除 .held 放行。
#
# 有限等待与失败语义（全部为可配置的有限等待，脚本不会无限挂起）：
#   - 每个 HTTP 调用都有 curl --max-time 上限：普通调用用 REQ_MAX，
#     首请求 A 用 A_MAX（预算覆盖正常暂停与竞争请求，不会提前破坏交错）；
#   - 传输失败（curl 非零）、HTTP 错误（非 2xx）、响应不完整（非完整
#     JSON）一律非零退出；退出前删除标记与 .held，并终止、回收后台请求；
#   - 只有三份完整有效的响应通过核验（A=3、B=6、终态=6）才报 PASS，
#     残缺响应即使含有 declared_length 片段也不予采信。
set -u

BASE=${BASE_URL:-http://localhost:8000}
PAUSE_DIR=${FIRMWARE_TEST_PAUSE_DIR:-/tmp/fw-pause}
# 每次运行使用唯一设备，避免与历史数据相互干扰（API 不提供删除）
DEV=${DEV:-race-http-$(date +%s)-$$}
OUT=${OUT_DIR:-/tmp/fw-race-out}
mkdir -p "$OUT"

MARKER="$PAUSE_DIR/declare_update"
HELD="$PAUSE_DIR/declare_update.held"
# 各等待环节的超时（秒）：覆盖慢启动/调度延迟，但绝不无限等
WAIT_MAX=${WAIT_MAX:-30}   # 等 .held 出现 / 放行后等 A 返回
REQ_MAX=${REQ_MAX:-10}     # 单次普通 HTTP 调用（健康检查/创建/查询/声明 B）的 curl 上限
# 首请求 A 的传输预算：合法路径上 A 从发出到返回要经过
# “等 .held（≤WAIT_MAX）+ 落库核对 GET（≤REQ_MAX）+ 竞争声明 B（≤REQ_MAX）
#  + 放行后返回（≤WAIT_MAX）”。预算必须覆盖正常暂停与竞争请求：
# 调小会让 A 在合法挂起期间被 curl 超时杀掉，提前破坏交错。
A_MAX=${A_MAX:-$(( WAIT_MAX * 2 + REQ_MAX * 2 ))}

PID_A=""

release_pause() { # 释放挂点：无论脚本以何种方式退出都让服务侧放行
  rm -f "$MARKER" "$HELD" 2>/dev/null || true
}

stop_bg() { # 终止并回收后台请求 A（若仍在传输），不留挂起的传输与僵尸进程
  if [ -n "$PID_A" ]; then
    kill "$PID_A" 2>/dev/null || true
    wait "$PID_A" 2>/dev/null || true
    PID_A=""
  fi
}

cleanup() { # 退出兜底：释放挂点 + 终止并回收后台请求
  release_pause
  stop_bg
}

fail() {
  echo "FAIL: $*" >&2
  exit 1   # EXIT trap 统一执行清理
}

# 兜底：异常退出（含 Ctrl-C）也释放挂点、回收后台请求，不在服务侧留下挂起的请求
trap cleanup EXIT

jqf() { if command -v jq >/dev/null 2>&1; then jq "$@"; else cat; fi; }
sha256_hex() { printf '%s' "$1" | sha256sum | cut -d' ' -f1; }
# 从整设备视图取“第一个非空 declared_length”（本例候选为 slot1；slot0 为
# 已确认槽，declared_length 为 null，不会匹配至少一位数字的模式）。
first_declared() { grep -o '"declared_length":[0-9][0-9]*' | head -1 | cut -d: -f2; }

# 完整 JSON 对象校验：响应必须是完整对象（最后一个非空白字符为 }）。
# 传输中断留下的残缺前缀即使包含 declared_length 片段也不算有效响应。
json_complete() { # path
  [ -s "$1" ] || return 1
  [ "$(tr -d '[:space:]' < "$1" | tail -c 1)" = "}" ]
}

req() { # method path [json-body]：有限等待（REQ_MAX），HTTP 错误（非 2xx）即非零退出
  if [ -n "${3:-}" ]; then
    curl -sS -f --max-time "$REQ_MAX" -X "$1" "$BASE$2" \
      -H 'Content-Type: application/json' -d "$3"
  else
    curl -sS -f --max-time "$REQ_MAX" -X "$1" "$BASE$2"
  fi
}

# 等待文件出现，带超时（0.05s 粒度，总时长 $2 秒）
wait_file() { # path timeout_sec
  local tries=$(( $2 * 20 ))
  while [ "$tries" -gt 0 ]; do
    [ -e "$1" ] && return 0
    sleep 0.05
    tries=$(( tries - 1 ))
  done
  return 1
}

echo ">> 健康检查"
curl -fsS --max-time "$REQ_MAX" "$BASE/health" >/dev/null \
  || { echo "服务未就绪：$BASE/health"; exit 1; }

echo ">> 设备：$DEV"
echo ">> 创建设备（初始镜像 base）"
INIT_B64=$(printf 'base' | base64 | tr -d '\n')
req POST /devices "$(printf '{"device_id":"%s","initial_image_base64":"%s"}' "$DEV" "$INIT_B64")" \
  > "$OUT/create.json" || fail "创建设备失败（传输错误/超时/HTTP 错误）"
jqf '.slots' < "$OUT/create.json" >/dev/null

SHA3=$(sha256_hex abc)
SHA6=$(sha256_hex abcdef)

echo ">> 放置挂点标记：$MARKER"
mkdir -p "$PAUSE_DIR"
release_pause          # 清掉可能残留的旧标记，保证本次协议从干净状态开始
: > "$MARKER"

echo ">> 发起声明 A（length=3），它会在提交后停在挂点上（传输预算 ${A_MAX}s）"
# 后台直接运行 curl（不经函数包装），PID_A 即 curl 进程本身：
# 失败清理能直接终止传输并 wait 回收
curl -sS -f --max-time "$A_MAX" -X PUT "$BASE/devices/$DEV/update" \
  -H 'Content-Type: application/json' \
  -d "$(printf '{"length":3,"sha256":"%s"}' "$SHA3")" \
  > "$OUT/respA.json" 2> "$OUT/curlA.log" &
PID_A=$!

echo ">> 等待 A 确认停在挂点（$HELD 出现；不能用数据库可见代替暂停确认）"
wait_file "$HELD" "$WAIT_MAX" \
  || fail "未等到 A 进入挂点（.held 未出现），已释放挂点"
# 附带核对：A 的写入已随 commit 落库（挂点在 commit 之后触发，必然可见）
RESP=$(req GET "/devices/$DEV") || fail "查询设备状态失败（传输错误/超时/HTTP 错误）"
L=$(printf '%s' "$RESP" | first_declared)
[ "$L" = "3" ] || fail "A 已挂起但库中 declared_length=$L（期望 3）"
echo "   A 已提交且停在返回前（库中=3）"

echo ">> 在 A 返回前，完整执行声明 B（length=6）"
req PUT "/devices/$DEV/update" "$(printf '{"length":6,"sha256":"%s"}' "$SHA6")" \
  > "$OUT/respB.json" || fail "声明 B 请求失败（传输错误/超时/HTTP 错误）"
json_complete "$OUT/respB.json" || fail "B 的响应不是完整 JSON（传输中断），不予采信"
LB=$(first_declared < "$OUT/respB.json")
[ "$LB" = "6" ] || fail "B 自身响应应为 6，实际：$LB"

echo ">> 确认 A 的响应仍未返回，然后删除 .held 放行"
kill -0 "$PID_A" 2>/dev/null || fail "A 在放行前已返回，交错不成立"
[ -s "$OUT/respA.json" ] && fail "A 的响应文件在放行前已有内容，交错不成立"
release_pause

echo ">> 等待 A 返回（带超时）"
tries=$(( WAIT_MAX * 20 ))
while [ "$tries" -gt 0 ]; do
  kill -0 "$PID_A" 2>/dev/null || break
  sleep 0.05
  tries=$(( tries - 1 ))
done
kill -0 "$PID_A" 2>/dev/null && fail "放行后 A 仍未返回"
RC_A=0
wait "$PID_A" || RC_A=$?
PID_A=""
# curl 非零（如 18 传输残缺、28 超时）：响应文件里的内容一律不予采信
[ "$RC_A" -eq 0 ] || fail "A 的传输失败（curl 退出码 $RC_A）：响应可能残缺，不予采信"
json_complete "$OUT/respA.json" || fail "A 的响应不是完整 JSON（传输中断），不予采信"

LA=$(first_declared < "$OUT/respA.json")
RESP=$(req GET "/devices/$DEV") || fail "查询最终状态失败（传输错误/超时/HTTP 错误）"
FINAL=$(printf '%s' "$RESP" | first_declared)

echo
echo "A 响应 declared_length = $LA   （期望 3）"
echo "B 响应 declared_length = $LB   （期望 6）"
echo "最终数据库 declared_length = $FINAL （期望 6）"
echo
if [ "$LA" = 3 ] && [ "$LB" = 6 ] && [ "$FINAL" = 6 ]; then
  echo "PASS：响应快照彼此独立，终态等价串行执行（A -> B）。"
  exit 0
else
  echo "FAIL：并发响应混入了后续写入。"
  echo "-- A 响应 --"; cat "$OUT/respA.json"
  exit 1
fi
