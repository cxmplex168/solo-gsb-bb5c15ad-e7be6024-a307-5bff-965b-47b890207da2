# Lease API — 分布式独占任务租约与滑动窗口限流服务

供多个工作进程协调独占任务的租约 API。客户端按任务键（`task_key`）申请带 TTL 的
租约；仅当任务未被占用或旧租约已过期时申请成功，并返回**所有权令牌**与**单调递增
的防栅序号（fencing token）**。续租与释放必须同时校验令牌与防栅序号，旧持有者在
超时后无法影响新租约；另提供校验接口，供下游判断一次操作是否仍属于当前租约。

此外提供基于 Redis Lua 的**滑动窗口限流**接口 `POST /v1/limits/check`：按
`client_key` 以 Redis 服务器时间累计窗口内成本，原子判定并扣减，超限返回
`429` 与 `retry_after_ms`。

技术栈：Python 3.12 · FastAPI · Redis 7（Lua 脚本保证原子性）

## 快速开始（一条命令）

```bash
docker compose up --build
```

启动后：

| 组件 | 地址 |
| --- | --- |
| API | http://localhost:8000 |
| 交互式文档 (Swagger UI) | http://localhost:8000/docs |
| 健康检查 | http://localhost:8000/healthz |
| Redis | localhost:6379 |

Compose 会同时拉起 Redis 与 API 两个容器，API 等待 Redis 健康检查通过后启动。

## 配置

全部通过环境变量配置（见 `docker-compose.yml` 中的示例）：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis 连接串 |
| `LEASE_KEY_PREFIX` | `lease` | Redis 键前缀，便于多环境共用实例 |
| `LEASE_MIN_TTL_MS` | `100` | 允许的最小 TTL（毫秒） |
| `LEASE_MAX_TTL_MS` | `300000` | 允许的最大 TTL（毫秒） |
| `LEASE_DEFAULT_TTL_MS` | `10000` | 请求未指定 `ttl_ms` 时的默认值 |
| `LIMIT_MIN_WINDOW_MS` | `1` | 限流允许的最小窗口长度（毫秒） |
| `LIMIT_MAX_WINDOW_MS` | `86400000` | 限流允许的最大窗口长度（毫秒，默认 1 天） |

服务端口固定为 **8000**（容器内），可在 `docker-compose.yml` 的 `ports` 中调整宿主机映射。

## API

所有端点位于 `/v1` 前缀下，请求与响应均为 JSON。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/leases/acquire` | 申请租约，成功 200 / 被占用 409 |
| POST | `/v1/leases/acquire-batch` | 批量申请 2～32 个不重复任务键（统一 TTL），全成全败 |
| POST | `/v1/leases/renew` | 续租，成功 200 / 失败 409（reason: `not_found` / `conflict` / `expired`） |
| POST | `/v1/leases/release` | 释放租约（幂等，始终 200） |
| POST | `/v1/leases/validate` | 校验 (令牌, 防栅序号) 是否仍属当前租约 |
| GET | `/v1/leases/{task_key}` | 查看任务键租约状态（不暴露令牌） |
| POST | `/v1/limits/check` | 滑动窗口限流检查（按成本扣减），放行 200 / 超限 429 |
| GET | `/healthz` | 健康检查 |

### 示例

```bash
# 1. 申请租约（TTL 5 秒）
curl -s -X POST localhost:8000/v1/leases/acquire \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "ttl_ms": 5000}'
# => {"acquired":true,"task_key":"report:daily","token":"9f3c...","fencing_token":1,"expires_at_ms":1758...}

# 占用期间他人申请 => 409 {"acquired":false,...}

# 2. 批量申请（2～32 个不重复 task_key，统一 TTL，全成全败）
curl -s -X POST localhost:8000/v1/leases/acquire-batch \
  -H 'Content-Type: application/json' \
  -d '{"task_keys": ["report:daily", "report:weekly", "report:monthly"], "ttl_ms": 5000}'
# 成功 200（leases 顺序与请求一致；expires_at_ms 为同一次服务器 TIME 的基准）：
# => {"acquired":true,"leases":[
#   {"task_key":"report:daily","token":"a1b2...","fencing_token":1,"expires_at_ms":1758...},
#   {"task_key":"report:weekly","token":"c3d4...","fencing_token":1,"expires_at_ms":1758...},
#   {"task_key":"report:monthly","token":"e5f6...","fencing_token":1,"expires_at_ms":1758...}
# ],"conflicting_task_keys":[]}
#
# 任一任务仍被占用 => 409，返回全部冲突键，不创建任何租约、不递增任何防栅计数器：
# => {"acquired":false,"leases":[],"conflicting_task_keys":["report:weekly"]}
#
# 过期租约与不存在的键同样视为可用，可被批量接管。
# 批量所得 (token, fencing_token) 与单个申请完全等价，
# 可直接用于下面的续租 / 校验 / 释放接口（每个任务键独立调用）。

# 3. 续租（必须同时带上令牌与防栅序号）
curl -s -X POST localhost:8000/v1/leases/renew \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "9f3c...", "fencing_token": 1, "ttl_ms": 5000}'

# 4. 下游校验某次操作是否仍属当前租约
curl -s -X POST localhost:8000/v1/leases/validate \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "9f3c...", "fencing_token": 1}'
# => {"valid":true,"remaining_ms":4210}

# 5. 释放租约
curl -s -X POST localhost:8000/v1/leases/release \
  -H 'Content-Type: application/json' \
  -d '{"task_key": "report:daily", "token": "9f3c...", "fencing_token": 1}'
```

### 滑动窗口限流

`POST /v1/limits/check` 请求字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `client_key` | string (1～512) | 限流对象标识，相同键共享同一窗口配额 |
| `limit` | int ≥ 1 | 单个窗口内允许累计的最大成本 |
| `window_ms` | int（服务端限制 1～86400000） | 滑动窗口长度（毫秒） |
| `cost` | int ≥ 1，缺省 1 | 本次请求消耗的成本，不得大于 `limit` |

放行返回 **200**：`remaining` 为扣减后剩余配额，`reset_at_ms` 为最早一条记录
滑出窗口的时刻（Redis 服务器时钟 Unix 毫秒）；超限返回 **429**，不扣减任何成本，
`retry_after_ms` 给出建议等待毫秒数，并同时下发秒级向上取整的 `Retry-After` 响应头。

```bash
# 每分钟最多 100 次，每次成本 1
curl -s -X POST localhost:8000/v1/limits/check \
  -H 'Content-Type: application/json' \
  -d '{"client_key": "tenant-42:send-sms", "limit": 100, "window_ms": 60000, "cost": 1}'
# => 200
# {"allowed":true,"client_key":"tenant-42:send-sms","limit":100,"window_ms":60000,
#  "remaining":99,"reset_at_ms":1758...,"retry_after_ms":null}

# 超限时：
# => 429（响应头 Retry-After: 42）
# {"allowed":false,"client_key":"tenant-42:send-sms","limit":100,"window_ms":60000,
#  "remaining":0,"reset_at_ms":1758...,"retry_after_ms":41237}
```

## 设计要点

- **原子性**：申请 / 批量申请 / 续租 / 释放 / 校验全部状态转换均由 Redis Lua 脚本
  （`app/lua.py`）在服务端原子执行，不存在"检查后修改"的竞态窗口。批量申请在同一
  脚本内先检查全部任务、再统一写入，任一任务被占用则整体失败——不创建任何租约，
  也不递增任何防栅计数器（全成全败）；过期租约可被接管。
- **过期判断使用 Redis 服务器时间**：Lua 内通过 `TIME` 命令取 Redis 服务器时钟，
  不依赖各客户端本地时间，避免时钟漂移导致的误判。批量脚本只调用一次 `TIME`，
  整批租约的 `expires_at_ms` 基于同一时刻计算。
- **防栅序号单调递增**：每个任务键对应一个独立的 `INCR` 计数器键且**永不过期**，
  即使租约键被 TTL 清除，序号也不会回退或复用。下游系统可用它拒绝迟到的旧操作
  （fencing token 经典用法）。
- **旧持有者隔离**：续租与释放要求令牌和防栅序号**同时**匹配；租约过期后不允许
  续租（必须重新申请）。因此旧持有者在超时后的任何写操作都不会影响新租约。
- **双重过期保障**：租约 Hash 内记录 `expires_at`（判断依据），同时设置
  `PEXPIRE`（自动清理残留键）；判断以 `expires_at` 与服务器时间的比较为准。
- **滑动窗口限流原子化**：`/v1/limits/check` 的"清理窗口外记录 → 累计成本 →
  判定 → 扣减"在单个 Lua 脚本（`app/lua.py` 的 `LIMIT_CHECK_LUA`）内一次完成，
  相同 `client_key` 的并发请求串行执行于同一脚本，**不会超额**；被拒绝的请求
  不写入任何记录（不扣减）。每次扣减是 ZSet 中的一条独立记录（score 为 Redis
  服务器毫秒时间，member 携带成本与服务端生成的 nonce），窗口外记录按 score
  删除，键本身设置 `PEXPIRE`，过期数据自动回收。超限时从最早记录起累加，计算
  需要滑出多少成本才放得下本次请求，得到精确的 `retry_after_ms`。
- **窗口长度参与限流键**：限流键形如 `{prefix}:limit:{client_key}:{window_ms}`，
  同一 `client_key` 以不同 `window_ms` 调用时各自独立计数、独立过期清理。

### Redis 数据模型

| 键 | 类型 | 说明 |
| --- | --- | --- |
| `{prefix}:lease:{task_key}` | Hash（`token` / `fencing` / `expires_at`），带 PEXPIRE | 当前租约 |
| `{prefix}:fencing:{task_key}` | String 计数器，无 TTL | 防栅序号，只增不减 |
| `{prefix}:limit:{client_key}:{window_ms}` | ZSet（score=扣减毫秒时间，member=`cost:nonce`），带 PEXPIRE | 滑动窗口扣减记录 |

## 冒烟测试

服务启动后执行（仅依赖 Python 标准库）：

```bash
python3 scripts/smoke_test.py            # 默认打 http://localhost:8000
```

覆盖：获取冲突、错误令牌/序号被拒绝、防栅序号单调递增、旧持有者超时后无法
续租/释放/干扰新租约等场景；以及滑动窗口限流的成本扣减、429 与
`Retry-After`、拒绝不扣减、窗口滑动后配额恢复、参数校验，和 40 路并发请求
下恰好放行限额次数、绝不超额。

## 本地开发（不用 Docker）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# 需要本地或远程有可用的 Redis，并通过 REDIS_URL 指向它
uvicorn app.main:app --reload --port 8000
```

## 项目结构

```
app/
  config.py    # 环境变量配置（pydantic-settings）
  lua.py       # 申请/批量申请/续租/释放/校验/限流六个原子 Lua 脚本
  schemas.py   # 请求 / 响应模型
  main.py      # FastAPI 应用、路由、Redis 生命周期
scripts/
  smoke_test.py
Dockerfile
docker-compose.yml
requirements.txt
```
