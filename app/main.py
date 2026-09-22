"""租约协调服务：多工作进程间的独占任务协调与滑动窗口限流。

所有租约状态转换与限流判定均由 Redis Lua 脚本原子完成（见 app/lua.py），
过期判断基于 Redis 服务器时间（TIME 命令）。
"""
from __future__ import annotations

import math
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .config import Settings, get_settings
from .lua import (
    ACQUIRE_BATCH_LUA,
    ACQUIRE_LUA,
    LIMIT_CHECK_LUA,
    RELEASE_LUA,
    RENEW_LUA,
    VALIDATE_LUA,
)
from .schemas import (
    AcquireBatchRequest,
    AcquireBatchResponse,
    AcquiredLease,
    AcquireRequest,
    AcquireResponse,
    LeaseStateResponse,
    LimitCheckRequest,
    LimitCheckResponse,
    ReleaseRequest,
    ReleaseResponse,
    RenewRequest,
    RenewResponse,
    ValidateRequest,
    ValidateResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.redis = redis
    # 启动时注册 Lua 脚本（EVALSHA 执行，脚本不存在时客户端自动回退 EVAL）
    app.state.scripts = SimpleNamespace(
        acquire=redis.register_script(ACQUIRE_LUA),
        acquire_batch=redis.register_script(ACQUIRE_BATCH_LUA),
        renew=redis.register_script(RENEW_LUA),
        release=redis.register_script(RELEASE_LUA),
        validate=redis.register_script(VALIDATE_LUA),
        limit_check=redis.register_script(LIMIT_CHECK_LUA),
    )
    yield
    await redis.aclose()


app = FastAPI(
    title="Lease API",
    version="1.0.0",
    description="供多个工作进程协调独占任务的租约服务与滑动窗口限流（FastAPI + Redis Lua）",
    lifespan=lifespan,
)


@app.exception_handler(RedisError)
async def redis_error_handler(request: Request, exc: RedisError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "redis unavailable"},
    )


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def get_scripts(request: Request) -> SimpleNamespace:
    return request.app.state.scripts


RedisDep = Annotated[Redis, Depends(get_redis)]
ScriptsDep = Annotated[SimpleNamespace, Depends(get_scripts)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def lease_key(settings: Settings, task_key: str) -> str:
    return f"{settings.key_prefix}:lease:{task_key}"


def fencing_key(settings: Settings, task_key: str) -> str:
    return f"{settings.key_prefix}:fencing:{task_key}"


def limit_key(settings: Settings, client_key: str, window_ms: int) -> str:
    # window_ms 入键：同一 client_key 使用不同窗口长度时互不干扰、各自清理
    return f"{settings.key_prefix}:limit:{client_key}:{window_ms}"


def resolve_window(window_ms: int, settings: Settings) -> int:
    if not settings.limit_min_window_ms <= window_ms <= settings.limit_max_window_ms:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"window_ms must be within "
                f"[{settings.limit_min_window_ms}, {settings.limit_max_window_ms}]"
            ),
        )
    return window_ms


def resolve_ttl(ttl_ms: int | None, settings: Settings) -> int:
    ttl = ttl_ms if ttl_ms is not None else settings.default_ttl_ms
    if not settings.min_ttl_ms <= ttl <= settings.max_ttl_ms:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"ttl_ms must be within "
                f"[{settings.min_ttl_ms}, {settings.max_ttl_ms}]"
            ),
        )
    return ttl


@app.get("/healthz")
async def healthz(redis: RedisDep) -> dict:
    await redis.ping()
    return {"status": "ok"}


@app.get("/")
async def root() -> dict:
    return {"service": "lease-api", "docs": "/docs"}


@app.post("/v1/leases/acquire", response_model=AcquireResponse)
async def acquire(
    req: AcquireRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> AcquireResponse:
    """申请租约。仅当任务未被占用或旧租约已过期时成功。

    成功返回 200 及所有权令牌 + 防栅序号；被占用返回 409。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    token = uuid.uuid4().hex
    ok, fencing, expires_at = await scripts.acquire(
        keys=[lease_key(settings, req.task_key), fencing_key(settings, req.task_key)],
        args=[token, ttl],
    )
    if ok:
        return AcquireResponse(
            acquired=True,
            task_key=req.task_key,
            token=token,
            fencing_token=fencing,
            expires_at_ms=expires_at,
        )
    response.status_code = status.HTTP_409_CONFLICT
    return AcquireResponse(
        acquired=False,
        task_key=req.task_key,
        fencing_token=fencing,
        expires_at_ms=expires_at,
    )


@app.post(
    "/v1/leases/acquire-batch",
    response_model=AcquireBatchResponse,
)
async def acquire_batch(
    req: AcquireBatchRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> AcquireBatchResponse:
    """批量申请租约（2～32 个不重复任务键，统一 TTL），全成全败。

    由单个 Lua 脚本、单次服务器 TIME 原子完成：任一任务仍被有效租约占用时
    返回 409 及全部冲突键，不创建任何租约、不递增任何防栅计数器；
    全部可用（含旧租约已过期可接管）时，按输入顺序返回每个任务独立的
    token、递增的 fencing_token 与同一基准计算的 expires_at_ms。
    所得凭据可直接用于现有的续租 / 释放 / 校验接口。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    tokens = [uuid.uuid4().hex for _ in req.task_keys]
    lease_keys = [lease_key(settings, k) for k in req.task_keys]
    fencer_keys = [fencing_key(settings, k) for k in req.task_keys]

    result = await scripts.acquire_batch(
        keys=lease_keys + fencer_keys,
        args=[str(len(req.task_keys)), *tokens, str(ttl)],
    )

    if result[0]:
        leases = [
            AcquiredLease(
                task_key=req.task_keys[i],
                token=tokens[i],
                fencing_token=result[1 + 2 * i],
                expires_at_ms=result[2 + 2 * i],
            )
            for i in range(len(req.task_keys))
        ]
        return AcquireBatchResponse(acquired=True, leases=leases)

    conflicting = [req.task_keys[idx - 1] for idx in result[1:]]
    response.status_code = status.HTTP_409_CONFLICT
    return AcquireBatchResponse(
        acquired=False, conflicting_task_keys=conflicting
    )


@app.post("/v1/leases/renew", response_model=RenewResponse)
async def renew(
    req: RenewRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> RenewResponse:
    """续租。必须同时匹配令牌与防栅序号，且租约尚未过期。

    失败返回 409，reason 取值：not_found / conflict / expired。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    ok, reason, expires_at = await scripts.renew(
        keys=[lease_key(settings, req.task_key)],
        args=[req.token, str(req.fencing_token), ttl],
    )
    if ok:
        return RenewResponse(renewed=True, expires_at_ms=expires_at)
    response.status_code = status.HTTP_409_CONFLICT
    return RenewResponse(renewed=False, reason=reason)


@app.post("/v1/leases/release", response_model=ReleaseResponse)
async def release(
    req: ReleaseRequest, scripts: ScriptsDep, settings: SettingsDep
) -> ReleaseResponse:
    """释放租约。令牌与防栅序号同时匹配才删除，否则幂等忽略。

    旧持有者在超时后调用本接口不会影响新租约。
    """
    released = await scripts.release(
        keys=[lease_key(settings, req.task_key)],
        args=[req.token, str(req.fencing_token)],
    )
    return ReleaseResponse(released=bool(released))


@app.post("/v1/leases/validate", response_model=ValidateResponse)
async def validate(
    req: ValidateRequest, scripts: ScriptsDep, settings: SettingsDep
) -> ValidateResponse:
    """校验 (令牌, 防栅序号) 是否仍对应当前有效租约，供下游判断操作归属。"""
    valid, remaining_ms = await scripts.validate(
        keys=[lease_key(settings, req.task_key)],
        args=[req.token, str(req.fencing_token)],
    )
    return ValidateResponse(valid=bool(valid), remaining_ms=remaining_ms)


@app.post(
    "/v1/limits/check",
    response_model=LimitCheckResponse,
    responses={
        429: {
            "model": LimitCheckResponse,
            "description": "超出窗口限额，未扣减；Retry-After 头给出秒级等待建议",
        }
    },
)
async def check_limit(
    req: LimitCheckRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> LimitCheckResponse:
    """滑动窗口限流检查（按成本扣减）。

    以 Redis 服务器时间为基准，累计同一 client_key 当前窗口内已扣减的成本：
    未超限（含本次成本）时原子扣减并返回剩余配额与窗口重置时刻；
    已超限时不扣减，返回 **429** 及建议等待毫秒数（同时下发
    `Retry-After` 头，秒级向上取整）。判定与扣减在单个 Lua 脚本内
    原子完成，相同 client_key 的并发请求不会超额；窗口外记录由脚本清理，
    键本身带 PEXPIRE，过期数据自动回收。
    """
    window = resolve_window(req.window_ms, settings)
    nonce = uuid.uuid4().hex
    result = await scripts.limit_check(
        keys=[limit_key(settings, req.client_key, window)],
        args=[req.limit, window, req.cost, f"{req.cost}:{nonce}"],
    )
    allowed = bool(result[0])
    if allowed:
        return LimitCheckResponse(
            allowed=True,
            client_key=req.client_key,
            limit=req.limit,
            window_ms=window,
            remaining=result[1],
            reset_at_ms=result[2],
        )

    retry_after_ms = result[3]
    # Retry-After 仅支持秒级粒度，向上取整保证等待足够时间
    response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
    response.headers["Retry-After"] = str(max(1, math.ceil(retry_after_ms / 1000)))
    return LimitCheckResponse(
        allowed=False,
        client_key=req.client_key,
        limit=req.limit,
        window_ms=window,
        remaining=result[1],
        reset_at_ms=result[2],
        retry_after_ms=retry_after_ms,
    )


@app.get("/v1/leases/{task_key:path}", response_model=LeaseStateResponse)
async def inspect(
    task_key: str, redis: RedisDep, settings: SettingsDep
) -> LeaseStateResponse:
    """查看任务键当前租约状态（只读，不暴露所有权令牌）。"""
    data = await redis.hgetall(lease_key(settings, task_key))
    if not data:
        return LeaseStateResponse(held=False)
    sec, usec = await redis.time()
    now = int(sec) * 1000 + int(usec) // 1000
    expires_at = int(data["expires_at"])
    held = expires_at > now
    return LeaseStateResponse(
        held=held,
        fencing_token=int(data["fencing"]),
        expires_at_ms=expires_at,
        remaining_ms=max(0, expires_at - now),
    )
