#!/usr/bin/env python3
"""端到端冒烟测试（仅依赖 Python 标准库）。

先启动服务（docker compose up --build），然后执行：
    python3 scripts/smoke_test.py [base_url]

覆盖场景：获取 / 冲突 / 校验 / 续租 / 释放 / 防栅序号单调递增 /
旧持有者在超时后无法影响新租约，以及批量申请的全成全败、
冲突时不写不递增、过期接管与凭据兼容；滑动窗口限流的成本扣减 /
429 与 Retry-After / 拒绝不扣减 / 窗口滑动自动清理 / 参数校验 /
高并发下不超额。
"""
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
FAILED = []


def call(method, path, body=None, with_headers=False):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        method=method,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            payload = json.loads(resp.read())
            return (resp.status, payload, dict(resp.headers)) if with_headers else (
                resp.status,
                payload,
            )
    except urllib.error.HTTPError as e:
        payload = json.loads(e.read())
        return (e.code, payload, dict(e.headers)) if with_headers else (
            e.code,
            payload,
        )


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f"  -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def main():
    key = f"smoke:{int(time.time() * 1000)}"

    code, body = call("GET", "/healthz")
    check("healthz", code == 200 and body.get("status") == "ok", f"{code} {body}")

    # 1. 申请租约
    code, a = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 1500})
    check("acquire #1", code == 200 and a["acquired"] and a["token"], f"{code} {a}")
    token_a, fence_a = a["token"], a["fencing_token"]

    # 2. 占用期间再次申请 -> 409
    code, b = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 1500})
    check("acquire conflict -> 409", code == 409 and not b["acquired"], f"{code} {b}")

    # 3. 校验当前持有者
    code, v = call(
        "POST",
        "/v1/leases/validate",
        {"task_key": key, "token": token_a, "fencing_token": fence_a},
    )
    check("validate holder", code == 200 and v["valid"] and v["remaining_ms"] > 0,
          f"{code} {v}")

    # 4. 错误令牌续租 -> 409
    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": "wrong", "fencing_token": fence_a, "ttl_ms": 1500},
    )
    check("renew with wrong token -> 409", code == 409 and not r["renewed"],
          f"{code} {r}")

    # 5. 正确续租
    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": token_a, "fencing_token": fence_a, "ttl_ms": 1500},
    )
    check("renew ok", code == 200 and r["renewed"], f"{code} {r}")

    # 6. 错误防栅序号释放 -> 幂等忽略
    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_a, "fencing_token": fence_a + 99},
    )
    check("release with wrong fencing ignored", code == 200 and not rel["released"],
          f"{code} {rel}")

    # 7. 正确释放
    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_a, "fencing_token": fence_a},
    )
    check("release ok", code == 200 and rel["released"], f"{code} {rel}")

    # 8. 重新申请，防栅序号必须单调递增
    code, b = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 800})
    check("re-acquire after release", code == 200 and b["acquired"], f"{code} {b}")
    check("fencing monotonic", b["fencing_token"] > fence_a,
          f"{b['fencing_token']} <= {fence_a}")
    token_b, fence_b = b["token"], b["fencing_token"]

    # 9. 等租约过期，新持有者可接管；旧持有者此后无法干扰
    time.sleep(1.0)
    code, c = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 5000})
    check("acquire after expiry", code == 200 and c["acquired"], f"{code} {c}")
    token_c, fence_c = c["token"], c["fencing_token"]

    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": token_b, "fencing_token": fence_b, "ttl_ms": 5000},
    )
    check("stale holder renew rejected", code == 409 and not r["renewed"],
          f"{code} {r}")

    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_b, "fencing_token": fence_b},
    )
    check("stale holder release ignored", code == 200 and not rel["released"],
          f"{code} {rel}")

    code, v = call(
        "POST",
        "/v1/leases/validate",
        {"task_key": key, "token": token_c, "fencing_token": fence_c},
    )
    check("new lease unaffected", code == 200 and v["valid"], f"{code} {v}")

    # 10. 状态查询
    code, st = call("GET", f"/v1/leases/{key}")
    check("inspect", code == 200 and st["held"] and st["fencing_token"] == fence_c,
          f"{code} {st}")

    batch_checks()
    limit_checks()

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All checks passed.")


def batch_checks():
    """acquire-batch：参数校验 / 全成 / 全败不写不递增 / 过期接管 / 凭据兼容。"""
    base = f"smoke-batch:{int(time.time() * 1000)}"
    k1, k2, k3, k4, k5 = [f"{base}:{i}" for i in range(5)]

    # 参数校验：少于 2 个 / 多于 32 个 / 有重复 -> 422
    code, _ = call("POST", "/v1/leases/acquire-batch", {"task_keys": [k1]})
    check("batch size < 2 -> 422", code == 422, f"code={code}")
    code, _ = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [f"{base}:x{i}" for i in range(33)]})
    check("batch size > 32 -> 422", code == 422, f"code={code}")
    code, _ = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k1, k1], "ttl_ms": 5000})
    check("batch duplicate keys -> 422", code == 422, f"code={code}")

    # 全成：顺序一致、token 独立、fencing 独立递增、expires_at 同一基准
    code, b = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k1, k2, k3], "ttl_ms": 5000})
    check("batch acquire ok", code == 200 and b["acquired"], f"{code} {b}")
    leases = b.get("leases", [])
    check("batch leases order/count",
          [l["task_key"] for l in leases] == [k1, k2, k3], f"{leases}")
    check("batch distinct tokens",
          len({l["token"] for l in leases}) == 3, f"{leases}")
    check("batch same expires baseline",
          len({l["expires_at_ms"] for l in leases}) == 1, f"{leases}")

    # 凭据兼容现有 validate 接口
    for l in leases:
        code, v = call("POST", "/v1/leases/validate",
                       {"task_key": l["task_key"], "token": l["token"],
                        "fencing_token": l["fencing_token"]})
        check(f"batch credential validate {l['task_key']}",
              code == 200 and v["valid"], f"{code} {v}")

    # 冲突：k3 仍被占用，k5 从未申请；应 409、仅报 k3，且 k5 不得被创建、计数器不得递增
    code, c = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k3, k5], "ttl_ms": 5000})
    check("batch conflict -> 409", code == 409 and not c.get("acquired"),
          f"{code} {c}")
    check("batch conflict keys",
          c.get("conflicting_task_keys") == [k3], f"{c}")
    code, st5 = call("GET", f"/v1/leases/{k5}")
    check("batch conflict created nothing",
          code == 200 and not st5["held"], f"{code} {st5}")
    # k5 从未申请成功；其第一次成功申请的 fencing 必须为 1，证明批量失败未 INCR
    code, a5 = call("POST", "/v1/leases/acquire", {"task_key": k5, "ttl_ms": 5000})
    check("batch conflict no fencing increment",
          code == 200 and a5["fencing_token"] == 1, f"{code} {a5}")

    # 全败时冲突方自己也不受影响：k3 的旧凭据仍可续租
    l3 = next(l for l in leases if l["task_key"] == k3)
    code, r = call("POST", "/v1/leases/renew",
                   {"task_key": k3, "token": l3["token"],
                    "fencing_token": l3["fencing_token"], "ttl_ms": 5000})
    check("batch conflict holder untouched", code == 200 and r["renewed"],
          f"{code} {r}")

    # 释放整批后，另起一批；过期租约可被接管，全新键与过期键混在一起
    for l in leases:
        code, rel = call("POST", "/v1/leases/release",
                         {"task_key": l["task_key"], "token": l["token"],
                          "fencing_token": l["fencing_token"]})
        check(f"batch release {l['task_key']}",
              code == 200 and rel["released"], f"{code} {rel}")
    code, rel = call("POST", "/v1/leases/release",
                     {"task_key": k5, "token": a5["token"],
                      "fencing_token": a5["fencing_token"]})
    check("batch setup release k5", code == 200 and rel["released"], f"{code} {rel}")

    # k1 用短 TTL 单独占用，等待过期后与全新的 k4 一起批量接管
    code, old = call("POST", "/v1/leases/acquire", {"task_key": k1, "ttl_ms": 300})
    check("batch takeover setup", code == 200 and old["acquired"], f"{code} {old}")
    time.sleep(0.7)
    code, t = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k1, k4], "ttl_ms": 5000})
    check("batch acquire after expiry", code == 200 and t["acquired"], f"{code} {t}")
    taken = {l["task_key"]: l for l in t.get("leases", [])}
    check("batch takeover fencing advanced",
          k1 in taken and taken[k1]["fencing_token"] > old["fencing_token"],
          f"{t.get('leases')}")
    check("batch free key fencing starts 1",
          taken.get(k4, {}).get("fencing_token") == 1, f"{t.get('leases')}")

    # 旧持有者释放不得影响批量接管后的新租约
    code, rel = call("POST", "/v1/leases/release",
                     {"task_key": k1, "token": old["token"],
                      "fencing_token": old["fencing_token"]})
    check("batch stale release ignored", code == 200 and not rel["released"],
          f"{code} {rel}")
    code, v = call("POST", "/v1/leases/validate",
                   {"task_key": k1, "token": taken[k1]["token"],
                    "fencing_token": taken[k1]["fencing_token"]})
    check("batch new lease valid", code == 200 and v["valid"], f"{code} {v}")


def limit_checks():
    """/v1/limits/check：成本扣减 / 429 / 并发不超额 / 窗口滑动清理 / 参数校验。"""
    base = f"smoke-limit:{int(time.time() * 1000)}"

    def check_req(key, limit, window, cost, ok_status=200):
        return call(
            "POST",
            "/v1/limits/check",
            {"client_key": key, "limit": limit, "window_ms": window, "cost": cost},
            with_headers=True,
        )

    # 1. 参数校验：limit<1 / window<=0 / cost<1 / cost>limit / 窗口越界 -> 4xx
    code, _, _ = check_req(f"{base}:bad", 0, 1000, 1)
    check("limit < 1 -> 422", code == 422, f"code={code}")
    code, _, _ = check_req(f"{base}:bad", 10, 0, 1)
    check("window_ms <= 0 -> 422", code == 422, f"code={code}")
    code, _, _ = check_req(f"{base}:bad", 10, 1000, 0)
    check("cost < 1 -> 422", code == 422, f"code={code}")
    code, _, _ = check_req(f"{base}:bad", 5, 1000, 6)
    check("cost > limit -> 422", code == 422, f"code={code}")
    code, _, _ = check_req(f"{base}:bad", 10, 86_400_001, 1)
    check("window_ms above max -> 400", code == 400, f"code={code}")

    # 2. 成本累计扣减：limit=10，3+3+3 成功（remaining 7/4/1），第 4 次 cost=3 超限
    key = f"{base}:cost"
    window = 60_000
    t0 = time.time()
    expected_remaining = 10
    for i in range(3):
        code, body, headers = check_req(key, 10, window, 3)
        expected_remaining -= 3
        check(f"cost allow #{i + 1}",
              code == 200 and body["allowed"]
              and body["remaining"] == expected_remaining,
              f"{code} {body}")
        check(f"allow echo fields #{i + 1}",
              body["client_key"] == key and body["limit"] == 10
              and body["window_ms"] == window and body["retry_after_ms"] is None,
              f"{body}")
        retry_header = headers.get("Retry-After") or headers.get("retry-after")
        check(f"allow no retry-after header #{i + 1}",
              retry_header is None, f"{headers}")
    first_reset = body["reset_at_ms"]
    check("reset_at within window",
          t0 * 1000 <= first_reset <= (t0 + 1) * 1000 + window,
          f"{first_reset}")

    code, body, headers = check_req(key, 10, window, 3)
    check("over limit -> 429", code == 429 and not body["allowed"], f"{code} {body}")
    check("429 remaining untouched", body["remaining"] == 1, f"{body}")
    check("429 retry_after_ms present",
          isinstance(body["retry_after_ms"], int) and body["retry_after_ms"] >= 1,
          f"{body}")
    retry_header = headers.get("Retry-After") or headers.get("retry-after")
    check("429 Retry-After header",
          bool(retry_header) and retry_header.isdigit()
          and int(retry_header) >= 1,
          f"{headers}")

    # 3. 拒绝不扣减：随后 cost=1 仍应成功（仍是最后一个配额）
    code, body, _ = check_req(key, 10, window, 1)
    check("denied request did not deduct",
          code == 200 and body["allowed"] and body["remaining"] == 0,
          f"{code} {body}")

    # 4. client_key 相互隔离：另一键额度独立
    other = f"{base}:other"
    code, body, _ = check_req(other, 10, window, 10)
    check("client keys isolated",
          code == 200 and body["allowed"] and body["remaining"] == 0,
          f"{code} {body}")

    # 5. 窗口滑动后自动恢复：短窗口内超额，等待滑出后最早记录释放配额
    slide = f"{base}:slide"
    code, body, _ = check_req(slide, 5, 800, 5)
    check("slide window exhausted",
          code == 200 and body["allowed"] and body["remaining"] == 0,
          f"{code} {body}")
    code, body, _ = check_req(slide, 5, 800, 1)
    check("slide window denied", code == 429 and not body["allowed"], f"{code} {body}")
    retry_after = body["retry_after_ms"]
    time.sleep(retry_after / 1000 + 0.3)
    code, body, _ = check_req(slide, 5, 800, 1)
    check("window slides and quota returns",
          code == 200 and body["allowed"] and body["remaining"] == 4,
          f"{code} {body}")

    # 6. 高并发不超额：40 个并发请求、每个 cost=1、limit=10，恰好 10 个成功
    conc_key = f"{base}:concurrent"
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: check_req(conc_key, 10, 60_000, 1),
                                range(40)))
    allowed = [r for r in results if r[0] == 200 and r[1]["allowed"]]
    denied = [r for r in results if r[0] == 429 and not r[1]["allowed"]]
    check("concurrent exact allowed count", len(allowed) == 10,
          f"allowed={len(allowed)}, denied={len(denied)}")
    check("concurrent exact denied count", len(denied) == 30,
          f"allowed={len(allowed)}, denied={len(denied)}")
    check("concurrent no other status",
          len(allowed) + len(denied) == 40,
          f"statuses={sorted({r[0] for r in results})}")
    check("concurrent remaining sum",
          sum(r[1]["remaining"] for r in allowed) == sum(range(10)),
          f"{[r[1]['remaining'] for r in allowed]}")

    # 7. 高并发按成本不超额：20 个并发请求、每个 cost=3、limit=10，最多 3 个成功
    cost_key = f"{base}:concurrent-cost"
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: check_req(cost_key, 10, 60_000, 3),
                                range(20)))
    allowed = [r for r in results if r[0] == 200 and r[1]["allowed"]]
    denied = [r for r in results if r[0] == 429 and not r[1]["allowed"]]
    check("concurrent cost never exceeds limit",
          len(allowed) == 3 and len(denied) == 17,
          f"allowed={len(allowed)}, denied={len(denied)}")

    # 8. 不同窗口长度互不干扰：同一 client_key 的长/短窗口独立计数
    wk = f"{base}:windows"
    code, b1, _ = check_req(wk, 5, 60_000, 5)
    code2, b2, _ = check_req(wk, 5, 1000, 5)
    check("distinct windows independent",
          b1["allowed"] and b2["allowed"]
          and b1["remaining"] == 0 and b2["remaining"] == 0,
          f"{b1} {b2}")


if __name__ == "__main__":
    main()
