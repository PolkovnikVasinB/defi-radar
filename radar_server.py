#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 DeFi Radar — агрегатор рыночных данных DeFi для ИИ-агентов (x402 v2, USDC)
================================================================================
Второй сервис экосистемы (рядом со SCV — Smart Contract Verification).

Что продаём: не «данные» (они публичны), а ГОТОВЫЙ ОТВЕТ одним запросом —
как Aviasales для билетов. Источники бесплатные и без ключей (DefiLlama,
DexScreener, GeckoTerminal), мы их нормализуем, кэшируем и отдаём агенту
в аккуратном JSON.

Схема работы (та же, что у SCV — клиентский код агента не меняется):
  1. GET /yields?chain=base&min_apy=20  ->  402 + PAYMENT-REQUIRED (base64)
  2. Агент подписывает EIP-3009 платёж (USDC, Base) локально, без газа
  3. Повтор ТОГО ЖЕ запроса + заголовок PAYMENT-SIGNATURE (base64-JSON)
  4. Сервер: фасилитатор /verify -> сборка ответа -> /settle -> 200 + отчёт
     (если работа не удалась — оплата НЕ списывается)

Ручки (все платные):
  /yields        топ пулов доходности: chain/min_apy/min_tvl/stable/limit   $0.003
  /pools         пулы DEX: trending/boosted/search по токену               $0.003
  /price         агрегированная цена токена по цепочкам + спред            $0.003
  /tvl           TVL цепочек/протокола                                     $0.002
  /snapshot      всё по токену сразу: цена + пулы + ликвидность            $0.005

  /safe-yields   (Этап 3) доходные И безопасные — мост к движку SCV

Бесплатные: / /health /pricing /openapi.json /llms.txt
            /.well-known/x402.json  /.well-known/x402  /robots.txt

Запуск:  python3 radar_server.py --port 8090
         (кошелёк и цены — в config.json рядом; env RDR_* сильнее файла)
Проверка без оплаты (локальные тесты):  python3 radar_server.py --free-test
================================================================================
"""

import argparse
import base64
import json
import os
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
SERVER_VERSION = "0.1.1"
X402_VERSION = 2
FACIL_TIMEOUT = 30          # секунд на ответ фасилитатора
HTTP_TIMEOUT = 25           # секунд на запрос к источникам данных

BAZAAR_DECLARATION = {
    "description": "DeFi market data aggregator for AI agents: yields, DEX "
                   "pools, token prices across chains, TVL. One request -> "
                   "normalized JSON answer.",
    "inputSchema": {"type": "object",
                    "properties": {
                        "chain": {"type": "string"},
                        "min_apy": {"type": "number"},
                        "min_tvl": {"type": "number"},
                        "limit": {"type": "integer"},
                        "q": {"type": "string"},
                        "token": {"type": "string"},
                        "mode": {"type": "string"},
                    }},
    "outputSchema": {"type": "object",
                     "properties": {"count": {"type": "integer"},
                                    "results": {"type": "array"}}},
}

CFG = {}
_PAYMENT_LOCK = threading.Lock()

# Иконка 64x64 PNG (тёмный круг, зелёные кольца радара, точка-цель):
# x402scan и каталоги показывают её рядом с именем ресурса. Без неё
# листинг выглядит «голым» (чек-лист x402scan: "Serve a /favicon.ico").
FAVICON_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAYAAACqaXHeAAAD0ElEQVR4nO1bO27cMBQcG75EmgC6gBsXbtxuArgJ4EalL6HGOYDTqMoNXKoJkMZAUrtx4cYXEJBmr+DOLrxUuBQ/88hHrYHdARaWKfKRM3x8lEgK2HMcLVnZ6cP9K5Pv+eJysXZVrYglnEJNQdQNx0iPbUfZaIY+eE9bDDVjPuIs4RR8gmgJUWzEJa5FOgRXjFIhigrb5GsTd2ELUSJCVsFdEndRKoS4QCn5ZuhvUnnGtvshtDldS0UQZc4hzxBOgREkVwQ6oyFfSpwkU1IWAC8ClUlC3m281J01bEpESGbIJZ9s5O+vs7Tx2x89+6QI0ZvsmC8l7kIqRDP067HtPjl5puuYCMfJ1mBZ8kw+u55m6Nf2XysPVVdQGcb1a5C3QXjC2k0LeULIC7yJEvJskPMJ8Ovs+3R99XQ7ux8TgCFv5QXgF4EaAh6Dork9Rd73f6jcpv4ZeQA/c545ZgJIoj45L9+5aT6yoXS3vKTnrfsA/G+sYg+QuL6PfA6MnRh50x6pF2wJkOp9lnwz9Hem0WPbXUsa5MKUZ3o+JkLIC7JiQAx2r4fI+wJeLB3AFzch5fYsaAGkUd8m74vmLtnQLCAd89KhMAkgfdnxQer2V0+30y9gzxft/+bGFt8woDyA6f0U+dRDzQwvK2/PWzEhKILEC9RjQAy0CAHy2u0BlARgXb8Z+ke8rN5/PrzfY6J90gtYnAA64z+FZugfN3Wch/OsPrtp2j0/th2aocfpw/3r88XlUdIDpNE/YIMg7w14/3LrZONA8RDQeODJIa81DKoHwVTvh+Z5k9+Ur4VFZwEXOS822tipAJ7ovih5YMcCAFtvc4uTBz6AAMDuyAMLCJAbzJipUwPFAmg+lUmgtd5wDPxfLPQdRMhdaXFsiLxAo/dDD3DuAuliMcAWISSEfa+26xucaBgZ2+7aLIPFXHJsu3NDMOYNKfJa7g8oCSBBbDgs1es2tjYKYm+FGosiGmDqYMc/oBwDas8INQSmBZDOBtoisPakr+9bAsSmQxZ272hvjLj2hTYAzPcHZ5uFWpsjm7xFDZeWj7WNFgCoJ4JBIoCJ8jNtiu0OF02DzdDfpETwBUZ2aLBeU/KUmn1AomStMLGmnzNUsnofIM8IpUQIVV4bqfqZg1LszpA33TmrU3wgUgKWfApRAZhzdrsQQeJ5RcfkDNiNk9pDgrWvelDSIEeETf5FT4pKyAMVD0uHhgP57CAuyx6MdFH9uLxGXGCPxQKVj8sblHwzwAgiGTYl5IHDJzOHj6YOn81pGLGxtx9O+rCXn87G8BE/nt57vAHrjhQL2ImmuwAAAABJRU5ErkJggg=="
)

# цены ручек, $USDC за вызов
PRICES = {
    "yields": 0.003,
    "pools": 0.003,
    "price": 0.003,
    "tvl": 0.002,
    "snapshot": 0.005,
}

PAYMENTS_LOG = HERE / "payments.log"


# ------------------------------------------------------------------
# журнал
# ------------------------------------------------------------------
def _utcnow_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def log(client, method, path, status, extra=""):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {client:<26} {method:<5} {path} -> {status} {extra}",
          flush=True)


def record_sale(tool, payer, tx, amount_atomic, total_ms, via=""):
    line = (f"{datetime.now().isoformat(timespec='seconds')} | {tool} | "
            f"${int(amount_atomic) / 1e6:.4f} | {payer} | "
            f"tx={tx} | {total_ms}ms | {via}\n")
    with _PAYMENT_LOCK:
        with PAYMENTS_LOG.open("a", encoding="utf-8") as f:
            f.write(line)


def cash_summary():
    """Касса: (продаж, сумма $) из payments.log."""
    if not PAYMENTS_LOG.exists():
        return 0, 0.0
    total, n = 0.0, 0
    with PAYMENTS_LOG.open(encoding="utf-8") as f:
        for line in f:
            for part in line.split("|"):
                part = part.strip()
                if part.startswith("$"):
                    try:
                        total += float(part[1:])
                        n += 1
                    except ValueError:
                        pass
    return n, total


# ------------------------------------------------------------------
# конфиг: config.json + env (env сильнее)
# ------------------------------------------------------------------
def _fail(msg):
    print("=" * 64)
    print(msg)
    print("=" * 64)
    sys.exit(1)


def load_config(args):
    p = HERE / "config.json"
    raw = {}
    if p.exists():
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            _fail(f"ОШИБКА: config.json повреждён ({e})")

    def val(env_key, file_key, default=None):
        if os.environ.get(env_key):
            return os.environ.get(env_key)
        if file_key in raw and raw[file_key] not in (None, ""):
            return raw[file_key]
        return default

    port = int(args.port or val("RDR_PORT", "port", 8090))
    pay_to = str(args.pay_to or val("RDR_PAY_TO", "pay_to", "") or "").strip()
    if not pay_to.startswith("0x") or len(pay_to) != 42:
        _fail(
            "ОШИБКА: не указан кошелёк для оплаты (pay_to).\n\n"
            "Что делать:\n"
            "  1. Откройте config.json рядом с radar_server.py\n"
            "  2. В строку \"pay_to\" вставьте адрес 0x...\n"
            "     (можно тот же, что у SCV — деньги идут в один карман)\n"
            "  Либо: export RDR_PAY_TO=0xВашАдрес")
    public_url = str(val("RDR_PUBLIC_URL", "public_url", "") or "").strip()
    facil = str(val("RDR_FACILITATOR", "facilitator_url",
                    "https://facilitator.payai.network")).strip().rstrip("/")

    net = {
        "caip2": "eip155:8453",
        "asset": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC (Base)
        "decimals": 6,
        "name": "USD Coin",
        "version": "2",
        "label": "Base (mainnet)",
    }
    free_test = bool(args.free_test)

    cfg = {
        "port": port,
        "pay_to": pay_to,
        "public_url": public_url,
        "facilitator": facil,
        "net": net,
        "free_test": free_test,
        "start_ts": time.time(),
    }
    for k, v in PRICES.items():
        key = f"{k.replace('-', '_')}_price_usd"
        try:
            PRICES[k] = float(val(f"RDR_{k.upper().replace('-', '_')}_PRICE",
                                  key, PRICES[k]))
        except (TypeError, ValueError):
            pass
    cfg["prices"] = PRICES
    return cfg


def atomic(usd):
    return str(int(round(float(usd) * (10 ** CFG["net"]["decimals"]))))


def requirements_for(resource_url, usd):
    net = CFG["net"]
    return {
        "scheme": "exact",
        "network": net["caip2"],
        "amount": atomic(usd),
        "asset": net["asset"],
        "payTo": CFG["pay_to"],
        "maxTimeoutSeconds": 60,
        "extra": {"name": net["name"], "version": net["version"]},
    }


def resource_info(path, description, tags):
    base = CFG["public_url"] or f"http://localhost:{CFG['port']}"
    return {
        "url": base + path,
        "description": description,
        "mimeType": "application/json",
        "serviceName": "DeFi Radar",
        "tags": tags,
    }


def payment_required(res, req, error):
    return {
        "x402Version": X402_VERSION,
        "error": error,
        "resource": res,
        "accepts": [req],
        "extensions": {"bazaar": BAZAAR_DECLARATION},
    }


# ------------------------------------------------------------------
# фасилитатор (PayAI): POST /verify и /settle
# ------------------------------------------------------------------
def facilitator_post(path, body):
    url = CFG["facilitator"] + path
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers={
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": f"DeFiRadar/{SERVER_VERSION}",
    })
    try:
        with urllib.request.urlopen(req, timeout=FACIL_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8")), "payai"
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode("utf-8")), "payai"
        except Exception:
            return {"__http_error__": e.code}, "payai"
    except Exception as e:
        return {"__network_error__": str(e)}, "payai"


# ------------------------------------------------------------------
# кэш источников (фоновое обновление больших файлов)
# ------------------------------------------------------------------
_CACHE = {}
_CACHE_LOCK = threading.Lock()


def cache_get(key, ttl):
    with _CACHE_LOCK:
        item = _CACHE.get(key)
        if item and (time.time() - item[0]) <= ttl:
            return item[1]
    return None


def cache_put(key, data):
    with _CACHE_LOCK:
        _CACHE[key] = (time.time(), data)


def http_json(url, timeout=HTTP_TIMEOUT, headers=None):
    h = {"User-Agent": f"DeFiRadar/{SERVER_VERSION}",
         "Accept": "application/json"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ------------------------- DefiLlama yields -------------------------
YIELDS_URL = "https://yields.llama.fi/pools"


def yields_raw():
    """Все пулы доходности DefiLlama (большой файл, кэш + фоновое обновление)."""
    data = cache_get("yields", 900)
    if data is not None:
        return data
    js = http_json(YIELDS_URL)
    pools = js.get("data") if isinstance(js, dict) else js
    pools = pools or []
    cache_put("yields", pools)
    return pools


def yields_refresher():
    while True:
        try:
            yields_raw()
        except Exception:
            pass
        time.sleep(600)


def _f(x, default=0.0):
    try:
        v = float(x)
        return v if v == v else default   # NaN guard
    except (TypeError, ValueError):
        return default


def yields_filter(chain, min_apy, min_tvl, stable_only, limit, project,
                  max_apy=None):
    pools = yields_raw() or []
    out = []
    for p in pools:
        if chain and str(p.get("chain", "")).lower() != chain.lower():
            continue
        if project and str(p.get("project", "")).lower() != project.lower():
            continue
        apy = _f(p.get("apy"), -1.0)
        tvl = _f(p.get("tvlUsd"))
        if min_apy is not None and apy < min_apy:
            continue
        if max_apy is not None and apy > max_apy:
            continue
        if min_tvl is not None and tvl < min_tvl:
            continue
        if stable_only and not p.get("stablecoin"):
            continue
        out.append({
            "project": p.get("project"),
            "chain": p.get("chain"),
            "symbol": p.get("symbol"),
            "apy_pct": round(apy, 2),
            "apy_base_pct": round(_f(p.get("apyBase")), 2),
            "apy_reward_pct": round(_f(p.get("apyReward")), 2),
            "tvl_usd": int(tvl),
            "stablecoin": bool(p.get("stablecoin")),
            "il_risk": p.get("ilRisk"),
            "exposure": p.get("exposure"),
            "pool_id": p.get("pool"),
            "url": "https://defillama.com/yields/pool/" + str(p.get("pool")),
        })
        if len(out) >= 4000:      # не тащим весь список в память результатов
            break
    out.sort(key=lambda r: (r["apy_pct"], r["tvl_usd"]), reverse=True)
    return out[:max(1, min(int(limit or 25), 100))], len(pools)


# ------------------------- DexScreener -------------------------
def dex_search(q, chain=None):
    key = f"dexsearch:{(q or '').lower()}:{(chain or '').lower()}"
    hit = cache_get(key, 60)
    if hit is not None:
        return hit
    js = http_json("https://api.dexscreener.com/latest/dex/search?q="
                   + urllib.parse.quote(q or ""))
    pairs = js.get("pairs") or []
    if chain:
        pairs = [p for p in pairs
                 if str(p.get("chainId", "")).lower() == chain.lower()]
    cache_put(key, pairs)
    return pairs


def dex_token(addresses):
    key = "dextokens:" + ",".join(a.lower() for a in addresses)
    hit = cache_get(key, 60)
    if hit is not None:
        return hit
    js = http_json("https://api.dexscreener.com/latest/dex/tokens/"
                   + ",".join(a for a in addresses))
    pairs = js.get("pairs") or []
    cache_put(key, pairs)
    return pairs


def dex_boosts():
    key = "dexboosts"
    hit = cache_get(key, 300)
    if hit is not None:
        return hit
    js = http_json("https://api.dexscreener.com/token-boosts/latest/v1")
    cache_put(key, js)
    return js


def norm_pair(p):
    base = p.get("baseToken") or {}
    quote = p.get("quoteToken") or {}
    vol = p.get("volume") or {}
    liq = p.get("liquidity") or {}
    txns = (p.get("txns") or {}).get("h24") or {}
    chg = p.get("priceChange") or {}
    return {
        "chain": p.get("chainId"),
        "dex": p.get("dexId"),
        "pair_address": p.get("pairAddress"),
        "base": base.get("symbol"),
        "base_address": base.get("address"),
        "quote": quote.get("symbol"),
        "price_usd": p.get("priceUsd"),
        "liquidity_usd": int(_f(liq.get("usd"))),
        "volume_h24_usd": int(_f(vol.get("h24"))),
        "volume_h1_usd": int(_f(vol.get("h1"))),
        "txns_h24": int(_f(txns.get("buys")) + _f(txns.get("sells"))),
        "price_change_h24_pct": _f(chg.get("h24")),
        "fdv_usd": int(_f(p.get("fdv"))),
        "url": p.get("url"),
    }


def pools_top(pairs, sort_by="liquidity", limit=25):
    rows = [norm_pair(p) for p in pairs]
    keyfn = {"liquidity": lambda r: r["liquidity_usd"],
             "volume": lambda r: r["volume_h24_usd"],
             "change": lambda r: r["price_change_h24_pct"]}.get(
                 sort_by, lambda r: r["liquidity_usd"])
    rows.sort(key=keyfn, reverse=True)
    return rows[:max(1, min(int(limit or 25), 50))]


# ------------------------- GeckoTerminal (fallback) -------------------------
def gt_trending(limit=25):
    key = "gt_trending"
    hit = cache_get(key, 300)
    if hit is not None:
        return hit
    js = http_json("https://api.geckoterminal.com/api/v2/networks/"
                   f"trending_pools?page=1&include=base_token,dex")
    rows = []
    inc = {i.get("id"): i for i in (js.get("included") or [])}
    for d in (js.get("data") or [])[:limit]:
        attrs = d.get("attributes") or {}
        rel = (d.get("relationships") or {})
        bt = (rel.get("base_token") or {}).get("data", {}).get("id", "")
        bt_attr = (inc.get(bt) or {}).get("attributes") or {}
        # сеть достаём из id пула (формат "eth_0x...") — в attributes её нет
        net_id = str(d.get("id") or "").split("_")[0] or None
        rows.append({
            "network": net_id or attrs.get("network"),
            "pool": attrs.get("address"),
            "name": attrs.get("name"),
            "base_symbol": bt_attr.get("symbol"),
            "price_usd": attrs.get("base_token_price_usd"),
            "volume_h24_usd": int(_f((attrs.get("volume_usd") or {})
                                     .get("h24"))),
            "liquidity_usd": int(_f(attrs.get("reserve_in_usd"))),
            "fdv_usd": int(_f(attrs.get("fdv_usd"))),
        })
    cache_put(key, rows)
    return rows


# ------------------------- DefiLlama TVL -------------------------
def llama_chains():
    key = "llama_chains"
    hit = cache_get(key, 600)
    if hit is not None:
        return hit
    js = http_json("https://api.llama.fi/v2/chains")
    rows = sorted(({"chain": c.get("name"),
                    "tvl_usd": int(_f(c.get("tvl"))),
                    "token": c.get("tokenSymbol")}
                   for c in js), key=lambda r: r["tvl_usd"], reverse=True)
    cache_put(key, rows)
    return rows


def llama_protocol_tvl(slug):
    key = f"llama_proto:{slug.lower()}"
    hit = cache_get(key, 600)
    if hit is not None:
        return hit
    tvl = http_json("https://api.llama.fi/tvl/" + urllib.parse.quote(slug))
    data = {"slug": slug, "tvl_usd": int(_f(tvl))}
    cache_put(key, data)
    return data


# ------------------------------------------------------------------
# работа ручек (вызывается ПОСЛЕ успешной оплаты)
# ------------------------------------------------------------------
def work_yields(q):
    def f():
        # строгая валидация: мусор в числах -> 400, а не молчаливый игнор
        def num(key):
            v = q.get(key)
            if v is None or str(v).strip() == "":
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                raise ValueError(f"{key} должен быть числом, получено: {v}")
        chain = (q.get("chain") or "").strip() or None
        min_apy = num("min_apy")
        min_tvl = num("min_tvl")
        try:
            limit = int(float(q.get("limit") or 25))
        except (TypeError, ValueError):
            raise ValueError("limit должен быть целым числом")
        stable = str(q.get("stable", "")).lower() in ("1", "true", "yes")
        project = (q.get("project") or "").strip() or None
        max_apy = num("max_apy")
        rows, total = yields_filter(chain, min_apy, min_tvl, stable,
                                    limit, project, max_apy)
        return {"tool": "yields", "count": len(rows), "total_pools_scanned":
                total, "filters": {"chain": chain, "min_apy": min_apy,
                                   "max_apy": max_apy,
                                   "min_tvl": min_tvl, "stablecoin": stable,
                                   "project": project},
                "results": rows,
                "source": "DefiLlama (yields.llama.fi)",
                "updated_at": _utcnow_iso()}
    return f


def work_pools(q):
    def f():
        mode = (q.get("mode") or "search").strip().lower()
        chain = (q.get("chain") or "").strip() or None
        qs = (q.get("q") or "").strip()
        limit = int(_f(q.get("limit"), 25))
        sort_by = (q.get("sort") or "liquidity").strip().lower()
        if mode == "search":
            if not qs:
                raise ValueError("mode=search требует параметр q "
                                 "(символ токена или адрес)")
            pairs = dex_search(qs, chain)
            rows = pools_top(pairs, sort_by, limit)
            return {"tool": "pools", "mode": mode, "count": len(rows),
                    "results": rows,
                    "source": "DexScreener",
                    "updated_at": _utcnow_iso()}
        if mode == "boosted":
            js = dex_boosts() or []
            rows = js if isinstance(js, list) else []
            if chain:
                rows = [r for r in rows
                        if str(r.get("chainId", "")).lower() == chain.lower()]
            rows = rows[:max(1, min(limit, 50))]
            return {"tool": "pools", "mode": mode, "count": len(rows),
                    "results": rows,
                    "note": "boosted tokens = прокси трендовости DexScreener",
                    "source": "DexScreener",
                    "updated_at": _utcnow_iso()}
        if mode == "trending":
            try:
                rows = gt_trending(limit)
                src = "GeckoTerminal"
            except Exception:
                # фоллбэк: boosted-токены DexScreener как прокси трендовости
                js = dex_boosts() or []
                rows = [{"chain": r.get("chainId"),
                         "token": r.get("tokenAddress"),
                         "boosts": r.get("amount"),
                         "url": r.get("url")}
                        for r in (js if isinstance(js, list) else [])]
                src = "DexScreener boosts (GeckoTerminal недоступен)"
            if chain:
                rows = [r for r in rows
                        if str(r.get("network", r.get("chain", "")))
                        .lower() == chain.lower()]
            rows = rows[:max(1, min(int(limit or 25), 50))]
            return {"tool": "pools", "mode": mode, "count": len(rows),
                    "results": rows, "source": src,
                    "updated_at": _utcnow_iso()}
        raise ValueError("mode должен быть: search | trending | boosted")
    return f


def work_price(q):
    def f():
        token = (q.get("token") or "").strip()
        if not token:
            raise ValueError("требуется параметр token (0x-адрес)")
        if not token.startswith("0x") or len(token) not in (42, 40):
            raise ValueError("token должен быть EVM-адресом 0x...")
        token = token if len(token) == 42 else "0x" + token
        pairs = dex_token([token])
        if not pairs:
            return {"tool": "price", "token": token, "count": 0,
                    "note": "не найдено на DEX (проверьте адрес)",
                    "source": "DexScreener"}
        # ВАЖНО: priceUsd в DexScreener — цена БАЗОВОГО токена пары.
        # Если запрошенный токен лежит в quote-стороне, priceUsd относится
        # к ДРУГОМУ токену (для USDC получали $0.70 вместо $1.00).
        # Оставляем только пары, где token — базовый.
        pairs = [p for p in pairs
                 if str((p.get("baseToken") or {}).get("address", ""))
                 .lower() == token.lower()]
        if not pairs:
            return {"tool": "price", "token": token, "count": 0,
                    "note": "токен встречается только в quote-стороне пар "
                            "— цена по базовой стороне недоступна",
                    "source": "DexScreener"}
        rows = []
        for p in pairs:
            n = norm_pair(p)
            if n["price_usd"] is None:
                continue
            rows.append({"chain": n["chain"], "dex": n["dex"],
                         "price_usd": float(_f(n["price_usd"], 0)),
                         "liquidity_usd": n["liquidity_usd"],
                         "volume_h24_usd": n["volume_h24_usd"],
                         "url": n["url"]})
        rows.sort(key=lambda r: r["liquidity_usd"], reverse=True)
        # цена цепочки = цена пары с МАКСИМАЛЬНОЙ ликвидностью в этой
        # цепочке (пары-пыль и клоны адреса не портят расчёт спреда)
        by_chain = {}
        for r in rows:
            if not r["price_usd"]:
                continue
            c = str(r["chain"] or "?").lower()
            cur = by_chain.get(c)
            if cur is None or r["liquidity_usd"] > cur["liquidity_usd"]:
                by_chain[c] = r
        chain_prices = [r["price_usd"] for r in by_chain.values()
                        if r["liquidity_usd"] >= 10000 and r["price_usd"] > 0]
        # выбросы: клоны адреса в чужих сетях — там по 0x-адресу лежит
        # ДРУГОЙ токен с ценой в миллионы раз ниже. Отбрасываем всё, что
        # дальше x5 от медианы, честно показывая это в ответе.
        excluded = 0
        if len(chain_prices) > 1:
            med = statistics.median(chain_prices)
            sane = [p for p in chain_prices if med / 5 <= p <= med * 5]
            excluded = len(chain_prices) - len(sane)
            chain_prices = sane
        spread = round((max(chain_prices) / min(chain_prices) - 1) * 100, 2) \
            if len(chain_prices) > 1 else 0.0
        return {"tool": "price", "token": token, "count": len(rows),
                "best_liquidity": rows[0] if rows else None,
                "cross_chain_spread_pct": spread,
                "outlier_chains_excluded": excluded,
                "chains_used": {r["chain"]: r["price_usd"]
                                for r in by_chain.values()},
                "results": rows[:50],
                "source": "DexScreener",
                "updated_at": _utcnow_iso()}
    return f


def work_tvl(q):
    def f():
        slug = (q.get("protocol") or "").strip()
        if slug:
            data = llama_protocol_tvl(slug)
            return {"tool": "tvl", "mode": "protocol", **data,
                    "source": "DefiLlama",
                    "updated_at": _utcnow_iso()}
        chains = llama_chains()
        limit = int(_f(q.get("limit"), 25))
        return {"tool": "tvl", "mode": "chains", "count": len(chains[:limit]),
                "total_tvl_usd": sum(c["tvl_usd"] for c in chains),
                "results": chains[:max(1, min(limit, 100))],
                "source": "DefiLlama",
                "updated_at": _utcnow_iso()}
    return f


def work_snapshot(q):
    def f():
        token = (q.get("token") or "").strip()
        if not token or not token.startswith("0x") or len(token) != 42:
            raise ValueError("требуется параметр token (EVM-адрес 0x...)")
        pairs = dex_token([token])
        # цену берём ТОЛЬКО из пар, где token — базовый (priceUsd = цена
        # базового токена); ликвидность/объём считаем по всем парам,
        # где токен участвует (обе стороны честно дают объём рынка)
        base_pairs = [p for p in pairs
                      if str((p.get("baseToken") or {}).get("address", ""))
                      .lower() == token.lower()]
        rows = [norm_pair(p) for p in base_pairs]
        rows.sort(key=lambda r: r["liquidity_usd"], reverse=True)
        all_rows = [norm_pair(p) for p in pairs]
        vol24 = sum(r["volume_h24_usd"] for r in all_rows)
        liq = sum(r["liquidity_usd"] for r in all_rows)
        best = rows[0] if rows else None
        out = {"tool": "snapshot", "token": token,
               "pairs_found": len(rows),
               "pairs_involving_token": len(all_rows),
               "best_pair": best,
               "price_usd": best["price_usd"] if best else None,
               "liquidity_total_usd": liq,
               "volume_h24_total_usd": vol24,
               "top_pairs": rows[:10],
               "sources": ["DexScreener"],
               "updated_at": _utcnow_iso()}
        return out
    return f


WORKS = {
    "yields": work_yields,
    "pools": work_pools,
    "price": work_price,
    "tvl": work_tvl,
    "snapshot": work_snapshot,
}

RES_DESC = {
    "yields": ("Top DeFi yield pools filtered by chain, min APY, min TVL, "
               "stablecoin flag. Sorted by APY. DefiLlama data."),
    "pools": ("DEX pools: mode=search (by token symbol/address), "
              "mode=trending, mode=boosted. Prices, liquidity, volumes, "
              "txns. DexScreener/GeckoTerminal."),
    "price": ("Token price aggregated across chains and DEXes with "
              "cross-chain spread and liquidity ranking."),
    "tvl": ("Total Value Locked: all chains overview or single protocol."),
    "snapshot": ("All-in-one token snapshot: price, best pair, total "
                 "liquidity, 24h volume, top-10 pairs."),
}

RES_TAGS = {
    "yields": ["defi", "yield", "apy", "farming", "pools"],
    "pools": ["defi", "dex", "pools", "liquidity", "trending"],
    "price": ["defi", "price", "token", "arbitrage", "cross-chain"],
    "tvl": ["defi", "tvl", "protocols", "chains"],
    "snapshot": ["defi", "token", "snapshot", "liquidity", "volume"],
}


# ------------------------------------------------------------------
# спеки (openapi / llms / manifest) — компактные
# ------------------------------------------------------------------
def openapi_spec():
    base = CFG["public_url"] or f"http://localhost:{CFG['port']}"
    paths = {}
    for name in WORKS:
        usd = PRICES[name]
        paths[f"/{name}"] = {
            "get": {
                "summary": RES_DESC[name],
                "x-402-price-usd": usd,
                "x-402-network": CFG["net"]["caip2"],
                "x-402-asset": CFG["net"]["asset"],
                "parameters": [
                    {"name": k, "in": "query", "schema": {"type": "string"}}
                    for k in ("chain", "min_apy", "max_apy", "min_tvl",
                              "limit", "q", "token", "mode", "protocol",
                              "sort", "project", "stable")
                ],
                "responses": {
                    "200": {"description": "Normalized JSON answer"},
                    "402": {"description": "Payment required (x402 v2): "
                            "sign EIP-3009 USDC payment, retry the same "
                            "request with PAYMENT-SIGNATURE header"},
                },
            }
        }
    # бесплатные ручки: в спеке с security: [] (чек-лист x402scan:
    # сканер отличает платные от бесплатных по этому полю)
    for free_path, free_desc in (
        ("/health", "Service status: version, uptime, sales, revenue"),
        ("/pricing", "Prices, wallet, facilitator, free endpoints"),
        ("/llms.txt", "Agent-friendly docs (plain text)"),
        ("/.well-known/x402.json", "x402 discovery manifest"),
        ("/favicon.ico", "Service icon (PNG 64x64)"),
    ):
        paths[free_path] = {
            "get": {
                "summary": free_desc,
                "security": [],
                "responses": {"200": {"description": "OK"}},
            }
        }
    return {
        "openapi": "3.1.0",
        "info": {"title": "DeFi Radar — DeFi market data for AI agents",
                 "version": SERVER_VERSION,
                 "description":
                     "Paid per call via x402 v2 (USDC on Base, no API keys). "
                     "Unpaid request -> 402 + PAYMENT-REQUIRED header. Sign "
                     "locally, retry with PAYMENT-SIGNATURE (base64 JSON "
                     "payment payload). Failed work = no charge.",
                 "contact": {"name": "DeFi Radar",
                             "email": "aaaddd999aaa@gmail.com"}},
        "servers": [{"url": base}],
        "paths": paths,
    }


def x402_manifest():
    base = CFG["public_url"] or f"http://localhost:{CFG['port']}"
    resources = []
    for name in WORKS:
        resources.append({
            "url": base + "/" + name,
            "description": RES_DESC[name],
            "mimeType": "application/json",
            "serviceName": "DeFi Radar",
            "tags": RES_TAGS[name],
            "x402PriceUsd": PRICES[name],
            "network": CFG["net"]["caip2"],
            "asset": CFG["net"]["asset"],
        })
    return {"x402Version": X402_VERSION, "resources": resources}


def llms_txt():
    base = CFG["public_url"] or f"http://localhost:{CFG['port']}"
    lines = ["# DeFi Radar", "",
             "DeFi market data aggregator for AI agents. x402 v2, USDC on "
             f"Base, no API keys. Base URL: {base}", "",
             "## Paid endpoints (GET, JSON)"]
    for name in WORKS:
        lines.append(f"- GET /{name} — ${PRICES[name]:.3f} per call. "
                     f"{RES_DESC[name]}")
    lines += ["",
              "## Payment flow (x402 v2)",
              "1. GET any paid endpoint -> 402 + PAYMENT-REQUIRED header "
              "(base64 JSON: accepts[0].amount, asset, payTo).",
              "2. Sign EIP-3009 USDC transfer locally (no gas).",
              "3. Retry same URL with PAYMENT-SIGNATURE header "
              "(base64 JSON payment payload).",
              "4. 200 + PAYMENT-RESPONSE header; answer JSON in body. "
              "Failed work = no charge.",
              "", "## Free", "- GET /health — status", "- GET /pricing — "
              "prices and facilitator", "- GET /openapi.json — full spec"]
    return ("\n".join(lines) + "\n").encode("utf-8")


def pricing_json():
    return {
        "service": "DeFi Radar",
        "version": SERVER_VERSION,
        "network": CFG["net"]["caip2"],
        "asset": CFG["net"]["asset"],
        "pay_to": CFG["pay_to"],
        "facilitator": CFG["facilitator"],
        "prices_usd": dict(PRICES),
        "free_endpoints": ["/", "/health", "/pricing", "/openapi.json",
                           "/llms.txt", "/robots.txt",
                           "/.well-known/x402.json", "/.well-known/x402"],
        "note": "x402 v2: 402 -> sign -> retry with PAYMENT-SIGNATURE",
    }


LANDING = (b"<!doctype html><html><head><meta charset='utf-8'>"
           b"<title>DeFi Radar</title></head>"
           b"<body style='font-family:system-ui;max-width:760px;margin:40px "
           b"auto;padding:0 16px'>"
           b"<h1>DeFi Radar</h1>"
           b"<p>DeFi market data for AI agents: yields, DEX pools, token "
           b"prices across chains, TVL. One request &mdash; normalized JSON "
           b"answer.</p>"
           b"<p><b>Pay per call via x402 v2</b> (USDC on Base), no API keys "
           b"or signup.</p>"
           b"<h3>For machines</h3><ul>"
           b"<li><code>/openapi.json</code> &mdash; OpenAPI spec</li>"
           b"<li><code>/llms.txt</code> &mdash; agent-friendly docs</li>"
           b"<li><code>/.well-known/x402.json</code> &mdash; discovery "
           b"manifest</li>"
           b"<li><code>/pricing</code> &mdash; prices</li></ul>"
           b"<h3>Endpoints</h3><ul>"
           b"<li>GET /yields &mdash; top yield pools ($0.003)</li>"
           b"<li>GET /pools &mdash; DEX pools ($0.003)</li>"
           b"<li>GET /price &mdash; token price across chains ($0.003)</li>"
           b"<li>GET /tvl &mdash; TVL ($0.002)</li>"
           b"<li>GET /snapshot &mdash; token snapshot ($0.005)</li></ul>"
           b"<p style='color:#666'>Powered by DefiLlama / DexScreener / "
           b"GeckoTerminal public data. Sibling service: "
           b"<a href='https://scv-agent.online'>SCV Contract Verification</a>"
           b"</p></body></html>")


# ------------------------------------------------------------------
# HTTP-обработчик
# ------------------------------------------------------------------
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer  # noqa: E402


class Handler(BaseHTTPRequestHandler):
    server_version = f"DeFiRadar/{SERVER_VERSION}"
    protocol_version = "HTTP/1.1"

    # известные боты/агенты — как в SCV v1.7.4
    _UA_TAGS = (
        "GPTBot", "OAI-SearchBot", "ChatGPT-User", "ChatGPT-Agent",
        "ClaudeBot", "Claude-Web", "Claude-User", "Claude-SearchBot",
        "PerplexityBot", "Perplexity-User", "Googlebot", "Google-Extended",
        "bingbot", "Applebot", "Bytespider", "CCBot", "Meta-ExternalAgent",
        "anthropic-ai", "DeepSeek", "YouBot", "cohere-ai",
    )

    def _guest(self):
        """cloudflared проксирует внешних как 127.0.0.1 — настоящий IP в
        CF-Connecting-IP / X-Forwarded-For. Рядом метка клиента."""
        ip = (self.headers.get("CF-Connecting-IP")
              or (self.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
              or self.client_address[0])
        ua = (self.headers.get("User-Agent") or "").strip()
        if not ua:
            return ip
        low = ua.lower()
        for bot in self._UA_TAGS:
            if bot.lower() in low:
                return f"{ip} [{bot}]"
        if "chrome" in low and ("edg" in low or "edg/" in low):
            return f"{ip} [Edge]"
        for br in ("Firefox", "Chrome", "Safari", "curl", "wget",
                   "python-requests", "python-httpx", "Go-http-client",
                   "okhttp", "undici", "axios"):
            if br.lower() in low:
                return f"{ip} [{br}]"
        return f"{ip} [{ua.split('/')[0].split(' ')[0][:24]}]"

    def log_message(self, fmt, *args):
        pass  # свой журнал: см. log()

    # ---------- низкоуровневые помощники ----------
    def _send_bytes(self, code, body, ctype, extra_headers=None):
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods",
                             "GET, POST, OPTIONS, HEAD")
            self.send_header("Access-Control-Allow-Headers",
                             "PAYMENT-SIGNATURE, Content-Type")
            for k, v in (extra_headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            if not getattr(self, "_head_only", False):
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, code, obj, extra_headers=None):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self._send_bytes(code, body, "application/json; charset=utf-8",
                         extra_headers)

    def _b64(self, obj):
        return base64.b64encode(
            json.dumps(obj, ensure_ascii=False).encode("utf-8")).decode("ascii")

    def _query(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        return parsed.path, {k: v[0] for k, v in qs.items() if v}

    def do_OPTIONS(self):
        self._send_bytes(204, b"", "text/plain")

    def do_HEAD(self):
        self._head_only = True
        try:
            self.do_GET()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            self._head_only = False

    # ---------- платный конвейер (x402 v2, как в SCV) ----------
    def _paid_tool(self, name, q, path_label):
        client = self._guest()
        t_start = time.perf_counter()
        net = CFG["net"]
        res = resource_info("/" + name, RES_DESC[name], RES_TAGS[name])
        req = requirements_for(res["url"], PRICES[name])

        def deny(error_msg):
            pr = payment_required(res, req, error_msg)
            self._json(402, pr, {"PAYMENT-REQUIRED": self._b64(pr)})
            log(client, "TOOL", path_label, 402, error_msg[:60])

        # режим локальных тестов: платёж пропускаем, данные реальные
        if CFG.get("free_test"):
            try:
                body = WORKS[name](q)()
            except ValueError as e:
                self._json(400, {"error": str(e)})
                log(client, "TOOL", path_label, 400, f"bad params: {e}")
                return
            except Exception as e:
                self._json(502, {"error": f"source failed: {e}"})
                log(client, "TOOL", path_label, 502, f"work failed: {e}")
                return
            body["note"] = "FREE-TEST MODE (no payment)"
            self._json(200, body)
            log(client, "TOOL", path_label, 200, "free-test")
            return

        payment_hdr = self.headers.get("PAYMENT-SIGNATURE")
        if not payment_hdr:
            deny("PAYMENT-SIGNATURE header is required")
            return
        try:
            payload = json.loads(base64.b64decode(payment_hdr))
        except Exception:
            deny("PAYMENT-SIGNATURE: ожидается base64-JSON (x402 V2)")
            return
        if not isinstance(payload, dict) or \
                payload.get("x402Version") != X402_VERSION:
            deny("PAYMENT-SIGNATURE: x402Version должен быть 2")
            return
        acc = payload.get("accepted") or {}
        if acc.get("scheme") != "exact" or acc.get("network") != net["caip2"]:
            deny("PAYMENT-SIGNATURE: accepted.scheme/network не совпадают")
            return
        pp = payload.get("payload") or {}
        if not (isinstance(pp, dict) and pp.get("signature")
                and isinstance(pp.get("authorization"), dict)):
            deny("PAYMENT-SIGNATURE: нет payload.signature/authorization")
            return

        vres, vprov = facilitator_post("/verify", {
            "x402Version": X402_VERSION,
            "paymentPayload": payload,
            "paymentRequirements": req,
        })
        if vres.get("__network_error__") or vres.get("__http_error__"):
            self._json(503, {"error": "Фасилитатор оплаты недоступен.",
                             "detail": vres})
            log(client, "TOOL", path_label, 503,
                f"facilitator {vprov} unreachable")
            return
        if not vres.get("isValid"):
            deny(f"Payment verification failed: "
                 f"{vres.get('invalidReason') or 'unknown'}")
            return
        payer = vres.get("payer")

        try:
            body = WORKS[name](q)()
        except ValueError as e:
            self._json(400, {"error": str(e)})
            log(client, "TOOL", path_label, 400,
                f"bad params, no charge: {str(e)[:50]}")
            return
        except Exception as e:
            self._json(502, {"error": f"Операция не выполнена, "
                                      f"оплата НЕ списана: {e}"})
            log(client, "TOOL", path_label, 502, "work failed, no charge")
            return
        work_ms = int((time.perf_counter() - t_start) * 1000)

        sres, sprov = facilitator_post("/settle", {
            "x402Version": X402_VERSION,
            "paymentPayload": payload,
            "paymentRequirements": req,
        })
        sres.setdefault("network", net["caip2"])
        if not sres.get("success"):
            sres["success"] = False
            sres.setdefault("errorReason", sres.get("error") or "unknown")
            sres.setdefault("transaction", "")
            self._json(402, {"error": f"Payment settlement failed: "
                                      f"{sres.get('errorReason')}"},
                       {"PAYMENT-RESPONSE": self._b64(sres)})
            log(client, "TOOL", path_label, 402,
                f"settle failed ({sprov}): {sres.get('errorReason')}")
            return

        body["payment"] = {
            "payer": sres.get("payer") or payer,
            "transaction": sres.get("transaction", ""),
            "network": sres.get("network"),
            "facilitator": sprov,
            "amount_atomic": req["amount"],
            "asset": req["asset"],
        }
        body["timing"] = {"total_ms": work_ms}
        self._json(200, body, {"PAYMENT-RESPONSE": self._b64(sres)})
        record_sale(name, body["payment"]["payer"],
                    body["payment"]["transaction"], int(req["amount"]),
                    work_ms, via=sprov)
        log(client, "TOOL", path_label, 200,
            f"payer={body['payment']['payer']} "
            f"tx={str(body['payment']['transaction'])[:18]}.. {work_ms}ms")

    # ---------- маршруты ----------
    def do_GET(self):
        route, q = self._query()
        if len(route) > 1 and route.endswith("/"):
            route = route.rstrip("/") or "/"
        client = self._guest()

        if route == "/":
            self._send_bytes(200, LANDING, "text/html; charset=utf-8")
            log(client, "GET", "/", 200)
        elif route == "/health":
            sales, cash = cash_summary()
            up = int(time.time() - CFG["start_ts"])
            self._json(200, {
                "service": "DeFi Radar", "version": SERVER_VERSION,
                "status": "ok", "uptime_s": up, "network": CFG["net"]["caip2"],
                "sales": sales, "revenue_usd": round(cash, 4),
                "free_test": CFG.get("free_test", False)})
            log(client, "GET", "/health", 200)
        elif route == "/pricing":
            self._json(200, pricing_json())
            log(client, "GET", "/pricing", 200)
        elif route == "/openapi.json":
            self._json(200, openapi_spec())
            log(client, "GET", "/openapi.json", 200)
        elif route == "/llms.txt":
            self._send_bytes(200, llms_txt(), "text/plain; charset=utf-8")
            log(client, "GET", "/llms.txt", 200)
        elif route == "/robots.txt":
            base = CFG["public_url"] or \
                f"http://localhost:{CFG['port']}"
            body = (f"User-agent: *\nAllow: /\n"
                    f"Sitemap: {base}/sitemap.xml\n").encode("utf-8")
            self._send_bytes(200, body, "text/plain; charset=utf-8")
            log(client, "GET", "/robots.txt", 200)
        elif route == "/sitemap.xml":
            base = CFG["public_url"] or \
                f"http://localhost:{CFG['port']}"
            urls = ["/"] + ["/" + n for n in WORKS]
            xml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                   '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                   + "".join(f"<url><loc>{base}{u}</loc></url>" for u in urls)
                   + "</urlset>").encode("utf-8")
            self._send_bytes(200, xml, "application/xml")
            log(client, "GET", "/sitemap.xml", 200)
        elif route in ("/.well-known/x402", "/.well-known/x402.json"):
            self._json(200, x402_manifest())
            log(client, "GET", route, 200)
        elif route.startswith("/") and route[1:] in WORKS:
            self._paid_tool(route[1:], q, route)
        elif route == "/favicon.ico":
            self._send_bytes(200, FAVICON_PNG, "image/png",
                             {"Cache-Control": "public, max-age=86400"})
            log(client, "GET", "/favicon.ico", 200)
        else:
            self._json(404, {"error": "Неизвестный путь. Смотри /llms.txt "
                                      "и /openapi.json"})
            log(client, "GET", route, 404)


# ------------------------------------------------------------------
# main
# ------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="DeFi Radar server (x402 v2)")
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--pay-to", dest="pay_to", default=None,
                    help="кошелёк 0x... (сильнее config.json)")
    ap.add_argument("--public-url", dest="public_url", default=None,
                    help="публичный адрес для манифеста")
    ap.add_argument("--free-test", action="store_true",
                    help="локальный тест: платные ручки без оплаты")
    args = ap.parse_args()
    global CFG
    CFG = load_config(args)
    if args.public_url:
        CFG["public_url"] = args.public_url

    print("=" * 64)
    print(f"  DeFi Radar v{SERVER_VERSION} — агрегатор данных для агентов")
    print("=" * 64)
    print(f"  Визитка (для людей):  http://localhost:{CFG['port']}/")
    print(f"  Машинам (агентам):    /openapi.json  /llms.txt")
    print(f"                        /.well-known/x402.json  /.well-known/x402")
    print(f"  Статус:               http://localhost:{CFG['port']}/health")
    prices_s = " | ".join(f"/{n} ${p:.3f}" for n, p in PRICES.items())
    print(f"  Платные ручки:        {prices_s}")
    print(f"  Сеть оплаты:          {CFG['net']['caip2']} — USDC")
    print(f"  Кошелёк для денег:    {CFG['pay_to']}")
    print(f"  Фасилитатор:          {CFG['facilitator']}")
    if CFG.get("free_test"):
        print("  !! РЕЖИМ FREE-TEST: платёж не требуется (только локально)")
    print("  Публичный адрес:      " + (CFG["public_url"] or "(не задан)"))
    print("  Остановка:            Ctrl+C")
    print("=" * 64)

    # фон: заранее качаем большой файл пулов доходности
    threading.Thread(target=yields_refresher, daemon=True).start()

    try:
        srv = ThreadingHTTPServer(("0.0.0.0", CFG["port"]), Handler)
    except OSError as e:
        _fail(f"ОШИБКА: не удалось занять порт {CFG['port']}: {e}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nОстановлено.")


if __name__ == "__main__":
    main()
