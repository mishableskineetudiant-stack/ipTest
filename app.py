"""
Render Worker — Teste les proxies pour HF
"""

import os
import time
import logging
import concurrent.futures

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("worker")

app = FastAPI(title="Proxy Test Worker", version="1.0.0")

TEST_URL = "http://httpbin.org/ip"
TIMEOUT = 5
MAX_WORKERS = 5
HEADERS = {"User-Agent": "Mozilla/5.0"}

_total_tested = 0
_total_working = 0
_total_batches = 0


class BatchRequest(BaseModel):
    proxies: list[str]
    protocol: str = "http"
    real_ip: str = ""


def test_one(proxy, protocol, real_ip):
    url = f"{protocol}://{proxy}"
    px = {"http": url, "https": url}
    try:
        start = time.time()
        r = requests.get(
            TEST_URL, proxies=px, timeout=TIMEOUT,
            headers=HEADERS, allow_redirects=True,
        )
        latency = time.time() - start
        if r.status_code != 200 or latency > 4.0:
            return None
        proxy_ip = r.json().get("origin", "").split(",")[0].strip()
        verified = bool(
            real_ip and proxy_ip
            and proxy_ip != "unknown"
            and proxy_ip != real_ip
        )
        return {
            "proxy": proxy,
            "latency": round(latency, 3),
            "proxy_ip": proxy_ip,
            "verified": verified,
        }
    except Exception:
        return None


@app.get("/")
async def root():
    return {
        "name": "Proxy Test Worker",
        "stats": {
            "batches": _total_batches,
            "tested": _total_tested,
            "working": _total_working,
        },
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ping")
async def ping():
    return {"pong": True}


@app.post("/test")
async def test_batch(req: BatchRequest):
    global _total_tested, _total_working, _total_batches

    if len(req.proxies) > 50:
        raise HTTPException(400, "Max 50 per batch")

    if not req.proxies:
        return {"working": [], "tested": 0, "duration": 0}

    start = time.time()
    working = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as ex:
        futs = {
            ex.submit(test_one, p, req.protocol, req.real_ip): p
            for p in req.proxies
        }
        for f in concurrent.futures.as_completed(futs):
            r = f.result()
            if r:
                working.append(r)

    duration = round(time.time() - start, 2)
    _total_batches += 1
    _total_tested += len(req.proxies)
    _total_working += len(working)

    logger.info(
        f"Batch #{_total_batches}: "
        f"{len(working)}/{len(req.proxies)} "
        f"({req.protocol}) {duration}s"
    )

    return {
        "working": working,
        "tested": len(req.proxies),
        "duration": duration,
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
