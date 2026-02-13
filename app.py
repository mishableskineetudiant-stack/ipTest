"""
Render Worker v3 — FAST
───────────────────────
• 20 workers (I/O bound, CPU idle)
• Timeout 3s (pas 5s)
• Batch de 50 (pas 10)
• Pas de pause entre batches
• Pas de limite de taille
• Progress en temps réel
"""

import os
import time
import logging
import threading
import concurrent.futures
import uuid

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("worker")

app = FastAPI(title="Proxy Worker v3", version="3.0.0")

TIMEOUT = 3
WORKERS = 20
BATCH_SIZE = 50
HEADERS = {"User-Agent": "Mozilla/5.0"}

TEST_APIS = [
    {"url": "http://httpbin.org/ip", "key": "origin"},
    {"url": "https://api.ipify.org?format=json", "key": "ip"},
    {"url": "https://ifconfig.me/all.json", "key": "ip_addr"},
]

_jobs = {}
_jobs_lock = threading.Lock()
_stats = {
    "total_tested": 0,
    "total_working": 0,
    "total_jobs": 0,
}


class BigBatchRequest(BaseModel):
    proxies: list[str]
    protocol: str = "http"
    real_ip: str = ""


class APIRotator:
    def __init__(self):
        self.apis = list(TEST_APIS)
        self.index = 0
        self.lock = threading.Lock()
        self._429_count = 0

    def get(self):
        with self.lock:
            return self.apis[self.index]

    def report_429(self):
        with self.lock:
            self._429_count += 1
            if self._429_count >= 3:
                old = self.apis[self.index]["url"]
                self.index = (self.index + 1) % len(self.apis)
                self._429_count = 0
                new = self.apis[self.index]["url"]
                if old != new:
                    logger.warning(f"API rotate: → {new}")

    def reset(self):
        with self.lock:
            self.index = 0
            self._429_count = 0


rotator = APIRotator()


def test_one(proxy, protocol, real_ip):
    url = f"{protocol}://{proxy}"
    px = {"http": url, "https": url}
    api = rotator.get()

    try:
        start = time.time()
        r = requests.get(
            api["url"], proxies=px, timeout=TIMEOUT,
            headers=HEADERS, allow_redirects=True,
        )
        latency = time.time() - start

        if r.status_code == 429:
            rotator.report_429()
            return None

        if r.status_code != 200 or latency > 3.5:
            return None

        try:
            proxy_ip = (
                r.json()
                .get(api["key"], "")
                .split(",")[0]
                .strip()
            )
        except Exception:
            proxy_ip = "unknown"

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


def process_job(job_id, proxies, protocol, real_ip):
    start = time.time()
    working = []
    tested = 0
    total = len(proxies)

    rotator.reset()

    logger.info(f"Job {job_id}: {total} proxies ({protocol})")

    # tout tester d'un coup avec 20 workers
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=WORKERS
    ) as ex:
        futs = {
            ex.submit(test_one, p, protocol, real_ip): p
            for p in proxies
        }

        for f in concurrent.futures.as_completed(futs):
            tested += 1
            r = f.result()
            if r:
                working.append(r)

            # update progress toutes les 50
            if tested % 50 == 0 or tested == total:
                progress = round(tested / total * 100, 1)
                with _jobs_lock:
                    if job_id in _jobs:
                        _jobs[job_id]["tested"] = tested
                        _jobs[job_id]["working"] = len(working)
                        _jobs[job_id]["progress"] = progress

                logger.info(
                    f"Job {job_id}: [{tested}/{total}] "
                    f"✅ {len(working)} ({progress}%)"
                )

    duration = round(time.time() - start, 1)

    logger.info(
        f"Job {job_id}: DONE "
        f"✅ {len(working)}/{total} in {duration}s"
    )

    _stats["total_tested"] += tested
    _stats["total_working"] += len(working)
    _stats["total_jobs"] += 1

    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "done"
            _jobs[job_id]["working"] = len(working)
            _jobs[job_id]["tested"] = tested
            _jobs[job_id]["duration"] = duration
            _jobs[job_id]["results"] = working
            _jobs[job_id]["progress"] = 100.0


@app.get("/")
async def root():
    active = sum(
        1 for j in _jobs.values() if j["status"] == "running"
    )
    return {
        "name": "Proxy Worker v3 FAST",
        "stats": _stats,
        "active_jobs": active,
        "config": {
            "workers": WORKERS,
            "timeout": TIMEOUT,
        },
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ping")
async def ping():
    return {"pong": True}


@app.post("/test")
async def test_batch(req: BigBatchRequest):
    if len(req.proxies) > 5000:
        raise HTTPException(400, "Max 5000")

    if not req.proxies:
        return {
            "job_id": None,
            "working": [],
            "tested": 0,
            "status": "empty",
        }

    job_id = str(uuid.uuid4())[:8]

    with _jobs_lock:
        _jobs[job_id] = {
            "status": "running",
            "total": len(req.proxies),
            "tested": 0,
            "working": 0,
            "progress": 0.0,
            "duration": 0,
            "results": [],
            "protocol": req.protocol,
            "started_at": time.time(),
        }

    thread = threading.Thread(
        target=process_job,
        args=(job_id, req.proxies, req.protocol, req.real_ip),
        daemon=True,
    )
    thread.start()

    return {
        "job_id": job_id,
        "total": len(req.proxies),
        "status": "running",
        "poll_url": f"/job/{job_id}",
    }


@app.get("/job/{job_id}")
async def get_job(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)

    if not job:
        raise HTTPException(404, "Not found")

    resp = {
        "job_id": job_id,
        "status": job["status"],
        "total": job["total"],
        "tested": job["tested"],
        "working": job["working"],
        "progress": job["progress"],
        "duration": job["duration"],
        "protocol": job["protocol"],
    }

    if job["status"] == "done":
        resp["results"] = job["results"]

    return resp


@app.delete("/job/{job_id}")
async def delete_job(job_id: str):
    with _jobs_lock:
        if job_id in _jobs:
            del _jobs[job_id]
            return {"deleted": True}
    raise HTTPException(404, "Not found")


@app.get("/jobs")
async def list_jobs():
    with _jobs_lock:
        return {
            "jobs": {
                jid: {
                    "status": j["status"],
                    "total": j["total"],
                    "tested": j["tested"],
                    "working": j["working"],
                    "progress": j["progress"],
                }
                for jid, j in _jobs.items()
            }
        }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
