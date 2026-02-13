"""
Render Worker v2 — Reçoit 1000 proxies, gère tout
──────────────────────────────────────────────────
• Reçoit un gros pack de proxies
• Teste par mini-batch de 10 avec 5 workers
• Gère le rate limit des APIs de test
• Renvoie TOUS les résultats d'un coup
• Background processing pour pas timeout
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

app = FastAPI(title="Proxy Worker v2", version="2.0.0")

TIMEOUT = 5
WORKERS = 5
MINI_BATCH = 10
MINI_BATCH_PAUSE = 2.0
HEADERS = {"User-Agent": "Mozilla/5.0"}

# APIs de test avec rotation
TEST_APIS = [
    {"url": "http://httpbin.org/ip", "key": "origin"},
    {"url": "https://api.ipify.org?format=json", "key": "ip"},
    {"url": "https://ifconfig.me/all.json", "key": "ip_addr"},
]

# état des jobs en background
_jobs = {}
_jobs_lock = threading.Lock()

# stats globales
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
    """Tourne entre les APIs, switch sur rate limit."""

    def __init__(self):
        self.apis = list(TEST_APIS)
        self.index = 0
        self.lock = threading.Lock()
        self.fails = {}

    def get(self):
        with self.lock:
            return self.apis[self.index]

    def report_429(self):
        with self.lock:
            old = self.apis[self.index]["url"]
            self.index = (self.index + 1) % len(self.apis)
            new = self.apis[self.index]["url"]
            if old != new:
                logger.warning(f"API rotate: {old} → {new}")

    def reset(self):
        with self.lock:
            self.index = 0


rotator = APIRotator()


def test_one(proxy, protocol, real_ip):
    """Teste UN proxy."""
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

        if r.status_code != 200 or latency > 4.0:
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


def process_big_batch(job_id, proxies, protocol, real_ip):
    """
    Traite un gros batch en background.
    Mini-batch de 10 + pause 2s + rotation API.
    """
    start = time.time()
    working = []
    tested = 0
    total = len(proxies)

    rotator.reset()

    logger.info(
        f"Job {job_id}: START {total} proxies ({protocol})"
    )

    for i in range(0, total, MINI_BATCH):
        batch = proxies[i:i + MINI_BATCH]

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=WORKERS
        ) as ex:
            futs = {
                ex.submit(test_one, p, protocol, real_ip): p
                for p in batch
            }
            for f in concurrent.futures.as_completed(futs):
                tested += 1
                r = f.result()
                if r:
                    working.append(r)

        done = min(i + MINI_BATCH, total)
        if done % 50 == 0 or done == total:
            logger.info(
                f"Job {job_id}: [{done}/{total}] "
                f"✅ {len(working)}"
            )

        # update job status
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["tested"] = tested
                _jobs[job_id]["working"] = len(working)
                _jobs[job_id]["progress"] = round(
                    done / total * 100, 1
                )

        # pause entre mini-batches
        if i + MINI_BATCH < total:
            time.sleep(MINI_BATCH_PAUSE)

    duration = round(time.time() - start, 1)

    logger.info(
        f"Job {job_id}: DONE "
        f"✅ {len(working)}/{total} in {duration}s"
    )

    # update stats
    _stats["total_tested"] += tested
    _stats["total_working"] += len(working)
    _stats["total_jobs"] += 1

    # save results
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "done"
            _jobs[job_id]["working"] = len(working)
            _jobs[job_id]["tested"] = tested
            _jobs[job_id]["duration"] = duration
            _jobs[job_id]["results"] = working
            _jobs[job_id]["progress"] = 100.0


# ═══════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════

@app.get("/")
async def root():
    return {
        "name": "Proxy Worker v2",
        "stats": _stats,
        "active_jobs": sum(
            1 for j in _jobs.values()
            if j["status"] == "running"
        ),
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ping")
async def ping():
    return {"pong": True}


@app.post("/test")
async def test_big_batch(req: BigBatchRequest):
    """
    Reçoit un gros batch, lance en background.
    Retourne un job_id pour récupérer les résultats.
    """
    if len(req.proxies) > 2000:
        raise HTTPException(400, "Max 2000 per job")

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

    # lance en background
    thread = threading.Thread(
        target=process_big_batch,
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
    """Récupère le statut/résultats d'un job."""
    with _jobs_lock:
        job = _jobs.get(job_id)

    if not job:
        raise HTTPException(404, "Job not found")

    response = {
        "job_id": job_id,
        "status": job["status"],
        "total": job["total"],
        "tested": job["tested"],
        "working": job["working"],
        "progress": job["progress"],
        "duration": job["duration"],
        "protocol": job["protocol"],
    }

    # inclure les résultats seulement quand c'est fini
    if job["status"] == "done":
        response["results"] = job["results"]

    return response


@app.delete("/job/{job_id}")
async def delete_job(job_id: str):
    """Supprime un job terminé (libère la mémoire)."""
    with _jobs_lock:
        if job_id in _jobs:
            del _jobs[job_id]
            return {"deleted": True}
    raise HTTPException(404, "Job not found")


@app.get("/jobs")
async def list_jobs():
    """Liste tous les jobs."""
    with _jobs_lock:
        return {
            "jobs": {
                jid: {
                    "status": j["status"],
                    "total": j["total"],
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
