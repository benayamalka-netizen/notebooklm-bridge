"""
גשר HTTP בין אפליקציית ארכיון השיעורים (Lovable) לבין NotebookLM, דרך notebooklm-py.

זרימה: האפליקציה שולחת mediaUrl ← הגשר מוריד את האודיו, פותח מחברת זמנית,
מעלה את הקובץ כמקור, מחכה לעיבוד, שולף את הטקסט המלא של המקור, ומוחק את המחברת.

משתני סביבה:
  BRIDGE_TOKEN           סוד משותף עם האפליקציה (כותרת Authorization: Bearer ...)
  ALLOWED_MEDIA_ORIGIN   המקור של האפליקציה, למשל https://my-app.lovable.app (מונע SSRF)
  NOTEBOOKLM_AUTH_JSON   תוכן storage_state.json של חשבון גוגל ייעודי (או קובץ ב-NOTEBOOKLM_HOME)
  MAX_CONCURRENT_JOBS    ברירת מחדל 2
  SOURCE_WAIT_SECONDS    כמה זמן לחכות לעיבוד המקור, ברירת מחדל 1800

חשוב להריץ worker אחד בלבד (המשימות נשמרות בזיכרון).
"""
import asyncio
import mimetypes
import os
import secrets
import shutil
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException
from notebooklm import NotebookLMClient
from pydantic import BaseModel

TOKEN = os.environ["BRIDGE_TOKEN"]
ALLOWED_ORIGIN = os.environ["ALLOWED_MEDIA_ORIGIN"].rstrip("/")
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_JOBS", "2"))
SOURCE_WAIT_SECONDS = float(os.getenv("SOURCE_WAIT_SECONDS", "1800"))
JOB_TTL_SECONDS = 6 * 3600

JOBS: dict[str, dict] = {}
SEM = asyncio.Semaphore(MAX_CONCURRENT)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # לקוח אחד לכל worker, לפי התיעוד של הספרייה
    async with NotebookLMClient.from_storage(keepalive=600) as client:
        app.state.client = client
        yield


app = FastAPI(lifespan=lifespan)


def require_token(authorization: str = Header(default="")) -> None:
    expected = f"Bearer {TOKEN}"
    if not secrets.compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="unauthorized")


class TranscribeRequest(BaseModel):
    mediaUrl: str
    title: str = "lesson"


def _purge_old_jobs() -> None:
    cutoff = time.time() - JOB_TTL_SECONDS
    for job_id in [k for k, v in JOBS.items() if v["created"] < cutoff]:
        JOBS.pop(job_id, None)


async def _run_job(client: NotebookLMClient, job_id: str, media_url: str, title: str) -> None:
    job = JOBS[job_id]
    workdir = Path(tempfile.mkdtemp(prefix="nlm-"))
    notebook_id: str | None = None
    try:
        async with SEM:
            async with httpx.AsyncClient(follow_redirects=False, timeout=httpx.Timeout(60.0, read=600.0)) as http:
                async with http.stream("GET", media_url) as resp:
                    resp.raise_for_status()
                    content_type = resp.headers.get("content-type", "").split(";")[0].strip().lower()
                    ext = mimetypes.guess_extension(content_type) or ".mp3"
                    path = workdir / f"lesson{ext}"
                    with path.open("wb") as fh:
                        async for chunk in resp.aiter_bytes(1 << 20):
                            fh.write(chunk)
            mime = content_type if content_type.startswith(("audio/", "video/")) else None

            notebook = await client.notebooks.create(f"transcribe-{job_id[:8]}")
            notebook_id = notebook.id
            source = await client.sources.add_file(
                notebook_id,
                path,
                mime,
                wait=True,
                wait_timeout=SOURCE_WAIT_SECONDS,
                title=title,
            )
            fulltext = await client.sources.get_fulltext(notebook_id, source.id)
            text = (fulltext.content or "").strip()
            if not text:
                raise RuntimeError("NotebookLM לא החזיר טקסט עבור הקובץ")
            job.update(status="completed", text=text)
    except Exception as exc:  # noqa: BLE001 - מחזירים לאפליקציה הודעה קריאה
        job.update(status="failed", error=f"{type(exc).__name__}: {exc}"[:500])
    finally:
        if notebook_id:
            try:
                await client.notebooks.delete(notebook_id)
            except Exception:  # noqa: BLE001
                pass
        shutil.rmtree(workdir, ignore_errors=True)


@app.post("/transcribe", dependencies=[Depends(require_token)])
async def start(req: TranscribeRequest):
    if not req.mediaUrl.startswith(ALLOWED_ORIGIN + "/"):
        raise HTTPException(status_code=400, detail="mediaUrl is not under the allowed origin")
    _purge_old_jobs()
    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"status": "pending", "created": time.time()}
    asyncio.create_task(_run_job(app.state.client, job_id, req.mediaUrl, req.title))
    return {"jobId": job_id}


@app.get("/transcribe/{job_id}", dependencies=[Depends(require_token)])
async def status(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="unknown job")
    return {k: v for k, v in job.items() if k != "created"}


@app.get("/health")
async def health():
    return {"ok": True}
