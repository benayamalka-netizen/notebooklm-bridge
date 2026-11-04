"""
גשר HTTP בין אתרי השיעורים לבין NotebookLM, דרך notebooklm-py.

מחברות ("תיקיות"): כל מקור חדש נכנס למחברת האחרונה בסדר המספרים, למשל "שיעורים 003".
כשהיא מלאה (SOURCE_LIMIT מקורות) נפתחת מחברת חדשה עם המספר הבא.

נקודות קצה (כולן עם Authorization: Bearer BRIDGE_TOKEN, חוץ מ-/health ומ-/files):
  POST /transcribe   {mediaUrl, title}              ← משימה: קובץ שמע ← מקור ← טקסט מלא
  POST /sources      {url | text, title}            ← משימה: קישור מכל סוג / טקסט ← מקור ← טקסט מלא
  POST /chat         {notebookId, question, sourceIds?, conversationId?}  ← תשובה מיידית
  POST /studio       {notebookId, kind, sourceIds?, instructions?, language?, options?}  ← משימה: תוצר סטודיו לקובץ
  GET  /jobs/{id}                                    ← מצב משימה (גם /transcribe/{id} לתאימות)
  GET  /files/{id}?exp=&sig=                         ← הורדת קובץ התוצר (קישור חתום, HMAC עם BRIDGE_TOKEN)
  GET  /health                                       ← {"ok": true, "auth": bool}

משתני סביבה:
  BRIDGE_TOKEN           סוד משותף עם האתרים
  ALLOWED_MEDIA_ORIGIN   מקורות מותרים לקובצי שמע, מופרדים בפסיקים (מונע SSRF)
  NOTEBOOKLM_MASTER_TOKEN_JSON  (מומלץ) תוכן master_token.json מ-"notebooklm login --master-token":
                         התחברות עמידה — הספרייה מנפיקה ממנה עוגיות חדשות כשהישנות פגות
  NOTEBOOKLM_AUTH_JSON   תוכן storage_state.json (עוגיות בלבד; פג תוך זמן קצר, רק לגיבוי)
  FOLDER_PREFIX          שם המחברות, ברירת מחדל "שיעורים"
  SOURCE_LIMIT           כמה מקורות במחברת לפני שנפתחת חדשה, ברירת מחדל 50 (בחשבון Pro: 300)
  MAX_CONCURRENT_JOBS    ברירת מחדל 2
  SOURCE_WAIT_SECONDS    כמה זמן לחכות לעיבוד מקור, ברירת מחדל 1800
  STUDIO_WAIT_SECONDS    כמה זמן לחכות לתוצר סטודיו, ברירת מחדל 2700

חשוב להריץ worker אחד בלבד (המשימות והקבצים נשמרים בזיכרון ובדיסק המקומי).
"""
import asyncio
import hashlib
import hmac
import json
import mimetypes
import os
import re
import secrets
import shutil
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from notebooklm import NotebookLMClient
from notebooklm import auth as nlm_auth
from notebooklm import paths as nlm_paths
from notebooklm import types as T
from pydantic import BaseModel

TOKEN = os.environ["BRIDGE_TOKEN"]
ALLOWED_PREFIXES = [o.strip().rstrip("/") + "/" for o in os.environ["ALLOWED_MEDIA_ORIGIN"].split(",") if o.strip()]
FOLDER_PREFIX = os.getenv("FOLDER_PREFIX", "שיעורים").strip()
SOURCE_LIMIT = int(os.getenv("SOURCE_LIMIT", "50"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_JOBS", "2"))
SOURCE_WAIT_SECONDS = float(os.getenv("SOURCE_WAIT_SECONDS", "1800"))
STUDIO_WAIT_SECONDS = float(os.getenv("STUDIO_WAIT_SECONDS", "2700"))
JOB_TTL_SECONDS = 6 * 3600
FILES_DIR = Path(tempfile.gettempdir()) / "nlm-files"
FILES_DIR.mkdir(parents=True, exist_ok=True)

JOBS: dict[str, dict] = {}
SEM = asyncio.Semaphore(MAX_CONCURRENT)
FOLDER_LOCK = asyncio.Lock()


async def _prepare_master_token() -> str:
    """Durable login: write the master token beside the profile's storage_state.json and mint
    fresh cookies from it. Inline NOTEBOOKLM_AUTH_JSON would bypass the library's re-mint path,
    so it's dropped when a master token is present."""
    secret = os.environ.pop("NOTEBOOKLM_MASTER_TOKEN_JSON", "").strip()
    if not secret:
        return "cookies" if os.getenv("NOTEBOOKLM_AUTH_JSON") else "none"
    os.environ.pop("NOTEBOOKLM_AUTH_JSON", None)
    token_path = nlm_paths.get_master_token_path()
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(secret, encoding="utf-8")
    os.chmod(token_path, 0o600)
    del secret
    await nlm_auth.bootstrap_missing_storage_from_master_token(nlm_paths.get_storage_path())
    return "master_token"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # לקוח אחד לכל worker, לפי התיעוד של הספרייה.
    # בלי התחברות לגוגל השרת עדיין עולה: /health מדווח auth=false והפעולות מחזירות 503.
    app.state.client = None
    app.state.auth_error = ""
    app.state.auth_mode = "none"
    try:
        app.state.auth_mode = await _prepare_master_token()
    except Exception as exc:  # noqa: BLE001
        app.state.auth_mode = "master_token"
        app.state.auth_error = f"master token: {type(exc).__name__}: {exc}"[:300]
    try:
        async with NotebookLMClient.from_storage(keepalive=600) as client:
            app.state.client = client
            yield
            return
    except Exception as exc:  # noqa: BLE001
        if app.state.client is not None:
            raise
        app.state.auth_error = (app.state.auth_error + " | " if app.state.auth_error else "") + f"{type(exc).__name__}: {exc}"[:300]
    yield


app = FastAPI(lifespan=lifespan)


def require_token(authorization: str = Header(default="")) -> None:
    if not secrets.compare_digest(authorization, f"Bearer {TOKEN}"):
        raise HTTPException(status_code=401, detail="unauthorized")


def client() -> NotebookLMClient:
    c = app.state.client
    if c is None:
        raise HTTPException(status_code=503, detail="NotebookLM is not logged in on the bridge")
    return c


# ---------- משימות ----------

def _purge_old() -> None:
    cutoff = time.time() - JOB_TTL_SECONDS
    for job_id in [k for k, v in JOBS.items() if v["created"] < cutoff]:
        job = JOBS.pop(job_id, None) or {}
        if job.get("file"):
            Path(job["file"]).unlink(missing_ok=True)


def _new_job(kind: str, coro_factory) -> str:
    _purge_old()
    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"status": "pending", "kind": kind, "created": time.time()}

    async def runner():
        try:
            async with SEM:
                JOBS[job_id]["status"] = "running"
                result = await coro_factory(job_id)
            JOBS[job_id].update(status="completed", **result)
        except Exception as exc:  # noqa: BLE001 - מחזירים לאתר הודעה קריאה
            JOBS[job_id].update(status="failed", error=f"{type(exc).__name__}: {exc}"[:500])

    asyncio.create_task(runner())
    return job_id


def _public(job: dict) -> dict:
    return {k: v for k, v in job.items() if k not in ("created", "file")}


# ---------- תיקיות (מחברות) ----------

def _folder_number(title: str) -> int | None:
    m = re.fullmatch(re.escape(FOLDER_PREFIX) + r"\s*(\d+)", (title or "").strip())
    return int(m.group(1)) if m else None


async def current_notebook(c: NotebookLMClient) -> str:
    """המחברת האחרונה לפי המספר; אם מלאה, פותחים את הבאה בתור."""
    async with FOLDER_LOCK:
        numbered = [(n, nb) for nb in await c.notebooks.list() if (n := _folder_number(nb.title)) is not None]
        if numbered:
            num, nb = max(numbered, key=lambda x: x[0])
            count = nb.sources_count
            if count is None:
                count = len(await c.notebooks.get_source_ids(nb.id))
            if count < SOURCE_LIMIT:
                return nb.id
            num += 1
        else:
            num = 1
        created = await c.notebooks.create(f"{FOLDER_PREFIX} {num:03d}")
        return created.id


async def _fulltext(c: NotebookLMClient, notebook_id: str, source_id: str) -> dict:
    ft = await c.sources.get_fulltext(notebook_id, source_id)
    text = (ft.content or "").strip()
    if not text:
        raise RuntimeError("NotebookLM לא החזיר טקסט עבור המקור")
    return {"text": text, "title": ft.title or "", "notebookId": notebook_id, "sourceId": source_id}


# ---------- תמלול קובץ שמע ----------

class TranscribeRequest(BaseModel):
    mediaUrl: str
    title: str = "lesson"


@app.post("/transcribe", dependencies=[Depends(require_token)])
async def transcribe(req: TranscribeRequest):
    if not any(req.mediaUrl.startswith(p) for p in ALLOWED_PREFIXES):
        raise HTTPException(status_code=400, detail="mediaUrl is not under the allowed origin")
    c = client()

    async def run(job_id: str) -> dict:
        workdir = Path(tempfile.mkdtemp(prefix="nlm-"))
        try:
            async with httpx.AsyncClient(follow_redirects=False, timeout=httpx.Timeout(60.0, read=600.0)) as http:
                async with http.stream("GET", req.mediaUrl) as resp:
                    resp.raise_for_status()
                    content_type = resp.headers.get("content-type", "").split(";")[0].strip().lower()
                    ext = mimetypes.guess_extension(content_type) or ".mp3"
                    path = workdir / f"lesson{ext}"
                    with path.open("wb") as fh:
                        async for chunk in resp.aiter_bytes(1 << 20):
                            fh.write(chunk)
            mime = content_type if content_type.startswith(("audio/", "video/")) else None
            notebook_id = await current_notebook(c)
            source = await c.sources.add_file(notebook_id, path, mime, wait=True, wait_timeout=SOURCE_WAIT_SECONDS, title=req.title)
            return await _fulltext(c, notebook_id, source.id)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    return {"jobId": _new_job("transcribe", run)}


# ---------- קישורים מכל הסוגים / טקסט ----------

class SourceRequest(BaseModel):
    url: str | None = None
    text: str | None = None
    title: str | None = None


DRIVE_RE = re.compile(r"https://(?:docs|drive)\.google\.com/.*?(?:/d/|[?&]id=)([A-Za-z0-9_-]{20,})")


@app.post("/sources", dependencies=[Depends(require_token)])
async def add_source(req: SourceRequest):
    if not (req.url or req.text):
        raise HTTPException(status_code=400, detail="url or text is required")
    if req.url and not re.match(r"https?://", req.url):
        raise HTTPException(status_code=400, detail="url must start with http(s)://")
    c = client()

    async def run(job_id: str) -> dict:
        notebook_id = await current_notebook(c)
        if req.text:
            source = await c.sources.add_text(notebook_id, (req.title or "טקסט")[:200], req.text, wait=True, wait_timeout=SOURCE_WAIT_SECONDS)
        elif (m := DRIVE_RE.match(req.url or "")):
            # Google Drive: מסמכים, מצגות, PDF וקבצי שמע מהדרייב של החשבון המחובר
            source = await c.sources.add_drive_file(notebook_id, m.group(1), title=req.title, wait=True, wait_timeout=SOURCE_WAIT_SECONDS)
        else:
            # אתרים, יוטיוב, קובצי PDF ושמע בכתובת ציבורית — NotebookLM מוריד בעצמו
            source = await c.sources.add_url(notebook_id, req.url, wait=True, wait_timeout=SOURCE_WAIT_SECONDS, title=req.title)
        return await _fulltext(c, notebook_id, source.id)

    return {"jobId": _new_job("source", run)}


# ---------- צ'אט ----------

class ChatRequest(BaseModel):
    notebookId: str
    question: str
    sourceIds: list[str] | None = None
    conversationId: str | None = None


@app.post("/chat", dependencies=[Depends(require_token)])
async def chat(req: ChatRequest):
    c = client()
    r = await c.chat.ask(req.notebookId, req.question, source_ids=req.sourceIds or None, conversation_id=req.conversationId)
    return {"answer": r.answer, "conversationId": r.conversation_id}


# ---------- סטודיו ----------

STUDIO_KINDS = {
    # kind: (סיומת הקובץ, סוג MIME)
    "audio": (".mp3", "audio/mpeg"),
    "video": (".mp4", "video/mp4"),
    "slide_deck": (".pdf", "application/pdf"),
    "slide_deck_pptx": (".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
    "infographic": (".png", "image/png"),
    "report": (".md", "text/markdown; charset=utf-8"),
    "study_guide": (".md", "text/markdown; charset=utf-8"),
    "quiz": (".json", "application/json"),
    "flashcards": (".json", "application/json"),
    "data_table": (".csv", "text/csv; charset=utf-8"),
    "mind_map": (".json", "application/json"),
}


class StudioRequest(BaseModel):
    notebookId: str
    kind: str
    sourceIds: list[str] | None = None
    instructions: str | None = None
    language: str = "he"
    options: dict[str, Any] = {}


def _enum(enum_cls, value):
    if not value:
        return None
    try:
        return enum_cls[str(value).upper()]
    except KeyError:
        raise HTTPException(status_code=400, detail=f"bad option {value!r} for {enum_cls.__name__}")


@app.post("/studio", dependencies=[Depends(require_token)])
async def studio(req: StudioRequest):
    if req.kind not in STUDIO_KINDS:
        raise HTTPException(status_code=400, detail=f"unknown kind; one of {sorted(STUDIO_KINDS)}")
    c = client()
    nb, src, lang, ins, o = req.notebookId, req.sourceIds or None, req.language, req.instructions, req.options
    a = c.artifacts

    # בודקים את האפשרויות לפני שמתחילים, כדי להחזיר שגיאה מיד
    opts = {
        "audio_format": _enum(T.AudioFormat, o.get("format")) if req.kind == "audio" else None,
        "audio_length": _enum(T.AudioLength, o.get("length")) if req.kind == "audio" else None,
        "video_format": _enum(T.VideoFormat, o.get("format")) if req.kind == "video" else None,
        "slide_format": _enum(T.SlideDeckFormat, o.get("format")) if req.kind.startswith("slide_deck") else None,
        "slide_length": _enum(T.SlideDeckLength, o.get("length")) if req.kind.startswith("slide_deck") else None,
        "orientation": _enum(T.InfographicOrientation, o.get("orientation")) if req.kind == "infographic" else None,
        "report_format": _enum(T.ReportFormat, o.get("format")) if req.kind == "report" else None,
        "quantity": _enum(T.QuizQuantity, o.get("quantity")) if req.kind in ("quiz", "flashcards") else None,
        "difficulty": _enum(T.QuizDifficulty, o.get("difficulty")) if req.kind in ("quiz", "flashcards") else None,
    }

    async def run(job_id: str) -> dict:
        ext, mime = STUDIO_KINDS[req.kind]
        out = FILES_DIR / f"{job_id}{ext}"
        k = req.kind
        if k == "mind_map":
            r = await a.generate_mind_map(nb, src, language=lang, instructions=ins)
            out.write_text(json.dumps(r.mind_map, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            if k == "audio":
                st = await a.generate_audio(nb, src, language=lang, instructions=ins, audio_format=opts["audio_format"], audio_length=opts["audio_length"])
            elif k == "video":
                st = await a.generate_video(nb, src, language=lang, instructions=ins, video_format=opts["video_format"])
            elif k.startswith("slide_deck"):
                st = await a.generate_slide_deck(nb, src, language=lang, instructions=ins, slide_format=opts["slide_format"], slide_length=opts["slide_length"])
            elif k == "infographic":
                st = await a.generate_infographic(nb, src, language=lang, instructions=ins, orientation=opts["orientation"])
            elif k == "report":
                fmt = opts["report_format"] or (T.ReportFormat.CUSTOM if ins else T.ReportFormat.BRIEFING_DOC)
                st = await a.generate_report(nb, fmt, src, language=lang, custom_prompt=ins if fmt == T.ReportFormat.CUSTOM else None,
                                             extra_instructions=ins if fmt != T.ReportFormat.CUSTOM else None)
            elif k == "study_guide":
                st = await a.generate_study_guide(nb, src, language=lang, extra_instructions=ins)
            elif k == "quiz":
                st = await a.generate_quiz(nb, src, instructions=ins, quantity=opts["quantity"], difficulty=opts["difficulty"])
            elif k == "flashcards":
                st = await a.generate_flashcards(nb, src, instructions=ins, quantity=opts["quantity"], difficulty=opts["difficulty"])
            elif k == "data_table":
                st = await a.generate_data_table(nb, src, language=lang, instructions=ins)
            else:  # pragma: no cover
                raise RuntimeError("unknown kind")
            if st.is_failed:
                raise RuntimeError(st.error or "NotebookLM לא התחיל ליצור את התוצר")
            st = await a.wait_for_completion(nb, st.task_id, timeout=STUDIO_WAIT_SECONDS, max_interval=20.0)
            if not st.is_complete:
                raise RuntimeError(st.error or f"התוצר לא הושלם ({st.status})")
            artifact_id = st.task_id
            if k == "audio":
                await a.download_audio(nb, str(out), artifact_id)
            elif k == "video":
                await a.download_video(nb, str(out), artifact_id)
            elif k == "slide_deck":
                await a.download_slide_deck(nb, str(out), artifact_id, output_format="pdf")
            elif k == "slide_deck_pptx":
                await a.download_slide_deck(nb, str(out), artifact_id, output_format="pptx")
            elif k == "infographic":
                await a.download_infographic(nb, str(out), artifact_id)
            elif k in ("report", "study_guide"):
                await a.download_report(nb, str(out), artifact_id)
            elif k == "quiz":
                await a.download_quiz(nb, str(out), artifact_id, output_format="json")
            elif k == "flashcards":
                await a.download_flashcards(nb, str(out), artifact_id, output_format="json")
            elif k == "data_table":
                await a.download_data_table(nb, str(out), artifact_id)
        JOBS[job_id]["file"] = str(out)
        result: dict[str, Any] = {"mime": mime, "fileName": f"{k}{ext}", "size": out.stat().st_size}
        # תוצרים טקסטואליים קטנים חוזרים גם כטקסט, כדי שהאתר יציג אותם בלי הורדה
        if ext in (".md", ".json", ".csv") and out.stat().st_size < 400_000:
            result["content"] = out.read_text(encoding="utf-8", errors="replace")
        return result

    return {"jobId": _new_job("studio", run)}


# ---------- מצב משימות וקבצים ----------

@app.get("/jobs/{job_id}", dependencies=[Depends(require_token)])
@app.get("/transcribe/{job_id}", dependencies=[Depends(require_token)])
async def job_status(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="unknown job")
    return _public(job)


def file_signature(job_id: str, exp: int) -> str:
    return hmac.new(TOKEN.encode(), f"{job_id}:{exp}".encode(), hashlib.sha256).hexdigest()


@app.get("/files/{job_id}")
async def download(job_id: str, exp: int, sig: str):
    if exp < time.time() or not secrets.compare_digest(sig, file_signature(job_id, exp)):
        raise HTTPException(status_code=403, detail="link expired or invalid")
    job = JOBS.get(job_id)
    if not job or not job.get("file") or not Path(job["file"]).exists():
        raise HTTPException(status_code=404, detail="file is gone; create it again")
    return FileResponse(job["file"], media_type=job.get("mime"), filename=job.get("fileName"))


@app.get("/health")
async def health():
    # the reason a login failed (exception text only, never credentials) helps fix it from afar
    return {"ok": True, "auth": app.state.client is not None, "mode": app.state.auth_mode,
            **({"error": app.state.auth_error} if app.state.client is None and app.state.auth_error else {})}
