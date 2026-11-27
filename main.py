"""
גשר HTTP בין אתרי השיעורים לבין NotebookLM, דרך notebooklm-py.

מחברות ("תיקיות"): כל מקור חדש נכנס למחברת האחרונה בסדר המספרים, למשל "שיעורים 003".
כשהיא מלאה (SOURCE_LIMIT מקורות) נפתחת מחברת חדשה עם המספר הבא.

נקודות קצה (כולן עם Authorization: Bearer BRIDGE_TOKEN, חוץ מ-/health ומ-/files):
  POST /transcribe   {mediaUrl, title, notebookId?, notebookTitle?}   ← משימה: קובץ (שמע / מסמך) ← מקור ← טקסט מלא
  POST /sources      {url | text, title, notebookId?, notebookTitle?}  ← משימה: קישור מכל סוג / טקסט ← מקור ← טקסט מלא
  GET  /notebooks                                    ← המחברות (החדשות קודם)
  GET  /notebooks/{id}/sources                       ← המקורות במחברת (החדשים קודם)
  GET  /notebooks/{id}/sources/{sourceId}/text       ← הטקסט המלא של מקור (למשל תמלול שיעור)
  (בלי notebookId/notebookTitle המקור נכנס למחברת האוטומטית "שיעורים NNN"; notebookTitle שלא קיים – נוצר)
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
  NOTEBOOK_LIMIT         כמה מחברות מותר בחשבון, ברירת מחדל 100 (בחשבון Pro: 500)
  CLEANUP_BATCH          כמה מחברות ישנות למחוק כשאין מקום, ברירת מחדל 5
  CLEANUP_HOURS          כל כמה שעות לבדוק אם צריך לפנות מקום, ברירת מחדל 6

מחברות ציבוריות: כל מחברת "שיעורים NNN" משותפת לכל משתמשי האתרים. כשהיא נפתחת נכתב בה פתק
"📢 מחברת ציבורית". כשמספר המחברות בחשבון מגיע למגבלה, נמחקות CLEANUP_BATCH המחברות הישנות ביותר
מסוג "שיעורים NNN" בלבד (מחברות אחרות בחשבון לא נוגעים בהן), גם בבדיקה התקופתית וגם כשיצירה נכשלת.

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
NOTEBOOK_LIMIT = int(os.getenv("NOTEBOOK_LIMIT", "100"))
CLEANUP_BATCH = int(os.getenv("CLEANUP_BATCH", "5"))
CLEANUP_HOURS = float(os.getenv("CLEANUP_HOURS", "6"))
PUBLIC_NOTE_TITLE = "📢 מחברת ציבורית"
PUBLIC_NOTE = ("מחברת ציבורית: כל משתמשי אתרי השיעורים מעלים אליה שיעורים, ואין לשמור בה תוכן פרטי.\n"
               "כשהמקום בחשבון מתמלא, המחברות הציבוריות הישנות ביותר נמחקות אוטומטית "
               f"({CLEANUP_BATCH} בכל פעם).")
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
            cleaner = asyncio.create_task(_cleanup_loop(client))
            try:
                yield
            finally:
                cleaner.cancel()
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
            print(f"job {kind} {job_id} failed: {type(exc).__name__}: {exc}"[:800], flush=True)

    asyncio.create_task(runner())
    return job_id


def _public(job: dict) -> dict:
    return {k: v for k, v in job.items() if k not in ("created", "file")}


# ---------- תיקיות (מחברות) ----------

def _folder_number(title: str) -> int | None:
    m = re.fullmatch(re.escape(FOLDER_PREFIX) + r"\s*(\d+)", (title or "").strip())
    return int(m.group(1)) if m else None


async def make_room(c: NotebookLMClient, items=None, force: bool = False) -> list[str]:
    """כשהחשבון מגיע למגבלת המחברות: מוחקים את CLEANUP_BATCH המחברות הציבוריות הישנות ביותר.
    רק "שיעורים NNN", ולעולם לא האחרונה שבהן (אליה נכנסים עכשיו מקורות). נקרא בתוך FOLDER_LOCK."""
    items = items if items is not None else await c.notebooks.list()
    if not force and len(items) < NOTEBOOK_LIMIT - 1:
        return []
    numbered = sorted(((n, nb) for nb in items if (n := _folder_number(nb.title)) is not None), key=lambda x: x[0])
    victims = [nb for _, nb in numbered[:-1]][:CLEANUP_BATCH]
    deleted = []
    for nb in victims:
        try:
            await c.notebooks.delete(nb.id)
            deleted.append(nb.id)
        except Exception as exc:  # noqa: BLE001
            print(f"cleanup: could not delete {nb.title}: {exc}", flush=True)
    if deleted:
        print(f"cleanup: deleted {len(deleted)} old public notebooks", flush=True)
    return deleted


async def _cleanup_loop(c: NotebookLMClient) -> None:
    while True:
        try:
            async with FOLDER_LOCK:
                await make_room(c)
        except Exception as exc:  # noqa: BLE001
            print(f"cleanup check failed: {exc}", flush=True)
        await asyncio.sleep(CLEANUP_HOURS * 3600)


async def open_public_notebook(c: NotebookLMClient, title: str, items) -> str:
    """פותח מחברת חדשה (מפנה מקום קודם אם צריך) וכותב בה שהיא ציבורית. נקרא בתוך FOLDER_LOCK."""
    await make_room(c, items)
    try:
        created = await c.notebooks.create(title)
    except Exception as exc:
        # המגבלה האמיתית נמוכה מ-NOTEBOOK_LIMIT: מפנים מקום ומנסים שוב פעם אחת (רק בשגיאת מכסה)
        if not re.search(r"exhaust|quota|limit|RESOURCE", str(exc), re.I) or not await make_room(c, force=True):
            raise
        created = await c.notebooks.create(title)
    try:
        await c.notes.create(created.id, PUBLIC_NOTE_TITLE, PUBLIC_NOTE)
    except Exception as exc:  # noqa: BLE001 - הפתק לא חיוני להעלאה עצמה
        print(f"public note failed for {title}: {exc}", flush=True)
    return created.id


async def current_notebook(c: NotebookLMClient) -> str:
    """המחברת האחרונה לפי המספר; אם מלאה, פותחים את הבאה בתור."""
    async with FOLDER_LOCK:
        items = await c.notebooks.list()
        numbered = [(n, nb) for nb in items if (n := _folder_number(nb.title)) is not None]
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
        return await open_public_notebook(c, f"{FOLDER_PREFIX} {num:03d}", items)


async def target_notebook(c: NotebookLMClient, notebook_id: str | None, notebook_title: str | None) -> str:
    """מחברת שנבחרה (לפי מזהה, או לפי שם – ואם אין כזו, נוצרת), אחרת המחברת האוטומטית."""
    if notebook_id:
        return notebook_id
    if notebook_title and notebook_title.strip():
        name = notebook_title.strip()
        async with FOLDER_LOCK:
            for nb in await c.notebooks.list():
                if (nb.title or "").strip() == name:
                    return nb.id
            return (await c.notebooks.create(name)).id
    return await current_notebook(c)


def _iso(d) -> str | None:
    return d.isoformat() if d else None


async def _fulltext(c: NotebookLMClient, notebook_id: str, source_id: str) -> dict:
    ft = await c.sources.get_fulltext(notebook_id, source_id)
    text = (ft.content or "").strip()
    if not text:
        raise RuntimeError("NotebookLM לא החזיר טקסט עבור המקור")
    return {"text": text, "title": ft.title or "", "notebookId": notebook_id, "sourceId": source_id}


SOURCE_ID_RE = re.compile(r"Source ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})")


async def _dropping_failed(c: NotebookLMClient, notebook_id: str, adding):
    """מוסיף מקור; אם העיבוד שלו נכשל, מוחק את המקור השבור כדי שלא יתפוס מקום במחברת."""
    try:
        return await adding
    except Exception as exc:  # noqa: BLE001
        if (m := SOURCE_ID_RE.search(str(exc))):
            try:
                await c.sources.delete(notebook_id, m.group(1))
                print(f"dropped failed source {m.group(1)}", flush=True)
            except Exception as del_exc:  # noqa: BLE001
                print(f"could not drop failed source {m.group(1)}: {del_exc}", flush=True)
        raise


# ---------- תמלול קובץ שמע ----------

class TranscribeRequest(BaseModel):
    mediaUrl: str
    title: str = "lesson"
    notebookId: str | None = None
    notebookTitle: str | None = None


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
            notebook_id = await target_notebook(c, req.notebookId, req.notebookTitle)
            source = await _dropping_failed(c, notebook_id, c.sources.add_file(notebook_id, path, mime, wait=True, wait_timeout=SOURCE_WAIT_SECONDS, title=req.title))
            return await _fulltext(c, notebook_id, source.id)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    return {"jobId": _new_job("transcribe", run)}


# ---------- קישורים מכל הסוגים / טקסט ----------

class SourceRequest(BaseModel):
    url: str | None = None
    text: str | None = None
    title: str | None = None
    notebookId: str | None = None
    notebookTitle: str | None = None


DRIVE_RE = re.compile(r"https://(?:docs|drive)\.google\.com/.*?(?:/d/|[?&]id=)([A-Za-z0-9_-]{20,})")


@app.post("/sources", dependencies=[Depends(require_token)])
async def add_source(req: SourceRequest):
    if not (req.url or req.text):
        raise HTTPException(status_code=400, detail="url or text is required")
    if req.url and not re.match(r"https?://", req.url):
        raise HTTPException(status_code=400, detail="url must start with http(s)://")
    c = client()

    async def run(job_id: str) -> dict:
        notebook_id = await target_notebook(c, req.notebookId, req.notebookTitle)
        if req.text:
            adding = c.sources.add_text(notebook_id, (req.title or "טקסט")[:200], req.text, wait=True, wait_timeout=SOURCE_WAIT_SECONDS)
        elif (m := DRIVE_RE.match(req.url or "")):
            # Google Drive: מסמכים, מצגות, PDF וקבצי שמע מהדרייב של החשבון המחובר
            adding = c.sources.add_drive_file(notebook_id, m.group(1), title=req.title, wait=True, wait_timeout=SOURCE_WAIT_SECONDS)
        else:
            # אתרים, יוטיוב, קובצי PDF ושמע בכתובת ציבורית — NotebookLM מוריד בעצמו
            adding = c.sources.add_url(notebook_id, req.url, wait=True, wait_timeout=SOURCE_WAIT_SECONDS, title=req.title)
        source = await _dropping_failed(c, notebook_id, adding)
        return await _fulltext(c, notebook_id, source.id)

    return {"jobId": _new_job("source", run)}


# ---------- עיון במחברות ----------

@app.get("/notebooks", dependencies=[Depends(require_token)])
async def notebooks():
    c = client()
    items = await c.notebooks.list()
    items.sort(key=lambda n: n.created_at.timestamp() if n.created_at else 0, reverse=True)
    return {"notebooks": [{"id": n.id, "title": n.title, "sources": n.sources_count, "created": _iso(n.created_at)} for n in items]}


@app.get("/notebooks/{notebook_id}/sources", dependencies=[Depends(require_token)])
async def notebook_sources(notebook_id: str):
    c = client()
    items = await c.sources.list(notebook_id)
    items.sort(key=lambda s: s.created_at.timestamp() if s.created_at else 0, reverse=True)
    return {"sources": [{"id": s.id, "title": s.title, "created": _iso(s.created_at), "url": s.url} for s in items]}


@app.get("/notebooks/{notebook_id}/sources/{source_id}/text", dependencies=[Depends(require_token)])
async def source_text(notebook_id: str, source_id: str):
    return await _fulltext(client(), notebook_id, source_id)


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


async def download_artifact(c: NotebookLMClient, nb: str, k: str, artifact_id: str, out: Path) -> None:
    """מוריד תוצר סטודיו קיים מ-NotebookLM לקובץ (לפי סוג ומזהה)."""
    a = c.artifacts
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
    else:
        raise HTTPException(status_code=400, detail=f"cannot download kind {k}")


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
            await download_artifact(c, nb, k, artifact_id, out)
        JOBS[job_id]["file"] = str(out)
        result: dict[str, Any] = {"mime": mime, "fileName": f"{k}{ext}", "size": out.stat().st_size, "notebookId": nb}
        if k != "mind_map":
            result["artifactId"] = artifact_id
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


ARTIFACT_CACHE = Path(tempfile.gettempdir()) / "nlm-artifacts"
ARTIFACT_CACHE.mkdir(parents=True, exist_ok=True)


def artifact_signature(nb: str, kind: str, aid: str, exp: int) -> str:
    return hmac.new(TOKEN.encode(), f"{nb}:{kind}:{aid}:{exp}".encode(), hashlib.sha256).hexdigest()


@app.get("/artifacts/{notebook_id}/{kind}/{artifact_id}")
async def artifact_file(notebook_id: str, kind: str, artifact_id: str, exp: int, sig: str):
    """קישור חתום וקבוע לתוצר: מוריד אותו מ-NotebookLM כשצריך, כך שהוא לא הולך לאיבוד כשהשרת מתאתחל."""
    if exp < time.time() or not secrets.compare_digest(sig, artifact_signature(notebook_id, kind, artifact_id, exp)):
        raise HTTPException(status_code=403, detail="link expired or invalid")
    if kind not in STUDIO_KINDS or kind == "mind_map":
        raise HTTPException(status_code=400, detail="unknown kind")
    ext, mime = STUDIO_KINDS[kind]
    out = ARTIFACT_CACHE / f"{hashlib.sha256(f'{notebook_id}:{kind}:{artifact_id}'.encode()).hexdigest()}{ext}"
    if not out.exists() or out.stat().st_size == 0:
        await download_artifact(client(), notebook_id, kind, artifact_id, out)
    return FileResponse(out, media_type=mime, filename=f"{kind}{ext}")


@app.get("/notebooks/{notebook_id}/artifacts", dependencies=[Depends(require_token)])
async def notebook_artifacts(notebook_id: str):
    """התוצרים במחברת (החדשים קודם): לאתר שחוזר אחרי שיצא באמצע יצירה."""
    items = await client().artifacts.list(notebook_id)
    items.sort(key=lambda x: x.created_at.timestamp() if x.created_at else 0, reverse=True)
    return {"artifacts": [{"id": x.id, "kind": str(getattr(x.kind, "value", x.kind)), "title": x.title,
                           "complete": bool(getattr(x, "is_completed", False)), "failed": bool(getattr(x, "is_failed", False)),
                           "created": _iso(x.created_at)} for x in items]}


@app.get("/health")
async def health():
    # the reason a login failed (exception text only, never credentials) helps fix it from afar
    return {"ok": True, "auth": app.state.client is not None, "mode": app.state.auth_mode,
            **({"error": app.state.auth_error} if app.state.client is None and app.state.auth_error else {})}
