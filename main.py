import os
import uuid
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from celery.result import AsyncResult

from config import settings
from schemas import JobStatus, TranscriptionResult
from worker import celery_app, transcribe_audio
from faster_whisper.utils import available_models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHUNK_SIZE = 64 * 1024


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(settings.upload_dir, exist_ok=True)
    logger.info(
        "Starting API (model_size=%s, device=%s, compute_type=%s)",
        settings.model_size,
        settings.device,
        settings.compute_type,
    )
    yield
    logger.info("Shutting down API")


app = FastAPI(title="Faster Whisper Transcriber", lifespan=lifespan)


def worker_alive():
    try:
        insp = celery_app.control.inspect(timeout=2)
        return bool(insp.ping())
    except Exception:
        return False


def get_queue_depth():
    insp = celery_app.control.inspect(timeout=2)
    active = insp.active() or {}
    reserved = insp.reserved() or {}
    total = sum(len(v) for v in active.values()) + sum(len(v) for v in reserved.values())
    return total


@app.post("/transcribe/async")
async def transcribe_async(
    file: UploadFile = File(...),
    language: str = Form(None),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    max_bytes = settings.max_file_size_mb * 1024 * 1024
    file_id = str(uuid.uuid4())
    ext = os.path.splitext(file.filename or "audio")[1] or ".ogg"
    safe_name = f"{file_id}{ext}"
    dest = os.path.join(settings.upload_dir, safe_name)

    os.makedirs(settings.upload_dir, exist_ok=True)
    total_read = 0
    with open(dest, "wb") as f:
        while True:
            chunk = await file.read(CHUNK_SIZE)
            if not chunk:
                break
            total_read += len(chunk)
            if total_read > max_bytes:
                f.close()
                os.remove(dest)
                raise HTTPException(
                    status_code=413,
                    detail=f"File too large (max {settings.max_file_size_mb}MB)",
                )
            f.write(chunk)

    task = transcribe_audio.delay(dest, language=language)
    return {"job_id": task.id}


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    task = AsyncResult(job_id, app=celery_app)

    if task.state == "PENDING":
        return JobStatus(
            job_id=job_id,
            status="pending",
            estimated_wait_seconds=120,
        ).model_dump()

    if task.state == "STARTED":
        meta = task.info or {}
        return JobStatus(
            job_id=job_id,
            status="running",
            progress=meta.get("progress"),
        ).model_dump()

    if task.state == "SUCCESS":
        result_data = task.result
        result_obj = TranscriptionResult(**result_data) if result_data else None
        return JobStatus(
            job_id=job_id,
            status="done",
            result=result_obj,
        ).model_dump()

    if task.state == "FAILURE":
        return JobStatus(
            job_id=job_id,
            status="failed",
            error=str(task.info) if task.info else "Unknown error",
        ).model_dump()

    return JobStatus(job_id=job_id, status=task.state.lower()).model_dump()


@app.get("/health")
async def health():
    queue_depth = get_queue_depth()
    alive = worker_alive()

    return {
        "status": "alive",
        "model_loaded": alive,
        "queue_depth": queue_depth,
    }


@app.get("/ready")
async def ready():
    if worker_alive():
        return JSONResponse(content={"status": "ready"})
    return JSONResponse(
        status_code=503,
        content={"status": "not_ready", "reason": "worker_not_available"},
    )


@app.get("/models")
async def models():
    all_models = available_models()
    return {
        "available": all_models,
        "current": settings.model_size,
    }
