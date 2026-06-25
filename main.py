import os
import json
import uuid
import shutil
import subprocess
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from celery.result import AsyncResult

from pymongo import MongoClient

from config import settings
from schemas import JobStatus, TranscriptionResult
from worker import celery_app, process_media
from faster_whisper.utils import available_models
import s3_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHUNK_SIZE = 64 * 1024

_mongo_client = None


def get_mongo():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(settings.mongo_url)
    return _mongo_client["whisper"]["transcriptions"]


def detect_media_type(file_path: str) -> dict:
    result = subprocess.run(
        ["ffprobe", "-v", "error",
         "-show_entries", "stream=codec_type,codec_name",
         "-of", "json", file_path],
        capture_output=True, text=True, check=True,
    )
    data = json.loads(result.stdout)
    has_video = False
    audio_codec = None
    for stream in data.get("streams", []):
        if stream.get("codec_type") == "video":
            has_video = True
        if stream.get("codec_type") == "audio":
            audio_codec = stream.get("codec_name")
    return {"has_video": has_video, "audio_codec": audio_codec}


def needs_conversion(media_info: dict, file_size: int) -> bool:
    if media_info["has_video"]:
        return True
    if file_size > 10 * 1024 * 1024:
        return True
    if media_info.get("audio_codec") not in ("opus", "vorbis"):
        return True
    return False


def prepare_audio(input_path: str, output_path: str) -> str:
    subprocess.run(
        ["ffmpeg", "-y", "-i", input_path,
         "-vn",
         "-ar", "16000",
         "-ac", "1",
         "-c:a", "libopus",
         "-b:a", "32k",
         output_path],
        capture_output=True, check=True,
    )
    return output_path


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(settings.local_processing_dir, exist_ok=True)
    s3_client.ensure_bucket()
    logger.info("Starting API (model_size=%s, device=%s)", settings.model_size, settings.device)
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
    model_size: str = Form(None),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    if model_size and model_size not in available_models():
        raise HTTPException(
            status_code=400,
            detail=f"Unknown model '{model_size}'. Available: {available_models()}",
        )

    job_id = str(uuid.uuid4())
    job_dir = os.path.join(settings.local_processing_dir, job_id)
    os.makedirs(job_dir, exist_ok=True)

    try:
        ext = os.path.splitext(file.filename or "media")[1] or ".bin"
        original_path = os.path.join(job_dir, f"original{ext}")
        max_bytes = settings.max_file_size_mb * 1024 * 1024
        total_read = 0
        with open(original_path, "wb") as f:
            while True:
                chunk = await file.read(CHUNK_SIZE)
                if not chunk:
                    break
                total_read += len(chunk)
                if total_read > max_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large (max {settings.max_file_size_mb}MB)",
                    )
                f.write(chunk)

        media_info = detect_media_type(original_path)

        if needs_conversion(media_info, total_read):
            compressed_path = os.path.join(job_dir, "compressed.ogg")
            prepare_audio(original_path, compressed_path)
            upload_path = compressed_path
        else:
            upload_path = original_path

        s3_key = f"{job_id}/compressed.ogg"
        s3_client.upload_file(upload_path, settings.s3_bucket, s3_key)

        task = process_media.apply_async(
            kwargs={
                "job_id": job_id,
                "original_filename": file.filename,
                "language": language,
                "model_size": model_size,
                "media_type": "video" if media_info["has_video"] else "audio",
            },
            task_id=job_id,
        )

        return {"job_id": job_id}

    finally:
        shutil.rmtree(job_dir, ignore_errors=True)


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
            s3_result_key=f"{job_id}/transcription.json",
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
    from worker import _models
    all_models = available_models()
    return {
        "available": all_models,
        "default": settings.model_size,
        "loaded": list(_models.keys()),
    }


@app.get("/transcriptions")
async def list_transcriptions(
    limit: int = 20,
    skip: int = 0,
    language: str = None,
    model_size: str = None,
):
    col = get_mongo()
    query = {}
    if language:
        query["language"] = language
    if model_size:
        query["model_size"] = model_size
    docs = list(
        col.find(query, {"_id": 0, "segments": 0})
           .sort("created_at", -1)
           .skip(skip)
           .limit(min(limit, 100))
    )
    total = col.count_documents(query)
    return {"total": total, "skip": skip, "limit": limit, "items": docs}


@app.get("/transcriptions/stats")
async def transcription_stats():
    col = get_mongo()
    docs = list(col.find({}, {"_id": 0, "audio_duration": 1, "processing_time_seconds": 1, "model_size": 1, "language": 1}))
    if not docs:
        return {"total_jobs": 0}
    total_audio = sum(d.get("audio_duration", 0) for d in docs)
    total_proc = sum(d.get("processing_time_seconds", 0) for d in docs)
    by_model = {}
    by_language = {}
    for d in docs:
        by_model[d.get("model_size", "unknown")] = by_model.get(d.get("model_size", "unknown"), 0) + 1
        by_language[d.get("language", "unknown")] = by_language.get(d.get("language", "unknown"), 0) + 1
    return {
        "total_jobs": len(docs),
        "total_audio_hours": round(total_audio / 3600, 3),
        "avg_processing_ratio": round(total_proc / total_audio, 3) if total_audio else 0,
        "by_model": by_model,
        "by_language": by_language,
    }


@app.get("/transcriptions/compare")
async def compare_transcriptions(job_ids: str):
    col = get_mongo()
    ids = [j.strip() for j in job_ids.split(",") if j.strip()]
    if not ids:
        raise HTTPException(status_code=400, detail="Provide at least one job_id in ?job_ids=id1,id2")
    docs = list(col.find(
        {"job_id": {"$in": ids}},
        {"_id": 0, "segments": 0}
    ))
    return {"jobs": docs}


@app.get("/transcriptions/{job_id}")
async def get_transcription(job_id: str):
    col = get_mongo()
    doc = col.find_one({"job_id": job_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Transcription not found")
    return doc


@app.get("/transcriptions/{job_id}/segments")
async def get_transcription_segments(job_id: str):
    col = get_mongo()
    doc = col.find_one({"job_id": job_id}, {"_id": 0, "segments": 1, "job_id": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Transcription not found")
    return {"job_id": doc["job_id"], "segments": doc.get("segments", [])}
