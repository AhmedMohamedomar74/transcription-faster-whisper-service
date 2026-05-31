import os
import math
import time
import datetime
import logging
import subprocess

from celery import Celery, chord
from celery.exceptions import Ignore
from celery.signals import worker_process_init
from faster_whisper import WhisperModel, BatchedInferencePipeline

from config import settings

logger = logging.getLogger(__name__)

celery_app = Celery(
    "transcriber",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    worker_concurrency=1,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=settings.task_soft_time_limit,
    task_time_limit=settings.task_time_limit,
    worker_max_tasks_per_child=50,
    task_track_started=True,
    result_expires=settings.job_result_ttl_seconds,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
)


@worker_process_init.connect
def preload_model_on_startup(**kwargs):
    get_model()


_models: dict = {}
_mongo_client = None


def get_mongo():
    global _mongo_client
    if _mongo_client is None:
        from pymongo import MongoClient
        _mongo_client = MongoClient(settings.mongo_url)
    return _mongo_client["whisper"]["transcriptions"]


def get_model(model_size: str = None):
    size = model_size or settings.model_size
    if size not in _models:
        logger.info(
            "Loading model: %s (device=%s, compute_type=%s, cpu_threads=%d)",
            size, settings.device, settings.compute_type, settings.cpu_threads,
        )
        base = WhisperModel(
            size,
            device=settings.device,
            compute_type=settings.compute_type,
            cpu_threads=settings.cpu_threads,
        )
        _models[size] = BatchedInferencePipeline(model=base)
        logger.info("Model ready: %s (batch_size=%d)", size, settings.batch_size)
    return _models[size]


def split_audio(audio_path: str, chunk_duration: int = 600, overlap: int = 5) -> list:
    probe = subprocess.run(
        [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            audio_path,
        ],
        capture_output=True, text=True, check=True,
    )
    total_duration = float(probe.stdout.strip())

    if total_duration <= chunk_duration:
        logger.info("Duration %.1fs <= %ds — no splitting needed", total_duration, chunk_duration)
        return [(audio_path, 0.0)]

    step = chunk_duration - overlap
    n_chunks = math.ceil((total_duration - overlap) / step)
    base = os.path.splitext(audio_path)[0]
    chunks = []

    logger.info(
        "Splitting %.1fs audio into %d chunks (chunk=%ds, overlap=%ds)",
        total_duration, n_chunks, chunk_duration, overlap,
    )

    for i in range(n_chunks):
        start = i * step
        duration = min(chunk_duration, total_duration - start)
        if duration <= 1.0:
            break
        chunk_path = f"{base}_chunk{i}.wav"
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", audio_path,
                "-ss", str(start),
                "-t", str(duration),
                "-ar", "16000",
                "-ac", "1",
                chunk_path,
            ],
            capture_output=True, check=True,
        )
        chunks.append((chunk_path, float(start)))

    return chunks


def _run_transcription(model, audio_path: str, language: str, offset: float = 0.0):
    t0 = time.time()
    segments, info = model.transcribe(
        audio_path,
        language=language,
        beam_size=settings.beam_size,
        best_of=settings.best_of,
        vad_filter=True,
        vad_parameters={"min_silence_duration_ms": 500, "speech_pad_ms": 200},
        word_timestamps=True,
        batch_size=settings.batch_size,
    )

    segments_list = []
    for segment in segments:
        words = []
        if segment.words:
            for word in segment.words:
                words.append({
                    "start":       round(word.start + offset, 3),
                    "end":         round(word.end   + offset, 3),
                    "word":        word.word,
                    "probability": word.probability,
                })
        segments_list.append({
            "start": round(segment.start + offset, 3),
            "end":   round(segment.end   + offset, 3),
            "text":  segment.text,
            "words": words,
        })

    return segments_list, info, round(time.time() - t0, 2)


@celery_app.task(bind=True, name="transcribe_audio")
def transcribe_audio(self, audio_path: str, language: str = None, model_size: str = None):
    active_size = model_size or settings.model_size
    logger.info("Job %s: %s (language=%s, model=%s)", self.request.id, audio_path, language, active_size)

    chunks = split_audio(audio_path, chunk_duration=settings.chunk_duration_seconds)

    if len(chunks) == 1:
        model = get_model(model_size)
        chunk_path, _ = chunks[0]
        segments_list, info, processing_time = _run_transcription(model, chunk_path, language)

        if chunk_path != audio_path and os.path.isfile(chunk_path):
            os.remove(chunk_path)
        if os.path.isfile(audio_path):
            os.remove(audio_path)

        result = {
            "language":             info.language,
            "language_probability": info.language_probability,
            "duration":             segments_list[-1]["end"] if segments_list else 0.0,
            "segments":             segments_list,
        }

        try:
            col = get_mongo()
            col.insert_one({
                "job_id":                  self.request.id,
                "filename":                os.path.basename(audio_path),
                "model_size":              active_size,
                "language":                info.language,
                "language_probability":    info.language_probability,
                "audio_duration":          round(info.duration, 3),
                "vad_removed_seconds":     round(info.duration - info.duration_after_vad, 3),
                "duration_after_vad":      round(info.duration_after_vad, 3),
                "processing_time_seconds": processing_time,
                "segments":                segments_list,
                "chunks":                  1,
                "created_at":              datetime.datetime.utcnow(),
            })
        except Exception as e:
            logger.warning("MongoDB save failed (non-fatal): %s", e)

        logger.info("Done: %d segments, %.1fs", len(segments_list), result["duration"])
        return result

    logger.info("Dispatching chord of %d chunk tasks for job %s", len(chunks), self.request.id)

    subtasks = [
        transcribe_audio_chunk.s(
            chunk_path,
            offset=offset,
            language=language,
            model_size=model_size,
        )
        for chunk_path, offset in chunks
    ]

    callback = merge_transcriptions.s(
        job_id=self.request.id,
        original_filename=os.path.basename(audio_path),
        model_size=model_size,
    )

    chord(subtasks)(callback)

    if os.path.isfile(audio_path):
        os.remove(audio_path)

    self.update_state(
        state="STARTED",
        meta={"progress": {"chunks": len(chunks), "status": "transcribing_chunks"}},
    )
    raise Ignore()


@celery_app.task(bind=True, name="transcribe_audio_chunk")
def transcribe_audio_chunk(self, audio_path: str, offset: float = 0.0,
                            language: str = None, model_size: str = None):
    model = get_model(model_size)
    logger.info("Chunk offset=%.1fs: %s", offset, audio_path)

    segments_list, info, processing_time = _run_transcription(model, audio_path, language, offset=offset)

    if os.path.isfile(audio_path):
        os.remove(audio_path)

    return {
        "offset":               offset,
        "language":             info.language,
        "language_probability": info.language_probability,
        "audio_duration":       round(info.duration, 3),
        "vad_removed_seconds":  round(info.duration - info.duration_after_vad, 3),
        "duration_after_vad":   round(info.duration_after_vad, 3),
        "processing_time":      processing_time,
        "segments":             segments_list,
    }


@celery_app.task(name="merge_transcriptions")
def merge_transcriptions(chunk_results: list, job_id: str,
                          original_filename: str, model_size: str = None):
    active_size = model_size or settings.model_size

    chunk_results.sort(key=lambda r: r["offset"])

    merged_segments = []
    seen_end = -1.0

    for chunk in chunk_results:
        for seg in chunk["segments"]:
            if seg["start"] >= seen_end - 0.5:
                merged_segments.append(seg)
                seen_end = max(seen_end, seg["end"])

    total_duration  = merged_segments[-1]["end"] if merged_segments else 0.0
    best_chunk      = max(chunk_results, key=lambda r: r["language_probability"])
    total_audio     = sum(r["audio_duration"]    for r in chunk_results)
    total_vad_cut   = sum(r["vad_removed_seconds"] for r in chunk_results)
    total_proc      = sum(r["processing_time"]   for r in chunk_results)

    merged_result = {
        "language":             best_chunk["language"],
        "language_probability": best_chunk["language_probability"],
        "duration":             round(total_duration, 3),
        "segments":             merged_segments,
    }

    try:
        col = get_mongo()
        col.insert_one({
            "job_id":                  job_id,
            "filename":                original_filename,
            "model_size":              active_size,
            "language":                merged_result["language"],
            "language_probability":    merged_result["language_probability"],
            "audio_duration":          round(total_audio, 3),
            "vad_removed_seconds":     round(total_vad_cut, 3),
            "duration_after_vad":      round(total_audio - total_vad_cut, 3),
            "processing_time_seconds": round(total_proc, 2),
            "segments":                merged_segments,
            "chunks":                  len(chunk_results),
            "created_at":              datetime.datetime.utcnow(),
        })
    except Exception as e:
        logger.warning("MongoDB save failed (non-fatal): %s", e)

    celery_app.backend.store_result(job_id, merged_result, "SUCCESS")

    logger.info(
        "Merge complete: job=%s, %d chunks, %d segments, %.1fs, proc=%.1fs",
        job_id, len(chunk_results), len(merged_segments), total_duration, total_proc,
    )
    return merged_result
