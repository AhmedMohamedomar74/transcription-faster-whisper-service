# Plan: Audio Chunking + Parallel Workers
### For: Junior Developer
### Goal: Cut 1-hour audio processing from ~15 min → ~7-8 min

---

## Why This Change

A 1-hour audio file currently runs as a **single Celery task on 1 worker** — sequential, uses 4 cores, takes ~15 min. This plan splits long files into 10-min chunks and processes them across **2 parallel workers** (each with 2 cores), then merges the results.

```
BEFORE:
  1 worker × 4 cores → 60 min audio → 15 min processing (sequential)

AFTER:
  Worker 1 (2 cores): chunk_0, chunk_2, chunk_4  ← parallel
  Worker 2 (2 cores): chunk_1, chunk_3, chunk_5  ← parallel
  → 3 rounds × 2.5 min = ~7.5 min total
```

**How the flow works:**
1. Client uploads 1-hour file → `POST /transcribe/async` → returns `job_id`
2. `transcribe_audio` task starts → splits file into 6 × 10-min WAV chunks using ffmpeg
3. Dispatches a **Celery chord**: 6 sub-tasks (`transcribe_audio_chunk`) + 1 merge callback
4. Each chunk task runs on whichever worker is free (Worker 1 or 2 picks it up)
5. When ALL 6 chunks finish → `merge_transcriptions` callback runs automatically
6. Merge callback applies timestamp offsets, deduplicates overlap zone, saves to MongoDB, stores final result under the original `job_id`
7. Client polling `GET /jobs/{job_id}` sees `running` during processing → `done` when merge completes

---

## Current File State (what's already done vs what needs doing)

| File | Status | What's needed |
|------|--------|---------------|
| `config.py` | ✅ Already updated | `chunk_duration_seconds`, `cpu_threads=2`, `batch_size=4` are in |
| `worker.py` | ❌ Needs full rewrite | Add `split_audio()`, `transcribe_audio_chunk`, `merge_transcriptions`, update `transcribe_audio` |
| `docker-compose.yml` | ❌ Needs update | 2 replicas, `OMP_NUM_THREADS=2`, `mem_limit=2g` per worker |
| `.env` | ❌ Needs update | `CPU_THREADS=2`, `BATCH_SIZE=4`, add `CHUNK_DURATION_SECONDS=600` |
| `.env.example` | ❌ Needs update | Same as `.env` + comments |

---

## Phase 1 — Rewrite `service/worker.py`

This is the main work. Replace the entire file with the code below.

**Key things being added:**
- `split_audio()` — uses ffmpeg to split any audio format into WAV chunks
- `_run_transcription()` — shared transcription logic (avoids code duplication)
- `transcribe_audio_chunk` — new Celery task for a single chunk, applies timestamp offset
- `merge_transcriptions` — Celery chord callback that merges all chunks
- Updated `transcribe_audio` — detects short vs long files, dispatches chord for long ones

### Complete `service/worker.py`:

```python
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
    """
    Split audio into chunks of `chunk_duration` seconds with `overlap` second overlap.
    Returns list of (chunk_path, start_offset_seconds).
    Works with any format ffmpeg supports: mp3, ogg, wav, flac, m4a, webm, etc.
    All chunks are output as 16kHz mono WAV (Whisper's native format).
    """
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
                "-ar", "16000",   # 16kHz — Whisper native sample rate
                "-ac", "1",       # mono
                chunk_path,
            ],
            capture_output=True, check=True,
        )
        chunks.append((chunk_path, float(start)))

    return chunks


def _run_transcription(model, audio_path: str, language: str, offset: float = 0.0):
    """
    Core transcription logic shared by single-file and chunk tasks.
    Applies `offset` to all segment and word timestamps.
    Returns (segments_list, info, processing_time_seconds).
    """
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

    # ── Short file: process inline (no splitting) ──────────────────
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

    # ── Long file: dispatch chord of chunk tasks ───────────────────
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

    # Delete original — chunks are now separate WAV files
    if os.path.isfile(audio_path):
        os.remove(audio_path)

    # Keep task in STARTED state so client sees "running".
    # merge_transcriptions will overwrite with SUCCESS + merged result when done.
    self.update_state(
        state="STARTED",
        meta={"progress": {"chunks": len(chunks), "status": "transcribing_chunks"}},
    )
    raise Ignore()


@celery_app.task(bind=True, name="transcribe_audio_chunk")
def transcribe_audio_chunk(self, audio_path: str, offset: float = 0.0,
                            language: str = None, model_size: str = None):
    """
    Processes one audio chunk. Called by the Celery chord inside transcribe_audio.
    `offset` is the chunk's start time in the original file — applied to all timestamps.
    """
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
    """
    Celery chord callback. Runs automatically after ALL chunk tasks finish.

    1. Sorts chunks by offset (guarantees order regardless of which worker ran first)
    2. Merges segments, deduplicates the 5-second overlap zone between chunks
    3. Saves final result to MongoDB
    4. Stores merged result under the original job_id in Redis
       → client polling GET /jobs/{job_id} will see status: done
    """
    active_size = model_size or settings.model_size

    chunk_results.sort(key=lambda r: r["offset"])

    merged_segments = []
    seen_end = -1.0

    for chunk in chunk_results:
        for seg in chunk["segments"]:
            # Only add segment if it starts after the last accepted end (minus 0.5s tolerance)
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

    # Overwrite the original task result so the client polling job_id sees SUCCESS
    celery_app.backend.store_result(job_id, merged_result, "SUCCESS")

    logger.info(
        "Merge complete: job=%s, %d chunks, %d segments, %.1fs, proc=%.1fs",
        job_id, len(chunk_results), len(merged_segments), total_duration, total_proc,
    )
    return merged_result
```

### ✅ Success Criteria — Phase 1 (worker.py)
- File saves without syntax errors: `python -c "import worker"` in the container
- Short file (< 10 min): worker logs show NO "Splitting" message, job completes normally
- Long file (> 10 min): worker logs show `"Splitting X.Xs audio into N chunks"`
- Chunk WAV files appear in `/tmp/uploads/` during processing, deleted after

---

## Phase 2 — Update `service/docker-compose.yml`

Change the `worker` service to run **2 replicas** with **2 cores each**.

**Full updated `docker-compose.yml`:**

```yaml
version: "3.9"

services:
  api:
    build: .
    ports:
      - "8000:8000"
    env_file: .env
    volumes:
      - uploads:/tmp/uploads
      - model_cache:/root/.cache/huggingface
    depends_on:
      - redis
      - mongo
    mem_limit: 2g

  worker:
    build: .
    command: celery -A worker worker --concurrency=1 --prefetch-multiplier=1 --loglevel=info
    env_file: .env
    environment:
      - OMP_NUM_THREADS=2        # 2 cores per worker × 2 workers = 4 cores total
    volumes:
      - uploads:/tmp/uploads
      - model_cache:/root/.cache/huggingface
    depends_on:
      - redis
      - mongo
    mem_limit: 2g               # ~950MB actual usage per worker — 2g is safe
    deploy:
      replicas: 2               # run 2 worker containers

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

  mongo:
    image: mongo:7
    volumes:
      - mongo_data:/data/db
    mem_limit: 512m

volumes:
  uploads:
  model_cache:
  redis_data:
  mongo_data:
```

**Key changes from current file:**
- `OMP_NUM_THREADS`: `4` → `2`
- `mem_limit` on worker: `4g` → `2g`
- Added `deploy.replicas: 2`
- Added `depends_on: mongo` to both api and worker

**⚠ Note on `deploy.replicas`:** Requires Docker Compose v2 (`docker compose` command, not `docker-compose`). Run with:
```bash
docker compose up --build
```

### ✅ Success Criteria — Phase 2
- `docker compose up --build` starts **4 containers**: api, worker-1, worker-2, redis, mongo (5 total)
- `docker compose ps` shows `service-worker-1` and `service-worker-2` both `running`
- Both workers show `"Model ready: small"` in logs on startup

---

## Phase 3 — Update `service/.env` and `service/.env.example`

### `.env` — replace performance section:

```dotenv
# Model configuration
MODEL_SIZE=small
DEVICE=cpu
COMPUTE_TYPE=int8

# Limits
MAX_FILE_SIZE_MB=200

# Redis
REDIS_URL=redis://redis:6379/0

# Storage
UPLOAD_DIR=/tmp/uploads
JOB_RESULT_TTL_SECONDS=86400

# MongoDB
MONGO_URL=mongodb://mongo:27017/whisper

# Task time limits
TASK_SOFT_TIME_LIMIT=3600
TASK_TIME_LIMIT=3660

# Audio chunking
CHUNK_DURATION_SECONDS=600

# Performance tuning
CPU_THREADS=2
BEAM_SIZE=1
BEST_OF=1
BATCH_SIZE=4
```

### `.env.example` — same content but with comments:

```dotenv
# Model configuration
MODEL_SIZE=small
DEVICE=cpu
COMPUTE_TYPE=int8

# Limits
MAX_FILE_SIZE_MB=200

# Redis
REDIS_URL=redis://redis:6379/0

# Storage
UPLOAD_DIR=/tmp/uploads
JOB_RESULT_TTL_SECONDS=86400

# MongoDB
MONGO_URL=mongodb://mongo:27017/whisper

# Task time limits (seconds). Raise for very long files.
TASK_SOFT_TIME_LIMIT=3600
TASK_TIME_LIMIT=3660

# Audio chunking — files longer than this are split into parallel tasks.
# Each chunk is processed by a separate worker. 600 = 10 min per chunk.
CHUNK_DURATION_SECONDS=600

# ── Performance tuning ──────────────────────────────────────────────
# OMP threads per worker. With 2 workers: 2 × 2 = 4 cores total.
# If running 1 worker only, set back to 4.
CPU_THREADS=2

# Beam search width. 1 = greedy (fastest). Raise to 3-5 for noisy audio.
BEAM_SIZE=1

# Sampling candidates. Must be >= BEAM_SIZE.
BEST_OF=1

# Parallel audio chunks per forward pass. 4 fits comfortably in 2g per worker.
# With 1 worker and 4g, you can raise this to 8.
BATCH_SIZE=4
```

### ✅ Success Criteria — Phase 3
- `docker compose up --build` uses new env values
- Worker logs show `"Loading model: small ... cpu_threads=2"` (not 4)

---

## End-to-End Verification Checklist

### Setup
- [ ] `docker compose up --build`
- [ ] `docker compose ps` → 5 running containers (api, worker×2, redis, mongo)
- [ ] Both workers log `"Model ready: small"` within ~30 seconds
- [ ] `GET /health` → `{ "model_loaded": true }`

### Short file test (< 10 min)
- [ ] `POST /transcribe/async` with a 5-min audio (mp3, ogg, wav, or flac)
- [ ] Worker logs show NO "Splitting" message
- [ ] `GET /jobs/{id}` → `status: done` with correct `segments` and `words`
- [ ] `GET /transcriptions/{id}` → MongoDB record shows `"chunks": 1`

### Long file test (> 10 min)
- [ ] `POST /transcribe/async` with a 1-hour audio
- [ ] Worker logs show `"Splitting Xs audio into N chunks"`
- [ ] BOTH workers show activity in logs simultaneously (parallel processing)
- [ ] `GET /jobs/{id}` shows `status: running` with `progress.chunks` while processing
- [ ] After completion → `status: done`, `result.segments` has correct timestamps (NOT reset to 0 for each chunk)
- [ ] Segments around the 600-second boundary have no duplicates and no gaps
- [ ] `GET /transcriptions/{id}` → `"chunks": 6` field in MongoDB record

### Multiple audio formats
- [ ] Submit `.mp3` → completes successfully
- [ ] Submit `.ogg` → completes successfully
- [ ] Submit `.wav` → completes successfully
- [ ] Submit `.flac` → completes successfully
- [ ] Submit `.m4a` → completes successfully

### Memory check
- [ ] `docker stats` during a long transcription → each worker stays under 2g

### Failure resilience
- [ ] Stop MongoDB: `docker compose stop mongo` → submit job → job still completes, worker logs `"MongoDB save failed (non-fatal)"`
- [ ] `docker compose start mongo` → `GET /transcriptions` → records resume saving

---

---

## Phase 4 — Update `service/API_DOCUMENTATION.md`

**Changes needed** (all additions, no removals):

### 4.1 — Update Architecture diagram
Add the chunking flow after the existing diagram:

```
Long file flow (> CHUNK_DURATION_SECONDS):
  transcribe_audio
      │ split_audio() via ffmpeg
      ├── chunk_0.wav ──► transcribe_audio_chunk (Worker 1)
      ├── chunk_1.wav ──► transcribe_audio_chunk (Worker 2)  ← parallel
      ├── chunk_2.wav ──► transcribe_audio_chunk (Worker 1)
      └── ...
            │ all chunks done
            ▼
      merge_transcriptions (applies offsets, deduplicates overlap)
            │
            ▼
      Redis (final result under original job_id)
      MongoDB (permanent record with chunks count)
```

### 4.2 — Update `POST /transcribe/async` — no request changes needed
The endpoint signature is unchanged. Add a note under the endpoint description:

> **Long file handling:** Files longer than `CHUNK_DURATION_SECONDS` (default: 600s / 10 min) are automatically split into parallel chunks. The `job_id` returned is still the polling ID — no client-side change required.

### 4.3 — Update `GET /jobs/{job_id}` — update `running` example
The `progress` field now shows chunk info for long files:

```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "running",
  "progress": { "chunks": 6, "status": "transcribing_chunks" }
}
```

### 4.4 — Update `TranscriptionRecord` schema table
Add new `chunks` field:

| Field | Type | Description |
|-------|------|-------------|
| `chunks` | integer | Number of audio chunks the file was split into (1 = no splitting) |

### 4.5 — Update Configuration table
Add new row:

| `CHUNK_DURATION_SECONDS` | `600` | Files longer than this are split into parallel chunks. Lower = more parallelism, higher = fewer chunks. |

### ✅ Success Criteria — Phase 4
- Architecture diagram shows chunking flow
- `GET /jobs` running example shows `progress.chunks`
- `TranscriptionRecord` table has `chunks` field
- Config table has `CHUNK_DURATION_SECONDS`

---

## Phase 5 — Update `service/faster-whisper.postman_collection.json`

**Changes needed:**

### 5.1 — Update "Get job status" — update `running` example response
Replace the existing `running` example body with:
```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "running",
  "progress": { "chunks": 6, "status": "transcribing_chunks" }
}
```

### 5.2 — Update "Get job status" — update test script
Add a check for the progress field when running:
```javascript
if (body.status === 'running' && body.progress) {
    pm.test('Progress has chunks field', () => {
        pm.expect(body.progress).to.have.property('chunks');
    });
}
```

### 5.3 — Update "Get full transcription" — update test script
Add check for `chunks` field in MongoDB record:
```javascript
pm.test('Has chunks field', () => {
    pm.expect(body).to.have.property('chunks');
    pm.expect(body.chunks).to.be.a('number').and.at.least(1);
});
```

### 5.4 — Update "Get full transcription" — update example response body
Add `"chunks": 6` to the full record example:
```json
{
  "job_id": "abc-123",
  "filename": "recording.mp3",
  "model_size": "small",
  "language": "ar",
  "audio_duration": 3600.0,
  "vad_removed_seconds": 45.2,
  "processing_time_seconds": 450.0,
  "chunks": 6,
  "segments": [...],
  "created_at": "2025-01-01T00:00:00Z"
}
```

### 5.5 — Update "List transcriptions" — update example response
Add `"chunks": 6` to the item in the list example.

### ✅ Success Criteria — Phase 5
- "Get job status" running example shows `progress.chunks`
- "Get full transcription" test checks for `chunks` field
- All example responses reflect the real API shape

---

## Summary of All File Changes

| File | Change |
|------|--------|
| `service/worker.py` | Full rewrite: add `split_audio()`, `_run_transcription()`, `transcribe_audio_chunk`, `merge_transcriptions`; update `transcribe_audio` |
| `service/docker-compose.yml` | `OMP_NUM_THREADS=2`, `mem_limit=2g`, `deploy.replicas=2`, `depends_on: mongo` |
| `service/.env` | `CPU_THREADS=2`, `BATCH_SIZE=4`, add `CHUNK_DURATION_SECONDS=600` |
| `service/.env.example` | Same as `.env` with explanatory comments |
| `service/config.py` | ✅ Already done — `chunk_duration_seconds`, `cpu_threads=2`, `batch_size=4` |
| `service/API_DOCUMENTATION.md` | Add chunking architecture diagram; update running example; add `chunks` to schema; add `CHUNK_DURATION_SECONDS` to config table |
| `service/faster-whisper.postman_collection.json` | Update running example response; update test scripts for `chunks` field |
