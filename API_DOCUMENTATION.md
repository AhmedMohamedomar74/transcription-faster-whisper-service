# Faster Whisper Transcription Service — API Documentation

## Overview

An async audio transcription REST API powered by [faster-whisper](https://github.com/SYSTRAN/faster-whisper).
Jobs are queued via Celery + Redis and processed by a background worker. The API never blocks on transcription — it returns a `job_id` immediately and lets you poll for results.

**Base URL:** `http://localhost:8000`

---

## Architecture

```
Client  ──POST /transcribe/async──►  FastAPI (api)
                                         │ saves file, enqueues task
                                         ▼
                                     Redis (broker + result backend)
                                         │
                                     Celery Worker
                                         │ loads WhisperModel, transcribes
                                         ▼
                                     Redis (stores result)

Client  ──GET /jobs/{job_id}──────►  FastAPI ──► Redis (reads result)
```

---

## Endpoints

### 1. `POST /transcribe/async` — Submit a transcription job

Uploads an audio file and returns a job ID. Processing happens asynchronously in the worker.

**Request**

| Part | Type | Required | Description |
|------|------|----------|-------------|
| `file` | `multipart/form-data` file | Yes | Audio file (any format ffmpeg supports: `.mp3`, `.ogg`, `.wav`, `.flac`, `.m4a`, …) |
| `language` | `form` string | No | BCP-47 language code (e.g. `ar`, `en`, `fr`). Omit for auto-detection. |

**Constraints**
- Max file size: `MAX_FILE_SIZE_MB` (default **200 MB**)

**Response `200 OK`**

```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6"
}
```

**Error responses**

| Status | Reason |
|--------|--------|
| `400` | No filename provided |
| `413` | File exceeds the size limit |

---

### 2. `GET /jobs/{job_id}` — Poll job status

Returns the current state of a transcription job.

**Path parameter**

| Name | Type | Description |
|------|------|-------------|
| `job_id` | string (UUID) | The ID returned by `POST /transcribe/async` |

**Response `200 OK`** — shape varies by `status`

#### `pending` — job is queued, not yet started

```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "pending",
  "estimated_wait_seconds": 120
}
```

#### `running` — worker is actively transcribing

```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "running",
  "progress": { "segments_done": 5 }
}
```

#### `done` — transcription complete

```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "done",
  "result": {
    "language": "ar",
    "language_probability": 0.998,
    "duration": 47.32,
    "segments": [
      {
        "start": 0.0,
        "end": 4.56,
        "text": "مرحباً بالعالم",
        "words": [
          { "start": 0.0,  "end": 1.2,  "word": "مرحباً", "probability": 0.99 },
          { "start": 1.3,  "end": 2.1,  "word": "بالعالم", "probability": 0.97 }
        ]
      }
    ]
  }
}
```

#### `failed` — transcription encountered an error

```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "failed",
  "error": "CUDA out of memory"
}
```

**`JobStatus` schema**

| Field | Type | Always present | Description |
|-------|------|---------------|-------------|
| `job_id` | string | Yes | UUID of the job |
| `status` | string | Yes | `pending` / `running` / `done` / `failed` |
| `position` | integer | No | Queue position (reserved for future use) |
| `progress` | object | No | Partial progress info while `running` |
| `estimated_wait_seconds` | integer | No | Rough ETA when `pending` |
| `result` | TranscriptionResult | No | Present only when `done` |
| `error` | string | No | Error message when `failed` |

**`TranscriptionResult` schema**

| Field | Type | Description |
|-------|------|-------------|
| `language` | string | Detected or forced language code |
| `language_probability` | float | Confidence of language detection (0–1) |
| `duration` | float | Audio duration in seconds (end of last segment) |
| `segments` | Segment[] | Ordered list of transcript segments |

**`Segment` schema**

| Field | Type | Description |
|-------|------|-------------|
| `start` | float | Start time in seconds |
| `end` | float | End time in seconds |
| `text` | string | Transcribed text for this segment |
| `words` | Word[] | Word-level timestamps (always present) |

**`Word` schema**

| Field | Type | Description |
|-------|------|-------------|
| `start` | float | Word start time in seconds |
| `end` | float | Word end time in seconds |
| `word` | string | The word token |
| `probability` | float | Token confidence (0–1) |

---

### 3. `GET /health` — Liveness check

Returns service liveness and basic metrics. Always returns `200`.

**Response `200 OK`**

```json
{
  "status": "alive",
  "model_loaded": true,
  "queue_depth": 2
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Always `"alive"` |
| `model_loaded` | boolean | `true` if at least one Celery worker responded to ping |
| `queue_depth` | integer | Number of active + reserved tasks across all workers |

---

### 4. `GET /ready` — Readiness check

Returns `200` only when a Celery worker is reachable. Use this as the Kubernetes/Docker readiness probe.

**Response `200 OK`**

```json
{ "status": "ready" }
```

**Response `503 Service Unavailable`** (worker not reachable)

```json
{ "status": "not_ready", "reason": "worker_not_available" }
```

---

### 5. `GET /models` — List available models

Returns all Whisper model sizes supported by faster-whisper and the currently loaded one.

**Response `200 OK`**

```json
{
  "available": ["tiny", "tiny.en", "base", "base.en", "small", "small.en", "medium", "medium.en", "large-v1", "large-v2", "large-v3"],
  "current": "small"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `available` | string[] | All model sizes faster-whisper knows about |
| `current` | string | The model loaded by the running worker (`MODEL_SIZE` env var) |

---

## Typical usage flow

```
1.  POST /transcribe/async  (upload file)
        ↓ { "job_id": "abc-123" }

2.  GET  /jobs/abc-123      (poll every 2–5 s)
        ↓ { "status": "pending", ... }
        ↓ { "status": "running", ... }
        ↓ { "status": "done", "result": { ... } }   ← stop polling
```

Results are stored in Redis for `JOB_RESULT_TTL_SECONDS` (default **24 hours**).

---

## Configuration (environment variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `MODEL_SIZE` | `small` | Whisper model variant |
| `DEVICE` | `cpu` | `cpu` or `cuda` |
| `COMPUTE_TYPE` | `int8` | `int8`, `float16`, `float32` |
| `MAX_FILE_SIZE_MB` | `200` | Maximum upload size |
| `REDIS_URL` | `redis://redis:6379/0` | Celery broker + backend |
| `UPLOAD_DIR` | `/tmp/uploads` | Temporary file storage |
| `JOB_RESULT_TTL_SECONDS` | `86400` | How long results are kept in Redis |

---

## Running locally

```bash
cp service/.env.example service/.env
docker compose -f service/docker-compose.yml up --build
```

The API will be available at `http://localhost:8000`.  
Interactive Swagger UI: `http://localhost:8000/docs`
